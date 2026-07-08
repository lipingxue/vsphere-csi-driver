/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vksregistervolume

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	clientset "k8s.io/client-go/kubernetes"
	vksregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/vksregistervolume/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
)

const (
	labelCreatedBy          = "cns.vmware.com/created-by"
	labelCreatedByValue     = "vksregistervolume"
	labelCRNamespace        = "cns.vmware.com/vksregistervolume-namespace"
	labelCRName             = "cns.vmware.com/vksregistervolume-name"
	annProvisionedBy        = "pv.kubernetes.io/provisioned-by"
	defaultFSType           = "ext4"
	supervisorCRNameSuffix  = "-reg-"
	supervisorCRHashLength  = 16
)

// buildSupervisorCRName returns the deterministic Supervisor CnsRegisterVolume CR name.
// Format: <tanzuKubernetesClusterUID>-reg-<sha256(guestNamespace+"/"+crName)[:16]>
func buildSupervisorCRName(tanzuKubernetesClusterUID, guestNamespace, crName string) string {
	h := sha256.Sum256([]byte(guestNamespace + "/" + crName))
	hash := fmt.Sprintf("%x", h)[:supervisorCRHashLength]
	return tanzuKubernetesClusterUID + supervisorCRNameSuffix + hash
}

// validateDiskURLPath checks that s is a well-formed Option-A URL:
// https scheme, /folder/ path prefix, and both dcPath and dsName query params present and non-empty.
func validateDiskURLPath(s string) error {
	if s == "" {
		return fmt.Errorf("diskURLPath must not be empty")
	}
	u, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("diskURLPath is not a valid URL: %v", err)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("diskURLPath must use https scheme, got %q", u.Scheme)
	}
	if !strings.HasPrefix(u.Path, "/folder/") {
		return fmt.Errorf("diskURLPath path must start with /folder/, got %q", u.Path)
	}
	q := u.Query()
	if q.Get("dcPath") == "" {
		return fmt.Errorf("diskURLPath missing dcPath query parameter")
	}
	if q.Get("dsName") == "" {
		return fmt.Errorf("diskURLPath missing dsName query parameter")
	}
	return nil
}

// buildGuestPVSpec constructs the guest PersistentVolume spec for direct binding.
// All capacity/access/mode/class are sourced from the pre-created PVC.
// volumeHandle is always the Supervisor PVC name (== supervisorCRName), NOT the FCD UUID.
func buildGuestPVSpec(
	guestPVName string,
	supervisorPVCName string,
	pvc *v1.PersistentVolumeClaim,
	cr *vksregistervolumev1alpha1.VKSRegisterVolume,
	accessibleTopology []map[string]string,
) *v1.PersistentVolume {
	// Determine volumeMode; default to Filesystem.
	volumeMode := v1.PersistentVolumeFilesystem
	if pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode == v1.PersistentVolumeBlock {
		volumeMode = v1.PersistentVolumeBlock
	}

	// Capacity from PVC request.
	storage := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	capacity := resource.NewQuantity(storage.Value(), resource.BinarySI)

	// CSI source.
	csiSource := &v1.CSIPersistentVolumeSource{
		Driver:           cnsoperatortypes.VSphereCSIDriverName,
		VolumeHandle:     supervisorPVCName,
		ReadOnly:         false,
	}
	if volumeMode == v1.PersistentVolumeFilesystem {
		csiSource.FSType = defaultFSType
	}

	// StorageClass from PVC.
	storageClassName := ""
	if pvc.Spec.StorageClassName != nil {
		storageClassName = *pvc.Spec.StorageClassName
	}

	// claimRef locks the PV to the pre-created PVC (Direct Binding).
	claimRef := &v1.ObjectReference{
		Kind:       "PersistentVolumeClaim",
		APIVersion: "v1",
		Namespace:  cr.Namespace,
		Name:       cr.Spec.PVCName,
	}

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: guestPVName,
			Labels: map[string]string{
				labelCreatedBy:   labelCreatedByValue,
				labelCRNamespace: cr.Namespace,
				labelCRName:      cr.Name,
			},
			Annotations: map[string]string{
				annProvisionedBy: cnsoperatortypes.VSphereCSIDriverName,
			},
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity:                      v1.ResourceList{v1.ResourceStorage: *capacity},
			AccessModes:                   pvc.Spec.AccessModes,
			VolumeMode:                    &volumeMode,
			StorageClassName:              storageClassName,
			PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimDelete,
			ClaimRef:                      claimRef,
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: csiSource,
			},
		},
	}

	// Node affinity from Supervisor PVC topology annotation.
	if len(accessibleTopology) > 0 {
		pv.Spec.NodeAffinity = syncer.GenerateVolumeNodeAffinity(toCSITopology(accessibleTopology))
	}

	return pv
}

// toCSITopology converts []map[string]string to []*csi.Topology.
func toCSITopology(topology []map[string]string) []*csi.Topology {
	result := make([]*csi.Topology, 0, len(topology))
	for _, m := range topology {
		t := &csi.Topology{Segments: m}
		result = append(result, t)
	}
	return result
}

// parseVolumeAccessibleTopology parses the AnnVolumeAccessibleTopology annotation value.
// Returns nil (not an error) when the annotation is absent or empty.
func parseVolumeAccessibleTopology(pvc *v1.PersistentVolumeClaim) ([]map[string]string, error) {
	raw, ok := pvc.Annotations[common.AnnVolumeAccessibleTopology]
	if !ok || raw == "" {
		return nil, nil
	}
	var result []map[string]string
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return nil, fmt.Errorf("failed to parse %s annotation on PVC %s/%s: %v",
			common.AnnVolumeAccessibleTopology, pvc.Namespace, pvc.Name, err)
	}
	return result, nil
}

// isPVCBound watches the named PVC for up to timeout and returns true when it becomes Bound.
func isPVCBound(ctx context.Context, client clientset.Interface,
	pvcName, namespace string, timeout time.Duration) (bool, error) {
	log := logger.GetLogger(ctx)
	timeoutSeconds := int64(timeout.Seconds())

	log.Infof("Watching PVC %s/%s for Bound phase (timeout %ds)", namespace, pvcName, timeoutSeconds)
	watchClaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Watch(
		ctx,
		metav1.ListOptions{
			FieldSelector:  fields.OneTermEqualSelector("metadata.name", pvcName).String(),
			TimeoutSeconds: &timeoutSeconds,
			Watch:          true,
		})
	if err != nil {
		return false, fmt.Errorf("failed to watch PVC %s/%s: %v", namespace, pvcName, err)
	}
	defer watchClaim.Stop()

	for event := range watchClaim.ResultChan() {
		pvc, ok := event.Object.(*v1.PersistentVolumeClaim)
		if !ok {
			continue
		}
		if pvc.Status.Phase == v1.ClaimBound {
			log.Infof("PVC %s/%s is Bound", namespace, pvcName)
			return true, nil
		}
	}
	return false, fmt.Errorf("PVC %s/%s did not reach Bound within %ds", namespace, pvcName, timeoutSeconds)
}

// getSupervisorStorageClass returns the svstorageclass parameter value from the given StorageClass,
// or "" if the param is absent.
func getSupervisorStorageClass(params map[string]string) string {
	for k, v := range params {
		if strings.EqualFold(k, common.AttributeSupervisorStorageClass) {
			return v
		}
	}
	return ""
}
