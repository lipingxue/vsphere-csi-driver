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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	apis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	vksregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/vksregistervolume/v1alpha1"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	commonconfig "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	cnsoperatortypes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer/cnsoperator/util"
)

const (
	workerThreadsEnvVar        = "WORKER_THREADS_VKS_REGISTER_VOLUME"
	defaultMaxWorkerThreads    = 10
	vksRegisterVolumeFinalizer = "cns.vmware.com/vks-register-volume"
	// pvcMissingTolerance is how long we wait for the referenced PVC to appear
	// before escalating to a terminal Failed (SnapService may create CR and PVC near-simultaneously).
	pvcMissingTolerance = 5 * time.Minute
	// pvcBindTimeout is the Watch timeout used in WaitingForGuestPVCBound.
	pvcBindTimeout = 1 * time.Minute
)

var (
	// backOffDuration is a map of VKSRegisterVolume NamespacedName to the next requeue delay.
	// Initialized to 1 second for new instances; doubled on each transient failure (capped at 5 min);
	// reset to 1 second on success.
	backOffDuration         map[apitypes.NamespacedName]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

// Add creates a new VKSRegisterVolume Controller and adds it to the Manager.
// The controller only activates in VKS (guest) clusters and when the vks-register-volume FSS is enabled.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *commonconfig.ConfigurationInfo, _ volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()

	if clusterFlavor != cnstypes.CnsClusterFlavorGuest {
		log.Debug("Not initializing the VKSRegisterVolume Controller: not a guest cluster")
		return nil
	}

	if !commonco.ContainerOrchestratorUtility.IsFSSEnabled(ctx, common.VKSRegisterVolume) {
		log.Infof("VKSRegisterVolume FSS %q is disabled; skipping controller registration",
			common.VKSRegisterVolume)
		return nil
	}

	// Build Supervisor clients.
	supervisorNamespace, err := commonconfig.GetSupervisorNamespace(ctx)
	if err != nil {
		log.Errorf("Failed to get supervisor namespace: %v", err)
		return err
	}

	restConfig := k8s.GetRestClientConfigForSupervisor(ctx, configInfo.Cfg.GC.Endpoint, configInfo.Cfg.GC.Port)

	supervisorClient, err := k8s.NewSupervisorClient(ctx, restConfig)
	if err != nil {
		log.Errorf("Failed to create supervisor client: %v", err)
		return err
	}

	supervisorCnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, apis.GroupName)
	if err != nil {
		log.Errorf("Failed to create supervisor cns operator client: %v", err)
		return err
	}

	tanzuKubernetesClusterUID := configInfo.Cfg.GC.TanzuKubernetesClusterUID

	// Guest k8s client for PV/PVC operations.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Failed to create k8s client: %v", err)
		return err
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{Interface: k8sclient.CoreV1().Events("")},
	)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: apis.GroupName})

	r := &ReconcileVKSRegisterVolume{
		client:                      mgr.GetClient(),
		scheme:                      mgr.GetScheme(),
		configInfo:                  configInfo,
		recorder:                    recorder,
		k8sclient:                   k8sclient,
		supervisorNamespace:         supervisorNamespace,
		tanzuKubernetesClusterUID:   tanzuKubernetesClusterUID,
		supervisorClient:            supervisorClient,
		supervisorCnsOperatorClient: supervisorCnsOperatorClient,
	}

	return add(mgr, r)
}

// add registers the controller with the manager and initialises the backoff map.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()

	maxWorkerThreads := util.GetMaxWorkerThreads(ctx, workerThreadsEnvVar, defaultMaxWorkerThreads)
	err := ctrl.NewControllerManagedBy(mgr).
		Named("vksregistervolume-controller").
		For(&vksregistervolumev1alpha1.VKSRegisterVolume{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxWorkerThreads}).
		Complete(r)
	if err != nil {
		log.Errorf("Failed to build VKSRegisterVolume controller: %v", err)
		return err
	}

	backOffDuration = make(map[apitypes.NamespacedName]time.Duration)
	return nil
}

// blank assignment to verify that ReconcileVKSRegisterVolume implements reconcile.Reconciler.
var _ reconcile.Reconciler = &ReconcileVKSRegisterVolume{}

// ReconcileVKSRegisterVolume reconciles a VKSRegisterVolume object.
type ReconcileVKSRegisterVolume struct {
	client                      client.Client
	scheme                      *runtime.Scheme
	configInfo                  *commonconfig.ConfigurationInfo
	recorder                    record.EventRecorder
	k8sclient                   clientset.Interface
	supervisorNamespace         string
	tanzuKubernetesClusterUID   string
	supervisorClient            clientset.Interface
	supervisorCnsOperatorClient client.Client
}

// Reconcile drives the VKSRegisterVolume phase state machine.
func (r *ReconcileVKSRegisterVolume) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)

	// Fetch the CR.
	instance := &vksregistervolumev1alpha1.VKSRegisterVolume{}
	if err := r.client.Get(ctx, request.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("VKSRegisterVolume %s/%s not found; assuming deleted",
				request.Namespace, request.Name)
			return reconcile.Result{}, nil
		}
		log.Errorf("Error reading VKSRegisterVolume %s/%s: %v", request.Namespace, request.Name, err)
		return reconcile.Result{}, err
	}

	// Handle deletion.
	if instance.DeletionTimestamp != nil {
		return r.reconcileDelete(ctx, instance, request)
	}

	// Already successfully registered — nothing to do.
	if instance.Status.Phase == vksregistervolumev1alpha1.PhaseRegistered {
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, request.NamespacedName)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	// Read/initialise per-CR backoff timeout.
	backOffDurationMapMutex.Lock()
	if _, exists := backOffDuration[request.NamespacedName]; !exists {
		backOffDuration[request.NamespacedName] = time.Second
	}
	timeout := backOffDuration[request.NamespacedName]
	backOffDurationMapMutex.Unlock()

	log.Infof("Reconciling VKSRegisterVolume %s/%s phase=%q timeout=%s",
		request.Namespace, request.Name, instance.Status.Phase, timeout)

	// --- Phase dispatch ---

	// Step 1: ensure finalizer is present.
	if !controllerutil.ContainsFinalizer(instance, vksRegisterVolumeFinalizer) {
		controllerutil.AddFinalizer(instance, vksRegisterVolumeFinalizer)
		if err := r.client.Update(ctx, instance); err != nil {
			log.Errorf("Failed to add finalizer to VKSRegisterVolume %s/%s: %v",
				request.Namespace, request.Name, err)
			r.setError(ctx, instance, "Failed to add finalizer")
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		return reconcile.Result{Requeue: true}, nil
	}

	// Phases "" and "Pending" both run validation + PVC resolution.
	if instance.Status.Phase == "" ||
		instance.Status.Phase == vksregistervolumev1alpha1.PhasePending {
		return r.reconcilePending(ctx, instance, request, timeout)
	}

	switch instance.Status.Phase {
	case vksregistervolumev1alpha1.PhaseCreatingSupervisorCR:
		return r.reconcileCreatingSupervisorCR(ctx, instance, request, timeout)
	case vksregistervolumev1alpha1.PhaseWaitingForSupervisorRegistration:
		return r.reconcileWaitingForSupervisorRegistration(ctx, instance, request, timeout)
	case vksregistervolumev1alpha1.PhaseWaitingForSupervisorBinding:
		return r.reconcileWaitingForSupervisorBinding(ctx, instance, request, timeout)
	case vksregistervolumev1alpha1.PhaseCreatingGuestPV:
		return r.reconcileCreatingGuestPV(ctx, instance, request, timeout)
	case vksregistervolumev1alpha1.PhaseWaitingForGuestPVCBound:
		return r.reconcileWaitingForGuestPVCBound(ctx, instance, request, timeout)
	default:
		log.Warnf("VKSRegisterVolume %s/%s has unexpected phase %q; re-queuing",
			request.Namespace, request.Name, instance.Status.Phase)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
}

// reconcilePending validates the CR spec and resolves the referenced PVC.
func (r *ReconcileVKSRegisterVolume) reconcilePending(ctx context.Context,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume,
	request reconcile.Request, timeout time.Duration) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	// Terminal spec validation.
	if instance.Spec.VolumeID == "" {
		r.setError(ctx, instance, "spec.volumeID must not be empty")
		return reconcile.Result{}, nil
	}
	if instance.Spec.PVCName == "" {
		r.setError(ctx, instance, "spec.pvcName must not be empty")
		return reconcile.Result{}, nil
	}
	if err := validateDiskURLPath(instance.Spec.DiskURLPath); err != nil {
		r.setError(ctx, instance, fmt.Sprintf("spec.diskURLPath invalid: %v", err))
		return reconcile.Result{}, nil
	}

	// Resolve the pre-created PVC.
	pvc, err := r.k8sclient.CoreV1().PersistentVolumeClaims(instance.Namespace).
		Get(ctx, instance.Spec.PVCName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Tolerate missing PVC for pvcMissingTolerance (SnapService race).
			if time.Since(instance.CreationTimestamp.Time) < pvcMissingTolerance {
				msg := fmt.Sprintf("PVC %s/%s not found; waiting", instance.Namespace, instance.Spec.PVCName)
				log.Infof(msg)
				r.setError(ctx, instance, msg)
				return reconcile.Result{RequeueAfter: timeout}, nil
			}
			r.setError(ctx, instance,
				fmt.Sprintf("PVC %s/%s not found after %s; giving up",
					instance.Namespace, instance.Spec.PVCName, pvcMissingTolerance))
			return reconcile.Result{}, nil
		}
		r.setError(ctx, instance, fmt.Sprintf("Failed to get PVC %s/%s: %v",
			instance.Namespace, instance.Spec.PVCName, err))
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// PVC must still be Pending (not yet Bound).
	if pvc.Status.Phase == v1.ClaimBound {
		r.setError(ctx, instance,
			fmt.Sprintf("PVC %s/%s is already Bound; cannot use as a Direct Binding target",
				instance.Namespace, instance.Spec.PVCName))
		return reconcile.Result{}, nil
	}

	// PVC must have spec.volumeName set (the future PV name the operator will create).
	if pvc.Spec.VolumeName == "" {
		r.setError(ctx, instance,
			fmt.Sprintf("PVC %s/%s has no spec.volumeName; SnapService must set this to the desired PV name",
				instance.Namespace, instance.Spec.PVCName))
		return reconcile.Result{}, nil
	}

	// PVC must have access modes and a storage request.
	if len(pvc.Spec.AccessModes) == 0 {
		r.setError(ctx, instance,
			fmt.Sprintf("PVC %s/%s has no accessModes", instance.Namespace, instance.Spec.PVCName))
		return reconcile.Result{}, nil
	}
	storageReq := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	if pvc.Spec.Resources.Requests == nil || storageReq.IsZero() {
		r.setError(ctx, instance,
			fmt.Sprintf("PVC %s/%s has no storage request", instance.Namespace, instance.Spec.PVCName))
		return reconcile.Result{}, nil
	}

	// PVC must reference a StorageClass that carries the svstorageclass parameter.
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
		r.setError(ctx, instance,
			fmt.Sprintf("PVC %s/%s has no storageClassName", instance.Namespace, instance.Spec.PVCName))
		return reconcile.Result{}, nil
	}
	sc, err := r.k8sclient.StorageV1().StorageClasses().Get(ctx, *pvc.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {
		r.setError(ctx, instance,
			fmt.Sprintf("Failed to get StorageClass %q: %v", *pvc.Spec.StorageClassName, err))
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	if getSupervisorStorageClass(sc.Parameters) == "" {
		r.setError(ctx, instance,
			fmt.Sprintf("StorageClass %q has no %q parameter", *pvc.Spec.StorageClassName,
				common.AttributeSupervisorStorageClass))
		return reconcile.Result{}, nil
	}

	// All checks passed — advance to CreatingSupervisorCR.
	log.Infof("VKSRegisterVolume %s/%s validated; PVC.volumeName=%q; advancing to %s",
		instance.Namespace, instance.Name, pvc.Spec.VolumeName,
		vksregistervolumev1alpha1.PhaseCreatingSupervisorCR)
	if err := r.setPhase(ctx, instance, vksregistervolumev1alpha1.PhaseCreatingSupervisorCR); err != nil {
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	return reconcile.Result{Requeue: true}, nil
}

// reconcileCreatingSupervisorCR creates the Supervisor CnsRegisterVolume CR if absent.
func (r *ReconcileVKSRegisterVolume) reconcileCreatingSupervisorCR(ctx context.Context,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume,
	request reconcile.Request, timeout time.Duration) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	svCRName := buildSupervisorCRName(r.tanzuKubernetesClusterUID, instance.Namespace, instance.Name)

	// Re-fetch the PVC to get current accessModes and volumeMode.
	pvc, err := r.k8sclient.CoreV1().PersistentVolumeClaims(instance.Namespace).
		Get(ctx, instance.Spec.PVCName, metav1.GetOptions{})
	if err != nil {
		r.setError(ctx, instance, fmt.Sprintf("Failed to re-fetch PVC %s/%s: %v",
			instance.Namespace, instance.Spec.PVCName, err))
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Check if the Supervisor CR already exists (idempotent re-entry).
	existing := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	err = r.supervisorCnsOperatorClient.Get(ctx,
		apitypes.NamespacedName{Namespace: r.supervisorNamespace, Name: svCRName}, existing)
	if err == nil {
		// Already exists — advance.
		log.Infof("Supervisor CnsRegisterVolume %s/%s already exists; advancing to %s",
			r.supervisorNamespace, svCRName, vksregistervolumev1alpha1.PhaseWaitingForSupervisorRegistration)
		if err := r.setPhase(ctx, instance,
			vksregistervolumev1alpha1.PhaseWaitingForSupervisorRegistration); err != nil {
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	if !apierrors.IsNotFound(err) {
		r.setError(ctx, instance,
			fmt.Sprintf("Failed to check Supervisor CnsRegisterVolume %s: %v", svCRName, err))
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Determine accessMode and volumeMode from PVC.
	accessMode := v1.ReadWriteOnce
	if len(pvc.Spec.AccessModes) > 0 {
		accessMode = pvc.Spec.AccessModes[0]
	}
	volumeMode := v1.PersistentVolumeFilesystem
	if pvc.Spec.VolumeMode != nil {
		volumeMode = *pvc.Spec.VolumeMode
	}

	svCR := &cnsregistervolumev1alpha1.CnsRegisterVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svCRName,
			Namespace: r.supervisorNamespace,
		},
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
			PvcName:     svCRName, // Supervisor PVC name == Supervisor CR name
			VolumeID:    instance.Spec.VolumeID,
			DiskURLPath: instance.Spec.DiskURLPath,
			AccessMode:  accessMode,
			VolumeMode:  volumeMode,
		},
	}

	if err := r.supervisorCnsOperatorClient.Create(ctx, svCR); err != nil {
		r.setError(ctx, instance,
			fmt.Sprintf("Failed to create Supervisor CnsRegisterVolume %s: %v", svCRName, err))
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	log.Infof("Created Supervisor CnsRegisterVolume %s/%s", r.supervisorNamespace, svCRName)
	if err := r.setPhase(ctx, instance,
		vksregistervolumev1alpha1.PhaseWaitingForSupervisorRegistration); err != nil {
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	return reconcile.Result{RequeueAfter: timeout}, nil
}

// reconcileWaitingForSupervisorRegistration polls the Supervisor CR until it is registered.
func (r *ReconcileVKSRegisterVolume) reconcileWaitingForSupervisorRegistration(ctx context.Context,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume,
	request reconcile.Request, timeout time.Duration) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	svCRName := buildSupervisorCRName(r.tanzuKubernetesClusterUID, instance.Namespace, instance.Name)

	svCR := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	if err := r.supervisorCnsOperatorClient.Get(ctx,
		apitypes.NamespacedName{Namespace: r.supervisorNamespace, Name: svCRName}, svCR); err != nil {
		r.setError(ctx, instance,
			fmt.Sprintf("Failed to get Supervisor CnsRegisterVolume %s: %v", svCRName, err))
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	if svCR.Status.Error != "" {
		// Supervisor reported a terminal failure.
		r.setError(ctx, instance,
			fmt.Sprintf("Supervisor CnsRegisterVolume %s failed: %s", svCRName, svCR.Status.Error))
		r.setPhase(ctx, instance, vksregistervolumev1alpha1.PhaseFailed) //nolint:errcheck
		return reconcile.Result{}, nil
	}

	if !svCR.Status.Registered {
		log.Infof("Supervisor CnsRegisterVolume %s/%s not yet registered; requeuing",
			r.supervisorNamespace, svCRName)
		r.setError(ctx, instance, fmt.Sprintf("Waiting for Supervisor CnsRegisterVolume %s", svCRName))
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	log.Infof("Supervisor CnsRegisterVolume %s/%s registered; advancing to %s",
		r.supervisorNamespace, svCRName, vksregistervolumev1alpha1.PhaseWaitingForSupervisorBinding)
	if err := r.setPhase(ctx, instance,
		vksregistervolumev1alpha1.PhaseWaitingForSupervisorBinding); err != nil {
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	return reconcile.Result{Requeue: true}, nil
}

// reconcileWaitingForSupervisorBinding polls the Supervisor PVC until it is Bound.
func (r *ReconcileVKSRegisterVolume) reconcileWaitingForSupervisorBinding(ctx context.Context,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume,
	request reconcile.Request, timeout time.Duration) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	svPVCName := buildSupervisorCRName(r.tanzuKubernetesClusterUID, instance.Namespace, instance.Name)

	svPVC, err := r.supervisorClient.CoreV1().PersistentVolumeClaims(r.supervisorNamespace).
		Get(ctx, svPVCName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Infof("Supervisor PVC %s/%s not yet found; requeuing", r.supervisorNamespace, svPVCName)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		r.setError(ctx, instance,
			fmt.Sprintf("Failed to get Supervisor PVC %s/%s: %v", r.supervisorNamespace, svPVCName, err))
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	if svPVC.Status.Phase != v1.ClaimBound {
		log.Infof("Supervisor PVC %s/%s phase=%s; requeuing",
			r.supervisorNamespace, svPVCName, svPVC.Status.Phase)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Parse topology annotation (nil is acceptable).
	accessibleTopology, err := parseVolumeAccessibleTopology(svPVC)
	if err != nil {
		log.Warnf("Failed to parse topology annotation on Supervisor PVC %s/%s: %v; continuing without topology",
			r.supervisorNamespace, svPVCName, err)
		accessibleTopology = nil
	}

	log.Infof("Supervisor PVC %s/%s is Bound; topology=%v; advancing to %s",
		r.supervisorNamespace, svPVCName, accessibleTopology,
		vksregistervolumev1alpha1.PhaseCreatingGuestPV)

	// Store topology in annotation on the VKSRegisterVolume CR so it survives restarts.
	if len(accessibleTopology) > 0 {
		topologyJSON, _ := json.Marshal(accessibleTopology)
		if instance.Annotations == nil {
			instance.Annotations = make(map[string]string)
		}
		instance.Annotations[common.AnnVolumeAccessibleTopology] = string(topologyJSON)
		if err := r.client.Update(ctx, instance); err != nil {
			log.Warnf("Failed to persist topology annotation on CR %s/%s: %v; continuing",
				instance.Namespace, instance.Name, err)
		}
	}

	if err := r.setPhase(ctx, instance,
		vksregistervolumev1alpha1.PhaseCreatingGuestPV); err != nil {
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	return reconcile.Result{Requeue: true}, nil
}

// reconcileCreatingGuestPV creates (or validates an existing) guest PersistentVolume.
func (r *ReconcileVKSRegisterVolume) reconcileCreatingGuestPV(ctx context.Context,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume,
	request reconcile.Request, timeout time.Duration) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	// Re-fetch the pre-created PVC to get the caller-chosen guestPVName and current spec.
	pvc, err := r.k8sclient.CoreV1().PersistentVolumeClaims(instance.Namespace).
		Get(ctx, instance.Spec.PVCName, metav1.GetOptions{})
	if err != nil {
		r.setError(ctx, instance,
			fmt.Sprintf("Failed to get PVC %s/%s: %v", instance.Namespace, instance.Spec.PVCName, err))
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	guestPVName := pvc.Spec.VolumeName
	supervisorPVCName := buildSupervisorCRName(r.tanzuKubernetesClusterUID, instance.Namespace, instance.Name)

	// Recover topology from CR annotation (stored in WaitingForSupervisorBinding).
	var accessibleTopology []map[string]string
	if raw, ok := instance.Annotations[common.AnnVolumeAccessibleTopology]; ok && raw != "" {
		json.Unmarshal([]byte(raw), &accessibleTopology) //nolint:errcheck
	}

	existingPV, err := r.k8sclient.CoreV1().PersistentVolumes().Get(ctx, guestPVName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		r.setError(ctx, instance, fmt.Sprintf("Failed to get PV %s: %v", guestPVName, err))
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	if apierrors.IsNotFound(err) {
		// Create the guest PV.
		pvSpec := buildGuestPVSpec(guestPVName, supervisorPVCName, pvc, instance, accessibleTopology)
		if _, err := r.k8sclient.CoreV1().PersistentVolumes().Create(ctx, pvSpec, metav1.CreateOptions{}); err != nil {
			r.setError(ctx, instance, fmt.Sprintf("Failed to create guest PV %s: %v", guestPVName, err))
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		log.Infof("Created guest PV %s with volumeHandle=%s claimRef=%s/%s",
			guestPVName, supervisorPVCName, instance.Namespace, instance.Spec.PVCName)
	} else {
		// Validate the existing PV's wiring.
		if existingPV.Spec.CSI == nil || existingPV.Spec.CSI.VolumeHandle != supervisorPVCName {
			r.setError(ctx, instance,
				fmt.Sprintf("Existing PV %s has wrong volumeHandle %q (expected %q)",
					guestPVName, existingPV.Spec.CSI.VolumeHandle, supervisorPVCName))
			r.setPhase(ctx, instance, vksregistervolumev1alpha1.PhaseFailed) //nolint:errcheck
			return reconcile.Result{}, nil
		}
		if existingPV.Spec.ClaimRef == nil ||
			existingPV.Spec.ClaimRef.Namespace != instance.Namespace ||
			existingPV.Spec.ClaimRef.Name != instance.Spec.PVCName {
			r.setError(ctx, instance,
				fmt.Sprintf("Existing PV %s claimRef points to wrong PVC", guestPVName))
			r.setPhase(ctx, instance, vksregistervolumev1alpha1.PhaseFailed) //nolint:errcheck
			return reconcile.Result{}, nil
		}
		log.Infof("Guest PV %s already exists and is correctly wired; advancing", guestPVName)
	}

	if err := r.setPhase(ctx, instance,
		vksregistervolumev1alpha1.PhaseWaitingForGuestPVCBound); err != nil {
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	return reconcile.Result{RequeueAfter: timeout}, nil
}

// reconcileWaitingForGuestPVCBound watches the guest PVC until it binds to the newly created PV.
func (r *ReconcileVKSRegisterVolume) reconcileWaitingForGuestPVCBound(ctx context.Context,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume,
	request reconcile.Request, timeout time.Duration) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	// Re-fetch PVC to get expected PV name.
	pvc, err := r.k8sclient.CoreV1().PersistentVolumeClaims(instance.Namespace).
		Get(ctx, instance.Spec.PVCName, metav1.GetOptions{})
	if err != nil {
		r.setError(ctx, instance,
			fmt.Sprintf("Failed to get PVC %s/%s: %v", instance.Namespace, instance.Spec.PVCName, err))
		return reconcile.Result{RequeueAfter: timeout}, nil
	}
	guestPVName := pvc.Spec.VolumeName

	// If already Bound, verify it's to the right PV.
	if pvc.Status.Phase == v1.ClaimBound {
		if pvc.Spec.VolumeName != guestPVName {
			r.setError(ctx, instance,
				fmt.Sprintf("PVC %s/%s is Bound to %q (expected %q)",
					instance.Namespace, instance.Spec.PVCName, pvc.Spec.VolumeName, guestPVName))
			r.setPhase(ctx, instance, vksregistervolumev1alpha1.PhaseFailed) //nolint:errcheck
			return reconcile.Result{}, nil
		}
		return r.markRegistered(ctx, instance, request)
	}

	// Watch for binding (blocks for up to pvcBindTimeout).
	isBound, err := isPVCBound(ctx, r.k8sclient, instance.Spec.PVCName, instance.Namespace, pvcBindTimeout)
	if err != nil {
		log.Infof("PVC %s/%s not bound yet: %v; requeuing", instance.Namespace, instance.Spec.PVCName, err)
	}
	if isBound {
		return r.markRegistered(ctx, instance, request)
	}

	r.setError(ctx, instance,
		fmt.Sprintf("Waiting for PVC %s/%s to become Bound", instance.Namespace, instance.Spec.PVCName))
	return reconcile.Result{RequeueAfter: timeout}, nil
}

// markRegistered sets Phase=Registered and Registered=true and clears the backoff entry.
func (r *ReconcileVKSRegisterVolume) markRegistered(ctx context.Context,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume,
	request reconcile.Request) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	orig := instance.DeepCopy()
	instance.Status.Phase = vksregistervolumev1alpha1.PhaseRegistered
	instance.Status.Registered = true
	instance.Status.Error = ""
	if err := patchStatus(ctx, r.client, orig, instance); err != nil {
		log.Errorf("Failed to mark VKSRegisterVolume %s/%s as Registered: %v",
			instance.Namespace, instance.Name, err)
		return reconcile.Result{RequeueAfter: time.Second}, nil
	}

	r.recorder.Event(instance, v1.EventTypeNormal, "VKSRegisterVolumeSucceeded",
		fmt.Sprintf("Successfully registered volume in namespace %s", instance.Namespace))

	backOffDurationMapMutex.Lock()
	delete(backOffDuration, apitypes.NamespacedName{Namespace: instance.Namespace, Name: instance.Name})
	backOffDurationMapMutex.Unlock()

	log.Infof("VKSRegisterVolume %s/%s is Registered", instance.Namespace, instance.Name)
	return reconcile.Result{}, nil
}

// reconcileDelete removes the Supervisor CnsRegisterVolume and then the guest finalizer.
// It does NOT delete the guest PV or PVC (those are user-owned).
func (r *ReconcileVKSRegisterVolume) reconcileDelete(ctx context.Context,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume,
	request reconcile.Request) (reconcile.Result, error) {
	log := logger.GetLogger(ctx)

	backOffDurationMapMutex.Lock()
	if _, exists := backOffDuration[request.NamespacedName]; !exists {
		backOffDuration[request.NamespacedName] = time.Second
	}
	timeout := backOffDuration[request.NamespacedName]
	backOffDurationMapMutex.Unlock()

	svCRName := buildSupervisorCRName(r.tanzuKubernetesClusterUID, instance.Namespace, instance.Name)

	svCR := &cnsregistervolumev1alpha1.CnsRegisterVolume{}
	err := r.supervisorCnsOperatorClient.Get(ctx,
		apitypes.NamespacedName{Namespace: r.supervisorNamespace, Name: svCRName}, svCR)
	if err == nil {
		if err := r.supervisorCnsOperatorClient.Delete(ctx, svCR); err != nil && !apierrors.IsNotFound(err) {
			log.Errorf("Failed to delete Supervisor CnsRegisterVolume %s/%s: %v",
				r.supervisorNamespace, svCRName, err)
			return reconcile.Result{RequeueAfter: timeout}, nil
		}
		log.Infof("Deleted Supervisor CnsRegisterVolume %s/%s", r.supervisorNamespace, svCRName)
	} else if !apierrors.IsNotFound(err) {
		log.Errorf("Failed to get Supervisor CnsRegisterVolume %s: %v", svCRName, err)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	// Remove the guest finalizer.
	controllerutil.RemoveFinalizer(instance, vksRegisterVolumeFinalizer)
	if err := r.client.Update(ctx, instance); err != nil {
		log.Errorf("Failed to remove finalizer from VKSRegisterVolume %s/%s: %v",
			instance.Namespace, instance.Name, err)
		return reconcile.Result{RequeueAfter: timeout}, nil
	}

	backOffDurationMapMutex.Lock()
	delete(backOffDuration, request.NamespacedName)
	backOffDurationMapMutex.Unlock()

	log.Infof("Finalizer removed from VKSRegisterVolume %s/%s", instance.Namespace, instance.Name)
	return reconcile.Result{}, nil
}

// setPhase patches Status.Phase on the CR (3 retries, 100ms sleep).
func (r *ReconcileVKSRegisterVolume) setPhase(ctx context.Context,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume,
	phase vksregistervolumev1alpha1.VKSRegisterVolumePhase) error {
	log := logger.GetLogger(ctx)
	orig := instance.DeepCopy()
	instance.Status.Phase = phase
	if err := patchStatus(ctx, r.client, orig, instance); err != nil {
		log.Errorf("Failed to set phase %s on VKSRegisterVolume %s/%s: %v",
			phase, instance.Namespace, instance.Name, err)
		return err
	}
	return nil
}

// setError patches Status.Error, advances backoff, and records a Warning event.
func (r *ReconcileVKSRegisterVolume) setError(ctx context.Context,
	instance *vksregistervolumev1alpha1.VKSRegisterVolume, errMsg string) {
	log := logger.GetLogger(ctx)
	orig := instance.DeepCopy()
	instance.Status.Error = errMsg
	if err := patchStatus(ctx, r.client, orig, instance); err != nil {
		log.Errorf("patchStatus failed for VKSRegisterVolume %s/%s: %v",
			instance.Namespace, instance.Name, err)
	}
	key := apitypes.NamespacedName{Namespace: instance.Namespace, Name: instance.Name}
	backOffDurationMapMutex.Lock()
	backOffDuration[key] = min(backOffDuration[key]*2, cnsoperatortypes.MaxBackOffDurationForReconciler)
	backOffDurationMapMutex.Unlock()
	r.recorder.Event(instance, v1.EventTypeWarning, "VKSRegisterVolumeFailed", errMsg)
}

// patchStatus does a JSON merge-patch of the Status subresource with up to 3 retries.
func patchStatus(ctx context.Context, c client.Client,
	oldObj, newObj *vksregistervolumev1alpha1.VKSRegisterVolume) error {
	log := logger.GetLogger(ctx)

	statusMap := map[string]interface{}{
		"registered": newObj.Status.Registered,
	}
	if newObj.Status.Phase != "" {
		statusMap["phase"] = string(newObj.Status.Phase)
	}
	if newObj.Status.Error != "" || (oldObj.Status.Error != "" && newObj.Status.Error == "") {
		statusMap["error"] = newObj.Status.Error
	}

	patchBytes, err := json.Marshal(map[string]interface{}{"status": statusMap})
	if err != nil {
		return fmt.Errorf("failed to marshal status patch: %v", err)
	}
	rawPatch := client.RawPatch(apitypes.MergePatchType, patchBytes)

	for attempt := 1; attempt <= 3; attempt++ {
		err := c.Status().Patch(ctx, oldObj, rawPatch)
		if err == nil {
			return nil
		}
		if attempt >= 3 {
			log.Errorf("Failed to patch VKSRegisterVolume status %s/%s after %d attempts: %v",
				oldObj.Namespace, oldObj.Name, attempt, err)
			return err
		}
		log.Warnf("Attempt %d: failed to patch VKSRegisterVolume status %s/%s: %v; retrying",
			attempt, oldObj.Namespace, oldObj.Name, err)
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}
