/*
Copyright 2019 The Kubernetes Authors.

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

package common

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/davecgh/go-spew/spew"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/mo"
	vim25types "github.com/vmware/govmomi/vim25/types"
	vsanfstypes "github.com/vmware/govmomi/vsan/vsanfs/types"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
	cnsvolume "sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/cns-lib/vsphere"
	"sigs.k8s.io/vsphere-csi-driver/pkg/common/utils"
	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"
)

// CreateBlockVolumeUtil is the helper function to create CNS block volume.
func CreateBlockVolumeUtil(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor, manager *Manager, spec *CreateVolumeSpec, sharedDatastores []*vsphere.DatastoreInfo) (*cnsvolume.CnsVolumeInfo, error) {
	log := logger.GetLogger(ctx)
	vc, err := GetVCenter(ctx, manager)
	if err != nil {
		log.Errorf("failed to get vCenter from Manager, err: %+v", err)
		return nil, err
	}
	if spec.ScParams.StoragePolicyName != "" {
		// Get Storage Policy ID from Storage Policy Name
		spec.StoragePolicyID, err = vc.GetStoragePolicyIDByName(ctx, spec.ScParams.StoragePolicyName)
		if err != nil {
			log.Errorf("Error occurred while getting Profile Id from Profile Name: %s, err: %+v", spec.ScParams.StoragePolicyName, err)
			return nil, err
		}
	}
	var datastores []vim25types.ManagedObjectReference
	if spec.ScParams.DatastoreURL == "" {
		// Check if datastore URL is specified by the storage pool parameter
		if spec.VsanDirectDatastoreURL != "" {
			// Create Datacenter object
			var dcList []*vsphere.Datacenter
			for _, dc := range vc.Config.DatacenterPaths {
				dcList = append(dcList,
					&vsphere.Datacenter{
						Datacenter: object.NewDatacenter(
							vc.Client.Client,
							vim25types.ManagedObjectReference{
								Type:  "Datacenter",
								Value: dc,
							}),
						VirtualCenterHost: vc.Config.Host,
					})
			}
			// Search the datastore from the URL in the datacenter list
			var datastoreObj *vsphere.Datastore
			for _, datacenter := range dcList {
				datastoreObj, err = datacenter.GetDatastoreByURL(ctx, spec.VsanDirectDatastoreURL)
				if err != nil {
					log.Warnf("Failed to find datastore with URL %q in datacenter %q from VC %q, Error: %+v",
						spec.VsanDirectDatastoreURL, datacenter.InventoryPath, vc.Config.Host, err)
					continue
				}
				log.Debugf("Successfully fetched the datastore %v from the URL: %v",
					datastoreObj.Reference(), spec.VsanDirectDatastoreURL)
				datastores = append(datastores, datastoreObj.Reference())
				break
			}
			if datastores == nil {
				errMsg := fmt.Sprintf("DatastoreURL: %s specified in the create volume spec is not found.",
					spec.VsanDirectDatastoreURL)
				return nil, errors.New(errMsg)
			}
		} else {
			//  If DatastoreURL is not specified in StorageClass, get all shared datastores
			datastores = getDatastoreMoRefs(sharedDatastores)
		}
	} else {
		// Check datastore specified in the StorageClass should be shared datastore across all nodes.

		// vc.GetDatacenters returns datacenters found on the VirtualCenter.
		// If no datacenters are mentioned in the VirtualCenterConfig during registration, all
		// Datacenters for the given VirtualCenter will be returned, else only the listed
		// Datacenters are returned.
		datacenters, err := vc.GetDatacenters(ctx)
		if err != nil {
			log.Errorf("failed to find datacenters from VC: %q, Error: %+v", vc.Config.Host, err)
			return nil, err
		}
		isSharedDatastoreURL := false
		var datastoreObj *vsphere.Datastore
		for _, datacenter := range datacenters {
			datastoreObj, err = datacenter.GetDatastoreByURL(ctx, spec.ScParams.DatastoreURL)
			if err != nil {
				log.Warnf("failed to find datastore with URL %q in datacenter %q from VC %q, Error: %+v", spec.ScParams.DatastoreURL, datacenter.InventoryPath, vc.Config.Host, err)
				continue
			}
			for _, sharedDatastore := range sharedDatastores {
				if sharedDatastore.Info.Url == spec.ScParams.DatastoreURL {
					isSharedDatastoreURL = true
					break
				}
			}
			if isSharedDatastoreURL {
				break
			}
		}
		if datastoreObj == nil {
			errMsg := fmt.Sprintf("DatastoreURL: %s specified in the storage class is not found.", spec.ScParams.DatastoreURL)
			log.Errorf(errMsg)
			return nil, errors.New(errMsg)
		}
		if isSharedDatastoreURL {
			datastores = append(datastores, datastoreObj.Reference())
		} else {
			errMsg := fmt.Sprintf("Datastore: %s specified in the storage class is not accessible to all nodes.", spec.ScParams.DatastoreURL)
			log.Errorf(errMsg)
			return nil, errors.New(errMsg)
		}
	}
	var containerClusterArray []cnstypes.CnsContainerCluster
	containerCluster := vsphere.GetContainerCluster(manager.CnsConfig.Global.ClusterID, manager.CnsConfig.VirtualCenter[vc.Config.Host].User, clusterFlavor, manager.CnsConfig.Global.ClusterDistribution)
	containerClusterArray = append(containerClusterArray, containerCluster)
	createSpec := &cnstypes.CnsVolumeCreateSpec{
		Name:       spec.Name,
		VolumeType: spec.VolumeType,
		Datastores: datastores,
		BackingObjectDetails: &cnstypes.CnsBlockBackingDetails{
			CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{
				CapacityInMb: spec.CapacityMB,
			},
		},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: containerClusterArray,
		},
	}
	if spec.StoragePolicyID != "" {
		profileSpec := &vim25types.VirtualMachineDefinedProfileSpec{
			ProfileId: spec.StoragePolicyID,
		}
		if spec.AffineToHost != "" {
			hostVsanUUID, err := getHostVsanUUID(ctx, spec.AffineToHost, vc)
			if err != nil {
				log.Errorf("failed to get the vSAN UUID for node: %s", spec.AffineToHost)
				return nil, err
			}
			param1 := vim25types.KeyValue{Key: VsanAffinityKey, Value: hostVsanUUID}
			param2 := vim25types.KeyValue{Key: VsanAffinityMandatory, Value: "1"}
			param3 := vim25types.KeyValue{Key: VsanMigrateForDecom, Value: "1"}
			profileSpec.ProfileParams = append(profileSpec.ProfileParams, param1, param2, param3)
		}
		createSpec.Profile = append(createSpec.Profile, profileSpec)
	}

	log.Debugf("vSphere CSI driver creating volume %s with create spec %+v", spec.Name, spew.Sdump(createSpec))
	volumeInfo, err := manager.VolumeManager.CreateVolume(ctx, createSpec)
	if err != nil {
		log.Errorf("failed to create disk %s with error %+v", spec.Name, err)
		return nil, err
	}
	return volumeInfo, nil
}

// CreateFileVolumeUtil is the helper function to create CNS file volume with datastores.
func CreateFileVolumeUtil(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor,
	manager *Manager, spec *CreateVolumeSpec, datastores []*vsphere.DatastoreInfo) (string, error) {
	log := logger.GetLogger(ctx)
	vc, err := GetVCenter(ctx, manager)
	if err != nil {
		log.Errorf("failed to get vCenter from Manager, err: %+v", err)
		return "", err
	}
	if spec.ScParams.StoragePolicyName != "" {
		// Get Storage Policy ID from Storage Policy Name
		spec.StoragePolicyID, err = vc.GetStoragePolicyIDByName(ctx, spec.ScParams.StoragePolicyName)
		if err != nil {
			log.Errorf("Error occurred while getting Profile Id from Profile Name: %q, err: %+v", spec.ScParams.StoragePolicyName, err)
			return "", err
		}
	}
	var datastoreMorefs []vim25types.ManagedObjectReference
	if spec.ScParams.DatastoreURL == "" {
		datastoreMorefs = getDatastoreMoRefs(datastores)
	} else {
		// If datastoreUrl is set in storage class, then check if part of input datastores.
		// If true, create the file volume on the datastoreUrl set in storage class.
		var isFound bool
		for _, dsInfo := range datastores {
			if spec.ScParams.DatastoreURL == dsInfo.Info.Url {
				isFound = true
				datastoreMorefs = append(datastoreMorefs, dsInfo.Reference())
				break
			}
		}
		if !isFound {
			msg := fmt.Sprintf("CSI user doesn't have permission on the datastore: %s specified in storage class",
				spec.ScParams.DatastoreURL)
			log.Error(msg)
			return "", errors.New(msg)
		}
	}

	// Retrieve net permissions from CnsConfig of manager and convert to required format
	netPerms := make([]vsanfstypes.VsanFileShareNetPermission, 0)
	for _, netPerm := range manager.CnsConfig.NetPermissions {
		netPerms = append(netPerms, vsanfstypes.VsanFileShareNetPermission{
			Ips:         netPerm.Ips,
			Permissions: netPerm.Permissions,
			AllowRoot:   !netPerm.RootSquash,
		})
	}

	var containerClusterArray []cnstypes.CnsContainerCluster
	containerCluster := vsphere.GetContainerCluster(manager.CnsConfig.Global.ClusterID, manager.CnsConfig.VirtualCenter[vc.Config.Host].User, clusterFlavor, manager.CnsConfig.Global.ClusterDistribution)
	containerClusterArray = append(containerClusterArray, containerCluster)
	createSpec := &cnstypes.CnsVolumeCreateSpec{
		Name:       spec.Name,
		VolumeType: spec.VolumeType,
		Datastores: datastoreMorefs,
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			CnsFileBackingDetails: cnstypes.CnsFileBackingDetails{
				CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{
					CapacityInMb: spec.CapacityMB,
				},
			},
		},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: containerClusterArray,
		},
		CreateSpec: &cnstypes.CnsVSANFileCreateSpec{
			SoftQuotaInMb: spec.CapacityMB,
			Permission:    netPerms,
		},
	}
	if spec.StoragePolicyID != "" {
		profileSpec := &vim25types.VirtualMachineDefinedProfileSpec{
			ProfileId: spec.StoragePolicyID,
		}
		createSpec.Profile = append(createSpec.Profile, profileSpec)
	}

	log.Debugf("vSphere CSI driver creating volume %q with create spec %+v", spec.Name, spew.Sdump(createSpec))
	volumeInfo, err := manager.VolumeManager.CreateVolume(ctx, createSpec)
	if err != nil {
		log.Errorf("failed to create file volume %q with error %+v", spec.Name, err)
		return "", err
	}
	return volumeInfo.VolumeID.Id, nil
}

// CreateFileVolumeUtilOld is the helper function to create CNS file volume with datastores from
// TargetvSANFileShareDatastoreURLs in vsphere conf.
func CreateFileVolumeUtilOld(ctx context.Context, clusterFlavor cnstypes.CnsClusterFlavor,
	manager *Manager, spec *CreateVolumeSpec) (string, error) {
	log := logger.GetLogger(ctx)
	vc, err := GetVCenter(ctx, manager)
	if err != nil {
		log.Errorf("failed to get vCenter from Manager, err: %+v", err)
		return "", err
	}
	if spec.ScParams.StoragePolicyName != "" {
		// Get Storage Policy ID from Storage Policy Name
		spec.StoragePolicyID, err = vc.GetStoragePolicyIDByName(ctx, spec.ScParams.StoragePolicyName)
		if err != nil {
			log.Errorf("Error occurred while getting Profile Id from Profile Name: %q, err: %+v", spec.ScParams.StoragePolicyName, err)
			return "", err
		}
	}
	var datastores []vim25types.ManagedObjectReference
	if spec.ScParams.DatastoreURL == "" {
		if len(manager.VcenterConfig.TargetvSANFileShareDatastoreURLs) == 0 {
			datacenters, err := vc.ListDatacenters(ctx)
			if err != nil {
				log.Errorf("failed to find datacenters from VC: %q, Error: %+v", vc.Config.Host, err)
				return "", err
			}
			// get all vSAN datastores from VC
			vsanDsURLToInfoMap, err := vc.GetVsanDatastores(ctx, datacenters)
			if err != nil {
				log.Errorf("failed to get vSAN datastores with error %+v", err)
				return "", err
			}
			var allvsanDatastoreUrls []string
			for dsURL := range vsanDsURLToInfoMap {
				allvsanDatastoreUrls = append(allvsanDatastoreUrls, dsURL)
			}
			fsEnabledMap, err := IsFileServiceEnabled(ctx, allvsanDatastoreUrls, vc, datacenters)
			if err != nil {
				log.Errorf("failed to get if file service is enabled on vsan datastores with error %+v", err)
				return "", err
			}
			for dsURL, dsInfo := range vsanDsURLToInfoMap {
				if val, ok := fsEnabledMap[dsURL]; ok {
					if val {
						datastores = append(datastores, dsInfo.Reference())
					}
				}
			}
			if len(datastores) == 0 {
				msg := "no file service enabled vsan datastore is present in the environment"
				log.Error(msg)
				return "", errors.New(msg)
			}
		} else {
			// If DatastoreURL is not specified in StorageClass, get all datastores from TargetvSANFileShareDatastoreURLs
			// in vcenter configuration.
			for _, TargetvSANFileShareDatastoreURL := range manager.VcenterConfig.TargetvSANFileShareDatastoreURLs {
				datastoreMoref, err := getDatastore(ctx, vc, TargetvSANFileShareDatastoreURL)
				if err != nil {
					log.Errorf("failed to get datastore %s. Error: %+v", TargetvSANFileShareDatastoreURL, err)
					return "", err
				}
				datastores = append(datastores, datastoreMoref)
			}
		}

	} else {
		// If datastoreUrl is set in storage class, then check the allowed list is empty.
		// If true, create the file volume on the datastoreUrl set in storage class.
		if len(manager.VcenterConfig.TargetvSANFileShareDatastoreURLs) == 0 {
			datastoreMoref, err := getDatastore(ctx, vc, spec.ScParams.DatastoreURL)
			if err != nil {
				log.Errorf("failed to get datastore %q. Error: %+v", spec.ScParams.DatastoreURL, err)
				return "", err
			}
			datastores = append(datastores, datastoreMoref)
		} else {
			// If datastoreUrl is set in storage class, then check if this is in the allowed list.
			found := false
			for _, targetVSANFSDsURL := range manager.VcenterConfig.TargetvSANFileShareDatastoreURLs {
				if spec.ScParams.DatastoreURL == targetVSANFSDsURL {
					found = true
					break
				}
			}
			if !found {
				msg := fmt.Sprintf("Datastore URL %q specified in storage class is not in the allowed list %+v",
					spec.ScParams.DatastoreURL, manager.VcenterConfig.TargetvSANFileShareDatastoreURLs)
				log.Error(msg)
				return "", errors.New(msg)
			}
			datastoreMoref, err := getDatastore(ctx, vc, spec.ScParams.DatastoreURL)
			if err != nil {
				log.Errorf("failed to get datastore %q. Error: %+v", spec.ScParams.DatastoreURL, err)
				return "", err
			}
			datastores = append(datastores, datastoreMoref)
		}
	}

	// Retrieve net permissions from CnsConfig of manager and convert to required format
	netPerms := make([]vsanfstypes.VsanFileShareNetPermission, 0)
	for _, netPerm := range manager.CnsConfig.NetPermissions {
		netPerms = append(netPerms, vsanfstypes.VsanFileShareNetPermission{
			Ips:         netPerm.Ips,
			Permissions: netPerm.Permissions,
			AllowRoot:   !netPerm.RootSquash,
		})
	}

	var containerClusterArray []cnstypes.CnsContainerCluster
	containerCluster := vsphere.GetContainerCluster(manager.CnsConfig.Global.ClusterID, manager.CnsConfig.VirtualCenter[vc.Config.Host].User, clusterFlavor, manager.CnsConfig.Global.ClusterDistribution)
	containerClusterArray = append(containerClusterArray, containerCluster)
	createSpec := &cnstypes.CnsVolumeCreateSpec{
		Name:       spec.Name,
		VolumeType: spec.VolumeType,
		Datastores: datastores,
		BackingObjectDetails: &cnstypes.CnsVsanFileShareBackingDetails{
			CnsFileBackingDetails: cnstypes.CnsFileBackingDetails{
				CnsBackingObjectDetails: cnstypes.CnsBackingObjectDetails{
					CapacityInMb: spec.CapacityMB,
				},
			},
		},
		Metadata: cnstypes.CnsVolumeMetadata{
			ContainerCluster:      containerCluster,
			ContainerClusterArray: containerClusterArray,
		},
		CreateSpec: &cnstypes.CnsVSANFileCreateSpec{
			SoftQuotaInMb: spec.CapacityMB,
			Permission:    netPerms,
		},
	}
	if spec.StoragePolicyID != "" {
		profileSpec := &vim25types.VirtualMachineDefinedProfileSpec{
			ProfileId: spec.StoragePolicyID,
		}
		createSpec.Profile = append(createSpec.Profile, profileSpec)
	}

	log.Debugf("vSphere CSI driver creating volume %q with create spec %+v", spec.Name, spew.Sdump(createSpec))
	volumeInfo, err := manager.VolumeManager.CreateVolume(ctx, createSpec)
	if err != nil {
		log.Errorf("failed to create file volume %q with error %+v", spec.Name, err)
		return "", err
	}
	return volumeInfo.VolumeID.Id, nil
}

// getHostVsanUUID returns the config.clusterInfo.nodeUuid of the ESX host's HostVsanSystem
func getHostVsanUUID(ctx context.Context, hostMoID string, vc *vsphere.VirtualCenter) (string, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("getHostVsanUUID for host moid: %v", hostMoID)

	// get host vsan UUID from the HostSystem
	hostMoRef := vim25types.ManagedObjectReference{Type: "HostSystem", Value: hostMoID}
	host := &vsphere.HostSystem{
		HostSystem: object.NewHostSystem(vc.Client.Client, hostMoRef),
	}
	nodeUUID, err := host.GetHostVsanNodeUUID(ctx)
	if err != nil {
		log.Errorf("Failed getting ESX host %v vsanUuid, err: %v", host, err)
		return "", err
	}
	log.Debugf("Got HostVsanUUID for host %s: %s", host.Reference(), nodeUUID)
	return nodeUUID, nil
}

// AttachVolumeUtil is the helper function to attach CNS volume to specified vm
func AttachVolumeUtil(ctx context.Context, manager *Manager,
	vm *vsphere.VirtualMachine,
	volumeID string, checkNVMeController bool) (string, error) {
	log := logger.GetLogger(ctx)
	log.Debugf("vSphere CSI driver is attaching volume: %q to vm: %q", volumeID, vm.String())
	diskUUID, err := manager.VolumeManager.AttachVolume(ctx, vm, volumeID, checkNVMeController)
	if err != nil {
		log.Errorf("failed to attach disk %q with VM: %q. err: %+v", volumeID, vm.String(), err)
		return "", err
	}
	log.Debugf("Successfully attached disk %s to VM %v. Disk UUID is %s", volumeID, vm, diskUUID)
	return diskUUID, nil
}

// DetachVolumeUtil is the helper function to detach CNS volume from specified vm
func DetachVolumeUtil(ctx context.Context, manager *Manager,
	vm *vsphere.VirtualMachine,
	volumeID string) error {
	log := logger.GetLogger(ctx)
	log.Debugf("vSphere CSI driver is detaching volume: %s from node vm: %s", volumeID, vm.InventoryPath)
	err := manager.VolumeManager.DetachVolume(ctx, vm, volumeID)
	if err != nil {
		log.Errorf("failed to detach disk %s with err %+v", volumeID, err)
		return err
	}
	log.Debugf("Successfully detached disk %s from VM %v.", volumeID, vm)
	return nil
}

// DeleteVolumeUtil is the helper function to delete CNS volume for given volumeId
func DeleteVolumeUtil(ctx context.Context, volManager cnsvolume.Manager, volumeID string, deleteDisk bool) error {
	log := logger.GetLogger(ctx)
	var err error
	log.Debugf("vSphere CSI driver is deleting volume: %s with deleteDisk flag: %t", volumeID, deleteDisk)
	err = volManager.DeleteVolume(ctx, volumeID, deleteDisk)
	if err != nil {
		log.Errorf("failed to delete disk %s, deleteDisk flag: %t with error %+v", volumeID, deleteDisk, err)
		return err
	}
	log.Debugf("Successfully deleted disk for volumeid: %s, deleteDisk flag: %t", volumeID, deleteDisk)
	return nil
}

// ExpandVolumeUtil is the helper function to extend CNS volume for given volumeId
func ExpandVolumeUtil(ctx context.Context, manager *Manager, volumeID string, capacityInMb int64, useAsyncQueryVolume,
	isIdempotencyHandlingEnabled bool) error {
	var err error
	log := logger.GetLogger(ctx)
	log.Debugf("vSphere CSI driver expanding volume %q to new size %d Mb.", volumeID, capacityInMb)

	if isIdempotencyHandlingEnabled {
		// Avoid querying volume when idempotency handling is enabled.
		err = manager.VolumeManager.ExpandVolume(ctx, volumeID, capacityInMb)
		if err != nil {
			log.Errorf("failed to expand volume %q with error %+v", volumeID, err)
			return err
		}
		log.Infof("Successfully expanded volume for volumeid %q to new size %d Mb.", volumeID, capacityInMb)
		return nil
	} else {
		expansionRequired, err := isExpansionRequired(ctx, volumeID, capacityInMb, manager, useAsyncQueryVolume)
		if err != nil {
			return err
		}
		if expansionRequired {
			log.Infof("Requested size %d Mb is greater than current size for volumeID: %q. Need volume expansion.",
				capacityInMb, volumeID)
			err = manager.VolumeManager.ExpandVolume(ctx, volumeID, capacityInMb)
			if err != nil {
				log.Errorf("failed to expand volume %q with error %+v", volumeID, err)
				return err
			}
			log.Infof("Successfully expanded volume for volumeid %q to new size %d Mb.", volumeID, capacityInMb)

		} else {
			log.Infof("Requested volume size is equal to current size %d Mb. Expansion not required.", capacityInMb)
		}
		return err

	}
}

// QueryVolumeByID is the helper function to query volume by volumeID
func QueryVolumeByID(ctx context.Context, volManager cnsvolume.Manager, volumeID string) (*cnstypes.CnsVolume, error) {
	log := logger.GetLogger(ctx)
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: []cnstypes.CnsVolumeId{{Id: volumeID}},
	}
	queryResult, err := volManager.QueryVolume(ctx, queryFilter)
	if err != nil {
		msg := fmt.Sprintf("QueryVolume failed for volumeID: %s with error %+v", volumeID, err)
		log.Error(msg)
		return nil, err
	}
	if len(queryResult.Volumes) == 0 {
		msg := fmt.Sprintf("volumeID %q not found in QueryVolume", volumeID)
		log.Error(msg)
		return nil, ErrNotFound
	}
	return &queryResult.Volumes[0], nil
}

func ListSnapshotsUtil(ctx context.Context, volManager cnsvolume.Manager, volumeID string, snapshotID string,
	token string, maxEntries int64) ([]*csi.Snapshot, string, error) {
	log := logger.GetLogger(ctx)
	if snapshotID != "" {
		// Retrieve specific snapshot information.
		// The snapshotID is of format: <volume-id>+<snapshot-id>
		volID, snapID, err := ParseCSISnapshotID(snapshotID)
		if err != nil {
			log.Errorf("Unable to determine the volume-id and snapshot-id")
			return nil, "", err
		}
		snapshots, err := QueryVolumeSnapshot(ctx, volManager, volID, snapID, maxEntries)
		if err != nil {
			// Failed to retrieve information of snapshot.
			log.Errorf("failed to retrieve snapshot information for volume-id: %s snapshot-id: %s err: %+v", volID, snapID, err)
			return nil, "", err
		}
		return snapshots, "", nil
	} else if volumeID != "" {
		// Retrieve all snapshots for a volume-id
		// Check if the volume-id specified is of Block type.
		queryFilter := cnstypes.CnsQueryFilter{
			VolumeIds: []cnstypes.CnsVolumeId{{Id: volumeID}},
		}
		// Validate that the volume-id is of block volume type.
		queryResult, err := volManager.QueryVolume(ctx, queryFilter)
		if err != nil {
			return nil, "", logger.LogNewErrorCodef(log, codes.Internal,
				"queryVolume failed with err=%+v", err)
		}

		if len(queryResult.Volumes) == 0 {
			return nil, "", logger.LogNewErrorCodef(log, codes.Internal,
				"volumeID %q not found in QueryVolume", volumeID)
		}
		if queryResult.Volumes[0].VolumeType == FileVolumeType {
			return nil, "", logger.LogNewErrorCodef(log, codes.Unimplemented,
				"ListSnapshot for file volume: %q not supported", volumeID)
		}
		return QueryVolumeSnapshotsByVolumeID(ctx, volManager, volumeID, maxEntries)
	} else {
		// Retrieve all snapshots in the inventory
		return QueryAllVolumeSnapshots(ctx, volManager, token, maxEntries)
	}
}

func QueryVolumeSnapshot(ctx context.Context, volManager cnsvolume.Manager, volID string, snapID string,
	maxEntries int64) ([]*csi.Snapshot, error) {
	log := logger.GetLogger(ctx)
	var snapshots []*csi.Snapshot
	// Retrieve the individual snapshot information using CNS QuerySnapshots
	querySpec := cnstypes.CnsSnapshotQuerySpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volID,
		},
		SnapshotId: &cnstypes.CnsSnapshotId{
			Id: snapID,
		},
	}
	querySnapFilter := cnstypes.CnsSnapshotQueryFilter{
		SnapshotQuerySpecs: []cnstypes.CnsSnapshotQuerySpec{querySpec},
		Cursor: &cnstypes.CnsCursor{
			Offset: 0,
			Limit:  maxEntries,
		},
	}
	queryResultEntries, _, err := utils.QuerySnapshotsUtil(ctx, volManager, querySnapFilter, QuerySnapshotLimit)
	if err != nil {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"failed retrieve snapshot info for volume-id: %s snapshot-id: %s err: %+v", volID, snapID, err)
	}
	if len(queryResultEntries) == 0 {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"no snapshot infos retrieved for volume-id: %s snapshot-id: %s from QuerySnapshot", volID, snapID)
	}
	snapshotResult := queryResultEntries[0]
	if snapshotResult.Error != nil {
		fault := snapshotResult.Error.Fault
		if _, ok := fault.(cnstypes.CnsSnapshotNotFoundFault); ok {
			return nil, logger.LogNewErrorCodef(log, codes.Internal,
				"snapshot-id: %s not found for volume-id: %s during QuerySnapshots, err: %+v", snapID, volID, fault)
		}
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"unexpected error received when retrieving snapshot info for volume-id: %s snapshot-id: %s err: %+v",
			volID, snapID, snapshotResult.Error.Fault)
	}
	//Retrieve the volume size to be returned as snapshot size.
	// TODO: Retrieve Snapshot size directly from CnsQuerySnapshot once supported.
	// Query capacity in MB, volume type and datastore url for block volume snapshot
	volumeIds := []cnstypes.CnsVolumeId{{Id: volID}}
	cnsVolumeDetailsMap, err := utils.QueryVolumeDetailsUtil(ctx, volManager, volumeIds)
	if err != nil {
		return nil, err
	}
	if _, ok := cnsVolumeDetailsMap[volID]; !ok {
		return nil, logger.LogNewErrorCodef(log, codes.Internal,
			"cns query volume did not retrieve the volume: %s", volID)
	}
	snapshotSizeInMB := cnsVolumeDetailsMap[volID].SizeInMB
	snapshotsInfo := snapshotResult.Snapshot
	snapshotCreateTimeInProto := timestamppb.New(snapshotsInfo.CreateTime)
	csiSnapshotInfo := &csi.Snapshot{
		SnapshotId:     volID + VSphereCSISnapshotIdDelimiter + snapID,
		SourceVolumeId: volID,
		CreationTime:   snapshotCreateTimeInProto,
		SizeBytes:      snapshotSizeInMB * MbInBytes,
		ReadyToUse:     true,
	}
	snapshots = append(snapshots, csiSnapshotInfo)
	return snapshots, nil
}

func QueryAllVolumeSnapshots(ctx context.Context, volManager cnsvolume.Manager, token string, maxEntries int64) (
	[]*csi.Snapshot, string, error) {
	log := logger.GetLogger(ctx)
	var csiSnapshots []*csi.Snapshot
	var offset int64
	var err error
	limit := QuerySnapshotLimit
	if maxEntries <= QuerySnapshotLimit {
		// If the max results that can be handled by callers is lower than the default limit, then
		// serve the results in a single call.
		limit = maxEntries
	}
	if token != "" {
		offset, err = strconv.ParseInt(token, 10, 64)
		if err != nil {
			log.Errorf("failed to parse the token: %s err: %v", token, err)
			return nil, "", err
		}
	}
	queryFilter := cnstypes.CnsSnapshotQueryFilter{
		Cursor: &cnstypes.CnsCursor{
			Offset: offset,
			Limit:  limit,
		},
	}
	queryResultEntries, nextToken, err := utils.QuerySnapshotsUtil(ctx, volManager, queryFilter, maxEntries)
	if err != nil {
		log.Errorf("failed to retrieve all the volume snapshots in inventory err: %+v", err)
		return nil, "", err
	}
	//populate list of volume-ids to retrieve the volume size.
	var volumeIds []cnstypes.CnsVolumeId
	for _, queryResult := range queryResultEntries {
		if queryResult.Error != nil {
			return nil, "", logger.LogNewErrorCodef(log, codes.Internal,
				"faults are not expected when invoking QuerySnapshots without volume-id and snapshot-id, fault: %+v",
				queryResult.Error.Fault)
		}
		volumeIds = append(volumeIds, queryResult.Snapshot.VolumeId)
	}
	// TODO: Retrieve Snapshot size directly from CnsQuerySnapshot once supported.
	cnsVolumeDetailsMap, err := utils.QueryVolumeDetailsUtil(ctx, volManager, volumeIds)
	if err != nil {
		log.Errorf("failed to retrieve volume details for volume-ids: %v, err: %+v", volumeIds, err)
		return nil, "", err
	}
	for _, queryResult := range queryResultEntries {
		snapshotCreateTimeInProto := timestamppb.New(queryResult.Snapshot.CreateTime)
		csiSnapshotId := queryResult.Snapshot.VolumeId.Id + VSphereCSISnapshotIdDelimiter + queryResult.Snapshot.SnapshotId.Id
		if _, ok := cnsVolumeDetailsMap[queryResult.Snapshot.VolumeId.Id]; !ok {
			return nil, "", logger.LogNewErrorCodef(log, codes.Internal,
				"cns query volume did not return the volume: %s", queryResult.Snapshot.VolumeId.Id)
		}
		csiSnapshotSize := cnsVolumeDetailsMap[queryResult.Snapshot.VolumeId.Id].SizeInMB * MbInBytes
		csiSnapshotInfo := &csi.Snapshot{
			SnapshotId:     csiSnapshotId,
			SourceVolumeId: queryResult.Snapshot.VolumeId.Id,
			CreationTime:   snapshotCreateTimeInProto,
			SizeBytes:      csiSnapshotSize,
			ReadyToUse:     true,
		}
		csiSnapshots = append(csiSnapshots, csiSnapshotInfo)
	}
	return csiSnapshots, nextToken, nil
}

func QueryVolumeSnapshotsByVolumeID(ctx context.Context, volManager cnsvolume.Manager, volumeID string,
	maxEntries int64) ([]*csi.Snapshot, string, error) {
	log := logger.GetLogger(ctx)
	var csiSnapshots []*csi.Snapshot
	limit := QuerySnapshotLimit
	if maxEntries <= QuerySnapshotLimit {
		limit = maxEntries
	}
	querySpec := cnstypes.CnsSnapshotQuerySpec{
		VolumeId: cnstypes.CnsVolumeId{
			Id: volumeID,
		},
	}
	queryFilter := cnstypes.CnsSnapshotQueryFilter{
		SnapshotQuerySpecs: []cnstypes.CnsSnapshotQuerySpec{querySpec},
		Cursor: &cnstypes.CnsCursor{
			Offset: 0,
			Limit:  limit,
		},
	}
	queryResultEntries, nextToken, err := utils.QuerySnapshotsUtil(ctx, volManager, queryFilter, maxEntries)
	if err != nil {
		log.Errorf("failed to retrieve csiSnapshots for volume-id: %s err: %+v", volumeID, err)
		return nil, "", err
	}
	// Retrieve the volume size as an approximation for snapshot size.
	// TODO: Retrieve Snapshot size directly from CnsQuerySnapshot once supported.
	// Query capacity in MB and datastore url for block volume snapshot
	volumeIds := []cnstypes.CnsVolumeId{{Id: volumeID}}
	cnsVolumeDetailsMap, err := utils.QueryVolumeDetailsUtil(ctx, volManager, volumeIds)
	if err != nil {
		log.Errorf("failed to retrieve the volume: %s details. err: %+v.", volumeID, err)
		return nil, "", err
	}
	for _, queryResult := range queryResultEntries {
		// check if the queryResult has fault, if so, the specific result can be ignored.
		if queryResult.Error != nil {
			// Currently, CnsVolumeNotFoundFault is the only possible fault when QuerySnapshots is
			// invoked with only volume-id
			fault := queryResult.Error.Fault
			if faultInfo, ok := fault.(cnstypes.CnsVolumeNotFoundFault); ok {
				faultVolumeId := faultInfo.VolumeId.Id
				log.Warnf("volume %s was not found during QuerySnapshots, ignore volume..", faultVolumeId)
				continue
			} else {
				return csiSnapshots, nextToken, logger.LogNewErrorCodef(log, codes.Internal,
					"unexpected fault %+v received in QuerySnapshots result: %+v for volume: %s", fault, queryResult, volumeID)
			}
		}
		snapshotCreateTimeInProto := timestamppb.New(queryResult.Snapshot.CreateTime)
		csiSnapshotId := queryResult.Snapshot.VolumeId.Id + VSphereCSISnapshotIdDelimiter + queryResult.Snapshot.SnapshotId.Id
		if _, ok := cnsVolumeDetailsMap[queryResult.Snapshot.VolumeId.Id]; !ok {
			return nil, "", logger.LogNewErrorCodef(log, codes.Internal,
				"cns query volume did not return the volume: %s", queryResult.Snapshot.VolumeId.Id)
		}
		csiSnapshotSize := cnsVolumeDetailsMap[queryResult.Snapshot.VolumeId.Id].SizeInMB * MbInBytes
		csiSnapshotInfo := &csi.Snapshot{
			SnapshotId:     csiSnapshotId,
			SourceVolumeId: queryResult.Snapshot.VolumeId.Id,
			CreationTime:   snapshotCreateTimeInProto,
			SizeBytes:      csiSnapshotSize,
			ReadyToUse:     true,
		}
		csiSnapshots = append(csiSnapshots, csiSnapshotInfo)
	}
	return csiSnapshots, nextToken, nil
}

// Helper function to get Datastore object for given datastoreURL in the given
// virtual center.
func getDatastoreObj(ctx context.Context, vc *vsphere.VirtualCenter,
	datastoreURL string) (*vsphere.Datastore, error) {
	log := logger.GetLogger(ctx)
	datacenters, err := vc.GetDatacenters(ctx)
	if err != nil {
		return nil, err
	}
	var datastoreObj *vsphere.Datastore
	for _, datacenter := range datacenters {
		datastoreObj, err = datacenter.GetDatastoreByURL(ctx, datastoreURL)
		if err != nil {
			log.Warnf("failed to find datastore with URL %q in datacenter %q from VC %q. Error: %+v",
				datastoreURL, datacenter.InventoryPath, vc.Config.Host, err)
		} else {
			return datastoreObj, nil
		}
	}

	return nil, logger.LogNewErrorf(log,
		"Unable to find datastore for datastore URL %s in VC %+v", datastoreURL, vc)
}

// Helper function to get DatastoreMoRefs
func getDatastoreMoRefs(datastores []*vsphere.DatastoreInfo) []vim25types.ManagedObjectReference {
	var datastoreMoRefs []vim25types.ManagedObjectReference
	for _, datastore := range datastores {
		datastoreMoRefs = append(datastoreMoRefs, datastore.Reference())
	}
	return datastoreMoRefs
}

// Helper function to get DatastoreMoRef for given datastoreURL in the given virtual center.
func getDatastore(ctx context.Context, vc *vsphere.VirtualCenter, datastoreURL string) (vim25types.ManagedObjectReference, error) {
	log := logger.GetLogger(ctx)
	datacenters, err := vc.GetDatacenters(ctx)
	if err != nil {
		return vim25types.ManagedObjectReference{}, err
	}
	var datastoreObj *vsphere.Datastore
	for _, datacenter := range datacenters {
		datastoreObj, err = datacenter.GetDatastoreByURL(ctx, datastoreURL)
		if err != nil {
			log.Warnf("failed to find datastore with URL %q in datacenter %q from VC %q, Error: %+v",
				datastoreURL, datacenter.InventoryPath, vc.Config.Host, err)
		} else {
			return datastoreObj.Reference(), nil
		}
	}

	msg := fmt.Sprintf("Unable to find datastore for datastore URL %s in VC %+v", datastoreURL, vc)
	return vim25types.ManagedObjectReference{}, errors.New(msg)
}

// isExpansionRequired verifies if the requested size to expand a volume is greater than the current size
func isExpansionRequired(ctx context.Context, volumeID string, requestedSize int64, manager *Manager, useAsyncQueryVolume bool) (bool, error) {
	log := logger.GetLogger(ctx)
	volumeIds := []cnstypes.CnsVolumeId{{Id: volumeID}}
	queryFilter := cnstypes.CnsQueryFilter{
		VolumeIds: volumeIds,
	}
	// Select only the backing object details.
	querySelection := cnstypes.CnsQuerySelection{
		Names: []string{
			string(cnstypes.QuerySelectionNameTypeBackingObjectDetails),
		},
	}
	// Query only the backing object details.
	queryResult, err := utils.QueryAllVolumeUtil(ctx, manager.VolumeManager, queryFilter, querySelection, useAsyncQueryVolume)
	if err != nil {
		log.Errorf("QueryVolume failed with err=%+v", err.Error())
		return false, err
	}

	var currentSize int64
	if len(queryResult.Volumes) > 0 {
		currentSize = queryResult.Volumes[0].BackingObjectDetails.GetCnsBackingObjectDetails().CapacityInMb
	} else {
		msg := fmt.Sprintf("failed to find volume by querying volumeID: %q", volumeID)
		log.Error(msg)
		return false, err
	}

	return currentSize < requestedSize, nil
}

// CreateSnapshotUtil is the helper function to create CNS snapshot for given volumeId
//
// The desc parameter denotes the snapshot description required by CNS CreateSnapshot API. This parameter is expected
// to be filled with the CSI CreateSnapshotRequest Name, which is generated by the CSI snapshotter sidecar.
//
// The returned string is a combination of CNS VolumeID and CNS SnapshotID concatenated by the "+" sign.
// The returned *time.Time denotes the creation time of snapshot from the storage system, i.e., CNS.
func CreateSnapshotUtil(ctx context.Context, manager *Manager, volumeID string, snapshotName string) (string,
	*time.Time, error) {
	log := logger.GetLogger(ctx)

	log.Debugf("vSphere CSI driver is creating snapshot with description, %q, on volume: %q", snapshotName, volumeID)
	cnsSnapshotInfo, err := manager.VolumeManager.CreateSnapshot(ctx, volumeID, snapshotName)
	if err != nil {
		log.Errorf("failed to create snapshot on volume %q with description %q with error %+v",
			volumeID, snapshotName, err)
		return "", nil, err
	}
	log.Debugf("Successfully created snapshot %q with description, %q, on volume: %q at timestamp %q",
		cnsSnapshotInfo.SnapshotID, snapshotName, volumeID, cnsSnapshotInfo.SnapshotCreationTimestamp)

	csiSnapshotID := volumeID + VSphereCSISnapshotIdDelimiter + cnsSnapshotInfo.SnapshotID

	return csiSnapshotID, &cnsSnapshotInfo.SnapshotCreationTimestamp, nil
}

// DeleteSnapshotUtil is the helper function to delete CNS snapshot for given snapshotId
func DeleteSnapshotUtil(ctx context.Context, manager *Manager, csiSnapshotID string) error {
	log := logger.GetLogger(ctx)

	cnsVolumeID, cnsSnapshotID, err := ParseCSISnapshotID(csiSnapshotID)
	if err != nil {
		return err
	}

	log.Debugf("vSphere CSI driver is deleting snapshot %q on volume: %q", cnsSnapshotID, cnsVolumeID)
	err = manager.VolumeManager.DeleteSnapshot(ctx, cnsVolumeID, cnsSnapshotID)
	if err != nil {
		return logger.LogNewErrorf(log, "failed to delete snapshot %q on volume %q with error %+v",
			cnsSnapshotID, cnsVolumeID, err)
	}
	log.Debugf("Successfully deleted snapshot %q on volume %q", cnsSnapshotID, cnsVolumeID)

	return nil
}

// GetNodeVMsWithAccessToDatastore finds out NodeVMs which have access to the given
// datastore URL by using the moref approach.
func GetNodeVMsWithAccessToDatastore(ctx context.Context, vc *vsphere.VirtualCenter, dsURL string,
	allNodeVMs []*vsphere.VirtualMachine) ([]*object.VirtualMachine, error) {

	log := logger.GetLogger(ctx)
	var accessibleNodes []*object.VirtualMachine
	totalNodes := len(allNodeVMs)

	// Create map of allNodeVM refs to nil value for easy retrieval.
	nodeMap := make(map[vim25types.ManagedObjectReference]struct{})
	for _, vmObj := range allNodeVMs {
		nodeMap[vmObj.Reference()] = struct{}{}
	}

	// Get datastore object.
	dsObj, err := getDatastoreObj(ctx, vc, dsURL)
	if err != nil {
		return nil, logger.LogNewErrorf(log, "failed to retrieve datastore object using datastore "+
			"URL %q. Error: %+v", dsURL, err)
	}
	// Get datastore host mounts.
	var ds mo.Datastore
	err = dsObj.Properties(ctx, dsObj.Reference(), []string{"host"}, &ds)
	if err != nil {
		return nil, logger.LogNewErrorf(log, "failed to get host mounts from datastore %q. Error: %+v",
			dsURL, err)
	}

	// For each host mount, get the list of VMs.
	for _, host := range ds.Host {
		hostObj := &vsphere.HostSystem{
			HostSystem: object.NewHostSystem(vc.Client.Client, host.Key),
		}
		var hs mo.HostSystem
		err = hostObj.Properties(ctx, hostObj.Reference(), []string{"vm"}, &hs)
		if err != nil {
			return nil, logger.LogNewErrorf(log, "failed to retrieve virtual machines from host %q. Error: %+v",
				hostObj.String(), err)
		}
		// For each VM on the host, check if the VM is a NodeVM participating in the k8s cluster.
		for _, vm := range hs.Vm {
			if _, exists := nodeMap[vm]; exists {
				accessibleNodes = append(accessibleNodes, object.NewVirtualMachine(vc.Client.Client, vm))
			}
			// Check if the length of accessible nodes is equal to total count of
			// NodeVMs in this cluster. If yes, we can stop searching for more VMs.
			if len(accessibleNodes) == totalNodes {
				log.Infof("Nodes that have access to datastore %q are %+v", dsURL, accessibleNodes)
				return accessibleNodes, nil
			}
		}

	}
	log.Infof("Nodes that have access to datastore %q are %+v", dsURL, accessibleNodes)
	return accessibleNodes, nil
}
