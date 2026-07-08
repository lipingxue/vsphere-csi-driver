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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VKSRegisterVolumePhase describes the current phase of a VKSRegisterVolume CR.
type VKSRegisterVolumePhase string

const (
	// PhasePending is the initial phase after CR creation (before finalizer is added).
	PhasePending VKSRegisterVolumePhase = "Pending"
	// PhaseCreatingSupervisorCR is set while creating the Supervisor CnsRegisterVolume CR.
	PhaseCreatingSupervisorCR VKSRegisterVolumePhase = "CreatingSupervisorCR"
	// PhaseWaitingForSupervisorRegistration is set while waiting for the Supervisor
	// CnsRegisterVolume to complete volume registration.
	PhaseWaitingForSupervisorRegistration VKSRegisterVolumePhase = "WaitingForSupervisorRegistration"
	// PhaseWaitingForSupervisorBinding is set while waiting for the Supervisor PVC to become Bound.
	PhaseWaitingForSupervisorBinding VKSRegisterVolumePhase = "WaitingForSupervisorBinding"
	// PhaseCreatingGuestPV is set while creating the guest PersistentVolume.
	PhaseCreatingGuestPV VKSRegisterVolumePhase = "CreatingGuestPV"
	// PhaseWaitingForGuestPVCBound is set while waiting for the pre-created guest PVC to bind to the PV.
	PhaseWaitingForGuestPVCBound VKSRegisterVolumePhase = "WaitingForGuestPVCBound"
	// PhaseRegistered is the terminal success phase.
	PhaseRegistered VKSRegisterVolumePhase = "Registered"
	// PhaseFailed is the terminal failure phase.
	PhaseFailed VKSRegisterVolumePhase = "Failed"
)

// VKSRegisterVolumeSpec defines the desired state of VKSRegisterVolume.
// +k8s:openapi-gen=true
type VKSRegisterVolumeSpec struct {
	// PVCName is the name of the pre-created guest PVC (in the CR's namespace) that
	// SnapService created with spec.volumeName set to the desired future PV name.
	// The PVC must be in Pending state (not yet Bound).
	PVCName string `json:"pvcName"`

	// VolumeID is the FCD UUID of the existing vSphere volume to register.
	VolumeID string `json:"volumeID"`

	// DiskURLPath is the full Option-A URL pointing to the VMDK backing the volume.
	// Format: https://<vc_ip>/folder/<vm_vmdk_path>?dcPath=<datacenterName>&dsName=<datastoreName>
	// Example: https://10.192.255.221/folder/path/to/disk.vmdk?dcPath=Datacenter-1&dsName=vsanDatastore
	// This URL is supplied by SnapService; the guest operator does not construct it.
	DiskURLPath string `json:"diskURLPath"`
}

// VKSRegisterVolumeStatus defines the observed state of VKSRegisterVolume.
// +k8s:openapi-gen=true
type VKSRegisterVolumeStatus struct {
	// Phase describes the current phase of the registration workflow.
	// +optional
	Phase VKSRegisterVolumePhase `json:"phase,omitempty"`

	// Registered indicates whether the volume registration completed successfully.
	Registered bool `json:"registered"`

	// Error is the last error encountered during registration, if any.
	// +optional
	Error string `json:"error,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VKSRegisterVolume is the Schema for the vksregistervolumes API.
// It triggers registration of an existing vSphere volume (FCD) into a VKS guest cluster,
// resulting in a bound guest PVC backed by a Supervisor-registered PV.
// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Registered",type=boolean,JSONPath=`.status.registered`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type VKSRegisterVolume struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VKSRegisterVolumeSpec   `json:"spec,omitempty"`
	Status VKSRegisterVolumeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VKSRegisterVolumeList contains a list of VKSRegisterVolume.
type VKSRegisterVolumeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VKSRegisterVolume `json:"items"`
}
