// Package v1alpha1 contains API Schema definitions for the pipeline v1alpha1 API group.
// This package is imported by both the runner (controllers) and the hub
// (to construct PipelineRun/TaskRun/Rollout objects for dispatch), so it
// must stay free of any runner- or hub-specific business logic.
//
// +kubebuilder:object:generate=true
// +groupName=pipeline.sdp.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is group version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "pipeline.sdp.io", Version: "v1alpha1"}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
