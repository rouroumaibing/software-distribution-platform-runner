package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TaskRunPhase represents the lifecycle phase of a single TaskRun.
type TaskRunPhase string

const (
	TaskRunPending   TaskRunPhase = "Pending"
	TaskRunRunning   TaskRunPhase = "Running"
	TaskRunSucceeded TaskRunPhase = "Succeeded"
	TaskRunFailed    TaskRunPhase = "Failed"
	// TaskRunSkipped is used when an upstream dependency failed, so this
	// task never becomes runnable.
	TaskRunSkipped TaskRunPhase = "Skipped"
)

// TaskRunSpec defines the desired state of a TaskRun.
// A TaskRun is created by the PipelineRun controller for each DAG node once
// all of its DependsOn entries have reached Succeeded. The TaskRun
// controller is the only thing that actually talks to the Job API.
type TaskRunSpec struct {
	// PipelineRunRef is the owning PipelineRun's name, also set as an
	// ownerReference for garbage collection.
	PipelineRunRef string `json:"pipelineRunRef"`

	TaskName string `json:"taskName"`

	// +kubebuilder:validation:Enum=Build;Release;Approval
	Type PipelineTaskType `json:"type"`

	Image string `json:"image,omitempty"`

	// ScriptPath/ScriptArgs mirror PipelineTaskSpec; see there for details.
	// The runner's Job builder turns these into an init container (git
	// checkout) plus a main container running `sh {ScriptPath} {ScriptArgs...}`.
	ScriptPath string   `json:"scriptPath,omitempty"`
	ScriptArgs []string `json:"scriptArgs,omitempty"`

	Command []string `json:"command,omitempty"`
	Args    []string `json:"args,omitempty"`

	Repo *RepoSource `json:"repo,omitempty"`

	Produces []string `json:"produces,omitempty"`
	Consumes []string `json:"consumes,omitempty"`

	Env       []corev1.EnvVar             `json:"env,omitempty"`
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	Namespace          string `json:"namespace"`
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	RetryPolicy *RetryPolicy `json:"retryPolicy,omitempty"`

	// TimeoutSeconds bounds how long the underlying Job may run.
	TimeoutSeconds int64 `json:"timeoutSeconds,omitempty"`

	// ApprovalConfig is populated when Type == Approval; the controller
	// pauses this TaskRun instead of creating a Job and waits for
	// Status.Approval to be resolved.
	ApprovalConfig *ApprovalConfig `json:"approvalConfig,omitempty"`

	// RolloutSpec is populated when Type == Release and a canary is
	// requested; the controller creates (or updates) a Rollout resource and
	// watches it instead of a plain Job.
	RolloutSpec *RolloutSpec `json:"rolloutSpec,omitempty"`

	// ReleaseSpec is populated when Type == Release; describes the chart or
	// manifest to apply, plus values injected from parameter management.
	ReleaseSpec *ReleaseSpec `json:"releaseSpec,omitempty"`
}

// TaskRunStatus defines the observed state of a TaskRun.
type TaskRunStatus struct {
	// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;Skipped
	Phase TaskRunPhase `json:"phase,omitempty"`

	// JobRef is the name of the Kubernetes Job this TaskRun created
	// (Normal tasks only; Deploy tasks use RolloutRef instead).
	JobRef string `json:"jobRef,omitempty"`

	// RolloutRef is the name of the Rollout this TaskRun created (Deploy tasks only).
	RolloutRef string `json:"rolloutRef,omitempty"`

	PodName string `json:"podName,omitempty"`

	StartTime      *metav1.Time `json:"startTime,omitempty"`
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	RetryCount int32  `json:"retryCount,omitempty"`
	ExitCode   *int32 `json:"exitCode,omitempty"`
	Message    string `json:"message,omitempty"`

	// LogsRef points to where full logs are archived once the Pod is gone
	// (e.g. an object storage key). Live logs are streamed directly from
	// the Pod through the runner's connector, not stored on this object.
	LogsRef string `json:"logsRef,omitempty"`

	// Approval mirrors PipelineRun.Status.CurrentApproval for this specific
	// task, so a single `kubectl get taskrun` shows everything needed to
	// make a decision.
	Approval *ApprovalStatus `json:"approval,omitempty"`

	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Task",type=string,JSONPath=`.spec.taskName`
// +kubebuilder:printcolumn:name="Retries",type=integer,JSONPath=`.status.retryCount`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TaskRun is the Schema for the taskruns API.
type TaskRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TaskRunSpec   `json:"spec,omitempty"`
	Status TaskRunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TaskRunList contains a list of TaskRun.
type TaskRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TaskRun `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TaskRun{}, &TaskRunList{})
}
