package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PipelineTaskType defines the category of a task within the DAG.
// The runner's PipelineRun controller branches its reconcile logic on this field:
//   - Build:    run a command (or repo script) inside a tool image; success/failure
//     is judged purely by the process exit code. No output inspection.
//   - Release:  apply a software unit (Helm chart or raw manifest) into the target
//     cluster, with values injected from the hub's parameter management.
//     An optional Canary sub-spec drives a progressive rollout.
//   - Approval: pause the pipeline and wait for an external decision relayed from
//     the hub.
type PipelineTaskType string

const (
	TaskTypeBuild    PipelineTaskType = "Build"
	TaskTypeRelease  PipelineTaskType = "Release"
	TaskTypeApproval PipelineTaskType = "Approval"
	// TaskTypeTest runs a test/verification command (e.g. a test suite,
	// smoke check, or QA gate) the same way a Build task runs its script —
	// it's a pod-executed command whose exit code decides pass/fail. Added
	// for the "转测 / Test" pipeline class (C-13).
	TaskTypeTest PipelineTaskType = "Test"
)

// ExecutionMode controls how the tasks that share a Stage are scheduled
// relative to each other. Cross-stage ordering is always serial (the hub
// derives DependsOn so every task in a later stage depends on all tasks in
// the preceding stage); ExecutionMode only governs ordering *within* a
// stage.
type ExecutionMode string

const (
	// ExecutionModeParallel (the default — also the zero value) lets every
	// task in a stage start as soon as its DependsOn is satisfied, with no
	// implied ordering between siblings.
	ExecutionModeParallel ExecutionMode = "Parallel"
	// ExecutionModeSerial runs the stage's tasks one at a time, in the order
	// they appear in Spec.Tasks: the next sibling only becomes runnable after
	// the previous one has Succeeded. Implemented Runner-side in
	// findRunnableTasks (C-06) — the hub does not need to synthesize
	// DependsOn for it.
	ExecutionModeSerial ExecutionMode = "Serial"
)

// EnvironmentType is the runner-side model of a target environment (C-13).
// It's a coarse classification used for scheduling/quarantine decisions and
// human-readable reporting; the authoritative environment identity is still
// PipelineRunSpec.EnvironmentID (a hub-assigned UUID). The runner never
// resolves environment semantics against the hub — it only validates/echoes
// the class.
type EnvironmentType string

const (
	EnvironmentDev       EnvironmentType = "dev"
	EnvironmentTest      EnvironmentType = "test"
	EnvironmentStaging   EnvironmentType = "staging"
	EnvironmentProd      EnvironmentType = "prod"
	EnvironmentCanary    EnvironmentType = "canary"
	EnvironmentBlueGreen EnvironmentType = "bluegreen"
)

// IsValidEnvironment reports whether s is a known EnvironmentType. Unknown
// values (including "") are rejected so a misconfigured hub payload fails
// fast rather than silently scheduling into an unmodeled environment.
func IsValidEnvironment(s string) bool {
	switch EnvironmentType(s) {
	case EnvironmentDev, EnvironmentTest, EnvironmentStaging,
		EnvironmentProd, EnvironmentCanary, EnvironmentBlueGreen:
		return true
	default:
		return false
	}
}

// PipelineRunPhase represents the overall lifecycle phase of a PipelineRun.
type PipelineRunPhase string

const (
	PipelineRunPending         PipelineRunPhase = "Pending"
	PipelineRunRunning         PipelineRunPhase = "Running"
	PipelineRunWaitingApproval PipelineRunPhase = "WaitingApproval"
	PipelineRunSucceeded       PipelineRunPhase = "Succeeded"
	PipelineRunFailed          PipelineRunPhase = "Failed"
	PipelineRunCancelled       PipelineRunPhase = "Cancelled"
)

// Param is a simple key/value pipeline parameter, substituted into task
// command/args/env by the hub before the PipelineRun is dispatched.
type Param struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// RepoSource describes where to check out source code from for
// script-based tasks (build.sh / run.sh / test.sh style execution).
// The hub resolves branch/tag names to a commit SHA at trigger time so
// every task in the same PipelineRun checks out the exact same commit,
// even if the run takes a while to finish.
type RepoSource struct {
	// URL is the git remote, e.g. https://github.com/org/repo.git
	URL string `json:"url"`

	// Ref is a commit SHA (recommended), resolved by the hub before dispatch.
	Ref string `json:"ref"`

	// Path is the subdirectory to treat as the working directory for
	// monorepos where one component lives in a subfolder.
	Path string `json:"path,omitempty"`

	// SecretRef names a Secret (already present in TargetNamespace) holding
	// git credentials, for private repos. The runner never receives raw
	// credentials from the hub over the wire.
	SecretRef string `json:"secretRef,omitempty"`
}

// RetryPolicy defines retry behavior for a failed task.
type RetryPolicy struct {
	// +kubebuilder:default=0
	MaxRetries int32 `json:"maxRetries,omitempty"`
	// Backoff between retries, e.g. "30s". Empty means retry immediately.
	Backoff metav1.Duration `json:"backoff,omitempty"`
}

// PipelineTaskSpec defines a single node in the pipeline DAG.
type PipelineTaskSpec struct {
	// Name must be unique within the PipelineRun; referenced by DependsOn.
	Name string `json:"name"`

	// +kubebuilder:validation:Enum=Build;Release;Approval
	Type PipelineTaskType `json:"type"`

	// Stage groups this task for UI display and sequencing. Tasks sharing a
	// Stage run in parallel (no implicit dependency between them); the hub
	// auto-generates DependsOn so every task in a later Stage depends on
	// every task in the immediately preceding Stage.
	Stage string `json:"stage,omitempty"`

	// ExecutionMode overrides how siblings within the same Stage are ordered
	// (defaults to ExecutionModeParallel). ExecutionModeSerial runs the
	// stage's tasks one at a time. See the ExecutionMode doc above (C-06).
	ExecutionMode ExecutionMode `json:"executionMode,omitempty"`

	// Image is the execution environment the script runs in (e.g. an image
	// with the language toolchain, kubectl/helm for deploy scripts, etc).
	Image string `json:"image,omitempty"`

	// ScriptPath is a path relative to the checked-out repo root (or
	// Repo.Path for monorepos), e.g. "build.sh", "test.sh", "run.sh".
	// The runner invokes it as `sh {ScriptPath} {ScriptArgs...}` from that
	// working directory.
	ScriptPath string `json:"scriptPath,omitempty"`

	// ScriptArgs are passed positionally to the script, e.g. ["beta"] so a
	// single run.sh can branch on target environment internally.
	ScriptArgs []string `json:"scriptArgs,omitempty"`

	// Command/Args remain available as an escape hatch for tasks that don't
	// follow the script convention (e.g. calling a prebuilt tool image
	// directly rather than a checked-out script).
	Command []string `json:"command,omitempty"`
	Args    []string `json:"args,omitempty"`

	// Repo overrides PipelineRunSpec.Repo for this task only; leave nil to
	// inherit the run-level checkout. Approval tasks ignore this field.
	Repo *RepoSource `json:"repo,omitempty"`

	// Produces lists artifact keys this task uploads to the artifact store
	// after ScriptPath exits 0 (e.g. ["build-output"]). Keys are scoped to
	// the PipelineRun automatically; the task only names them.
	Produces []string `json:"produces,omitempty"`

	// Consumes lists artifact keys (produced by any upstream task in this
	// same PipelineRun) that the runner downloads into the working
	// directory before ScriptPath runs.
	Consumes []string `json:"consumes,omitempty"`

	// DependsOn lists upstream task names that must reach Succeeded before
	// this task becomes runnable. Tasks with no DependsOn are entry nodes.
	// Usually left empty and derived from Stage instead of set by hand.
	DependsOn []string `json:"dependsOn,omitempty"`

	// Privileged (G-4) runs the task container with
	// securityContext.privileged=true so docker-in-docker /
	// containerd-in-containerd build images work. Opt-in per task.
	Privileged bool `json:"privileged,omitempty"`

	RetryPolicy *RetryPolicy `json:"retryPolicy,omitempty"`

	// TimeoutSeconds bounds how long the corresponding TaskRun may run
	// before being marked Failed. 0 means use the pipeline-level default.
	TimeoutSeconds int64 `json:"timeoutSeconds,omitempty"`

	// ApprovalConfig is required when Type == Approval, ignored otherwise.
	ApprovalConfig *ApprovalConfig `json:"approvalConfig,omitempty"`

	// RolloutSpec, when set, drives a progressive rollout (canary) for a
	// Release task instead of a one-shot apply. Ignored for Build/Approval.
	RolloutSpec *RolloutSpec `json:"rolloutSpec,omitempty"`

	// ReleaseSpec is required when Type == Release; it describes the chart
	// or manifest to apply plus the values injected from parameter management.
	ReleaseSpec *ReleaseSpec `json:"releaseSpec,omitempty"`
}

// ApprovalConfig describes who must approve an Approval-type task.
type ApprovalConfig struct {
	// RequiredApprovals is the number of distinct approvers needed to proceed.
	// +kubebuilder:validation:Minimum=1
	RequiredApprovals int32 `json:"requiredApprovals"`

	// AllowedApprovers restricts who may approve by identity (email/user ID,
	// resolved and enforced by the hub). Empty means anyone with project
	// access may approve.
	AllowedApprovers []string `json:"allowedApprovers,omitempty"`

	// TimeoutSeconds auto-fails the PipelineRun if no decision is made in
	// time. 0 means wait indefinitely.
	TimeoutSeconds int64 `json:"timeoutSeconds,omitempty"`
}

// PipelineRunSpec defines the desired state of a PipelineRun.
// A PipelineRun is created by the hub (via the runner's long-lived
// connection) whenever a pipeline is triggered against a specific
// environment on this cluster.
type PipelineRunSpec struct {
	// PipelineRef optionally names a reusable pipeline definition owned by
	// the hub, for display/audit purposes only — the runner never fetches
	// it; Tasks below is always the full, already-resolved DAG.
	PipelineRef string `json:"pipelineRef,omitempty"`

	// Tasks is the fully resolved DAG definition to execute.
	// +kubebuilder:validation:MinItems=1
	Tasks []PipelineTaskSpec `json:"tasks"`

	// Repo is the default checkout source for every script-based task in
	// this run; individual tasks may override via PipelineTaskSpec.Repo.
	Repo *RepoSource `json:"repo,omitempty"`

	Params []Param `json:"params,omitempty"`

	// TenantID / ProjectID / EnvironmentID carry multi-tenancy context down
	// to the runner so it can apply namespace routing, quotas, and RBAC
	// scoping without a round-trip back to the hub.
	TenantID      string `json:"tenantID"`
	ProjectID     string `json:"projectID"`
	EnvironmentID string `json:"environmentID"`

	// TargetNamespace is the namespace the runner creates Jobs/TaskRuns in.
	// Convention: {tenant}-{project}-{env}.
	TargetNamespace string `json:"targetNamespace"`

	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// TimeoutSeconds bounds the whole pipeline run; 0 means no overall limit.
	TimeoutSeconds int64 `json:"timeoutSeconds,omitempty"`

	// TriggeredBy records the hub-side user/system identity that started
	// this run, for audit display via kubectl.
	TriggeredBy string `json:"triggeredBy,omitempty"`
}

// TaskRunStatusSummary is an embedded, lightweight view of a TaskRun's
// progress, kept in sync by the PipelineRun controller so the whole DAG
// state is visible from a single `kubectl get pipelinerun -o yaml` without
// needing to list TaskRuns separately.
type TaskRunStatusSummary struct {
	Name           string       `json:"name"`
	TaskRunRef     string       `json:"taskRunRef,omitempty"`
	Phase          TaskRunPhase `json:"phase"`
	RetryCount     int32        `json:"retryCount,omitempty"`
	StartTime      *metav1.Time `json:"startTime,omitempty"`
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
	Message        string       `json:"message,omitempty"`
}

// ApprovalStatus tracks the live state of a paused Approval task.
type ApprovalStatus struct {
	TaskName      string      `json:"taskName"`
	RequestedAt   metav1.Time `json:"requestedAt"`
	RequiredCount int32       `json:"requiredCount"`

	// ApprovedBy is appended to by the hub patching this status (relayed
	// through the runner) as each required approver signs off.
	ApprovedBy []string `json:"approvedBy,omitempty"`

	// RejectedBy is set the moment any allowed approver rejects; a single
	// rejection fails the PipelineRun.
	RejectedBy string `json:"rejectedBy,omitempty"`
}

// PipelineRunStatus defines the observed state of a PipelineRun.
type PipelineRunStatus struct {
	// +kubebuilder:validation:Enum=Pending;Running;WaitingApproval;Succeeded;Failed;Cancelled
	Phase PipelineRunPhase `json:"phase,omitempty"`

	StartTime      *metav1.Time `json:"startTime,omitempty"`
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Tasks mirrors the progress of every DAG node; index-aligned with
	// Spec.Tasks by Name, not by array position.
	Tasks []TaskRunStatusSummary `json:"tasks,omitempty"`

	// CurrentApproval is set while Phase == WaitingApproval and cleared
	// once the gate is resolved.
	CurrentApproval *ApprovalStatus `json:"currentApproval,omitempty"`

	// ObservedGeneration lets the controller (and the hub) detect whether
	// Status reflects the latest Spec.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Message carries a human-readable explanation for Failed/Cancelled runs.
	Message string `json:"message,omitempty"`

	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Tenant",type=string,JSONPath=`.spec.tenantID`
// +kubebuilder:printcolumn:name="Environment",type=string,JSONPath=`.spec.environmentID`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PipelineRun is the Schema for the pipelineruns API.
type PipelineRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PipelineRunSpec   `json:"spec,omitempty"`
	Status PipelineRunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PipelineRunList contains a list of PipelineRun.
type PipelineRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PipelineRun `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PipelineRun{}, &PipelineRunList{})
}

// ReleaseSourceType selects how a Release task applies its software unit.
type ReleaseSourceType string

const (
	// ReleaseSourceChart applies a Helm chart (helm upgrade --install).
	ReleaseSourceChart ReleaseSourceType = "Chart"
	// ReleaseSourceManifest applies raw YAML manifest(s) via kubectl apply.
	ReleaseSourceManifest ReleaseSourceType = "Manifest"
)

// ChartSource identifies a Helm chart to release.
type ChartSource struct {
	// RepoURL is the Helm repository URL (e.g. https://charts.example.com).
	// Omit for charts served from the default/cluster-local registry.
	RepoURL string `json:"repoURL,omitempty"`
	// Name is the chart name (e.g. "nginx").
	Name string `json:"name"`
	// Version pins the chart version; empty means latest.
	Version string `json:"version,omitempty"`
}

// ManifestSource carries inline YAML manifest(s) applied verbatim.
type ManifestSource struct {
	// Content is one or more YAML documents applied via `kubectl apply -f -`.
	Content string `json:"content"`
}

// ReleaseSpec defines a Release task: apply a software unit (chart or
// manifest) into the target cluster, with values injected from the hub's
// parameter management. An optional Canary sub-spec turns the one-shot
// apply into a progressive rollout driven by the existing Rollout machinery.
//
// Success/failure is judged by the release Job's exit code, exactly like a
// Build task — the runner never inspects the applied object's contents.
type ReleaseSpec struct {
	// Source selects Chart vs Manifest application.
	Source ReleaseSourceType `json:"source"`

	Chart *ChartSource `json:"chart,omitempty"`
	// Manifest is required when Source == Manifest.
	Manifest *ManifestSource `json:"manifest,omitempty"`

	// Values are injected (from the hub's parameter management) as Helm
	// --set pairs (chart) or object fields (manifest). Kept as a flat
	// key/value map so the runner stays free of any templating logic.
	Values map[string]string `json:"values,omitempty"`

	// TargetNamespace overrides the run namespace for this release.
	TargetNamespace string `json:"targetNamespace,omitempty"`

	// Canary, when set, drives a progressive rollout via the Rollout
	// controller instead of a single apply.
	Canary *RolloutSpec `json:"canary,omitempty"`
}
