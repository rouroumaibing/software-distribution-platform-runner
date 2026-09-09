package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// StatusUpdatePayload is sent Runner -> Hub over MessageStatusUpdate. It
// carries the live phase of a PipelineRun plus a per-task progress summary,
// so the hub can keep its pipeline_runs / task_runs history rows in sync
// without the runner shipping raw CRD objects over the wire.
type StatusUpdatePayload struct {
	ClusterID            string                 `json:"clusterID"`
	PipelineRunName      string                 `json:"pipelineRunName"`
	PipelineRunNamespace string                 `json:"pipelineRunNamespace"`
	Phase                PipelineRunPhase       `json:"phase"`
	Message              string                 `json:"message,omitempty"`
	StartTime            *metav1.Time           `json:"startTime,omitempty"`
	CompletionTime       *metav1.Time           `json:"completionTime,omitempty"`
	Tasks                []TaskRunStatusSummary `json:"tasks,omitempty"`
}

// LogChunkPayload is sent Runner -> Hub over MessageLogChunk. Live logs are
// streamed per chunk rather than archived; the hub stores them for display.
type LogChunkPayload struct {
	ClusterID       string `json:"clusterID"`
	PipelineRunName string `json:"pipelineRunName"`
	TaskName        string `json:"taskName,omitempty"`
	Stream          string `json:"stream,omitempty"` // "stdout" | "stderr"
	Chunk           string `json:"chunk"`
}

// ApplyPipelineRunPayload is the Hub -> Runner envelope carried by
// MessageApplyPipelineRun. Name/Namespace identify the PipelineRun CR the
// Runner must create in its own cluster; they MUST match the Hub's
// pipeline_runs row (CRName / CRNamespace) so the status updates the Runner
// streams back route to the correct run on the Hub side. Spec is the fully
// resolved DAG to execute.
type ApplyPipelineRunPayload struct {
	Name      string          `json:"name"`
	Namespace string          `json:"namespace"`
	Spec      PipelineRunSpec `json:"spec"`
}

// RolloutAction is the operator command carried by RolloutControlPayload.
type RolloutAction string

const (
	RolloutActionPause    RolloutAction = "pause"    // hold the rollout at the current weight
	RolloutActionPromote  RolloutAction = "promote"  // resume / advance past the current step
	RolloutActionRollback RolloutAction = "rollback" // revert to 0% canary weight and abort
)

// RolloutControlPayload is the Hub -> Runner envelope carried by
// MessageRolloutControl. It relays an operator's progressive-delivery
// command for a Release-type task's Rollout CR, identified by the owning
// PipelineRun CR name and the task name. The dispatch handler patches the
// matching Rollout with a control annotation; the RolloutReconciler observes
// it, applies the command to Status (scale/step changes), and clears the
// annotation — keeping all Rollout writes inside the reconciler.
type RolloutControlPayload struct {
	PipelineRunName string `json:"pipelineRunName"`
	TaskName        string `json:"taskName"`
	Action          RolloutAction `json:"action"`
	// Operator is the hub-resolved identity of who issued the command;
	// recorded in the Rollout's status message for audit.
	Operator string `json:"operator,omitempty"`
}

// ApproveTaskPayload is the Hub -> Runner envelope carried by
// MessageApproveTask. It relays an approver's decision for a paused
// Approval-type task, identified by the owning PipelineRun CR name and the
// task name. The Runner patches the corresponding TaskRun.Approval status,
// which the PipelineRun controller observes to advance (or fail) the DAG.
type ApproveTaskPayload struct {
	PipelineRunName string `json:"pipelineRunName"`
	TaskName        string `json:"taskName"`
	// Approver is the hub-resolved identity (email/user id) of the person
	// who made this decision; recorded in TaskRun.Status.Approval.ApprovedBy.
	Approver string `json:"approver"`
	// Rejected, when true, fails the PipelineRun immediately.
	Rejected bool `json:"rejected,omitempty"`
}
