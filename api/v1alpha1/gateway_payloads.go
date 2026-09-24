package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// StatusUpdatePayload is sent Runner -> Hub over MessageStatusUpdate. It
// carries the live phase of a PipelineRun plus a per-task progress summary,
// so the hub can keep its pipeline_runs / task_runs history rows in sync
// without the runner shipping raw CRD objects over the wire.
type StatusUpdatePayload struct {
	TargetID             string                 `json:"targetID"`
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
	TargetID        string `json:"targetID"`
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
	// PublishVersion is the hub-side pipeline/publish version this run was
	// triggered from. It's an optional, immutable marker captured by the
	// ApplyHandler as a snapshot annotation so the exact stage/task
	// definitions that produced this run are reconstructable later
	// (see C-03 — "trigger-time snapshot 固化").
	PublishVersion string `json:"publishVersion,omitempty"`
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
	PipelineRunName string        `json:"pipelineRunName"`
	TaskName        string        `json:"taskName"`
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

// RerunTaskPayload is the Hub -> Runner envelope carried by
// MessageRerunTask. It asks the Runner to reset a single task (and,
// transitively, every downstream task that depends on it) so the DAG picks
// it up again — without re-dispatching the entire PipelineRun. The Runner's
// RerunHandler finds the TaskRun by the pipeline-run/task labels, clears its
// terminal state, and deletes the underlying Job/Rollout so it is rebuilt.
type RerunTaskPayload struct {
	PipelineRunName string `json:"pipelineRunName"`
	TaskName        string `json:"taskName"`
	// Operator is the hub-resolved identity issuing the rerun (audit only).
	Operator string `json:"operator,omitempty"`
}

// Agent op lifecycle states reported via MessageAgentOpStatus. They mirror
// the hub-side agent_ops ledger (hub internal/target/models): the Runner may
// move an op forward queued→running→succeeded|failed; terminal states are
// immutable and rejected by the hub's transition guard.
const (
	AgentOpStatusRunning   = "running"
	AgentOpStatusSucceeded = "succeeded"
	AgentOpStatusFailed    = "failed"
	AgentOpStreamStdout    = "stdout"
	AgentOpStreamStderr    = "stderr"

	// AgentOpTypeExec is the only op type dispatched to the Runner today
	// (§16.5 裁定); install/upgrade await the §9.9 bootstrap flow.
	AgentOpTypeExec = "exec"
)

// AgentOpDispatchPayload is the Hub -> Runner envelope carried by
// MessageAgentOp. OpID identifies the hub-side agent_ops ledger row the
// Runner's status/log reports must reference. Detail carries the op argument
// verbatim (the command or script for exec). Kubeconfig is set only for
// kubeconfig-access environments: the Runner is the direct-connect executor
// and legitimately needs cluster access the hub itself does not have; the
// wire is the authenticated gateway connection (§9.5 trust boundary).
type AgentOpDispatchPayload struct {
	OpID       string `json:"opID"`
	TargetID   string `json:"targetID"`
	EnvID      string `json:"envID,omitempty"`
	OpType     string `json:"opType"` // exec (install/upgrade dispatch is deferred to the bootstrap flow)
	Detail     string `json:"detail"`
	Namespace  string `json:"namespace,omitempty"` // where the op's Job runs; hub supplies the environment namespace
	Kubeconfig []byte `json:"kubeconfig,omitempty"`
}

// AgentOpStatusPayload is the Runner -> Hub envelope carried by
// MessageAgentOpStatus: one lifecycle transition of a dispatched op.
type AgentOpStatusPayload struct {
	OpID    string `json:"opID"`
	Status  string `json:"status"` // running | succeeded | failed
	Message string `json:"message,omitempty"`
}

// AgentOpLogPayload is the Runner -> Hub envelope carried by
// MessageAgentOpLog: one streamed output chunk of a running op.
type AgentOpLogPayload struct {
	OpID   string `json:"opID"`
	Stream string `json:"stream,omitempty"` // stdout | stderr
	Chunk  string `json:"chunk"`
}
