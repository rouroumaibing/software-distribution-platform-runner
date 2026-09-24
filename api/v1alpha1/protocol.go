package v1alpha1

import "encoding/json"

// MessageType distinguishes the small set of message shapes exchanged over
// the Hub-Spoke long connection. It lives in the shared api/v1alpha1
// package (not in either binary's internal connector) so the Hub and the
// Runner agree on exactly one wire format.
type MessageType string

const (
	// Hub -> Runner
	MessageApplyPipelineRun MessageType = "apply_pipeline_run"
	MessageApproveTask      MessageType = "approve_task"
	// MessageRerunTask asks the Runner to re-run a single failed/pending task
	// (and its downstream dependents) without re-dispatching the whole run.
	MessageRerunTask MessageType = "rerun_task"
	// MessageCancelPipelineRun asks the Runner to stop a running PipelineRun:
	// its reconciler marks the CR Cancelled and tears down every in-flight
	// TaskRun so no further work is scheduled. It is the operator's emergency
	// brake for a runaway or wedged run (see CancelPipelineRunPayload).
	MessageCancelPipelineRun MessageType = "cancel_pipeline_run"
	// MessageRolloutControl relays an operator's pause/promote/rollback
	// command for a Release task's Rollout CR (see RolloutControlPayload).
	MessageRolloutControl MessageType = "rollout_control"
	// MessageAgentOp dispatches one agent operation (§9.5 exec / §9.9 接入编排)
	// to the Runner that owns the target's cluster access. The Runner executes
	// it out-of-band and reports progress via agent_op_status / agent_op_log.
	MessageAgentOp MessageType = "agent_op"

	// Runner -> Hub
	MessageStatusUpdate MessageType = "status_update"
	MessageLogChunk     MessageType = "log_chunk"
	MessageHeartbeat    MessageType = "heartbeat"
	// MessageAgentOpStatus reports a lifecycle transition of a dispatched
	// agent op (queued→running→succeeded|failed); the hub validates the
	// transition and updates its agent_ops ledger row.
	MessageAgentOpStatus MessageType = "agent_op_status"
	// MessageAgentOpLog streams one output chunk produced by an agent op
	// (stdout/stderr); the hub persists it for replay and fans it out to SSE
	// subscribers.
	MessageAgentOpLog MessageType = "agent_op_log"
)

// Message is the envelope for every frame on the connection: a typed
// discriminant plus an opaque JSON payload decoded by the receiving side
// according to Type.
type Message struct {
	Type    MessageType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}
