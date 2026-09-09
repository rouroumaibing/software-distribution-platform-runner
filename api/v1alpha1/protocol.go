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
	// MessageRolloutControl relays an operator's pause/promote/rollback
	// command for a Release task's Rollout CR (see RolloutControlPayload).
	MessageRolloutControl MessageType = "rollout_control"

	// Runner -> Hub
	MessageStatusUpdate MessageType = "status_update"
	MessageLogChunk     MessageType = "log_chunk"
	MessageHeartbeat    MessageType = "heartbeat"
)

// Message is the envelope for every frame on the connection: a typed
// discriminant plus an opaque JSON payload decoded by the receiving side
// according to Type.
type Message struct {
	Type    MessageType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}
