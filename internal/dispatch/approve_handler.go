package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"sigs.k8s.io/controller-runtime/pkg/client"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// ApproverHandler receives MessageApproveTask frames and records the decision
// as an intent on the matching TaskRun. TaskRunReconciler consumes the intent
// into Status.Approval; the PipelineRun controller then advances (enough
// approvals) or fails the run (a rejection). The handler never writes Status.
type ApproverHandler struct {
	Client client.Client
}

// Handle appends the decision to the TaskRun's approval-intent annotation.
func (h *ApproverHandler) Handle(payload json.RawMessage) error {
	var p sdpv1alpha1.ApproveTaskPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}
	if p.PipelineRunName == "" || p.TaskName == "" {
		return fmt.Errorf("dispatch: approve payload needs pipelineRunName and taskName")
	}

	var list sdpv1alpha1.TaskRunList
	if err := h.Client.List(context.Background(), &list,
		client.MatchingLabels{
			"sdp.io/pipeline-run": p.PipelineRunName,
			"sdp.io/task":         p.TaskName,
		},
	); err != nil {
		return err
	}
	if len(list.Items) == 0 {
		log.Printf("dispatch: no TaskRun for approval %s/%s (ignored)", p.PipelineRunName, p.TaskName)
		return nil
	}
	tr := list.Items[0]

	var decisions []sdpv1alpha1.ApprovalDecisionRecord
	if raw := tr.Annotations[sdpv1alpha1.AnnApprovalDecision]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &decisions); err != nil {
			return fmt.Errorf("dispatch: invalid approval decision intent: %w", err)
		}
	}
	for _, d := range decisions {
		if d.Approver == p.Approver {
			return nil // idempotent: same approver twice is a no-op
		}
	}
	decisions = append(decisions, sdpv1alpha1.ApprovalDecisionRecord{
		Approver: p.Approver, Rejected: p.Rejected,
	})
	raw, err := json.Marshal(decisions)
	if err != nil {
		return err
	}

	base := tr.DeepCopy()
	if tr.Annotations == nil {
		tr.Annotations = map[string]string{}
	}
	tr.Annotations[sdpv1alpha1.AnnApprovalDecision] = string(raw)
	if err := h.Client.Patch(context.Background(), &tr, client.MergeFrom(base)); err != nil {
		return err
	}
	log.Printf("dispatch: approval intent for %s/%s approver=%s rejected=%v",
		p.PipelineRunName, p.TaskName, p.Approver, p.Rejected)
	return nil
}
