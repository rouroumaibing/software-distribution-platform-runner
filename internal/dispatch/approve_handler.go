package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// ApproverHandler receives MessageApproveTask frames and records the decision
// on the matching TaskRun's Status.Approval. The PipelineRun controller
// watches TaskRun status and either advances the DAG (enough approvals) or
// fails the run (a rejection).
type ApproverHandler struct {
	Client client.Client
}

// Handle finds the paused Approval TaskRun and applies the decision.
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

	// Make sure the approval status is initialized (the TaskRun controller
	// also does this, but a late approval arriving before the first
	// reconcile shouldn't panic).
	if tr.Status.Approval == nil {
		tr.Status.Approval = &sdpv1alpha1.ApprovalStatus{
			TaskName:      p.TaskName,
			RequestedAt:   metav1.Now(),
			RequiredCount: requiredCount(&tr),
		}
	}

	if p.Rejected {
		tr.Status.Approval.RejectedBy = p.Approver
		tr.Status.Phase = sdpv1alpha1.TaskRunFailed
		tr.Status.Message = "rejected by " + p.Approver
	} else {
		for _, a := range tr.Status.Approval.ApprovedBy {
			if a == p.Approver {
				return nil // idempotent: same approver twice is a no-op
			}
		}
		tr.Status.Approval.ApprovedBy = append(tr.Status.Approval.ApprovedBy, p.Approver)
		if int32(len(tr.Status.Approval.ApprovedBy)) >= tr.Status.Approval.RequiredCount {
			tr.Status.Phase = sdpv1alpha1.TaskRunSucceeded
			tr.Status.Message = "approved"
		}
	}

	if err := h.Client.Status().Update(context.Background(), &tr); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	log.Printf("dispatch: applied decision for %s/%s approver=%s rejected=%v",
		p.PipelineRunName, p.TaskName, p.Approver, p.Rejected)
	return nil
}

func requiredCount(tr *sdpv1alpha1.TaskRun) int32 {
	if tr.Spec.ApprovalConfig != nil {
		return tr.Spec.ApprovalConfig.RequiredApprovals
	}
	return 1
}
