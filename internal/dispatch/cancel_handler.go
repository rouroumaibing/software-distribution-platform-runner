package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// CancelHandler receives MessageCancelPipelineRun frames and asks the Runner
// to stop a running PipelineRun.
//
// It deliberately does NOT write Status itself: it stamps the
// AnnCancelRequested annotation and lets PipelineRunReconciler apply the
// cancel. Writing the terminal phase here would race the reconcile loop — a
// reconcile holding a stale copy would clobber Cancelled on its next
// updateStatus, and findRunnableTasks would re-create the TaskRuns this
// handler had just deleted. Keeping the write inside the reconciler (which is
// serialized per object) makes the cancel race-free.
type CancelHandler struct {
	Client client.Client
}

// Handle stamps the cancel annotation; the reconciler performs the teardown
// (mark Cancelled + delete in-flight TaskRuns).
func (h *CancelHandler) Handle(payload json.RawMessage) error {
	var p sdpv1alpha1.CancelPipelineRunPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}
	if p.PipelineRunName == "" || p.Namespace == "" {
		return fmt.Errorf("dispatch: cancel payload needs pipelineRunName and namespace")
	}

	var pr sdpv1alpha1.PipelineRun
	key := client.ObjectKey{Namespace: p.Namespace, Name: p.PipelineRunName}
	if err := h.Client.Get(context.Background(), key, &pr); err != nil {
		if apierrors.IsNotFound(err) {
			log.Printf("dispatch: cancel for unknown PipelineRun %s/%s (ignored)", p.Namespace, p.PipelineRunName)
			return nil
		}
		return err
	}
	if isTerminalRunPhase(pr.Status.Phase) {
		log.Printf("dispatch: cancel for already-terminal PipelineRun %s/%s (%s) ignored",
			p.Namespace, p.PipelineRunName, pr.Status.Phase)
		return nil
	}

	base := pr.DeepCopy()
	if pr.Annotations == nil {
		pr.Annotations = map[string]string{}
	}
	pr.Annotations[sdpv1alpha1.AnnCancelRequested] = "true"
	pr.Annotations[sdpv1alpha1.AnnCancelOperator] = p.Operator
	if err := h.Client.Patch(context.Background(), &pr, client.MergeFrom(base)); err != nil {
		return err
	}
	log.Printf("dispatch: cancel requested for PipelineRun %s/%s by %s",
		p.Namespace, p.PipelineRunName, p.Operator)
	return nil
}

// isTerminalRunPhase mirrors the controller's isTerminalPhase (kept local so
// dispatch doesn't depend on the controller package).
func isTerminalRunPhase(phase sdpv1alpha1.PipelineRunPhase) bool {
	switch phase {
	case sdpv1alpha1.PipelineRunSucceeded,
		sdpv1alpha1.PipelineRunFailed,
		sdpv1alpha1.PipelineRunCancelled:
		return true
	default:
		return false
	}
}
