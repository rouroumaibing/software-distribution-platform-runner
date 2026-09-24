package controller

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// applyCancelIfRequested honours an operator cancel request for pr. The
// dispatch-side CancelHandler only stamps the AnnCancelRequested annotation;
// this block performs the actual teardown, so the reconciler remains the sole
// writer of PipelineRun status.
//
// It returns done=true when it handled the request, telling Reconcile to
// return immediately (nothing else must run for a cancelled run).
//
// Steps are ordered so a mid-way failure is safe to retry on the next
// reconcile: the work is torn down first, then the terminal state is persisted
// and streamed, then the annotation is cleared. Every step is idempotent.
func (r *PipelineRunReconciler) applyCancelIfRequested(ctx context.Context, pr *sdpv1alpha1.PipelineRun) (bool, error) {
	if _, ok := pr.Annotations[sdpv1alpha1.AnnCancelRequested]; !ok {
		return false, nil
	}

	// A run that already reached a terminal phase is not cancellable — a
	// cancel arriving after Succeeded/Failed (or a redundant one after
	// Cancelled) must never rewrite the outcome. Just clear the stale request
	// so it isn't re-applied on the next pass.
	if isTerminalPhase(pr.Status.Phase) {
		if err := r.clearCancelAnnotations(ctx, pr); err != nil {
			return true, err
		}
		return true, nil
	}

	operator := pr.Annotations[sdpv1alpha1.AnnCancelOperator]
	now := metav1.Now()
	msg := "cancelled"
	if operator != "" {
		msg = "cancelled by " + operator
	}
	pr.Status.Phase = sdpv1alpha1.PipelineRunCancelled
	pr.Status.CompletionTime = &now
	pr.Status.CurrentApproval = nil
	pr.Status.Message = msg

	// 1) Stop the work, capturing what was in flight. The summary reported in
	//    step 2 is the hub's ONLY source for its task rows, so tasks that had
	//    not finished must be reported terminal here or they stay "Running"
	//    on the hub forever after the CRs are gone.
	existing, err := r.cancelTaskRuns(ctx, pr)
	if err != nil {
		return true, err
	}
	pr.Status.Tasks = cancelledTaskSummary(pr.Spec.Tasks, existing, &now, msg)

	// 2) Persist + stream the terminal state. Once this lands the reconcile
	//    loop short-circuits on every later pass, so no new TaskRun is
	//    scheduled even if step 3 has to be retried.
	if err := r.updateStatus(ctx, pr); err != nil {
		return true, err
	}

	// 3) Clear the request so a later reconcile doesn't re-run this.
	if err := r.clearCancelAnnotations(ctx, pr); err != nil {
		return true, err
	}
	return true, nil
}

// cancelledTaskSummary rebuilds the run's task summary with every unfinished
// task marked terminal: a task that had been scheduled (it has a TaskRunRef)
// is Failed, one that never became runnable is Skipped.
func cancelledTaskSummary(tasks []sdpv1alpha1.PipelineTaskSpec, existing map[string]*sdpv1alpha1.TaskRun, now *metav1.Time, msg string) []sdpv1alpha1.TaskRunStatusSummary {
	summaries := summarizeTasks(tasks, existing)
	for i := range summaries {
		s := &summaries[i]
		if isTerminalTaskPhase(s.Phase) {
			continue
		}
		s.Message = msg
		if s.TaskRunRef == "" {
			// Never scheduled — it never became runnable because the run was
			// cancelled, which is exactly what Skipped means.
			s.Phase = sdpv1alpha1.TaskRunSkipped
			continue
		}
		s.Phase = sdpv1alpha1.TaskRunFailed
		s.CompletionTime = now
	}
	return summaries
}

// isTerminalTaskPhase reports whether a TaskRun phase is final.
func isTerminalTaskPhase(p sdpv1alpha1.TaskRunPhase) bool {
	switch p {
	case sdpv1alpha1.TaskRunSucceeded, sdpv1alpha1.TaskRunFailed, sdpv1alpha1.TaskRunSkipped:
		return true
	default:
		return false
	}
}

// cancelTaskRuns deletes every TaskRun of pr, returning them keyed by task
// name so the caller can still report their state. Each TaskRun owns its
// Job/Rollout via a controller reference, so those are garbage-collected with
// it and the underlying pods stop.
func (r *PipelineRunReconciler) cancelTaskRuns(ctx context.Context, pr *sdpv1alpha1.PipelineRun) (map[string]*sdpv1alpha1.TaskRun, error) {
	var list sdpv1alpha1.TaskRunList
	if err := r.List(ctx, &list, client.InNamespace(pr.Namespace), client.MatchingLabels{
		"sdp.io/pipeline-run": pr.Name,
	}); err != nil {
		return nil, err
	}
	existing := make(map[string]*sdpv1alpha1.TaskRun, len(list.Items))
	for i := range list.Items {
		existing[list.Items[i].Spec.TaskName] = &list.Items[i]
	}
	for _, tr := range existing {
		if err := r.Delete(ctx, tr); err != nil && !apierrors.IsNotFound(err) {
			return existing, err
		}
	}
	return existing, nil
}

// clearCancelAnnotations removes the cancel-request pair from the CR's
// metadata (no-op when neither is present).
func (r *PipelineRunReconciler) clearCancelAnnotations(ctx context.Context, pr *sdpv1alpha1.PipelineRun) error {
	_, hasRequested := pr.Annotations[sdpv1alpha1.AnnCancelRequested]
	_, hasOperator := pr.Annotations[sdpv1alpha1.AnnCancelOperator]
	if !hasRequested && !hasOperator {
		return nil
	}
	base := pr.DeepCopy()
	delete(pr.Annotations, sdpv1alpha1.AnnCancelRequested)
	delete(pr.Annotations, sdpv1alpha1.AnnCancelOperator)
	return r.Patch(ctx, pr, client.MergeFrom(base))
}
