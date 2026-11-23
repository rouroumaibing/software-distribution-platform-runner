package controller

import (
	"context"
	"encoding/json"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

func commandTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = sdpv1alpha1.AddToScheme(scheme)
	return scheme
}

// The TaskRun reconciler consumes the rerun intent: it tears down the Job,
// resets observed status, and clears the intent. The handler itself writes
// none of this.
func TestApplyRerunIntentResetsTaskRun(t *testing.T) {
	ctx := context.Background()
	scheme := commandTestScheme(t)
	tr := &sdpv1alpha1.TaskRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pr-a", Namespace: "ns",
			Annotations: map[string]string{
				sdpv1alpha1.AnnRerunRequested: "true",
				sdpv1alpha1.AnnRerunOperator:  "alice",
			},
		},
		Spec:   sdpv1alpha1.TaskRunSpec{PipelineRunRef: "pr", TaskName: "a", Namespace: "ns"},
		Status: sdpv1alpha1.TaskRunStatus{Phase: sdpv1alpha1.TaskRunFailed, JobRef: "job-a", RetryCount: 3},
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "job-a", Namespace: "ns"}}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&sdpv1alpha1.TaskRun{}).WithObjects(tr, job).Build()
	r := &TaskRunReconciler{Client: c}

	done, err := r.applyRerunIntent(ctx, tr)
	if err != nil || !done {
		t.Fatalf("applyRerunIntent done=%v err=%v", done, err)
	}
	var got sdpv1alpha1.TaskRun
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pr-a"}, &got); err != nil {
		t.Fatalf("get taskrun: %v", err)
	}
	if got.Status.Phase != sdpv1alpha1.TaskRunPending || got.Status.JobRef != "" || got.Status.RetryCount != 0 {
		t.Errorf("status not reset: phase=%s jobRef=%q retries=%d", got.Status.Phase, got.Status.JobRef, got.Status.RetryCount)
	}
	if got.Annotations[sdpv1alpha1.AnnRerunRequested] != "" || got.Annotations[sdpv1alpha1.AnnRerunOperator] != "" {
		t.Errorf("intent not cleared: %v", got.Annotations)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "job-a"}, &batchv1.Job{}); !apierrors.IsNotFound(err) {
		t.Errorf("old Job should be deleted, got err=%v", err)
	}
}

// Approval decisions are consumed into Status.Approval by the reconciler,
// not written by the handler.
func TestApplyApprovalIntentRecordsDecisions(t *testing.T) {
	ctx := context.Background()
	scheme := commandTestScheme(t)
	decisions, _ := json.Marshal([]sdpv1alpha1.ApprovalDecisionRecord{
		{Approver: "alice"}, {Approver: "bob"},
	})
	tr := &sdpv1alpha1.TaskRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pr-approval", Namespace: "ns",
			Annotations: map[string]string{sdpv1alpha1.AnnApprovalDecision: string(decisions)},
		},
		Spec: sdpv1alpha1.TaskRunSpec{
			PipelineRunRef: "pr", TaskName: "approval", Namespace: "ns",
			Type:           sdpv1alpha1.TaskTypeApproval,
			ApprovalConfig: &sdpv1alpha1.ApprovalConfig{RequiredApprovals: 2},
		},
		Status: sdpv1alpha1.TaskRunStatus{Phase: sdpv1alpha1.TaskRunPending},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&sdpv1alpha1.TaskRun{}).WithObjects(tr).Build()
	r := &TaskRunReconciler{Client: c}

	done, err := r.applyApprovalIntent(ctx, tr)
	if err != nil || !done {
		t.Fatalf("applyApprovalIntent done=%v err=%v", done, err)
	}
	var got sdpv1alpha1.TaskRun
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pr-approval"}, &got); err != nil {
		t.Fatalf("get taskrun: %v", err)
	}
	if got.Status.Phase != sdpv1alpha1.TaskRunSucceeded {
		t.Errorf("phase = %s, want Succeeded after 2 approvals", got.Status.Phase)
	}
	if got.Status.Approval == nil || len(got.Status.Approval.ApprovedBy) != 2 {
		t.Errorf("approvedBy = %v, want two approvers", got.Status.Approval)
	}
	if got.Annotations[sdpv1alpha1.AnnApprovalDecision] != "" {
		t.Error("approval intent must be cleared after consumption")
	}
}

// Only the reconciler revives a Failed run, and a Cancelled run stays
// cancelled regardless of the stamped intent.
func TestApplyRerunReviveIntent(t *testing.T) {
	ctx := context.Background()
	scheme := commandTestScheme(t)
	makePR := func(name string, phase sdpv1alpha1.PipelineRunPhase) *sdpv1alpha1.PipelineRun {
		return &sdpv1alpha1.PipelineRun{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: "ns",
				Annotations: map[string]string{
					sdpv1alpha1.AnnRerunRequested: "true",
					sdpv1alpha1.AnnRerunOperator:  "bob",
				},
			},
			Status: sdpv1alpha1.PipelineRunStatus{Phase: phase},
		}
	}
	failed, cancelled := makePR("pr-failed", sdpv1alpha1.PipelineRunFailed), makePR("pr-cancelled", sdpv1alpha1.PipelineRunCancelled)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&sdpv1alpha1.PipelineRun{}).WithObjects(failed, cancelled).Build()
	r := &PipelineRunReconciler{Client: c}

	done, err := r.applyRerunReviveIntent(ctx, failed)
	if err != nil || !done {
		t.Fatalf("failed revive done=%v err=%v", done, err)
	}
	var got sdpv1alpha1.PipelineRun
	_ = c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pr-failed"}, &got)
	if got.Status.Phase != sdpv1alpha1.PipelineRunRunning {
		t.Errorf("failed run phase = %s, want Running", got.Status.Phase)
	}
	if got.Annotations[sdpv1alpha1.AnnRerunRequested] != "" {
		t.Error("revive intent must be cleared after consumption")
	}

	done, err = r.applyRerunReviveIntent(ctx, cancelled)
	if err != nil || !done {
		t.Fatalf("cancelled revive done=%v err=%v", done, err)
	}
	_ = c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pr-cancelled"}, &got)
	if got.Status.Phase != sdpv1alpha1.PipelineRunCancelled {
		t.Errorf("cancelled run phase = %s, want Cancelled", got.Status.Phase)
	}
}
