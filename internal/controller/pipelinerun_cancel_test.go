package controller

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

func newCancelReconciler(objs ...client.Object) *PipelineRunReconciler {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = sdpv1alpha1.AddToScheme(scheme)
	b := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&sdpv1alpha1.PipelineRun{}, &sdpv1alpha1.TaskRun{})
	if len(objs) > 0 {
		b = b.WithObjects(objs...)
	}
	return &PipelineRunReconciler{Client: b.Build()}
}

func TestReconcileAppliesCancel(t *testing.T) {
	pr := &sdpv1alpha1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pr1", Namespace: "ns",
			Annotations: map[string]string{
				sdpv1alpha1.AnnCancelRequested: "true",
				sdpv1alpha1.AnnCancelOperator:  "alice",
			},
		},
		Spec: sdpv1alpha1.PipelineRunSpec{
			Tasks: []sdpv1alpha1.PipelineTaskSpec{mkTask("a", "s", "Parallel")},
		},
		Status: sdpv1alpha1.PipelineRunStatus{Phase: sdpv1alpha1.PipelineRunRunning},
	}
	tr := &sdpv1alpha1.TaskRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pr1-a", Namespace: "ns",
			Labels: map[string]string{"sdp.io/pipeline-run": "pr1", "sdp.io/task": "a"},
		},
		Spec:   sdpv1alpha1.TaskRunSpec{PipelineRunRef: "pr1", TaskName: "a", Namespace: "ns", Type: sdpv1alpha1.TaskTypeBuild},
		Status: sdpv1alpha1.TaskRunStatus{Phase: sdpv1alpha1.TaskRunRunning, JobRef: "job-a"},
	}

	r := newCancelReconciler(pr, tr)
	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "pr1"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var got sdpv1alpha1.PipelineRun
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "pr1"}, &got); err != nil {
		t.Fatalf("get pr: %v", err)
	}
	if got.Status.Phase != sdpv1alpha1.PipelineRunCancelled {
		t.Errorf("phase = %s, want Cancelled", got.Status.Phase)
	}
	if got.Status.CompletionTime == nil {
		t.Error("CompletionTime should be stamped on cancel")
	}
	if got.Status.Message != "cancelled by alice" {
		t.Errorf("message = %q, want %q", got.Status.Message, "cancelled by alice")
	}
	if got.Status.CurrentApproval != nil {
		t.Error("CurrentApproval should be cleared on cancel")
	}
	// The request annotation must be cleared so it isn't applied twice.
	if _, ok := got.Annotations[sdpv1alpha1.AnnCancelRequested]; ok {
		t.Error("cancel annotation should be cleared after applying")
	}

	// Unfinished tasks must be reported terminal, not left on Running: the
	// summary in the status update is the hub's only source for its task rows.
	if len(got.Status.Tasks) != 1 {
		t.Fatalf("task summary = %+v, want exactly one entry", got.Status.Tasks)
	}
	if got.Status.Tasks[0].Phase != sdpv1alpha1.TaskRunFailed {
		t.Errorf("task summary phase = %s, want Failed", got.Status.Tasks[0].Phase)
	}

	// In-flight work must be torn down.
	var gotTR sdpv1alpha1.TaskRun
	err = r.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "pr1-a"}, &gotTR)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("in-flight TaskRun should be deleted on cancel, got err=%v phase=%s", err, gotTR.Status.Phase)
	}
}

func TestReconcileIgnoresCancelForAlreadyTerminalRun(t *testing.T) {
	// A cancel arriving after the run already succeeded must not flip a
	// successful run to Cancelled; it only clears the stale annotation.
	pr := &sdpv1alpha1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pr3", Namespace: "ns",
			Annotations: map[string]string{sdpv1alpha1.AnnCancelRequested: "true"},
		},
		Status: sdpv1alpha1.PipelineRunStatus{Phase: sdpv1alpha1.PipelineRunSucceeded},
	}
	r := newCancelReconciler(pr)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "pr3"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	var got sdpv1alpha1.PipelineRun
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "pr3"}, &got); err != nil {
		t.Fatalf("get pr: %v", err)
	}
	if got.Status.Phase != sdpv1alpha1.PipelineRunSucceeded {
		t.Errorf("phase = %s, want Succeeded (a finished run is not cancellable)", got.Status.Phase)
	}
}
