package dispatch

import (
	"context"
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

func newCancelHandler(objs ...client.Object) *CancelHandler {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = sdpv1alpha1.AddToScheme(scheme)
	b := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&sdpv1alpha1.PipelineRun{})
	if len(objs) > 0 {
		b = b.WithObjects(objs...)
	}
	return &CancelHandler{Client: b.Build()}
}

func TestCancelHandlerStampsAnnotationWithoutTouchingStatus(t *testing.T) {
	pr := &sdpv1alpha1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "pr1", Namespace: "ns"},
		Status:     sdpv1alpha1.PipelineRunStatus{Phase: sdpv1alpha1.PipelineRunRunning},
	}
	h := newCancelHandler(pr)
	payload, _ := json.Marshal(sdpv1alpha1.CancelPipelineRunPayload{
		PipelineRunName: "pr1", Namespace: "ns", Operator: "alice",
	})
	if err := h.Handle(payload); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	var got sdpv1alpha1.PipelineRun
	if err := h.Client.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "pr1"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Annotations[sdpv1alpha1.AnnCancelRequested] != "true" {
		t.Errorf("cancel annotation not stamped: %v", got.Annotations)
	}
	if got.Annotations[sdpv1alpha1.AnnCancelOperator] != "alice" {
		t.Errorf("operator annotation = %q, want alice", got.Annotations[sdpv1alpha1.AnnCancelOperator])
	}
	// The reconciler owns status; the handler must never write it.
	if got.Status.Phase != sdpv1alpha1.PipelineRunRunning {
		t.Errorf("handler must not change phase, got %s", got.Status.Phase)
	}
}

func TestCancelHandlerIgnoresTerminalRun(t *testing.T) {
	pr := &sdpv1alpha1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "pr2", Namespace: "ns"},
		Status:     sdpv1alpha1.PipelineRunStatus{Phase: sdpv1alpha1.PipelineRunSucceeded},
	}
	h := newCancelHandler(pr)
	payload, _ := json.Marshal(sdpv1alpha1.CancelPipelineRunPayload{
		PipelineRunName: "pr2", Namespace: "ns", Operator: "alice",
	})
	if err := h.Handle(payload); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	var got sdpv1alpha1.PipelineRun
	if err := h.Client.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "pr2"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, ok := got.Annotations[sdpv1alpha1.AnnCancelRequested]; ok {
		t.Error("a terminal run must not receive a cancel annotation")
	}
}

func TestCancelHandlerIgnoresUnknownRun(t *testing.T) {
	h := newCancelHandler()
	payload, _ := json.Marshal(sdpv1alpha1.CancelPipelineRunPayload{PipelineRunName: "nope", Namespace: "ns"})
	if err := h.Handle(payload); err != nil {
		t.Fatalf("unknown run should be ignored, got %v", err)
	}
}

func TestCancelHandlerRejectsIncompletePayload(t *testing.T) {
	h := newCancelHandler()
	if err := h.Handle(json.RawMessage(`{"pipelineRunName":"","namespace":""}`)); err == nil {
		t.Fatal("expected an error for a payload missing name/namespace")
	}
}
