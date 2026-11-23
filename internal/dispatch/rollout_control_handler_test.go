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

// The dispatch handler's only Rollout write is the Spec intent. It never
// writes Status and never writes a control annotation, so every command has
// exactly one write path (reconciler applies intent → Status).
func TestRolloutControlHandlerWritesSpecIntentOnly(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = sdpv1alpha1.AddToScheme(scheme)
	tr := &sdpv1alpha1.TaskRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pr-a", Namespace: "ns",
			Labels: map[string]string{"sdp.io/pipeline-run": "pr", "sdp.io/task": "a"},
		},
		Spec:   sdpv1alpha1.TaskRunSpec{PipelineRunRef: "pr", TaskName: "a", Namespace: "ns"},
		Status: sdpv1alpha1.TaskRunStatus{RolloutRef: "ro"},
	}
	ro := &sdpv1alpha1.Rollout{ObjectMeta: metav1.ObjectMeta{Name: "ro", Namespace: "ns"}}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&sdpv1alpha1.TaskRun{}, &sdpv1alpha1.Rollout{}).
		WithObjects(tr, ro).Build()

	h := &RolloutControlHandler{Client: c}
	payload, _ := json.Marshal(sdpv1alpha1.RolloutControlPayload{
		PipelineRunName: "pr", TaskName: "a",
		Action: sdpv1alpha1.RolloutActionPromote, Operator: "alice",
	})
	if err := h.Handle(payload); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	var got sdpv1alpha1.Rollout
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "ro"}, &got); err != nil {
		t.Fatalf("get rollout: %v", err)
	}
	if got.Spec.DesiredAction != sdpv1alpha1.RolloutActionPromote {
		t.Errorf("desiredAction = %q, want promote", got.Spec.DesiredAction)
	}
	if got.Spec.DesiredActionOperator != "alice" {
		t.Errorf("operator = %q, want alice", got.Spec.DesiredActionOperator)
	}
	for _, key := range []string{
		sdpv1alpha1.AnnRolloutAction, sdpv1alpha1.AnnRolloutOperator,
		sdpv1alpha1.AnnRolloutPaused, sdpv1alpha1.AnnRolloutAborted,
	} {
		if _, ok := got.Annotations[key]; ok {
			t.Errorf("handler wrote legacy control annotation %q", key)
		}
	}
}
