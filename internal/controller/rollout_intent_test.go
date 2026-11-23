package controller

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

func newIntentReconciler(t *testing.T, ro *sdpv1alpha1.Rollout) (*RolloutReconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = sdpv1alpha1.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&sdpv1alpha1.Rollout{}).
		WithObjects(ro).Build()
	return &RolloutReconciler{Client: c}, c
}

func intentRollout() *sdpv1alpha1.Rollout {
	ten := int32(10)
	return &sdpv1alpha1.Rollout{
		ObjectMeta: metav1.ObjectMeta{Name: "ro", Namespace: "ns"},
		Spec: sdpv1alpha1.RolloutSpec{
			WorkloadRef:    "wl",
			Replicas:       &ten,
			StableImage:    "img:stable",
			CanaryImage:    "img:canary",
			Steps:          []sdpv1alpha1.CanaryStep{{SetWeight: &ten}},
			HealthCheck:    sdpv1alpha1.HealthCheckSpec{Type: sdpv1alpha1.HealthCheckPodReady},
			TrafficRouting: sdpv1alpha1.TrafficRoutingSpec{Type: sdpv1alpha1.TrafficRoutingDeploymentWeight},
		},
		Status: sdpv1alpha1.RolloutStatus{Phase: sdpv1alpha1.RolloutProgressing},
	}
}

// Pause is a persistent Spec intent: applying it writes Status once, leaves
// the intent in place, and a second pass is a no-op (the hold branch owns the
// paused state). No annotation is written as an intent.
func TestDesiredActionPauseSingleWritePath(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	ro.Spec.DesiredAction = sdpv1alpha1.RolloutActionPause
	ro.Spec.DesiredActionOperator = "alice"
	r, c := newIntentReconciler(t, ro)

	acted, err := r.applyDesiredAction(ctx, ro)
	if err != nil || !acted {
		t.Fatalf("first pass acted=%v err=%v, want applied", acted, err)
	}
	var got sdpv1alpha1.Rollout
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "ro"}, &got); err != nil {
		t.Fatalf("get rollout: %v", err)
	}
	if got.Status.Phase != sdpv1alpha1.RolloutPaused {
		t.Errorf("phase = %s, want Paused", got.Status.Phase)
	}
	if got.Spec.DesiredAction != sdpv1alpha1.RolloutActionPause {
		t.Errorf("pause intent must persist, got %q", got.Spec.DesiredAction)
	}
	if got.Annotations[sdpv1alpha1.AnnRolloutAction] != "" || got.Annotations[sdpv1alpha1.AnnRolloutPaused] != "" {
		t.Error("Spec-driven pause must not write control annotations")
	}
	// A second pass must not re-apply (no hot status-write loop).
	acted, err = r.applyDesiredAction(ctx, &got)
	if err != nil || acted {
		t.Errorf("second pass acted=%v err=%v, want no-op", acted, err)
	}
}

// Promote advances exactly one step and clears the one-shot Spec intent.
func TestDesiredActionPromoteSingleWritePath(t *testing.T) {
	ctx := context.Background()
	ten := int32(10)
	ro := intentRollout()
	ro.Spec.Steps = []sdpv1alpha1.CanaryStep{{SetWeight: &ten}, {SetWeight: &ten}}
	ro.Status.CurrentWeight = 10
	ro.Spec.DesiredAction = sdpv1alpha1.RolloutActionPromote
	r, c := newIntentReconciler(t, ro)

	acted, err := r.applyDesiredAction(ctx, ro)
	if err != nil || !acted {
		t.Fatalf("apply acted=%v err=%v", acted, err)
	}
	var got sdpv1alpha1.Rollout
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "ro"}, &got); err != nil {
		t.Fatalf("get rollout: %v", err)
	}
	if got.Spec.DesiredAction != "" || got.Spec.DesiredActionOperator != "" {
		t.Errorf("promote intent must be cleared, got %q/%q", got.Spec.DesiredAction, got.Spec.DesiredActionOperator)
	}
	if got.Status.CurrentStepIndex != 1 {
		t.Errorf("step index = %d, want 1", got.Status.CurrentStepIndex)
	}
	if !strings.Contains(got.Status.Message, "promoted") {
		t.Errorf("message = %q, want promoted", got.Status.Message)
	}
}

// Rollback reverts to 0% canary, marks the terminal phase, and clears the
// one-shot intent — all from the reconciler; the handler never writes Status.
func TestDesiredActionRollbackSingleWritePath(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	ro.Status.CurrentWeight = 10
	ro.Spec.DesiredAction = sdpv1alpha1.RolloutActionRollback
	r, c := newIntentReconciler(t, ro)

	acted, err := r.applyDesiredAction(ctx, ro)
	if err != nil || !acted {
		t.Fatalf("apply acted=%v err=%v", acted, err)
	}
	var got sdpv1alpha1.Rollout
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "ro"}, &got); err != nil {
		t.Fatalf("get rollout: %v", err)
	}
	if got.Spec.DesiredAction != "" {
		t.Errorf("rollback intent must be cleared, got %q", got.Spec.DesiredAction)
	}
	if got.Status.Phase != sdpv1alpha1.RolloutRolledBack {
		t.Errorf("phase = %s, want RolledBack", got.Status.Phase)
	}
	if got.Status.CurrentWeight != 0 || got.Status.CanaryReplicas != 0 {
		t.Errorf("canary weight/replicas not reverted: %d/%d", got.Status.CurrentWeight, got.Status.CanaryReplicas)
	}
	if got.Annotations[sdpv1alpha1.AnnRolloutAborted] != "" {
		t.Error("Spec-driven rollback must not write the legacy aborted annotation")
	}
}

// The compatibility helper resolves legacy annotations into the same
// vocabulary as Spec.DesiredAction, with the Spec field taking precedence.
func TestRolloutIntentFromAnnotationsCompat(t *testing.T) {
	cases := []struct {
		annotations map[string]string
		want        sdpv1alpha1.RolloutAction
	}{
		{map[string]string{sdpv1alpha1.AnnRolloutAction: "promote"}, sdpv1alpha1.RolloutActionPromote},
		{map[string]string{sdpv1alpha1.AnnRolloutAborted: "true"}, sdpv1alpha1.RolloutActionRollback},
		{map[string]string{sdpv1alpha1.AnnRolloutPaused: "true"}, sdpv1alpha1.RolloutActionPause},
		{map[string]string{}, ""},
	}
	for _, tc := range cases {
		if got := sdpv1alpha1.RolloutIntentFromAnnotations(tc.annotations); got != tc.want {
			t.Errorf("annotations %v: got %q want %q", tc.annotations, got, tc.want)
		}
	}
}

// A stale legacy annotation is cleared but never overrides the Spec intent.
func TestDesiredActionWinsOverLegacyAnnotation(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	ten := int32(10)
	ro.Spec.Steps = []sdpv1alpha1.CanaryStep{{SetWeight: &ten}, {SetWeight: &ten}}
	ro.Status.CurrentWeight = 10
	ro.Spec.DesiredAction = sdpv1alpha1.RolloutActionPromote
	ro.Annotations = map[string]string{
		sdpv1alpha1.AnnRolloutAction:   string(sdpv1alpha1.RolloutActionRollback),
		sdpv1alpha1.AnnRolloutOperator: "legacy-user",
	}
	r, c := newIntentReconciler(t, ro)

	if _, err := r.applyDesiredAction(ctx, ro); err != nil {
		t.Fatalf("apply: %v", err)
	}
	var got sdpv1alpha1.Rollout
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "ro"}, &got); err != nil {
		t.Fatalf("get rollout: %v", err)
	}
	if got.Status.Phase == sdpv1alpha1.RolloutRolledBack {
		t.Error("legacy annotation must not override the Spec promote intent")
	}
	if got.Status.CurrentStepIndex != 1 {
		t.Errorf("step index = %d, want 1 from Spec promote", got.Status.CurrentStepIndex)
	}
	if got.Annotations[sdpv1alpha1.AnnRolloutAction] != "" {
		t.Error("stale legacy action annotation should be cleared")
	}
}
