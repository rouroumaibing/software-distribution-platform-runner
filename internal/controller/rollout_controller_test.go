package controller

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/canary"
)

func TestResolveTotalReplicas(t *testing.T) {
	tests := []struct {
		name string
		spec *int32
		live *int32
		want int32
	}{
		{"spec wins", pi32(5), pi32(3), 5},
		{"live when no spec", nil, pi32(4), 4},
		{"default when neither", nil, nil, defaultRolloutReplicas},
		{"spec over live", pi32(7), pi32(2), 7},
	}
	for _, tc := range tests {
		got := resolveTotalReplicas(tc.spec, tc.live)
		if got != tc.want {
			t.Errorf("%s: resolveTotalReplicas=%d want %d", tc.name, got, tc.want)
		}
	}
}

func TestResolveServiceTargetPort(t *testing.T) {
	depWithPort := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{
		Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Ports: []corev1.ContainerPort{{ContainerPort: 80}}}},
		}}},
	}
	depNoPort := &appsv1.Deployment{}
	tests := []struct {
		name    string
		stable  *appsv1.Deployment
		envPort int32
		want    int32
	}{
		{"env overrides all", depWithPort, 9090, 9090},
		{"infer from stable container port", depWithPort, 0, 80},
		{"fallback when no declared port", depNoPort, 0, 8080},
		{"fallback when stable nil", nil, 0, 8080},
	}
	for _, tc := range tests {
		if got := resolveServiceTargetPort(tc.stable, tc.envPort); got != tc.want {
			t.Errorf("%s: resolveServiceTargetPort=%d want %d", tc.name, got, tc.want)
		}
	}
}

func pi32(v int32) *int32 { return &v }

func timedPauseRollout() *sdpv1alpha1.Rollout {
	ro := intentRollout()
	ro.Spec.Steps = []sdpv1alpha1.CanaryStep{
		{SetWeight: pi32(10)},
		{Pause: &sdpv1alpha1.PauseStep{Duration: &metav1.Duration{Duration: time.Minute}}},
	}
	ro.Status.CurrentStepIndex = 1
	ro.Status.CurrentWeight = 10
	return ro
}

func reconcileRollout(t *testing.T, r *RolloutReconciler) {
	t.Helper()
	key := client.ObjectKey{Namespace: "ns", Name: "ro"}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func getReconciledRollout(t *testing.T, c client.Client) sdpv1alpha1.Rollout {
	t.Helper()
	var got sdpv1alpha1.Rollout
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "ro"}, &got); err != nil {
		t.Fatalf("get rollout: %v", err)
	}
	return got
}

func TestTimedPauseStampsStatusClock(t *testing.T) {
	ro := timedPauseRollout()
	r, c := newIntentReconciler(t, ro)

	key := client.ObjectKey{Namespace: "ns", Name: "ro"}
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != time.Minute {
		t.Errorf("RequeueAfter = %v, want 1m", res.RequeueAfter)
	}
	got := getReconciledRollout(t, c)
	if got.Status.PauseStartTime == nil {
		t.Fatal("PauseStartTime not written to Status")
	}
	if got.Status.PauseStepIndex != 1 {
		t.Errorf("PauseStepIndex = %d, want 1", got.Status.PauseStepIndex)
	}
	if got.Annotations[canary.PauseStartAnnotation] != "" {
		t.Errorf("pause timing must not be written to annotations, got %q", got.Annotations[canary.PauseStartAnnotation])
	}
}

func TestTimedPauseMigratesLegacyAnnotation(t *testing.T) {
	legacy := time.Now().UTC().Truncate(time.Second).Add(-30 * time.Second)
	ro := timedPauseRollout()
	ro.Annotations = map[string]string{
		canary.PauseStartAnnotation: canary.FormatPauseStart(1, legacy),
	}
	r, c := newIntentReconciler(t, ro)

	reconcileRollout(t, r)
	got := getReconciledRollout(t, c)
	if got.Status.PauseStartTime == nil {
		t.Fatal("legacy annotation not migrated into Status")
	}
	if !got.Status.PauseStartTime.Time.Equal(legacy) {
		t.Errorf("migrated time = %v, want %v", got.Status.PauseStartTime.Time, legacy)
	}
	if got.Status.PauseStepIndex != 1 {
		t.Errorf("PauseStepIndex = %d, want 1", got.Status.PauseStepIndex)
	}
	if _, ok := got.Annotations[canary.PauseStartAnnotation]; ok {
		t.Error("legacy annotation must be removed after migration")
	}
}

func TestTimedPauseMismatchedAnnotationNotMigrated(t *testing.T) {
	legacy := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	ro := timedPauseRollout()
	ro.Annotations = map[string]string{
		canary.PauseStartAnnotation: canary.FormatPauseStart(0, legacy),
	}
	r, c := newIntentReconciler(t, ro)

	reconcileRollout(t, r)
	got := getReconciledRollout(t, c)
	if got.Status.PauseStartTime == nil {
		t.Fatal("fresh Status clock not stamped")
	}
	if got.Status.PauseStartTime.Time.Equal(legacy) {
		t.Error("annotation for a different step index must not be migrated")
	}
	if got.Status.PauseStepIndex != 1 {
		t.Errorf("PauseStepIndex = %d, want 1", got.Status.PauseStepIndex)
	}
}

func TestLeavingTimedPauseClearsStatusClock(t *testing.T) {
	ro := timedPauseRollout()
	ro.Spec.Steps = []sdpv1alpha1.CanaryStep{
		{SetWeight: pi32(10)},
		{SetWeight: pi32(20)},
	}
	stale := metav1.Now()
	ro.Status.PauseStartTime = &stale
	ro.Status.PauseStepIndex = 0
	r, c := newIntentReconciler(t, ro)

	reconcileRollout(t, r)
	got := getReconciledRollout(t, c)
	if got.Status.PauseStartTime != nil || got.Status.PauseStepIndex != 0 {
		t.Errorf("stale clock not cleared: %v/%d", got.Status.PauseStartTime, got.Status.PauseStepIndex)
	}
}

func TestPromoteAndRollbackClearPauseTiming(t *testing.T) {
	ctx := context.Background()
	for _, action := range []sdpv1alpha1.RolloutAction{
		sdpv1alpha1.RolloutActionPromote,
		sdpv1alpha1.RolloutActionRollback,
	} {
		ro := intentRollout()
		ro.Spec.Steps = []sdpv1alpha1.CanaryStep{{SetWeight: pi32(10)}, {SetWeight: pi32(10)}}
		ro.Status.CurrentWeight = 10
		now := metav1.Now()
		ro.Status.PauseStartTime = &now
		ro.Status.PauseStepIndex = 0
		ro.Spec.DesiredAction = action
		r, c := newIntentReconciler(t, ro)

		if _, err := r.applyDesiredAction(ctx, ro); err != nil {
			t.Fatalf("%s: apply: %v", action, err)
		}
		got := getReconciledRollout(t, c)
		if got.Status.PauseStartTime != nil || got.Status.PauseStepIndex != 0 {
			t.Errorf("%s: pause timing not cleared: %v/%d", action, got.Status.PauseStartTime, got.Status.PauseStepIndex)
		}
	}
}
