package canary

import (
	"testing"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

func ptrI32(v int32) *int32 { return &v }

func mkRollout() *sdpv1alpha1.Rollout {
	w := int32(50)
	return &sdpv1alpha1.Rollout{
		Spec: sdpv1alpha1.RolloutSpec{
			WorkloadRef: "app",
			StableImage: "s",
			CanaryImage: "c",
			Steps:       []sdpv1alpha1.CanaryStep{{SetWeight: &w}},
			HealthCheck: sdpv1alpha1.HealthCheckSpec{
				Type:            sdpv1alpha1.HealthCheckPodReady,
				IntervalSeconds: 1,
				FailureThreshold: 3,
			},
			TrafficRouting: sdpv1alpha1.TrafficRoutingSpec{Type: sdpv1alpha1.TrafficRoutingDeploymentWeight},
			AutoRollback:    true,
		},
		Status: sdpv1alpha1.RolloutStatus{CurrentStepIndex: 0, CurrentWeight: 0},
	}
}

func TestEvaluateScalingTowardWeight(t *testing.T) {
	ro := mkRollout()
	// First step targets 50; we're at 0 and canary isn't up yet.
	d := Evaluate(ro, 0, 0, 4, true)
	if d.Phase != sdpv1alpha1.RolloutProgressing {
		t.Fatalf("expected Progressing while scaling, got %s", d.Phase)
	}
	if d.CurrentWeight != 50 {
		t.Fatalf("expected target weight 50, got %d", d.CurrentWeight)
	}
}

func TestEvaluateWaitingForPodsReady(t *testing.T) {
	ro := mkRollout()
	ro.Status.CurrentWeight = 50
	// At target weight but pods not ready: just progress, no failure count.
	d := Evaluate(ro, 0, 2, 4, false)
	if d.ConsecutiveFailures != 0 {
		t.Fatalf("pods-not-ready should not increment failures, got %d", d.ConsecutiveFailures)
	}
	if d.Phase != sdpv1alpha1.RolloutProgressing {
		t.Fatalf("expected Progressing, got %s", d.Phase)
	}
}

func TestEvaluateHealthFailureCounts(t *testing.T) {
	ro := mkRollout()
	ro.Status.CurrentWeight = 50
	// Pods ready but probe failing -> failure count increments.
	d := Evaluate(ro, 2, 2, 4, false)
	if d.ConsecutiveFailures != 1 {
		t.Fatalf("expected consecutive failures 1, got %d", d.ConsecutiveFailures)
	}
}

func TestEvaluateHealthFailureTriggersRollback(t *testing.T) {
	ro := mkRollout()
	ro.Status.CurrentWeight = 50
	ro.Status.ConsecutiveFailures = 2 // one more pushes past threshold (3)
	d := Evaluate(ro, 2, 2, 4, false)
	if d.Phase != sdpv1alpha1.RolloutRollingBack {
		t.Fatalf("expected RollingBack past threshold, got %s", d.Phase)
	}
	if d.CanaryReplicas != 0 || d.StableReplicas != 4 {
		t.Fatalf("rollback must send 0 canary / all stable: canary=%d stable=%d",
			d.CanaryReplicas, d.StableReplicas)
	}
}

func TestEvaluateHealthyAdvances(t *testing.T) {
	ro := mkRollout()
	ro.Status.CurrentWeight = 50
	d := Evaluate(ro, 2, 2, 4, true)
	if d.CurrentStepIndex != 1 {
		t.Fatalf("expected to advance to step index 1, got %d", d.CurrentStepIndex)
	}
	if d.Phase != sdpv1alpha1.RolloutProgressing {
		t.Fatalf("expected Progressing (advancing), got %s", d.Phase)
	}
}

func TestEvaluateAllStepsComplete(t *testing.T) {
	ro := mkRollout()
	ro.Status.CurrentStepIndex = 1 // already past the only step
	ro.Status.CurrentWeight = 50
	d := Evaluate(ro, 2, 2, 4, true)
	if d.Phase != sdpv1alpha1.RolloutHealthy {
		t.Fatalf("expected Healthy when steps exhausted, got %s", d.Phase)
	}
}

func TestHTTPProbeHealthCheckType(t *testing.T) {
	// A rollout using HTTPProbe: engine only advances when the probe passes.
	ro := mkRollout()
	ro.Spec.HealthCheck.Type = sdpv1alpha1.HealthCheckHTTPProbe
	ro.Spec.HealthCheck.HTTPPath = "/healthz"
	ro.Status.CurrentWeight = 50
	// Pods ready but healthy=false (probe will be called by the controller;
	// here we simulate a failing probe result).
	d := Evaluate(ro, 2, 2, 4, false)
	if d.ConsecutiveFailures != 1 {
		t.Fatalf("HTTPProbe failure should count, got %d", d.ConsecutiveFailures)
	}
}
