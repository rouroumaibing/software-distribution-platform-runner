// Package canary holds the pure, K8s-free decision logic for progressive
// delivery. The rollout controller feeds it the current Rollout state plus
// the live canary Deployment replica counts, and applies whatever Decision
// comes back (scale Deployments, patch Ingress, persist status). Keeping
// this logic dependency-free makes it trivially unit-testable.
package canary

import (
	"time"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// Decision is the controller-facing result of Evaluate. The controller
// applies it: writes Rollout.Status, scales the stable/canary Deployments
// (DeploymentWeight routing) or patches the Ingress (IngressCanary).
type Decision struct {
	Phase               sdpv1alpha1.RolloutPhase
	CurrentStepIndex    int32
	CurrentWeight       int32
	CanaryReplicas      int32
	StableReplicas      int32
	ConsecutiveFailures int32
	RequeueAfter        time.Duration
	Message             string
}

// Evaluate computes the next Decision for a Rollout.
//
//	canaryReady   = canary Deployment's Status.AvailableReplicas
//	canaryDesired = canary Deployment's Spec.Replicas (what we last scaled to)
//	totalReplicas = desired replica count of the stable workload
//	healthy       = whether the canary passes its HealthCheck at the current
//	                weight. The controller computes this: for PodReady it's
//	                just (canaryReady >= canaryDesired); for HTTPProbe /
//	                PrometheusQuery it's that AND the active probe result.
//
// The engine walks the canary Steps: first scale to a step's weight, wait for
// the canary pods to become Ready (and healthy), then advance. Pause steps
// hold at the current weight. AutoRollback reverts to 0% canary weight when
// the health probe fails past the threshold (B-05 — real health checks
// instead of only PodReady).
func Evaluate(ro *sdpv1alpha1.Rollout, canaryReady, canaryDesired, totalReplicas int32, healthy bool) Decision {
	spec := ro.Spec
	status := ro.Status

	// All steps consumed: the rollout is complete. Keep the last weight so
	// the canary stays at whatever the final step set.
	if status.CurrentStepIndex >= int32(len(spec.Steps)) {
		return Decision{
			Phase:            sdpv1alpha1.RolloutHealthy,
			CurrentStepIndex: status.CurrentStepIndex,
			CurrentWeight:    status.CurrentWeight,
			CanaryReplicas:   weightReplicas(status.CurrentWeight, totalReplicas),
			StableReplicas:   totalReplicas - weightReplicas(status.CurrentWeight, totalReplicas),
			Message:          "all steps complete",
		}
	}

	step := spec.Steps[status.CurrentStepIndex]

	// Pause step: hold at the current weight and (optionally) requeue after
	// the configured duration. An indefinite pause waits for an external
	// resume (not implemented in M1) — it just stays Paused.
	if step.Pause != nil {
		d := Decision{
			Phase:            sdpv1alpha1.RolloutPaused,
			CurrentStepIndex: status.CurrentStepIndex,
			CurrentWeight:    status.CurrentWeight,
			CanaryReplicas:   weightReplicas(status.CurrentWeight, totalReplicas),
			StableReplicas:   totalReplicas - weightReplicas(status.CurrentWeight, totalReplicas),
			Message:          "paused at step",
		}
		if step.Pause.Duration != nil {
			d.RequeueAfter = step.Pause.Duration.Duration
		}
		return d
	}

	// Weight step. A malformed step (no setWeight, no pause) is skipped.
	if step.SetWeight == nil {
		return Decision{
			Phase:            sdpv1alpha1.RolloutProgressing,
			CurrentStepIndex: status.CurrentStepIndex + 1,
			CurrentWeight:    status.CurrentWeight,
			CanaryReplicas:   weightReplicas(status.CurrentWeight, totalReplicas),
			StableReplicas:   totalReplicas - weightReplicas(status.CurrentWeight, totalReplicas),
			Message:          "step had no setWeight/pause, skipping",
		}
	}

	target := *step.SetWeight

	// Still scaling toward the step's target weight — don't judge health
	// until the canary is actually at that weight.
	if status.CurrentWeight != target {
		return Decision{
			Phase:            sdpv1alpha1.RolloutProgressing,
			CurrentStepIndex: status.CurrentStepIndex,
			CurrentWeight:    target,
			CanaryReplicas:   weightReplicas(target, totalReplicas),
			StableReplicas:   totalReplicas - weightReplicas(target, totalReplicas),
			RequeueAfter:     healthInterval(spec),
			Message:          "scaling to weight",
		}
	}

	// At the target weight: wait for the canary pods to become Ready AND
	// pass the health check before declaring the step healthy.
	ready := canaryDesired > 0 && canaryReady >= canaryDesired && healthy
	if !ready {
		// Distinguish "pods still coming up" (just progress) from a real
		// health-check failure (pods are up but the probe failed). Only the
		// latter counts toward ConsecutiveFailures / the rollback threshold,
		// so a slow-to-start canary isn't rolled back mid-scale (B-05).
		cf := status.ConsecutiveFailures
		podsReady := canaryDesired > 0 && canaryReady >= canaryDesired
		if podsReady && !healthy {
			cf++
		}
		if ro.Spec.AutoRollback && spec.HealthCheck.FailureThreshold > 0 && cf >= spec.HealthCheck.FailureThreshold {
			d := RollbackDecision(ro, totalReplicas)
			d.CanaryReplicas = 0
			d.StableReplicas = totalReplicas
			d.Message = "health check failed past threshold, rolling back"
			return d
		}
		return Decision{
			Phase:               sdpv1alpha1.RolloutProgressing,
			CurrentStepIndex:    status.CurrentStepIndex,
			CurrentWeight:       target,
			CanaryReplicas:      weightReplicas(target, totalReplicas),
			StableReplicas:      totalReplicas - weightReplicas(target, totalReplicas),
			ConsecutiveFailures: cf,
			RequeueAfter:        healthInterval(spec),
			Message:             "waiting for canary health",
		}
	}

	// Healthy at this weight: advance to the next step, pre-setting
	// CurrentWeight to the next step's target so the controller scales to it.
	nextWeight := target
	if status.CurrentStepIndex+1 < int32(len(spec.Steps)) {
		if nxt := spec.Steps[status.CurrentStepIndex+1]; nxt.SetWeight != nil {
			nextWeight = *nxt.SetWeight
		}
	}
	return Decision{
		Phase:               sdpv1alpha1.RolloutProgressing,
		CurrentStepIndex:    status.CurrentStepIndex + 1,
		CurrentWeight:       nextWeight,
		CanaryReplicas:      weightReplicas(nextWeight, totalReplicas),
		StableReplicas:      totalReplicas - weightReplicas(nextWeight, totalReplicas),
		ConsecutiveFailures: 0,
		RequeueAfter:        healthInterval(spec),
		Message:             "healthy at weight, advancing",
	}
}

// RollbackDecision builds the Decision used when a health probe fails past
// the threshold: revert to 0% canary weight (stable takes all traffic).
func RollbackDecision(ro *sdpv1alpha1.Rollout, totalReplicas int32) Decision {
	return Decision{
		Phase:               sdpv1alpha1.RolloutRollingBack,
		CurrentStepIndex:    ro.Status.CurrentStepIndex,
		CurrentWeight:       0,
		CanaryReplicas:      0,
		StableReplicas:      totalReplicas,
		ConsecutiveFailures: ro.Status.ConsecutiveFailures + 1,
		Message:             "health check failed, rolling back",
	}
}

func weightReplicas(weight, total int32) int32 {
	if total <= 0 {
		return 0
	}
	return int32((int64(weight) * int64(total)) / 100)
}

func healthInterval(spec sdpv1alpha1.RolloutSpec) time.Duration {
	if spec.HealthCheck.IntervalSeconds <= 0 {
		return 30 * time.Second
	}
	return time.Duration(spec.HealthCheck.IntervalSeconds) * time.Second
}
