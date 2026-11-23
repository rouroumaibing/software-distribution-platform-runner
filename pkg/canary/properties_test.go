package canary

import (
	"math/rand"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// Property tests with a deterministic generator; no external dependency.

// Round-trip of the legacy annotation format read by the migration path:
// FormatPauseStart then ParsePauseStart recovers index and instant.
func TestPauseStartRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(20261121))
	for i := 0; i < 600; i++ {
		idx := int32(r.Intn(1000))
		sec := int64(r.Intn(1_700_000_000))
		moment := time.Unix(sec, 0).UTC().Truncate(time.Second)
		s := FormatPauseStart(idx, moment)
		gotIdx, gotTime, ok := ParsePauseStart(s)
		if !ok {
			t.Fatalf("parse failed for %q", s)
		}
		if gotIdx != idx || !gotTime.Equal(moment) {
			t.Fatalf("round-trip mismatch: idx=%d/%d time=%v/%v", idx, gotIdx, moment, gotTime)
		}
	}
}

// Invariants: weightReplicas is bounded [0,total] and monotonic in weight.
func TestWeightReplicasInvariants(t *testing.T) {
	r := rand.New(rand.NewSource(20261122))
	for i := 0; i < 600; i++ {
		total := int32(r.Intn(200))
		// canary weight is a percentage; its valid input domain is [0,100].
		weight := int32(r.Intn(101))
		got := weightReplicas(weight, total)
		if got < 0 || got > total {
			t.Fatalf("weightReplicas(%d,%d)=%d out of [0,%d]", weight, total, got, total)
		}
		if weight == 0 && got != 0 {
			t.Fatalf("zero weight should yield 0, got %d", got)
		}
		if weight == 100 && got != total {
			t.Fatalf("weight=100 should yield total=%d, got %d", total, got)
		}
		// Monotonic: increasing weight never decreases replicas.
		if weight > 0 && weightReplicas(weight-1, total) > got {
			t.Fatalf("not monotonic at weight=%d total=%d", weight, total)
		}
	}
	// Equivalence: at the endpoints and midpoint the closed form matches exact arithmetic.
	if weightReplicas(50, 10) != 5 || weightReplicas(0, 10) != 0 || weightReplicas(100, 10) != 10 {
		t.Fatal("weightReplicas closed-form checkpoints failed")
	}
}

// A Status clock advances a timed pause only when it belongs to the current
// step, the elapsed time covers the duration, and the canary is healthy.
func TestTimedPauseStatusClockInvariants(t *testing.T) {
	r := rand.New(rand.NewSource(20261123))
	const duration = 30 * time.Second
	for i := 0; i < 600; i++ {
		idx := int32(r.Intn(5))
		elapsed := time.Duration(r.Intn(120)) * time.Second
		healthy := r.Intn(2) == 0

		ro := mkRollout()
		ro.Spec.Steps = []sdpv1alpha1.CanaryStep{
			{Pause: &sdpv1alpha1.PauseStep{Duration: &metav1.Duration{Duration: duration}}},
		}
		started := metav1.NewTime(time.Now().Add(-elapsed))
		ro.Status.PauseStartTime = &started
		ro.Status.PauseStepIndex = idx

		d := Evaluate(ro, 0, 0, 4, healthy)
		wantAdvance := idx == ro.Status.CurrentStepIndex && elapsed >= duration && healthy
		gotAdvance := d.CurrentStepIndex == 1 && d.Phase == sdpv1alpha1.RolloutProgressing
		if gotAdvance != wantAdvance {
			t.Fatalf("idx=%d elapsed=%v healthy=%v: advance=%v want %v",
				idx, elapsed, healthy, gotAdvance, wantAdvance)
		}
	}
}
