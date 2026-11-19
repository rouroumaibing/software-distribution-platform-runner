package canary

import (
	"math/rand"
	"testing"
	"time"
)

// Property tests with a deterministic generator; no external dependency.

// Round-trip: FormatPauseStart then ParsePauseStart recovers index and instant.
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
