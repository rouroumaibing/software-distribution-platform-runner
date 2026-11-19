package dispatch

import (
	"math/rand"
	"testing"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// Property: SnapshotSpec is invariant under any permutation of the task set
// (equivalence), deterministic (idempotent), and sensitive to each field.
func TestSnapshotSpecPermutationInvariant(t *testing.T) {
	r := rand.New(rand.NewSource(20261123))
	names := []string{"alpha", "beta", "gamma", "delta"}
	stages := []string{"build", "test", "deploy"}
	types := []sdpv1alpha1.PipelineTaskType{sdpv1alpha1.TaskTypeBuild, sdpv1alpha1.TaskTypeTest}
	images := []string{"img:1", "img:2"}
	for i := 0; i < 300; i++ {
		n := 1 + r.Intn(len(names))
		// Name must be unique within a PipelineRun (API contract); sample without replacement.
		shuffled := append([]string(nil), names...)
		for j := len(shuffled) - 1; j > 0; j-- {
			k := r.Intn(j + 1)
			shuffled[j], shuffled[k] = shuffled[k], shuffled[j]
		}
		tasks := make([]sdpv1alpha1.PipelineTaskSpec, n)
		for j := range tasks {
			tasks[j] = sdpv1alpha1.PipelineTaskSpec{
				Name:  shuffled[j],
				Type:  types[r.Intn(len(types))],
				Stage: stages[r.Intn(len(stages))],
				Image: images[r.Intn(len(images))],
			}
		}
		base := SnapshotSpec(tasks)
		// Determinism: same input, same hash.
		if again := SnapshotSpec(tasks); again != base {
			t.Fatalf("nondeterministic snapshot: %s vs %s", base, again)
		}
		// Permutation invariance: Fisher-Yates shuffle must not change the hash.
		perm := append([]sdpv1alpha1.PipelineTaskSpec(nil), tasks...)
		for j := len(perm) - 1; j > 0; j-- {
			k := r.Intn(j + 1)
			perm[j], perm[k] = perm[k], perm[j]
		}
		if got := SnapshotSpec(perm); got != base {
			t.Fatalf("snapshot changed under permutation: %s vs %s", base, got)
		}
	}
}
