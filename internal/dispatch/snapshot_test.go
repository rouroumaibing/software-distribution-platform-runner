package dispatch

import (
	"testing"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

func sampleTasks() []sdpv1alpha1.PipelineTaskSpec {
	return []sdpv1alpha1.PipelineTaskSpec{
		{Name: "build", Type: sdpv1alpha1.TaskTypeBuild, Stage: "build", Image: "img:1", ScriptPath: "build.sh"},
		{Name: "test", Type: sdpv1alpha1.TaskTypeTest, Stage: "build", DependsOn: []string{"build"}},
	}
}

func TestSnapshotSpecStableAndSensitive(t *testing.T) {
	a := SnapshotSpec(sampleTasks())
	b := SnapshotSpec(sampleTasks())
	if a != b {
		t.Fatalf("snapshot not deterministic: %s vs %s", a, b)
	}
	if len(a) != 64 {
		t.Fatalf("expected sha256 hex (64 chars), got %d", len(a))
	}

	// Changing a field must change the hash.
	changed := sampleTasks()
	changed[0].ScriptPath = "build-v2.sh"
	if SnapshotSpec(changed) == a {
		t.Fatal("snapshot should change when a task field changes")
	}

	// Reordering the task array must NOT change the hash (order-independent).
	reordered := []sdpv1alpha1.PipelineTaskSpec{sampleTasks()[1], sampleTasks()[0]}
	if SnapshotSpec(reordered) != a {
		t.Fatal("snapshot should be order-independent")
	}
}

func TestSnapshotSpecEmpty(t *testing.T) {
	if SnapshotSpec(nil) == "" {
		t.Fatal("empty snapshot should still produce a hash")
	}
}
