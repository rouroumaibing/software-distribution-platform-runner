package dispatch

import (
	"strings"
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
	if !strings.HasPrefix(a, SnapshotAlgV2+snapshotSep) || len(a) != len(SnapshotAlgV2)+len(snapshotSep)+64 {
		t.Fatalf("expected a versioned sha256 hex value, got %q", a)
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

// The full-spec hash reacts to every declared field, including those that a
// partial projection would leave invisible.
func TestSnapshotSpecSensitiveToPreviouslyIgnoredFields(t *testing.T) {
	base := []sdpv1alpha1.PipelineTaskSpec{{
		Name:       "build",
		Type:       sdpv1alpha1.TaskTypeBuild,
		Image:      "img:1",
		ScriptPath: "build.sh",
		ScriptArgs: []string{"beta"},
		Command:    []string{"sh"},
		Args:       []string{"v1"},
		Repo:       &sdpv1alpha1.RepoSource{URL: "https://example.invalid/r.git", Ref: "abc"},
		Produces:   []string{"build-output"},
		Consumes:   []string{"upstream"},
	}}
	baseHash := SnapshotSpec(base)
	mutations := []struct {
		name   string
		mutate func(*sdpv1alpha1.PipelineTaskSpec)
	}{
		{"ScriptArgs", func(t *sdpv1alpha1.PipelineTaskSpec) { t.ScriptArgs = []string{"gamma"} }},
		{"Produces", func(t *sdpv1alpha1.PipelineTaskSpec) { t.Produces = []string{"other"} }},
		{"Consumes", func(t *sdpv1alpha1.PipelineTaskSpec) { t.Consumes = []string{"other"} }},
		{"Command", func(t *sdpv1alpha1.PipelineTaskSpec) { t.Command = []string{"bash"} }},
		{"Args", func(t *sdpv1alpha1.PipelineTaskSpec) { t.Args = []string{"v2"} }},
		{"Repo", func(t *sdpv1alpha1.PipelineTaskSpec) { t.Repo.Ref = "def" }},
	}
	for _, tc := range mutations {
		changed := []sdpv1alpha1.PipelineTaskSpec{*base[0].DeepCopy()}
		tc.mutate(&changed[0])
		if got := SnapshotSpec(changed); got == baseHash {
			t.Errorf("changing %s did not change the snapshot hash", tc.name)
		}
	}
}

func TestParseSnapshot(t *testing.T) {
	v2 := SnapshotSpec(sampleTasks())
	alg, digest, err := ParseSnapshot(v2)
	if err != nil || alg != SnapshotAlgV2 || !isSHA256Hex(digest) {
		t.Fatalf("ParseSnapshot(v2) = %q, %q, %v", alg, digest, err)
	}

	legacy := strings.Repeat("a", 64)
	alg, digest, err = ParseSnapshot(legacy)
	if err != nil || alg != SnapshotAlgV1 || digest != legacy {
		t.Fatalf("ParseSnapshot(v1) = %q, %q, %v", alg, digest, err)
	}

	for _, bad := range []string{"", "v3:" + legacy, "v2:short", "not-a-snapshot"} {
		if _, _, err := ParseSnapshot(bad); err == nil {
			t.Errorf("ParseSnapshot(%q) should fail", bad)
		}
	}
}

func TestSameSnapshot(t *testing.T) {
	a := SnapshotSpec(sampleTasks())
	if same, err := SameSnapshot(a, a); err != nil || !same {
		t.Fatalf("SameSnapshot(same) = %t, %v", same, err)
	}
	changed := sampleTasks()
	changed[0].ScriptPath = "other.sh"
	if same, err := SameSnapshot(a, SnapshotSpec(changed)); err != nil || same {
		t.Fatalf("SameSnapshot(different) = %t, %v", same, err)
	}
	if _, err := SameSnapshot(a, strings.Repeat("b", 64)); err == nil {
		t.Fatal("comparing v2 with v1 must fail")
	}
}
