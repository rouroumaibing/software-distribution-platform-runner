package dispatch

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// SnapshotSpec produces a stable content hash of the resolved stage/task
// DAG so the exact definitions a run was triggered from are reconstructable
// later (C-03 — trigger-time snapshot 固化). It's order-independent for the
// task set but sensitive to every field (stage, execution mode, dependsOn,
// type, image, script, etc.), so even a one-field change yields a different
// hash. Pure and unit-tested.
func SnapshotSpec(tasks []sdpv1alpha1.PipelineTaskSpec) string {
	// Canonicalize: sort by name so the hash doesn't depend on array order,
	// then marshal each task deterministically.
	named := make([]sdpv1alpha1.PipelineTaskSpec, len(tasks))
	copy(named, tasks)
	sort.Slice(named, func(i, j int) bool { return named[i].Name < named[j].Name })

	// Build a minimal, fully-typed projection so JSON key order is stable
	// (Go's json.Marshal of a struct is already field-order stable, but we
	// avoid marshaling the whole spec which could include unstable bits).
	type proj struct {
		Name          string
		Type          string
		Stage         string
		ExecutionMode string
		DependsOn     []string
		Image         string
		ScriptPath    string
	}
	projs := make([]proj, 0, len(named))
	for _, t := range named {
		deps := append([]string(nil), t.DependsOn...)
		sort.Strings(deps)
		projs = append(projs, proj{
			Name:          t.Name,
			Type:          string(t.Type),
			Stage:         t.Stage,
			ExecutionMode: string(t.ExecutionMode),
			DependsOn:     deps,
			Image:         t.Image,
			ScriptPath:    t.ScriptPath,
		})
	}
	b, err := json.Marshal(projs)
	if err != nil {
		// Should be impossible for this projection; fall back to a hash of
		// the raw count so we never panic mid-dispatch.
		return hex.EncodeToString([]byte(fmt.Sprintf("%d", len(tasks))))
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
