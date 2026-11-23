package dispatch

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

const (
	// SnapshotAlgV1 is the bare 7-field projection hash written by older runners.
	SnapshotAlgV1 = "v1"
	// SnapshotAlgV2 hashes the complete PipelineTaskSpec of every task.
	SnapshotAlgV2 = "v2"
	snapshotSep   = ":"
)

// SnapshotSpec produces a self-describing content hash of the resolved task DAG
// so the exact task definitions a run was triggered from are reconstructable
// later (C-03 — trigger-time snapshot 固化). Tasks are sorted by name and their
// complete specs are marshalled as deterministic JSON before hashing, so every
// declared field takes part while array order does not. Pure and unit-tested.
func SnapshotSpec(tasks []sdpv1alpha1.PipelineTaskSpec) string {
	// Sort a copy by name so the hash does not depend on array order.
	named := make([]sdpv1alpha1.PipelineTaskSpec, len(tasks))
	copy(named, tasks)
	sort.Slice(named, func(i, j int) bool { return named[i].Name < named[j].Name })

	// The full spec (not a projection) is marshalled in Go struct field
	// order, which is stable. No field is omitted, so the hash is sensitive
	// to every declared field of every task.
	b, err := json.Marshal(named)
	if err != nil {
		// Unreachable for this type tree; a deterministic fallback keeps
		// dispatch alive rather than panicking if a future field breaks
		// marshaling.
		return SnapshotAlgV2 + snapshotSep + hashBytes([]byte(fmt.Sprintf("%d", len(tasks))))
	}
	return SnapshotAlgV2 + snapshotSep + hashBytes(b)
}

func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ParseSnapshot splits a stored snapshot into its algorithm tag and digest.
// Bare 64-character hex values are v1 records; anything else must carry an
// algorithm tag. Unknown tags and malformed digests are errors so callers
// cannot mistake an unreadable snapshot for a content mismatch.
func ParseSnapshot(value string) (alg string, digest string, err error) {
	alg, digest, ok := strings.Cut(value, snapshotSep)
	if !ok {
		if isSHA256Hex(value) {
			return SnapshotAlgV1, value, nil
		}
		return "", "", fmt.Errorf("unrecognized spec snapshot %q", value)
	}
	if alg != SnapshotAlgV2 || !isSHA256Hex(digest) {
		return "", "", fmt.Errorf("unrecognized spec snapshot %q", value)
	}
	return alg, digest, nil
}

// SameSnapshot compares two snapshots only when both use the same algorithm.
// Cross-algorithm comparison is an error rather than a silent "different"
// result, because the digests encode different field sets.
func SameSnapshot(a, b string) (bool, error) {
	algA, digestA, err := ParseSnapshot(a)
	if err != nil {
		return false, err
	}
	algB, digestB, err := ParseSnapshot(b)
	if err != nil {
		return false, err
	}
	if algA != algB {
		return false, fmt.Errorf("cannot compare spec snapshots: %s and %s", algA, algB)
	}
	return digestA == digestB, nil
}

func isSHA256Hex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}
