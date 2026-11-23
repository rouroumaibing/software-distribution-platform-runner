package agentops

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// upgradeJobName returns a fixed-length, deterministic Job-name suffix for an
// op. Slicing the raw OpID panics on short IDs; hashing first yields 8 hex
// characters for any non-empty ID, and the hash keeps the same op mapping to
// the same Job across redeliveries.
func upgradeJobName(opID string) (string, error) {
	if opID == "" {
		return "", fmt.Errorf("agentops: empty opID has no upgrade job name")
	}
	sum := sha256.Sum256([]byte(opID))
	return hex.EncodeToString(sum[:])[:8], nil
}
