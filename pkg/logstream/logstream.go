// Package logstream turns a pod's raw log byte stream into the chunked
// LogChunkPayload frames the Runner ships to the Hub over the long connection
// (B-02 — live log streaming). The chunking logic is pure and unit-testable;
// the actual tailing of a pod is done by the TaskRun controller via a
// kubernetes.Interface, outside this package.
package logstream

import (
	"bufio"
	"context"
	"io"
	"strings"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// Sender ships a single log chunk to the Hub. The connector's Send satisfies
// this; a test double can capture payloads without a live connection.
type Sender interface {
	SendLogChunk(payload sdpv1alpha1.LogChunkPayload) error
}

// chunkSize is the soft per-frame cap. Lines longer than this are still sent
// whole (a single very long line shouldn't be split mid-token); multiple
// short lines are batched up to this cap.
const chunkSize = 8 * 1024

// SplitChunks reads r line-by-line and returns log chunks of at most
// chunkSize bytes, preserving line boundaries. It's the pure core of the
// streaming path and the main thing worth unit-testing.
func SplitChunks(r io.Reader, maxChunkSize int) ([]string, error) {
	if maxChunkSize <= 0 {
		maxChunkSize = chunkSize
	}
	var chunks []string
	var buf strings.Builder
	flush := func() {
		if buf.Len() > 0 {
			chunks = append(chunks, buf.String())
			buf.Reset()
		}
	}
	scanner := bufio.NewScanner(r)
	// Allow arbitrarily long lines (pod logs can exceed the default 64k).
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		// Each line is written with a trailing newline as a terminator so a
		// flush boundary always lands between lines, never mid-line.
		candidate := line + "\n"
		if buf.Len() > 0 && buf.Len()+len(candidate) > maxChunkSize {
			flush() // buf already ends with a newline from the prior line
		}
		buf.WriteString(candidate)
		if buf.Len() >= maxChunkSize {
			flush()
		}
	}
	flush()
	if err := scanner.Err(); err != nil {
		return chunks, err
	}
	return chunks, nil
}

// StreamLogs reads from rc until EOF (or ctx done) and sends each batched
// chunk as a LogChunkPayload. stream ("stdout"/"stderr") and the task
// identity come from the caller. The Send is best-effort: a send error is
// returned so the caller can stop tailing, but individual drops don't abort
// the whole run.
func StreamLogs(ctx context.Context, rc io.ReadCloser, sender Sender, p sdpv1alpha1.LogChunkPayload) error {
	defer rc.Close()
	chunks, err := SplitChunks(rc, chunkSize)
	if err != nil {
		return err
	}
	for _, c := range chunks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		p.Chunk = c
		if sendErr := sender.SendLogChunk(p); sendErr != nil {
			return sendErr
		}
	}
	return nil
}
