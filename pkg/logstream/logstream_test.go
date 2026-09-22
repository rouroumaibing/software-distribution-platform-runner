package logstream

import (
	"strings"
	"testing"
)

func TestSplitChunksRespectsLineBoundaries(t *testing.T) {
	// 3 short lines well under the chunk cap -> 1 chunk.
	in := "line one\nline two\nline three\n"
	chunks, err := SplitChunks(strings.NewReader(in), chunkSize)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d: %v", len(chunks), chunks)
	}
	if chunks[0] != in {
		t.Fatalf("chunk lost content: %q", chunks[0])
	}
}

func TestSplitChunksBatchesUpToCap(t *testing.T) {
	// Many small lines; with a tiny cap they must be split but never broken
	// mid-line.
	const capSize = 20
	var sb strings.Builder
	for i := 0; i < 10; i++ {
		sb.WriteString("0123456789\n") // 11 bytes each
	}
	chunks, err := SplitChunks(strings.NewReader(sb.String()), capSize)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks with cap %d, got %d", capSize, len(chunks))
	}
	for i, c := range chunks {
		if len(c) > capSize {
			t.Fatalf("chunk %d exceeds cap: %d > %d (%q)", i, len(c), capSize, c)
		}
		if !strings.HasSuffix(c, "\n") {
			t.Fatalf("chunk %d breaks a line (no trailing newline): %q", i, c)
		}
	}
}

func TestSplitChunksEmpty(t *testing.T) {
	chunks, err := SplitChunks(strings.NewReader(""), chunkSize)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) != 0 {
		t.Fatalf("expected 0 chunks for empty input, got %d", len(chunks))
	}
}
