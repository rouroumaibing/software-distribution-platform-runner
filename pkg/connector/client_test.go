package connector

import (
	"testing"
	"time"

	runnerapi "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// Send must never park its caller: a stalled connection fills the outbox and
// every report path (reconcile status, log tail, agent ops) would otherwise
// pile up goroutines on the channel send. See features/connector-backpressure.md.
func TestSend_DropsWhenOutboxFull(t *testing.T) {
	c := New("ws://127.0.0.1:1/nope", "target-a", "token")
	c.outbox = make(chan runnerapi.Message, 2)

	fill := map[string]string{"t": "x"}
	for i := 0; i < 2; i++ {
		if err := c.Send(runnerapi.MessageHeartbeat, fill); err != nil {
			t.Fatalf("send %d into empty outbox: %v", i, err)
		}
	}
	if got := len(c.outbox); got != 2 {
		t.Fatalf("outbox len = %d, want 2", got)
	}

	done := make(chan error, 1)
	go func() { done <- c.Send(runnerapi.MessageHeartbeat, fill) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("send into full outbox returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked on a full outbox")
	}

	if got := c.dropped.Load(); got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}

	// Draining makes room again; subsequent sends must land (not drop).
	<-c.outbox
	if err := c.Send(runnerapi.MessageHeartbeat, fill); err != nil {
		t.Fatalf("send after drain: %v", err)
	}
	if got := c.dropped.Load(); got != 1 {
		t.Fatalf("dropped after drain = %d, want still 1", got)
	}
	if got := len(c.outbox); got != 2 {
		t.Fatalf("outbox len after drain+send = %d, want 2", got)
	}
}

// Marshal failures must still surface as errors and must not consume outbox
// capacity or bump the drop counter.
func TestSend_MarshalError(t *testing.T) {
	c := New("ws://127.0.0.1:1/nope", "target-a", "token")
	c.outbox = make(chan runnerapi.Message, 1)

	if err := c.Send(runnerapi.MessageHeartbeat, make(chan int)); err == nil {
		t.Fatal("expected marshal error for unsupportable payload, got nil")
	}
	if got := c.dropped.Load(); got != 0 {
		t.Fatalf("dropped = %d, want 0 (marshal failure is not a drop)", got)
	}
	if got := len(c.outbox); got != 0 {
		t.Fatalf("outbox len = %d, want 0", got)
	}
}
