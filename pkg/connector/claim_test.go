package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	runnerapi "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// hub claim 占坑在 hub 侧原子完成 ⇒ 一次成功 claim 的 payload 必须喂给本地
// apply handler；handler 未就绪时绝不触 hub（否则 job 被标记 dispatched 却
// 无人消费）。
func TestClaimOnceAppliesPayloadToRegisteredHandler(t *testing.T) {
	var hits int32
	payload := map[string]any{"Name": "pr-x", "Namespace": "sdp-run"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		require.Equal(t, "/gateway/ws/claims", r.URL.Path)
		require.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		require.Equal(t, "t-1", r.Header.Get("X-Target-Name"))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(claimResponse{Jobs: []claimedJob{{
			JobID:         "job-1",
			PipelineRunID: "run-1",
			Payload:       mustJSON(t, payload),
		}}})
	}))
	defer srv.Close()

	c := New(srv.URL+"/gateway/ws", "t-1", "test-token")
	var applied int32
	c.OnMessage(runnerapi.MessageApplyPipelineRun, func(p json.RawMessage) error {
		atomic.AddInt32(&applied, 1)
		var got map[string]any
		require.NoError(t, json.Unmarshal(p, &got))
		require.Equal(t, "pr-x", got["Name"])
		return nil
	})

	n, err := c.ClaimOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, int32(1), atomic.LoadInt32(&applied))
	require.Equal(t, int32(1), atomic.LoadInt32(&hits))
}

func TestClaimOnceSkipsHubWhenHandlerMissing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("hub must not be hit before the apply handler is registered")
	}))
	defer srv.Close()

	c := New(srv.URL+"/gateway/ws", "t-1", "test-token")
	n, err := c.ClaimOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

func TestClaimOnceToleratesUnauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := New(srv.URL+"/gateway/ws", "t-1", "test-token")
	c.OnMessage(runnerapi.MessageApplyPipelineRun, func(p json.RawMessage) error { return nil })
	n, err := c.ClaimOnce(context.Background())
	require.NoError(t, err, "401 = 凭据未换发（enroll 前），静默等下一轮")
	require.Equal(t, 0, n)
}

func TestRunClaimPollerDisabledOnZeroInterval(t *testing.T) {
	c := New("http://unused", "t-1", "test-token")
	done := make(chan struct{})
	go func() {
		c.RunClaimPoller(context.Background(), 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("zero interval must disable the poller immediately")
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}
