package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	runnerapi "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// claimedJob mirrors the hub claim endpoint response item: the full apply
// payload plus identifiers (coordination-design §2.6 目标态).
type claimedJob struct {
	JobID         string          `json:"job_id"`
	PipelineRunID string          `json:"pipeline_run_id"`
	Payload       json.RawMessage `json:"payload"`
}

type claimResponse struct {
	Jobs []claimedJob `json:"jobs"`
}

// RunClaimPoller periodically pulls pending dispatch jobs for this target via
// the hub's HTTP claim endpoint (POST {gateway path}/claims) and feeds each
// payload into the locally registered apply handler. It is the pull track of
// the dual-track delivery: the WS push stays authoritative for instant
// delivery, the poller recovers jobs whose push failed or whose notification
// never arrived (e.g. the runner was offline). Claim occupancy is settled
// atomically hub-side, so the two tracks never hand out the same job twice.
//
// A zero/negative interval disables the poller.
func (c *Client) RunClaimPoller(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := c.ClaimOnce(ctx)
			if err != nil {
				log.Printf("connector: claim poll failed: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("connector: claimed %d job(s) via HTTP pull", n)
			}
		}
	}
}

// ClaimOnce performs one claim round and applies every returned payload.
// It must not touch the hub when the apply handler is not wired yet: the hub
// marks a job dispatched as part of a successful claim, so claiming before
// the consumer is ready would drop the job.
func (c *Client) ClaimOnce(ctx context.Context) (int, error) {
	apply, ok := c.handlers[runnerapi.MessageApplyPipelineRun]
	if !ok {
		return 0, nil
	}

	body, err := json.Marshal(map[string]string{"target": c.targetName})
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.hubURL+"/claims", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	// Same identity headers as the WS dial: per-target credential wins after
	// the enroll handshake; legacy deployments keep the shared token.
	req.Header.Set("Authorization", "Bearer "+c.currentToken())
	req.Header.Set("X-Target-Name", c.targetName)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusNotFound {
		// 401: per-target credential not issued yet (pre-enroll cold start) —
		// the WS handshake will produce it; retry on the next tick.
		return 0, nil
	}
	if resp.StatusCode != http.StatusOK {
		return 0, errUnexpectedStatus(resp.StatusCode)
	}
	var parsed claimResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return 0, err
	}
	for i := range parsed.Jobs {
		if err := apply(parsed.Jobs[i].Payload); err != nil {
			log.Printf("connector: apply claimed job %s failed: %v", parsed.Jobs[i].JobID, err)
			continue
		}
	}
	return len(parsed.Jobs), nil
}

type statusError struct{ code int }

func (e *statusError) Error() string {
	return "hub claim endpoint returned " + http.StatusText(e.code)
}

func errUnexpectedStatus(code int) error { return &statusError{code: code} }
