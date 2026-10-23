// Package connector implements the Runner side of the Hub-Spoke long
// connection: an outbound-only WebSocket client that lets this cluster's
// Agent receive dispatched work from the Hub and stream status/logs back,
// without ever exposing this cluster's K8s API server to the Hub.
package connector

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	runnerapi "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// The wire protocol lives in the shared api/v1alpha1 package so the Hub and
// Runner agree on one message format. We re-export the types/constants here
// so existing callers (e.g. cmd/runner/main.go) keep compiling unchanged.
type MessageType = runnerapi.MessageType
type Message = runnerapi.Message

var (
	MessageApplyPipelineRun  = runnerapi.MessageApplyPipelineRun
	MessageApproveTask       = runnerapi.MessageApproveTask
	MessageRolloutControl    = runnerapi.MessageRolloutControl
	MessageRerunTask         = runnerapi.MessageRerunTask
	MessageCancelPipelineRun = runnerapi.MessageCancelPipelineRun
	MessageAgentOp           = runnerapi.MessageAgentOp
	MessageStatusUpdate      = runnerapi.MessageStatusUpdate
	MessageLogChunk          = runnerapi.MessageLogChunk
	MessageHeartbeat         = runnerapi.MessageHeartbeat
	MessageAgentOpStatus     = runnerapi.MessageAgentOpStatus
	MessageAgentOpLog        = runnerapi.MessageAgentOpLog
	MessageAgentInfo         = runnerapi.MessageAgentInfo
)

// Handler processes an inbound message from the Hub. Registered per
// MessageType by whatever wires this package up in cmd/runner/main.go —
// typically the PipelineRun controller for MessageApplyPipelineRun.
type Handler func(payload json.RawMessage) error

// OnConnectFunc is invoked once per (re)connection, immediately after the
// WebSocket is established. The Runner uses it to resync local state with
// the Hub (re-send status for in-flight runs) so work isn't silently lost
// across a connection blip (C-05).
type OnConnectFunc func()

// Client maintains a single outbound connection to the Hub's gateway,
// reconnecting with backoff on any failure. Every reconnect is followed by
// an OnConnect callback so the Runner can resync its state (C-05).
type Client struct {
	hubURL     string
	targetName string
	authToken  string // Legacy shared gateway token (dev / pre-migration deployments).
	// Per-target bootstrap (STEELMAN-ADR2-LEFTOVERS §7): the one-time enroll
	// token from the join bundle is traded for a per-target credential on the
	// first dial; the credential persists at credentialFile and wins over the
	// enroll token and the legacy token thereafter.
	enrollToken    string
	credentialFile string

	// Enrollment attributes (ADR-0001 bootstrap-on-first-connect): when both
	// are non-empty they ride every dial as X-Target-Vendor/X-Target-Region,
	// letting the Hub auto-create a not-yet-registered target. Empty means
	// "plain connect" — an unknown target then fails loudly with 404.
	vendor string
	region string

	handlers  map[runnerapi.MessageType]Handler
	onConnect OnConnectFunc

	// agentInfo is the one-shot identity frame sent right after every
	// successful (re)connect (RUNNER-REFLUX-SPEC §5). Nil means "old runner
	// behavior" — no agent_info frame is emitted.
	agentInfo *runnerapi.AgentInfoPayload

	conn   *websocket.Conn
	outbox chan Message
}

func New(hubURL, targetName, authToken string) *Client {
	return &Client{
		hubURL:     hubURL,
		targetName: targetName,
		authToken:  authToken,
		handlers:   make(map[MessageType]Handler),
		outbox:     make(chan Message, 256),
	}
}

// WithEnrollInfo sets the target registry attributes carried on every dial
// (hub auto-enrolls an unknown target that presents them alongside a valid
// gateway token). Returns the receiver for chaining at the call site.
func (c *Client) WithEnrollInfo(vendor, region string) *Client {
	c.vendor, c.region = vendor, region
	return c
}

// WithEnrollBootstrap wires one-time enroll-token bootstrap
// (STEELMAN-ADR2-LEFTOVERS §7 runner state): on the first dial the enroll
// token (from the join bundle) rides as the bearer; the hub consumes it and
// returns the per-target credential on the 101 response header
// (X-SDP-Runner-Credential), which the connector persists at credentialFile
// (0600) and prefers for every later dial. Empty enrollToken or empty
// credentialFile disables the bootstrap path (legacy shared-token mode).
func (c *Client) WithEnrollBootstrap(enrollToken, credentialFile string) *Client {
	c.enrollToken = enrollToken
	c.credentialFile = credentialFile
	return c
}

// currentToken picks the bearer for this dial: persisted per-target
// credential > one-time enroll token > legacy shared token.
func (c *Client) currentToken() string {
	if c.credentialFile != "" {
		if b, err := os.ReadFile(c.credentialFile); err == nil {
			if tok := strings.TrimSpace(string(b)); tok != "" {
				return tok
			}
		}
	}
	if c.enrollToken != "" {
		return c.enrollToken
	}
	return c.authToken
}

// persistCredential stores the hub-issued per-target credential for all
// future dials. Best effort, but a failure is loud: the enroll token is
// single-use and already burned, so without the file the next dial would 401.
func (c *Client) persistCredential(cred string) {
	if c.credentialFile == "" || cred == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(c.credentialFile), 0o700); err != nil {
		log.Printf("connector: WARNING cannot create %s: %v (enroll token already burned)", filepath.Dir(c.credentialFile), err)
		return
	}
	if err := os.WriteFile(c.credentialFile, []byte(cred+"\n"), 0o600); err != nil {
		log.Printf("connector: WARNING cannot store per-target credential at %s: %v (enroll token already burned)", c.credentialFile, err)
		return
	}
	log.Printf("connector: per-target credential stored at %s", c.credentialFile)
}

// WithAgentInfo sets the identity frame (RUNNER-REFLUX-SPEC §5) emitted once
// per (re)connect. cmd/runner/main.go fills AgentVersion from ldflags; OS /
// Arch / GoVersion default from runtime here when left empty.
func (c *Client) WithAgentInfo(info runnerapi.AgentInfoPayload) *Client {
	if info.OS == "" {
		info.OS = runtime.GOOS
	}
	if info.Arch == "" {
		info.Arch = runtime.GOARCH
	}
	if info.GoVersion == "" {
		info.GoVersion = runtime.Version()
	}
	c.agentInfo = &info
	return c
}

// OnMessage registers a handler for an inbound message type.
func (c *Client) OnMessage(t runnerapi.MessageType, h Handler) {
	c.handlers[t] = h
}

// OnConnect registers a callback fired on every successful (re)connection.
// The hub already runs DrainTarget on connect; this is the Runner-side
// counterpart that re-asserts local state upstream.
func (c *Client) OnConnect(fn OnConnectFunc) {
	c.onConnect = fn
}

// Send queues a message for delivery to the Hub; safe to call from
// multiple goroutines (e.g. several TaskRun reconciles reporting status
// concurrently).
func (c *Client) Send(t runnerapi.MessageType, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	c.outbox <- runnerapi.Message{Type: t, Payload: raw}
	return nil
}

// Run blocks until ctx is cancelled, maintaining the connection and
// reconnecting with exponential backoff (capped at 30s) on any failure.
// Intended to be started as a goroutine from cmd/runner/main.go, running
// alongside the controller-runtime manager.
func (c *Client) Run(ctx context.Context) {
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := c.connectAndServe(ctx); err != nil {
			log.Printf("connector: connection lost: %v (retrying in %s)", err, backoff)
			time.Sleep(backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
}

func (c *Client) connectAndServe(ctx context.Context) error {
	// Bearer resolved per dial: after the first enroll-token handshake the
	// persisted per-target credential wins; before it, the enroll token
	// itself; legacy deployments keep the shared token.
	header := map[string][]string{
		"Authorization": {"Bearer " + c.currentToken()},
		"X-Target-Name": {c.targetName},
	}
	if c.vendor != "" && c.region != "" {
		header["X-Target-Vendor"] = []string{c.vendor}
		header["X-Target-Region"] = []string{c.region}
	}
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, c.hubURL, header)
	if err != nil {
		return err
	}
	// Enroll-token handshake: the hub consumed the one-time token and hands
	// back the per-target credential on the 101 response; store it so every
	// later dial (reconnects here, cold starts elsewhere) uses it.
	if resp != nil {
		c.persistCredential(resp.Header.Get("X-SDP-Runner-Credential"))
	}
	defer conn.Close()
	c.conn = conn

	// Identity frame first (RUNNER-REFLUX-SPEC §5): the hub records
	// agent_version before any resync traffic, so an upgrade followed by a
	// reconnect reports the new version as the very first statement this
	// binary makes.
	if c.agentInfo != nil {
		if err := c.Send(MessageAgentInfo, *c.agentInfo); err != nil {
			log.Printf("connector: agent_info send failed: %v", err)
		}
	}

	// We just (re)established the long connection. Fire the resync hook so
	// the Runner re-asserts its local state with the Hub (C-05). The hub
	// side runs DrainTarget on connect; this is the Runner counterpart.
	if c.onConnect != nil {
		c.onConnect()
	}

	readErr := make(chan error, 1)
	go c.readLoop(conn, readErr)

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-readErr:
			return err
		case <-heartbeat.C:
			_ = c.Send(MessageHeartbeat, map[string]string{"target": c.targetName})
		case msg := <-c.outbox:
			if err := conn.WriteJSON(msg); err != nil {
				return err
			}
		}
	}
}

func (c *Client) readLoop(conn *websocket.Conn, errCh chan<- error) {
	for {
		var msg runnerapi.Message
		if err := conn.ReadJSON(&msg); err != nil {
			log.Printf("connector: readLoop error: %v", err)
			errCh <- err
			return
		}
		handler, ok := c.handlers[msg.Type]
		if !ok {
			log.Printf("connector: no handler registered for message type %q", msg.Type)
			continue
		}
		if err := handler(msg.Payload); err != nil {
			log.Printf("connector: handler for %q failed: %v", msg.Type, err)
		}
	}
}
