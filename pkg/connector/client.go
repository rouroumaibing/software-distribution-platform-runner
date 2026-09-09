// Package connector implements the Runner side of the Hub-Spoke long
// connection: an outbound-only WebSocket client that lets this cluster's
// Agent receive dispatched work from the Hub and stream status/logs back,
// without ever exposing this cluster's K8s API server to the Hub.
package connector

import (
	"context"
	"encoding/json"
	"log"
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
	MessageApplyPipelineRun = runnerapi.MessageApplyPipelineRun
	MessageApproveTask      = runnerapi.MessageApproveTask
	MessageRolloutControl   = runnerapi.MessageRolloutControl
	MessageStatusUpdate     = runnerapi.MessageStatusUpdate
	MessageLogChunk         = runnerapi.MessageLogChunk
	MessageHeartbeat        = runnerapi.MessageHeartbeat
)

// Handler processes an inbound message from the Hub. Registered per
// MessageType by whatever wires this package up in cmd/runner/main.go —
// typically the PipelineRun controller for MessageApplyPipelineRun.
type Handler func(payload json.RawMessage) error

// Client maintains a single outbound connection to the Hub's gateway,
// reconnecting with backoff on any failure. Every reconnect is followed by
// a Reconcile-triggering resync request (not implemented here — left as a
// TODO hook) so in-flight work isn't silently lost across a connection
// blip.
type Client struct {
	hubURL      string
	clusterName string
	authToken   string // Cluster registration token, rotated periodically — see hub's clusters table.

	handlers map[runnerapi.MessageType]Handler

	conn   *websocket.Conn
	outbox chan Message
}

func New(hubURL, clusterName, authToken string) *Client {
	return &Client{
		hubURL:      hubURL,
		clusterName: clusterName,
		authToken:   authToken,
		handlers:    make(map[MessageType]Handler),
		outbox:      make(chan Message, 256),
	}
}

func (c *Client) OnMessage(t runnerapi.MessageType, h Handler) {
	c.handlers[t] = h
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
	header := map[string][]string{
		"Authorization":  {"Bearer " + c.authToken},
		"X-Cluster-Name": {c.clusterName},
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.hubURL, header)
	if err != nil {
		return err
	}
	defer conn.Close()
	c.conn = conn

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
			_ = c.Send(MessageHeartbeat, map[string]string{"cluster": c.clusterName})
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
