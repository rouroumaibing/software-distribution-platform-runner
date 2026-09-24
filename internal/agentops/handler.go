// Package agentops executes hub-dispatched agent operations (§9.5 exec,
// UNIMPLEMENTED-MODULES-PLAN §16.5). The Runner is the direct-connect
// executor: it receives an AgentOpDispatchPayload over the authenticated
// gateway WS and runs the command as a Kubernetes Job — in its own cluster
// (agent access, in-cluster credentials) or in the target cluster described
// by the payload's kubeconfig (kubeconfig access).
//
// Why a Job (steelman §16.5): a Job is K8s-native audit trail (the pod spec
// records exactly what ran), gives timeout/backoff semantics for free
// (batchv1), and matches the TaskRun execution model this Runner already
// uses. An in-process pod-exec was rejected: it has no "which pod" answer for
// a cluster-scoped exec request and leaves no object behind.
//
// Output streaming: pod logs are polled (not tailed) and shipped as
// agent_op_log chunks; the hub persists them (agent_op_logs) so late SSE
// subscribers replay the full output. Poll-with-offset keeps the
// implementation simple and restart-safe; ops output is bounded by the
// overall timeout.
package agentops

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sort"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	runnerapi "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// Default execution knobs; overridable via env in cmd/runner/main.go.
const (
	DefaultImage   = "busybox:1.36"
	DefaultTimeout = 10 * time.Minute

	// jobTTLSeconds removes the finished Job from the target cluster one
	// hour after completion. The durable audit trail lives hub-side
	// (agent_ops + agent_op_logs), so the cluster copy is transient.
	jobTTLSeconds = int32(3600)

	// fallbackNamespace receives the op Job when the payload carries no
	// environment namespace (exec against a target without an env binding).
	fallbackNamespace = "default"
)

// Sender is the subset of connector.Client the handler needs: report status
// and log chunks back to the hub.
type Sender interface {
	Send(t runnerapi.MessageType, payload any) error
}

// Handler processes MessageAgentOp frames. Handle returns immediately (the
// op runs on its own goroutine) so a long exec never blocks the connector's
// read loop — the same non-blocking contract every dispatch handler keeps.
type Handler struct {
	// Clientset is the in-cluster client (agent-access ops). Per-op
	// kubeconfig clients are built on demand for kubeconfig-access ops.
	Clientset  *kubernetes.Clientset
	TargetName string
	Conn       Sender
	Image      string
	Timeout    time.Duration
}

// Handle decodes and launches. Decode errors are returned (the connector
// logs them); execution errors are reported to the hub as op status.
func (h *Handler) Handle(payload json.RawMessage) error {
	var p runnerapi.AgentOpDispatchPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("agentops: decode dispatch payload: %w", err)
	}
	go h.execute(p)
	return nil
}

// BuildJobSpec is the pure part of execution: it turns a dispatch payload
// into the batchv1.Job plus the namespace it runs in. Kept side-effect-free
// so the op envelope is unit-testable without a cluster.
func BuildJobSpec(p *runnerapi.AgentOpDispatchPayload, image string) (string, *batchv1.Job) {
	ns := p.Namespace
	if ns == "" {
		ns = fallbackNamespace
	}
	backoffLimit := int32(0)
	ttl := jobTTLSeconds
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			// GenerateName lets the apiserver dedupe concurrent creates of
			// the same op (e.g. a drained offline queue racing a redelivery).
			GenerateName: "sdp-agentop-",
			Namespace:    ns,
			Labels: map[string]string{
				"sdp.io/agent-op": p.OpID,
				"sdp.io/target":   p.TargetID,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:    "exec",
						Image:   image,
						Command: []string{"sh", "-c", p.Detail},
					}},
				},
			},
		},
	}
	return ns, job
}

func (h *Handler) execute(p runnerapi.AgentOpDispatchPayload) {
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout())
	defer cancel()

	report := func(status, message string) {
		if err := h.Conn.Send(runnerapi.MessageAgentOpStatus, runnerapi.AgentOpStatusPayload{
			OpID: p.OpID, Status: status, Message: message,
		}); err != nil {
			log.Printf("agentops: report %s for op %s failed: %v", status, p.OpID, err)
		}
	}
	sendChunk := func(stream, chunk string) {
		if chunk == "" {
			return
		}
		if err := h.Conn.Send(runnerapi.MessageAgentOpLog, runnerapi.AgentOpLogPayload{
			OpID: p.OpID, Stream: stream, Chunk: chunk,
		}); err != nil {
			log.Printf("agentops: log chunk for op %s failed: %v", p.OpID, err)
		}
	}

	if err := h.Conn.Send(runnerapi.MessageAgentOpStatus, runnerapi.AgentOpStatusPayload{
		OpID: p.OpID, Status: runnerapi.AgentOpStatusRunning,
	}); err != nil {
		log.Printf("agentops: report running for op %s failed: %v", p.OpID, err)
	}

	if p.OpType != runnerapi.AgentOpTypeExec {
		report(runnerapi.AgentOpStatusFailed, fmt.Sprintf("runner cannot execute op type %q", p.OpType))
		return
	}

	cs := h.Clientset
	if len(p.Kubeconfig) > 0 {
		cfg, err := clientcmd.RESTConfigFromKubeConfig(p.Kubeconfig)
		if err != nil {
			report(runnerapi.AgentOpStatusFailed, fmt.Sprintf("invalid kubeconfig: %v", err))
			return
		}
		cs, err = kubernetes.NewForConfig(cfg)
		if err != nil {
			report(runnerapi.AgentOpStatusFailed, fmt.Sprintf("build client from kubeconfig: %v", err))
			return
		}
	}
	if cs == nil {
		report(runnerapi.AgentOpStatusFailed, "no cluster credentials available (no in-cluster config, no kubeconfig)")
		return
	}

	ns, job := BuildJobSpec(&p, h.image())
	created, err := cs.BatchV1().Jobs(ns).Create(ctx, job, metav1.CreateOptions{})
	if kerrors.IsNotFound(err) {
		// Namespace missing on a fresh target: create it (best effort — the
		// runner's SA may lack the permission, then the op fails visibly).
		if _, nsErr := cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		}, metav1.CreateOptions{}); nsErr != nil && !kerrors.IsAlreadyExists(nsErr) {
			log.Printf("agentops: create namespace %s for op %s: %v", ns, p.OpID, nsErr)
		}
		created, err = cs.BatchV1().Jobs(ns).Create(ctx, job, metav1.CreateOptions{})
	}
	if err != nil {
		report(runnerapi.AgentOpStatusFailed, fmt.Sprintf("create job: %v", err))
		return
	}
	log.Printf("agentops: op %s executing as job %s/%s", p.OpID, ns, created.Name)

	status, message := h.watchJob(ctx, cs, ns, created.Name, sendChunk)
	report(status, message)
}

// watchJob polls the Job until completion (or ctx timeout), shipping new log
// bytes every tick. Returns the terminal op status + message.
func (h *Handler) watchJob(ctx context.Context, cs *kubernetes.Clientset, ns, jobName string, sendChunk func(stream, chunk string)) (string, string) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	offsets := map[string]int64{}

	for {
		select {
		case <-ctx.Done():
			return runnerapi.AgentOpStatusFailed, fmt.Sprintf("execution timed out after %s", h.timeout())
		case <-ticker.C:
		}

		job, err := cs.BatchV1().Jobs(ns).Get(ctx, jobName, metav1.GetOptions{})
		if err != nil {
			return runnerapi.AgentOpStatusFailed, fmt.Sprintf("get job: %v", err)
		}
		drainLogs(ctx, cs, ns, jobName, offsets, sendChunk)

		if job.Status.Succeeded > 0 {
			return runnerapi.AgentOpStatusSucceeded, ""
		}
		for _, c := range job.Status.Conditions {
			if c.Type == batchv1.JobFailed {
				return runnerapi.AgentOpStatusFailed, c.Message
			}
		}
	}
}

// drainLogs ships each pod's log bytes past the per-pod offset. Pods are
// visited in name order so chunk order is stable across ticks. The full log
// is re-read each tick and only the delta is sent — exec output is bounded by
// the overall timeout, so re-read cost is proportionate and the code stays
// restart-safe without persistent per-pod readers.
func drainLogs(ctx context.Context, cs *kubernetes.Clientset, ns, jobName string, offsets map[string]int64, sendChunk func(stream, chunk string)) {
	pods, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "job-name=" + jobName,
	})
	if err != nil {
		return // transient; retried next tick
	}
	names := make([]string, 0, len(pods.Items))
	for _, pod := range pods.Items {
		names = append(names, pod.Name)
	}
	sort.Strings(names)

	for _, name := range names {
		req := cs.CoreV1().Pods(ns).GetLogs(name, &corev1.PodLogOptions{})
		stream, err := req.Stream(ctx)
		if err != nil {
			continue // pod not ready yet
		}
		buf, _ := io.ReadAll(stream)
		stream.Close()
		sent := offsets[name]
		if int64(len(buf)) > sent {
			sendChunk(runnerapi.AgentOpStreamStdout, string(buf[sent:]))
			offsets[name] = int64(len(buf))
		}
	}
}

func (h *Handler) image() string {
	if h.Image != "" {
		return h.Image
	}
	return DefaultImage
}

func (h *Handler) timeout() time.Duration {
	if h.Timeout > 0 {
		return h.Timeout
	}
	return DefaultTimeout
}
