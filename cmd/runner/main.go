// Command runner is the entrypoint deployed into each managed cluster: one
// binary that's both a controller-runtime manager (running
// PipelineRunReconciler/TaskRunReconciler/RolloutReconciler) and a
// connector.Client maintaining the outbound long connection back to the Hub.
package main

import (
	"context"
	"log"
	"os"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
	"github.com/rouroumaibing/software-distribution-platform-runner/internal/agentops"
	"github.com/rouroumaibing/software-distribution-platform-runner/internal/controller"
	"github.com/rouroumaibing/software-distribution-platform-runner/internal/dispatch"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/connector"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/executor"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/metrics"
)

// maxManagerStartAttempts bounds how many times we retry building/starting the
// controller-runtime manager before giving up (D-01). A transient cache-sync
// timeout during a hub rollout that briefly partitions the cluster is absorbed
// within this window; a permanently-wedged manager (CRD removed, RBAC revoked)
// eventually exits non-zero so the pod CrashLoopBackOffs and the real error
// surfaces in logs instead of the runner silently spinning forever.
const maxManagerStartAttempts = 10

func main() {
	scheme := clientgoscheme.Scheme
	utilruntime.Must(sdpv1alpha1.AddToScheme(scheme))

	restCfg, err := ctrl.GetConfig()
	if err != nil {
		log.Fatalf("unable to get kube config: %v", err)
	}

	targetName := mustEnv("TARGET_NAME")

	// Outbound long connection to the Hub. cmd/runner/main.go is the ONLY
	// place that wires the connector's message handlers. The connector keeps
	// its own reconnect loop entirely independent of the controller-runtime
	// manager: a websocket blip must NEVER trigger a manager rebuild (D-01).
	conn := connector.New(
		mustEnv("HUB_GATEWAY_URL"),
		targetName,
		mustEnv("TARGET_AUTH_TOKEN"),
	)

	// Typed clientset is needed to stream pod logs (controller-runtime's
	// client can't tail logs) for the B-02 live-log feature. Built once from
	// the shared rest.Config; survives manager rebuilds across retries.
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("unable to build clientset: %v", err)
	}

	// Install Prometheus collectors (B-09). No-op if the /metrics endpoint
	// isn't scraped; the downgrade flags live in pkg/metrics.
	metrics.Register()

	// §9.5 直连执行（§16.5）：agent op 执行器走与 manager 无关的 in-cluster
	// clientset，在 connector readLoop 之外异步执行 exec op（kubeconfig-access
	// 的 op 用 payload 自带凭据按需建 client）。注册在 startManager 之外 ——
	// 它不依赖 manager，重试重建 manager 无需重新挂接。
	agentOpHandler := &agentops.Handler{
		Clientset:  clientset,
		TargetName: targetName,
		Conn:       conn,
		Image:      envOr("SDP_AGENT_EXEC_IMAGE", agentops.DefaultImage),
		Timeout:    envDurationOr("SDP_AGENT_EXEC_TIMEOUT", agentops.DefaultTimeout),
	}
	conn.OnMessage(connector.MessageAgentOp, agentOpHandler.Handle)

	jobBuilder := executor.NewJobBuilder()

	// startManager builds a fresh controller-runtime manager, (re)wires the
	// Hub -> Runner message handlers against that manager's client, registers
	// the reconcilers, and blocks in mgr.Start. Each call is identical to
	// first boot, so a retry reuses the exact same informer tolerance that
	// worked on startup (D-01). It returns nil only when ctx is cancelled
	// (clean shutdown); any other error is a (possibly transient) start
	// failure the caller may retry.
	startManager := func(ctx context.Context) error {
		mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
			Scheme: scheme,
			// LeaderElection: true 支持这个组件在同一集群里跑多副本做高可用,
			// controller-runtime 基于 K8s 的 Lease 资源自动选主,不需要额外引入
			// etcd/Redis。
			LeaderElection:   os.Getenv("LEADER_ELECTION") == "true",
			LeaderElectionID: "sdp-runner-leader",
		})
		if err != nil {
			return err
		}

		// Register the Hub -> Runner message handlers against the *current*
		// manager's client. Re-registering (rather than mutating) is safe:
		// the handler map is keyed by message type, overwriting the prior
		// attempt's handlers which are now tied to a stopped manager.
		applyHandler := &dispatch.ApplyHandler{Client: mgr.GetClient(), TargetName: targetName}
		conn.OnMessage(connector.MessageApplyPipelineRun, applyHandler.Handle)
		approveHandler := &dispatch.ApproverHandler{Client: mgr.GetClient()}
		conn.OnMessage(connector.MessageApproveTask, approveHandler.Handle)
		rolloutControlHandler := &dispatch.RolloutControlHandler{Client: mgr.GetClient()}
		conn.OnMessage(connector.MessageRolloutControl, rolloutControlHandler.Handle)
		rerunHandler := &dispatch.RerunHandler{Client: mgr.GetClient()}
		conn.OnMessage(connector.MessageRerunTask, rerunHandler.Handle)
		cancelHandler := &dispatch.CancelHandler{Client: mgr.GetClient()}
		conn.OnMessage(connector.MessageCancelPipelineRun, cancelHandler.Handle)

		// C-05: on every (re)connection, re-assert local state with the Hub so
		// in-flight work isn't silently lost. The hub runs DrainTarget on
		// connect; this is the Runner counterpart. Re-registered each attempt
		// so it always captures the live manager's client.
		conn.OnConnect(func() {
			metrics.IncReconnect()
			if err := controller.ResyncAll(context.Background(), mgr.GetClient(), conn, targetName); err != nil {
				log.Printf("runner: resync after reconnect failed: %v", err)
			}
		})

		if err := (&controller.PipelineRunReconciler{
			Client:     mgr.GetClient(),
			Conn:       conn,
			TargetName: targetName,
		}).SetupWithManager(mgr); err != nil {
			return err
		}
		if err := (&controller.TaskRunReconciler{
			Client:     mgr.GetClient(),
			JobBuilder: jobBuilder,
			Conn:       conn,
			Clientset:  clientset,
			TargetName: targetName,
		}).SetupWithManager(mgr); err != nil {
			return err
		}
		if err := (&controller.RolloutReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
			return err
		}

		return mgr.Start(ctx)
	}

	ctx := ctrl.SetupSignalHandler()

	// The connector's outbound long connection runs for the whole process
	// lifetime, reconnecting on its own backoff. It is intentionally NOT
	// restarted when the manager fails (D-01).
	go conn.Run(ctx)

	log.Printf("runner for target %q starting", targetName)

	// D-01: resilient manager start. A transient cache-sync timeout (e.g.
	// during a hub rollout) retries with exponential backoff instead of
	// fatal-exiting into a CrashLoop. The connector keeps serving the whole
	// time. Clean shutdown happens only on ctx cancellation.
	backoff := time.Second
	for attempt := 1; ; attempt++ {
		if err := startManager(ctx); err != nil {
			if ctx.Err() != nil {
				log.Printf("runner: manager stopped: %v", err)
				return
			}
			if attempt >= maxManagerStartAttempts {
				log.Fatalf("runner: manager failed to start after %d attempts: %v", attempt, err)
			}
			log.Printf("runner: manager start attempt %d failed (retrying in %s): %v", attempt, backoff, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		// startManager returned nil → ctx cancelled → clean shutdown.
		return
	}
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required env var %s", key)
	}
	return v
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDurationOr(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Printf("runner: bad %s %q — using default %s", key, v, fallback)
	}
	return fallback
}
