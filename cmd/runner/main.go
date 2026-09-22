// Command runner is the entrypoint deployed into each managed cluster: one
// binary that's both a controller-runtime manager (running
// PipelineRunReconciler/TaskRunReconciler/RolloutReconciler) and a
// connector.Client maintaining the outbound long connection back to the Hub.
package main

import (
	"context"
	"log"
	"os"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
	"github.com/rouroumaibing/software-distribution-platform-runner/internal/controller"
	"github.com/rouroumaibing/software-distribution-platform-runner/internal/dispatch"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/connector"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/executor"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/metrics"
)

func main() {
	scheme := clientgoscheme.Scheme
	utilruntime.Must(sdpv1alpha1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		// LeaderElection: true 支持这个组件在同一集群里跑多副本做高可用,
		// controller-runtime 基于 K8s 的 Lease 资源自动选主,不需要额外引入
		// etcd/Redis。
		LeaderElection:   os.Getenv("LEADER_ELECTION") == "true",
		LeaderElectionID: "sdp-runner-leader",
	})
	if err != nil {
		log.Fatalf("unable to start manager: %v", err)
	}

	targetName := mustEnv("TARGET_NAME")

	// Outbound long connection to the Hub. cmd/runner/main.go is the ONLY
	// place that wires the connector's message handlers.
	conn := connector.New(
		mustEnv("HUB_GATEWAY_URL"),
		targetName,
		mustEnv("TARGET_AUTH_TOKEN"),
	)

	jobBuilder := executor.NewJobBuilder()

	// Typed clientset is needed to stream pod logs (controller-runtime's
	// client can't tail logs) for the B-02 live-log feature.
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		log.Fatalf("unable to build clientset: %v", err)
	}

	// Install Prometheus collectors (B-09). No-op if the /metrics endpoint
	// isn't scraped; the downgrade flags live in pkg/metrics.
	metrics.Register()

	// Register the Hub -> Runner message handlers BEFORE starting the
	// connection so no dispatch is missed on connect.
	applyHandler := &dispatch.ApplyHandler{Client: mgr.GetClient(), TargetName: targetName}
	conn.OnMessage(connector.MessageApplyPipelineRun, applyHandler.Handle)
	approveHandler := &dispatch.ApproverHandler{Client: mgr.GetClient()}
	conn.OnMessage(connector.MessageApproveTask, approveHandler.Handle)
	rolloutControlHandler := &dispatch.RolloutControlHandler{Client: mgr.GetClient()}
	conn.OnMessage(connector.MessageRolloutControl, rolloutControlHandler.Handle)
	rerunHandler := &dispatch.RerunHandler{Client: mgr.GetClient()}
	conn.OnMessage(connector.MessageRerunTask, rerunHandler.Handle)

	// C-05: on every (re)connection, re-assert local state with the Hub so
	// in-flight work isn't silently lost. The hub runs DrainTarget on
	// connect; this is the Runner counterpart.
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
		log.Fatalf("unable to setup PipelineRunReconciler: %v", err)
	}
	if err := (&controller.TaskRunReconciler{
		Client:     mgr.GetClient(),
		JobBuilder: jobBuilder,
		Conn:       conn,
		Clientset:  clientset,
		TargetName: targetName,
	}).SetupWithManager(mgr); err != nil {
		log.Fatalf("unable to setup TaskRunReconciler: %v", err)
	}
	if err := (&controller.RolloutReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		log.Fatalf("unable to setup RolloutReconciler: %v", err)
	}

	ctx := ctrl.SetupSignalHandler()
	go conn.Run(ctx)

	log.Printf("runner for target %q starting manager", targetName)
	if err := mgr.Start(ctx); err != nil {
		log.Fatalf("manager exited: %v", err)
	}
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required env var %s", key)
	}
	return v
}
