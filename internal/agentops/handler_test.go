package agentops

// BuildJobSpec 单测：op 信封（命名空间回退、命令逐字、审计标签、TTL/退避）。
// 执行循环依赖真实集群，不在单测范围。

import (
	"strings"
	"testing"
	"time"

	runnerapi "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

func TestBuildJobSpec_EnvNamespaceAndVerbatimCommand(t *testing.T) {
	p := &runnerapi.AgentOpDispatchPayload{
		OpID:      "op-123",
		TargetID:  "target-abc",
		OpType:    runnerapi.AgentOpTypeExec,
		Detail:    "kubectl get pods -A | head",
		Namespace: "team-x-prod",
	}
	ns, job := BuildJobSpec(p, DefaultImage)
	if ns != "team-x-prod" {
		t.Fatalf("namespace must come from the payload, got %q", ns)
	}
	if job.GenerateName != "sdp-agentop-" {
		t.Fatalf("job must use GenerateName, got %q", job.GenerateName)
	}
	cmd := job.Spec.Template.Spec.Containers[0].Command
	if len(cmd) != 3 || cmd[0] != "sh" || cmd[1] != "-c" || cmd[2] != p.Detail {
		t.Fatalf("command must carry the detail verbatim via sh -c, got %v", cmd)
	}
	if job.Labels["sdp.io/agent-op"] != "op-123" {
		t.Fatalf("audit label missing: %v", job.Labels)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Fatal("backoff must be 0 (retry policy is the hub's re-dispatch concern)")
	}
	if job.Spec.TTLSecondsAfterFinished == nil || *job.Spec.TTLSecondsAfterFinished != 3600 {
		t.Fatal("finished jobs must be TTL-reaped (audit trail lives hub-side)")
	}
}

func TestBuildJobSpec_NamespaceFallback(t *testing.T) {
	p := &runnerapi.AgentOpDispatchPayload{OpID: "op-1", TargetID: "t", OpType: runnerapi.AgentOpTypeExec, Detail: "echo hi"}
	ns, _ := BuildJobSpec(p, DefaultImage)
	if ns != fallbackNamespace {
		t.Fatalf("no payload namespace must fall back to %q, got %q", fallbackNamespace, ns)
	}
}

func TestHandlerDefaults(t *testing.T) {
	h := &Handler{}
	if h.image() != DefaultImage {
		t.Fatalf("default image = %q", h.image())
	}
	if h.timeout() != DefaultTimeout {
		t.Fatalf("default timeout = %s", h.timeout())
	}
	h2 := &Handler{Image: "alpine:3", Timeout: 3 * time.Minute}
	if h2.image() != "alpine:3" || h2.timeout() != 3*time.Minute {
		t.Fatal("configured overrides must win")
	}
}

// Handle 必须异步返回（长 exec 不能阻塞 connector readLoop）——通过 payload
// 解码失败同步报错、解码成功立即返回不执行来验证非阻塞契约。
func TestHandle_ReturnsDecodeErrorSynchronously(t *testing.T) {
	h := &Handler{}
	if err := h.Handle([]byte("not json")); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("bad payload must error synchronously, got %v", err)
	}
}
