package agentops

import (
	"strings"
	"testing"

	runnerapi "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// BuildUpgradeJobSpec 是自升级的纯函数外壳（INSTALL-UPGRADE-EXECUTOR-DESIGN
// §4.1）：镜像/脚本/SA/幂等名都可离线断言，无需集群。
func TestBuildUpgradeJobSpec(t *testing.T) {
	p := &runnerapi.AgentOpDispatchPayload{
		OpID:     "aaaaaaaa-bbbb-cccc-dddd-eeeeffff0000",
		TargetID: "target-1",
		OpType:   runnerapi.AgentOpTypeUpgrade,
		Detail:   "v0.0.2",
	}
	opts := upgradeOpts{
		Namespace:   "sdp-workflow",
		Deployment:  "runner",
		Container:   "container-1",
		ServiceAcct: "sdp-runner",
		NewImage:    "harbor.sdpworkflow.com/sdp/software-distribution-platform-runner",
		KubectlImg:  DefaultKubectlImage,
		Timeout:     "8m",
	}
	ns, job := BuildUpgradeJobSpec(p, opts)
	if ns != "sdp-workflow" {
		t.Fatalf("namespace = %q, want sdp-workflow", ns)
	}
	// 幂等键 1/2：确定性 Job 名（重派发命中 AlreadyExists 而非堆叠）
	wantName := "sdp-upgrade-aaaaaaaa"
	if job.Name != wantName {
		t.Errorf("job name = %q, want %q", job.Name, wantName)
	}
	// 幂等键 2/2：opID 标签（label 探测用）
	if job.Labels["sdp.io/agent-op"] != p.OpID {
		t.Errorf("op label = %q, want %q", job.Labels["sdp.io/agent-op"], p.OpID)
	}
	c := job.Spec.Template.Spec.Containers[0]
	if c.Image != DefaultKubectlImage {
		t.Errorf("container image = %q, want %q", c.Image, DefaultKubectlImage)
	}
	if job.Spec.Template.Spec.ServiceAccountName != "sdp-runner" {
		t.Errorf("serviceAccount = %q, want sdp-runner", job.Spec.Template.Spec.ServiceAccountName)
	}
	// 脚本必须把目标镜像 patch 到本 deployment 并等待 rollout
	script := strings.Join(c.Command, " ")
	for _, want := range []string{
		"set image deployment/runner container-1=",
		"software-distribution-platform-runner:v0.0.2",
		"rollout status deployment/runner",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("script missing %q:\n%s", want, script)
		}
	}
}

func TestBuildUpgradeJobSpecDefaultsToRunnerImageName(t *testing.T) {
	if !strings.HasPrefix(DefaultRunnerImageName, "harbor.sdpworkflow.com/sdp/") {
		t.Fatalf("runner image %q must live in the harbor sdp project (#22① 命名约定)", DefaultRunnerImageName)
	}
}

// SelfUpgradeDisabled=true 时 upgrade op 必须显式回 failed（chart
// runner.selfUpgrade.enabled=false 的运行时语义），而不是静默忽略或靠
// in-cluster 兜底执行 —— hub 发起方要能立即知道该 target 不支持自我替换。
func TestExecuteUpgradeDisabledReportsFailed(t *testing.T) {
	h := &Handler{SelfUpgradeDisabled: true}
	var status, msg string
	h.executeUpgrade(
		runnerapi.AgentOpDispatchPayload{OpID: "op-1", OpType: runnerapi.AgentOpTypeUpgrade, Detail: "v0.0.2"},
		func(s, m string) { status, msg = s, m },
		func(string, string) { t.Error("no chunk should be streamed when disabled") },
	)
	if status != runnerapi.AgentOpStatusFailed {
		t.Errorf("status = %q, want %q", status, runnerapi.AgentOpStatusFailed)
	}
	if !strings.Contains(msg, "disabled") {
		t.Errorf("message = %q, want mention of disabled", msg)
	}
}
