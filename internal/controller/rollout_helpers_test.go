package controller

import (
	"os"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// 纯决策 helper 的表驱动测试。这些函数零 I/O、零 client 依赖，
// 把状态机里可判定的边界全部钉死，避免"测一个分支要搭整个 fake client"。

func TestCurrentStep(t *testing.T) {
	steps := []sdpv1alpha1.CanaryStep{
		{SetWeight: pi32(10)},
		{SetWeight: pi32(20)},
	}
	tests := []struct {
		name  string
		index int32
		want  *int32
	}{
		{"first step", 0, pi32(10)},
		{"last step", 1, pi32(20)},
		{"index at len -> nil", 2, nil},
		{"index beyond len -> nil", 9, nil},
		{"negative index -> nil（防御）", -1, nil},
	}
	for _, tc := range tests {
		ro := intentRollout()
		ro.Spec.Steps = steps
		ro.Status.CurrentStepIndex = tc.index
		got := currentStep(ro)
		if tc.want == nil {
			if got != nil {
				t.Errorf("%s: currentStep=%v want nil", tc.name, got)
			}
			continue
		}
		if got == nil || got.SetWeight == nil || *got.SetWeight != *tc.want {
			t.Errorf("%s: currentStep=%v want weight %d", tc.name, got, *tc.want)
		}
	}
}

func TestWeightReplicas(t *testing.T) {
	tests := []struct {
		name          string
		weight, total int32
		want          int32
	}{
		{"half", 50, 10, 5},
		{"zero weight", 0, 10, 0},
		{"full weight", 100, 10, 10},
		{"round down", 33, 10, 3},
		{"total zero -> 0", 50, 0, 0},
		{"total negative -> 0", 50, -3, 0},
		{"weight over 100 not clamped here", 150, 10, 15},
	}
	for _, tc := range tests {
		if got := weightReplicas(tc.weight, tc.total); got != tc.want {
			t.Errorf("%s: weightReplicas(%d,%d)=%d want %d", tc.name, tc.weight, tc.total, got, tc.want)
		}
	}
}

func TestCanaryReplicaState(t *testing.T) {
	if ready, desired := canaryReplicaState(nil); ready != 0 || desired != 0 {
		t.Errorf("nil dep: got (%d,%d) want (0,0)", ready, desired)
	}

	withReplicas := &appsv1.Deployment{
		Spec:   appsv1.DeploymentSpec{Replicas: pi32(4)},
		Status: appsv1.DeploymentStatus{AvailableReplicas: 3},
	}
	if ready, desired := canaryReplicaState(withReplicas); ready != 3 || desired != 4 {
		t.Errorf("with replicas: got (%d,%d) want (3,4)", ready, desired)
	}

	// replicas nil（Deployment 默认值场景）→ desired 应为 0，不能 panic。
	noReplicas := &appsv1.Deployment{Status: appsv1.DeploymentStatus{AvailableReplicas: 2}}
	if ready, desired := canaryReplicaState(noReplicas); ready != 2 || desired != 0 {
		t.Errorf("nil spec.replicas: got (%d,%d) want (2,0)", ready, desired)
	}
}

func TestCanaryDeploymentHasReplicaFailure(t *testing.T) {
	if canaryDeploymentHasReplicaFailure(nil) {
		t.Error("nil dep must not be a failure")
	}
	if canaryDeploymentHasReplicaFailure(&appsv1.Deployment{}) {
		t.Error("no conditions must not be a failure")
	}

	depWith := func(conds ...appsv1.DeploymentCondition) *appsv1.Deployment {
		return &appsv1.Deployment{Status: appsv1.DeploymentStatus{Conditions: conds}}
	}
	tests := []struct {
		name string
		cond appsv1.DeploymentCondition
		want bool
	}{
		{
			"ReplicaFailure=True",
			appsv1.DeploymentCondition{Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionTrue},
			true,
		},
		{
			"ReplicaFailure=False",
			appsv1.DeploymentCondition{Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionFalse},
			false,
		},
		{
			"ReplicaFailure=Unknown",
			appsv1.DeploymentCondition{Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionUnknown},
			false,
		},
		{
			"other condition True",
			appsv1.DeploymentCondition{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue},
			false,
		},
	}
	for _, tc := range tests {
		if got := canaryDeploymentHasReplicaFailure(depWith(tc.cond)); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}

	// 非失败条件在前、失败条件在后 -> 必须继续扫描（不能命中第一个就返回）。
	mixed := depWith(
		appsv1.DeploymentCondition{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue},
		appsv1.DeploymentCondition{Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionTrue},
	)
	if !canaryDeploymentHasReplicaFailure(mixed) {
		t.Error("failure condition after a healthy one must still be detected")
	}
}

func TestRolloutSvcTargetPortEnv(t *testing.T) {
	tests := []struct {
		name  string
		value string
		set   bool
		want  int32
	}{
		{"unset -> 0", "", false, 0},
		{"empty -> 0", "", true, 0},
		{"valid port", "9090", true, 9090},
		{"non-numeric -> 0", "abc", true, 0},
		{"zero -> 0", "0", true, 0},
		{"negative -> 0", "-1", true, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv("SDP_ROLLOUT_SVC_TARGET_PORT", tc.value)
			} else {
				// t.Setenv 登记清理（记住原值），os.Unsetenv 精确制造"未设置"态。
				t.Setenv("SDP_ROLLOUT_SVC_TARGET_PORT", "")
				if err := os.Unsetenv("SDP_ROLLOUT_SVC_TARGET_PORT"); err != nil {
					t.Fatal(err)
				}
			}
			if got := rolloutSvcTargetPortEnv(); got != tc.want {
				t.Errorf("got %d want %d", got, tc.want)
			}
		})
	}
}

func TestCanaryIngressSpec(t *testing.T) {
	spec := canaryIngressSpec("svc.sdp.local", "/api", "wl", "ns")
	if len(spec.Rules) != 1 {
		t.Fatalf("rules = %d want 1", len(spec.Rules))
	}
	rule := spec.Rules[0]
	if rule.Host != "svc.sdp.local" {
		t.Errorf("host = %q", rule.Host)
	}
	if rule.HTTP == nil || len(rule.HTTP.Paths) != 1 {
		t.Fatalf("http paths malformed: %v", rule.HTTP)
	}
	p := rule.HTTP.Paths[0]
	if p.Path != "/api" || p.PathType == nil || *p.PathType != networkingv1.PathTypePrefix {
		t.Errorf("path = %+v want /api + Prefix", p)
	}
	if p.Backend.Service == nil || p.Backend.Service.Name != "wl-sdp" || p.Backend.Service.Port.Number != 80 {
		t.Errorf("backend = %+v want wl-sdp:80", p.Backend.Service)
	}
}

// canaryHealthy 的纯判定：podsReady 是前置门，未就绪直接 false（不打任何探针）；
// PodReady 类型在就绪时直接 true；PrometheusQuery 无 endpoint 时 fail-safe 为 false。
func TestCanaryHealthyPureGates(t *testing.T) {
	ro := intentRollout()

	// podsReady=false 时任何 HealthCheck 类型都应短路为 false，不触发网络。
	ro.Spec.HealthCheck = sdpv1alpha1.HealthCheckSpec{Type: sdpv1alpha1.HealthCheckHTTPProbe}
	if canaryHealthy(ro, 3, 3, false) {
		t.Error("pods not ready must be unhealthy regardless of probe type")
	}

	// 默认 PodReady：就绪即健康。
	ro.Spec.HealthCheck = sdpv1alpha1.HealthCheckSpec{Type: sdpv1alpha1.HealthCheckPodReady}
	if !canaryHealthy(ro, 3, 3, true) {
		t.Error("PodReady + podsReady must be healthy")
	}

	// PrometheusQuery 无 endpoint：fail-safe，不推进。
	oldEP := PrometheusEndpoint
	PrometheusEndpoint = ""
	t.Cleanup(func() { PrometheusEndpoint = oldEP })
	ro.Spec.HealthCheck = sdpv1alpha1.HealthCheckSpec{
		Type:                sdpv1alpha1.HealthCheckPrometheusQuery,
		PrometheusQueryExpr: "up",
	}
	if canaryHealthy(ro, 3, 3, true) {
		t.Error("PrometheusQuery without endpoint must hold (unhealthy)")
	}
}
