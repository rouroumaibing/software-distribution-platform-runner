package controller

import (
	"context"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/canary"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/metrics"
)

// client 报错降级分支测试：用 interceptor 注入 Get/Create/Patch 错误，验证这些
// best-effort 辅助函数出错时不 panic，并按设计返回 nil / 记日志 / 继续主流程。
// 这些分支此前 0% 覆盖，是 rollout_controller 变异存活的主要来源。

func ctrlScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = sdpv1alpha1.AddToScheme(s)
	return s
}

// 构造带 interceptor 的 reconciler；objs 为预置对象。
func newErrReconciler(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) *RolloutReconciler {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(ctrlScheme()).
		WithStatusSubresource(&sdpv1alpha1.Rollout{}).
		WithObjects(objs...).WithInterceptorFuncs(funcs).Build()
	return &RolloutReconciler{Client: c}
}

var errBoom = errors.New("injected client failure")

func notFoundErr() error {
	return apierrors.NewNotFound(schema.GroupResource{Resource: "deployments"}, "x")
}

// getStable：Get 任何错误都应降级为 nil（ResourceNotFound 与真实错误同路）。
func TestGetStableGetErrorReturnsNil(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	r := newErrReconciler(t, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			return errBoom
		},
	})
	if got := r.getStable(ctx, ro); got != nil {
		t.Errorf("getStable on Get error = %v, want nil", got)
	}
}

// ensureCanary：Get 非 NotFound 错误 → 降级 nil；NotFound → 创建。
// 这里验证非 NotFound 错误路径返回 nil，且不 panic。
func TestEnsureCanaryGetErrorReturnsNil(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	r := newErrReconciler(t, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			return errBoom
		},
	})
	if got := r.ensureCanary(ctx, ro, 2); got != nil {
		t.Errorf("ensureCanary on non-NotFound Get error = %v, want nil", got)
	}
}

// ensureCanary：创建 canary Deployment 时 Create 报错（非 AlreadyExists）→ 返回 nil。
func TestEnsureCanaryCreateErrorReturnsNil(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	r := newErrReconciler(t, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			return notFoundErr()
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			return errBoom
		},
	})
	if got := r.ensureCanary(ctx, ro, 2); got != nil {
		t.Errorf("ensureCanary on Create error = %v, want nil", got)
	}
}

// ensureService：Get 非 NotFound 错误 → 返回 nil。
func TestEnsureServiceGetErrorReturnsNil(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	r := newErrReconciler(t, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			return errBoom
		},
	})
	if got := r.ensureService(ctx, ro, nil); got != nil {
		t.Errorf("ensureService on non-NotFound Get error = %v, want nil", got)
	}
}

// ensureService：Create 报错 → 返回 nil。
func TestEnsureServiceCreateErrorReturnsNil(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	r := newErrReconciler(t, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			return notFoundErr()
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			return errBoom
		},
	})
	if got := r.ensureService(ctx, ro, nil); got != nil {
		t.Errorf("ensureService on Create error = %v, want nil", got)
	}
}

// ensureWorkload：稳定 Deployment 不存在时 Create 报错（非 AlreadyExists）仍返回解析出的副本数。
func TestEnsureWorkloadCreateErrorStillReturnsReplicas(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout() // Spec.Replicas=10
	ro.Spec.DesiredAction = ""
	r := newErrReconciler(t, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			return notFoundErr()
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			return errBoom
		},
	})
	if got := r.ensureWorkload(ctx, ro); got != 10 {
		t.Errorf("ensureWorkload on Create error = %d, want 10 (resolved replicas)", got)
	}
}

// scaleDeployment：Patch 报错不 panic；replicas 已相等时直接返回不发 Patch。
func TestScaleDeploymentPatchErrorAndNoop(t *testing.T) {
	ctx := context.Background()
	r := newErrReconciler(t, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			return errBoom
		},
	})
	dep := buildDepForScale(3)
	// replicas 不同 → 走 Patch（注入错误），不得 panic。
	r.scaleDeployment(ctx, dep, 5)
	// replicas 已相等 → 早返回，不发 Patch（这条路径不依赖 interceptor）。
	r.scaleDeployment(ctx, dep, 3)
}

// applyDecision：stable/canary 为 nil 时跳过对应 scale，不 panic；client 报错也不 panic。
func TestApplyDecisionNilAndClientErrorNoPanic(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	r := newErrReconciler(t, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			return errBoom
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			return errBoom
		},
	})
	// stable/canaryDep 均为 nil → 两个 scale 分支跳过，ensureService 走 Get 错误降级。
	r.applyDecision(ctx, ro, canaryDecision(), nil, 2)
	// 传入 dep → stable/canary scale 走 Patch 错误路径，不得 panic。
	r.applyDecision(ctx, ro, canaryDecision(), buildDepForScale(1), 2)
}

// ensureCanaryIngress：非 IngressCanary 路由类型 + feature 关闭均为 no-op。
func TestEnsureCanaryIngressNoop(t *testing.T) {
	ctx := context.Background()
	r := newErrReconciler(t, interceptor.Funcs{})
	ro := intentRollout() // TrafficRouting.Type = DeploymentWeight
	// 非 IngressCanary → 第一行早返回，不触发任何 client 调用。
	r.ensureCanaryIngress(ctx, ro, 10)

	ro.Spec.TrafficRouting.Type = sdpv1alpha1.TrafficRoutingIngressCanary
	old := metrics.CanaryIngressEnabled
	metrics.CanaryIngressEnabled = false
	defer func() { metrics.CanaryIngressEnabled = old }()
	r.ensureCanaryIngress(ctx, ro, 10)
}

func buildDepForScale(replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{Replicas: int32Ptr(replicas)},
	}
}

func canaryDecision() canary.Decision {
	return canary.Decision{
		Phase:          sdpv1alpha1.RolloutProgressing,
		CurrentWeight:  10,
		CanaryReplicas: 1,
		StableReplicas: 1,
	}
}
