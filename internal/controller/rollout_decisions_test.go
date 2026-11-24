package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// Reconcile / applyDesiredAction 的决策分支测试。全部用 fake client + 构造对象
// 触发（不 mock client 报错），覆盖状态机的"短路/保持/保护"语义——这些分支
// 决定"已终态不被复活""回滚不被 promote 撤销"等关键不变量。

const testKeyName = "ro"

func roKey() client.ObjectKey { return client.ObjectKey{Namespace: "ns", Name: testKeyName} }

func ctrlRequest() ctrl.Request { return ctrl.Request{NamespacedName: roKey()} }

func getRO(t *testing.T, c client.Client) sdpv1alpha1.Rollout {
	t.Helper()
	var got sdpv1alpha1.Rollout
	if err := c.Get(context.Background(), roKey(), &got); err != nil {
		t.Fatalf("get rollout: %v", err)
	}
	return got
}

// terminal 态（Failed/RolledBack）必须短路：不再评估步骤、不再重排队。
func TestReconcileTerminalHoldDoesNotResume(t *testing.T) {
	for _, phase := range []sdpv1alpha1.RolloutPhase{
		sdpv1alpha1.RolloutFailed,
		sdpv1alpha1.RolloutRolledBack,
	} {
		t.Run(string(phase), func(t *testing.T) {
			ctx := context.Background()
			ro := intentRollout()
			ro.Status.Phase = phase
			ro.Status.CurrentStepIndex = 0
			ro.Status.CurrentWeight = 0
			r, _ := newIntentReconciler(t, ro)

			res, err := r.Reconcile(ctx, ctrlRequest())
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if res.RequeueAfter != 0 {
				t.Errorf("terminal hold must not requeue, got %v", res.RequeueAfter)
			}
			got := getRO(t, r.Client)
			if got.Status.CurrentStepIndex != 0 || got.Status.CurrentWeight != 0 {
				t.Errorf("terminal state mutated: step=%d weight=%d", got.Status.CurrentStepIndex, got.Status.CurrentWeight)
			}
			if got.Status.Phase != phase {
				t.Errorf("phase changed: %s want %s", got.Status.Phase, phase)
			}
		})
	}
}

// 旧版 aborted 注解的兼容保持：canary 钉在 0%，CompletionTime 幂等（不覆盖已有值）。
func TestReconcileAbortedAnnotationHoldsWithIdempotentCompletion(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	ro.Annotations = map[string]string{sdpv1alpha1.AnnRolloutAborted: "true"}
	ro.Status.Phase = sdpv1alpha1.RolloutProgressing
	fixed := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	ro.Status.CompletionTime = &fixed
	r, _ := newIntentReconciler(t, ro)

	if _, err := r.Reconcile(ctx, ctrlRequest()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := getRO(t, r.Client)
	if got.Status.Phase != sdpv1alpha1.RolloutRolledBack {
		t.Errorf("phase = %s want RolledBack", got.Status.Phase)
	}
	if got.Status.CurrentWeight != 0 || got.Status.CanaryReplicas != 0 {
		t.Errorf("aborted canary not pinned to 0: %d/%d", got.Status.CurrentWeight, got.Status.CanaryReplicas)
	}
	if got.Status.CompletionTime == nil || !got.Status.CompletionTime.Equal(&fixed) {
		t.Errorf("CompletionTime overwritten: %v want %v", got.Status.CompletionTime, fixed)
	}
}

// Pause 保持分支（Spec 意图）：相位停在 Paused、不重排步骤，但仍按健康间隔重排队。
func TestReconcilePauseIntentHolds(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	ro.Spec.DesiredAction = sdpv1alpha1.RolloutActionPause
	ro.Status.CurrentWeight = 10
	ro.Status.CurrentStepIndex = 0
	r, _ := newIntentReconciler(t, ro)

	// 先执行一次让意图被消费（applyDesiredAction 走 pause），进入保持态。
	if _, err := r.Reconcile(ctx, ctrlRequest()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	got := getRO(t, r.Client)
	if got.Status.Phase != sdpv1alpha1.RolloutPaused {
		t.Fatalf("phase = %s want Paused", got.Status.Phase)
	}

	// 第二次 reconcile 走 paused hold 分支：相位与权重保持。
	if _, err := r.Reconcile(ctx, ctrlRequest()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	got = getRO(t, r.Client)
	if got.Status.Phase != sdpv1alpha1.RolloutPaused || got.Status.CurrentWeight != 10 {
		t.Errorf("paused hold drifted: phase=%s weight=%d", got.Status.Phase, got.Status.CurrentWeight)
	}
}

// 旧版 paused 注解（无 Spec 意图）同样进入保持分支。
func TestReconcileLegacyPauseAnnotationHolds(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	ro.Annotations = map[string]string{sdpv1alpha1.AnnRolloutPaused: "true"}
	ro.Status.Phase = sdpv1alpha1.RolloutPaused
	ro.Status.CurrentWeight = 20
	r, _ := newIntentReconciler(t, ro)

	if _, err := r.Reconcile(ctx, ctrlRequest()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := getRO(t, r.Client)
	if got.Status.Phase != sdpv1alpha1.RolloutPaused {
		t.Errorf("phase = %s want Paused", got.Status.Phase)
	}
	if got.Status.CurrentWeight != 20 {
		t.Errorf("weight = %d want 20 (hold at observed weight)", got.Status.CurrentWeight)
	}
}

// promote 不得撤销回滚：无论回滚来自注解还是 Status 终态，都应被拒。
func TestApplyDesiredActionPromoteRejectedAfterRollback(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name        string
		annotations map[string]string
		phase       sdpv1alpha1.RolloutPhase
	}{
		{"aborted annotation", map[string]string{sdpv1alpha1.AnnRolloutAborted: "true"}, sdpv1alpha1.RolloutProgressing},
		{"terminal rolledback status", map[string]string{}, sdpv1alpha1.RolloutRolledBack},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ro := intentRollout()
			ten := int32(10)
			ro.Spec.Steps = []sdpv1alpha1.CanaryStep{{SetWeight: &ten}, {SetWeight: &ten}}
			ro.Status.Phase = tc.phase
			ro.Annotations = tc.annotations
			ro.Spec.DesiredAction = sdpv1alpha1.RolloutActionPromote
			r, c := newIntentReconciler(t, ro)

			if _, err := r.applyDesiredAction(ctx, ro); err != nil {
				t.Fatalf("apply: %v", err)
			}
			got := getRO(t, c)
			if got.Status.CurrentStepIndex != 0 {
				t.Errorf("promote advanced a rolled-back rollout: step=%d", got.Status.CurrentStepIndex)
			}
			if got.Status.Message != "promote ignored: rollout was rolled back" {
				t.Errorf("message = %q", got.Status.Message)
			}
		})
	}
}

// promote 越过最后一步 → Healthy，CompletionTime 只写一次。
func TestApplyDesiredActionPromoteToCompletionIdempotent(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	ro.Spec.Steps = []sdpv1alpha1.CanaryStep{{SetWeight: pi32(10)}}
	ro.Status.CurrentStepIndex = 1 // 已越过唯一一步
	ro.Spec.DesiredAction = sdpv1alpha1.RolloutActionPromote
	fixed := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	ro.Status.CompletionTime = &fixed
	r, c := newIntentReconciler(t, ro)

	if _, err := r.applyDesiredAction(ctx, ro); err != nil {
		t.Fatalf("apply: %v", err)
	}
	got := getRO(t, c)
	if got.Status.Phase != sdpv1alpha1.RolloutHealthy {
		t.Errorf("phase = %s want Healthy", got.Status.Phase)
	}
	if got.Status.CompletionTime == nil || !got.Status.CompletionTime.Equal(&fixed) {
		t.Errorf("CompletionTime overwritten: %v", got.Status.CompletionTime)
	}
	if got.Status.Message != "promoted to completion by operator" {
		t.Errorf("message = %q", got.Status.Message)
	}
}

// operator 归属写入 Message；两种动作都要带 "by <operator>"。
func TestApplyDesiredActionOperatorInMessage(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	ro.Spec.Steps = []sdpv1alpha1.CanaryStep{{SetWeight: pi32(10)}, {SetWeight: pi32(20)}}
	ro.Status.CurrentWeight = 10
	ro.Spec.DesiredAction = sdpv1alpha1.RolloutActionPromote
	ro.Spec.DesiredActionOperator = "bob"
	r, c := newIntentReconciler(t, ro)

	if _, err := r.applyDesiredAction(ctx, ro); err != nil {
		t.Fatalf("apply: %v", err)
	}
	got := getRO(t, c)
	if got.Status.Message != "promoted by bob" {
		t.Errorf("message = %q want 'promoted by bob'", got.Status.Message)
	}
}

// paused 时收到 promote：清掉旧 paused 注解，让保持分支不再生效。
func TestApplyDesiredActionPromoteClearsLegacyPaused(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	ro.Spec.Steps = []sdpv1alpha1.CanaryStep{{SetWeight: pi32(10)}, {SetWeight: pi32(20)}}
	ro.Status.CurrentWeight = 10
	ro.Status.Phase = sdpv1alpha1.RolloutPaused
	ro.Annotations = map[string]string{sdpv1alpha1.AnnRolloutPaused: "true"}
	ro.Spec.DesiredAction = sdpv1alpha1.RolloutActionPromote
	r, c := newIntentReconciler(t, ro)

	if _, err := r.applyDesiredAction(ctx, ro); err != nil {
		t.Fatalf("apply: %v", err)
	}
	got := getRO(t, c)
	if _, ok := got.Annotations[sdpv1alpha1.AnnRolloutPaused]; ok {
		t.Error("legacy paused annotation must be cleared on promote")
	}
	if got.Status.Phase != sdpv1alpha1.RolloutProgressing {
		t.Errorf("phase = %s want Progressing", got.Status.Phase)
	}
}

// rollback 的 CompletionTime 幂等。
func TestApplyDesiredActionRollbackCompletionIdempotent(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	ro.Status.CurrentWeight = 10
	ro.Spec.DesiredAction = sdpv1alpha1.RolloutActionRollback
	fixed := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	ro.Status.CompletionTime = &fixed
	r, c := newIntentReconciler(t, ro)

	if _, err := r.applyDesiredAction(ctx, ro); err != nil {
		t.Fatalf("apply: %v", err)
	}
	got := getRO(t, c)
	if got.Status.CompletionTime == nil || !got.Status.CompletionTime.Equal(&fixed) {
		t.Errorf("CompletionTime overwritten: %v", got.Status.CompletionTime)
	}
}

// pause 在 Healthy/Degraded 上被忽略，且清掉一次性 Spec 意图。
func TestApplyDesiredActionPauseIgnoredOnTerminalHealth(t *testing.T) {
	ctx := context.Background()
	for _, phase := range []sdpv1alpha1.RolloutPhase{
		sdpv1alpha1.RolloutHealthy,
		sdpv1alpha1.RolloutDegraded,
	} {
		t.Run(string(phase), func(t *testing.T) {
			ro := intentRollout()
			ro.Status.Phase = phase
			ro.Spec.DesiredAction = sdpv1alpha1.RolloutActionPause
			r, c := newIntentReconciler(t, ro)

			if _, err := r.applyDesiredAction(ctx, ro); err != nil {
				t.Fatalf("apply: %v", err)
			}
			got := getRO(t, c)
			if got.Status.Phase == sdpv1alpha1.RolloutPaused {
				t.Error("pause must be ignored on terminal health phase")
			}
			if got.Spec.DesiredAction != "" {
				t.Errorf("ignored pause intent must be cleared, got %q", got.Spec.DesiredAction)
			}
			want := "pause ignored: rollout already " + string(phase)
			if got.Status.Message != want {
				t.Errorf("message = %q want %q", got.Status.Message, want)
			}
		})
	}
}

// 未知动作被拒且意图被清掉，不留悬挂的 Spec 字段。
func TestApplyDesiredActionUnknownIntentCleared(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	ro.Spec.DesiredAction = sdpv1alpha1.RolloutAction("frobnicate")
	r, c := newIntentReconciler(t, ro)

	if _, err := r.applyDesiredAction(ctx, ro); err != nil {
		t.Fatalf("apply: %v", err)
	}
	got := getRO(t, c)
	if got.Spec.DesiredAction != "" {
		t.Errorf("unknown intent must be cleared, got %q", got.Spec.DesiredAction)
	}
	if got.Status.Message != "ignored unknown rollout action frobnicate" {
		t.Errorf("message = %q", got.Status.Message)
	}
}
