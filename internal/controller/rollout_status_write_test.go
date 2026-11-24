package controller

import (
	"context"
	"testing"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// 锁定 Status/Spec 写入隔离的本质不变量：
// Status 是观测、Spec 是意图，二者不得共用同一个写对象。Spec/metadata 的
// Patch 走副本，Status 写走 fresh Get。这样"先写 Status 再 Patch Spec"的
// 顺序不再影响结果——从机制上消除 Patch 响应覆盖未落盘 Status 的可能。

// 不变量一：patchRollout 修改 Spec 后，调用方对象上已设置的 Status 不受影响。
// 旧实现直接 r.Patch(ctx, ro, ...)，服务端响应会把 ro.Status 反序列化覆盖，
// 内存里未落盘的 Message 丢失。
func TestPatchRolloutDoesNotClobberInMemoryStatus(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	r, c := newIntentReconciler(t, ro)

	ro.Status.Message = "set before spec patch"
	ro.Status.CurrentWeight = 7

	if err := r.patchRollout(ctx, ro, func(p *sdpv1alpha1.Rollout) {
		p.Spec.DesiredAction = sdpv1alpha1.RolloutActionPromote
	}); err != nil {
		t.Fatalf("patchRollout: %v", err)
	}

	if ro.Status.Message != "set before spec patch" {
		t.Errorf("in-memory Status.Message clobbered by spec patch: got %q", ro.Status.Message)
	}
	if ro.Status.CurrentWeight != 7 {
		t.Errorf("in-memory Status.CurrentWeight clobbered: got %d", ro.Status.CurrentWeight)
	}

	// 副本 Patch 确实改到了服务端的 Spec。
	got := getRO(t, c)
	if got.Spec.DesiredAction != sdpv1alpha1.RolloutActionPromote {
		t.Errorf("spec patch not persisted: got %q", got.Spec.DesiredAction)
	}
}

// 不变量二：Spec Patch 之后 commitStatus 仍能写入完整 Status，且不被前序
// Spec 写覆盖。这验证 fresh Get 用最新 resourceVersion，避免 stale rv 冲突。
func TestCommitStatusAfterSpecPatchPersistsBothWrites(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	r, _ := newIntentReconciler(t, ro)

	if err := r.patchRollout(ctx, ro, func(p *sdpv1alpha1.Rollout) {
		p.Spec.DesiredAction = sdpv1alpha1.RolloutActionRollback
	}); err != nil {
		t.Fatalf("patchRollout: %v", err)
	}
	ro.Status.Phase = sdpv1alpha1.RolloutRolledBack
	ro.Status.Message = "rolled back by operator"
	if err := r.commitStatus(ctx, ro); err != nil {
		t.Fatalf("commitStatus after spec patch: %v", err)
	}

	got := getRO(t, r.Client)
	if got.Spec.DesiredAction != sdpv1alpha1.RolloutActionRollback {
		t.Errorf("spec write lost: got %q", got.Spec.DesiredAction)
	}
	if got.Status.Phase != sdpv1alpha1.RolloutRolledBack || got.Status.Message != "rolled back by operator" {
		t.Errorf("status write lost: phase=%s message=%q", got.Status.Phase, got.Status.Message)
	}
}

// 场景回归：pause 在 Healthy 态被忽略时，Spec intent 被清、Status.Message 被写。
// 旧实现"先写 Message 再 clearDesiredAction(Patch)"导致 Message 丢失；机制隔离
// 后该顺序不再重要，这里仍按最坏顺序（先 struct 字段赋值在 Patch 之前）验证。
func TestApplyDesiredActionPauseIgnoredKeepsMessageWithSpecClear(t *testing.T) {
	ctx := context.Background()
	ro := intentRollout()
	ro.Spec.DesiredAction = sdpv1alpha1.RolloutActionPause
	ro.Spec.DesiredActionOperator = "alice"
	ro.Status.Phase = sdpv1alpha1.RolloutHealthy
	r, _ := newIntentReconciler(t, ro)

	acted, err := r.applyDesiredAction(ctx, ro)
	if err != nil || !acted {
		t.Fatalf("applyDesiredAction: acted=%v err=%v", acted, err)
	}

	got := getRO(t, r.Client)
	if got.Spec.DesiredAction != "" {
		t.Errorf("one-shot intent not cleared: %q", got.Spec.DesiredAction)
	}
	want := "pause ignored: rollout already " + string(sdpv1alpha1.RolloutHealthy)
	if got.Status.Message != want {
		t.Errorf("message lost across spec clear: got %q want %q", got.Status.Message, want)
	}
}
