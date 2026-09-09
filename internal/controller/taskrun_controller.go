// TaskRunReconciler is the only piece of code that actually talks to the
// Job API. Approval-type TaskRuns are deliberately left untouched here —
// they only advance when the Hub patches Status.Approval, relayed through
// the connector, not through anything this reconciler does.
package controller

import (
	"context"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/executor"
)

type TaskRunReconciler struct {
	client.Client
	JobBuilder *executor.JobBuilder
}

func (r *TaskRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var tr sdpv1alpha1.TaskRun
	if err := r.Get(ctx, req.NamespacedName, &tr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Approval 任务不创建 Job,只是挂起等待外部(Hub)决策。初始化
	// Status.Approval 让 PipelineRun 控制器能识别到"有节点在等审批"。
	if tr.Spec.Type == sdpv1alpha1.TaskTypeApproval {
		if tr.Status.Phase == "" {
			tr.Status.Phase = sdpv1alpha1.TaskRunPending
		}
		if tr.Status.Approval == nil {
			required := int32(1)
			if tr.Spec.ApprovalConfig != nil {
				required = tr.Spec.ApprovalConfig.RequiredApprovals
			}
			tr.Status.Approval = &sdpv1alpha1.ApprovalStatus{
				TaskName:      tr.Spec.TaskName,
				RequestedAt:   metav1.Now(),
				RequiredCount: required,
			}
		}
		// 一旦 Hub 已经通过 connector 把 Phase 改成 Succeeded/Failed,
		// 这里保持不动,让 DAG 继续推进。
		if tr.Status.Phase == sdpv1alpha1.TaskRunPending {
			return ctrl.Result{}, r.Status().Update(ctx, &tr)
		}
		return ctrl.Result{}, nil
	}

	// Release 任务若带了 Canary 子规格,交给 Rollout 控制器做渐进式发布:
	// 本 reconciler 创建/持有 Rollout CR 并 watch 它,Rollout 控制器推进金丝雀
	// 并回填 Rollout.Status,我们再映射回 TaskRun.Status。没有 Canary 的
	// Release(以及所有 Build 任务)走下方通用 Job 路径。
	if tr.Spec.Type == sdpv1alpha1.TaskTypeRelease && tr.Spec.RolloutSpec != nil {
		return r.reconcileDeploy(ctx, &tr)
	}

	// 已经有终态了,不用再处理。
	if tr.Status.Phase == sdpv1alpha1.TaskRunSucceeded || tr.Status.Phase == sdpv1alpha1.TaskRunFailed {
		return ctrl.Result{}, nil
	}

	// 还没建过 Job -> 建一个。
	if tr.Status.JobRef == "" {
		job := r.JobBuilder.Build(&tr)
		if err := controllerutil.SetControllerReference(&tr, job, r.Scheme()); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		tr.Status.Phase = sdpv1alpha1.TaskRunRunning
		tr.Status.JobRef = job.Name
		now := metav1.Now()
		tr.Status.StartTime = &now
		return ctrl.Result{}, r.Status().Update(ctx, &tr)
	}

	// 已经建过 Job -> 查它现在的状态,同步回 TaskRun。
	var job batchv1.Job
	if err := r.Get(ctx, client.ObjectKey{Namespace: tr.Spec.Namespace, Name: tr.Status.JobRef}, &job); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	switch {
	case job.Status.Succeeded > 0:
		tr.Status.Phase = sdpv1alpha1.TaskRunSucceeded
		now := metav1.Now()
		tr.Status.CompletionTime = &now

	case job.Status.Failed > 0:
		maxRetries := int32(0)
		if tr.Spec.RetryPolicy != nil {
			maxRetries = tr.Spec.RetryPolicy.MaxRetries
		}
		if tr.Status.RetryCount < maxRetries {
			// 还有重试次数:删掉旧 Job,清空 JobRef,下次 Reconcile 会重新创建。
			// 具体的退避等待(RetryPolicy.Backoff)通过 RequeueAfter 实现。
			if err := r.Delete(ctx, &job); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			tr.Status.RetryCount++
			tr.Status.JobRef = ""
			backoff := tr.Spec.RetryPolicy.Backoff.Duration
			if err := r.Status().Update(ctx, &tr); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: backoff}, nil
		}
		tr.Status.Phase = sdpv1alpha1.TaskRunFailed
		now := metav1.Now()
		tr.Status.CompletionTime = &now
	}

	return ctrl.Result{}, r.Status().Update(ctx, &tr)
}

// reconcileDeploy makes sure a Rollout CR exists for this Deploy task, then
// mirrors the Rollout's phase back onto the TaskRun. The actual canary
// stepping is performed by RolloutReconciler, which owns the stable/canary
// Deployments.
func (r *TaskRunReconciler) reconcileDeploy(ctx context.Context, tr *sdpv1alpha1.TaskRun) (ctrl.Result, error) {
	if tr.Spec.RolloutSpec == nil {
		tr.Status.Phase = sdpv1alpha1.TaskRunFailed
		tr.Status.Message = "deploy task has no rolloutSpec"
		return ctrl.Result{}, r.Status().Update(ctx, tr)
	}

	if tr.Status.RolloutRef == "" {
		ro := &sdpv1alpha1.Rollout{
			ObjectMeta: metav1.ObjectMeta{
				Name:      tr.Name,
				Namespace: tr.Spec.Namespace,
				Labels: map[string]string{
					"sdp.io/pipeline-run": tr.Spec.PipelineRunRef,
					"sdp.io/task-run":     tr.Name,
				},
			},
			Spec: *tr.Spec.RolloutSpec,
		}
		if err := controllerutil.SetControllerReference(tr, ro, r.Scheme()); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, ro); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		tr.Status.Phase = sdpv1alpha1.TaskRunRunning
		tr.Status.RolloutRef = ro.Name
		now := metav1.Now()
		tr.Status.StartTime = &now
		return ctrl.Result{}, r.Status().Update(ctx, tr)
	}

	var ro sdpv1alpha1.Rollout
	if err := r.Get(ctx, client.ObjectKey{Namespace: tr.Spec.Namespace, Name: tr.Status.RolloutRef}, &ro); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	switch ro.Status.Phase {
	case sdpv1alpha1.RolloutHealthy:
		tr.Status.Phase = sdpv1alpha1.TaskRunSucceeded
		now := metav1.Now()
		tr.Status.CompletionTime = &now
	case sdpv1alpha1.RolloutDegraded, sdpv1alpha1.RolloutRollingBack:
		tr.Status.Phase = sdpv1alpha1.TaskRunFailed
		tr.Status.Message = ro.Status.Message
		now := metav1.Now()
		tr.Status.CompletionTime = &now
	default:
		tr.Status.Phase = sdpv1alpha1.TaskRunRunning
	}
	return ctrl.Result{}, r.Status().Update(ctx, tr)
}

func (r *TaskRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&sdpv1alpha1.TaskRun{}).
		Owns(&batchv1.Job{}).         // Job 状态变化会触发父 TaskRun 重新 Reconcile
		Owns(&sdpv1alpha1.Rollout{}). // Rollout 状态变化会触发父 TaskRun 重新 Reconcile
		Complete(r)
}
