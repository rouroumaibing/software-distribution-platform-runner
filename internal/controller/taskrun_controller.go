// TaskRunReconciler is the only piece of code that actually talks to the
// Job API. Approval-type TaskRuns are deliberately left untouched here —
// they only advance when the Hub patches Status.Approval, relayed through
// the connector, not through anything this reconciler does.
package controller

import (
	"context"
	"io"
	"log"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/connector"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/executor"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/logstream"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/metrics"
)

type TaskRunReconciler struct {
	client.Client
	JobBuilder *executor.JobBuilder

	// Conn, when set, streams live pod logs back to the Hub (B-02). Nil in
	// unit tests / standalone CRD demos.
	Conn *connector.Client
	// Clientset is the typed K8s client used to tail pod logs (the
	// controller-runtime client can't stream logs). Nil disables streaming.
	Clientset kubernetes.Interface
	// TargetName is this Runner's identity, copied into log chunks so the
	// Hub can attribute them.
	TargetName string

	// streaming tracks in-flight log streamers so a single Job isn't tailed
	// by multiple goroutines across reconciles.
	streaming sync.Map
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
		// Kick off live log streaming for this Job (B-02). Best-effort: a
		// missing pod / connection just means no live logs for this run.
		r.maybeStreamLogs(&tr)
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
		metrics.RecordTaskRunPhase(string(tr.Spec.Type), string(tr.Status.Phase))
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
		metrics.RecordTaskRunPhase(string(tr.Spec.Type), string(tr.Status.Phase))
		metrics.Alert("taskrun", tr.Spec.TaskName, tr.Status.Message)
		now := metav1.Now()
		tr.Status.CompletionTime = &now
	}

	return ctrl.Result{}, r.Status().Update(ctx, &tr)
}

// connLogSender adapts connector.Client to logstream.Sender (B-02).
type connLogSender struct{ conn *connector.Client }

func (s connLogSender) SendLogChunk(p sdpv1alpha1.LogChunkPayload) error {
	return s.conn.Send(connector.MessageLogChunk, p)
}

// maybeStreamLogs launches a goroutine that tails the TaskRun's Job pod and
// ships log chunks to the Hub — but only once per Job (guarded by the
// in-memory streaming map). It's a no-op when Conn/Clientset are unset or
// log streaming is disabled by the SDP_LOG_STREAMING flag (B-09 downgrade).
func (r *TaskRunReconciler) maybeStreamLogs(tr *sdpv1alpha1.TaskRun) {
	if r.Conn == nil || r.Clientset == nil || !metrics.LogStreamingEnabled {
		return
	}
	if tr.Status.JobRef == "" {
		return
	}
	key := tr.Spec.PipelineRunRef + "/" + tr.Status.JobRef
	if _, loaded := r.streaming.LoadOrStore(key, true); loaded {
		return
	}
	go func() {
		defer r.streaming.Delete(key)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sender := connLogSender{conn: r.Conn}
		p := sdpv1alpha1.LogChunkPayload{
			TargetID:        r.TargetName,
			PipelineRunName: tr.Spec.PipelineRunRef,
			TaskName:        tr.Spec.TaskName,
		}
		// The pod may not be scheduled yet; retry briefly before giving up.
		var lastErr error
		for attempt := 0; attempt < 10; attempt++ {
			_, rc, ok := r.jobPodLog(ctx, tr)
			if ok {
				streamErr := logstream.StreamLogs(ctx, rc, sender, p)
				if streamErr != nil {
					log.Printf("taskrun: log stream ended for %s: %v", tr.Status.JobRef, streamErr)
				}
				return
			}
			if lastErr != nil {
				log.Printf("taskrun: waiting for pod log %s (attempt %d): %v", tr.Status.JobRef, attempt, lastErr)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}()
}

// jobPodLog finds the first pod belonging to the TaskRun's Job and opens its
// log stream. Returns (podName, stream, true) on success.
func (r *TaskRunReconciler) jobPodLog(ctx context.Context, tr *sdpv1alpha1.TaskRun) (string, io.ReadCloser, bool) {
	pods, err := r.Clientset.CoreV1().Pods(tr.Spec.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "job-name=" + tr.Status.JobRef,
	})
	if err != nil || len(pods.Items) == 0 {
		return "", nil, false
	}
	pod := pods.Items[0]
	req := r.Clientset.CoreV1().Pods(tr.Spec.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{})
	rc, err := req.Stream(ctx)
	if err != nil {
		return "", nil, false
	}
	return pod.Name, rc, true
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
