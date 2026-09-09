// PipelineRunReconciler implements the DAG-scheduling half of the
// execution flow discussed earlier: each Reconcile call finds whichever
// DAG nodes have all their DependsOn satisfied and creates a TaskRun for
// each of them, then re-triggers itself (via the owned-TaskRun watch)
// once those complete, until the whole PipelineRun reaches a terminal
// phase. It never talks to the Job API directly — TaskRunReconciler owns
// that.
package controller

import (
	"context"
	"log"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/connector"
)

type PipelineRunReconciler struct {
	client.Client

	// Conn, when set, lets the reconciler stream status updates back to the
	// Hub over the long connection. Left nil in unit tests / when the runner
	// is started without a Hub (e.g. a pure in-cluster CRD demo).
	Conn *connector.Client

	// ClusterName is this Runner's identity, copied into status reports so
	// the Hub can attribute the update (the Hub ultimately keys on the
	// connection's resolved cluster UUID, not this string).
	ClusterName string
}

// updateStatus persists the PipelineRun status and, if a Hub connection is
// wired, streams the same state up. Kept as one call so every reconcile path
// reports consistently instead of only the terminal ones.
func (r *PipelineRunReconciler) updateStatus(ctx context.Context, pr *sdpv1alpha1.PipelineRun) error {
	if err := r.Status().Update(ctx, pr); err != nil {
		return err
	}
	r.reportStatus(pr)
	return nil
}

// reportStatus best-effort streams the current PipelineRun phase + per-task
// summary to the Hub. Runs in a goroutine so a slow/unavailable connection
// never blocks the reconcile loop.
func (r *PipelineRunReconciler) reportStatus(pr *sdpv1alpha1.PipelineRun) {
	if r.Conn == nil {
		return
	}
	payload := &sdpv1alpha1.StatusUpdatePayload{
		ClusterID:            r.ClusterName,
		PipelineRunName:      pr.Name,
		PipelineRunNamespace: pr.Namespace,
		Phase:                pr.Status.Phase,
		Message:              pr.Status.Message,
		StartTime:            pr.Status.StartTime,
		CompletionTime:       pr.Status.CompletionTime,
		Tasks:                pr.Status.Tasks,
	}
	go func() {
		if err := r.Conn.Send(connector.MessageStatusUpdate, payload); err != nil {
			log.Printf("pipelinerun: status send failed: %v", err)
		}
	}()
}

func (r *PipelineRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pr sdpv1alpha1.PipelineRun
	if err := r.Get(ctx, req.NamespacedName, &pr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if pr.Status.Phase == "" {
		pr.Status.Phase = sdpv1alpha1.PipelineRunPending
	}
	if pr.Status.Phase == sdpv1alpha1.PipelineRunSucceeded ||
		pr.Status.Phase == sdpv1alpha1.PipelineRunFailed ||
		pr.Status.Phase == sdpv1alpha1.PipelineRunCancelled {
		return ctrl.Result{}, nil // 终态,不用再处理
	}

	var taskRuns sdpv1alpha1.TaskRunList
	if err := r.List(ctx, &taskRuns, client.InNamespace(pr.Namespace), client.MatchingLabels{
		"sdp.io/pipeline-run": pr.Name,
	}); err != nil {
		return ctrl.Result{}, err
	}

	existingByName := map[string]*sdpv1alpha1.TaskRun{}
	for i := range taskRuns.Items {
		tr := &taskRuns.Items[i]
		existingByName[tr.Spec.TaskName] = tr
	}

	// 汇总状态到 PipelineRun.Status.Tasks,供 kubectl / Hub 一次性看到全貌。
	pr.Status.Tasks = summarizeTasks(pr.Spec.Tasks, existingByName)

	// 有节点在等审批 -> 整个 PipelineRun 标记 WaitingApproval。
	if approval := findPendingApproval(pr.Spec.Tasks, existingByName); approval != nil {
		pr.Status.Phase = sdpv1alpha1.PipelineRunWaitingApproval
		pr.Status.CurrentApproval = approval
		return ctrl.Result{}, r.updateStatus(ctx, &pr)
	}
	pr.Status.CurrentApproval = nil

	// 有节点失败 -> 整个 PipelineRun 标记 Failed,不再调度新节点。
	if hasFailedTask(existingByName) {
		pr.Status.Phase = sdpv1alpha1.PipelineRunFailed
		return ctrl.Result{}, r.updateStatus(ctx, &pr)
	}

	// 找出依赖已全部 Succeeded、但还没创建 TaskRun 的节点,批量创建。
	runnable := findRunnableTasks(pr.Spec.Tasks, existingByName)
	for _, task := range runnable {
		tr := buildTaskRun(&pr, task)
		if err := controllerutil.SetControllerReference(&pr, tr, r.Scheme()); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, tr); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
	}

	if len(runnable) > 0 {
		pr.Status.Phase = sdpv1alpha1.PipelineRunRunning
	} else if allTasksSucceeded(pr.Spec.Tasks, existingByName) {
		pr.Status.Phase = sdpv1alpha1.PipelineRunSucceeded
		now := metav1.Now()
		pr.Status.CompletionTime = &now
	}

	return ctrl.Result{}, r.updateStatus(ctx, &pr)
}

// --- DAG 推进的核心逻辑,全部是纯函数,方便单测,不直接碰 K8s API ---

func findRunnableTasks(tasks []sdpv1alpha1.PipelineTaskSpec, existing map[string]*sdpv1alpha1.TaskRun) []sdpv1alpha1.PipelineTaskSpec {
	var runnable []sdpv1alpha1.PipelineTaskSpec
	for _, t := range tasks {
		if _, started := existing[t.Name]; started {
			continue
		}
		if dependenciesSatisfied(t.DependsOn, existing) {
			runnable = append(runnable, t)
		}
	}
	return runnable
}

func dependenciesSatisfied(dependsOn []string, existing map[string]*sdpv1alpha1.TaskRun) bool {
	for _, dep := range dependsOn {
		tr, ok := existing[dep]
		if !ok || tr.Status.Phase != sdpv1alpha1.TaskRunSucceeded {
			return false
		}
	}
	return true
}

func allTasksSucceeded(tasks []sdpv1alpha1.PipelineTaskSpec, existing map[string]*sdpv1alpha1.TaskRun) bool {
	for _, t := range tasks {
		tr, ok := existing[t.Name]
		if !ok || tr.Status.Phase != sdpv1alpha1.TaskRunSucceeded {
			return false
		}
	}
	return true
}

func hasFailedTask(existing map[string]*sdpv1alpha1.TaskRun) bool {
	for _, tr := range existing {
		if tr.Status.Phase == sdpv1alpha1.TaskRunFailed {
			return true
		}
	}
	return false
}

func findPendingApproval(tasks []sdpv1alpha1.PipelineTaskSpec, existing map[string]*sdpv1alpha1.TaskRun) *sdpv1alpha1.ApprovalStatus {
	for _, t := range tasks {
		if t.Type != sdpv1alpha1.TaskTypeApproval {
			continue
		}
		tr, ok := existing[t.Name]
		if !ok || tr.Status.Phase != sdpv1alpha1.TaskRunPending {
			continue
		}
		if tr.Status.Approval != nil {
			return tr.Status.Approval
		}
	}
	return nil
}

func summarizeTasks(tasks []sdpv1alpha1.PipelineTaskSpec, existing map[string]*sdpv1alpha1.TaskRun) []sdpv1alpha1.TaskRunStatusSummary {
	summaries := make([]sdpv1alpha1.TaskRunStatusSummary, 0, len(tasks))
	for _, t := range tasks {
		tr, ok := existing[t.Name]
		if !ok {
			summaries = append(summaries, sdpv1alpha1.TaskRunStatusSummary{Name: t.Name, Phase: sdpv1alpha1.TaskRunPending})
			continue
		}
		summaries = append(summaries, sdpv1alpha1.TaskRunStatusSummary{
			Name:           t.Name,
			TaskRunRef:     tr.Name,
			Phase:          tr.Status.Phase,
			RetryCount:     tr.Status.RetryCount,
			StartTime:      tr.Status.StartTime,
			CompletionTime: tr.Status.CompletionTime,
			Message:        tr.Status.Message,
		})
	}
	return summaries
}

func buildTaskRun(pr *sdpv1alpha1.PipelineRun, task sdpv1alpha1.PipelineTaskSpec) *sdpv1alpha1.TaskRun {
	var repo *sdpv1alpha1.RepoSource
	if task.Repo != nil {
		repo = task.Repo
	} else {
		repo = pr.Spec.Repo
	}

	return &sdpv1alpha1.TaskRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pr.Name + "-" + task.Name,
			Namespace: pr.Namespace,
			Labels: map[string]string{
				"sdp.io/pipeline-run": pr.Name,
				"sdp.io/task":         task.Name,
			},
		},
		Spec: sdpv1alpha1.TaskRunSpec{
			PipelineRunRef:     pr.Name,
			TaskName:           task.Name,
			Type:               task.Type,
			Image:              task.Image,
			ScriptPath:         task.ScriptPath,
			ScriptArgs:         task.ScriptArgs,
			Command:            task.Command,
			Args:               task.Args,
			Repo:               repo,
			Produces:           task.Produces,
			Consumes:           task.Consumes,
			Namespace:          pr.Spec.TargetNamespace,
			ServiceAccountName: pr.Spec.ServiceAccountName,
			TimeoutSeconds:     task.TimeoutSeconds,
			ApprovalConfig:     task.ApprovalConfig,
			RolloutSpec:        task.RolloutSpec,
		},
	}
}

func (r *PipelineRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&sdpv1alpha1.PipelineRun{}).
		Owns(&sdpv1alpha1.TaskRun{}). // TaskRun 状态变化会触发父 PipelineRun 重新 Reconcile
		Complete(r)
}
