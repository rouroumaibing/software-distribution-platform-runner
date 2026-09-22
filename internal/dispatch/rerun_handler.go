package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// RerunHandler receives MessageRerunTask frames and re-runs a single task
// (plus, transitively, every downstream task that depends on it) without
// re-dispatching the whole PipelineRun. This complements the whole-run
// redispatch path: operators get surgical reruns of a flaky/failed node.
type RerunHandler struct {
	Client client.Client
}

// Handle resets the target TaskRun and deletes its downstream dependents.
func (h *RerunHandler) Handle(payload json.RawMessage) error {
	var p sdpv1alpha1.RerunTaskPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}
	if p.PipelineRunName == "" || p.TaskName == "" {
		return fmt.Errorf("dispatch: rerun payload needs pipelineRunName and taskName")
	}

	var list sdpv1alpha1.TaskRunList
	if err := h.Client.List(context.Background(), &list,
		client.MatchingLabels{
			"sdp.io/pipeline-run": p.PipelineRunName,
			"sdp.io/task":         p.TaskName,
		},
	); err != nil {
		return err
	}
	if len(list.Items) == 0 {
		log.Printf("dispatch: no TaskRun for rerun %s/%s (ignored)", p.PipelineRunName, p.TaskName)
		return nil
	}
	tr := list.Items[0]

	// Load the owning PipelineRun so we have the full DAG to compute
	// transitive downstream dependents.
	var pr sdpv1alpha1.PipelineRun
	if err := h.Client.Get(context.Background(),
		client.ObjectKey{Namespace: tr.Spec.Namespace, Name: tr.Spec.PipelineRunRef}, &pr); err != nil {
		return err
	}

	// Tear down the target's concrete work (Job/Rollout) before clearing
	// status so a half-built object isn't orphaned.
	if tr.Status.JobRef != "" {
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: tr.Spec.Namespace, Name: tr.Status.JobRef}}
		if err := h.Client.Delete(context.Background(), job); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	if tr.Status.RolloutRef != "" {
		ro := &sdpv1alpha1.Rollout{ObjectMeta: metav1.ObjectMeta{Namespace: tr.Spec.Namespace, Name: tr.Status.RolloutRef}}
		if err := h.Client.Delete(context.Background(), ro); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}

	// Reset the target TaskRun's status so the TaskRun reconciler rebuilds
	// its Job/Rollout from scratch.
	tr.Status.Phase = sdpv1alpha1.TaskRunPending
	tr.Status.JobRef = ""
	tr.Status.RolloutRef = ""
	tr.Status.PodName = ""
	tr.Status.RetryCount = 0
	tr.Status.ExitCode = nil
	tr.Status.StartTime = nil
	tr.Status.CompletionTime = nil
	tr.Status.Message = "rerun requested"
	tr.Status.Approval = nil
	if err := h.Client.Status().Update(context.Background(), &tr); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	// Delete downstream dependents so the DAG recreates them once the target
	// succeeds again. Cascade GC removes their Jobs/Rollouts.
	downstream := DownstreamTasks(pr.Spec.Tasks, tr.Spec.TaskName)
	for _, name := range downstream {
		dt := &sdpv1alpha1.TaskRun{ObjectMeta: metav1.ObjectMeta{Namespace: tr.Spec.Namespace, Name: pr.Name + "-" + name}}
		if err := h.Client.Delete(context.Background(), dt); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}

	log.Printf("dispatch: rerun %s/%s (downstream=%d) by %s",
		p.PipelineRunName, p.TaskName, len(downstream), p.Operator)
	return nil
}

// DownstreamTasks returns, in no particular guaranteed order, the names of
// every task that (directly or transitively) depends on taskName, derived
// from the DependsOn edges in tasks. Pure and unit-tested (C-07).
func DownstreamTasks(tasks []sdpv1alpha1.PipelineTaskSpec, taskName string) []string {
	// dependents[task] = tasks that list task in their DependsOn.
	dependents := map[string][]string{}
	for _, t := range tasks {
		for _, dep := range t.DependsOn {
			dependents[dep] = append(dependents[dep], t.Name)
		}
	}
	seen := map[string]bool{}
	var out []string
	queue := []string{taskName}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, d := range dependents[cur] {
			if seen[d] {
				continue
			}
			seen[d] = true
			out = append(out, d)
			queue = append(queue, d)
		}
	}
	return out
}
