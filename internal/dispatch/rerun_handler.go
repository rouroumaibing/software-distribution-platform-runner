package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// RerunHandler receives MessageRerunTask frames and asks the Runner to re-run
// a single task (plus, transitively, every downstream task that depends on
// it). It only stamps intents and deletes the downstream TaskRuns: the
// TaskRun and PipelineRun reconcilers consume the intents and are the sole
// writers of Status, so a rerun has one status write path.
type RerunHandler struct {
	Client client.Client
}

// Handle stamps rerun intents on the target TaskRun and its owning
// PipelineRun, then deletes downstream TaskRuns so the DAG recreates them.
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

	var pr sdpv1alpha1.PipelineRun
	if err := h.Client.Get(context.Background(),
		client.ObjectKey{Namespace: tr.Spec.Namespace, Name: tr.Spec.PipelineRunRef}, &pr); err != nil {
		return err
	}

	// Delete the target TaskRun instead of stamping a reset intent. Re-running
	// is a delete+recreate: the PipelineRun DAG reconciler sees no TaskRun for
	// this task and rebuilds it, while Job/Rollout are GC'd via ownerRef. A
	// two-phase intent (patch annotations, then reset Status in another API
	// write) loses the rerun forever if the Status update conflicts after the
	// annotation patch landed — deleting is a single atomic write with no such
	// window, and keeps hasFailedTask false so a revived run cannot fall back
	// to Failed before the replacement TaskRun reports in.
	if err := h.Client.Delete(context.Background(), &tr, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	basePR := pr.DeepCopy()
	if pr.Annotations == nil {
		pr.Annotations = map[string]string{}
	}
	pr.Annotations[sdpv1alpha1.AnnRerunRequested] = "true"
	pr.Annotations[sdpv1alpha1.AnnRerunOperator] = p.Operator
	if err := h.Client.Patch(context.Background(), &pr, client.MergeFrom(basePR)); err != nil {
		return err
	}

	downstream := DownstreamTasks(pr.Spec.Tasks, tr.Spec.TaskName)
	for _, name := range downstream {
		dt := &sdpv1alpha1.TaskRun{ObjectMeta: metav1.ObjectMeta{Name: pr.Name + "-" + name, Namespace: tr.Spec.Namespace}}
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
