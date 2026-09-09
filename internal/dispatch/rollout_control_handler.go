package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"sigs.k8s.io/controller-runtime/pkg/client"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// Rollout control annotations — must match the keys the RolloutReconciler
// consumes (kept in sync by hand; both live in this repo's runner module).
const (
	annRolloutAction = "sdp.io/rollout-action"
	annRolloutOper   = "sdp.io/rollout-operator"
)

// RolloutControlHandler receives MessageRolloutControl frames and stamps the
// operator command onto the matching Rollout CR as an annotation. The
// RolloutReconciler observes the annotation, applies pause/promote/rollback
// to Status, and clears it — so all Rollout writes stay inside the
// reconciler (the handler never touches Status directly).
type RolloutControlHandler struct {
	Client client.Client
}

// Handle finds the Release task's Rollout and stamps the control command.
func (h *RolloutControlHandler) Handle(payload json.RawMessage) error {
	var p sdpv1alpha1.RolloutControlPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}
	if p.PipelineRunName == "" || p.TaskName == "" {
		return fmt.Errorf("dispatch: rollout control payload needs pipelineRunName and taskName")
	}
	switch p.Action {
	case sdpv1alpha1.RolloutActionPause, sdpv1alpha1.RolloutActionPromote, sdpv1alpha1.RolloutActionRollback:
	default:
		return fmt.Errorf("dispatch: unknown rollout action %q", p.Action)
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
		log.Printf("dispatch: no TaskRun for rollout control %s/%s (ignored)", p.PipelineRunName, p.TaskName)
		return nil
	}
	tr := list.Items[0]
	if tr.Status.RolloutRef == "" {
		log.Printf("dispatch: task %s/%s has no rollout yet (ignored)", p.PipelineRunName, p.TaskName)
		return nil
	}

	var ro sdpv1alpha1.Rollout
	key := client.ObjectKey{Namespace: tr.Spec.Namespace, Name: tr.Status.RolloutRef}
	if err := h.Client.Get(context.Background(), key, &ro); err != nil {
		return err
	}

	// Stamp the one-shot command; the reconciler consumes and clears it.
	base := ro.DeepCopy()
	if ro.Annotations == nil {
		ro.Annotations = map[string]string{}
	}
	ro.Annotations[annRolloutAction] = string(p.Action)
	if p.Operator != "" {
		ro.Annotations[annRolloutOper] = p.Operator
	}
	if err := h.Client.Patch(context.Background(), &ro, client.MergeFrom(base)); err != nil {
		return err
	}
	log.Printf("dispatch: rollout control %s stamped on %s/%s by %s",
		p.Action, ro.Namespace, ro.Name, p.Operator)
	return nil
}
