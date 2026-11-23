package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"sigs.k8s.io/controller-runtime/pkg/client"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// RolloutControlHandler receives MessageRolloutControl frames and writes the
// operator command to Rollout.Spec.DesiredAction. The RolloutReconciler is
// the only writer of Rollout status: it consumes the intent, applies it to
// Status and clears one-shot commands.
type RolloutControlHandler struct {
	Client client.Client
}

// Handle finds the Release task's Rollout and records the control intent.
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

	// The Spec field is the single write path for control intent. Legacy
	// annotations are read by the reconciler for compatibility only.
	base := ro.DeepCopy()
	ro.Spec.DesiredAction = p.Action
	ro.Spec.DesiredActionOperator = p.Operator
	if err := h.Client.Patch(context.Background(), &ro, client.MergeFrom(base)); err != nil {
		return err
	}
	log.Printf("dispatch: rollout control %s written on %s/%s by %s",
		p.Action, ro.Namespace, ro.Name, p.Operator)
	return nil
}
