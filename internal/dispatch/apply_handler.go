// Package dispatch holds the connector message handlers that turn Hub-
// originated commands into concrete actions in this Runner's cluster.
// Keeping them here (not in cmd/runner/main.go) keeps main.go a thin wiring
// layer and makes each handler unit-testable with a fake client.Client.
package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// errMissingName is returned when a dispatch payload omits the CR name, which
// the Hub always sets to match its pipeline_runs row (CRName).
var errMissingName = errors.New("dispatch: ApplyPipelineRunPayload.Name is required")

// ApplyHandler receives MessageApplyPipelineRun frames and materializes a
// PipelineRun CR in this cluster. Once the CR exists, the PipelineRun
// controller takes over and schedules TaskRuns — this handler only does the
// "create the root object" hop.
type ApplyHandler struct {
	Client      client.Client
	ClusterName string
}

// Handle decodes the payload and creates the PipelineRun CR (idempotently:
// if a CR with the same name already exists, it's left as-is so a Hub resend
// after a reconnect doesn't double-schedule work).
func (h *ApplyHandler) Handle(payload json.RawMessage) error {
	log.Printf("dispatch: apply handler invoked, payloadBytes=%d", len(payload))
	var p sdpv1alpha1.ApplyPipelineRunPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}
	if p.Name == "" {
		return errMissingName
	}
	ns := p.Namespace
	if ns == "" {
		ns = "sdp-run"
	}

	if err := h.ensureNamespace(context.Background(), ns); err != nil {
		return err
	}

	pr := &sdpv1alpha1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.Name,
			Namespace: ns,
			Labels: map[string]string{
				"sdp.io/cluster": h.ClusterName,
			},
			Annotations: map[string]string{
				"sdp.io/cluster": h.ClusterName,
			},
		},
		Spec: p.Spec,
	}

	existing := &sdpv1alpha1.PipelineRun{}
	err := h.Client.Get(context.Background(), client.ObjectKey{Name: p.Name, Namespace: ns}, existing)
	if err == nil {
		log.Printf("dispatch: PipelineRun %s/%s already exists, skipping create", ns, p.Name)
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	if err := h.Client.Create(context.Background(), pr); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}
	log.Printf("dispatch: created PipelineRun %s/%s (%d tasks) on cluster %s",
		ns, p.Name, len(p.Spec.Tasks), h.ClusterName)
	return nil
}

func (h *ApplyHandler) ensureNamespace(ctx context.Context, name string) error {
	ns := &corev1.Namespace{}
	err := h.Client.Get(ctx, client.ObjectKey{Name: name}, ns)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	ns = &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"sdp.io/managed": "true",
			},
		},
	}
	if err := h.Client.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}
