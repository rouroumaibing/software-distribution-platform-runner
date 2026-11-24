package dispatch

import (
	"context"
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// The approval handler only appends to the intent annotation; it never
// writes TaskRun status. The reconciler consumes the intent into Status.
func TestApproverHandlerStampsIntentOnly(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = sdpv1alpha1.AddToScheme(scheme)
	tr := &sdpv1alpha1.TaskRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pr-approval", Namespace: "ns",
			Labels: map[string]string{"sdp.io/pipeline-run": "pr", "sdp.io/task": "approval"},
		},
		Spec:   sdpv1alpha1.TaskRunSpec{PipelineRunRef: "pr", TaskName: "approval", Namespace: "ns", Type: sdpv1alpha1.TaskTypeApproval},
		Status: sdpv1alpha1.TaskRunStatus{Phase: sdpv1alpha1.TaskRunPending},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&sdpv1alpha1.TaskRun{}).WithObjects(tr).Build()
	h := &ApproverHandler{Client: c}

	for _, approver := range []string{"alice", "bob"} {
		payload, _ := json.Marshal(sdpv1alpha1.ApproveTaskPayload{
			PipelineRunName: "pr", TaskName: "approval", Approver: approver,
		})
		if err := h.Handle(payload); err != nil {
			t.Fatalf("Handle(%s): %v", approver, err)
		}
	}
	// A duplicate decision must not append again.
	payload, _ := json.Marshal(sdpv1alpha1.ApproveTaskPayload{
		PipelineRunName: "pr", TaskName: "approval", Approver: "alice",
	})
	if err := h.Handle(payload); err != nil {
		t.Fatalf("duplicate Handle: %v", err)
	}

	var got sdpv1alpha1.TaskRun
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "pr-approval"}, &got); err != nil {
		t.Fatalf("get taskrun: %v", err)
	}
	var decisions []sdpv1alpha1.ApprovalDecisionRecord
	if err := json.Unmarshal([]byte(got.Annotations[sdpv1alpha1.AnnApprovalDecision]), &decisions); err != nil {
		t.Fatalf("decode intent: %v", err)
	}
	if len(decisions) != 2 {
		t.Errorf("decisions = %v, want two distinct approvers", decisions)
	}
	// Assert the exact identities, not just the count. A `== -> !=` mutation
	// on the idempotency check would still land on two records (alice twice,
	// bob skipped) and slip past a count-only assertion.
	approvers := map[string]bool{}
	for _, d := range decisions {
		approvers[d.Approver] = true
	}
	if !approvers["alice"] || !approvers["bob"] {
		t.Errorf("decisions = %v, want both alice and bob", decisions)
	}
	if got.Status.Approval != nil || got.Status.Phase != sdpv1alpha1.TaskRunPending {
		t.Errorf("handler must not write status: approval=%v phase=%s", got.Status.Approval, got.Status.Phase)
	}
}
