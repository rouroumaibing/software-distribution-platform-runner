package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := sdpv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// Each payload field is independently required. A missing taskName with a
// present pipelineRunName (and vice versa) must be rejected — this is the
// short-circuit branch that a `|| -> &&` mutation would silently break.
func TestApproveAndRerunRequireBothIdentityFields(t *testing.T) {
	cases := []struct {
		name     string
		pr, task string
	}{
		{"missing task", "pr", ""},
		{"missing pipeline run", "", "task"},
	}
	for _, tc := range cases {
		t.Run("approve/"+tc.name, func(t *testing.T) {
			h := &ApproverHandler{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).Build()}
			payload, _ := json.Marshal(sdpv1alpha1.ApproveTaskPayload{
				PipelineRunName: tc.pr, TaskName: tc.task, Approver: "alice",
			})
			err := h.Handle(payload)
			if err == nil || !strings.Contains(err.Error(), "needs pipelineRunName and taskName") {
				t.Fatalf("Handle = %v, want identity-fields error", err)
			}
		})
		t.Run("rerun/"+tc.name, func(t *testing.T) {
			h := &RerunHandler{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).Build()}
			payload, _ := json.Marshal(sdpv1alpha1.RerunTaskPayload{
				PipelineRunName: tc.pr, TaskName: tc.task, Operator: "alice",
			})
			err := h.Handle(payload)
			if err == nil || !strings.Contains(err.Error(), "needs pipelineRunName and taskName") {
				t.Fatalf("Handle = %v, want identity-fields error", err)
			}
		})
	}
}

// A Delete failure that is NOT NotFound must abort the rerun. Ignoring it
// would leave a stale downstream TaskRun and corrupt the DAG replay.
func TestRerunSurfacesNonNotFoundDeleteError(t *testing.T) {
	pr := &sdpv1alpha1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "pr1", Namespace: "ns"},
		Spec: sdpv1alpha1.PipelineRunSpec{Tasks: []sdpv1alpha1.PipelineTaskSpec{
			mkTask("a", "s", "Parallel"), mkTask("b", "s", "Parallel", "a"),
		}},
	}
	trA := &sdpv1alpha1.TaskRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pr1-a", Namespace: "ns",
			Labels: map[string]string{"sdp.io/pipeline-run": "pr1", "sdp.io/task": "a"},
		},
		Spec: sdpv1alpha1.TaskRunSpec{PipelineRunRef: "pr1", TaskName: "a", Namespace: "ns"},
	}
	deleteBoom := errors.New("delete boom")
	funcs := interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if strings.HasSuffix(obj.GetName(), "-b") {
				return deleteBoom
			}
			return c.Delete(ctx, obj, opts...)
		},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithStatusSubresource(&sdpv1alpha1.TaskRun{}, &sdpv1alpha1.PipelineRun{}).
		WithObjects(pr, trA).WithInterceptorFuncs(funcs).Build()
	h := &RerunHandler{Client: c}
	payload, _ := json.Marshal(sdpv1alpha1.RerunTaskPayload{PipelineRunName: "pr1", TaskName: "a"})
	err := h.Handle(payload)
	if !errors.Is(err, deleteBoom) {
		t.Fatalf("Handle err = %v, want delete error surfaced", err)
	}
}

// NotFound on downstream delete is benign (already gone) and must not fail
// the rerun — the complementary branch to the test above.
func TestRerunIgnoresNotFoundDelete(t *testing.T) {
	pr := &sdpv1alpha1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "pr2", Namespace: "ns"},
		Spec: sdpv1alpha1.PipelineRunSpec{Tasks: []sdpv1alpha1.PipelineTaskSpec{
			mkTask("a", "s", "Parallel"), mkTask("b", "s", "Parallel", "a"),
		}},
	}
	trA := &sdpv1alpha1.TaskRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pr2-a", Namespace: "ns",
			Labels: map[string]string{"sdp.io/pipeline-run": "pr2", "sdp.io/task": "a"},
		},
		Spec: sdpv1alpha1.TaskRunSpec{PipelineRunRef: "pr2", TaskName: "a", Namespace: "ns"},
	}
	nf := apierrors.NewNotFound(schema.GroupResource{Group: "pipeline.sdp.io", Resource: "taskruns"}, "pr2-b")
	funcs := interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if strings.HasSuffix(obj.GetName(), "-b") {
				return nf
			}
			return c.Delete(ctx, obj, opts...)
		},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithStatusSubresource(&sdpv1alpha1.TaskRun{}, &sdpv1alpha1.PipelineRun{}).
		WithObjects(pr, trA).WithInterceptorFuncs(funcs).Build()
	h := &RerunHandler{Client: c}
	payload, _ := json.Marshal(sdpv1alpha1.RerunTaskPayload{PipelineRunName: "pr2", TaskName: "a"})
	if err := h.Handle(payload); err != nil {
		t.Fatalf("Handle should ignore NotFound delete, got: %v", err)
	}
}

// A failed Patch must propagate — swallowing it would report an approval that
// was never persisted.
func TestApproveSurfacesPatchError(t *testing.T) {
	tr := &sdpv1alpha1.TaskRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pr-a", Namespace: "ns",
			Labels: map[string]string{"sdp.io/pipeline-run": "pr", "sdp.io/task": "a"},
		},
		Spec: sdpv1alpha1.TaskRunSpec{PipelineRunRef: "pr", TaskName: "a", Namespace: "ns"},
	}
	patchBoom := errors.New("patch boom")
	funcs := interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			return patchBoom
		},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithStatusSubresource(&sdpv1alpha1.TaskRun{}).
		WithObjects(tr).WithInterceptorFuncs(funcs).Build()
	h := &ApproverHandler{Client: c}
	payload, _ := json.Marshal(sdpv1alpha1.ApproveTaskPayload{
		PipelineRunName: "pr", TaskName: "a", Approver: "alice",
	})
	if err := h.Handle(payload); !errors.Is(err, patchBoom) {
		t.Fatalf("Handle err = %v, want patch error surfaced", err)
	}
}

// Illegal hex characters in a snapshot must fail validation — a `|| -> &&`
// mutation here would accept garbage digests as valid v1 snapshots.
func TestIsSHA256HexRejectsIllegalChars(t *testing.T) {
	bad := []string{
		strings.Repeat("g", 64),       // non-hex letter
		strings.Repeat("a", 63) + "z", // trailing non-hex
		strings.Repeat("A", 64),       // uppercase is not lowercase hex
		strings.Repeat("a", 32) + "-" + strings.Repeat("a", 31),
		strings.Repeat("a", 63), // wrong length
	}
	for _, s := range bad {
		if isSHA256Hex(s) {
			t.Errorf("isSHA256Hex accepted invalid value %q", s)
		}
	}
	if !isSHA256Hex(strings.Repeat("a", 64)) {
		t.Error("isSHA256Hex rejected a valid lowercase hex digest")
	}
}
