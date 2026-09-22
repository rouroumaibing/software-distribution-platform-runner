package dispatch

import (
	"context"
	"encoding/json"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

func mkTask(name, stage, mode string, deps ...string) sdpv1alpha1.PipelineTaskSpec {
	return sdpv1alpha1.PipelineTaskSpec{
		Name:          name,
		Type:          sdpv1alpha1.TaskTypeBuild,
		Stage:         stage,
		ExecutionMode: sdpv1alpha1.ExecutionMode(mode),
		DependsOn:     deps,
	}
}

func TestDownstreamTasksLinear(t *testing.T) {
	tasks := []sdpv1alpha1.PipelineTaskSpec{
		mkTask("a", "s", "Parallel"),
		mkTask("b", "s", "Parallel", "a"),
		mkTask("c", "s", "Parallel", "b"),
	}
	got := DownstreamTasks(tasks, "a")
	want := map[string]bool{"b": true, "c": true}
	if len(got) != 2 {
		t.Fatalf("expected 2 downstream, got %v", got)
	}
	for _, g := range got {
		if !want[g] {
			t.Fatalf("unexpected downstream %q", g)
		}
	}
	if len(DownstreamTasks(tasks, "c")) != 0 {
		t.Fatal("leaf has no downstream")
	}
}

func TestDownstreamTasksDiamond(t *testing.T) {
	tasks := []sdpv1alpha1.PipelineTaskSpec{
		mkTask("a", "s", "Parallel"),
		mkTask("b", "s", "Parallel", "a"),
		mkTask("c", "s", "Parallel", "a"),
		mkTask("d", "s", "Parallel", "b", "c"),
	}
	got := DownstreamTasks(tasks, "a")
	if len(got) != 3 {
		t.Fatalf("expected 3 downstream in diamond, got %v", got)
	}
}

func newRerunClient(objs ...client.Object) *RerunHandler {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = sdpv1alpha1.AddToScheme(scheme)
	b := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&sdpv1alpha1.TaskRun{}, &sdpv1alpha1.PipelineRun{})
	if len(objs) > 0 {
		b = b.WithObjects(objs...)
	}
	return &RerunHandler{Client: b.Build()}
}

func TestRerunHandlerResetsTargetAndDeletesDownstream(t *testing.T) {
	pr := &sdpv1alpha1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "pr1", Namespace: "ns"},
		Spec: sdpv1alpha1.PipelineRunSpec{
			Tasks: []sdpv1alpha1.PipelineTaskSpec{
				mkTask("a", "s", "Parallel"),
				mkTask("b", "s", "Parallel", "a"),
			},
		},
	}
	trA := &sdpv1alpha1.TaskRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pr1-a", Namespace: "ns",
			Labels: map[string]string{"sdp.io/pipeline-run": "pr1", "sdp.io/task": "a"},
		},
		Spec: sdpv1alpha1.TaskRunSpec{PipelineRunRef: "pr1", TaskName: "a", Namespace: "ns", Type: sdpv1alpha1.TaskTypeBuild},
		Status: sdpv1alpha1.TaskRunStatus{Phase: sdpv1alpha1.TaskRunFailed, JobRef: "job-a", RetryCount: 2},
	}
	trB := &sdpv1alpha1.TaskRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pr1-b", Namespace: "ns",
			Labels: map[string]string{"sdp.io/pipeline-run": "pr1", "sdp.io/task": "b"},
		},
		Spec: sdpv1alpha1.TaskRunSpec{PipelineRunRef: "pr1", TaskName: "b", Namespace: "ns", Type: sdpv1alpha1.TaskTypeBuild},
		Status: sdpv1alpha1.TaskRunStatus{Phase: sdpv1alpha1.TaskRunPending, JobRef: "job-b"},
	}

	h := newRerunClient(pr, trA, trB)
	payload, _ := json.Marshal(sdpv1alpha1.RerunTaskPayload{PipelineRunName: "pr1", TaskName: "a", Operator: "alice"})
	if err := h.Handle(payload); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}

	var gotA sdpv1alpha1.TaskRun
	if err := h.Client.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "pr1-a"}, &gotA); err != nil {
		t.Fatalf("get target: %v", err)
	}
	if gotA.Status.Phase != sdpv1alpha1.TaskRunPending {
		t.Errorf("target should be reset to Pending, got %s", gotA.Status.Phase)
	}
	if gotA.Status.JobRef != "" || gotA.Status.RetryCount != 0 {
		t.Errorf("target JobRef/RetryCount not cleared: jobRef=%q retries=%d", gotA.Status.JobRef, gotA.Status.RetryCount)
	}

	var gotB sdpv1alpha1.TaskRun
	err := h.Client.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: "pr1-b"}, &gotB)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("downstream TaskRun should be deleted, got err=%v phase=%s", err, gotB.Status.Phase)
	}
}
