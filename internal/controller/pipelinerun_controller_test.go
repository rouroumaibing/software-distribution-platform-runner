package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
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

func TestFindRunnableTasksSerialOneAtATime(t *testing.T) {
	tasks := []sdpv1alpha1.PipelineTaskSpec{
		mkTask("a", "stage1", "Serial"),
		mkTask("b", "stage1", "Serial"),
		mkTask("c", "stage1", "Serial"),
	}
	existing := map[string]*sdpv1alpha1.TaskRun{}
	runnable := findRunnableTasks(tasks, existing)
	if len(runnable) != 1 || runnable[0].Name != "a" {
		t.Fatalf("serial stage should schedule only the first task, got %v", names(runnable))
	}

	// a succeeded -> b becomes runnable.
	existing["a"] = &sdpv1alpha1.TaskRun{Status: sdpv1alpha1.TaskRunStatus{Phase: sdpv1alpha1.TaskRunSucceeded}}
	runnable = findRunnableTasks(tasks, existing)
	if len(runnable) != 1 || runnable[0].Name != "b" {
		t.Fatalf("after a succeeds, only b should run, got %v", names(runnable))
	}

	// a and b succeeded -> c runs.
	existing["b"] = &sdpv1alpha1.TaskRun{Status: sdpv1alpha1.TaskRunStatus{Phase: sdpv1alpha1.TaskRunSucceeded}}
	runnable = findRunnableTasks(tasks, existing)
	if len(runnable) != 1 || runnable[0].Name != "c" {
		t.Fatalf("after a,b succeed, only c should run, got %v", names(runnable))
	}
}

func TestFindRunnableTasksParallelAllAtOnce(t *testing.T) {
	tasks := []sdpv1alpha1.PipelineTaskSpec{
		mkTask("a", "stage1", "Parallel"),
		mkTask("b", "stage1", "Parallel"),
	}
	runnable := findRunnableTasks(tasks, map[string]*sdpv1alpha1.TaskRun{})
	if len(runnable) != 2 {
		t.Fatalf("parallel stage should schedule all, got %v", names(runnable))
	}
}

func TestFindRunnableTasksSerialWaitsForEarlierSibling(t *testing.T) {
	// Within a Serial stage, a later sibling must wait even if its own
	// DependsOn (elsewhere) is satisfied.
	tasks := []sdpv1alpha1.PipelineTaskSpec{
		mkTask("a", "stage1", "Serial"),
		mkTask("b", "stage1", "Serial"),
	}
	existing := map[string]*sdpv1alpha1.TaskRun{
		// a hasn't succeeded yet.
		"a": {Status: sdpv1alpha1.TaskRunStatus{Phase: sdpv1alpha1.TaskRunRunning}},
	}
	runnable := findRunnableTasks(tasks, existing)
	if len(runnable) != 0 {
		t.Fatalf("b must wait for a (serial), got %v", names(runnable))
	}
}

func names(ts []sdpv1alpha1.PipelineTaskSpec) []string {
	out := make([]string, 0, len(ts))
	for _, t := range ts {
		out = append(out, t.Name)
	}
	return out
}

// stubSender captures status frames pushed during resync (C-05).
type stubSender struct {
	sent []string
}

func (s *stubSender) Send(t connectorMsgType, payload any) error {
	if p, ok := payload.(*sdpv1alpha1.StatusUpdatePayload); ok {
		s.sent = append(s.sent, p.PipelineRunName)
	}
	return nil
}

// connectorMsgType aliases the connector's MessageType so the test doesn't
// import the connector package just for the interface shape.
type connectorMsgType = sdpv1alpha1.MessageType

func TestResyncAllSendsOnlyNonTerminal(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = sdpv1alpha1.AddToScheme(scheme)

	running := &sdpv1alpha1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "pr-running", Namespace: "ns", Labels: map[string]string{"sdp.io/target": "tgt"}},
		Status:     sdpv1alpha1.PipelineRunStatus{Phase: sdpv1alpha1.PipelineRunRunning},
	}
	done := &sdpv1alpha1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "pr-done", Namespace: "ns", Labels: map[string]string{"sdp.io/target": "tgt"}},
		Status:     sdpv1alpha1.PipelineRunStatus{Phase: sdpv1alpha1.PipelineRunSucceeded},
	}
	other := &sdpv1alpha1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "pr-other", Namespace: "ns", Labels: map[string]string{"sdp.io/target": "elsewhere"}},
		Status:     sdpv1alpha1.PipelineRunStatus{Phase: sdpv1alpha1.PipelineRunRunning},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(running, done, other).Build()
	s := &stubSender{}
	if err := ResyncAll(context.Background(), c, s, "tgt"); err != nil {
		t.Fatalf("ResyncAll error: %v", err)
	}
	if len(s.sent) != 1 || s.sent[0] != "pr-running" {
		t.Fatalf("resync should send only the non-terminal run for this target, got %v", s.sent)
	}
}
