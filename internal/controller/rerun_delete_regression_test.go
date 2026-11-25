package controller

import (
	"testing"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// A rerun deletes the target TaskRun (delete+recreate). Once it is gone from
// the observed set, the PipelineRun reconciler must no longer see a failed
// task (which used to flip the revived run straight back to Failed) and must
// schedule the task again. This locks in the invariant that makes the rerun
// delete strategy immune to the cross-controller race that regressed cancel-
// rerun: the reconcile after a revive must not fall back to Failed.
func TestRerunDeleteRemovesFailedTaskAndReschedules(t *testing.T) {
	tasks := []sdpv1alpha1.PipelineTaskSpec{
		mkTask("flaky", "s", "Parallel"),
	}

	// Before the handler deletes it: a Failed TaskRun makes hasFailedTask true,
	// which is exactly what flipped a revived PipelineRun back to Failed.
	before := map[string]*sdpv1alpha1.TaskRun{
		"flaky": {Status: sdpv1alpha1.TaskRunStatus{Phase: sdpv1alpha1.TaskRunFailed}},
	}
	if !hasFailedTask(before) {
		t.Fatal("precondition: a Failed TaskRun must be detected")
	}

	// After the handler deletes the target TaskRun, the DAG reconciler's
	// observed set no longer contains it: no fallback to Failed, and the task
	// becomes runnable so it is recreated.
	after := map[string]*sdpv1alpha1.TaskRun{}
	if hasFailedTask(after) {
		t.Fatal("deleted target must not count as a failed task")
	}
	runnable := findRunnableTasks(tasks, after)
	if len(runnable) != 1 || runnable[0].Name != "flaky" {
		t.Fatalf("deleted target must be rescheduled, got %v", names(runnable))
	}
}
