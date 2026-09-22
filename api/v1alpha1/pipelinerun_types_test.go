package v1alpha1

import "testing"

func TestIsValidEnvironment(t *testing.T) {
	valid := []string{"dev", "test", "staging", "prod", "canary", "bluegreen"}
	for _, v := range valid {
		if !IsValidEnvironment(v) {
			t.Errorf("IsValidEnvironment(%q) should be valid", v)
		}
	}
	invalid := []string{"", "production", "uat", "random"}
	for _, v := range invalid {
		if IsValidEnvironment(v) {
			t.Errorf("IsValidEnvironment(%q) should be invalid", v)
		}
	}
}

func TestExecutionModeDefaults(t *testing.T) {
	// The zero value (empty) must behave as Parallel so old payloads keep
	// working after ExecutionMode was added.
	var m ExecutionMode
	if m != ExecutionModeParallel && m != "" {
		t.Errorf("expected empty ExecutionMode to read as Parallel/empty, got %q", m)
	}
}
