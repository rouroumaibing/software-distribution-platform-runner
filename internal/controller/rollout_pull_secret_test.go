package controller

import (
	"testing"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
)

// Private registries reject anonymous pulls. The generated canary/stable
// Deployments must carry the configured pull secret in their Pod spec: the run
// namespace is created dynamically, so patching its default SA at deploy time
// cannot work. These tests lock that in (and the public-registry no-op case).
func TestRolloutPullSecretsEnv(t *testing.T) {
	t.Setenv("SDP_ROLLOUT_IMAGE_PULL_SECRET", "  ")
	if got := rolloutPullSecrets(); got != nil {
		t.Fatalf("blank secret must inject nothing, got %v", got)
	}

	t.Setenv("SDP_ROLLOUT_IMAGE_PULL_SECRET", "  default-secret  ")
	got := rolloutPullSecrets()
	if len(got) != 1 || got[0].Name != "default-secret" {
		t.Fatalf("secret must be trimmed and injected, got %v", got)
	}
}

func TestBuildStableCarriesPullSecret(t *testing.T) {
	t.Setenv("SDP_ROLLOUT_IMAGE_PULL_SECRET", "default-secret")
	ro := &sdpv1alpha1.Rollout{}
	ro.Spec.WorkloadRef = "nginx-canary"
	ro.Spec.StableImage = "reg.example/nginx:v1"
	dep := (&RolloutReconciler{}).buildStable(ro, 1)
	got := dep.Spec.Template.Spec.ImagePullSecrets
	if len(got) != 1 || got[0].Name != "default-secret" {
		t.Fatalf("stable Deployment must carry pull secret, got %v", got)
	}
}

func TestBuildStableNoPullSecretWhenUnset(t *testing.T) {
	t.Setenv("SDP_ROLLOUT_IMAGE_PULL_SECRET", "")
	ro := &sdpv1alpha1.Rollout{}
	ro.Spec.WorkloadRef = "nginx-canary"
	ro.Spec.StableImage = "nginx:latest"
	dep := (&RolloutReconciler{}).buildStable(ro, 1)
	if got := dep.Spec.Template.Spec.ImagePullSecrets; got != nil {
		t.Fatalf("unset secret must not inject imagePullSecrets, got %v", got)
	}
}
