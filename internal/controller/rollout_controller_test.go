package controller

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

func TestResolveTotalReplicas(t *testing.T) {
	tests := []struct {
		name string
		spec *int32
		live *int32
		want int32
	}{
		{"spec wins", pi32(5), pi32(3), 5},
		{"live when no spec", nil, pi32(4), 4},
		{"default when neither", nil, nil, defaultRolloutReplicas},
		{"spec over live", pi32(7), pi32(2), 7},
	}
	for _, tc := range tests {
		got := resolveTotalReplicas(tc.spec, tc.live)
		if got != tc.want {
			t.Errorf("%s: resolveTotalReplicas=%d want %d", tc.name, got, tc.want)
		}
	}
}

func TestResolveServiceTargetPort(t *testing.T) {
	depWithPort := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{
		Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Ports: []corev1.ContainerPort{{ContainerPort: 80}}}},
		}}},
	}
	depNoPort := &appsv1.Deployment{}
	tests := []struct {
		name    string
		stable  *appsv1.Deployment
		envPort int32
		want    int32
	}{
		{"env overrides all", depWithPort, 9090, 9090},
		{"infer from stable container port", depWithPort, 0, 80},
		{"fallback when no declared port", depNoPort, 0, 8080},
		{"fallback when stable nil", nil, 0, 8080},
	}
	for _, tc := range tests {
		if got := resolveServiceTargetPort(tc.stable, tc.envPort); got != tc.want {
			t.Errorf("%s: resolveServiceTargetPort=%d want %d", tc.name, got, tc.want)
		}
	}
}

func pi32(v int32) *int32 { return &v }
