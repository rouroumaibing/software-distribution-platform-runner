// Hub TLS CA propagation: the runner holds the platform CA (mounted from its
// own secret) and syncs it into each job namespace as a ConfigMap, so job
// pods trust the hub backend's self-signed TLS via SSL_CERT_FILE — curl in
// user pipeline scripts picks it up natively, zero script changes. No-op when
// no CA bundle is configured (real-PKI deployments trust the system pool).
package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/executor"
)

// ensureHubCAMap upserts the sdp-hub-ca ConfigMap in ns with the runner's CA
// bundle. Called right before Job creation so the pod mount never dangles.
func (r *TaskRunReconciler) ensureHubCAMap(ctx context.Context, ns string) error {
	bundle := r.JobBuilder.HubCABundle
	if bundle == "" {
		return nil
	}
	want := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: executor.HubCAConfigMapName, Namespace: ns},
		Data:       map[string]string{executor.HubCAKey: bundle},
	}
	var got corev1.ConfigMap
	err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: executor.HubCAConfigMapName}, &got)
	switch {
	case apierrors.IsNotFound(err):
		return r.Create(ctx, want)
	case err != nil:
		return err
	}
	if got.Data[executor.HubCAKey] == bundle {
		return nil
	}
	got.Data = want.Data
	return r.Update(ctx, &got)
}
