package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// deployRoleRules is the least-privilege rule set a Release/Build execution
// Job needs in the run namespace: deploy workloads + read pods/events for
// rollout status. Mirrors the SA shim the 2026-09-26 E2E applied by hand
// before G-1 was fixed (apps/deployments, core services/configmaps/secrets/
// pods, networking.k8s.io/ingresses).
var deployRoleRules = []rbacv1.PolicyRule{
	{
		APIGroups: []string{"apps"},
		Resources: []string{"deployments"},
		Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
	},
	{
		APIGroups: []string{""},
		Resources: []string{"services", "configmaps", "secrets", "pods", "pods/log"},
		Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
	},
	{
		APIGroups: []string{"networking.k8s.io"},
		Resources: []string{"ingresses"},
		Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
	},
}

// ensureRunRBAC (G-1, 2026-09-26) makes execution Jobs deploy-capable
// out-of-the-box: before the first TaskRun of a run is created in the target
// namespace, make sure (idempotently) that
//   - the ServiceAccount named by the run's spec exists,
//   - a least-privilege deploy Role exists, and
//   - a RoleBinding grants that Role to the SA.
//
// The hub injects spec.ServiceAccountName from SDP_JOB_SERVICE_ACCOUNT
// (default "sdp-deploy"); empty SA name keeps the legacy behavior (namespace
// default SA, no RBAC managed here).
func ensureRunRBAC(ctx context.Context, c client.Client, namespace, saName string) error {
	if saName == "" || namespace == "" {
		return nil
	}
	log := logf.FromContext(ctx).WithValues("namespace", namespace, "serviceAccount", saName)

	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: namespace}}
	if err := c.Create(ctx, sa); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	} else if err == nil {
		log.V(1).Info("created run service account")
	}

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "sdp-deploy", Namespace: namespace},
		Rules:      deployRoleRules,
	}
	if err := c.Create(ctx, role); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}

	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "sdp-deploy-" + saName, Namespace: namespace},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     "sdp-deploy",
		},
		Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: saName, Namespace: namespace}},
	}
	if err := c.Create(ctx, binding); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}
