// RolloutReconciler implements progressive delivery for Deploy-type tasks.
// For each Rollout CR (created by the TaskRun controller) it manages a stable
// Deployment (WorkloadRef, StableImage) and a canary Deployment
// (WorkloadRef-canary, CanaryImage) behind a single SDP-managed Service, then
// walks the canary Steps — scaling the canary replica count to each step's
// weight, waiting for the canary pods to become Ready, and advancing. A
// canary Deployment that fails to make progress triggers an automatic
// rollback to 0% canary weight.
package controller

import (
	"context"
	"fmt"
	"log"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	sdpv1alpha1 "github.com/rouroumaibing/software-distribution-platform-runner/api/v1alpha1"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/canary"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/health"
	"github.com/rouroumaibing/software-distribution-platform-runner/pkg/metrics"
)

// PrometheusEndpoint is the base URL used to evaluate PrometheusQuery health
// checks. It's set from the environment by cmd/runner/main.go. When empty, a
// PrometheusQuery check is treated as "not yet evaluable" (the rollout holds
// at its current weight rather than advancing blind).
var PrometheusEndpoint string

// defaultRolloutReplicas is the replica count used for the stable workload
// when the RolloutSpec doesn't carry an explicit count (the runner never
// sees the live Deployment's replica count in M1).
const defaultRolloutReplicas int32 = 2

const (
	appLabel         = "app"
	rolloutLabel     = "sdp.io/rollout"
	managedByLabel   = "sdp.io/managed"
	managedByRollout = "rollout"
)

// Operator-control annotations. The hub relays pause/promote/rollback
// commands via the gateway; the dispatch handler stamps annRolloutAction on
// the Rollout, and the reconciler consumes it here (applying the command to
// Status) and clears it — keeping every Rollout write inside the reconciler.
const (
	annRolloutAction  = "sdp.io/rollout-action"  // one-shot command: pause | promote | rollback
	annRolloutPaused  = "sdp.io/rollout-paused"  // hold flag: stay at the current weight
	annRolloutAborted = "sdp.io/rollout-aborted" // terminal flag: canary back to 0
	annRolloutOper    = "sdp.io/rollout-operator"
)

// RolloutReconciler is registered with the manager in cmd/runner/main.go.
type RolloutReconciler struct {
	client.Client
}

func (r *RolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var ro sdpv1alpha1.Rollout
	if err := r.Get(ctx, req.NamespacedName, &ro); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Initialize status on first sight.
	if ro.Status.Phase == "" {
		ro.Status.Phase = sdpv1alpha1.RolloutProgressing
		ro.Status.CurrentStepIndex = 0
		ro.Status.CurrentWeight = 0
		now := metav1.Now()
		ro.Status.StartTime = &now
	}

	// Operator control: a pause/promote/rollback command stamped by the
	// dispatch handler takes precedence over the automatic walk. It is
	// applied here and the annotation cleared (a metadata update, which
	// re-triggers this reconcile so the hold flags below take over).
	if acted, err := r.applyControlCommand(ctx, &ro); err != nil || acted {
		return ctrl.Result{RequeueAfter: healthCheckInterval}, err
	}

	// Terminal hold: an operator rollback pins the canary at 0% and marks
	// the rollout Degraded; nothing else progresses after that.
	if ro.Annotations[annRolloutAborted] == "true" {
		total := r.ensureWorkload(ctx, &ro)
		canaryDep := r.ensureCanary(ctx, &ro, total)
		r.applyDecision(ctx, &ro, canary.Decision{
			Phase:          sdpv1alpha1.RolloutDegraded,
			CurrentWeight:  0,
			CanaryReplicas: 0,
			StableReplicas: total,
			Message:        "rolled back by operator",
		}, canaryDep, total)
		ro.Status.Phase = sdpv1alpha1.RolloutDegraded
		ro.Status.CurrentWeight = 0
		ro.Status.CanaryReplicas = 0
		ro.Status.StableReplicas = total
		now := metav1.Now()
		if ro.Status.CompletionTime == nil {
			ro.Status.CompletionTime = &now
		}
		return ctrl.Result{}, r.Status().Update(ctx, &ro)
	}

	// Paused hold: stay at the current step/weight until promoted.
	if ro.Annotations[annRolloutPaused] == "true" {
		total := r.ensureWorkload(ctx, &ro)
		canaryDep := r.ensureCanary(ctx, &ro, total)
		weight := ro.Status.CurrentWeight
		r.applyDecision(ctx, &ro, canary.Decision{
			Phase:            sdpv1alpha1.RolloutPaused,
			CurrentStepIndex: ro.Status.CurrentStepIndex,
			CurrentWeight:    weight,
			CanaryReplicas:   weightReplicas(weight, total),
			StableReplicas:   total - weightReplicas(weight, total),
			Message:          "paused by operator",
		}, canaryDep, total)
		ro.Status.Phase = sdpv1alpha1.RolloutPaused
		ro.Status.Message = "paused by operator"
		return ctrl.Result{RequeueAfter: healthCheckInterval}, r.Status().Update(ctx, &ro)
	}

	total := r.ensureWorkload(ctx, &ro)

	canaryDep := r.ensureCanary(ctx, &ro, total)
	canaryReady, canaryDesired := canaryReplicaState(canaryDep)

	// Health gate: PodReady readiness is the baseline; HTTPProbe /
	// PrometheusQuery add an active check (B-05). The result flows into the
	// canary engine so it won't advance a step the canary hasn't actually
	// passed.
	healthy := canaryReady >= canaryDesired && canaryDesired > 0
	healthy = canaryHealthy(&ro, canaryReady, canaryDesired, healthy)

	// Auto-rollback: a canary Deployment that can't make progress (image pull
	// error, crashloop, etc.) is a hard failure regardless of weight.
	if canaryDeploymentHasReplicaFailure(canaryDep) && ro.Spec.AutoRollback {
		decision := canary.RollbackDecision(&ro, total)
		r.applyDecision(ctx, &ro, decision, canaryDep, total)
		ro.Status.Phase = sdpv1alpha1.RolloutDegraded
		now := metav1.Now()
		ro.Status.CompletionTime = &now
		metrics.Alert("rollout", ro.Name, "canary replica failure")
		if err := r.Status().Update(ctx, &ro); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	decision := canary.Evaluate(&ro, canaryReady, canaryDesired, total, healthy)
	r.applyDecision(ctx, &ro, decision, canaryDep, total)

	ro.Status.Phase = decision.Phase
	ro.Status.CurrentStepIndex = decision.CurrentStepIndex
	ro.Status.CurrentWeight = decision.CurrentWeight
	ro.Status.StableReplicas = decision.StableReplicas
	ro.Status.CanaryReplicas = decision.CanaryReplicas
	ro.Status.ConsecutiveFailures = decision.ConsecutiveFailures
	ro.Status.Message = decision.Message
	now := metav1.Now()
	ro.Status.LastHealthCheckTime = &now
	if decision.Phase == sdpv1alpha1.RolloutHealthy && ro.Status.CompletionTime == nil {
		ro.Status.CompletionTime = &now
	}
	metrics.SetRolloutWeight(decision.CurrentWeight)
	if decision.Phase == sdpv1alpha1.RolloutDegraded || decision.Phase == sdpv1alpha1.RolloutRollingBack {
		metrics.Alert("rollout", ro.Name, decision.Message)
	}

	if err := r.Status().Update(ctx, &ro); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: decision.RequeueAfter}, nil
}

// applyDecision scales the stable/canary Deployments to match the decision
// and ensures an SDP-managed Service routes to both.
func (r *RolloutReconciler) applyDecision(ctx context.Context, ro *sdpv1alpha1.Rollout, d canary.Decision, canaryDep *appsv1.Deployment, total int32) {
	stable := r.getStable(ctx, ro)
	if stable != nil {
		r.scaleDeployment(ctx, stable, d.StableReplicas)
	}
	if canaryDep != nil {
		r.scaleDeployment(ctx, canaryDep, d.CanaryReplicas)
	}
	_ = r.ensureService(ctx, ro)
	r.ensureCanaryIngress(ctx, ro, d.CurrentWeight)
}

// ensureWorkload makes sure the stable Deployment exists and returns the
// effective total replica count. Resolution order (B-06 — prefer the real
// replica count over the historical constant 2):
//  1. RolloutSpec.Replicas, if set by the hub;
//  2. the live stable Deployment's spec.replicas, if it already exists;
//  3. defaultRolloutReplicas.
func (r *RolloutReconciler) ensureWorkload(ctx context.Context, ro *sdpv1alpha1.Rollout) int32 {
	var liveReplicas *int32
	dep := r.getStable(ctx, ro)
	if dep != nil && dep.Spec.Replicas != nil {
		liveReplicas = dep.Spec.Replicas
	}
	total := resolveTotalReplicas(ro.Spec.Replicas, liveReplicas)
	if dep == nil {
		stable := r.buildStable(ro, total)
		if err := controllerutil.SetControllerReference(ro, stable, r.Scheme()); err != nil {
			log.Printf("rollout: set owner on stable failed: %v", err)
		}
		if err := r.Create(ctx, stable); err != nil && !apierrors.IsAlreadyExists(err) {
			log.Printf("rollout: create stable failed: %v", err)
		}
	}
	return total
}

// resolveTotalReplicas is the pure resolution step behind ensureWorkload so
// it can be unit-tested without a cluster (B-06).
func resolveTotalReplicas(specReplicas, liveReplicas *int32) int32 {
	if specReplicas != nil {
		return *specReplicas
	}
	if liveReplicas != nil {
		return *liveReplicas
	}
	return defaultRolloutReplicas
}

func (r *RolloutReconciler) getStable(ctx context.Context, ro *sdpv1alpha1.Rollout) *appsv1.Deployment {
	var dep appsv1.Deployment
	key := client.ObjectKey{Namespace: ro.Namespace, Name: ro.Spec.WorkloadRef}
	if err := r.Get(ctx, key, &dep); err != nil {
		return nil
	}
	return &dep
}

func (r *RolloutReconciler) buildStable(ro *sdpv1alpha1.Rollout, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ro.Spec.WorkloadRef,
			Namespace: ro.Namespace,
			Labels: map[string]string{
				appLabel:       ro.Spec.WorkloadRef,
				rolloutLabel:   ro.Spec.WorkloadRef,
				managedByLabel: managedByRollout,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: ro.Spec.WorkloadRef}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{appLabel: ro.Spec.WorkloadRef}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "app", Image: ro.Spec.StableImage},
					},
				},
			},
		},
	}
}

func (r *RolloutReconciler) ensureCanary(ctx context.Context, ro *sdpv1alpha1.Rollout, total int32) *appsv1.Deployment {
	name := ro.Spec.WorkloadRef + "-canary"
	var dep appsv1.Deployment
	err := r.Get(ctx, client.ObjectKey{Namespace: ro.Namespace, Name: name}, &dep)
	if err == nil {
		return &dep
	}
	if !apierrors.IsNotFound(err) {
		log.Printf("rollout: get canary failed: %v", err)
		return nil
	}
	dep = appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ro.Namespace,
			Labels: map[string]string{
				appLabel:       ro.Spec.WorkloadRef,
				rolloutLabel:   ro.Spec.WorkloadRef,
				managedByLabel: managedByRollout,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(0),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: ro.Spec.WorkloadRef}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{appLabel: ro.Spec.WorkloadRef}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "app", Image: ro.Spec.CanaryImage},
					},
				},
			},
		},
	}
	if err := controllerutil.SetControllerReference(ro, &dep, r.Scheme()); err != nil {
		log.Printf("rollout: set owner on canary failed: %v", err)
	}
	if err := r.Create(ctx, &dep); err != nil && !apierrors.IsAlreadyExists(err) {
		log.Printf("rollout: create canary failed: %v", err)
		return nil
	}
	return &dep
}

// ensureService creates a single ClusterIP Service that selects both the
// stable and canary pods (shared app label), so traffic splits by replica
// ratio — the DeploymentWeight routing mechanism. IngressCanary routing is
// recorded but, in M1, falls back to the same replica split (a dedicated
// canary Ingress resource is a documented follow-up).
func (r *RolloutReconciler) ensureService(ctx context.Context, ro *sdpv1alpha1.Rollout) *corev1.Service {
	name := ro.Spec.WorkloadRef + "-sdp"
	var svc corev1.Service
	err := r.Get(ctx, client.ObjectKey{Namespace: ro.Namespace, Name: name}, &svc)
	if err == nil {
		return &svc
	}
	if !apierrors.IsNotFound(err) {
		return nil
	}
	svc = corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ro.Namespace,
			Labels: map[string]string{
				rolloutLabel:   ro.Spec.WorkloadRef,
				managedByLabel: managedByRollout,
			},
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: map[string]string{appLabel: ro.Spec.WorkloadRef},
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 80, TargetPort: intstr.FromInt(8080)},
			},
		},
	}
	if err := controllerutil.SetControllerReference(ro, &svc, r.Scheme()); err != nil {
		log.Printf("rollout: set owner on service failed: %v", err)
	}
	if err := r.Create(ctx, &svc); err != nil && !apierrors.IsAlreadyExists(err) {
		log.Printf("rollout: create service failed: %v", err)
		return nil
	}
	return &svc
}

func (r *RolloutReconciler) scaleDeployment(ctx context.Context, dep *appsv1.Deployment, replicas int32) {
	if dep.Spec.Replicas != nil && *dep.Spec.Replicas == replicas {
		return
	}
	dep.Spec.Replicas = int32Ptr(replicas)
	if err := r.Update(ctx, dep); err != nil {
		log.Printf("rollout: scale %s failed: %v", dep.Name, err)
	}
}

// ensureCanaryIngress creates (or updates) a dedicated canary Ingress for
// IngressCanary traffic routing, annotated with the current canary weight
// (ingress-nginx style). M1 still performs the real traffic split via
// DeploymentWeight replica ratio (a canary Ingress alone can't split without
// a matching primary Ingress and host), so this resource is the documented
// B-04 deliverable: it exists and tracks weight, and operators can wire the
// primary Ingress to the same host to activate precise percentage routing.
// It's a no-op unless TrafficRouting.Type == IngressCanary (and the
// canary-ingress feature flag is on).
func (r *RolloutReconciler) ensureCanaryIngress(ctx context.Context, ro *sdpv1alpha1.Rollout, weight int32) {
	if ro.Spec.TrafficRouting.Type != sdpv1alpha1.TrafficRoutingIngressCanary {
		return
	}
	if !metrics.CanaryIngressEnabled {
		return
	}
	name := ro.Spec.WorkloadRef + "-sdp-canary"
	host := fmt.Sprintf("%s.sdp.local", ro.Spec.WorkloadRef)
	path := ro.Spec.TrafficRouting.IngressRef
	if path == "" {
		path = "/"
	}
	annotations := map[string]string{
		"kubernetes.io/ingress.class":               "nginx",
		"nginx.ingress.kubernetes.io/canary":        "true",
		"nginx.ingress.kubernetes.io/canary-weight": fmt.Sprintf("%d", weight),
	}
	var ing networkingv1.Ingress
	err := r.Get(ctx, client.ObjectKey{Namespace: ro.Namespace, Name: name}, &ing)
	if apierrors.IsNotFound(err) {
		ing = networkingv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ro.Namespace,
				Labels: map[string]string{
					rolloutLabel:   ro.Spec.WorkloadRef,
					managedByLabel: managedByRollout,
				},
				Annotations: annotations,
			},
			Spec: canaryIngressSpec(host, path, ro.Spec.WorkloadRef, ro.Namespace),
		}
		if err := controllerutil.SetControllerReference(ro, &ing, r.Scheme()); err != nil {
			log.Printf("rollout: set owner on canary ingress failed: %v", err)
		}
		if err := r.Create(ctx, &ing); err != nil && !apierrors.IsAlreadyExists(err) {
			log.Printf("rollout: create canary ingress failed: %v", err)
		}
		return
	}
	if err != nil {
		log.Printf("rollout: get canary ingress failed: %v", err)
		return
	}
	// Update weight in place (keep host/path stable).
	base := ing.DeepCopy()
	if ing.Annotations == nil {
		ing.Annotations = map[string]string{}
	}
	ing.Annotations["nginx.ingress.kubernetes.io/canary-weight"] = fmt.Sprintf("%d", weight)
	if err := r.Patch(ctx, &ing, client.MergeFrom(base)); err != nil {
		log.Printf("rollout: patch canary ingress weight failed: %v", err)
	}
}

func canaryIngressSpec(host, path, workloadRef, namespace string) networkingv1.IngressSpec {
	prefix := networkingv1.PathTypePrefix
	return networkingv1.IngressSpec{
		Rules: []networkingv1.IngressRule{
			{
				Host: host,
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{
						Paths: []networkingv1.HTTPIngressPath{
							{
								Path:     path,
								PathType: &prefix,
								Backend: networkingv1.IngressBackend{
									Service: &networkingv1.IngressServiceBackend{
										Name: workloadRef + "-sdp",
										Port: networkingv1.ServiceBackendPort{Number: 80},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func (r *RolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&sdpv1alpha1.Rollout{}).
		Owns(&appsv1.Deployment{}). // Deployment status changes trigger re-eval
		Owns(&corev1.Service{}).
		Owns(&networkingv1.Ingress{}). // canary Ingress (B-04) GCs with the Rollout
		Complete(r)
}

// --- helpers ---

// applyControlCommand consumes a one-shot operator command stamped in
// annRolloutAction by the dispatch handler (relayed from the hub). It applies
// the command to Status and clears the annotation, returning true when a
// command was processed. Keep semantics aligned with the canary engine:
// pause holds at the current weight, promote advances one step (or completes
// the rollout past the last step), rollback reverts to 0% canary and aborts.
func (r *RolloutReconciler) applyControlCommand(ctx context.Context, ro *sdpv1alpha1.Rollout) (bool, error) {
	action := ro.Annotations[annRolloutAction]
	if action == "" {
		return false, nil
	}
	operator := ro.Annotations[annRolloutOper]
	clear := func() error {
		patch := client.MergeFrom(ro.DeepCopy())
		delete(ro.Annotations, annRolloutAction)
		delete(ro.Annotations, annRolloutOper)
		return r.Patch(ctx, ro, patch)
	}
	if err := clear(); err != nil {
		return true, err
	}

	by := " by operator"
	if operator != "" {
		by = " by " + operator
	}

	switch sdpv1alpha1.RolloutAction(action) {
	case sdpv1alpha1.RolloutActionPause:
		// Pausing a finished/degraded rollout is a no-op; otherwise hold.
		if ro.Status.Phase == sdpv1alpha1.RolloutHealthy || ro.Status.Phase == sdpv1alpha1.RolloutDegraded {
			ro.Status.Message = "pause ignored: rollout already " + string(ro.Status.Phase)
		} else {
			patchMeta := client.MergeFrom(ro.DeepCopy())
			if ro.Annotations == nil {
				ro.Annotations = map[string]string{}
			}
			ro.Annotations[annRolloutPaused] = "true"
			if err := r.Patch(ctx, ro, patchMeta); err != nil {
				return true, err
			}
			ro.Status.Phase = sdpv1alpha1.RolloutPaused
			ro.Status.Message = "paused" + by
		}
	case sdpv1alpha1.RolloutActionPromote:
		// Promoting an aborted rollout would undo the rollback — refuse.
		if ro.Annotations[annRolloutAborted] == "true" {
			ro.Status.Message = "promote ignored: rollout was rolled back"
			break
		}
		// Clear the paused hold.
		if ro.Annotations[annRolloutPaused] == "true" {
			patchMeta := client.MergeFrom(ro.DeepCopy())
			delete(ro.Annotations, annRolloutPaused)
			if err := r.Patch(ctx, ro, patchMeta); err != nil {
				return true, err
			}
		}
		steps := len(ro.Spec.Steps)
		if int(ro.Status.CurrentStepIndex) >= steps {
			// Already past the last step: promoting completes the rollout.
			ro.Status.Phase = sdpv1alpha1.RolloutHealthy
			now := metav1.Now()
			if ro.Status.CompletionTime == nil {
				ro.Status.CompletionTime = &now
			}
			ro.Status.Message = "promoted to completion" + by
		} else {
			// Advance one step; pre-set the weight to the next step's target
			// so the engine scales straight to it (same convention as
			// canary.Evaluate's "healthy at weight, advancing" branch).
			next := ro.Status.CurrentStepIndex + 1
			weight := ro.Status.CurrentWeight
			if int(next) < steps {
				if sw := ro.Spec.Steps[next].SetWeight; sw != nil {
					weight = *sw
				}
			}
			ro.Status.CurrentStepIndex = next
			ro.Status.CurrentWeight = weight
			ro.Status.Phase = sdpv1alpha1.RolloutProgressing
			ro.Status.Message = "promoted" + by
		}
	case sdpv1alpha1.RolloutActionRollback:
		patchMeta := client.MergeFrom(ro.DeepCopy())
		if ro.Annotations == nil {
			ro.Annotations = map[string]string{}
		}
		delete(ro.Annotations, annRolloutPaused)
		ro.Annotations[annRolloutAborted] = "true"
		if err := r.Patch(ctx, ro, patchMeta); err != nil {
			return true, err
		}
		ro.Status.Phase = sdpv1alpha1.RolloutRollingBack
		ro.Status.Message = "rollback requested" + by
	default:
		ro.Status.Message = "ignored unknown rollout action " + action
	}

	if err := r.Status().Update(ctx, ro); err != nil {
		return true, err
	}
	return true, nil
}

func weightReplicas(weight, total int32) int32 {
	if total <= 0 {
		return 0
	}
	return int32((int64(weight) * int64(total)) / 100)
}

// canaryHealthy folds the active HealthCheck result into the PodReady
// baseline. podsReady is the replica-readiness gate; for HTTPProbe /
// PrometheusQuery the probe must also pass (B-05). When the probe can't be
// evaluated (no Prometheus endpoint configured) it's treated as not-yet-
// healthy so the rollout holds rather than advancing blind.
func canaryHealthy(ro *sdpv1alpha1.Rollout, canaryReady, canaryDesired int32, podsReady bool) bool {
	if !podsReady {
		return false
	}
	hc := ro.Spec.HealthCheck
	timeout := time.Duration(hc.IntervalSeconds) * time.Second
	switch hc.Type {
	case sdpv1alpha1.HealthCheckHTTPProbe:
		path := hc.HTTPPath
		if path == "" {
			path = "/"
		}
		url := fmt.Sprintf("http://%s-sdp.%s.svc.cluster.local:80%s",
			ro.Spec.WorkloadRef, ro.Namespace, path)
		return health.HTTPProbe(url, timeout)
	case sdpv1alpha1.HealthCheckPrometheusQuery:
		if PrometheusEndpoint == "" {
			log.Printf("rollout: PrometheusQuery health check has no endpoint configured; holding")
			return false
		}
		return health.PrometheusQueryOK(PrometheusEndpoint, hc.PrometheusQueryExpr, hc.PrometheusThreshold, timeout)
	default: // PodReady
		return true
	}
}

// healthCheckInterval paces the paused/hold requeue so operator state stays
// reflected (and status fresh) without hot-looping the reconciler.
const healthCheckInterval = 30 * time.Second

func canaryReplicaState(dep *appsv1.Deployment) (ready, desired int32) {
	if dep == nil {
		return 0, 0
	}
	ready = dep.Status.AvailableReplicas
	if dep.Spec.Replicas != nil {
		desired = *dep.Spec.Replicas
	}
	return
}

// canaryDeploymentHasReplicaFailure reports whether the canary Deployment is
// stuck (image pull error / crashloop) — a hard failure that should roll back.
func canaryDeploymentHasReplicaFailure(dep *appsv1.Deployment) bool {
	if dep == nil {
		return false
	}
	for _, c := range dep.Status.Conditions {
		if c.Type == appsv1.DeploymentReplicaFailure && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func int32Ptr(v int32) *int32 { return &v }
