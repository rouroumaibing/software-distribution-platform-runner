package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RolloutPhase represents the lifecycle phase of a Rollout.
type RolloutPhase string

const (
	RolloutProgressing RolloutPhase = "Progressing"
	RolloutPaused      RolloutPhase = "Paused"
	RolloutHealthy     RolloutPhase = "Healthy"
	RolloutDegraded    RolloutPhase = "Degraded"
	RolloutRollingBack RolloutPhase = "RollingBack"
)

// TrafficRoutingType selects how canary traffic is split.
type TrafficRoutingType string

const (
	// TrafficRoutingDeploymentWeight splits traffic by adjusting the ratio
	// of stable vs. canary Pod replicas behind a single Service selector.
	TrafficRoutingDeploymentWeight TrafficRoutingType = "DeploymentWeight"
	// TrafficRoutingIngressCanary uses an Ingress controller's native
	// canary-weight annotation (e.g. ingress-nginx) for precise percentage
	// splits independent of replica count.
	TrafficRoutingIngressCanary TrafficRoutingType = "IngressCanary"
)

// CanaryStep is one step of a progressive rollout: shift weight, then
// optionally pause before the controller evaluates health and proceeds.
type CanaryStep struct {
	// SetWeight is the percentage of traffic (0-100) routed to the canary version.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	SetWeight *int32 `json:"setWeight,omitempty"`

	// Pause holds the rollout at the current weight. Omit both SetWeight
	// and Pause is invalid; a step must do at least one of the two.
	Pause *PauseStep `json:"pause,omitempty"`
}

// PauseStep defines how long to pause at a given step.
type PauseStep struct {
	// Duration to pause for; if omitted, pause is indefinite until resumed
	// externally (e.g. by an operator approving progression, relayed via
	// the hub patching Rollout.Spec or a resume annotation).
	Duration *metav1.Duration `json:"duration,omitempty"`
}

// HealthCheckType selects how the rollout controller judges canary health
// at each step before advancing to the next one.
type HealthCheckType string

const (
	HealthCheckPodReady        HealthCheckType = "PodReady"
	HealthCheckHTTPProbe       HealthCheckType = "HTTPProbe"
	HealthCheckPrometheusQuery HealthCheckType = "PrometheusQuery"
)

// HealthCheckSpec defines how canary health is evaluated at each step.
type HealthCheckSpec struct {
	// +kubebuilder:validation:Enum=PodReady;HTTPProbe;PrometheusQuery
	Type HealthCheckType `json:"type"`

	// IntervalSeconds between health evaluations.
	// +kubebuilder:default=30
	IntervalSeconds int64 `json:"intervalSeconds,omitempty"`

	// FailureThreshold is the number of consecutive failed checks before
	// the controller triggers a rollback (if AutoRollback is true).
	// +kubebuilder:default=3
	FailureThreshold int32 `json:"failureThreshold,omitempty"`

	// HTTPPath is used when Type == HTTPProbe, evaluated against the
	// canary Pods directly.
	HTTPPath string `json:"httpPath,omitempty"`

	// PrometheusQueryExpr / PrometheusThreshold are used when
	// Type == PrometheusQuery, e.g. checking canary error rate against a
	// configured ceiling.
	PrometheusQueryExpr string `json:"prometheusQueryExpr,omitempty"`
	PrometheusThreshold string `json:"prometheusThreshold,omitempty"`
}

// TrafficRoutingSpec defines how traffic is actually split between
// stable and canary versions of the workload.
type TrafficRoutingSpec struct {
	// +kubebuilder:validation:Enum=DeploymentWeight;IngressCanary
	Type TrafficRoutingType `json:"type"`

	// IngressRef is required when Type == IngressCanary.
	IngressRef string `json:"ingressRef,omitempty"`
}

// RolloutSpec defines the desired state of a Rollout.
// Created by the PipelineRun/TaskRun controller for Deploy-type tasks, or
// directly by an operator for a standalone progressive deployment.
type RolloutSpec struct {
	// WorkloadRef is the name of the Deployment being progressively updated.
	WorkloadRef string `json:"workloadRef"`

	StableImage string `json:"stableImage"`
	CanaryImage string `json:"canaryImage"`

	// Steps defines the full progressive delivery plan, evaluated in order.
	// +kubebuilder:validation:MinItems=1
	Steps []CanaryStep `json:"steps"`

	HealthCheck HealthCheckSpec `json:"healthCheck"`

	TrafficRouting TrafficRoutingSpec `json:"trafficRouting"`

	// AutoRollback, when true, automatically reverts to 0% canary weight
	// once HealthCheck.FailureThreshold consecutive checks fail.
	// +kubebuilder:default=true
	AutoRollback bool `json:"autoRollback,omitempty"`
}

// RolloutStatus defines the observed state of a Rollout.
type RolloutStatus struct {
	// +kubebuilder:validation:Enum=Progressing;Paused;Healthy;Degraded;RollingBack
	Phase RolloutPhase `json:"phase,omitempty"`

	CurrentStepIndex int32 `json:"currentStepIndex,omitempty"`
	CurrentWeight    int32 `json:"currentWeight,omitempty"`

	StableReplicas int32 `json:"stableReplicas,omitempty"`
	CanaryReplicas int32 `json:"canaryReplicas,omitempty"`

	ConsecutiveFailures int32 `json:"consecutiveFailures,omitempty"`

	LastHealthCheckTime *metav1.Time `json:"lastHealthCheckTime,omitempty"`
	StartTime           *metav1.Time `json:"startTime,omitempty"`
	CompletionTime      *metav1.Time `json:"completionTime,omitempty"`

	Message string `json:"message,omitempty"`

	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Weight",type=integer,JSONPath=`.status.currentWeight`
// +kubebuilder:printcolumn:name="Workload",type=string,JSONPath=`.spec.workloadRef`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Rollout is the Schema for the rollouts API.
type Rollout struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RolloutSpec   `json:"spec,omitempty"`
	Status RolloutStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RolloutList contains a list of Rollout.
type RolloutList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Rollout `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Rollout{}, &RolloutList{})
}
