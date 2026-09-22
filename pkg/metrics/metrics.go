// Package metrics exposes the Runner's observability surface (B-09): a small
// set of Prometheus counters/gauges plus feature flags that act as downgrade
// switches for heavier behaviors (live log streaming, canary Ingress
// creation, alert emission). Everything here is best-effort — a disabled
// flag means the corresponding code path is skipped entirely, so a missing
// /metrics scrape or a Prometheus outage never blocks the reconcile loop.
package metrics

import (
	"log"
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Feature flags double as downgrade switches. They default to ON; set the
// matching env var to "false" to turn a behavior off without a redeploy.
//
//	SDP_LOG_STREAMING   -> live pod log streaming to the hub
//	SDP_CANARY_INGRESS  -> create a dedicated canary Ingress for IngressCanary routing
//	SDP_ALERTING        -> emit failure alerts (log + alert counter)
var (
	LogStreamingEnabled  = envOn("SDP_LOG_STREAMING")
	CanaryIngressEnabled = envOn("SDP_CANARY_INGRESS")
	AlertingEnabled      = envOn("SDP_ALERTING")
)

func envOn(key string) bool {
	v := os.Getenv(key)
	if v == "" {
		return true // opt-out semantics: default on
	}
	return v != "false" && v != "0" && v != "off"
}

var (
	taskRunPhase = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "sdp_runner",
		Subsystem: "taskrun",
		Name:      "phase_transitions_total",
		Help:      "Count of TaskRun phase transitions by task type and phase.",
	}, []string{"type", "phase"})

	rolloutWeight = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "sdp_runner",
		Subsystem: "rollout",
		Name:      "current_canary_weight",
		Help:      "Current canary weight (0-100) across all rollouts.",
	})

	reconnects = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "sdp_runner",
		Subsystem: "connector",
		Name:      "reconnects_total",
		Help:      "Number of Hub long-connection (re)connects.",
	})

	failures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "sdp_runner",
		Subsystem: "alert",
		Name:      "failures_total",
		Help:      "Runner-side failure alerts by kind (pipelinerun, taskrun, rollout).",
	}, []string{"kind"})

	registerOnce sync.Once
	registered   bool
)

// Register installs the collectors with the default Prometheus registry. Safe
// to call multiple times (only the first takes effect).
func Register() {
	registerOnce.Do(func() {
		prometheus.MustRegister(taskRunPhase, rolloutWeight, reconnects, failures)
		registered = true
	})
}

// Registered reports whether the collectors were installed.
func Registered() bool { return registered }

// RecordTaskRunPhase increments the phase-transition counter.
func RecordTaskRunPhase(taskType, phase string) {
	if registered {
		taskRunPhase.WithLabelValues(taskType, phase).Inc()
	}
}

// SetRolloutWeight records the live canary weight gauge.
func SetRolloutWeight(weight int32) {
	if registered {
		rolloutWeight.Set(float64(weight))
	}
}

// IncReconnect counts a Hub (re)connection.
func IncReconnect() {
	if registered {
		reconnects.Inc()
	}
}

// Alert records a failure alert. When alerting is enabled it also logs a
// structured line so the event surfaces even without a Prometheus scraper.
func Alert(kind, name, detail string) {
	if registered {
		failures.WithLabelValues(kind).Inc()
	}
	if AlertingEnabled {
		log.Printf("runner alert: kind=%s name=%s detail=%s", kind, name, detail)
	}
}
