// Package health holds the (mostly pure) helpers the Rollout controller uses
// to judge canary health at each step. PodReady readiness is derived from the
// canary Deployment's replica counts; HTTPProbe and PrometheusQuery add an
// active check. The probe functions do real I/O, but the decision helpers
// (ThresholdMet) are pure so they can be unit-tested without a cluster.
package health

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// HTTPProbe performs a GET against url and reports success for any 2xx
// status. A connection error or non-2xx response is treated as unhealthy so
// the rollout holds (and, past the failure threshold, rolls back).
func HTTPProbe(url string, timeout time.Duration) bool {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// ThresholdMet parses the numeric query result and the configured threshold
// (both as strings, exactly as they arrive over the wire) and reports whether
// the result is at or below the ceiling. Prometheus error-rate style checks
// treat "value <= threshold" as healthy. An unparseable/empty value is
// unhealthy (fail safe — don't advance on a bad read).
func ThresholdMet(value, threshold string) (bool, error) {
	v, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return false, fmt.Errorf("health: cannot parse query result %q: %w", value, err)
	}
	t, err := strconv.ParseFloat(strings.TrimSpace(threshold), 64)
	if err != nil {
		return false, fmt.Errorf("health: cannot parse threshold %q: %w", threshold, err)
	}
	return v <= t, nil
}

// PrometheusQueryOK evaluates expr against a Prometheus endpoint and reports
// whether the single scalar result is within threshold. It is intentionally
// minimal: a text/plain 200 response body is parsed as a float. Real
// Prometheus scalar JSON parsing can be layered on later without changing
// callers. A transport error or unparseable body fails safe (unhealthy).
func PrometheusQueryOK(endpoint, expr, threshold string, timeout time.Duration) bool {
	if threshold == "" {
		// No ceiling configured: presence of a successful response is enough.
	}
	url := endpoint
	if strings.Contains(endpoint, "?") {
		url = endpoint + "&query=" + expr
	} else {
		url = endpoint + "?query=" + expr
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	if n == 0 {
		return false
	}
	body := strings.TrimSpace(string(buf[:n]))
	// Prometheus text format: "metric value" — take the last whitespace-
	// separated token as the numeric value.
	fields := strings.Fields(body)
	if len(fields) == 0 {
		return false
	}
	met, err := ThresholdMet(fields[len(fields)-1], threshold)
	if err != nil {
		return false
	}
	return met
}
