package health

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestThresholdMet(t *testing.T) {
	tests := []struct {
		value     string
		threshold string
		want      bool
		wantErr   bool
	}{
		{"0.5", "1.0", true, false},
		{"1.0", "1.0", true, false},
		{"2.0", "1.0", false, false},
		{"", "1.0", false, true},   // empty value unparseable -> unhealthy
		{"0.5", "abc", false, true}, // bad threshold
	}
	for _, tc := range tests {
		got, err := ThresholdMet(tc.value, tc.threshold)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ThresholdMet(%q,%q) expected error", tc.value, tc.threshold)
			}
			continue
		}
		if err != nil {
			t.Errorf("ThresholdMet(%q,%q) unexpected error: %v", tc.value, tc.threshold, err)
		}
		if got != tc.want {
			t.Errorf("ThresholdMet(%q,%q)=%v want %v", tc.value, tc.threshold, got, tc.want)
		}
	}
}

func TestHTTPProbe(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()

	if !HTTPProbe(ok.URL, time.Second) {
		t.Error("HTTPProbe should report healthy on 200")
	}
	if HTTPProbe(bad.URL, time.Second) {
		t.Error("HTTPProbe should report unhealthy on 500")
	}
	if HTTPProbe("http://127.0.0.1:0/never", time.Millisecond) {
		t.Error("HTTPProbe should report unhealthy on connection error")
	}
}

func TestPrometheusQueryOKNoEndpoint(t *testing.T) {
	// Without a reachable endpoint the probe fails safe (unhealthy) rather
	// than advancing a rollout blind.
	if PrometheusQueryOK("http://127.0.0.1:0/query", "up", "1", time.Millisecond) {
		t.Error("PrometheusQueryOK should fail safe with no endpoint")
	}
}
