package controller

import "testing"

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

func pi32(v int32) *int32 { return &v }
