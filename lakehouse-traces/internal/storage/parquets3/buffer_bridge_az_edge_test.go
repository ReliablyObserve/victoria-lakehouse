package parquets3

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func TestBufferBridge_SetEndpointsWithZones_AllSameAZ(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            true,
		CrossAZFallback:    false,
	}
	bb := NewBufferBridge(cfg, config.ModeTraces)

	epZones := map[string]string{
		"http://insert-0:9428": "az-a",
		"http://insert-1:9428": "az-a",
		"http://insert-2:9428": "az-a",
	}
	bb.SetEndpointsWithZones(epZones, "az-a")

	bb.mu.RLock()
	defer bb.mu.RUnlock()

	if len(bb.sameAZEndpoints) != 3 {
		t.Errorf("expected 3 same-AZ, got %d", len(bb.sameAZEndpoints))
	}
	if len(bb.crossAZEndpoints) != 0 {
		t.Errorf("expected 0 cross-AZ, got %d", len(bb.crossAZEndpoints))
	}
}

func TestBufferBridge_SetEndpointsWithZones_AllCrossAZ(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            true,
		CrossAZFallback:    false,
	}
	bb := NewBufferBridge(cfg, config.ModeTraces)

	epZones := map[string]string{
		"http://insert-0:9428": "az-b",
		"http://insert-1:9428": "az-c",
	}
	bb.SetEndpointsWithZones(epZones, "az-a")

	bb.mu.RLock()
	eps := bb.getQueryEndpoints()
	bb.mu.RUnlock()

	if len(eps) != 2 {
		t.Errorf("strict with no same-AZ should fallback to all, got %d", len(eps))
	}
}

func TestBufferBridge_SetEndpointsWithZones_EmptyMap(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            true,
	}
	bb := NewBufferBridge(cfg, config.ModeTraces)

	bb.SetEndpointsWithZones(map[string]string{}, "az-a")

	bb.mu.RLock()
	defer bb.mu.RUnlock()

	if len(bb.endpoints) != 0 {
		t.Errorf("expected 0 endpoints, got %d", len(bb.endpoints))
	}
	if len(bb.sameAZEndpoints) != 0 {
		t.Errorf("expected 0 same-AZ, got %d", len(bb.sameAZEndpoints))
	}
}

func TestBufferBridge_SetEndpointsWithZones_OverwritesPrevious(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            true,
		CrossAZFallback:    true,
	}
	bb := NewBufferBridge(cfg, config.ModeTraces)

	bb.SetEndpointsWithZones(map[string]string{
		"http://a:9428": "az-a",
		"http://b:9428": "az-b",
	}, "az-a")

	bb.mu.RLock()
	if len(bb.endpoints) != 2 {
		t.Errorf("first set: expected 2, got %d", len(bb.endpoints))
	}
	bb.mu.RUnlock()

	bb.SetEndpointsWithZones(map[string]string{
		"http://c:9428": "az-c",
	}, "az-c")

	bb.mu.RLock()
	defer bb.mu.RUnlock()

	if len(bb.endpoints) != 1 {
		t.Errorf("overwrite: expected 1, got %d", len(bb.endpoints))
	}
	if len(bb.sameAZEndpoints) != 1 {
		t.Errorf("overwrite: expected 1 same-AZ, got %d", len(bb.sameAZEndpoints))
	}
}

func TestBufferBridge_getQueryEndpoints_EmptySelfAZ(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            true,
		CrossAZFallback:    false,
	}
	bb := NewBufferBridge(cfg, config.ModeTraces)
	bb.endpoints = []string{"http://a:9428", "http://b:9428"}
	bb.selfAZ = ""

	eps := bb.getQueryEndpoints()
	if len(eps) != 2 {
		t.Errorf("empty selfAZ should return all endpoints, got %d", len(eps))
	}
}

func TestBufferBridge_SetEndpoints_ThenSetWithZones(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            true,
		CrossAZFallback:    false,
	}
	bb := NewBufferBridge(cfg, config.ModeTraces)

	bb.SetEndpoints([]string{"http://a:9428", "http://b:9428"})

	bb.mu.RLock()
	if bb.selfAZ != "" {
		t.Error("SetEndpoints should not set selfAZ")
	}
	bb.mu.RUnlock()

	bb.SetEndpointsWithZones(map[string]string{
		"http://a:9428": "az-a",
		"http://b:9428": "az-b",
	}, "az-a")

	bb.mu.RLock()
	defer bb.mu.RUnlock()

	if bb.selfAZ != "az-a" {
		t.Errorf("expected selfAZ=az-a, got %q", bb.selfAZ)
	}
	if len(bb.sameAZEndpoints) != 1 {
		t.Errorf("expected 1 same-AZ endpoint, got %d", len(bb.sameAZEndpoints))
	}
}

func TestBufferBridge_MixedModes(t *testing.T) {
	tests := []struct {
		name            string
		azAware         bool
		crossAZFallback bool
		selfAZ          string
		sameAZ          int
		wantCount       int
	}{
		{"az_off", false, true, "az-a", 1, 3},
		{"preferred_with_same", true, true, "az-a", 1, 3},
		{"strict_with_same", true, false, "az-a", 1, 3},
		{"strict_no_same", true, false, "az-x", 0, 3},
		{"no_selfaz", true, false, "", 0, 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.SelectConfig{
				BufferQueryEnabled: true,
				BufferQueryTimeout: 2 * time.Second,
				AZAware:            tc.azAware,
				CrossAZFallback:    tc.crossAZFallback,
			}
			bb := NewBufferBridge(cfg, config.ModeTraces)

			epZones := map[string]string{
				"http://a:9428": "az-a",
				"http://b:9428": "az-b",
				"http://c:9428": "az-c",
			}
			bb.SetEndpointsWithZones(epZones, tc.selfAZ)

			bb.mu.RLock()
			eps := bb.getQueryEndpoints()
			bb.mu.RUnlock()

			if len(eps) != tc.wantCount {
				t.Errorf("expected %d endpoints, got %d", tc.wantCount, len(eps))
			}
		})
	}
}
