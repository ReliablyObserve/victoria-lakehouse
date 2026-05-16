package parquets3

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func TestBufferBridge_SetEndpointsWithZones(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            true,
		CrossAZFallback:    true,
	}
	bb := NewBufferBridge(cfg, config.ModeLogs)

	epZones := map[string]string{
		"http://insert-0:9428": "az-a",
		"http://insert-1:9428": "az-a",
		"http://insert-2:9428": "az-b",
	}
	bb.SetEndpointsWithZones(epZones, "az-a")

	bb.mu.RLock()
	defer bb.mu.RUnlock()

	if len(bb.endpoints) != 3 {
		t.Errorf("expected 3 total endpoints, got %d", len(bb.endpoints))
	}
	if len(bb.sameAZEndpoints) != 2 {
		t.Errorf("expected 2 same-AZ endpoints, got %d", len(bb.sameAZEndpoints))
	}
	if len(bb.crossAZEndpoints) != 1 {
		t.Errorf("expected 1 cross-AZ endpoint, got %d", len(bb.crossAZEndpoints))
	}
}

func TestBufferBridge_AlwaysQueriesAllEndpoints(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            true,
		CrossAZFallback:    false,
	}
	bb := NewBufferBridge(cfg, config.ModeLogs)

	epZones := map[string]string{
		"http://insert-0:9428": "az-a",
		"http://insert-1:9428": "az-b",
	}
	bb.SetEndpointsWithZones(epZones, "az-a")

	bb.mu.RLock()
	eps := bb.getQueryEndpoints()
	bb.mu.RUnlock()

	if len(eps) != 2 {
		t.Errorf("buffer queries must reach ALL insert pods, got %d (want 2)", len(eps))
	}
}

func TestBufferBridge_PreferredMode_AllEndpoints(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            true,
		CrossAZFallback:    true,
	}
	bb := NewBufferBridge(cfg, config.ModeLogs)

	epZones := map[string]string{
		"http://insert-0:9428": "az-a",
		"http://insert-1:9428": "az-b",
	}
	bb.SetEndpointsWithZones(epZones, "az-a")

	bb.mu.RLock()
	eps := bb.getQueryEndpoints()
	bb.mu.RUnlock()

	if len(eps) != 2 {
		t.Errorf("preferred: expected 2 endpoints, got %d", len(eps))
	}
}

func TestBufferBridge_NoAZ_AllEndpoints(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            false,
	}
	bb := NewBufferBridge(cfg, config.ModeLogs)

	bb.SetEndpoints([]string{"http://a:9428", "http://b:9428"})

	bb.mu.RLock()
	eps := bb.getQueryEndpoints()
	bb.mu.RUnlock()

	if len(eps) != 2 {
		t.Errorf("no AZ: expected 2 endpoints, got %d", len(eps))
	}
}
