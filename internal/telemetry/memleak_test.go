package telemetry

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func TestMemLeak_Telemetry_InitNoopCycles(t *testing.T) {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 50
	for c := 0; c < cycles; c++ {
		shutdown, err := Init(context.Background(), config.TelemetryConfig{
			Enabled: false,
		}, "test-service")
		if err != nil {
			t.Fatalf("Init failed: %v", err)
		}
		_ = shutdown(context.Background())
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Telemetry Init noop: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Noop provider setup: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Init(noop) cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Telemetry_InitDiscardCycles(t *testing.T) {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 20
	for c := 0; c < cycles; c++ {
		shutdown, err := Init(context.Background(), config.TelemetryConfig{
			Enabled:    true,
			Endpoint:   "", // discard exporter
			SampleRate: 1.0,
		}, "test-service")
		if err != nil {
			t.Fatalf("Init failed: %v", err)
		}
		_ = shutdown(context.Background())
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Telemetry Init discard: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// TracerProvider setup/teardown cycles: 5MB max
	maxGrowth := uint64(5 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Init(discard) cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Telemetry_TracerSpanCycles(t *testing.T) {
	// Initialize with noop once
	_, err := Init(context.Background(), config.TelemetryConfig{
		Enabled: false,
	}, "test-service")
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 500
	for c := 0; c < cycles; c++ {
		tr := Tracer()
		ctx, span := tr.Start(context.Background(), "test-span")
		_ = ctx
		span.End()
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Telemetry Tracer spans: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Noop spans have no allocations: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d span cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Telemetry_TracerGetCycles(t *testing.T) {
	_, err := Init(context.Background(), config.TelemetryConfig{Enabled: false}, "svc")
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 1000
	for c := 0; c < cycles; c++ {
		tr := Tracer()
		_ = tr
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Telemetry Tracer get: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Tracer() just returns global reference: 1MB max
	maxGrowth := uint64(1 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Tracer() cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}
