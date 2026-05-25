package selectapi

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func TestMemLeak_Handler_Creation(t *testing.T) {
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.Bucket = "test-bucket"
	cfg.Query.MaxConcurrent = 32
	cfg.Query.Timeout = 5 * time.Second

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 100
	for c := 0; c < cycles; c++ {
		h := NewHandler(mockStore{}, cfg)
		_ = h
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Handler creation: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Handler allocates a semaphore channel: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Handler creation cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Handler_Register(t *testing.T) {
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.Bucket = "test-bucket"
	cfg.Query.MaxConcurrent = 32
	cfg.Query.Timeout = 5 * time.Second

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 50
	for c := 0; c < cycles; c++ {
		h := NewHandler(mockStore{}, cfg)
		mux := http.NewServeMux()
		h.Register(mux)
		_ = mux
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Handler Register: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// ServeMux + registered routes: 5MB max
	maxGrowth := uint64(5 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Register cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Handler_TailNoopRequestCycles(t *testing.T) {
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.Bucket = "test-bucket"
	cfg.Query.MaxConcurrent = 32
	cfg.Query.Timeout = 5 * time.Second

	h := NewHandler(mockStore{}, cfg)

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 200
	for c := 0; c < cycles; c++ {
		req := httptest.NewRequest(http.MethodGet, "/select/logsql/tail", nil)
		rr := httptest.NewRecorder()
		h.handleTailNoop(rr, req)
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Handler handleTailNoop: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Each request+response allocates and is discarded: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d tail noop cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Handler_NormalizeTimeParams(t *testing.T) {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 500
	for c := 0; c < cycles; c++ {
		// Simulate Grafana millisecond timestamps
		req := httptest.NewRequest(http.MethodGet, "/select/logsql/query?start=1700000000000&end=1700003600000", nil)
		normalizeTimeParams(req)
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("normalizeTimeParams: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Request form parsing: 3MB max
	maxGrowth := uint64(3 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d normalize cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Handler_SemaphoreCycles(t *testing.T) {
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.Bucket = "test-bucket"
	cfg.Query.MaxConcurrent = 8
	cfg.Query.Timeout = 5 * time.Second

	h := NewHandler(mockStore{}, cfg)

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	// Exhaust semaphore and check rejection path
	const cycles = 100
	for c := 0; c < cycles; c++ {
		// Acquire all slots
		for i := 0; i < cap(h.sem); i++ {
			h.sem <- struct{}{}
		}
		// Try to acquire one more — should be over capacity
		req := httptest.NewRequest(http.MethodGet, "/select/logsql/tail", nil)
		rr := httptest.NewRecorder()
		h.handleTailNoop(rr, req)

		// Release all slots
		for i := 0; i < cap(h.sem); i++ {
			<-h.sem
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Handler semaphore cycles: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Channel operations: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d semaphore cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}
