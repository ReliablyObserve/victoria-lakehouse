package metrics

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestMemLeak_Counter_IncAddCycles(t *testing.T) {
	c := NewCounter("test_memleak_counter_inc_total")

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 10000
	for i := 0; i < cycles; i++ {
		c.Inc()
		c.Add(5)
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Counter Inc/Add: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Atomic counter ops: 1MB max
	maxGrowth := uint64(1 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Counter cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Gauge_SetIncDecCycles(t *testing.T) {
	g := NewGauge("test_memleak_gauge_ops")

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 10000
	for i := 0; i < cycles; i++ {
		g.Set(int64(i))
		g.Inc()
		g.Dec()
		g.Add(10)
		_ = g.Get()
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Gauge ops: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Atomic gauge ops: 1MB max
	maxGrowth := uint64(1 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Gauge cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Histogram_ObserveCycles(t *testing.T) {
	h := NewHistogram("test_memleak_histogram_obs", DefBuckets)

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 10000
	for i := 0; i < cycles; i++ {
		h.Observe(float64(i%1000) * 0.001)
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Histogram Observe: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Fixed-bucket histogram: 1MB max
	maxGrowth := uint64(1 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Observe cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_CounterVec_LabeledIncCycles(t *testing.T) {
	cv := NewCounterVec("test_memleak_counter_vec_total", "op")

	const labels = 5
	labelValues := make([]string, labels)
	for i := range labelValues {
		labelValues[i] = fmt.Sprintf("op%d", i)
	}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 1000
	for c := 0; c < cycles; c++ {
		for _, lv := range labelValues {
			cv.Inc(lv)
			cv.Add(lv, 3)
			_ = cv.Get(lv)
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("CounterVec labeled: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Bounded by fixed label count: 1MB max
	maxGrowth := uint64(1 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d CounterVec cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_WritePrometheus_Cycles(t *testing.T) {
	// Warm up metrics
	InsertRowsTotal.Inc()
	QueryDuration.Observe(0.1)
	S3RequestsTotal.Inc("get")

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 50
	for c := 0; c < cycles; c++ {
		var buf bytes.Buffer
		WritePrometheus(&buf, false)
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("WritePrometheus: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Buffer written and discarded: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d WritePrometheus cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Metrics_ConcurrentOps(t *testing.T) {
	c := NewCounter("test_memleak_metrics_concurrent_total")
	g := NewGauge("test_memleak_metrics_concurrent_gauge")
	h := NewHistogram("test_memleak_metrics_concurrent_hist", DefBuckets)

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	var wg sync.WaitGroup
	const goroutines = 8
	const opsPerGoroutine = 100

	for g2 := 0; g2 < goroutines; g2++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				c.Inc()
				g.Set(int64(i))
				h.Observe(float64(i) * 0.001)
			}
		}()
	}
	wg.Wait()

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Metrics concurrent: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Bounded counters/gauges/histograms: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after concurrent metric ops",
			heapBefore/1024, heapAfter/1024)
	}
}
