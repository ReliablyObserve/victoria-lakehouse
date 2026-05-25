package config

import (
	"runtime"
	"testing"
	"time"
)

func TestMemLeak_Config_DefaultCycles(t *testing.T) {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 200
	for c := 0; c < cycles; c++ {
		cfg := Default()
		_ = cfg
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Config Default: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Config structs should be GC'd after use: 1MB max
	maxGrowth := uint64(1 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Default() cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Config_ValidateCycles(t *testing.T) {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 200
	for c := 0; c < cycles; c++ {
		cfg := Default()
		cfg.Mode = ModeLogs
		cfg.S3.Bucket = "test-bucket"
		_ = cfg.Validate()
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Config Validate: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Validation should not retain memory: 1MB max
	maxGrowth := uint64(1 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Validate() cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Config_ParseSizeBytesCycles(t *testing.T) {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 500
	inputs := []string{"512MB", "1GB", "256KB", "100B", "2TB", "invalid", ""}
	for c := 0; c < cycles; c++ {
		for _, input := range inputs {
			_, _ = ParseSizeBytes(input)
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("ParseSizeBytes: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Pure computation, no allocations expected: 1MB max
	maxGrowth := uint64(1 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d ParseSizeBytes cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_Config_ActiveMethodsCycles(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test-bucket"
	cfg.Logs.BloomColumns = []string{"level", "app", "host"}
	cfg.Traces.BloomColumns = []string{"service.name", "operation"}

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 1000
	for c := 0; c < cycles; c++ {
		_ = cfg.ActiveBloomColumns()
		_ = cfg.ActiveDeletePrefix()
		_ = cfg.ActiveCompatVersion()
		_ = cfg.InsertEnabled()
		_ = cfg.SelectEnabled()
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("Config Active methods: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Slice/string returns from existing fields: 1MB max
	maxGrowth := uint64(1 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d active-methods cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}
