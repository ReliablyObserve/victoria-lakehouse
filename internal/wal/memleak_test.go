package wal

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// walForceGC runs two GC cycles to ensure all unreachable objects are collected.
func walForceGC() {
	runtime.GC()
	runtime.GC()
}

// walHeapInUse returns current HeapInuse after forcing GC.
func walHeapInUse() uint64 {
	var m runtime.MemStats
	walForceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// TestMemLeak_WAL_WriteReplay verifies that repeated write+replay+truncate
// cycles do not cause unbounded heap growth.
func TestMemLeak_WAL_WriteReplay(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatalf("Open WAL: %v", err)
	}
	defer func() { _ = w.Close() }()

	// Warm up — write, replay, truncate
	for i := 0; i < 100; i++ {
		row := &schema.LogRow{
			TimestampUnixNano: int64(i) * 1e9,
			Body:              fmt.Sprintf("warm-up message %d", i),
			ServiceName:       fmt.Sprintf("svc-%d", i%5),
		}
		if err := w.AppendLog(row); err != nil {
			t.Fatalf("AppendLog: %v", err)
		}
		if i%10 == 9 {
			_, _, _ = w.Replay()
			if err := w.Truncate(); err != nil {
				t.Fatalf("Truncate: %v", err)
			}
		}
	}
	walForceGC()

	before := walHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		row := &schema.LogRow{
			TimestampUnixNano: int64(i) * 1e9,
			Body:              fmt.Sprintf("message %d", i),
			ServiceName:       fmt.Sprintf("svc-%d", i%5),
		}
		if err := w.AppendLog(row); err != nil {
			// WAL might be full; truncate and continue
			if err2 := w.Truncate(); err2 != nil {
				t.Fatalf("Truncate: %v", err2)
			}
			continue
		}
		if i%50 == 49 {
			_, _, _ = w.Replay()
			if err := w.Truncate(); err != nil {
				t.Fatalf("Truncate: %v", err)
			}
		}
	}

	walForceGC()
	after := walHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024) // 10 MB
	if growth > maxAllowed {
		t.Errorf("WAL write/replay memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// TestMemLeak_WAL_WriteReplayTraces verifies trace row write+replay cycles
// do not leak memory.
func TestMemLeak_WAL_WriteReplayTraces(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(filepath.Join(dir, "traces.wal"), 512*1024*1024)
	if err != nil {
		t.Fatalf("Open WAL: %v", err)
	}
	defer func() { _ = w.Close() }()

	// Warm up
	for i := 0; i < 100; i++ {
		row := &schema.TraceRow{
			TimestampUnixNano: int64(i) * 1e9,
			TraceID:           fmt.Sprintf("trace-%d", i),
			ServiceName:       fmt.Sprintf("svc-%d", i%5),
		}
		if err := w.AppendTrace(row); err != nil {
			t.Fatalf("AppendTrace: %v", err)
		}
		if i%10 == 9 {
			_, _, _ = w.Replay()
			if err := w.Truncate(); err != nil {
				t.Fatalf("Truncate: %v", err)
			}
		}
	}
	walForceGC()

	before := walHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		row := &schema.TraceRow{
			TimestampUnixNano: int64(i) * 1e9,
			TraceID:           fmt.Sprintf("trace-%d", i),
			ServiceName:       fmt.Sprintf("svc-%d", i%5),
		}
		if err := w.AppendTrace(row); err != nil {
			if err2 := w.Truncate(); err2 != nil {
				t.Fatalf("Truncate: %v", err2)
			}
			continue
		}
		if i%50 == 49 {
			_, _, _ = w.Replay()
			if err := w.Truncate(); err != nil {
				t.Fatalf("Truncate: %v", err)
			}
		}
	}

	walForceGC()
	after := walHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("WAL trace write/replay memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// TestMemLeak_WAL_OpenClose verifies that repeatedly opening and closing
// WAL files releases all file descriptors and heap allocations.
func TestMemLeak_WAL_OpenClose(t *testing.T) {
	dir := t.TempDir()

	// Warm up
	for i := 0; i < 10; i++ {
		path := filepath.Join(dir, fmt.Sprintf("wal-%d.bin", i))
		w, err := Open(path, 1*1024*1024)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		_ = w.AppendLog(&schema.LogRow{TimestampUnixNano: int64(i), Body: "warm"})
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	walForceGC()

	before := walHeapInUse()

	const iterations = 200
	for i := 0; i < iterations; i++ {
		// Reuse a small set of paths to avoid FS pressure
		path := filepath.Join(dir, fmt.Sprintf("cyclewal-%d.bin", i%5))
		w, err := Open(path, 1*1024*1024)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		_ = w.AppendLog(&schema.LogRow{
			TimestampUnixNano: int64(i) * 1e9,
			Body:              fmt.Sprintf("msg-%d", i),
			ServiceName:       "test-svc",
		})
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	walForceGC()
	after := walHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("WAL Open/Close memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// TestMemLeak_WAL_Rotation verifies that filling a WAL to capacity and then
// truncating does not leak memory.
func TestMemLeak_WAL_Rotation(t *testing.T) {
	dir := t.TempDir()

	// Use a small WAL max size to trigger fullness often
	const maxBytes = 64 * 1024 // 64 KB
	path := filepath.Join(dir, "rotate.wal")

	w, err := Open(path, maxBytes)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = w.Close() }()

	// Warm up: fill and truncate a few times
	for cycle := 0; cycle < 5; cycle++ {
		for {
			row := &schema.LogRow{
				TimestampUnixNano: 1e9,
				Body:              "fill message that consumes space in the WAL for rotation testing purposes",
				ServiceName:       "svc",
			}
			if err := w.AppendLog(row); err != nil {
				break // WAL full
			}
		}
		_, _, _ = w.Replay()
		if err := w.Truncate(); err != nil {
			t.Fatalf("Truncate: %v", err)
		}
	}
	walForceGC()

	before := walHeapInUse()

	const cycles = 100
	for cycle := 0; cycle < cycles; cycle++ {
		// Fill WAL to capacity
		for {
			row := &schema.LogRow{
				TimestampUnixNano: int64(cycle) * 1e9,
				Body:              "rotation test message body content",
				ServiceName:       "svc",
			}
			if err := w.AppendLog(row); err != nil {
				break // WAL full — expected
			}
		}
		// Replay and truncate
		_, _, _ = w.Replay()
		if err := w.Truncate(); err != nil {
			t.Fatalf("Truncate cycle %d: %v", cycle, err)
		}
	}

	walForceGC()
	after := walHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("WAL rotation memory grew by %d bytes over %d rotation cycles (max allowed %d)", growth, cycles, maxAllowed)
	}
}
