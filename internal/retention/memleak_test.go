package retention

// mockManifest, mockDeleter, and testLogger are defined in retention_test.go.

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// retForceGC runs two GC cycles to ensure all unreachable objects are collected.
func retForceGC() {
	runtime.GC()
	runtime.GC()
}

// retHeapInUse returns current HeapInuse after forcing GC.
func retHeapInUse() uint64 {
	var m runtime.MemStats
	retForceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// TestMemLeak_Retention_ResolveTTL verifies that repeated TTL resolution
// for files does not accumulate heap allocations.
func TestMemLeak_Retention_ResolveTTL(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
		Rules: []Rule{
			{Match: map[string]string{"env": "production"}, Keep: "365d"},
			{Match: map[string]string{"env": "staging"}, Keep: "30d"},
		},
	}

	mf := newMockManifest(nil)
	del := &mockDeleter{}
	mgr, err := New(cfg, mf, del, "test-bucket", testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	files := []manifest.FileInfo{
		{Key: "file-1.parquet", MaxTimeNs: time.Now().Add(-10 * 24 * time.Hour).UnixNano(), Labels: map[string][]string{"env": {"production"}}},
		{Key: "file-2.parquet", MaxTimeNs: time.Now().Add(-5 * 24 * time.Hour).UnixNano(), Labels: map[string][]string{"env": {"staging"}}},
		{Key: "file-3.parquet", MaxTimeNs: time.Now().Add(-100 * 24 * time.Hour).UnixNano(), Labels: nil},
	}

	// Warm up
	for i := 0; i < 1000; i++ {
		for _, fi := range files {
			_ = mgr.ResolveTTL(fi)
		}
	}
	retForceGC()

	before := retHeapInUse()

	const iterations = 100000
	for i := 0; i < iterations; i++ {
		fi := files[i%len(files)]
		ttl := mgr.ResolveTTL(fi)
		_ = ttl
	}

	retForceGC()
	after := retHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("ResolveTTL memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// TestMemLeak_Retention_RunOnce verifies that repeated retention pass
// executions do not accumulate allocations.
func TestMemLeak_Retention_RunOnce(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "7d",
		CheckInterval: "1h",
	}

	// Build a manifest with files split between old (expired) and new (retained)
	files := make(map[string][]manifest.FileInfo)
	now := time.Now()
	for d := 0; d < 10; d++ {
		partition := fmt.Sprintf("dt=2026-01-%02d/hour=00", d+1)
		// Half of each partition's files are old (30d+) — will be deleted
		for j := 0; j < 3; j++ {
			age := time.Duration(j+1) * 30 * 24 * time.Hour
			files[partition] = append(files[partition], manifest.FileInfo{
				Key:       fmt.Sprintf("logs/%s/file-%d.parquet", partition, j),
				Size:      1024,
				MaxTimeNs: now.Add(-age).UnixNano(),
			})
		}
	}

	// Warm up with a fresh manifest each cycle (simulate re-runs)
	for i := 0; i < 20; i++ {
		mf := newMockManifest(files)
		del := &mockDeleter{}
		mgr, err := New(cfg, mf, del, "bucket", testLogger())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		mgr.nowFunc = func() time.Time { return now }
		_, _ = mgr.RunOnce(context.Background())
	}
	retForceGC()

	before := retHeapInUse()

	const iterations = 500
	for i := 0; i < iterations; i++ {
		mf := newMockManifest(files)
		del := &mockDeleter{}
		mgr, err := New(cfg, mf, del, "bucket", testLogger())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		mgr.nowFunc = func() time.Time { return now }
		_, _ = mgr.RunOnce(context.Background())
	}

	retForceGC()
	after := retHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("RunOnce memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}

// TestMemLeak_Retention_PolicyEvaluation verifies that evaluating retention
// rules against files with labels is bounded.
func TestMemLeak_Retention_PolicyEvaluation(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
		Rules: []Rule{
			{Match: map[string]string{"env": "production", "team": "backend"}, Keep: "365d"},
			{Match: map[string]string{"env": "staging"}, Keep: "30d"},
			{Match: map[string]string{"team": "infra"}, Keep: "180d"},
		},
	}

	mf := newMockManifest(nil)
	del := &mockDeleter{}
	mgr, err := New(cfg, mf, del, "bucket", testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Vary labels to exercise rule matching logic
	now := time.Now()
	fileVariants := []manifest.FileInfo{
		{Key: "f1.parquet", MaxTimeNs: now.Add(-10 * 24 * time.Hour).UnixNano(), Labels: map[string][]string{"env": {"production"}, "team": {"backend"}}},
		{Key: "f2.parquet", MaxTimeNs: now.Add(-10 * 24 * time.Hour).UnixNano(), Labels: map[string][]string{"env": {"staging"}}},
		{Key: "f3.parquet", MaxTimeNs: now.Add(-10 * 24 * time.Hour).UnixNano(), Labels: map[string][]string{"team": {"infra"}}},
		{Key: "f4.parquet", MaxTimeNs: now.Add(-10 * 24 * time.Hour).UnixNano(), Labels: map[string][]string{"env": {"dev"}}},
		{Key: "f5.parquet", MaxTimeNs: now.Add(-10 * 24 * time.Hour).UnixNano(), Labels: nil},
	}

	// Warm up
	for i := 0; i < 1000; i++ {
		_ = mgr.ResolveTTL(fileVariants[i%len(fileVariants)])
	}
	retForceGC()

	before := retHeapInUse()

	const iterations = 100000
	for i := 0; i < iterations; i++ {
		fi := fileVariants[i%len(fileVariants)]
		ttl := mgr.ResolveTTL(fi)
		_ = ttl
	}

	retForceGC()
	after := retHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("policy evaluation memory grew by %d bytes over %d iterations (max allowed %d)", growth, iterations, maxAllowed)
	}
}
