package retention

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// --- New validation: zero/negative check_interval ---

func TestNew_ZeroCheckInterval(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "0s",
	}
	_, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err == nil {
		t.Fatal("expected error for zero check_interval")
	}
}

// --- Start tests ---

func TestStart_Disabled(t *testing.T) {
	cfg := Config{
		Enabled:       false,
		Default:       "90d",
		CheckInterval: "1h",
	}
	mgr, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}

	// Start should return immediately when disabled.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		mgr.Start(ctx)
		close(done)
	}()

	select {
	case <-done:
		// Returned immediately as expected.
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return promptly when disabled")
	}
}

func TestStart_TickAndCancel(t *testing.T) {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)

	// Expired file: 100 days old, default TTL 90d.
	expired := manifest.FileInfo{
		Key:       "data/expired.parquet",
		Size:      1024,
		MaxTimeNs: now.Add(-100 * 24 * time.Hour).UnixNano(),
		Labels:    map[string][]string{"env": {"prod"}},
	}
	mf := newMockManifest(map[string][]manifest.FileInfo{
		"dt=2026-02-14/hour=10": {expired},
	})
	deleter := &mockDeleter{}

	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "10ms", // very short for testing
	}
	mgr, err := New(cfg, mf, deleter, "test-bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	mgr.nowFunc = func() time.Time { return now }

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		mgr.Start(ctx)
		close(done)
	}()

	// Wait long enough for at least one tick to fire.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Start returned after cancel.
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}

	// The ticker should have fired and deleted the expired file.
	deleted := deleter.getDeleted()
	if len(deleted) == 0 {
		t.Fatal("expected at least one file to be deleted during Start loop")
	}
}

func TestStart_TickWithError(t *testing.T) {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)

	expired := manifest.FileInfo{
		Key:       "data/fail.parquet",
		Size:      512,
		MaxTimeNs: now.Add(-100 * 24 * time.Hour).UnixNano(),
		Labels:    map[string][]string{"env": {"prod"}},
	}
	mf := newMockManifest(map[string][]manifest.FileInfo{
		"p1": {expired},
	})
	// Deleter always errors — RunOnce succeeds but deletes 0 files.
	deleter := &mockDeleter{err: fmt.Errorf("s3 unavailable")}

	// Use a dedicated quiet logger to avoid noisy test output.
	quietLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError + 1}))

	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "10ms",
	}
	mgr, err := New(cfg, mf, deleter, "bucket", quietLogger)
	if err != nil {
		t.Fatal(err)
	}
	mgr.nowFunc = func() time.Time { return now }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		mgr.Start(ctx)
		close(done)
	}()

	// Let several ticks fire (with error logging path).
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after cancel")
	}

	// Files should remain since delete always failed.
	remaining := mf.AllFiles()
	if _, ok := remaining["p1"]; !ok {
		t.Fatal("file should remain in manifest after delete errors")
	}
}

func TestStart_TickDeleteSuccessLogged(t *testing.T) {
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)

	// Two expired files to exercise "deleted > 0" log branch.
	f1 := manifest.FileInfo{
		Key:       "data/a.parquet",
		Size:      100,
		MaxTimeNs: now.Add(-200 * 24 * time.Hour).UnixNano(),
		Labels:    map[string][]string{"env": {"prod"}},
	}
	f2 := manifest.FileInfo{
		Key:       "data/b.parquet",
		Size:      200,
		MaxTimeNs: now.Add(-200 * 24 * time.Hour).UnixNano(),
		Labels:    map[string][]string{"env": {"prod"}},
	}
	mf := newMockManifest(map[string][]manifest.FileInfo{
		"p1": {f1, f2},
	})
	deleter := &mockDeleter{}

	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "10ms",
	}
	mgr, err := New(cfg, mf, deleter, "bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	mgr.nowFunc = func() time.Time { return now }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		mgr.Start(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if len(deleter.getDeleted()) < 2 {
		t.Fatalf("expected at least 2 deletions, got %d", len(deleter.getDeleted()))
	}
}
