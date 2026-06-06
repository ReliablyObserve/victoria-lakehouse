package wal

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestStress_HighThroughput_AppendThenReplay writes 50_000 entries in a tight
// loop, then Replays. Designed to catch a quadratic Replay (e.g. via repeated
// slice growth on the logs/traces accumulators) or a per-entry allocation
// blow-up that would push wall time past 30 s on any reasonable laptop.
//
// Acceptance:
//  1. Append phase completes.
//  2. Replay phase produces exactly the same number of logs as were appended.
//  3. Total wall time stays under 30 s.
func TestStress_HighThroughput_AppendThenReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test under -short")
	}

	const entries = 50_000
	dir := t.TempDir()
	path := filepath.Join(dir, "stress.wal")

	w, err := Open(path, 0) // unlimited
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	start := time.Now()

	for i := 0; i < entries; i++ {
		row := &schema.LogRow{
			TimestampUnixNano: int64(i),
			Body:              "stress",
			ServiceName:       "svc",
		}
		if err := w.AppendLog(row); err != nil {
			t.Fatalf("AppendLog[%d]: %v", i, err)
		}
	}

	if err := w.file.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := Open(path, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = w2.Close() }()

	logs, traces, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	elapsed := time.Since(start)
	if elapsed > 30*time.Second {
		t.Errorf("stress run took %v, want < 30s (possible quadratic Replay)", elapsed)
	}
	if len(logs) != entries {
		t.Fatalf("replayed %d logs, want %d", len(logs), entries)
	}
	if len(traces) != 0 {
		t.Errorf("replayed %d traces, want 0", len(traces))
	}
}
