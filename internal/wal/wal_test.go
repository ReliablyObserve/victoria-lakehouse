package wal

import (
	"path/filepath"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestWAL_AppendReplayLogs(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "hello", ServiceName: "svc1"},
		{TimestampUnixNano: 2000, Body: "world", ServiceName: "svc2"},
	}
	for i := range rows {
		if err := w.AppendLog(&rows[i]); err != nil {
			t.Fatalf("AppendLog[%d]: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	logs, traces, err := w2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(traces) != 0 {
		t.Errorf("traces = %d, want 0", len(traces))
	}
	if len(logs) != 2 {
		t.Fatalf("logs = %d, want 2", len(logs))
	}
	if logs[0].Body != "hello" {
		t.Errorf("logs[0].Body = %q, want hello", logs[0].Body)
	}
	if logs[1].ServiceName != "svc2" {
		t.Errorf("logs[1].ServiceName = %q, want svc2", logs[1].ServiceName)
	}
}
