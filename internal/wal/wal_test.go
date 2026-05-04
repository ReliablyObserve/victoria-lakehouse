package wal

import (
	"os"
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

func TestWAL_AppendReplayTraces(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	row := schema.TraceRow{TimestampUnixNano: 3000, TraceID: "t1", SpanID: "s1", SpanName: "op"}
	if err := w.AppendTrace(&row); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2, _ := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	defer w2.Close()
	logs, traces, _ := w2.Replay()

	if len(logs) != 0 {
		t.Errorf("logs = %d, want 0", len(logs))
	}
	if len(traces) != 1 {
		t.Fatalf("traces = %d, want 1", len(traces))
	}
	if traces[0].TraceID != "t1" {
		t.Errorf("TraceID = %q, want t1", traces[0].TraceID)
	}
}

func TestWAL_Truncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	w, _ := Open(path, 512*1024*1024)

	w.AppendLog(&schema.LogRow{TimestampUnixNano: 1000, Body: "before"})
	if w.Size() == 0 {
		t.Fatal("size should be > 0 after append")
	}

	if err := w.Truncate(); err != nil {
		t.Fatal(err)
	}
	if w.Size() != 0 {
		t.Errorf("size after truncate = %d, want 0", w.Size())
	}

	w.AppendLog(&schema.LogRow{TimestampUnixNano: 2000, Body: "after"})
	w.Close()

	w2, _ := Open(path, 512*1024*1024)
	defer w2.Close()
	logs, _, _ := w2.Replay()
	if len(logs) != 1 {
		t.Fatalf("logs = %d, want 1", len(logs))
	}
	if logs[0].Body != "after" {
		t.Errorf("Body = %q, want after", logs[0].Body)
	}
}

func TestWAL_Full(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(filepath.Join(dir, "wal.bin"), 100) // 100 bytes max

	for i := 0; i < 100; i++ {
		err := w.AppendLog(&schema.LogRow{TimestampUnixNano: int64(i), Body: "msg"})
		if err != nil {
			if !w.IsFull() {
				t.Error("should report full")
			}
			w.Close()
			return
		}
	}
	w.Close()
	t.Fatal("expected WAL full error")
}

func TestWAL_CorruptPartialEntry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	w, _ := Open(path, 512*1024*1024)

	w.AppendLog(&schema.LogRow{TimestampUnixNano: 1000, Body: "good"})
	w.AppendLog(&schema.LogRow{TimestampUnixNano: 2000, Body: "also good"})
	w.Close()

	// Truncate file mid-entry to simulate crash
	data, _ := os.ReadFile(path)
	os.WriteFile(path, data[:len(data)-5], 0o644)

	w2, _ := Open(path, 512*1024*1024)
	defer w2.Close()
	logs, _, err := w2.Replay()
	if err != nil {
		t.Fatalf("replay should succeed with partial entry: %v", err)
	}
	if len(logs) < 1 {
		t.Fatal("should recover at least the first complete entry")
	}
	if logs[0].Body != "good" {
		t.Errorf("first entry Body = %q, want good", logs[0].Body)
	}
}

func TestWAL_EmptyReplay(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	defer w.Close()

	logs, traces, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 0 || len(traces) != 0 {
		t.Error("empty WAL should replay nothing")
	}
}

func TestWAL_MixedLogTrace(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)

	w.AppendLog(&schema.LogRow{TimestampUnixNano: 1000, Body: "log1"})
	w.AppendTrace(&schema.TraceRow{TimestampUnixNano: 2000, TraceID: "t1"})
	w.AppendLog(&schema.LogRow{TimestampUnixNano: 3000, Body: "log2"})
	w.Close()

	w2, _ := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	defer w2.Close()
	logs, traces, _ := w2.Replay()

	if len(logs) != 2 {
		t.Errorf("logs = %d, want 2", len(logs))
	}
	if len(traces) != 1 {
		t.Errorf("traces = %d, want 1", len(traces))
	}
}
