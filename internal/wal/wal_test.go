package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
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
	defer func() { _ = w2.Close() }()

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
	_ = w.Close()

	w2, _ := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	defer func() { _ = w2.Close() }()
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

	_ = w.AppendLog(&schema.LogRow{TimestampUnixNano: 1000, Body: "before"})
	if w.Size() == 0 {
		t.Fatal("size should be > 0 after append")
	}

	if err := w.Truncate(); err != nil {
		t.Fatal(err)
	}
	if w.Size() != 0 {
		t.Errorf("size after truncate = %d, want 0", w.Size())
	}

	_ = w.AppendLog(&schema.LogRow{TimestampUnixNano: 2000, Body: "after"})
	_ = w.Close()

	w2, _ := Open(path, 512*1024*1024)
	defer func() { _ = w2.Close() }()
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
			_ = w.Close()
			return
		}
	}
	_ = w.Close()
	t.Fatal("expected WAL full error")
}

func TestWAL_CorruptPartialEntry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	w, _ := Open(path, 512*1024*1024)

	_ = w.AppendLog(&schema.LogRow{TimestampUnixNano: 1000, Body: "good"})
	_ = w.AppendLog(&schema.LogRow{TimestampUnixNano: 2000, Body: "also good"})
	_ = w.Close()

	// Truncate file mid-entry to simulate crash
	data, _ := os.ReadFile(path)
	_ = os.WriteFile(path, data[:len(data)-5], 0o600)

	w2, _ := Open(path, 512*1024*1024)
	defer func() { _ = w2.Close() }()
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
	defer func() { _ = w.Close() }()

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

	_ = w.AppendLog(&schema.LogRow{TimestampUnixNano: 1000, Body: "log1"})
	_ = w.AppendTrace(&schema.TraceRow{TimestampUnixNano: 2000, TraceID: "t1"})
	_ = w.AppendLog(&schema.LogRow{TimestampUnixNano: 3000, Body: "log2"})
	_ = w.Close()

	w2, _ := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	defer func() { _ = w2.Close() }()
	logs, traces, _ := w2.Replay()

	if len(logs) != 2 {
		t.Errorf("logs = %d, want 2", len(logs))
	}
	if len(traces) != 1 {
		t.Errorf("traces = %d, want 1", len(traces))
	}
}

func TestWAL_Open_InvalidPath(t *testing.T) {
	// MkdirAll should fail on an impossible path
	_, err := Open("/dev/null/impossible/wal.bin", 1024)
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
	if !strings.Contains(err.Error(), "create WAL dir") {
		t.Errorf("expected 'create WAL dir' in error, got: %v", err)
	}
}

func TestWAL_Open_UnreadableFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")
	// Create a directory where the file would be — prevents OpenFile
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := Open(path, 1024)
	if err == nil {
		t.Fatal("expected error opening a directory as file")
	}
}

func TestWAL_AppendAfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	err = w.AppendLog(&schema.LogRow{TimestampUnixNano: 1000, Body: "after close"})
	if err == nil {
		t.Fatal("expected error appending to closed WAL")
	}
}

func TestWAL_AppendTraceAfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	err = w.AppendTrace(&schema.TraceRow{TimestampUnixNano: 1000, TraceID: "t1"})
	if err == nil {
		t.Fatal("expected error appending trace to closed WAL")
	}
}

func TestWAL_ReplayUnknownModeByte(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")

	// First write a valid log entry to get proper gob data
	w, err := Open(path, 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.AppendLog(&schema.LogRow{TimestampUnixNano: 1000, Body: "valid"})
	_ = w.Close()

	// Read the file, find the mode byte of the first entry (at offset 4) and change it
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Build a synthetic WAL with an unknown mode byte.
	// Entry format: [4 bytes LE length][1 byte mode][length bytes gob data]
	// Extract the gob payload from the first entry
	entryLen := binary.LittleEndian.Uint32(data[:4])
	gobData := data[5 : 5+entryLen]

	// Write a valid entry followed by an unknown-mode entry
	var synth []byte
	// First: valid log entry
	synth = append(synth, data[:5+entryLen]...)
	// Second: unknown mode 'X' with same gob payload
	header := make([]byte, 5)
	binary.LittleEndian.PutUint32(header[:4], uint32(len(gobData)))
	header[4] = 'X' // unknown mode
	synth = append(synth, header...)
	synth = append(synth, gobData...)

	_ = os.WriteFile(path, synth, 0o600)

	w2, err := Open(path, 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	logs, traces, err := w2.Replay()
	if err != nil {
		t.Fatalf("replay should succeed (stop at unknown mode): %v", err)
	}
	// Should recover the first valid log entry and stop at the unknown mode
	if len(logs) != 1 {
		t.Errorf("logs = %d, want 1", len(logs))
	}
	if len(traces) != 0 {
		t.Errorf("traces = %d, want 0", len(traces))
	}
}

func TestWAL_ReplayCorruptGobData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")

	// Write a valid entry first
	w, err := Open(path, 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.AppendLog(&schema.LogRow{TimestampUnixNano: 1000, Body: "valid"})
	_ = w.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	entryLen := binary.LittleEndian.Uint32(data[:4])
	validEntry := data[:5+entryLen]

	// Append a corrupt log entry: valid header but garbage gob data
	var synth []byte
	synth = append(synth, validEntry...)
	corruptHeader := make([]byte, 5)
	corruptPayload := []byte("this is not valid gob data at all")
	binary.LittleEndian.PutUint32(corruptHeader[:4], uint32(len(corruptPayload)))
	corruptHeader[4] = 'L' // log mode
	synth = append(synth, corruptHeader...)
	synth = append(synth, corruptPayload...)

	_ = os.WriteFile(path, synth, 0o600)

	w2, err := Open(path, 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	logs, _, err := w2.Replay()
	if err != nil {
		t.Fatalf("replay error: %v", err)
	}
	if len(logs) != 1 {
		t.Errorf("logs = %d, want 1 (corrupt entry should stop replay)", len(logs))
	}
}

func TestWAL_ReplayCorruptTraceGobData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")

	// Write a valid trace entry
	w, err := Open(path, 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.AppendTrace(&schema.TraceRow{TimestampUnixNano: 1000, TraceID: "t1"})
	_ = w.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	entryLen := binary.LittleEndian.Uint32(data[:4])
	validEntry := data[:5+entryLen]

	// Append corrupt trace entry
	var synth []byte
	synth = append(synth, validEntry...)
	corruptHeader := make([]byte, 5)
	corruptPayload := []byte("corrupt trace gob data here")
	binary.LittleEndian.PutUint32(corruptHeader[:4], uint32(len(corruptPayload)))
	corruptHeader[4] = 'T' // trace mode
	synth = append(synth, corruptHeader...)
	synth = append(synth, corruptPayload...)

	_ = os.WriteFile(path, synth, 0o600)

	w2, err := Open(path, 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	_, traces, err := w2.Replay()
	if err != nil {
		t.Fatalf("replay error: %v", err)
	}
	if len(traces) != 1 {
		t.Errorf("traces = %d, want 1 (corrupt entry should stop replay)", len(traces))
	}
}

func TestWAL_LargeEntry(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}

	largeBody := strings.Repeat("x", 64*1024) // 64KB body
	row := &schema.LogRow{TimestampUnixNano: 1000, Body: largeBody, ServiceName: "bigsvc"}
	if err := w.AppendLog(row); err != nil {
		t.Fatalf("AppendLog with large body: %v", err)
	}

	sizeBefore := w.Size()
	if sizeBefore < int64(64*1024) {
		t.Errorf("size %d should be >= 64KB for large entry", sizeBefore)
	}

	_ = w.Close()

	w2, _ := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	defer func() { _ = w2.Close() }()
	logs, _, err := w2.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 {
		t.Fatalf("logs = %d, want 1", len(logs))
	}
	if logs[0].Body != largeBody {
		t.Error("large body not round-tripped correctly")
	}
}

func TestWAL_IsFull_Transition(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	if w.IsFull() {
		t.Error("new WAL should not be full")
	}

	// Now create a WAL with tiny capacity
	w2, err := Open(filepath.Join(dir, "wal_tiny.bin"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()

	// Append will fail (any entry is > 1 byte)
	err = w2.AppendLog(&schema.LogRow{TimestampUnixNano: 1, Body: "a"})
	if err != nil {
		// WAL was full from the start (capacity 1, but size 0 so first append may work)
		// Actually, the capacity check is w.size >= w.max. size starts at 0, max=1.
		// First append should succeed because 0 < 1.
		t.Fatalf("first append to tiny WAL should succeed: %v", err)
	}
	// After append, size should exceed 1 byte
	if !w2.IsFull() {
		t.Error("tiny WAL should be full after one append")
	}

	// Second append should fail
	err = w2.AppendLog(&schema.LogRow{TimestampUnixNano: 2, Body: "b"})
	if err == nil {
		t.Fatal("expected WAL full error on second append")
	}
	if !strings.Contains(err.Error(), "WAL full") {
		t.Errorf("expected 'WAL full' in error, got: %v", err)
	}
}

func TestWAL_ReplayAfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	// Replay on closed file should fail at seek
	_, _, err = w.Replay()
	if err == nil {
		t.Fatal("expected error replaying closed WAL")
	}
}

func TestWAL_DoubleClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Second close should return an error
	err = w.Close()
	if err == nil {
		t.Fatal("expected error on double close")
	}
}

func TestWAL_TruncateAfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.AppendLog(&schema.LogRow{TimestampUnixNano: 1, Body: "test"})
	_ = w.Close()

	// Truncate on closed WAL should fail at the initial file.Close()
	err = w.Truncate()
	if err == nil {
		t.Fatal("expected error truncating closed WAL")
	}
}

func TestWAL_ReplayPartialHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.bin")

	// Write just 3 bytes (less than the 5-byte header)
	_ = os.WriteFile(path, []byte{0x01, 0x02, 0x03}, 0o600)

	w, err := Open(path, 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	logs, traces, err := w.Replay()
	if err != nil {
		t.Fatalf("replay should succeed with partial header: %v", err)
	}
	if len(logs) != 0 || len(traces) != 0 {
		t.Error("should recover nothing from partial header")
	}
}

func TestWAL_Size_AfterMultipleAppends(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(filepath.Join(dir, "wal.bin"), 512*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()

	if w.Size() != 0 {
		t.Errorf("initial size = %d, want 0", w.Size())
	}

	_ = w.AppendLog(&schema.LogRow{TimestampUnixNano: 1, Body: "a"})
	s1 := w.Size()
	if s1 == 0 {
		t.Fatal("size should be > 0 after first append")
	}

	_ = w.AppendLog(&schema.LogRow{TimestampUnixNano: 2, Body: "b"})
	s2 := w.Size()
	if s2 <= s1 {
		t.Errorf("size should grow: %d <= %d", s2, s1)
	}
}
