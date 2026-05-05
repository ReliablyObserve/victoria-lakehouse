# Phase A: Write Durability & Adaptive Sizing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add WAL crash recovery, adaptive file sizing, buffer query bridge, and manifest label pruning so the write path is production-durable and queries see data with near-zero delay.

**Architecture:** WAL is a new `internal/wal/` package — append-only binary file with gob encoding. Adaptive sizing extends `BatchWriter.checkSizeThreshold()` with per-partition byte estimation. Buffer bridge adds an HTTP endpoint on insert pods and a client in the select path. FileInfo.Labels populated during flush for manifest-level query pruning.

**Tech Stack:** Go stdlib (`encoding/gob`, `os`, `sync`), existing parquet-go, existing manifest/config/schema packages.

**Spec:** `docs/superpowers/specs/2026-05-04-storage-parity-design.md` — Phase A sections A1-A4.

---

## File Structure

### New Files
| File | Responsibility |
|---|---|
| `internal/wal/wal.go` | WAL: append, truncate, replay, size tracking |
| `internal/wal/wal_test.go` | WAL unit tests |
| `internal/insertapi/buffer_handler.go` | HTTP endpoint: `/internal/buffer/query` |
| `internal/insertapi/buffer_handler_test.go` | Buffer query endpoint tests |
| `internal/storage/parquets3/buffer_bridge.go` | Select-side client for buffer queries |
| `internal/storage/parquets3/buffer_bridge_test.go` | Buffer bridge tests |
| `internal/storage/parquets3/labels.go` | Label extraction from log/trace rows |
| `internal/storage/parquets3/labels_test.go` | Label extraction tests |

### Modified Files
| File | Changes |
|---|---|
| `internal/config/config.go` | Add `TargetFileSize`, `WALMaxBytes` to InsertConfig; add `SelectConfig` with buffer query fields; add `RetentionConfig`, `CompactionConfig` stubs; validation and merge |
| `internal/config/config_test.go` | Tests for new config fields |
| `internal/manifest/manifest.go` | Add `Labels` field to FileInfo; add `MatchesLabels()`, `AllFiles()`, `RemoveFile()` methods |
| `internal/manifest/manifest_test.go` | Tests for label matching and new methods |
| `internal/storage/parquets3/writer.go` | Integrate WAL, adaptive sizing trigger, label population in flush |
| `internal/storage/parquets3/writer_test.go` | Tests for WAL integration, adaptive sizing, label population |
| `internal/storage/parquets3/storage.go` | Integrate buffer bridge in RunQuery, WAL startup recovery |

---

## Task 1: WAL Core — Append and Replay

**Files:**
- Create: `internal/wal/wal.go`
- Create: `internal/wal/wal_test.go`

- [ ] **Step 1: Write the failing test for append + replay round-trip**

In `internal/wal/wal_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/wal/ -run TestWAL_AppendReplayLogs -v`
Expected: FAIL — package doesn't exist yet

- [ ] **Step 3: Write WAL implementation**

In `internal/wal/wal.go`:

```go
package wal

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

const (
	modeLog   byte = 'L'
	modeTrace byte = 'T'
)

type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
	size int64
	max  int64
}

func Open(path string, maxBytes int64) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &WAL{file: f, path: path, size: info.Size(), max: maxBytes}, nil
}

func (w *WAL) AppendLog(row *schema.LogRow) error {
	return w.append(modeLog, row)
}

func (w *WAL) AppendTrace(row *schema.TraceRow) error {
	return w.append(modeTrace, row)
}

func (w *WAL) append(mode byte, row any) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.size >= w.max {
		return fmt.Errorf("WAL full (%d >= %d bytes)", w.size, w.max)
	}

	var buf []byte
	enc := gob.NewEncoder(writerFunc(func(p []byte) (int, error) {
		buf = append(buf, p...)
		return len(p), nil
	}))
	if err := enc.Encode(row); err != nil {
		return fmt.Errorf("gob encode: %w", err)
	}

	header := make([]byte, 5)
	binary.LittleEndian.PutUint32(header[:4], uint32(len(buf)))
	header[4] = mode

	if _, err := w.file.Write(header); err != nil {
		return err
	}
	if _, err := w.file.Write(buf); err != nil {
		return err
	}

	w.size += int64(5 + len(buf))
	return nil
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func (w *WAL) Replay() ([]schema.LogRow, []schema.TraceRow, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, nil, err
	}

	var logs []schema.LogRow
	var traces []schema.TraceRow

	for {
		var header [5]byte
		if _, err := io.ReadFull(w.file, header[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return logs, traces, err
		}
		length := binary.LittleEndian.Uint32(header[:4])
		mode := header[4]

		data := make([]byte, length)
		if _, err := io.ReadFull(w.file, data); err != nil {
			break // partial entry from crash — stop here
		}

		switch mode {
		case modeLog:
			var row schema.LogRow
			if err := gob.NewDecoder(readerFunc(data)).Decode(&row); err != nil {
				break // corrupt entry — stop replay
			}
			logs = append(logs, row)
		case modeTrace:
			var row schema.TraceRow
			if err := gob.NewDecoder(readerFunc(data)).Decode(&row); err != nil {
				break
			}
			traces = append(traces, row)
		}
	}

	return logs, traces, nil
}

type readerFunc []byte

func (r *readerFunc) Read(p []byte) (int, error) {
	if len(*r) == 0 {
		return 0, io.EOF
	}
	n := copy(p, *r)
	*r = (*r)[n:]
	return n, nil
}

func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Close(); err != nil {
		return err
	}

	tmp := w.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	f.Close()

	if err := os.Rename(tmp, w.path); err != nil {
		return err
	}

	w.file, err = os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.size = 0
	return nil
}

func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

func (w *WAL) IsFull() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size >= w.max
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/wal/ -run TestWAL_AppendReplayLogs -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /tmp/victoria-lakehouse
git add internal/wal/wal.go internal/wal/wal_test.go
git commit -m "feat(wal): append-only WAL with gob encoding and replay"
```

---

## Task 2: WAL — Truncate, Size Limits, and Corrupt Recovery

**Files:**
- Modify: `internal/wal/wal_test.go`

- [ ] **Step 1: Write tests for truncate, size limit, and corrupt recovery**

Append to `internal/wal/wal_test.go`:

```go
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
```

- [ ] **Step 2: Run tests**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/wal/ -v`
Expected: ALL PASS

- [ ] **Step 3: Commit**

```bash
cd /tmp/victoria-lakehouse
git add internal/wal/wal_test.go
git commit -m "test(wal): truncate, size limits, corrupt recovery, mixed types"
```

---

## Task 3: Config — TargetFileSize, WALMaxBytes, SelectConfig

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`

- [ ] **Step 1: Write failing tests for new config fields**

Append to `internal/config/config_test.go`:

```go
func TestDefaultConfig_TargetFileSize(t *testing.T) {
	cfg := Default()
	if cfg.Insert.TargetFileSize != "128MB" {
		t.Errorf("TargetFileSize = %q, want 128MB", cfg.Insert.TargetFileSize)
	}
}

func TestDefaultConfig_WALMaxBytes(t *testing.T) {
	cfg := Default()
	if cfg.Insert.WALMaxBytes != "512MB" {
		t.Errorf("WALMaxBytes = %q, want 512MB", cfg.Insert.WALMaxBytes)
	}
}

func TestDefaultConfig_WALEnabled(t *testing.T) {
	cfg := Default()
	if !cfg.Insert.WALEnabled {
		t.Error("WALEnabled should default to true")
	}
}

func TestDefaultConfig_SelectConfig(t *testing.T) {
	cfg := Default()
	if !cfg.Select.BufferQueryEnabled {
		t.Error("BufferQueryEnabled should default to true")
	}
	if cfg.Select.BufferQueryTimeout != 2*time.Second {
		t.Errorf("BufferQueryTimeout = %v, want 2s", cfg.Select.BufferQueryTimeout)
	}
}

func TestInsertConfig_TargetFileSizeN(t *testing.T) {
	ic := &InsertConfig{TargetFileSize: "128MB"}
	got := ic.TargetFileSizeN()
	want := int64(128 * 1024 * 1024)
	if got != want {
		t.Errorf("TargetFileSizeN = %d, want %d", got, want)
	}
}

func TestInsertConfig_WALMaxBytesN(t *testing.T) {
	ic := &InsertConfig{WALMaxBytes: "512MB"}
	got := ic.WALMaxBytesN()
	want := int64(512 * 1024 * 1024)
	if got != want {
		t.Errorf("WALMaxBytesN = %d, want %d", got, want)
	}
}

func TestValidate_TargetFileSizeRequired(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Insert.TargetFileSize = ""
	if err := cfg.Validate(); err == nil {
		t.Error("empty TargetFileSize should fail validation")
	}
}

func TestMergeConfig_SelectFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Select.BufferQueryTimeout = 5 * time.Second
	overlay.Select.InsertHeadlessService = "lakehouse-insert-headless"

	result := mergeConfig(base, overlay)
	if result.Select.BufferQueryTimeout != 5*time.Second {
		t.Errorf("BufferQueryTimeout = %v", result.Select.BufferQueryTimeout)
	}
	if result.Select.InsertHeadlessService != "lakehouse-insert-headless" {
		t.Errorf("InsertHeadlessService = %q", result.Select.InsertHeadlessService)
	}
}

func TestMergeConfig_TargetFileSize(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Insert.TargetFileSize = "256MB"

	result := mergeConfig(base, overlay)
	if result.Insert.TargetFileSize != "256MB" {
		t.Errorf("TargetFileSize = %q, want 256MB", result.Insert.TargetFileSize)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/config/ -run "TestDefaultConfig_TargetFileSize|TestDefaultConfig_WALMaxBytes|TestDefaultConfig_WALEnabled|TestDefaultConfig_SelectConfig|TestInsertConfig_TargetFileSizeN|TestInsertConfig_WALMaxBytesN|TestValidate_TargetFileSizeRequired|TestMergeConfig_SelectFields|TestMergeConfig_TargetFileSize" -v`
Expected: FAIL — fields don't exist yet

- [ ] **Step 3: Add new fields to config.go**

Add `TargetFileSize` and `WALMaxBytes` to `InsertConfig`:

```go
type InsertConfig struct {
	FlushInterval    time.Duration `yaml:"flush_interval"`
	MaxBufferRows    int           `yaml:"max_buffer_rows"`
	MaxBufferBytes   string        `yaml:"max_buffer_bytes"`
	TargetFileSize   string        `yaml:"target_file_size"`
	RowGroupSize     int           `yaml:"row_group_size"`
	BloomColumns     []string      `yaml:"bloom_columns"`
	CompressionLevel int           `yaml:"compression_level"`
	WALEnabled       bool          `yaml:"wal_enabled"`
	WALDir           string        `yaml:"wal_dir"`
	WALMaxBytes      string        `yaml:"wal_max_bytes"`
}
```

Add helper methods:

```go
func (c *InsertConfig) TargetFileSizeN() int64 {
	n, _ := ParseSizeBytes(c.TargetFileSize)
	if n <= 0 {
		return 128 * 1024 * 1024
	}
	return n
}

func (c *InsertConfig) WALMaxBytesN() int64 {
	n, _ := ParseSizeBytes(c.WALMaxBytes)
	if n <= 0 {
		return 512 * 1024 * 1024
	}
	return n
}
```

Add `SelectConfig` type:

```go
type SelectConfig struct {
	BufferQueryEnabled    bool          `yaml:"buffer_query_enabled"`
	InsertHeadlessService string        `yaml:"insert_headless_service"`
	BufferQueryTimeout    time.Duration `yaml:"buffer_query_timeout"`
}
```

Add `Select SelectConfig` field to `Config` struct.

Update `Default()` — set `WALEnabled: true`, `WALMaxBytes: "512MB"`, `TargetFileSize: "128MB"`, and `Select` defaults.

Update `Validate()` — add check for non-empty `TargetFileSize` when insert enabled.

Update `mergeConfig()` — add Select section merge.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/config/ -v`
Expected: ALL PASS

- [ ] **Step 5: Run full test suite**

Run: `cd /tmp/victoria-lakehouse && go test ./... `
Expected: ALL PASS (738+ tests)

- [ ] **Step 6: Commit**

```bash
cd /tmp/victoria-lakehouse
git add internal/config/config.go internal/config/config_test.go
git commit -m "feat(config): add TargetFileSize, WALMaxBytes, SelectConfig"
```

---

## Task 4: Manifest — Labels Field and MatchesLabels

**Files:**
- Modify: `internal/manifest/manifest.go`
- Modify: `internal/manifest/manifest_test.go`

- [ ] **Step 1: Write failing tests for label matching**

Append to `internal/manifest/manifest_test.go`:

```go
func TestFileInfo_Labels(t *testing.T) {
	fi := FileInfo{
		Key:  "test.parquet",
		Size: 1000,
		Labels: map[string][]string{
			"service.name":  {"api", "worker"},
			"severity_text": {"INFO", "ERROR"},
		},
	}

	if !fi.MatchesLabel("service.name", "api") {
		t.Error("should match service.name=api")
	}
	if !fi.MatchesLabel("service.name", "worker") {
		t.Error("should match service.name=worker")
	}
	if fi.MatchesLabel("service.name", "unknown") {
		t.Error("should not match service.name=unknown")
	}
	if fi.MatchesLabel("missing.field", "any") {
		t.Error("should not match missing field")
	}
}

func TestFileInfo_MatchesLabel_NilLabels(t *testing.T) {
	fi := FileInfo{Key: "test.parquet", Size: 1000}
	if fi.MatchesLabel("service.name", "api") {
		t.Error("nil labels should not match")
	}
}

func TestManifest_AllFiles(t *testing.T) {
	m := newTestManifest()
	m.AddFile("dt=2026-05-02/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	m.AddFile("dt=2026-05-02/hour=11", FileInfo{Key: "b.parquet", Size: 200})
	m.AddFile("dt=2026-05-02/hour=10", FileInfo{Key: "c.parquet", Size: 300})

	all := m.AllFiles()
	total := 0
	for _, files := range all {
		total += len(files)
	}
	if total != 3 {
		t.Errorf("AllFiles total = %d, want 3", total)
	}
}

func TestManifest_RemoveFile(t *testing.T) {
	m := newTestManifest()
	m.AddFile("dt=2026-05-02/hour=10", FileInfo{Key: "a.parquet", Size: 100})
	m.AddFile("dt=2026-05-02/hour=10", FileInfo{Key: "b.parquet", Size: 200})

	m.RemoveFile("dt=2026-05-02/hour=10", "a.parquet")

	if m.TotalFiles() != 1 {
		t.Errorf("TotalFiles = %d, want 1", m.TotalFiles())
	}
	if m.TotalBytes() != 200 {
		t.Errorf("TotalBytes = %d, want 200", m.TotalBytes())
	}
}

func TestManifest_RemoveFile_NotFound(t *testing.T) {
	m := newTestManifest()
	m.AddFile("dt=2026-05-02/hour=10", FileInfo{Key: "a.parquet", Size: 100})

	m.RemoveFile("dt=2026-05-02/hour=10", "nonexistent.parquet")

	if m.TotalFiles() != 1 {
		t.Errorf("TotalFiles = %d, want 1 (no change)", m.TotalFiles())
	}
}

func TestManifest_SaveLoadRoundTrip_WithLabels(t *testing.T) {
	m := newTestManifest()
	may2h10 := time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)

	fi := FileInfo{
		Key:       "test.parquet",
		Size:      1000,
		RowCount:  50,
		MinTimeNs: may2h10.UnixNano(),
		MaxTimeNs: may2h10.Add(30 * time.Minute).UnixNano(),
		Labels: map[string][]string{
			"service.name": {"api", "worker"},
		},
	}
	m.AddFile("dt=2026-05-02/hour=10", fi)

	path := t.TempDir() + "/manifest.json"
	m.SaveTo(path)

	m2 := newTestManifest()
	m2.LoadFrom(path)

	files := m2.GetFilesForRange(may2h10.UnixNano(), may2h10.Add(time.Hour).UnixNano())
	if len(files) != 1 {
		t.Fatalf("files = %d, want 1", len(files))
	}
	if !files[0].MatchesLabel("service.name", "api") {
		t.Error("labels not preserved after save/load")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/manifest/ -run "TestFileInfo_Labels|TestFileInfo_MatchesLabel_NilLabels|TestManifest_AllFiles|TestManifest_RemoveFile|TestManifest_SaveLoadRoundTrip_WithLabels" -v`
Expected: FAIL

- [ ] **Step 3: Add Labels field and methods to manifest.go**

Add `Labels` field to `FileInfo` (already has json tags for other fields):

```go
type FileInfo struct {
	Key               string              `json:"key"`
	Size              int64               `json:"size"`
	RowCount          int64               `json:"row_count,omitempty"`
	MinTimeNs         int64               `json:"min_time_ns,omitempty"`
	MaxTimeNs         int64               `json:"max_time_ns,omitempty"`
	RawBytes          int64               `json:"raw_bytes,omitempty"`
	SchemaFingerprint string              `json:"schema_fp,omitempty"`
	CompactionLevel   int                 `json:"compaction_level,omitempty"`
	Labels            map[string][]string `json:"labels,omitempty"`
}
```

Add `MatchesLabel` method:

```go
func (fi FileInfo) MatchesLabel(field, value string) bool {
	if fi.Labels == nil {
		return false
	}
	for _, v := range fi.Labels[field] {
		if v == value {
			return true
		}
	}
	return false
}
```

Add `AllFiles` and `RemoveFile` to Manifest:

```go
func (m *Manifest) AllFiles() map[string][]FileInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snap := make(map[string][]FileInfo, len(m.files))
	for k, v := range m.files {
		cp := make([]FileInfo, len(v))
		copy(cp, v)
		snap[k] = cp
	}
	return snap
}

func (m *Manifest) RemoveFile(partition string, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	files := m.files[partition]
	for i, fi := range files {
		if fi.Key == key {
			m.totalFiles--
			m.totalBytes -= fi.Size
			m.files[partition] = append(files[:i], files[i+1:]...)
			if len(m.files[partition]) == 0 {
				delete(m.files, partition)
			}
			return
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/manifest/ -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
cd /tmp/victoria-lakehouse
git add internal/manifest/manifest.go internal/manifest/manifest_test.go
git commit -m "feat(manifest): Labels field, MatchesLabel, AllFiles, RemoveFile"
```

---

## Task 5: Label Extraction from Rows

**Files:**
- Create: `internal/storage/parquets3/labels.go`
- Create: `internal/storage/parquets3/labels_test.go`

- [ ] **Step 1: Write failing tests**

In `internal/storage/parquets3/labels_test.go`:

```go
package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestExtractLogLabels(t *testing.T) {
	rows := []schema.LogRow{
		{ServiceName: "api", SeverityText: "INFO", K8sNamespaceName: "prod"},
		{ServiceName: "api", SeverityText: "ERROR", K8sNamespaceName: "prod"},
		{ServiceName: "worker", SeverityText: "INFO", K8sNamespaceName: "staging"},
	}

	labels := extractLogLabels(rows)

	if len(labels["service.name"]) != 2 {
		t.Errorf("service.name values = %d, want 2", len(labels["service.name"]))
	}
	if len(labels["severity_text"]) != 2 {
		t.Errorf("severity_text values = %d, want 2", len(labels["severity_text"]))
	}
	if len(labels["k8s.namespace.name"]) != 2 {
		t.Errorf("k8s.namespace.name values = %d, want 2", len(labels["k8s.namespace.name"]))
	}
}

func TestExtractLogLabels_Empty(t *testing.T) {
	labels := extractLogLabels(nil)
	if len(labels) != 0 {
		t.Error("empty rows should produce empty labels")
	}
}

func TestExtractLogLabels_Cap(t *testing.T) {
	rows := make([]schema.LogRow, 200)
	for i := range rows {
		rows[i].ServiceName = fmt.Sprintf("svc-%d", i)
	}

	labels := extractLogLabels(rows)
	if len(labels["service.name"]) > 100 {
		t.Errorf("should cap at 100, got %d", len(labels["service.name"]))
	}
}

func TestExtractTraceLabels(t *testing.T) {
	rows := []schema.TraceRow{
		{ServiceName: "api", SpanName: "GET /users"},
		{ServiceName: "api", SpanName: "POST /orders"},
	}

	labels := extractTraceLabels(rows)

	if len(labels["service.name"]) != 1 {
		t.Errorf("service.name values = %d, want 1", len(labels["service.name"]))
	}
	if len(labels["span.name"]) != 2 {
		t.Errorf("span.name values = %d, want 2", len(labels["span.name"]))
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/storage/parquets3/ -run "TestExtractLogLabels|TestExtractTraceLabels" -v`
Expected: FAIL

- [ ] **Step 3: Implement label extraction**

In `internal/storage/parquets3/labels.go`:

```go
package parquets3

import (
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

const maxLabelsPerField = 100

func extractLogLabels(rows []schema.LogRow) map[string][]string {
	if len(rows) == 0 {
		return nil
	}
	sets := map[string]map[string]bool{}
	for i := range rows {
		addLabel(sets, "service.name", rows[i].ServiceName)
		addLabel(sets, "severity_text", rows[i].SeverityText)
		addLabel(sets, "k8s.namespace.name", rows[i].K8sNamespaceName)
		addLabel(sets, "k8s.pod.name", rows[i].K8sPodName)
		addLabel(sets, "k8s.deployment.name", rows[i].K8sDeploymentName)
		addLabel(sets, "k8s.node.name", rows[i].K8sNodeName)
		addLabel(sets, "deployment.environment", rows[i].DeployEnv)
		addLabel(sets, "cloud.region", rows[i].CloudRegion)
		addLabel(sets, "host.name", rows[i].HostName)
		addLabel(sets, "trace_id", rows[i].TraceID)
	}
	return setsToLabels(sets)
}

func extractTraceLabels(rows []schema.TraceRow) map[string][]string {
	if len(rows) == 0 {
		return nil
	}
	sets := map[string]map[string]bool{}
	for i := range rows {
		addLabel(sets, "service.name", rows[i].ServiceName)
		addLabel(sets, "span.name", rows[i].SpanName)
		addLabel(sets, "status.code", rows[i].StatusCode)
	}
	return setsToLabels(sets)
}

func addLabel(sets map[string]map[string]bool, field, value string) {
	if value == "" {
		return
	}
	s, ok := sets[field]
	if !ok {
		s = make(map[string]bool)
		sets[field] = s
	}
	if len(s) < maxLabelsPerField {
		s[value] = true
	}
}

func setsToLabels(sets map[string]map[string]bool) map[string][]string {
	labels := make(map[string][]string, len(sets))
	for k, vs := range sets {
		vals := make([]string, 0, len(vs))
		for v := range vs {
			vals = append(vals, v)
		}
		labels[k] = vals
	}
	return labels
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/storage/parquets3/ -run "TestExtractLogLabels|TestExtractTraceLabels" -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
cd /tmp/victoria-lakehouse
git add internal/storage/parquets3/labels.go internal/storage/parquets3/labels_test.go
git commit -m "feat(labels): extract label summary from log/trace rows"
```

---

## Task 6: Integrate WAL and Labels into BatchWriter

**Files:**
- Modify: `internal/storage/parquets3/writer.go`
- Modify: `internal/storage/parquets3/writer_test.go`

- [ ] **Step 1: Write failing tests**

Append to `internal/storage/parquets3/writer_test.go`:

```go
func TestFlushAll_PopulatesLabels(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	bw, m := testWriter(t, s3srv.URL)

	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	rows := []schema.LogRow{
		{TimestampUnixNano: base.UnixNano(), Body: "a", ServiceName: "api", SeverityText: "INFO"},
		{TimestampUnixNano: base.Add(time.Second).UnixNano(), Body: "b", ServiceName: "worker", SeverityText: "ERROR"},
	}
	bw.AddLogRows(rows)

	if err := bw.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}

	files := m.GetFilesForRange(base.UnixNano(), base.Add(time.Hour).UnixNano())
	if len(files) != 1 {
		t.Fatalf("files = %d, want 1", len(files))
	}
	if files[0].Labels == nil {
		t.Fatal("Labels should be populated")
	}
	if !files[0].MatchesLabel("service.name", "api") {
		t.Error("should contain service.name=api")
	}
	if !files[0].MatchesLabel("service.name", "worker") {
		t.Error("should contain service.name=worker")
	}
	if !files[0].MatchesLabel("severity_text", "INFO") {
		t.Error("should contain severity_text=INFO")
	}
}

func TestAdaptiveFlush_TargetFileSize(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()

	pool := testPool(t, s3srv.URL)
	m := manifest.New("test-bucket", "logs/", slog.Default())
	cfg := testInsertConfig()
	cfg.MaxBufferRows = 1000000 // high row limit so it doesn't trigger
	cfg.TargetFileSize = "1KB" // very low target so byte check triggers
	bw := NewBatchWriter(cfg, pool, m, "logs/", config.ModeLogs, slog.Default())

	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	bw.AddLogRows(sampleLogRows(50, base))

	// Should have auto-flushed due to per-partition size exceeding 1KB
	if got := m.TotalFiles(); got < 1 {
		t.Errorf("TotalFiles = %d, want >= 1 (adaptive flush should trigger)", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/storage/parquets3/ -run "TestFlushAll_PopulatesLabels|TestAdaptiveFlush_TargetFileSize" -v`
Expected: FAIL

- [ ] **Step 3: Integrate labels into flush and adaptive sizing into checkSizeThreshold**

In `writer.go`, update `flushLogPartition` to populate labels:

```go
func (w *BatchWriter) flushLogPartition(ctx context.Context, partition string, rows []schema.LogRow) error {
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].TimestampUnixNano < rows[j].TimestampUnixNano
	})

	result, err := writeLogsParquet(rows, w.cfg.RowGroupSize, w.cfg.CompressionLevel)
	if err != nil {
		return fmt.Errorf("write parquet: %w", err)
	}

	batchID := randomBatchID()
	key := fmt.Sprintf("%s%s/%s.parquet", w.prefix, partition, batchID)

	if err := w.pool.Upload(ctx, key, result.Data); err != nil {
		return err
	}

	fi := manifest.FileInfo{
		Key:               key,
		Size:              int64(len(result.Data)),
		RowCount:          int64(len(rows)),
		MinTimeNs:         rows[0].TimestampUnixNano,
		MaxTimeNs:         rows[len(rows)-1].TimestampUnixNano,
		RawBytes:          result.RawBytes,
		SchemaFingerprint: schemaFingerprint(w.mode),
		Labels:            extractLogLabels(rows),
	}
	w.manifest.AddFile(partition, fi)
	// ... rest unchanged
}
```

Similarly for `flushTracePartition`, add `Labels: extractTraceLabels(rows)`.

Update `checkSizeThreshold` to check per-partition byte estimate:

```go
func (w *BatchWriter) checkSizeThreshold() {
	total := int(w.totalRows.Load())
	if total >= w.cfg.MaxBufferRows {
		w.triggerFlush()
		return
	}

	targetBytes := w.cfg.TargetFileSizeN()
	if targetBytes <= 0 {
		return
	}

	w.mu.Lock()
	var needsFlush bool
	for _, rows := range w.logBufs {
		if estimateRawBytesLogs(rows) >= targetBytes {
			needsFlush = true
			break
		}
	}
	if !needsFlush {
		for _, rows := range w.traceBufs {
			if estimateRawBytesTraces(rows) >= targetBytes {
				needsFlush = true
				break
			}
		}
	}
	w.mu.Unlock()

	if needsFlush {
		w.triggerFlush()
	}
}

func (w *BatchWriter) triggerFlush() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := w.FlushAll(ctx); err != nil {
		w.logger.Error("triggered flush failed", "error", err)
	}
}
```

Add `TargetFileSize` field to `testInsertConfig()` in tests.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/storage/parquets3/ -v`
Expected: ALL PASS

- [ ] **Step 5: Run full test suite**

Run: `cd /tmp/victoria-lakehouse && go test ./...`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
cd /tmp/victoria-lakehouse
git add internal/storage/parquets3/writer.go internal/storage/parquets3/writer_test.go
git commit -m "feat(writer): integrate labels into flush, adaptive file sizing"
```

---

## Task 7: WAL Integration in BatchWriter

**Files:**
- Modify: `internal/storage/parquets3/writer.go`
- Modify: `internal/storage/parquets3/writer_test.go`

- [ ] **Step 1: Write failing test for WAL integration**

Append to `internal/storage/parquets3/writer_test.go`:

```go
func TestBatchWriter_WALIntegration(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()

	pool := testPool(t, s3srv.URL)
	m := manifest.New("test-bucket", "logs/", slog.Default())
	cfg := testInsertConfig()
	cfg.WALEnabled = true
	cfg.WALDir = t.TempDir()
	cfg.WALMaxBytes = "10MB"
	bw := NewBatchWriter(cfg, pool, m, "logs/", config.ModeLogs, slog.Default())

	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	bw.AddLogRows(sampleLogRows(5, base))

	// WAL should have data
	if bw.wal == nil {
		t.Fatal("WAL should be initialized")
	}
	if bw.wal.Size() == 0 {
		t.Error("WAL should have data after AddLogRows")
	}

	// Flush should truncate WAL
	if err := bw.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if bw.wal.Size() != 0 {
		t.Errorf("WAL size after flush = %d, want 0", bw.wal.Size())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/storage/parquets3/ -run TestBatchWriter_WALIntegration -v`
Expected: FAIL — `bw.wal` field doesn't exist

- [ ] **Step 3: Add WAL field to BatchWriter and wire it up**

In `writer.go`, add to BatchWriter struct:

```go
type BatchWriter struct {
	// ... existing fields ...
	wal *wal.WAL // nil if WAL disabled
}
```

Add import `"github.com/ReliablyObserve/victoria-lakehouse/internal/wal"`.

Update `NewBatchWriter`:

```go
func NewBatchWriter(cfg *config.InsertConfig, pool *s3reader.ClientPool,
	m *manifest.Manifest, prefix string, mode config.Mode, logger *slog.Logger) *BatchWriter {

	bw := &BatchWriter{
		cfg:       cfg,
		pool:      pool,
		manifest:  m,
		prefix:    prefix,
		mode:      mode,
		logger:    logger.With("component", "writer"),
		logBufs:   make(map[string][]schema.LogRow),
		traceBufs: make(map[string][]schema.TraceRow),
		stopCh:    make(chan struct{}),
	}

	if cfg.WALEnabled && cfg.WALDir != "" {
		walPath := filepath.Join(cfg.WALDir, "lakehouse.wal")
		w, err := wal.Open(walPath, cfg.WALMaxBytesN())
		if err != nil {
			logger.Error("WAL open failed, continuing without WAL", "error", err)
		} else {
			bw.wal = w
		}
	}

	return bw
}
```

Add import `"path/filepath"`.

Update `AddLogRows` to append to WAL:

```go
func (w *BatchWriter) AddLogRows(rows []schema.LogRow) {
	if len(rows) == 0 {
		return
	}

	if w.wal != nil {
		for i := range rows {
			if err := w.wal.AppendLog(&rows[i]); err != nil {
				w.logger.Error("WAL append failed", "error", err)
				break
			}
		}
	}

	// ... existing partition buffering unchanged ...
}
```

Same for `AddTraceRows` with `w.wal.AppendTrace`.

Update `FlushAll` to truncate WAL on success:

```go
func (w *BatchWriter) FlushAll(ctx context.Context) error {
	// ... existing flush logic ...

	if len(errs) == 0 && w.wal != nil {
		if err := w.wal.Truncate(); err != nil {
			w.logger.Error("WAL truncate failed", "error", err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("flush errors: %v", errs)
	}
	return nil
}
```

Add `ReplayWAL()` method for startup recovery:

```go
func (w *BatchWriter) ReplayWAL() (int, int) {
	if w.wal == nil {
		return 0, 0
	}
	logs, traces, err := w.wal.Replay()
	if err != nil {
		w.logger.Error("WAL replay error", "error", err)
	}
	if len(logs) > 0 {
		w.AddLogRows(logs)
	}
	if len(traces) > 0 {
		w.AddTraceRows(traces)
	}
	w.logger.Info("WAL replayed", "logs", len(logs), "traces", len(traces))
	return len(logs), len(traces)
}
```

Update `CanWriteData` to check WAL full:

```go
func (w *BatchWriter) CanWriteData(ctx context.Context) error {
	if w.wal != nil && w.wal.IsFull() {
		return fmt.Errorf("WAL full")
	}
	testKey := w.prefix + "_write_check"
	return w.pool.Upload(ctx, testKey, []byte("ok"))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/storage/parquets3/ -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
cd /tmp/victoria-lakehouse
git add internal/storage/parquets3/writer.go internal/storage/parquets3/writer_test.go
git commit -m "feat(writer): integrate WAL — append on write, truncate on flush, replay on startup"
```

---

## Task 8: Buffer Query Endpoint

**Files:**
- Create: `internal/insertapi/buffer_handler.go`
- Create: `internal/insertapi/buffer_handler_test.go`

- [ ] **Step 1: Write failing tests**

In `internal/insertapi/buffer_handler_test.go`:

```go
package insertapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

type mockBufferStore struct {
	logRows   []schema.LogRow
	traceRows []schema.TraceRow
}

func (m *mockBufferStore) BufferedLogRows(startNs, endNs int64) []schema.LogRow {
	var result []schema.LogRow
	for _, r := range m.logRows {
		if r.TimestampUnixNano >= startNs && r.TimestampUnixNano < endNs {
			result = append(result, r)
		}
	}
	return result
}

func (m *mockBufferStore) BufferedTraceRows(startNs, endNs int64) []schema.TraceRow {
	var result []schema.TraceRow
	for _, r := range m.traceRows {
		if r.TimestampUnixNano >= startNs && r.TimestampUnixNano < endNs {
			result = append(result, r)
		}
	}
	return result
}

func TestBufferQuery_Logs(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	store := &mockBufferStore{
		logRows: []schema.LogRow{
			{TimestampUnixNano: base.UnixNano(), Body: "a", ServiceName: "svc"},
			{TimestampUnixNano: base.Add(time.Second).UnixNano(), Body: "b", ServiceName: "svc"},
			{TimestampUnixNano: base.Add(time.Hour).UnixNano(), Body: "out of range"},
		},
	}

	h := NewBufferHandler(store)
	req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start="+
		fmt.Sprintf("%d", base.UnixNano())+"&end="+fmt.Sprintf("%d", base.Add(time.Minute).UnixNano())+
		"&mode=logs", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	body, _ := io.ReadAll(rec.Body)
	lines := splitNDJSON(body)
	if len(lines) != 2 {
		t.Errorf("got %d rows, want 2", len(lines))
	}
}

func TestBufferQuery_MissingParams(t *testing.T) {
	h := NewBufferHandler(&mockBufferStore{})
	req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestBufferQuery_Empty(t *testing.T) {
	h := NewBufferHandler(&mockBufferStore{})
	req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start=0&end=1000&mode=logs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if len(body) != 0 {
		t.Errorf("empty buffer should return empty body, got %d bytes", len(body))
	}
}

func splitNDJSON(data []byte) []json.RawMessage {
	var result []json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(data))
	for dec.More() {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			break
		}
		result = append(result, raw)
	}
	return result
}
```

Add missing imports (`"bytes"`, `"fmt"`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/insertapi/ -run "TestBufferQuery" -v`
Expected: FAIL

- [ ] **Step 3: Implement buffer query handler**

In `internal/insertapi/buffer_handler.go`:

```go
package insertapi

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

type BufferQuerier interface {
	BufferedLogRows(startNs, endNs int64) []schema.LogRow
	BufferedTraceRows(startNs, endNs int64) []schema.TraceRow
}

type BufferHandler struct {
	store BufferQuerier
}

func NewBufferHandler(store BufferQuerier) *BufferHandler {
	return &BufferHandler{store: store}
}

func (h *BufferHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	mode := r.URL.Query().Get("mode")

	if startStr == "" || endStr == "" || mode == "" {
		http.Error(w, "start, end, and mode parameters required", http.StatusBadRequest)
		return
	}

	startNs, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid start parameter", http.StatusBadRequest)
		return
	}
	endNs, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid end parameter", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")

	enc := json.NewEncoder(w)
	switch mode {
	case "logs":
		for _, row := range h.store.BufferedLogRows(startNs, endNs) {
			enc.Encode(row)
		}
	case "traces":
		for _, row := range h.store.BufferedTraceRows(startNs, endNs) {
			enc.Encode(row)
		}
	default:
		http.Error(w, "mode must be logs or traces", http.StatusBadRequest)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/insertapi/ -run "TestBufferQuery" -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
cd /tmp/victoria-lakehouse
git add internal/insertapi/buffer_handler.go internal/insertapi/buffer_handler_test.go
git commit -m "feat(insertapi): buffer query endpoint /internal/buffer/query"
```

---

## Task 9: Buffer Bridge — Select-Side Client

**Files:**
- Create: `internal/storage/parquets3/buffer_bridge.go`
- Create: `internal/storage/parquets3/buffer_bridge_test.go`

- [ ] **Step 1: Write failing tests**

In `internal/storage/parquets3/buffer_bridge_test.go`:

```go
package parquets3

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestBufferBridge_QueryLogs(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	rows := []schema.LogRow{
		{TimestampUnixNano: base.UnixNano(), Body: "hello", ServiceName: "svc"},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		enc := json.NewEncoder(w)
		for _, row := range rows {
			enc.Encode(row)
		}
	}))
	defer srv.Close()

	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
	}, config.ModeLogs)
	bridge.SetEndpoints([]string{srv.URL})

	got, err := bridge.QueryLogs(context.Background(), base.UnixNano(), base.Add(time.Hour).UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d rows, want 1", len(got))
	}
	if got[0].Body != "hello" {
		t.Errorf("Body = %q, want hello", got[0].Body)
	}
}

func TestBufferBridge_Disabled(t *testing.T) {
	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: false,
	}, config.ModeLogs)

	got, err := bridge.QueryLogs(context.Background(), 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Error("disabled bridge should return empty")
	}
}

func TestBufferBridge_NoEndpoints(t *testing.T) {
	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
	}, config.ModeLogs)

	got, err := bridge.QueryLogs(context.Background(), 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Error("no endpoints should return empty")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/storage/parquets3/ -run "TestBufferBridge" -v`
Expected: FAIL

- [ ] **Step 3: Implement buffer bridge**

In `internal/storage/parquets3/buffer_bridge.go`:

```go
package parquets3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

type BufferBridge struct {
	cfg     *config.SelectConfig
	mode    config.Mode
	client  *http.Client
	mu      sync.RWMutex
	endpoints []string
}

func NewBufferBridge(cfg *config.SelectConfig, mode config.Mode) *BufferBridge {
	return &BufferBridge{
		cfg:  cfg,
		mode: mode,
		client: &http.Client{
			Timeout: cfg.BufferQueryTimeout,
		},
	}
}

func (b *BufferBridge) SetEndpoints(endpoints []string) {
	b.mu.Lock()
	b.endpoints = endpoints
	b.mu.Unlock()
}

func (b *BufferBridge) QueryLogs(ctx context.Context, startNs, endNs int64) ([]schema.LogRow, error) {
	if !b.cfg.BufferQueryEnabled {
		return nil, nil
	}

	b.mu.RLock()
	eps := b.endpoints
	b.mu.RUnlock()

	if len(eps) == 0 {
		return nil, nil
	}

	var mu sync.Mutex
	var all []schema.LogRow
	var wg sync.WaitGroup

	for _, ep := range eps {
		wg.Add(1)
		go func(endpoint string) {
			defer wg.Done()
			rows, err := b.fetchLogs(ctx, endpoint, startNs, endNs)
			if err != nil {
				return
			}
			mu.Lock()
			all = append(all, rows...)
			mu.Unlock()
		}(ep)
	}
	wg.Wait()

	return all, nil
}

func (b *BufferBridge) fetchLogs(ctx context.Context, endpoint string, startNs, endNs int64) ([]schema.LogRow, error) {
	url := fmt.Sprintf("%s/internal/buffer/query?start=%d&end=%d&mode=%s",
		endpoint, startNs, endNs, string(b.mode))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("buffer query returned %d", resp.StatusCode)
	}

	var rows []schema.LogRow
	dec := json.NewDecoder(resp.Body)
	for dec.More() {
		var row schema.LogRow
		if err := dec.Decode(&row); err != nil {
			break
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (b *BufferBridge) QueryTraces(ctx context.Context, startNs, endNs int64) ([]schema.TraceRow, error) {
	if !b.cfg.BufferQueryEnabled {
		return nil, nil
	}

	b.mu.RLock()
	eps := b.endpoints
	b.mu.RUnlock()

	if len(eps) == 0 {
		return nil, nil
	}

	var mu sync.Mutex
	var all []schema.TraceRow
	var wg sync.WaitGroup

	for _, ep := range eps {
		wg.Add(1)
		go func(endpoint string) {
			defer wg.Done()
			url := fmt.Sprintf("%s/internal/buffer/query?start=%d&end=%d&mode=%s",
				endpoint, startNs, endNs, string(b.mode))
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			resp, err := b.client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var rows []schema.TraceRow
			dec := json.NewDecoder(resp.Body)
			for dec.More() {
				var row schema.TraceRow
				if err := dec.Decode(&row); err != nil {
					break
				}
				rows = append(rows, row)
			}
			mu.Lock()
			all = append(all, rows...)
			mu.Unlock()
		}(ep)
	}
	wg.Wait()

	return all, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /tmp/victoria-lakehouse && go test ./internal/storage/parquets3/ -run "TestBufferBridge" -v`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
cd /tmp/victoria-lakehouse
git add internal/storage/parquets3/buffer_bridge.go internal/storage/parquets3/buffer_bridge_test.go
git commit -m "feat(bridge): select-side buffer query client with parallel fan-out"
```

---

## Task 10: Integration — Wire Everything in Storage and Run Full Suite

**Files:**
- Modify: `internal/storage/parquets3/storage.go` — WAL replay on startup, buffer bridge in RunQuery
- Modify: `internal/insertapi/handler.go` — register buffer query endpoint

- [ ] **Step 1: Wire WAL replay in Storage.New() or StartWriter()**

In `storage.go`, after writer creation in `New()` or `StartWriter()`:

```go
func (s *Storage) StartWriter() {
	if s.writer == nil {
		return
	}

	logCount, traceCount := s.writer.ReplayWAL()
	if logCount > 0 || traceCount > 0 {
		s.logger.Info("WAL recovery complete", "logs", logCount, "traces", traceCount)
	}

	s.writer.Start()
}
```

- [ ] **Step 2: Wire buffer bridge in RunQuery()**

In `storage.go`, add buffer bridge field to Storage and integrate in RunQuery:

```go
// After existing Parquet query in RunQuery:
if s.bufferBridge != nil {
	switch s.mode {
	case config.ModeLogs:
		bufRows, _ := s.bufferBridge.QueryLogs(ctx, qctx.StartNs, qctx.EndNs)
		if len(bufRows) > 0 {
			db := s.rowsToDataBlock(bufRows, qctx)
			if db != nil && db.RowsCount > 0 {
				writeBlock(db)
			}
		}
	case config.ModeTraces:
		// similar for traces
	}
}
```

- [ ] **Step 3: Register buffer query endpoint in handler.go**

In `insertapi/handler.go` `Register()` method:

```go
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/insert/jsonline", h.handleJSONLine)
	mux.HandleFunc("/insert/loki/api/v1/push", h.handleLokiPush)
	mux.HandleFunc("/insert/elasticsearch/_bulk", h.handleESBulk)
	if h.bufferHandler != nil {
		mux.Handle("/internal/buffer/query", h.bufferHandler)
	}
}
```

- [ ] **Step 4: Run full test suite**

Run: `cd /tmp/victoria-lakehouse && go test ./...`
Expected: ALL PASS (770+ tests)

- [ ] **Step 5: Run build**

Run: `cd /tmp/victoria-lakehouse && go build ./...`
Expected: Success

- [ ] **Step 6: Commit**

```bash
cd /tmp/victoria-lakehouse
git add internal/storage/parquets3/storage.go internal/insertapi/handler.go
git commit -m "feat: wire WAL replay, buffer bridge in RunQuery, buffer endpoint registration"
```

---

## Task 11: Final Integration Test and PR

- [ ] **Step 1: Run full test suite with race detector**

Run: `cd /tmp/victoria-lakehouse && go test -race ./...`
Expected: ALL PASS, no races

- [ ] **Step 2: Run linter**

Run: `cd /tmp/victoria-lakehouse && golangci-lint run ./...`
Expected: No new errors

- [ ] **Step 3: Verify build**

Run: `cd /tmp/victoria-lakehouse && go build -o /dev/null ./cmd/lakehouse/`
Expected: Success

- [ ] **Step 4: Create PR**

```bash
git checkout -b feat/phase-a-write-durability
git push -u origin feat/phase-a-write-durability
gh pr create --title "feat: Phase A — WAL, adaptive sizing, buffer bridge, label pruning" --body "$(cat <<'EOF'
## Summary
- WAL crash recovery: append-only binary file, gob encoding, corrupt entry recovery
- Adaptive file sizing: target-file-size trigger alongside time/rows/memory triggers
- Buffer query bridge: /internal/buffer/query endpoint + select-side fan-out client
- FileInfo.Labels: manifest-level query pruning, populated during flush
- Config: TargetFileSize, WALMaxBytes, SelectConfig with buffer query settings

## Test plan
- [ ] WAL: append/replay round-trip, truncate, size limit backpressure, corrupt recovery
- [ ] Adaptive sizing: per-partition byte estimate triggers flush before time interval
- [ ] Buffer bridge: HTTP endpoint returns NDJSON, select-side parallel fan-out
- [ ] Labels: extracted from rows during flush, preserved in manifest save/load
- [ ] Full suite: 770+ tests, race detector clean

Spec: docs/superpowers/specs/2026-05-04-storage-parity-design.md (Phase A)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
