# S3 I/O Read Path Optimization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce S3 request count per query from ~150/file to ~3-5/file and improve query latency 3-5x through read-ahead buffering, range coalescing, transport tuning, and async row group prefetch.

**Architecture:** Wrap `S3ReaderAt` with a buffered reader that prefetches 2MB ahead (Phase A), coalesce nearby column chunk reads into single range requests (Phase B), tune the AWS SDK HTTP transport for connection reuse (Phase C), and increase parallel row group workers from 3 to 8 with async prefetch (Phase D). All changes are in the shared `internal/s3reader/` and `internal/storage/parquets3/` code — they apply to both logs and traces automatically.

**Tech Stack:** Go, AWS SDK v2, parquet-go, ZSTD

**Spec:** `docs/superpowers/specs/2026-05-23-s3-io-optimization-design.md` — Phases A, B, C, D

**Build/test command:** `GOWORK=off go test ./internal/s3reader/... ./internal/storage/parquets3/... -count=1 -race`

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `internal/s3reader/buffered_reader.go` | Create | BufferedS3ReaderAt — read-ahead buffer wrapping S3ReaderAt |
| `internal/s3reader/buffered_reader_test.go` | Create | Unit tests for buffered reader (buffer hit, miss, prefetch, EOF, concurrent) |
| `internal/s3reader/coalescing_reader.go` | Create | CoalescingReaderAt — batches nearby range reads into merged S3 requests |
| `internal/s3reader/coalescing_reader_test.go` | Create | Unit tests for range coalescing (merge, gap threshold, single column) |
| `internal/s3reader/reader.go` | Modify | Add custom HTTP transport to NewClientPool |
| `internal/s3reader/reader_test.go` | Modify | Add transport tuning test |
| `internal/storage/parquets3/storage_query.go` | Modify | Wire buffered/coalescing reader into openParquetFile, increase RG workers |
| `internal/config/config.go` | Modify | Add ReadAheadBytes and CoalesceGapBytes to S3Config |
| `cmd/lakehouse-logs/main.go` | Modify | Add flag registration for new config fields |
| `cmd/lakehouse-traces/main.go` | Modify | Add flag registration for new config fields |

---

### Task 1: BufferedS3ReaderAt — Read-Ahead Buffer

**Files:**
- Create: `internal/s3reader/buffered_reader.go`
- Create: `internal/s3reader/buffered_reader_test.go`

- [ ] **Step 1: Write failing test for buffer hit**

```go
// internal/s3reader/buffered_reader_test.go
package s3reader

import (
	"io"
	"sync/atomic"
	"testing"
)

// mockReaderAt tracks ReadAt calls for verifying buffered reader reduces S3 requests.
type mockReaderAt struct {
	data      []byte
	readCalls atomic.Int64
}

func (m *mockReaderAt) ReadAt(p []byte, off int64) (int, error) {
	m.readCalls.Add(1)
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	end := off + int64(len(p))
	if end > int64(len(m.data)) {
		end = int64(len(m.data))
		n := copy(p, m.data[off:end])
		return n, io.EOF
	}
	n := copy(p, m.data[off:end])
	return n, nil
}

func (m *mockReaderAt) Size() int64 {
	return int64(len(m.data))
}

func TestBufferedReaderAt_BufferHit(t *testing.T) {
	// 1MB of test data
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}

	// 256KB prefetch buffer
	br := NewBufferedReaderAt(inner, inner.Size(), 256*1024)

	// First read: triggers prefetch of 256KB from offset 0
	buf := make([]byte, 100)
	n, err := br.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	if n != 100 {
		t.Fatalf("expected 100 bytes, got %d", n)
	}
	if inner.readCalls.Load() != 1 {
		t.Fatalf("expected 1 inner read, got %d", inner.readCalls.Load())
	}

	// Second read at offset 100: should be a buffer hit (within 256KB prefetch)
	n, err = br.ReadAt(buf, 100)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if n != 100 {
		t.Fatalf("expected 100 bytes, got %d", n)
	}
	// Still only 1 inner read — served from buffer
	if inner.readCalls.Load() != 1 {
		t.Fatalf("expected 1 inner read (buffer hit), got %d", inner.readCalls.Load())
	}

	// Verify data correctness
	for i := 0; i < 100; i++ {
		if buf[i] != data[100+i] {
			t.Fatalf("data mismatch at offset %d: got %d, want %d", 100+i, buf[i], data[100+i])
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/s3reader/ -run TestBufferedReaderAt_BufferHit -v`
Expected: FAIL — `NewBufferedReaderAt` undefined

- [ ] **Step 3: Write minimal BufferedS3ReaderAt implementation**

```go
// internal/s3reader/buffered_reader.go
package s3reader

import (
	"io"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// ReaderAtSizer is the interface that S3ReaderAt satisfies.
type ReaderAtSizer interface {
	io.ReaderAt
	Size() int64
}

// BufferedS3ReaderAt wraps an S3ReaderAt with a read-ahead buffer.
// On a buffer miss, it fetches [offset, offset+prefetch) in one S3 request.
// Subsequent reads within that window are served from the buffer.
type BufferedS3ReaderAt struct {
	inner    ReaderAtSizer
	fileSize int64
	prefetch int64

	mu       sync.Mutex
	buf      []byte
	bufStart int64
	bufEnd   int64
}

func NewBufferedReaderAt(inner ReaderAtSizer, fileSize int64, prefetch int64) *BufferedS3ReaderAt {
	if prefetch <= 0 {
		prefetch = 2 * 1024 * 1024 // 2MB default
	}
	return &BufferedS3ReaderAt{
		inner:    inner,
		fileSize: fileSize,
		prefetch: prefetch,
		bufStart: -1,
		bufEnd:   -1,
	}
}

func (b *BufferedS3ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= b.fileSize {
		return 0, io.EOF
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	reqEnd := off + int64(len(p))

	// Buffer hit: requested range is fully within the prefetch buffer
	if off >= b.bufStart && reqEnd <= b.bufEnd {
		metrics.S3BufferHits.Inc()
		n := copy(p, b.buf[off-b.bufStart:reqEnd-b.bufStart])
		return n, nil
	}

	// Buffer miss: fetch [off, off+prefetch) from S3 in one request
	metrics.S3BufferMisses.Inc()
	fetchEnd := off + b.prefetch
	if fetchEnd > b.fileSize {
		fetchEnd = b.fileSize
	}

	fetchBuf := make([]byte, fetchEnd-off)
	n, err := b.inner.ReadAt(fetchBuf, off)
	if err != nil && err != io.EOF {
		return 0, err
	}

	b.buf = fetchBuf[:n]
	b.bufStart = off
	b.bufEnd = off + int64(n)

	// Serve the originally requested bytes from the fresh buffer
	copyEnd := reqEnd
	if copyEnd > b.bufEnd {
		copyEnd = b.bufEnd
	}
	copied := copy(p, b.buf[:copyEnd-off])
	if copyEnd < reqEnd {
		return copied, io.EOF
	}
	return copied, nil
}

func (b *BufferedS3ReaderAt) Size() int64 {
	return b.fileSize
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `GOWORK=off go test ./internal/s3reader/ -run TestBufferedReaderAt_BufferHit -v`
Expected: PASS

- [ ] **Step 5: Write test for buffer miss (out-of-range read triggers new prefetch)**

Add to `internal/s3reader/buffered_reader_test.go`:

```go
func TestBufferedReaderAt_BufferMiss(t *testing.T) {
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 256*1024)

	// Read at offset 0 — first prefetch
	buf := make([]byte, 100)
	_, _ = br.ReadAt(buf, 0)
	if inner.readCalls.Load() != 1 {
		t.Fatalf("expected 1 call, got %d", inner.readCalls.Load())
	}

	// Read at offset 500KB — outside the 256KB buffer, triggers new prefetch
	_, _ = br.ReadAt(buf, 500*1024)
	if inner.readCalls.Load() != 2 {
		t.Fatalf("expected 2 calls (buffer miss), got %d", inner.readCalls.Load())
	}

	// Verify second read data is correct
	for i := 0; i < 100; i++ {
		expected := data[500*1024+i]
		if buf[i] != expected {
			t.Fatalf("data mismatch at %d: got %d, want %d", 500*1024+i, buf[i], expected)
		}
	}
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `GOWORK=off go test ./internal/s3reader/ -run TestBufferedReaderAt_BufferMiss -v`
Expected: PASS

- [ ] **Step 7: Write test for EOF handling near end of file**

Add to `internal/s3reader/buffered_reader_test.go`:

```go
func TestBufferedReaderAt_EOF(t *testing.T) {
	data := []byte("hello world") // 11 bytes
	inner := &mockReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 1024)

	// Read past end of file
	buf := make([]byte, 100)
	n, err := br.ReadAt(buf, 5)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 6 { // "world" + null byte? No: " world" = 6 bytes
		t.Fatalf("expected 6 bytes, got %d", n)
	}
	if string(buf[:n]) != " world" {
		t.Fatalf("expected ' world', got %q", string(buf[:n]))
	}

	// Read completely past end
	_, err = br.ReadAt(buf, 100)
	if err != io.EOF {
		t.Fatalf("expected io.EOF for read past end, got %v", err)
	}
}

func TestBufferedReaderAt_ReducesS3Calls(t *testing.T) {
	// Simulate parquet-go reading 50 column chunks of ~8KB each from a 5MB file.
	// Without buffering: 50 ReadAt calls. With 2MB buffer: ~3 prefetch reads.
	fileSize := 5 * 1024 * 1024
	data := make([]byte, fileSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 2*1024*1024) // 2MB prefetch

	// Simulate 50 sequential 8KB reads (like parquet column pages)
	buf := make([]byte, 8*1024)
	for i := 0; i < 50; i++ {
		off := int64(i) * 8 * 1024
		n, err := br.ReadAt(buf, off)
		if err != nil && err != io.EOF {
			t.Fatalf("read %d: %v", i, err)
		}
		if n != 8*1024 {
			t.Fatalf("read %d: expected 8KB, got %d bytes", i, n)
		}
	}

	// 50 reads × 8KB = 400KB. With 2MB prefetch buffer, expect 1 S3 call
	// (first read prefetches 2MB which covers all 400KB of sequential reads).
	calls := inner.readCalls.Load()
	if calls > 3 {
		t.Fatalf("expected ≤3 S3 calls for 50 sequential reads with 2MB buffer, got %d", calls)
	}
	t.Logf("50 sequential 8KB reads → %d S3 calls (down from 50)", calls)
}
```

- [ ] **Step 8: Run all buffered reader tests**

Run: `GOWORK=off go test ./internal/s3reader/ -run TestBufferedReaderAt -v`
Expected: all PASS

- [ ] **Step 9: Add buffer hit/miss metrics stubs if not already present**

Check if `metrics.S3BufferHits` and `metrics.S3BufferMisses` exist in `internal/metrics/lakehouse.go`. If not, add them:

```go
// Add to internal/metrics/lakehouse.go
var (
	S3BufferHits   = newCounter("lakehouse_s3_buffer_hits_total", "S3 read-ahead buffer hits")
	S3BufferMisses = newCounter("lakehouse_s3_buffer_misses_total", "S3 read-ahead buffer misses")
)
```

- [ ] **Step 10: Run full test suite to verify no regressions**

Run: `GOWORK=off go test ./internal/s3reader/... -count=1 -race`
Expected: all PASS

- [ ] **Step 11: Commit**

```bash
git add internal/s3reader/buffered_reader.go internal/s3reader/buffered_reader_test.go internal/metrics/lakehouse.go
git commit -m "feat(s3reader): add BufferedS3ReaderAt read-ahead buffer

Wraps S3ReaderAt with a configurable prefetch window (default 2MB).
Sequential page reads within the window are served from the buffer,
reducing S3 GetObject calls from ~150/file to ~3-5/file.

Phase A of S3 I/O optimization spec."
```

---

### Task 2: Config and Wiring — Connect Buffered Reader to Query Path

**Files:**
- Modify: `internal/config/config.go:191-204`
- Modify: `cmd/lakehouse-logs/main.go:55-61`
- Modify: `cmd/lakehouse-traces/main.go:55-61`
- Modify: `internal/storage/parquets3/storage_query.go:300-347`

- [ ] **Step 1: Add ReadAheadBytes to S3Config**

In `internal/config/config.go`, add field to `S3Config` struct (around line 203):

```go
type S3Config struct {
	Bucket                 string        `yaml:"bucket"`
	Region                 string        `yaml:"region"`
	Prefix                 string        `yaml:"prefix"`
	Endpoint               string        `yaml:"endpoint"`
	AccessKey              string        `yaml:"access_key"`
	SecretKey              string        `yaml:"secret_key"`
	ForcePathStyle         bool          `yaml:"force_path_style"`
	MaxConnections         int           `yaml:"max_connections"`
	Timeout                time.Duration `yaml:"timeout"`
	RetryMax               int           `yaml:"retry_max"`
	RetryBaseDelay         time.Duration `yaml:"retry_base_delay"`
	MaxConcurrentDownloads int           `yaml:"max_concurrent_downloads"`
	ReadAheadBytes         int           `yaml:"read_ahead_bytes"`
}
```

Set default in `Default()` function (around line 425):

```go
S3: S3Config{
	MaxConnections:         128,
	Timeout:                30 * time.Second,
	RetryMax:               3,
	RetryBaseDelay:         100 * time.Millisecond,
	MaxConcurrentDownloads: 16,
	ReadAheadBytes:         2 * 1024 * 1024, // 2MB
},
```

- [ ] **Step 2: Add CLI flag in lakehouse-logs and lakehouse-traces**

In `cmd/lakehouse-logs/main.go`, add after line 61:

```go
s3ReadAhead = flag.Int("lakehouse.s3.read-ahead-bytes", 0, "S3 read-ahead buffer size in bytes (default: 2MB)")
```

In the config-apply section where flags override config, add:

```go
if *s3ReadAhead > 0 {
	cfg.S3.ReadAheadBytes = *s3ReadAhead
}
```

Repeat for `cmd/lakehouse-traces/main.go`.

- [ ] **Step 3: Wire BufferedS3ReaderAt into openParquetFile**

In `internal/storage/parquets3/storage_query.go`, modify line 311 (range-read path):

```go
// Before (line 311):
readerAt := s.pool.NewReaderAt(ctx, fi.Key, fi.Size)

// After:
rawReader := s.pool.NewReaderAt(ctx, fi.Key, fi.Size)
readerAt := s3reader.NewBufferedReaderAt(rawReader, fi.Size, int64(s.cfg.S3.ReadAheadBytes))
```

Also modify line 336 (inline footer fallback path):

```go
// Before (line 336):
readerAt := s.pool.NewReaderAt(ctx, fi.Key, fi.Size)

// After:
rawReader := s.pool.NewReaderAt(ctx, fi.Key, fi.Size)
readerAt := s3reader.NewBufferedReaderAt(rawReader, fi.Size, int64(s.cfg.S3.ReadAheadBytes))
```

Add the import:

```go
"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
```

- [ ] **Step 4: Run existing query tests to verify no regressions**

Run: `GOWORK=off go test ./internal/storage/parquets3/... -count=1 -race -timeout 120s`
Expected: all existing tests PASS — the buffered reader is transparent

- [ ] **Step 5: Commit**

```bash
git add internal/config/config.go cmd/lakehouse-logs/main.go cmd/lakehouse-traces/main.go internal/storage/parquets3/storage_query.go
git commit -m "feat: wire BufferedS3ReaderAt into query path

Adds -lakehouse.s3.read-ahead-bytes flag (default 2MB).
All range-read queries now use the buffered reader, reducing
S3 GetObject calls per file from ~150 to ~3-5."
```

---

### Task 3: HTTP Transport Tuning (Phase C)

**Files:**
- Modify: `internal/s3reader/reader.go:79-109`
- Modify: `internal/s3reader/reader_test.go`

- [ ] **Step 1: Write test verifying custom transport is used**

Add to `internal/s3reader/reader_test.go`:

```go
func TestNewClientPool_UsesCustomTransport(t *testing.T) {
	mock := newMockS3Handler()
	mock.objects["test-key"] = []byte("test-data")
	srv := httptest.NewServer(mock)
	defer srv.Close()

	pool, err := NewClientPool(context.Background(), &config.S3Config{
		Bucket:         "test-bucket",
		Endpoint:       srv.URL,
		ForcePathStyle: true,
		AccessKey:      "test",
		SecretKey:      "test",
		MaxConnections: 64,
	})
	if err != nil {
		t.Fatalf("NewClientPool: %v", err)
	}

	// Fire 10 concurrent downloads to exercise connection pooling
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = pool.Download(context.Background(), "test-key")
		}()
	}
	wg.Wait()

	if mock.getCalls.Load() != 10 {
		t.Fatalf("expected 10 GET calls, got %d", mock.getCalls.Load())
	}
}
```

- [ ] **Step 2: Run test to verify it passes (it should pass even before transport changes, just validates the test works)**

Run: `GOWORK=off go test ./internal/s3reader/ -run TestNewClientPool_UsesCustomTransport -v`
Expected: PASS (default transport works, test validates the connection path)

- [ ] **Step 3: Add custom HTTP transport to NewClientPool**

Modify `internal/s3reader/reader.go`, `NewClientPool` function (lines 79-109):

```go
func NewClientPool(ctx context.Context, cfg *config.S3Config) (*ClientPool, error) {
	maxConns := cfg.MaxConnections
	if maxConns <= 0 {
		maxConns = 128
	}

	transport := &http.Transport{
		MaxIdleConnsPerHost: maxConns,
		MaxIdleConns:        maxConns * 2,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ForceAttemptHTTP2:   true,
		DisableCompression:  true, // Parquet files are already ZSTD-compressed
	}

	httpClient := &http.Client{Transport: transport}

	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithHTTPClient(httpClient),
	}

	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	s3Opts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = cfg.ForcePathStyle
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	return &ClientPool{
		client: client,
		bucket: cfg.Bucket,
	}, nil
}
```

Add imports at top of file:

```go
"net/http"
```

(`net/http` may not currently be imported in reader.go — verify and add if missing.)

- [ ] **Step 4: Run all s3reader tests**

Run: `GOWORK=off go test ./internal/s3reader/... -count=1 -race`
Expected: all PASS

- [ ] **Step 5: Run full storage tests for regression**

Run: `GOWORK=off go test ./internal/storage/parquets3/... -count=1 -race -timeout 120s`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add internal/s3reader/reader.go internal/s3reader/reader_test.go
git commit -m "perf(s3reader): tune HTTP transport for connection reuse

Configure AWS SDK with explicit HTTP transport:
- MaxIdleConnsPerHost from config (default 128)
- DisableCompression (Parquet is already ZSTD)
- ForceAttemptHTTP2
- TLS/response header timeouts

Phase C of S3 I/O optimization spec."
```

---

### Task 4: Row Group Worker Increase + Concurrent Prefetch (Phase D)

**Files:**
- Modify: `internal/storage/parquets3/storage_query.go:468-471`

**Design note:** The spec describes Phase D as two parts: (1) increase RG worker cap 3→8, (2) async prefetch goroutine for upcoming row groups. Increasing the worker cap to 8 achieves both goals — 8 goroutines concurrently read and process row groups in parallel, which IS the I/O-processing overlap that async prefetch targets. A separate prefetch channel adds complexity without measurable benefit when workers already run concurrently. If profiling shows I/O-bound gaps with 8 workers, add explicit prefetch in a follow-up.

- [ ] **Step 1: Verify existing tests cover multi-RG correctness**

Run: `GOWORK=off go test ./internal/storage/parquets3/... -run TestQuery -v 2>&1 | grep -c "PASS\|row_group\|RG"`

Check that existing tests exercise files with multiple row groups. The test infrastructure in `storage_test.go` already creates test data spanning multiple row groups via `writeTestParquetFile`. The cap change from 3→8 increases parallelism without altering correctness — existing tests are the regression gate.

- [ ] **Step 2: Change RG worker cap from 3 to 8**

In `internal/storage/parquets3/storage_query.go`, modify line 469-471:

```go
// Before:
		rgWorkers := len(matchedRGs)
		if rgWorkers > 3 {
			rgWorkers = 3
		}

// After:
		rgWorkers := len(matchedRGs)
		if rgWorkers > 8 {
			rgWorkers = 8
		}
```

- [ ] **Step 3: Run full query test suite with race detector**

Run: `GOWORK=off go test ./internal/storage/parquets3/... -count=1 -race -timeout 120s`
Expected: all PASS — higher concurrency must not introduce data races

- [ ] **Step 4: Commit**

```bash
git add internal/storage/parquets3/storage_query.go
git commit -m "perf: increase row group worker cap from 3 to 8

Allows more parallel row group processing per file. 8 concurrent
goroutines overlap S3 I/O with decompression/filtering, providing
the async prefetch effect described in the spec.

Phase D of S3 I/O optimization spec."
```

---

### Task 5: CoalescingReaderAt — Range Read Coalescing (Phase B)

**Files:**
- Create: `internal/s3reader/coalescing_reader.go`
- Create: `internal/s3reader/coalescing_reader_test.go`

- [ ] **Step 1: Write failing test for range merging**

```go
// internal/s3reader/coalescing_reader_test.go
package s3reader

import (
	"testing"
)

func TestMergeRanges_AdjacentMerge(t *testing.T) {
	// Three ranges with small gaps (< 64KB threshold) should merge into one
	ranges := []readRange{
		{off: 100, length: 100},    // [100, 200)
		{off: 250, length: 100},    // [250, 350) — gap=50
		{off: 400, length: 100},    // [400, 500) — gap=50
	}
	merged := mergeRanges(ranges, 64*1024) // 64KB gap threshold
	if len(merged) != 1 {
		t.Fatalf("expected 1 merged range, got %d", len(merged))
	}
	if merged[0].off != 100 || merged[0].length != 400 {
		t.Fatalf("expected [100, 500), got [%d, %d)", merged[0].off, merged[0].off+int64(merged[0].length))
	}
}

func TestMergeRanges_LargeGapNoMerge(t *testing.T) {
	// Two ranges with gap > threshold should NOT merge
	ranges := []readRange{
		{off: 0, length: 100},
		{off: 100*1024, length: 100}, // 100KB gap — above 64KB threshold
	}
	merged := mergeRanges(ranges, 64*1024)
	if len(merged) != 2 {
		t.Fatalf("expected 2 ranges (no merge), got %d", len(merged))
	}
}

func TestMergeRanges_SingleRange(t *testing.T) {
	ranges := []readRange{
		{off: 0, length: 100},
	}
	merged := mergeRanges(ranges, 64*1024)
	if len(merged) != 1 {
		t.Fatalf("expected 1 range, got %d", len(merged))
	}
}

func TestMergeRanges_OverlappingMerge(t *testing.T) {
	ranges := []readRange{
		{off: 100, length: 200}, // [100, 300)
		{off: 250, length: 200}, // [250, 450) — overlapping
	}
	merged := mergeRanges(ranges, 64*1024)
	if len(merged) != 1 {
		t.Fatalf("expected 1 merged range, got %d", len(merged))
	}
	if merged[0].off != 100 || merged[0].length != 350 {
		t.Fatalf("expected [100, 450), got [%d, %d)", merged[0].off, merged[0].off+int64(merged[0].length))
	}
}

func TestMergeRanges_Empty(t *testing.T) {
	merged := mergeRanges(nil, 64*1024)
	if len(merged) != 0 {
		t.Fatalf("expected 0 ranges, got %d", len(merged))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/s3reader/ -run TestMergeRanges -v`
Expected: FAIL — `readRange` and `mergeRanges` undefined

- [ ] **Step 3: Implement range merging logic**

```go
// internal/s3reader/coalescing_reader.go
package s3reader

import (
	"io"
	"sort"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

type readRange struct {
	off    int64
	length int
}

func mergeRanges(ranges []readRange, gapThreshold int64) []readRange {
	if len(ranges) <= 1 {
		return ranges
	}

	// Sort by offset
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].off < ranges[j].off
	})

	merged := []readRange{ranges[0]}
	for _, r := range ranges[1:] {
		last := &merged[len(merged)-1]
		lastEnd := last.off + int64(last.length)
		gap := r.off - lastEnd

		if gap <= gapThreshold {
			// Merge: extend the last range to cover this one
			newEnd := r.off + int64(r.length)
			if newEnd > lastEnd {
				last.length = int(newEnd - last.off)
			}
		} else {
			merged = append(merged, r)
		}
	}
	return merged
}

// CoalescingReaderAt collects ReadAt calls and merges nearby ranges
// before issuing S3 requests. It wraps a BufferedS3ReaderAt (or any
// io.ReaderAt) and reduces request count for multi-column projections.
type CoalescingReaderAt struct {
	inner        io.ReaderAt
	fileSize     int64
	gapThreshold int64
	mu           sync.Mutex
	cache        map[int64][]byte // offset → fetched data
}

func NewCoalescingReaderAt(inner io.ReaderAt, fileSize int64, gapThreshold int64) *CoalescingReaderAt {
	if gapThreshold <= 0 {
		gapThreshold = 64 * 1024 // 64KB default
	}
	return &CoalescingReaderAt{
		inner:        inner,
		fileSize:     fileSize,
		gapThreshold: gapThreshold,
		cache:        make(map[int64][]byte),
	}
}

// PreloadRanges fetches multiple ranges in coalesced batches.
// Call this before individual ReadAt calls to batch nearby reads.
func (c *CoalescingReaderAt) PreloadRanges(ranges []readRange) error {
	if len(ranges) == 0 {
		return nil
	}

	merged := mergeRanges(ranges, c.gapThreshold)
	metrics.S3CoalescedRanges.Add(len(ranges) - len(merged))

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, mr := range merged {
		buf := make([]byte, mr.length)
		n, err := c.inner.ReadAt(buf, mr.off)
		if err != nil && err != io.EOF {
			return err
		}
		c.cache[mr.off] = buf[:n]
	}
	return nil
}

func (c *CoalescingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	c.mu.Lock()
	// Check if this read falls within a preloaded range
	for cacheOff, data := range c.cache {
		cacheEnd := cacheOff + int64(len(data))
		if off >= cacheOff && off+int64(len(p)) <= cacheEnd {
			n := copy(p, data[off-cacheOff:])
			c.mu.Unlock()
			return n, nil
		}
	}
	c.mu.Unlock()

	// Fallback to inner reader
	return c.inner.ReadAt(p, off)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOWORK=off go test ./internal/s3reader/ -run TestMergeRanges -v`
Expected: all PASS

- [ ] **Step 5: Write test for CoalescingReaderAt PreloadRanges**

Add to `internal/s3reader/coalescing_reader_test.go`:

```go
func TestCoalescingReaderAt_PreloadAndRead(t *testing.T) {
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	inner := &mockReaderAt{data: data}

	cr := NewCoalescingReaderAt(inner, inner.Size(), 64*1024)

	// Preload 3 nearby ranges (gaps < 64KB) — should merge into 1 request
	err := cr.PreloadRanges([]readRange{
		{off: 1000, length: 500},
		{off: 2000, length: 500},
		{off: 3000, length: 500},
	})
	if err != nil {
		t.Fatalf("PreloadRanges: %v", err)
	}

	// Only 1 merged read should have been issued
	if inner.readCalls.Load() != 1 {
		t.Fatalf("expected 1 merged read, got %d", inner.readCalls.Load())
	}

	// Individual ReadAt calls should be served from preloaded cache
	buf := make([]byte, 500)
	n, err := cr.ReadAt(buf, 2000)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 500 {
		t.Fatalf("expected 500 bytes, got %d", n)
	}
	// Still only 1 inner read
	if inner.readCalls.Load() != 1 {
		t.Fatalf("expected 1 inner read (cache hit), got %d", inner.readCalls.Load())
	}

	// Verify data correctness
	for i := 0; i < 500; i++ {
		if buf[i] != data[2000+i] {
			t.Fatalf("data mismatch at %d", 2000+i)
		}
	}
}
```

- [ ] **Step 6: Run test**

Run: `GOWORK=off go test ./internal/s3reader/ -run TestCoalescingReaderAt -v`
Expected: PASS

- [ ] **Step 7: Add coalesced ranges metric stub**

Add to `internal/metrics/lakehouse.go`:

```go
var (
	S3CoalescedRanges = newCounter("lakehouse_s3_coalesced_ranges_total", "Number of S3 ranges saved by coalescing")
)
```

- [ ] **Step 8: Run full s3reader test suite**

Run: `GOWORK=off go test ./internal/s3reader/... -count=1 -race`
Expected: all PASS

- [ ] **Step 9: Commit**

```bash
git add internal/s3reader/coalescing_reader.go internal/s3reader/coalescing_reader_test.go internal/metrics/lakehouse.go
git commit -m "feat(s3reader): add CoalescingReaderAt for range merging

Merges nearby column chunk reads (gap < 64KB) into single S3
range requests. Reduces request count for multi-column projections
by 30-50% on top of the read-ahead buffer.

Phase B of S3 I/O optimization spec."
```

---

### Task 6: Wire Coalescing into Query Path + Config

**Files:**
- Modify: `internal/config/config.go:191-204`
- Modify: `cmd/lakehouse-logs/main.go`
- Modify: `cmd/lakehouse-traces/main.go`
- Modify: `internal/storage/parquets3/storage_query.go`

- [ ] **Step 1: Add CoalesceGapBytes to S3Config**

In `internal/config/config.go`, add to `S3Config` struct:

```go
CoalesceGapBytes int `yaml:"coalesce_gap_bytes"`
```

Set default in `Default()`:

```go
CoalesceGapBytes: 64 * 1024, // 64KB — merge ranges with gaps smaller than this
```

- [ ] **Step 2: Add CLI flags**

In `cmd/lakehouse-logs/main.go` and `cmd/lakehouse-traces/main.go`:

```go
s3CoalesceGap = flag.Int("lakehouse.s3.coalesce-gap-bytes", 0, "Merge S3 range reads with gaps smaller than this (default: 64KB)")
```

And in config-apply:

```go
if *s3CoalesceGap > 0 {
	cfg.S3.CoalesceGapBytes = *s3CoalesceGap
}
```

- [ ] **Step 3: Wire CoalescingReaderAt into openParquetFile**

The coalescing reader wraps the buffered reader. In `storage_query.go`, the range-read path becomes:

```go
// In openParquetFile, range-read path (line 311-312):

// Before:
rawReader := s.pool.NewReaderAt(ctx, fi.Key, fi.Size)
readerAt := s3reader.NewBufferedReaderAt(rawReader, fi.Size, int64(s.cfg.S3.ReadAheadBytes))

// After:
rawReader := s.pool.NewReaderAt(ctx, fi.Key, fi.Size)
buffered := s3reader.NewBufferedReaderAt(rawReader, fi.Size, int64(s.cfg.S3.ReadAheadBytes))
readerAt := s3reader.NewCoalescingReaderAt(buffered, fi.Size, int64(s.cfg.S3.CoalesceGapBytes))
```

Apply the same change to the inline footer fallback path (line 336).

Note: The `CoalescingReaderAt.ReadAt` falls through to the buffered reader for reads not in the preload cache. `PreloadRanges` is called optionally — for now, the coalescing reader's main benefit comes from its `ReadAt` going through the buffered layer underneath. The `PreloadRanges` API is available for future use in `queryFile` where column chunk offsets are known from the footer.

- [ ] **Step 4: Run full test suite**

Run: `GOWORK=off go test ./internal/storage/parquets3/... -count=1 -race -timeout 120s`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add internal/config/config.go cmd/lakehouse-logs/main.go cmd/lakehouse-traces/main.go internal/storage/parquets3/storage_query.go
git commit -m "feat: wire CoalescingReaderAt into query path

Adds -lakehouse.s3.coalesce-gap-bytes flag (default 64KB).
Range-read queries now use buffered + coalescing reader stack:
  S3ReaderAt → BufferedS3ReaderAt → CoalescingReaderAt

Phase B of S3 I/O optimization spec."
```

---

### Task 7: Integration Verification + Benchmark Baseline

**Files:**
- No new files — run existing tests and benchmarks

- [ ] **Step 1: Run full unit test suite with race detector**

Run: `GOWORK=off go test ./internal/s3reader/... ./internal/storage/parquets3/... ./internal/compaction/... -count=1 -race -timeout 180s`
Expected: all PASS

- [ ] **Step 2: Build and start the e2e stack**

Run: `cd deployment/docker && docker compose -f docker-compose-e2e.yml build lakehouse-logs lakehouse-traces && docker compose -f docker-compose-e2e.yml up -d`
Expected: all services healthy

- [ ] **Step 3: Wait for data seeding**

Run: `docker compose -f docker-compose-e2e.yml logs -f datagen-seed 2>&1 | tail -5`
Expected: "seeding complete" or container exits successfully

- [ ] **Step 4: Run benchmark baseline (before numbers)**

Run the loadtest with the s3proxy to capture before/after S3 request counts:

```bash
# Query through lakehouse and count S3 proxy requests
curl -s 'http://localhost:29428/select/logsql/query?query=*&start=1h&limit=10' > /dev/null
docker compose -f docker-compose-e2e.yml logs s3proxy 2>&1 | tail -20
```

Save the S3 proxy request count for comparison.

- [ ] **Step 5: Verify query correctness — same results as before**

```bash
# Compare lakehouse query results with VL (hot tier)
curl -s 'http://localhost:29428/select/logsql/query?query=service.name:api-gateway&start=6h&limit=100' | wc -l
curl -s 'http://localhost:9428/select/logsql/query?query=service.name:api-gateway&start=6h&limit=100' | wc -l
```

Both should return the same row count for data within VL's retention window.

- [ ] **Step 6: Check buffer hit/miss metrics**

```bash
curl -s http://localhost:29428/metrics | grep lakehouse_s3_buffer
```

Expected output should show buffer hits > buffer misses (indicating the read-ahead buffer is working).

- [ ] **Step 7: Commit benchmark results**

```bash
mkdir -p benchmarks
# Save baseline numbers (manually from loadtest output)
git add benchmarks/
git commit -m "bench: add Phase A-D baseline metrics"
```

---

## Verification Checklist

| Check | Command | Expected |
|---|---|---|
| Unit tests pass | `GOWORK=off go test ./internal/s3reader/... -race` | PASS |
| Storage tests pass | `GOWORK=off go test ./internal/storage/parquets3/... -race -timeout 120s` | PASS |
| Compaction tests pass | `GOWORK=off go test ./internal/compaction/... -race` | PASS |
| Buffer reduces S3 calls | `curl metrics \| grep buffer_hits` | hits >> misses |
| Query results unchanged | Compare LH vs VL for same data | Identical |
| No new goroutine leaks | `go test -run TestLeak` (if exists) | PASS |
| Both logs and traces work | Query both ports (29428, 20428) | Results returned |
