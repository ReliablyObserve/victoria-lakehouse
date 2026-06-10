package s3reader

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// recordingReaderAt records every (off, len) passed to ReadAt so tests can
// assert the exact underlying GET pattern.
type recordingReaderAt struct {
	data  []byte
	mu    sync.Mutex
	reads []readRange
}

func (r *recordingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	r.reads = append(r.reads, readRange{off: off, length: len(p)})
	r.mu.Unlock()
	m := &mockReaderAt{data: r.data}
	return m.ReadAt(p, off)
}

func (r *recordingReaderAt) Size() int64 { return int64(len(r.data)) }

func (r *recordingReaderAt) recorded() []readRange {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]readRange, len(r.reads))
	copy(out, r.reads)
	return out
}

// TestBufferedReaderAt_AdaptiveWindowGrowth: 2+ consecutive forward-sequential
// misses double the window up to the configured max; a random seek resets it
// to the base.
func TestBufferedReaderAt_AdaptiveWindowGrowth(t *testing.T) {
	growsBefore := metrics.S3ReadAheadGrows.Get()
	resetsBefore := metrics.S3ReadAheadResets.Get()

	data := make([]byte, 64*1024)
	inner := &recordingReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 1024, 4096)

	if got := br.Window(); got != 1024 {
		t.Fatalf("initial window = %d, want 1024", got)
	}

	buf := make([]byte, 1024)
	// Miss 1: cold fetch at 0 (no eviction, no classification).
	mustRead(t, br, buf, 0)
	// Miss 2: forward-sequential (off == bufEnd) — seqMisses=1, no growth yet.
	mustRead(t, br, buf, 1024)
	if got := br.Window(); got != 1024 {
		t.Fatalf("window after 1 sequential miss = %d, want 1024", got)
	}
	// Miss 3: forward-sequential — seqMisses=2 → window doubles to 2048.
	mustRead(t, br, buf, 2048)
	if got := br.Window(); got != 2048 {
		t.Fatalf("window after 2 sequential misses = %d, want 2048", got)
	}
	// Offset 3072 is inside the grown window [2048, 4096) — buffer hit.
	mustRead(t, br, buf, 3072)
	// Miss 4: forward-sequential off the grown window — doubles to 4096 (max).
	mustRead(t, br, buf, 4096)
	if got := br.Window(); got != 4096 {
		t.Fatalf("window after 3 sequential misses = %d, want 4096 (max)", got)
	}
	// Further sequential misses must NOT exceed the max.
	mustRead(t, br, buf, 8192)
	mustRead(t, br, buf, 12288)
	if got := br.Window(); got != 4096 {
		t.Fatalf("window exceeded max: %d, want 4096", got)
	}

	if grows := metrics.S3ReadAheadGrows.Get() - growsBefore; grows != 2 {
		t.Errorf("S3ReadAheadGrows delta = %d, want 2", grows)
	}

	// Random seek backwards: window resets to base.
	mustRead(t, br, buf, 0)
	if got := br.Window(); got != 1024 {
		t.Fatalf("window after random seek = %d, want base 1024", got)
	}
	if resets := metrics.S3ReadAheadResets.Get() - resetsBefore; resets != 1 {
		t.Errorf("S3ReadAheadResets delta = %d, want 1", resets)
	}
}

// TestBufferedReaderAt_MaxClampedToBase: maxPrefetch below the base never
// shrinks the configured window.
func TestBufferedReaderAt_MaxClampedToBase(t *testing.T) {
	inner := &mockReaderAt{data: make([]byte, 1024)}
	br := NewBufferedReaderAt(inner, inner.Size(), 4096, 1024)
	if br.maxWindow != 4096 {
		t.Fatalf("maxWindow = %d, want clamped to base 4096", br.maxWindow)
	}
}

// TestBufferedReaderAt_HeadBypass: a tiny read at offset 0 (parquet magic
// probe) is served by an exact-size GET, not a full window fetch.
func TestBufferedReaderAt_HeadBypass(t *testing.T) {
	bypassBefore := metrics.S3HeadBypassReads.Get()

	data := make([]byte, 32*1024)
	copy(data, []byte("PAR1"))
	inner := &recordingReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 8*1024, 8*1024)

	magic := make([]byte, 4)
	mustRead(t, br, magic, 0)
	if !bytes.Equal(magic, []byte("PAR1")) {
		t.Fatalf("magic = %q, want PAR1", magic)
	}

	reads := inner.recorded()
	if len(reads) != 1 || reads[0].off != 0 || reads[0].length != 4 {
		t.Fatalf("expected one exact 4-byte GET at offset 0, got %+v", reads)
	}
	if d := metrics.S3HeadBypassReads.Get() - bypassBefore; d != 1 {
		t.Errorf("S3HeadBypassReads delta = %d, want 1", d)
	}

	// A larger read at offset 0 takes the normal window path...
	big := make([]byte, 100)
	mustRead(t, br, big, 0)
	reads = inner.recorded()
	if len(reads) != 2 || reads[1].length != 8*1024 {
		t.Fatalf("expected a window fetch (8KB), got %+v", reads)
	}
	// ...after which a tiny read at 0 is a buffer hit, not a bypass GET.
	mustRead(t, br, magic, 0)
	if got := len(inner.recorded()); got != 2 {
		t.Fatalf("tiny read after window fill issued a GET; reads = %d, want 2", got)
	}
}

// TestBufferedReaderAt_WastedBytes: bytes fetched into a window but never
// served are counted on eviction.
func TestBufferedReaderAt_WastedBytes(t *testing.T) {
	wasteBefore := metrics.S3BufferWastedBytes.Get()

	data := make([]byte, 32*1024)
	inner := &recordingReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 1024, 1024)

	// Fill a 1KB window at 0, serve only the first 100 bytes.
	buf := make([]byte, 100)
	mustRead(t, br, buf, 0)
	// Random jump evicts the window → 1024-100 = 924 wasted bytes.
	mustRead(t, br, buf, 16*1024)

	if d := metrics.S3BufferWastedBytes.Get() - wasteBefore; d != 924 {
		t.Errorf("S3BufferWastedBytes delta = %d, want 924", d)
	}
}

func mustRead(t *testing.T, br *BufferedS3ReaderAt, p []byte, off int64) {
	t.Helper()
	if _, err := br.ReadAt(p, off); err != nil {
		t.Fatalf("ReadAt(%d) failed: %v", off, err)
	}
}

// TestMergeRangesWithOverfetch verifies the over-fetch accounting: only gap
// bytes between merged ranges count; overlaps and unmerged ranges do not.
func TestMergeRangesWithOverfetch(t *testing.T) {
	cases := []struct {
		name      string
		ranges    []readRange
		gap       int64
		wantLen   int
		wantBytes int64
	}{
		{
			name:      "gap merged",
			ranges:    []readRange{{off: 0, length: 100}, {off: 200, length: 100}},
			gap:       128,
			wantLen:   1,
			wantBytes: 100,
		},
		{
			name:      "overlap no overfetch",
			ranges:    []readRange{{off: 0, length: 100}, {off: 50, length: 100}},
			gap:       128,
			wantLen:   1,
			wantBytes: 0,
		},
		{
			name:      "beyond gap unmerged",
			ranges:    []readRange{{off: 0, length: 100}, {off: 1000, length: 100}},
			gap:       128,
			wantLen:   2,
			wantBytes: 0,
		},
		{
			name: "chain of gaps",
			ranges: []readRange{
				{off: 0, length: 10}, {off: 20, length: 10}, {off: 40, length: 10},
			},
			gap:       16,
			wantLen:   1,
			wantBytes: 20,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			merged, overfetch := mergeRangesWithOverfetch(tc.ranges, tc.gap)
			if len(merged) != tc.wantLen {
				t.Errorf("merged len = %d, want %d", len(merged), tc.wantLen)
			}
			if overfetch != tc.wantBytes {
				t.Errorf("overfetch = %d, want %d", overfetch, tc.wantBytes)
			}
		})
	}
}

// TestPhaseReaderAt verifies per-phase GET attribution and the open-phase
// GET count used by the per-open histogram.
func TestPhaseReaderAt(t *testing.T) {
	openBefore := metrics.S3GetsByPhase.Get("open")
	pageBefore := metrics.S3GetsByPhase.Get("page")

	inner := &mockReaderAt{data: make([]byte, 1024)}
	pr := NewPhaseReaderAt(inner)

	buf := make([]byte, 10)
	_, _ = pr.ReadAt(buf, 0)
	_, _ = pr.ReadAt(buf, 100)
	if got := pr.OpenGets(); got != 2 {
		t.Fatalf("OpenGets = %d, want 2", got)
	}

	pr.SetPhase(PhasePage)
	_, _ = pr.ReadAt(buf, 200)
	if got := pr.OpenGets(); got != 2 {
		t.Fatalf("OpenGets after page phase = %d, want 2", got)
	}

	if d := metrics.S3GetsByPhase.Get("open") - openBefore; d != 2 {
		t.Errorf("open-phase GETs delta = %d, want 2", d)
	}
	if d := metrics.S3GetsByPhase.Get("page") - pageBefore; d != 1 {
		t.Errorf("page-phase GETs delta = %d, want 1", d)
	}
}

// slowS3Handler is a minimal S3 GET handler that delays every response so
// concurrent callers genuinely overlap, and counts the GETs it serves.
type slowS3Handler struct {
	data     []byte
	delay    time.Duration
	getCalls atomic.Int64
}

func (h *slowS3Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	h.getCalls.Add(1)
	time.Sleep(h.delay)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(h.data)
}

// TestDownloadDedup_Singleflight: N concurrent DownloadDedup calls for the
// same key share ONE underlying GET; every caller gets the full payload.
func TestDownloadDedup_Singleflight(t *testing.T) {
	dedupBefore := metrics.S3MetaSingleflightDedup.Get("bloom")

	payload := bytes.Repeat([]byte("x"), 4096)
	handler := &slowS3Handler{data: payload, delay: 150 * time.Millisecond}
	ts := httptest.NewServer(handler)
	defer ts.Close()

	pool := newDedupTestPool(t, ts.URL)

	const callers = 8
	var wg sync.WaitGroup
	errs := make([]error, callers)
	results := make([][]byte, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = pool.DownloadDedup(context.Background(), "bloom", "part/file.parquet.bloom")
		}(i)
	}
	wg.Wait()

	for i := 0; i < callers; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d failed: %v", i, errs[i])
		}
		if !bytes.Equal(results[i], payload) {
			t.Fatalf("caller %d got %d bytes, want %d", i, len(results[i]), len(payload))
		}
	}
	if got := handler.getCalls.Load(); got != 1 {
		t.Errorf("server saw %d GETs, want 1 (singleflight dedup)", got)
	}
	if d := metrics.S3MetaSingleflightDedup.Get("bloom") - dedupBefore; d == 0 {
		t.Errorf("S3MetaSingleflightDedup did not move; want > 0")
	}
}

// TestDownloadRangeDedup_DistinctRanges: different ranges of the same key do
// NOT share a flight (the key includes offset+length).
func TestDownloadRangeDedup_DistinctRanges(t *testing.T) {
	payload := bytes.Repeat([]byte("y"), 4096)
	handler := &slowS3Handler{data: payload, delay: 50 * time.Millisecond}
	ts := httptest.NewServer(handler)
	defer ts.Close()

	pool := newDedupTestPool(t, ts.URL)

	var wg sync.WaitGroup
	for _, off := range []int64{0, 1024} {
		wg.Add(1)
		go func(off int64) {
			defer wg.Done()
			_, _ = pool.DownloadRangeDedup(context.Background(), "footer", "part/file.parquet", off, 512)
		}(off)
	}
	wg.Wait()

	if got := handler.getCalls.Load(); got != 2 {
		t.Errorf("server saw %d GETs, want 2 (distinct ranges must not dedup)", got)
	}
}

// TestDownloadDedup_WaiterCancellation: a waiter whose context is cancelled
// unblocks with ctx.Err() while the flight completes for the initiator.
func TestDownloadDedup_WaiterCancellation(t *testing.T) {
	payload := []byte("payload")
	handler := &slowS3Handler{data: payload, delay: 300 * time.Millisecond}
	ts := httptest.NewServer(handler)
	defer ts.Close()

	pool := newDedupTestPool(t, ts.URL)

	var wg sync.WaitGroup
	wg.Add(1)
	var initiatorErr error
	go func() {
		defer wg.Done()
		_, initiatorErr = pool.DownloadDedup(context.Background(), "bloom", "k")
	}()

	// Give the initiator a head start so the flight is in progress.
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pool.DownloadDedup(ctx, "bloom", "k"); err == nil {
		t.Fatal("cancelled waiter returned nil error, want context.Canceled")
	}

	wg.Wait()
	if initiatorErr != nil {
		t.Fatalf("initiator failed: %v", initiatorErr)
	}
}

func newDedupTestPool(t *testing.T, endpoint string) *ClientPool {
	t.Helper()
	cfg := &config.S3Config{
		Bucket:         "test-bucket",
		Region:         "us-east-1",
		Endpoint:       endpoint,
		AccessKey:      "k",
		SecretKey:      "s",
		ForcePathStyle: true,
	}
	pool, err := NewClientPool(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewClientPool: %v", err)
	}
	return pool
}
