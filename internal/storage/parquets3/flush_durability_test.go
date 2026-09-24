package parquets3

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// faultyUploader stands in for object storage: fail decides, per key, whether
// an upload fails; block, when set, holds every upload until it is closed.
type faultyUploader struct {
	mu       sync.Mutex
	fail     func(key string) error
	block    chan struct{}
	started  chan struct{}
	uploaded map[string]int
}

func (u *faultyUploader) Upload(ctx context.Context, key string, _ []byte) error {
	if u.started != nil {
		select {
		case u.started <- struct{}{}:
		default:
		}
	}
	if u.block != nil {
		select {
		case <-u.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.fail != nil {
		if err := u.fail(key); err != nil {
			return err
		}
	}
	if u.uploaded == nil {
		u.uploaded = make(map[string]int)
	}
	u.uploaded[key]++
	return nil
}

// durabilityWriter is a logs BatchWriter whose uploads go to u.
func durabilityWriter(t *testing.T, u *faultyUploader) (*BatchWriter, *manifest.Manifest) {
	t.Helper()
	s3srv := mockS3()
	t.Cleanup(s3srv.Close)
	bw, m := testWriter(t, s3srv.URL)
	bw.cfg.MaxBufferRows = 1 << 30 // flushes happen only when the test calls FlushAll
	bw.SetTenantBucket(func(uint32, uint32) string { return "durability" })
	bw.SetTenantPool(func(string) PoolWriter { return u })
	return bw, m
}

func committedRows(m *manifest.Manifest) (rows int64, files int) {
	for _, part := range m.AllFiles() {
		for _, fi := range part {
			rows += fi.RowCount
			files++
		}
	}
	return rows, files
}

// rowsAt returns n log rows one second apart from base, of one tenant.
func rowsAt(base time.Time, n int, account uint32) []schema.LogRow {
	rows := sampleLogRows(n, base)
	for i := range rows {
		rows[i].AccountID = account
	}
	return rows
}

var errPutFailed = errors.New("PutObject: context deadline exceeded")

// A failed upload keeps its rows buffered and readable; the next flush
// writes them — once.
func TestFlush_FailedUploadKeepsItsRows(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)
	u := &faultyUploader{fail: func(string) error {
		if failing.Load() {
			return errPutFailed
		}
		return nil
	}}
	bw, m := durabilityWriter(t, u)
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	bw.AddLogRows(rowsAt(base, 50, 0))
	requeued0 := metrics.InsertRowsRequeued.Get()

	if err := bw.FlushAll(context.Background()); err == nil {
		t.Fatal("a flush whose upload failed reported success")
	}
	if rows, files := committedRows(m); rows != 0 || files != 0 {
		t.Fatalf("committed %d rows in %d files after a failed upload", rows, files)
	}
	if got := bw.BufferedRows(); got != 50 {
		t.Fatalf("buffered rows after the failed flush = %d, want 50", got)
	}
	if got := len(bw.BufferedLogRows(0, base.Add(time.Hour).UnixNano())); got != 50 {
		t.Fatalf("buffer query sees %d rows after the failed flush, want 50", got)
	}
	if d := metrics.InsertRowsRequeued.Get() - requeued0; d != 50 {
		t.Errorf("requeued rows counted = %d, want 50", d)
	}
	if bw.pendingBytes.Load() <= 0 {
		t.Error("the failed rows are not counted as pending")
	}

	failing.Store(false)
	if err := bw.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rows, files := committedRows(m); rows != 50 || files != 1 {
		t.Fatalf("after the retry: %d rows in %d files, want 50 in 1", rows, files)
	}
	if bw.BufferedRows() != 0 || len(bw.BufferedLogRows(0, base.Add(time.Hour).UnixNano())) != 0 {
		t.Error("committed rows are still buffered")
	}
	if got := bw.pendingBytes.Load(); got != 0 {
		t.Errorf("pending bytes after everything is committed = %d, want 0", got)
	}
}

// Tenants of one partition are written as separate objects: only the group
// that failed is retried, the one that was written is never written again.
func TestFlush_OnlyTheFailedTenantGroupIsRetried(t *testing.T) {
	var failB atomic.Bool
	failB.Store(true)
	u := &faultyUploader{fail: func(key string) error {
		if failB.Load() && strings.HasPrefix(key, "tenant-7/") {
			return errPutFailed
		}
		return nil
	}}
	bw, m := durabilityWriter(t, u)
	bw.SetTenantPrefix(func(account, _ uint32) string {
		if account == 7 {
			return "tenant-7/"
		}
		return "tenant-0/"
	})
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	bw.AddLogRows(append(rowsAt(base, 30, 0), rowsAt(base.Add(time.Minute), 20, 7)...))

	if err := bw.FlushAll(context.Background()); err == nil {
		t.Fatal("expected the tenant-7 upload to fail")
	}
	if rows, files := committedRows(m); rows != 30 || files != 1 {
		t.Fatalf("after the partial failure: %d rows in %d files, want tenant 0's 30 in 1", rows, files)
	}
	if got := bw.BufferedRows(); got != 20 {
		t.Fatalf("buffered = %d, want tenant 7's 20", got)
	}

	failB.Store(false)
	if err := bw.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rows, files := committedRows(m); rows != 50 || files != 2 {
		t.Fatalf("after the retry: %d rows in %d files, want 50 in 2 (no tenant written twice)", rows, files)
	}
	for key, n := range u.uploaded {
		if n != 1 {
			t.Errorf("%s uploaded %d times", key, n)
		}
	}
}

// When the flush deadline passes, the partitions it has not reached are put
// back, not dropped — the failure that lost 40 % of a seeded benchmark.
func TestFlush_PartitionsNotReachedBeforeTheDeadlineArePutBack(t *testing.T) {
	var slow atomic.Bool
	slow.Store(true)
	u := &faultyUploader{fail: func(string) error {
		if slow.Load() {
			time.Sleep(30 * time.Millisecond)
		}
		return nil
	}}
	bw, m := durabilityWriter(t, u)
	base := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	for h := 0; h < 12; h++ { // twelve partitions
		bw.AddLogRows(rowsAt(base.Add(time.Duration(h)*time.Hour), 10, 0))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err := bw.FlushAll(ctx)
	cancel()
	if err == nil {
		t.Fatal("expected the deadline to cut the flush short")
	}
	rows, _ := committedRows(m)
	if rows+bw.BufferedRows() != 120 {
		t.Fatalf("committed %d + buffered %d != 120: rows were lost", rows, bw.BufferedRows())
	}
	if rows == 120 {
		t.Fatal("the deadline did not cut the flush short; the test proves nothing")
	}

	slow.Store(false)
	if err := bw.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rows, _ := committedRows(m); rows != 120 {
		t.Fatalf("after the retry %d rows are committed, want 120", rows)
	}
}

// Rows being uploaded stay visible to buffer queries until committed, so a
// query never misses a row that is on its way to object storage.
func TestFlush_RowsStayVisibleWhileInFlight(t *testing.T) {
	u := &faultyUploader{block: make(chan struct{}), started: make(chan struct{}, 1)}
	bw, m := durabilityWriter(t, u)
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	bw.AddLogRows(rowsAt(base, 25, 0))

	done := make(chan error, 1)
	go func() { done <- bw.FlushAll(context.Background()) }()
	<-u.started

	window := base.Add(time.Hour).UnixNano()
	if got := len(bw.BufferedLogRows(0, window)); got != 25 {
		t.Fatalf("buffer query during the upload sees %d rows, want 25", got)
	}
	if rows, _ := committedRows(m); rows != 0 {
		t.Fatal("rows committed before the upload finished")
	}

	close(u.block)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := len(bw.BufferedLogRows(0, window)); got != 0 {
		t.Fatalf("buffer query after the commit sees %d rows, want 0 (they are in the manifest now)", got)
	}
	if rows, _ := committedRows(m); rows != 25 {
		t.Fatalf("committed %d rows, want 25", rows)
	}
}

// Under random upload failures and concurrent inserts, every row is
// committed exactly once in the end: nothing lost, nothing written twice.
func TestFlush_RandomFailuresUnderConcurrentInsertsLoseNothing(t *testing.T) {
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic fault schedule
	var rngMu sync.Mutex
	var faults atomic.Bool
	faults.Store(true)
	u := &faultyUploader{fail: func(string) error {
		if !faults.Load() {
			return nil
		}
		rngMu.Lock()
		defer rngMu.Unlock()
		if rng.Intn(3) == 0 {
			return errPutFailed
		}
		return nil
	}}
	bw, m := durabilityWriter(t, u)
	base := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)

	const writers, batches, perBatch = 4, 25, 8
	var wg sync.WaitGroup
	for wi := 0; wi < writers; wi++ {
		wg.Add(1)
		go func(wi int) {
			defer wg.Done()
			for b := 0; b < batches; b++ {
				at := base.Add(time.Duration((wi*batches+b)%6) * time.Hour).Add(time.Duration(b) * time.Minute)
				bw.AddLogRows(rowsAt(at, perBatch, uint32(wi%2)))
			}
		}(wi)
	}
	stop := make(chan struct{})
	flushed := make(chan struct{})
	go func() {
		defer close(flushed)
		for {
			select {
			case <-stop:
				return
			default:
				_ = bw.FlushAll(context.Background())
			}
		}
	}()
	wg.Wait()
	close(stop)
	<-flushed

	faults.Store(false)
	if err := bw.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	const want = writers * batches * perBatch
	if rows, _ := committedRows(m); rows != want {
		t.Fatalf("committed %d rows, want exactly %d", rows, want)
	}
	if bw.BufferedRows() != 0 || bw.pendingBytes.Load() != 0 {
		t.Errorf("after the final flush: %d rows / %d bytes still pending", bw.BufferedRows(), bw.pendingBytes.Load())
	}
}

// Past insert.max_buffer_bytes of unwritten rows the writer refuses inserts
// with 429, as VictoriaLogs does when it cannot take writes, instead of
// growing until the process runs out of memory; it accepts again once the
// rows are written.
func TestCanWriteData_TooMuchUnwrittenDataIs429(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)
	u := &faultyUploader{fail: func(string) error {
		if failing.Load() {
			return errPutFailed
		}
		return nil
	}}
	bw, _ := durabilityWriter(t, u)
	bw.cfg.MaxBufferBytes = "4KB"
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	bw.AddLogRows(rowsAt(base, 200, 0))
	_ = bw.FlushAll(context.Background())

	rejected0 := metrics.InsertRejected.Get("buffer_full")
	err := bw.CanWriteData(context.Background())
	var esc *httpserver.ErrorWithStatusCode
	if !errors.As(err, &esc) || esc.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("CanWriteData over the limit = %v, want a 429", err)
	}
	if metrics.InsertRejected.Get("buffer_full") <= rejected0 {
		t.Error("the rejection was not counted")
	}

	failing.Store(false)
	if err := bw.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := bw.CanWriteData(context.Background()); err != nil {
		t.Fatalf("CanWriteData after the rows were written = %v, want nil", err)
	}
}

// The write probe is not a PUT per insert request: its outcome is reused,
// and a storage that refuses writes answers 503.
func TestCanWriteData_ProbeIsReusedAndAnUnwritableStoreIs503(t *testing.T) {
	var probes atomic.Int64
	var refuse atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "_write_check") {
			probes.Add(1)
			if refuse.Load() {
				w.WriteHeader(http.StatusForbidden)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	bw, _ := testWriter(t, srv.URL)

	for i := 0; i < 50; i++ {
		if err := bw.CanWriteData(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := probes.Load(); got != 1 {
		t.Fatalf("50 CanWriteData calls issued %d write probes, want 1", got)
	}

	refuse.Store(true)
	bw.probeMu.Lock()
	bw.probeAt = time.Time{} // the reuse window has passed
	bw.probeMu.Unlock()
	err := bw.CanWriteData(context.Background())
	var esc *httpserver.ErrorWithStatusCode
	if !errors.As(err, &esc) || esc.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("CanWriteData against a refusing store = %v, want a 503", err)
	}
}

// Stop's final flush failing loses the rows it could not write (the legacy
// staging path has no WAL); they are counted, not silently gone.
func TestStop_RowsTheFinalFlushCannotWriteAreCounted(t *testing.T) {
	u := &faultyUploader{fail: func(string) error { return errPutFailed }}
	bw, _ := durabilityWriter(t, u)
	bw.cfg.FlushInterval = time.Hour
	bw.Start()
	bw.AddLogRows(rowsAt(time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC), 12, 0))
	lost0 := metrics.InsertRowsLostAtShutdown.Get()
	bw.Stop()
	if d := metrics.InsertRowsLostAtShutdown.Get() - lost0; d != 12 {
		t.Fatalf("rows counted as lost at shutdown = %d, want 12", d)
	}
}

