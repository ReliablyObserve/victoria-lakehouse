package parquets3

// A row read answers completely or fails (issue #503). Before the fix a read
// answered 200 with rows missing when (a) the read of an object it selected
// failed with anything but a 404 (an S3 error, a corrupt footer, a truncated
// body), (b) its caller cancelled it or its deadline passed while the last object
// was being read, or (c) the per-query live-bytes budget cancelled it. A listed
// object that a compaction or a retention run removed (404) stays tolerated.
//
// Mirrored in lakehouse-traces/internal/storage/parquets3/row_read_complete_test.go.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	smithy "github.com/aws/smithy-go"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// faultS3 is the mock S3 with a per-object fault: a request for an object with a
// fault is answered by the fault, every other request by the mock.
type faultS3 struct {
	mock *mockS3Server
	srv  *httptest.Server

	mu     sync.Mutex
	faults map[string]func(w http.ResponseWriter, r *http.Request)
}

func newFaultS3(t testing.TB) *faultS3 {
	t.Helper()
	f := &faultS3{mock: newMockS3Server(), faults: map[string]func(http.ResponseWriter, *http.Request){}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		var fault func(http.ResponseWriter, *http.Request)
		for key, fn := range f.faults {
			if strings.HasSuffix(r.URL.Path, "/"+key) {
				fault = fn
			}
		}
		f.mu.Unlock()
		if fault != nil {
			fault(w, r)
			return
		}
		f.mock.handler(w, r)
	}))
	t.Cleanup(func() {
		f.srv.Close()
		f.mock.close()
	})
	return f
}

func (f *faultS3) setFault(key string, fn func(http.ResponseWriter, *http.Request)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[key] = fn
}

func (f *faultS3) clearFault(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.faults, key)
}

// accessDenied is an S3 error that is not a missing object and not retried. Its
// request id spells "404", as about 0.3 % of the 16-hex-digit ids of a real store
// do: the message of the error must not be taken for a missing object.
func accessDenied(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("x-amz-request-id", "7A404C0FFEE00000")
	w.Header().Set("x-amz-id-2", "dGVzdC1ob3N0LWlkLzQwNC8=")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusForbidden)
	_, _ = fmt.Fprint(w, `<?xml version="1.0"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`)
}

// truncatedBody announces a long body and closes the connection after a few bytes.
func truncatedBody(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Length", "1000000")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("PAR1"))
	if hj, ok := w.(http.Hijacker); ok {
		if conn, _, err := hj.Hijack(); err == nil {
			_ = conn.Close()
		}
	}
}

// hang never answers until the client gives up.
func hang(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }

// pagesEnd is the offset where the data pages of a Parquet file end: the first
// byte of its page index (or, without one, of its footer).
func pagesEnd(t testing.TB, data []byte) int {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	end := int64(footerStart(data))
	for _, rg := range f.Metadata().RowGroups {
		for _, c := range rg.Columns {
			if c.ColumnIndexOffset > 0 && c.ColumnIndexOffset < end {
				end = c.ColumnIndexOffset
			}
		}
	}
	return int(end)
}

func footerStart(data []byte) int {
	return len(data) - 8 - int(binary.LittleEndian.Uint32(data[len(data)-8:]))
}

// corruptPages returns data with the first 48 bytes of the column chunks of the
// row groups from rowGroup on overwritten (those of the named columns, or of all
// when none is named), so the footer and the page index still parse and a read of
// the column fails.
func corruptPages(t testing.TB, data []byte, rowGroup int, columns ...string) []byte {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	out := append([]byte(nil), data...)
	for i, rg := range f.Metadata().RowGroups {
		if i < rowGroup {
			continue
		}
		for _, c := range rg.Columns {
			if len(columns) > 0 && !slices.Contains(columns, strings.Join(c.MetaData.PathInSchema, ".")) {
				continue
			}
			off := c.MetaData.DataPageOffset
			if d := c.MetaData.DictionaryPageOffset; d > 0 && d < off {
				off = d
			}
			for j := int(off); j < int(off)+48 && j < len(out); j++ {
				out[j] = byte(0xA5 ^ j)
			}
		}
	}
	return out
}

// corruptPageIndex returns data with its page index overwritten.
func corruptPageIndex(t testing.TB, data []byte) []byte {
	t.Helper()
	out := append([]byte(nil), data...)
	for i := pagesEnd(t, data); i < footerStart(data); i++ {
		out[i] = byte(0xA5 ^ i)
	}
	return out
}

const (
	rcRowsPerFile = 300 // > one 256-row read buffer: every file emits several blocks
	rcFiles       = 3
)

// rcLogRow is logRow plus the stream columns, so the stream surfaces have a
// column to read.
type rcLogRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
	Body              string `parquet:"body"`
	SeverityText      string `parquet:"severity_text"`
	ServiceName       string `parquet:"service.name"`
	Stream            string `parquet:"_stream"`
	StreamID          string `parquet:"_stream_id"`
}

// rcBody is the body of row j of object i: "padding" repeated (compressible, and the
// word the filtered queries look for) or random hex.
func rcBody(i, j int, incompressible bool, rng *rand.Rand) string {
	if incompressible {
		return fmt.Sprintf("file=%d row=%d padding %016x%016x%016x%016x", i, j, rng.Uint64(), rng.Uint64(), rng.Uint64(), rng.Uint64())
	}
	return fmt.Sprintf("file=%d row=%d %s", i, j, strings.Repeat("padding ", 24))
}

type rcFixture struct {
	fs    *faultS3
	s     *Storage
	keys  []string
	datas [][]byte
	now   time.Time
	start int64
	end   int64
}

// newRCFixture registers rcFiles objects of rcRowsPerFile rows each.
func newRCFixture(t *testing.T, workers int, keyFmt string) *rcFixture {
	return newRCFixtureN(t, workers, rcFiles, keyFmt)
}

// newRCFixtureN registers n objects.
func newRCFixtureN(t *testing.T, workers, n int, keyFmt string) *rcFixture {
	return newRCFixtureShape(t, workers, n, keyFmt, false)
}

// newRCFixtureShape registers n objects whose bodies are compressible (a repeated
// padding) or, with incompressible, random hex: stored size ~ decoded size.
func newRCFixtureShape(t *testing.T, workers, n int, keyFmt string, incompressible bool) *rcFixture {
	t.Helper()
	rng := rand.New(rand.NewSource(11))
	fs := newFaultS3(t)
	s := testStorageWithS3(t, fs.srv.URL)
	s.cfg.Query.FileWorkers = workers
	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	fx := &rcFixture{fs: fs, s: s, now: now, start: now.Add(-time.Hour).UnixNano(), end: now.Add(time.Hour).UnixNano()}
	for i := 0; i < n; i++ {
		rows := make([]rcLogRow, rcRowsPerFile)
		for j := range rows {
			svc := fmt.Sprintf("svc-%d", i)
			rows[j] = rcLogRow{
				TimestampUnixNano: now.Add(time.Duration(i*rcRowsPerFile+j) * time.Millisecond).UnixNano(),
				Body:              rcBody(i, j, incompressible, rng),
				SeverityText:      "INFO",
				ServiceName:       svc,
				Stream:            fmt.Sprintf(`{service.name=%q}`, svc),
				StreamID:          fmt.Sprintf("%048x", i),
			}
		}
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[rcLogRow](&buf, parquet.Compression(&parquet.Zstd), parquet.MaxRowsPerRowGroup(100))
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		data := buf.Bytes()
		key := fmt.Sprintf(keyFmt, i)
		registerFileInMockS3(t, s, fs.mock, key, data, now)
		fx.keys = append(fx.keys, key)
		fx.datas = append(fx.datas, data)
	}
	return fx
}

func (fx *rcFixture) query(t *testing.T, ctx context.Context, qs string, onBlock func(*logstorage.DataBlock)) (int, error) {
	t.Helper()
	q := mustParseQueryWithTime(t, qs, fx.start, fx.end)
	var rows atomic.Int64
	err := fx.s.RunQuery(ctx, nil, q, func(_ uint, db *logstorage.DataBlock) {
		rows.Add(int64(db.RowsCount()))
		if onBlock != nil {
			onBlock(db)
		}
	})
	return int(rows.Load()), err
}

const rcKeyFmt = "logs/dt=2026-05-10/hour=14/rc-%d.parquet"

// A control: with no fault the read returns every row.
func TestRowRead_NoFault_ReturnsEveryRow(t *testing.T) {
	fx := newRCFixture(t, 4, rcKeyFmt)
	waits := metrics.QueryMemoryBudgetExceeded.Get()
	rows, err := fx.query(t, context.Background(), "*", nil)
	if got := metrics.QueryMemoryBudgetExceeded.Get(); got != waits {
		t.Fatalf("the budget made a worker wait under the default ceiling (%d waits): small objects must not be held back", got-waits)
	}
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	if want := rcFiles * rcRowsPerFile; rows != want {
		t.Fatalf("rows = %d, want %d", rows, want)
	}
}

// (a) An object that cannot be read fails the read, whatever its position among
// the objects and however many workers read them.
func TestRowRead_UnreadableObject_FailsInsteadOfPartial(t *testing.T) {
	if testing.Short() {
		t.Skip("fault matrix: runs in the heavy job")
	}
	faults := map[string]func(t *testing.T, fx *rcFixture, idx int){
		"s3_access_denied": func(t *testing.T, fx *rcFixture, idx int) { fx.fs.setFault(fx.keys[idx], accessDenied) },
		"truncated_body":   func(t *testing.T, fx *rcFixture, idx int) { fx.fs.setFault(fx.keys[idx], truncatedBody) },
		"corrupt_footer": func(t *testing.T, fx *rcFixture, idx int) {
			fx.fs.mock.putFile(fx.keys[idx], bytes.Repeat([]byte{0xFF}, len(fx.datas[idx])))
		},
		"corrupt_pages": func(t *testing.T, fx *rcFixture, idx int) {
			fx.fs.mock.putFile(fx.keys[idx], corruptPages(t, fx.datas[idx], 0))
		},
		"corrupt_page_index": func(t *testing.T, fx *rcFixture, idx int) {
			fx.fs.mock.putFile(fx.keys[idx], corruptPageIndex(t, fx.datas[idx]))
		},
	}
	for name, inject := range faults {
		for _, workers := range []int{1, 4} {
			for _, idx := range []int{0, rcFiles - 1} { // the first and the last object read with 1 worker
				t.Run(fmt.Sprintf("%s/workers=%d/object=%d", name, workers, idx), func(t *testing.T) {
					if name == "truncated_body" && testing.Short() {
						t.Skip("the SDK retries a truncated body with back-off")
					}
					fx := newRCFixture(t, workers, rcKeyFmt)
					inject(t, fx, idx)
					before := metrics.QueryFileErrorsTotal.Get()
					rows, err := fx.query(t, context.Background(), "*", nil)
					if err == nil {
						t.Fatalf("RunQuery answered without error and with %d of %d rows: a read that could not read an object must fail", rows, rcFiles*rcRowsPerFile)
					}
					if !strings.Contains(err.Error(), fx.keys[idx]) {
						t.Fatalf("error %q does not name the unreadable object %s", err, fx.keys[idx])
					}
					if isObjectGone(err, fx.keys[idx]) {
						t.Fatalf("the failure %q is classified as a missing object", err)
					}
					if got := metrics.QueryFileErrorsTotal.Get(); got <= before {
						t.Fatalf("lakehouse_query_file_errors_total did not count the failure (%d -> %d)", before, got)
					}
				})
			}
		}
	}
}

// A listed object that was removed meanwhile (compaction, retention) is
// tolerated: the read answers with the rows of the other objects, as before.
func TestRowRead_RemovedObject_StaysTolerated(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			fx := newRCFixture(t, workers, rcKeyFmt)
			fx.fs.mock.mu.Lock()
			delete(fx.fs.mock.files, fx.keys[1])
			fx.fs.mock.mu.Unlock()
			before := metrics.QueryFileNotFoundTotal.Get()
			rows, err := fx.query(t, context.Background(), "*", nil)
			if err != nil {
				t.Fatalf("RunQuery: %v, a removed object must stay tolerated", err)
			}
			if want := (rcFiles - 1) * rcRowsPerFile; rows != want {
				t.Fatalf("rows = %d, want the %d of the objects still there", rows, want)
			}
			if got := metrics.QueryFileNotFoundTotal.Get(); got <= before {
				t.Fatalf("the removed object was not counted as not found (%d -> %d)", before, got)
			}
		})
	}
}

// KNOWN GAP (#772): a source object a compaction deleted while the query ran is
// skipped silently, though its rows are in the merged object. The compactor
// publishes the merge and deletes its sources at once, and a query lists once, so
// the query answers without those rows and without an error. This test pins that
// behaviour so that the gap stays visible; when #772 is fixed it must be turned
// into the assertion that every row arrives.
func TestRowRead_RemovedSourceMidQuery_KnownGap772(t *testing.T) {
	fx := newRCFixture(t, 1, rcKeyFmt) // one worker: the objects are read one after the other
	source := fx.keys[1]
	// The merged object holds the source's rows; the query's listing does not have it.
	fx.fs.mock.putFile("logs/dt=2026-05-10/hour=14/compacted-merge.parquet", fx.datas[1])
	fx.fs.setFault(source, func(w http.ResponseWriter, r *http.Request) {
		// The compactor deletes the source just before the query reads it.
		fx.fs.mock.mu.Lock()
		delete(fx.fs.mock.files, source)
		fx.fs.mock.mu.Unlock()
		fx.fs.clearFault(source)
		fx.fs.mock.handler(w, r)
	})
	rows, err := fx.query(t, context.Background(), "*", nil)
	if err != nil {
		t.Fatalf("RunQuery: %v; the gap pinned here is a silent skip", err)
	}
	if want := (rcFiles - 1) * rcRowsPerFile; rows != want {
		t.Fatalf("rows = %d, want %d (the rows of the deleted source are missing: known gap #772)", rows, want)
	}
}

// A key that spells "404" must not turn an unreadable object into a missing one.
func TestRowRead_KeyContaining404_IsNotAMissingObject(t *testing.T) {
	key := "logs/dt=2026-05-10/hour=14/part-404.parquet"
	err := fmt.Errorf("s3 GetObject %s: AccessDenied", key)
	if !isFileNotFoundError(err) {
		t.Fatal("the premise changed: the substring match no longer trips on a key containing 404")
	}
	if isObjectGone(err, key) {
		t.Fatalf("%q is classified as a missing object because its key contains 404", err)
	}
	if !isObjectGone(fmt.Errorf("s3 GetObject %s: NoSuchKey: The specified key does not exist", key), key) {
		t.Fatal("a NoSuchKey error is no longer recognised as a missing object")
	}

	fx := newRCFixture(t, 2, "logs/dt=2026-05-10/hour=14/part-404-%d.parquet")
	fx.fs.setFault(fx.keys[0], accessDenied)
	if rows, err := fx.query(t, context.Background(), "*", nil); err == nil {
		t.Fatalf("RunQuery answered %d rows without error for an unreadable object whose key contains 404", rows)
	}
}

// The failure of a read keeps the S3 error in its chain, and the S3 error decides:
// a 403 whose request id spells "404" is not a missing object, a NoSuchKey is.
func TestRowRead_ClassifiedByTheS3Error_NotByItsText(t *testing.T) {
	fx := newRCFixture(t, 2, rcKeyFmt)
	fx.fs.setFault(fx.keys[1], accessDenied)
	_, err := fx.query(t, context.Background(), "*", nil)
	if err == nil {
		t.Fatal("RunQuery answered without error for an unreadable object")
	}
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "AccessDenied" {
		t.Fatalf("the S3 error is lost on its way to the caller (a layer formats it with %%v): %v", err)
	}
	if !strings.Contains(err.Error(), "404") {
		t.Fatalf("the request id with 404 is not in the error text, the test no longer tests the text match: %v", err)
	}
	if !isFileNotFoundError(err) {
		t.Fatal("the premise changed: the substring match no longer trips on a request id containing 404")
	}
	if isObjectGone(err, fx.keys[1]) {
		t.Fatalf("a 403 is classified as a missing object because its request id contains 404: %v", err)
	}

	fx2 := newRCFixture(t, 2, rcKeyFmt)
	fx2.fs.mock.mu.Lock()
	delete(fx2.fs.mock.files, fx2.keys[1])
	fx2.fs.mock.mu.Unlock()
	if rows, err := fx2.query(t, context.Background(), "*", nil); err != nil || rows != (rcFiles-1)*rcRowsPerFile {
		t.Fatalf("a NoSuchKey must stay tolerated: %d rows, err %v", rows, err)
	}
}

// (b) The caller's cancellation or deadline during the last object's read is not
// a complete answer.
func TestRowRead_CallerStopsDuringLastObject_Fails(t *testing.T) {
	if testing.Short() {
		t.Skip("sleeps on a hung object: runs in the heavy job")
	}
	t.Run("deadline", func(t *testing.T) {
		fx := newRCFixture(t, 1, rcKeyFmt) // one worker: the last object is read last
		fx.fs.setFault(fx.keys[rcFiles-1], hang)
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer cancel()
		rows, err := fx.query(t, ctx, "*", nil)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("RunQuery = %v after %d rows, want the deadline error", err, rows)
		}
		// The caller's own deadline is not an unreadable object: no 502.
		var esc *httpserver.ErrorWithStatusCode
		if errors.As(err, &esc) {
			t.Fatalf("a deadline was wrapped as status %d: %v", esc.StatusCode, err)
		}
	})
	t.Run("cancel", func(t *testing.T) {
		fx := newRCFixture(t, 1, rcKeyFmt)
		fx.fs.setFault(fx.keys[rcFiles-1], hang)
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(400*time.Millisecond, cancel)
		rows, err := fx.query(t, ctx, "*", nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunQuery = %v after %d rows, want the cancellation error", err, rows)
		}
		var esc *httpserver.ErrorWithStatusCode
		if errors.As(err, &esc) {
			t.Fatalf("a cancellation was wrapped as status %d: %v", esc.StatusCode, err)
		}
	})
}

// (c) The live-bytes budget makes a worker wait for the consumer; it never
// cancels the read or drops a block, also while the last objects are read.
func TestRowRead_LiveBytesBudget_WaitsNeverDrops(t *testing.T) {
	for _, workers := range []int{2, 3} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			fx := newRCFixture(t, workers, rcKeyFmt)
			fx.s.cfg.Query.MaxLiveBytes = 1 // one object at a time: a second worker must wait

			start := metrics.QueryMemoryBudgetExceeded.Get()
			var first sync.Once
			rows, err := fx.query(t, context.Background(), "*", func(db *logstorage.DataBlock) {
				// Hold the first block until another worker has reached the ceiling
				// behind it: it cannot reserve while this object's bytes are held, so
				// the wait is certain, not a matter of timing.
				first.Do(func() {
					deadline := time.Now().Add(10 * time.Second)
					for metrics.QueryMemoryBudgetExceeded.Get() <= start && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
				})
			})
			if err != nil {
				t.Fatalf("RunQuery: %v, the budget must make workers wait, not fail the read", err)
			}
			if want := rcFiles * rcRowsPerFile; rows != want {
				t.Fatalf("rows = %d, want %d: the budget dropped blocks", rows, want)
			}
			if metrics.QueryMemoryBudgetExceeded.Get() <= start {
				t.Fatal("no worker waited at the budget: the test does not exercise it")
			}
		})
	}
}

// The ceiling stays a ceiling: a read that reaches max_rows ends without a new
// kind of failure (its behaviour is unchanged by this fix).
func TestRowRead_RowCeiling_Unchanged(t *testing.T) {
	fx := newRCFixture(t, 1, rcKeyFmt)
	fx.s.cfg.Query.MaxRows = 100
	rows, err := fx.query(t, context.Background(), "*", nil)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("RunQuery: %v", err)
	}
	if rows >= rcFiles*rcRowsPerFile {
		t.Fatalf("rows = %d, the ceiling did not stop the read", rows)
	}
}

// A failed read leaves no state behind: the same reader answers completely once
// the fault is gone, and two readers over one bucket fail and recover alike.
func TestRowRead_MultiReader_FailAndRecover(t *testing.T) {
	fx := newRCFixture(t, 4, rcKeyFmt)
	second := testStorageWithS3(t, fx.fs.srv.URL)
	second.cfg.Query.FileWorkers = 4
	second.manifest = fx.s.manifest // the same listing, as a second reader pod would see
	readers := []*Storage{fx.s, second}

	run := func(s *Storage) (int, error) {
		q := mustParseQueryWithTime(t, "*", fx.start, fx.end)
		var rows atomic.Int64
		err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) { rows.Add(int64(db.RowsCount())) })
		return int(rows.Load()), err
	}
	fx.fs.setFault(fx.keys[1], accessDenied)
	var wg sync.WaitGroup
	errs := make([]error, len(readers))
	for i, r := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = run(r)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err == nil {
			t.Fatalf("reader %d answered without error while an object was unreadable", i)
		}
	}
	fx.fs.clearFault(fx.keys[1])
	for i, r := range readers {
		rows, err := run(r)
		if err != nil || rows != rcFiles*rcRowsPerFile {
			t.Fatalf("reader %d after the fault is gone: %d rows, err %v; want %d rows", i, rows, err, rcFiles*rcRowsPerFile)
		}
	}
}

// Every read surface that scans objects fails on an unreadable object, and
// tolerates a removed one.
func TestRowRead_EverySurface_FailsOnUnreadableObject(t *testing.T) {
	if testing.Short() {
		t.Skip("surface matrix: runs in the heavy job")
	}
	surfaces := map[string]func(fx *rcFixture) error{
		"field_names": func(fx *rcFixture) error {
			_, err := fx.s.GetFieldNames(context.Background(), nil, mustParseQueryWithTime(t, "*", fx.start, fx.end))
			return err
		},
		"field_values": func(fx *rcFixture) error {
			_, err := fx.s.GetFieldValues(context.Background(), nil, mustParseQueryWithTime(t, "*", fx.start, fx.end), "service.name", 100)
			return err
		},
		"streams": func(fx *rcFixture) error {
			_, err := fx.s.GetStreams(context.Background(), nil, mustParseQueryWithTime(t, "*", fx.start, fx.end), 100)
			return err
		},
		"stream_ids": func(fx *rcFixture) error {
			_, err := fx.s.GetStreamIDs(context.Background(), nil, mustParseQueryWithTime(t, "*", fx.start, fx.end), 100)
			return err
		},
		"stream_field_names": func(fx *rcFixture) error {
			_, err := fx.s.GetStreamFieldNames(context.Background(), nil, mustParseQueryWithTime(t, "*", fx.start, fx.end))
			return err
		},
		"stream_field_values": func(fx *rcFixture) error {
			_, err := fx.s.GetStreamFieldValues(context.Background(), nil, mustParseQueryWithTime(t, "*", fx.start, fx.end), "service.name", 100)
			return err
		},
		"hits_stats": func(fx *rcFixture) error {
			// hits and stats read the rows through RunQuery with a timestamp-only projection.
			_, err := fx.query(t, context.Background(), "* | stats count()", nil)
			return err
		},
		"filtered_rows": func(fx *rcFixture) error {
			_, err := fx.query(t, context.Background(), "padding", nil)
			return err
		},
	}
	for name, surface := range surfaces {
		t.Run(name+"/unreadable", func(t *testing.T) {
			for _, fault := range []string{"access_denied", "corrupt_pages", "corrupt_page_index"} {
				if fault == "corrupt_pages" && name == "hits_stats" {
					// A count over an unfiltered window is answered from the
					// row-group metadata of the footer: no data page is read, so a
					// damaged page cannot change the (exact) answer.
					continue
				}
				fx := newRCFixture(t, 4, rcKeyFmt)
				switch fault {
				case "access_denied":
					fx.fs.setFault(fx.keys[1], accessDenied)
				case "corrupt_pages":
					fx.fs.mock.putFile(fx.keys[1], corruptPages(t, fx.datas[1], 0))
				case "corrupt_page_index":
					fx.fs.mock.putFile(fx.keys[1], corruptPageIndex(t, fx.datas[1]))
				}
				if err := surface(fx); err == nil {
					t.Fatalf("%s answered without error with an unreadable object (%s)", name, fault)
				}
			}
		})
		t.Run(name+"/removed", func(t *testing.T) {
			fx := newRCFixture(t, 4, rcKeyFmt)
			fx.fs.mock.mu.Lock()
			delete(fx.fs.mock.files, fx.keys[1])
			fx.fs.mock.mu.Unlock()
			if err := surface(fx); err != nil {
				t.Fatalf("%s: %v, a removed object must stay tolerated", name, err)
			}
		})
	}
}

// A compacted object (many row groups, much wider than a flushed one) that fails
// after some of its row groups were emitted still fails the read.
func TestRowRead_CompactedObject_FailsMidFile(t *testing.T) {
	fs := newFaultS3(t)
	s := testStorageWithS3(t, fs.srv.URL)
	s.cfg.Query.FileWorkers = 2
	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := make([]logRow, 900)
	for i := range rows {
		rows[i] = logRow{TimestampUnixNano: now.Add(time.Duration(i) * time.Millisecond).UnixNano(), Body: fmt.Sprintf("row=%d", i), SeverityText: "INFO", ServiceName: "svc"}
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[logRow](&buf, parquet.Compression(&parquet.Zstd), parquet.MaxRowsPerRowGroup(100))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	key := "logs/dt=2026-05-10/hour=14/compacted-L2-000.parquet"
	registerFileInMockS3(t, s, fs.mock, key, data, now)
	// Damage only the second half of the pages: the first row groups decode and
	// are emitted, a later one fails.
	fs.mock.putFile(key, corruptPages(t, data, 5))

	q := mustParseQueryWithTime(t, "*", now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	var got atomic.Int64
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) { got.Add(int64(db.RowsCount())) })
	if err == nil {
		t.Fatalf("RunQuery answered %d of %d rows without error from a damaged compacted object", got.Load(), len(rows))
	}
}

// Under real pressure (a 256 KiB ceiling, sixteen workers, a slow consumer) the
// budget bounds the heap by making workers wait, and the answer is complete.
// Before the fix the same run was cancelled by the budget and answered with the
// rows of the files not yet read missing. Numbers are logged for the PR record.
// pressureRun reads 40 objects x 5,000 rows with 16 workers and a slow consumer
// under the given ceiling, and reports the peak heap growth and the high-water
// mark of the bytes reserved at the ceiling.
func pressureRun(t *testing.T, ceiling int64) (int64, int64, int64) {
	t.Helper()
	fs := newFaultS3(t)
	s := testStorageWithS3(t, fs.srv.URL)
	s.cfg.Query.FileWorkers = 16
	s.cfg.Query.MaxLiveBytes = ceiling
	var gate *liveBytesGate
	liveGateHook = func(g *liveBytesGate) { gate = g }
	t.Cleanup(func() { liveGateHook = nil })
	const files, rowsPerFile = 40, 5000
	rng := rand.New(rand.NewSource(7))
	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	for i := 0; i < files; i++ {
		rows := make([]logRow, rowsPerFile)
		for j := range rows {
			rows[j] = logRow{
				TimestampUnixNano: now.Add(time.Duration(i*rowsPerFile+j) * time.Microsecond).UnixNano(),
				Body:              fmt.Sprintf("file=%d row=%d %016x%016x%016x%016x%016x%016x", i, j, rng.Uint64(), rng.Uint64(), rng.Uint64(), rng.Uint64(), rng.Uint64(), rng.Uint64()), // incompressible, so stored size ~ decoded size
				SeverityText:      "INFO",
				ServiceName:       fmt.Sprintf("svc-%d", i%8),
			}
		}
		registerFileInMockS3(t, s, fs.mock, fmt.Sprintf("logs/dt=2026-05-10/hour=14/pressure-%03d.parquet", i), writeParquetToBytes(t, rows), now)
	}
	q := mustParseQueryWithTime(t, "*", now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())

	defer debug.SetGCPercent(debug.SetGCPercent(20))
	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	var peak atomic.Int64
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		var ms runtime.MemStats
		for {
			select {
			case <-stop:
				return
			default:
			}
			runtime.ReadMemStats(&ms)
			if cur := int64(ms.HeapAlloc) - int64(before.HeapAlloc); cur > peak.Load() {
				peak.Store(cur)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	waits := metrics.QueryMemoryBudgetExceeded.Get()
	var rows, maxBlock atomic.Int64
	started := time.Now()
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		rows.Add(int64(db.RowsCount()))
		if b := dataBlockApproxBytes(db); b > maxBlock.Load() {
			maxBlock.Store(b)
		}
		time.Sleep(500 * time.Microsecond) // a slow consumer: the HTTP writer
	})
	close(stop)
	<-done
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	t.Logf("MEASURED ceiling=%d B: rows=%d/%d budget_waits=%d peak_heap_growth=%.1f MiB high_water_reserved=%.1f MiB max_block=%.2f MiB elapsed=%s",
		ceiling, rows.Load(), files*rowsPerFile, metrics.QueryMemoryBudgetExceeded.Get()-waits,
		float64(peak.Load())/(1<<20), float64(gate.peak.Load())/(1<<20), float64(maxBlock.Load())/(1<<20), time.Since(started).Round(time.Millisecond))
	return rows.Load(), peak.Load(), gate.peak.Load()
}

// Under real pressure (sixteen workers, a slow consumer) the ceiling bounds what is
// held at once, and every row arrives. The same run with the ceiling out of reach
// is the comparison; the numbers are logged for the PR record.
func TestRowRead_LiveBytesBudget_BoundsHeapAndKeepsEveryRow(t *testing.T) {
	if testing.Short() {
		t.Skip("memory measurement: seconds, real S3 code paths")
	}
	const want = 40 * 5000
	defer debug.SetGCPercent(debug.SetGCPercent(20))
	rowsOn, heapOn, reservedOn := pressureRun(t, 1<<20)
	rowsOff, heapOff, reservedOff := pressureRun(t, 8<<30)
	t.Logf("COMPARE ceiling 1 MiB: heap growth %.1f MiB, reserved high-water %.1f MiB; ceiling out of reach: heap growth %.1f MiB, reserved high-water %.1f MiB",
		float64(heapOn)/(1<<20), float64(reservedOn)/(1<<20), float64(heapOff)/(1<<20), float64(reservedOff)/(1<<20))
	if rowsOn != want || rowsOff != want {
		t.Fatalf("rows = %d / %d, want %d: the budget dropped rows", rowsOn, rowsOff, want)
	}
	if reservedOn >= reservedOff {
		t.Fatalf("the ceiling did not lower what is reserved at once (%d >= %d B)", reservedOn, reservedOff)
	}
	if heapOn > 384<<20 {
		t.Fatalf("peak heap growth %.1f MiB exceeds the 384 MiB production ceiling of the memory-budget tests", float64(heapOn)/(1<<20))
	}
}

// --- benchmarks of the touched read path ------------------------------------

func benchColdFixture(b *testing.B) (*Storage, int64, int64) {
	b.Helper()
	fs := newFaultS3(b)
	s := testStorageWithS3(b, fs.srv.URL)
	s.cfg.Query.FileWorkers = 8
	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	for i := 0; i < 12; i++ {
		rows := make([]logRow, 2000)
		for j := range rows {
			rows[j] = logRow{
				TimestampUnixNano: now.Add(time.Duration(i*2000+j) * time.Microsecond).UnixNano(),
				Body:              fmt.Sprintf("file=%d row=%d payload", i, j),
				SeverityText:      []string{"INFO", "WARN", "ERROR"}[j%3],
				ServiceName:       fmt.Sprintf("svc-%d", j%7),
			}
		}
		key := fmt.Sprintf("logs/dt=2026-05-10/hour=14/bench-%02d.parquet", i)
		fs.mock.putFile(key, writeParquetToBytes(b, rows))
		data := fs.mock.files[key]
		s.manifest.AddFile("dt=2026-05-10/hour=14", manifest.FileInfo{Key: key, Size: int64(len(data)), MinTimeNs: now.Add(-time.Minute).UnixNano(), MaxTimeNs: now.Add(time.Minute).UnixNano()})
	}
	return s, now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano()
}

// BenchmarkRunQuery_ColdScan is a wildcard row read over twelve objects: the file
// worker loop, processOneFile and the per-block budget gate.
func BenchmarkRunQuery_ColdScan(b *testing.B) {
	s, lo, hi := benchColdFixture(b)
	q, err := logstorage.ParseQuery("*")
	if err != nil {
		b.Fatal(err)
	}
	q.AddTimeFilter(lo, hi)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var rows atomic.Int64
		if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) { rows.Add(int64(db.RowsCount())) }); err != nil {
			b.Fatal(err)
		}
		if rows.Load() != 12*2000 {
			b.Fatalf("rows = %d", rows.Load())
		}
	}
}

// BenchmarkGetFieldValues_ColdScan is a field_values scan over the same objects:
// scanFieldValuesParallel and scanProjectedFieldValues.
func BenchmarkGetFieldValues_ColdScan(b *testing.B) {
	s, lo, hi := benchColdFixture(b)
	q, err := logstorage.ParseQuery("*")
	if err != nil {
		b.Fatal(err)
	}
	q.AddTimeFilter(lo, hi)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vals, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
		if err != nil || len(vals) != 7 {
			b.Fatalf("values = %d, err %v", len(vals), err)
		}
	}
}

// The first error of a read is kept whatever its concrete type: a cancellation
// and a wrapped file error recorded one after the other must not panic (an
// atomic.Value takes one concrete type only).
func TestRowRead_FirstErrorKeepsMixedErrorTypes(t *testing.T) {
	var v atomic.Value
	if recordedReadError(&v) != nil {
		t.Fatal("an unset read has an error")
	}
	first := context.DeadlineExceeded
	recordReadError(&v, first)
	recordReadError(&v, fmt.Errorf("cannot read k: %w", errors.New("boom")))
	recordReadError(&v, context.Canceled)
	if got := recordedReadError(&v); got != first {
		t.Fatalf("recorded %v, want the first error %v", got, first)
	}
	var w atomic.Value
	recordReadError(&w, fmt.Errorf("cannot read k: %w", errors.New("boom")))
	recordReadError(&w, context.Canceled)
	if got := recordedReadError(&w); got == nil || !strings.Contains(got.Error(), "cannot read k") {
		t.Fatalf("recorded %v, want the file error", got)
	}
}

// The first failure stops the other workers: they would only read objects for an
// answer that is already lost.
func TestRowRead_FailureStopsTheOtherWorkers(t *testing.T) {
	const files = 24
	fs := newFaultS3(t)
	s := testStorageWithS3(t, fs.srv.URL)
	s.cfg.Query.FileWorkers = 2
	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	var keys []string
	for i := 0; i < files; i++ {
		rows := make([]logRow, 40)
		for j := range rows {
			rows[j] = logRow{TimestampUnixNano: now.Add(time.Duration(i*40+j) * time.Millisecond).UnixNano(), Body: "x", SeverityText: "INFO", ServiceName: "svc"}
		}
		key := fmt.Sprintf("logs/dt=2026-05-10/hour=14/stop-%02d.parquet", i)
		registerFileInMockS3(t, s, fs.mock, key, writeParquetToBytes(t, rows), now)
		keys = append(keys, key)
	}
	var served atomic.Int64
	slow := func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(15 * time.Millisecond)
		served.Add(1)
		fs.mock.handler(w, r)
	}
	for _, k := range keys {
		fs.setFault(k, slow)
	}
	q := mustParseQueryWithTime(t, "*", now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	run := func() error {
		return s.RunQuery(context.Background(), nil, q, func(_ uint, _ *logstorage.DataBlock) {})
	}
	if err := run(); err != nil {
		t.Fatalf("control: %v", err)
	}
	full := served.Load()

	// A second reader over the same listing, with cold caches: the first run
	// left the objects in the caches of the first.
	s2 := testStorageWithS3(t, fs.srv.URL)
	s2.cfg.Query.FileWorkers = 2
	s2.manifest = s.manifest
	run = func() error {
		return s2.RunQuery(context.Background(), nil, q, func(_ uint, _ *logstorage.DataBlock) {})
	}
	served.Store(0)
	fs.setFault(keys[0], accessDenied)
	if err := run(); err == nil {
		t.Fatal("RunQuery answered without error while an object was unreadable")
	}
	if got := served.Load(); got > full/2 {
		t.Fatalf("%d requests served after the first failure, %d for the whole read: the other workers kept reading", got, full)
	}
}

// A worker waiting for a row-group decoder slot stops waiting when its query's
// context ends, instead of holding its place until a slot frees.
func TestAcquireRGDecode_StopsWaitingWhenContextEnds(t *testing.T) {
	var held []func()
	for i := 0; i < cap(rgDecodeSem); i++ {
		rel, err := acquireRGDecode(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, rel)
	}
	defer func() {
		for _, r := range held {
			r()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := acquireRGDecode(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want the deadline", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the wait did not end with the context")
	}
}

// --- liveBytesGate --------------------------------------------------------

func TestLiveBytesGate_WaitsForRelease(t *testing.T) {
	var g liveBytesGate
	rel, err := g.reserve(context.Background(), 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error)
	go func() {
		r, err := g.reserve(context.Background(), 10, 100)
		if err == nil {
			r()
		}
		done <- err
	}()
	select {
	case <-done:
		t.Fatal("reserve returned while the bytes reserved filled the ceiling")
	case <-time.After(50 * time.Millisecond):
	}
	rel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("reserve gave up although room was made: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reserve was not woken by the release")
	}
}

func TestLiveBytesGate_ContextEndReleasesWaiter(t *testing.T) {
	var g liveBytesGate
	if _, err := g.reserve(context.Background(), 100, 100); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() {
		_, err := g.reserve(ctx, 10, 100)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want the cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled query stayed blocked at the budget")
	}
	if got := g.live.Load(); got != 100 {
		t.Fatalf("live = %d, a cancelled waiter must reserve nothing", got)
	}
}

// An object (block) larger than the ceiling is admitted alone, and only when nothing else
// is reserved: no object waits for room that never comes, and none passes the
// ceiling next to another.
func TestLiveBytesGate_OversizeBlockAdmittedWhenIdle(t *testing.T) {
	var g liveBytesGate
	rel, err := g.reserve(context.Background(), 1<<20, 10)
	if err != nil {
		t.Fatalf("an idle gate refused an oversize object: %v", err)
	}
	done := make(chan struct{})
	go func() {
		r, _ := g.reserve(context.Background(), 1, 10)
		r()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("an object was admitted next to an oversize one")
	case <-time.After(50 * time.Millisecond):
	}
	rel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the gate stayed closed after the oversize object was released")
	}
}

// The reserved bytes never pass the ceiling, whatever the interleaving, and every
// worker finishes.
func TestLiveBytesGate_ConcurrentWorkersAllFinish(t *testing.T) {
	var g liveBytesGate
	const workers, rounds, size, ceiling = 8, 200, 40, 100
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := 0; b < rounds; b++ {
				rel, err := g.reserve(context.Background(), size, ceiling)
				if err != nil {
					t.Error(err)
					return
				}
				if cur := g.live.Load(); cur > ceiling {
					t.Errorf("reserved %d bytes, ceiling %d", cur, ceiling)
				}
				rel()
			}
		}()
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("workers deadlocked at the gate")
	}
	if g.live.Load() != 0 {
		t.Fatalf("live = %d after every reservation was released", g.live.Load())
	}
	if g.peak.Load() > ceiling {
		t.Fatalf("high-water mark %d passed the ceiling %d", g.peak.Load(), ceiling)
	}
}

// The ceiling bounds the DECODED bytes in flight, however cold the footer cache
// and however compressible the data. The ceiling is sized from a run with no
// ceiling (the real decoded size of a row group, measured in writeBlock), never
// from the estimator under test; the bytes in flight are the blocks between the
// decoder and the consumer. A gate that estimates too low (the compressed size, a
// constant, no footer) lets the workers pile up past it.
func TestRowRead_LiveBytesCeiling_InFlightDecodedBytesAreBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("in-flight ceiling matrix: runs in the heavy job")
	}
	const files = 12
	type tcase struct {
		name, query    string
		warm           bool
		incompressible bool
	}
	// Each dimension (footer cold/warm, compressible/not, filtered/not) is crossed
	// with the others in at least one case; all eight would cost the package its
	// CI time budget.
	cases := []tcase{
		{"compressible/cold/unfiltered", "*", false, false},
		{"compressible/warm/filtered", "padding", true, false},
		{"incompressible/cold/filtered", "padding", false, true},
		{"incompressible/warm/unfiltered", "*", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() { inflightBlockHook = nil; liveGateHook = nil })
			measure := func(fx *rcFixture, pile bool) (peak, maxBlock, totalBytes, blocks int64, rows int, err error) {
				var inflight, pk, mb, total, n atomic.Int64
				var piled sync.Once
				inflightBlockHook = func(d int64) {
					cur := inflight.Add(d)
					for {
						p := pk.Load()
						if cur <= p || pk.CompareAndSwap(p, cur) {
							break
						}
					}
					if d > 0 {
						total.Add(d)
						n.Add(1)
						for {
							m := mb.Load()
							if d <= m || mb.CompareAndSwap(m, d) {
								break
							}
						}
					}
				}
				rows, err = fx.query(t, context.Background(), tc.query, func(*logstorage.DataBlock) {
					if pile {
						// Hold the first block until a second one has reached the consumer's
						// door, so that the unbounded run overlaps whatever the scheduler does.
						piled.Do(func() {
							for deadline := time.Now().Add(10 * time.Second); inflight.Load() <= mb.Load() && time.Now().Before(deadline); {
								time.Sleep(time.Millisecond)
							}
						})
					}
					time.Sleep(2 * time.Millisecond)
				})
				return pk.Load(), mb.Load(), total.Load(), n.Load(), rows, err
			}

			// Without a ceiling: the real decoded size of a row group, and what the
			// workers pile up to (the process-wide row-group decoder limit caps that at
			// two on a small machine). The ceiling is three row groups, or lower when
			// the unbounded run itself did not reach that, so that the limit
			// (ceiling + one block) is always below what a gate-less run holds.
			ref := newRCFixtureShape(t, 16, files, rcKeyFmt, tc.incompressible)
			ref.s.cfg.Query.MaxLiveBytes = 8 << 30
			unboundedPeak, maxBlock, total, blocks, rows, err := measure(ref, true)
			if err != nil || rows != files*rcRowsPerFile {
				t.Fatalf("unbounded run: %d rows, err %v", rows, err)
			}
			rowGroup := total / blocks // one block per row group in these objects
			ceiling := 3 * rowGroup
			if unboundedPeak <= ceiling+maxBlock {
				ceiling = unboundedPeak - maxBlock - rowGroup/2
			}
			if ceiling < 1 || unboundedPeak <= ceiling+maxBlock {
				t.Fatalf("without a ceiling the workers hold only %d B in flight (one block is %d B): the test cannot tell a gate from none", unboundedPeak, maxBlock)
			}

			fx := newRCFixtureShape(t, 16, files, rcKeyFmt, tc.incompressible)
			fx.s.cfg.Query.MaxLiveBytes = ceiling
			if tc.warm {
				if _, err := fx.query(t, context.Background(), tc.query, nil); err != nil { // fills the footer cache
					t.Fatal(err)
				}
			}
			waits := metrics.QueryMemoryBudgetExceeded.Get()
			peak, _, _, _, rows, err := measure(fx, false)
			if err != nil || rows != files*rcRowsPerFile {
				t.Fatalf("%d rows of %d, err %v: the ceiling must make workers wait, not drop rows", rows, files*rcRowsPerFile, err)
			}
			t.Logf("MEASURED %s: ceiling=%d B (3 row groups of ~%d B), in-flight peak=%d B (limit ceiling+block=%d B), without a ceiling=%d B, waits=%d",
				tc.name, ceiling, rowGroup, peak, ceiling+maxBlock, unboundedPeak, metrics.QueryMemoryBudgetExceeded.Get()-waits)
			if peak > ceiling+maxBlock {
				t.Fatalf("decoded bytes in flight peaked at %d B, over the ceiling %d B plus one block %d B", peak, ceiling, maxBlock)
			}
			if metrics.QueryMemoryBudgetExceeded.Get() <= waits {
				t.Fatal("no worker ever waited at the ceiling: the test does not exercise it")
			}
		})
	}
}

// The estimate is not below what is decoded: for every row group, the sum of the
// blocks it emits (as writeBlock sees them) stays within the footer-derived
// estimate, on compressible and on incompressible data.
func TestDecodedEstimate_CoversWhatIsDecoded(t *testing.T) {
	for _, shape := range []string{"compressible", "incompressible"} {
		t.Run(shape, func(t *testing.T) {
			fx := newRCFixtureShape(t, 2, 2, rcKeyFmt, shape == "incompressible")
			for i := range fx.keys {
				f, err := parquet.OpenFile(bytes.NewReader(fx.datas[i]), int64(len(fx.datas[i])))
				if err != nil {
					t.Fatal(err)
				}
				for idx, rg := range f.RowGroups() {
					var got int64
					err := fx.s.readOneRowGroup(context.Background(), f, rg, fx.start, fx.end, nil, nil, func(_ uint, db *logstorage.DataBlock) { got += dataBlockApproxBytes(db) }, nil)
					if err != nil {
						t.Fatal(err)
					}
					est := decodedEstimate(f.Metadata(), idx, nil, int64(len(fx.datas[i])))
					if est < got {
						t.Fatalf("row group %d of %s: estimate %d B is below the %d B it decodes to", idx, shape, est, got)
					}
					if est > 8*got {
						t.Fatalf("row group %d of %s: estimate %d B is more than 8x the %d B it decodes to", idx, shape, est, got)
					}
				}
			}
		})
	}
}

// reserveRowGroup reserves what THIS row group's footer says, not a guess from the
// object's stored size: at least the uncompressed sizes of its columns, and not the
// stored-size fallback.
func TestReserveRowGroup_UsesTheFooterOfThatRowGroup(t *testing.T) {
	fx := newRCFixtureN(t, 1, 1, rcKeyFmt)
	data := fx.datas[0]
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	var g liveBytesGate
	ctx := withLiveBytesGate(context.Background(), &g, 1<<40)
	size := int64(len(data))
	for idx, rg := range f.Metadata().RowGroups {
		var uncompressed int64
		for _, c := range rg.Columns {
			if !schema.IsInternalColumn(c.MetaData.PathInSchema[0]) {
				uncompressed += c.MetaData.TotalUncompressedSize
			}
		}
		rel, err := reserveRowGroup(ctx, f, idx, nil, size)
		if err != nil {
			t.Fatal(err)
		}
		got := g.live.Load()
		rel()
		if got < uncompressed {
			t.Fatalf("row group %d: reserved %d B, below its %d B of uncompressed column data", idx, got, uncompressed)
		}
		if got == size*assumedDecodeExpansion || got == size*assumedDecodeExpansion/int64(len(f.Metadata().RowGroups)) {
			t.Fatalf("row group %d: reserved %d B, the stored-size fallback, not what its footer says", idx, got)
		}
	}
	if g.live.Load() != 0 {
		t.Fatalf("live = %d after the reservations were released", g.live.Load())
	}
}

// The estimator itself: the sum of the projected columns' sizes (the larger of the
// uncompressed size and values x the longest statistic), never the compressed size,
// and the fallbacks for a row group without sizes.
func TestDecodedEstimate_Estimator(t *testing.T) {
	col := func(name string, values, uncompressed, compressed int64, minV, maxV string) format.ColumnChunk {
		return format.ColumnChunk{MetaData: format.ColumnMetaData{
			PathInSchema: []string{name}, NumValues: values, TotalUncompressedSize: uncompressed, TotalCompressedSize: compressed,
			Statistics: format.Statistics{MinValue: []byte(minV), MaxValue: []byte(maxV)},
		}}
	}
	md := &format.FileMetaData{RowGroups: []format.RowGroup{
		{Columns: []format.ColumnChunk{
			col("body", 1000, 50_000, 500, "", ""),               // plain: uncompressed size wins
			col("service.name", 1000, 40, 30, "svc-a", "svc-zz"), // dictionary: values x longest statistic wins (6,000)
			col("timestamp_unix_nano", 1000, 8_000, 800, "", ""),
		}},
		{Columns: nil},
	}}
	if got, want := decodedEstimate(md, 0, nil, 1<<20), int64(50_000+6_000+8_000); got != want {
		t.Fatalf("all columns: %d, want %d", got, want)
	}
	if got, want := decodedEstimate(md, 0, map[string]bool{"service.name": true}, 1<<20), int64(6_000); got != want {
		t.Fatalf("projected: %d, want %d", got, want)
	}
	if got := decodedEstimate(md, 0, map[string]bool{"absent": true}, 1<<20); got != (1<<20)*assumedDecodeExpansion/2 {
		t.Fatalf("no projected column in the row group: %d, want the stored-size fallback", got)
	}
	if got := decodedEstimate(md, 1, nil, 1<<20); got != (1<<20)*assumedDecodeExpansion/2 {
		t.Fatalf("a row group without columns: %d, want the stored-size fallback", got)
	}
	if got := decodedEstimate(nil, 0, nil, 4000); got != 4000*assumedDecodeExpansion {
		t.Fatalf("no metadata: %d, want %d", got, 4000*assumedDecodeExpansion)
	}
	if got := decodedEstimate(md, 7, nil, 0); got != 1 {
		t.Fatalf("an index past the row groups and no stored size: %d, want 1", got)
	}
}

// KNOWN GAP (#793): a query whose consumer is stalled can still hold the
// process-wide file-budget and row-group-decoder slots another query needs: its
// workers hold them across a blocking writeBlock, whatever the ceiling. This pins
// today's behaviour at the DEFAULT ceiling so that it stays visible; when the gap is
// fixed the second query finishes and this test must become that assertion.
func TestRowRead_StalledConsumer_StarvesOtherQueries_KnownGap793(t *testing.T) {
	if testing.Short() {
		t.Skip("stalled-consumer pin: runs in the heavy job")
	}
	a := newRCFixtureN(t, 12, 12, rcKeyFmt) // more workers and objects than the process has file slots (at most 8)
	b := newRCFixtureN(t, 4, rcFiles, "logs/dt=2026-05-10/hour=14/starve-b-%d.parquet")
	stall := make(chan struct{})
	started := make(chan struct{}, 1)
	aDone := make(chan error, 1)
	go func() {
		q := mustParseQueryWithTime(t, "*", a.start, a.end)
		aDone <- a.s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-stall
		})
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("query A never reached its consumer")
	}
	time.Sleep(300 * time.Millisecond) // let A's other workers take their slots

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	type result struct {
		rows int
		err  error
	}
	res := make(chan result, 1)
	go func() {
		rows, err := b.query(t, ctx, "*", nil)
		res <- result{rows, err}
	}()
	select {
	case r := <-res:
		if r.err == nil {
			t.Logf("query B finished (%d rows) while A's consumer was stalled: the gap #793 may be fixed; turn this test into that assertion", r.rows)
			t.Fail()
		} else if !errors.Is(r.err, context.DeadlineExceeded) {
			t.Fatalf("query B: %v, want it to miss its deadline (the pinned gap)", r.err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("query B did not return 58 s after its deadline")
	}
	close(stall)
	if err := <-aDone; err != nil {
		t.Fatalf("query A: %v", err)
	}
}
