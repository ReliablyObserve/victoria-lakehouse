package parquets3

// A row read answers completely or fails (issue #503). Before the fix a read
// answered 200 with rows missing when (a) the read of an object it selected
// failed with anything but a 404 (an S3 error, a corrupt footer, a truncated
// body), (b) its caller cancelled it or its deadline passed while the last object
// was being read, or (c) the per-query live-bytes budget cancelled it. A listed
// object that a compaction or a retention run removed (404) stays tolerated.
//
// Mirror of internal/storage/parquets3/row_read_complete_test.go for spans: the
// same guarantees over trace objects, plus trace-by-ID through the adapter.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"
	smithy "github.com/aws/smithy-go"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
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
	rcRowsPerFile = 300 // > one 256-row read buffer: every object emits several blocks
	rcFiles       = 3
)

type rcFixture struct {
	fs    *faultS3
	s     *Storage
	keys  []string
	datas [][]byte
	base  time.Time
	start int64
	end   int64
}

func rcTraceID(file, row int) string { return fmt.Sprintf("%032x", uint64(file)<<32|uint64(row/7)) }

func rcSpans(base time.Time, file, n int) []schema.TraceRow {
	rows := make([]schema.TraceRow, n)
	for j := range rows {
		ts := base.Add(time.Duration(file*n+j) * time.Millisecond).UnixNano()
		svc := fmt.Sprintf("svc-%d", file)
		rows[j] = schema.TraceRow{
			TimestampUnixNano: ts,
			StartTimeUnixNano: schema.Int64Ptr(ts),
			TraceID:           rcTraceID(file, j),
			SpanID:            fmt.Sprintf("%016x", file*n+j),
			SpanName:          fmt.Sprintf("op-%d", j%5),
			ServiceName:       svc,
			DurationNs:        schema.Int64Ptr(int64(1000 * (1 + j%4))),
			StatusCode:        schema.Int32Ptr(0),
			Stream:            fmt.Sprintf(`{resource_attr:service.name=%q}`, svc),
			StreamID:          fmt.Sprintf("%048x", file),
		}
	}
	return rows
}

// newRCFixture registers rcFiles span objects of rcRowsPerFile spans each.
func newRCFixture(t *testing.T, workers int, keyFmt string) *rcFixture {
	return newRCFixtureN(t, workers, rcFiles, keyFmt)
}

// newRCFixtureN registers n objects.
func newRCFixtureN(t *testing.T, workers, n int, keyFmt string) *rcFixture {
	t.Helper()
	fs := newFaultS3(t)
	s := testStorageWithS3(t, fs.srv.URL)
	s.cfg.Mode = config.ModeTraces
	s.cfg.Query.FileWorkers = workers
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	fx := &rcFixture{fs: fs, s: s, base: base, start: base.Add(-time.Hour).UnixNano(), end: base.Add(time.Hour).UnixNano()}
	for i := 0; i < n; i++ {
		res, err := writeTracesParquet(rcSpans(base, i, rcRowsPerFile), 100, 3)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf(keyFmt, i)
		registerFileInMockS3(t, s, fs.mock, key, res.Data, base)
		fx.keys = append(fx.keys, key)
		fx.datas = append(fx.datas, res.Data)
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

const rcKeyFmt = "traces/dt=2026-10-23/hour=10/rc-%d.parquet"

// A control: with no fault the read returns every row.
func TestRowRead_NoFault_ReturnsEveryRow(t *testing.T) {
	fx := newRCFixture(t, 4, rcKeyFmt)
	rows, err := fx.query(t, context.Background(), "*", nil)
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
	fx.fs.mock.putFile("traces/dt=2026-10-23/hour=10/compacted-merge.parquet", fx.datas[1])
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
	key := "traces/dt=2026-10-23/hour=10/part-404.parquet"
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

	fx := newRCFixture(t, 2, "traces/dt=2026-10-23/hour=10/part-404-%d.parquet")
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
	t.Run("deadline", func(t *testing.T) {
		fx := newRCFixture(t, 1, rcKeyFmt) // one worker: the last object is read last
		fx.fs.setFault(fx.keys[rcFiles-1], hang)
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer cancel()
		rows, err := fx.query(t, ctx, "*", nil)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("RunQuery = %v after %d rows, want the deadline error", err, rows)
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
			_, err := fx.query(t, context.Background(), "svc-1", nil)
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
	s.cfg.Mode = config.ModeTraces
	s.cfg.Query.FileWorkers = 2
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	rows := rcSpans(base, 0, 900)
	res, err := writeTracesParquet(rows, 100, 3)
	if err != nil {
		t.Fatal(err)
	}
	key := "traces/dt=2026-10-23/hour=10/compacted-L2-000.parquet"
	registerFileInMockS3(t, s, fs.mock, key, res.Data, base)
	// Damage only the second half of the pages: the first row groups decode and
	// are emitted, a later one fails.
	fs.mock.putFile(key, corruptPages(t, res.Data, 5))

	q := mustParseQueryWithTime(t, "*", base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	var got atomic.Int64
	if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) { got.Add(int64(db.RowsCount())) }); err == nil {
		t.Fatalf("RunQuery answered %d of %d spans without error from a damaged compacted object", got.Load(), len(rows))
	}
}

// Trace-by-ID (what Jaeger's GetTrace and Tempo's trace lookup issue) through the
// traces adapter: an unreadable object fails the lookup instead of returning the
// trace without the spans it held; a removed one stays tolerated.
func TestRowRead_TraceByID_ThroughAdapter(t *testing.T) {
	run := func(fx *rcFixture, tid string) ([]map[string]string, error) {
		vtstorageadapter.Init(fx.s)
		t.Cleanup(func() { vtstorage.SetExternalStorage(nil) })
		q := mustParseQueryWithTime(t, fmt.Sprintf("trace_id:=%q", tid), fx.start, fx.end)
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, nil)
		var mu sync.Mutex
		var out []map[string]string
		err := vtstorage.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			rows := blockRowFields([]*logstorage.DataBlock{db})
			mu.Lock()
			out = append(out, rows...)
			mu.Unlock()
		})
		return out, err
	}
	tid := rcTraceID(1, 0)

	fx := newRCFixture(t, 2, rcKeyFmt)
	rows, err := run(fx, tid)
	if err != nil || len(rows) != 7 {
		t.Fatalf("control: %d spans, err %v; want 7 spans", len(rows), err)
	}

	fx = newRCFixture(t, 2, rcKeyFmt)
	fx.fs.setFault(fx.keys[1], accessDenied)
	if rows, err := run(fx, tid); err == nil {
		t.Fatalf("trace-by-ID answered %d spans without error while an object was unreadable", len(rows))
	}

	fx = newRCFixture(t, 2, rcKeyFmt)
	fx.fs.mock.mu.Lock()
	delete(fx.fs.mock.files, fx.keys[2]) // an object the trace is not in, removed meanwhile
	fx.fs.mock.mu.Unlock()
	if rows, err := run(fx, tid); err != nil || len(rows) != 7 {
		t.Fatalf("removed object: %d spans, err %v; want 7 spans and no error", len(rows), err)
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
	s.cfg.Mode = config.ModeTraces
	s.cfg.Query.FileWorkers = 2
	now := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	var keys []string
	for i := 0; i < files; i++ {
		res, err := writeTracesParquet(rcSpans(now, i, 40), 40, 3)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("traces/dt=2026-10-23/hour=10/stop-%02d.parquet", i)
		registerFileInMockS3(t, s, fs.mock, key, res.Data, now)
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
	s2.cfg.Mode = config.ModeTraces
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
	began := time.Now()
	if _, err := acquireRGDecode(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the deadline", err)
	}
	if time.Since(began) > 5*time.Second {
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

// An object larger than the ceiling is admitted alone, and only when nothing else
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

// The ceiling bounds the decoded bytes in flight for real: with sixteen workers
// and a consumer slower than the decoders, the high-water mark of the bytes
// reserved stays within the ceiling plus one object, while every row arrives. A
// gate that counts but does not wait (the budget as a bare counter) lets every
// worker reserve at once and fails this.
func TestRowRead_LiveBytesCeiling_HighWaterMarkIsBounded(t *testing.T) {
	const files = 12
	fx := newRCFixtureN(t, 16, files, rcKeyFmt)
	var gatePtr atomic.Pointer[liveBytesGate]
	liveGateHook = func(g *liveBytesGate) { gatePtr.Store(g) }
	t.Cleanup(func() { liveGateHook = nil })

	// One object is estimated at about E bytes; a ceiling of 2.5 E lets two be
	// read at once, of twelve.
	var est int64
	for i := range fx.keys {
		if e := fx.s.estimateDecodedBytes(fx.info(i)); e > est {
			est = e
		}
	}
	ceiling := est * 5 / 2
	fx.s.cfg.Query.MaxLiveBytes = ceiling

	waits := metrics.QueryMemoryBudgetExceeded.Get()
	// The high-water mark is also sampled from the outside, so a gate that stops
	// recording its own mark cannot hide a breach.
	var sampled atomic.Int64
	stopSampler, samplerDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(samplerDone)
		for {
			select {
			case <-stopSampler:
				return
			default:
			}
			if g := gatePtr.Load(); g != nil {
				if cur := g.live.Load(); cur > sampled.Load() {
					sampled.Store(cur)
				}
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()
	rows, err := fx.query(t, context.Background(), "*", func(*logstorage.DataBlock) { time.Sleep(2 * time.Millisecond) })
	close(stopSampler)
	<-samplerDone
	gate := gatePtr.Load()
	if err != nil || rows != files*rcRowsPerFile {
		t.Fatalf("%d rows of %d, err %v: the ceiling must make workers wait, not drop rows", rows, files*rcRowsPerFile, err)
	}
	var maxEst int64
	for i := range fx.keys {
		if e := fx.s.estimateDecodedBytes(fx.info(i)); e > maxEst {
			maxEst = e
		}
	}
	t.Logf("MEASURED ceiling=%d B, largest object estimate=%d B, high-water mark of reserved bytes=%d B, waits=%d", ceiling, maxEst, gate.peak.Load(), metrics.QueryMemoryBudgetExceeded.Get()-waits)
	if peak := gate.peak.Load(); peak > ceiling+maxEst {
		t.Fatalf("high-water mark %d B passed the ceiling %d B by more than one object (%d B)", peak, ceiling, maxEst)
	}
	if got := sampled.Load(); got > ceiling+maxEst {
		t.Fatalf("sampled reserved bytes %d B passed the ceiling %d B by more than one object (%d B)", got, ceiling, maxEst)
	}
	if gate.peak.Load() < est {
		t.Fatalf("high-water mark %d B is below one object (%d B): nothing was reserved", gate.peak.Load(), est)
	}
	if metrics.QueryMemoryBudgetExceeded.Get() <= waits {
		t.Fatal("no worker ever waited at the ceiling: the test does not exercise it")
	}
}

// A query whose consumer is stalled does not starve the others of the
// process-wide slots: its workers wait at the budget holding nothing, so another
// query (own storage, normal budget) finishes inside its deadline.
func TestRowRead_StalledConsumerDoesNotStarveOtherQueries(t *testing.T) {
	a := newRCFixtureN(t, 12, 12, rcKeyFmt) // more workers and objects than the process has file slots (at most 8)
	a.s.cfg.Query.MaxLiveBytes = 1          // one object at a time
	b := newRCFixtureN(t, 4, rcFiles, "logs/dt=2026-05-10/hour=14/starve-b-%d.parquet")
	if strings.HasPrefix(a.keys[0], "traces/") {
		b = newRCFixtureN(t, 4, rcFiles, "traces/dt=2026-10-23/hour=10/starve-b-%d.parquet")
	}

	stall := make(chan struct{})
	started := make(chan struct{}, 1)
	aDone := make(chan error, 1)
	var aRows atomic.Int64
	go func() {
		q := mustParseQueryWithTime(t, "*", a.start, a.end)
		aDone <- a.s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-stall
			aRows.Add(int64(db.RowsCount()))
		})
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("query A never reached its consumer")
	}
	time.Sleep(200 * time.Millisecond) // let A's other workers reach the budget

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	began := time.Now()
	rows, err := b.query(t, ctx, "*", nil)
	if err != nil || rows != rcFiles*rcRowsPerFile {
		close(stall)
		<-aDone
		t.Fatalf("query B: %d rows, err %v after %s: a stalled consumer of query A starved it", rows, err, time.Since(began))
	}
	t.Logf("query B finished in %s while query A's consumer was stalled", time.Since(began).Round(time.Millisecond))

	close(stall)
	select {
	case err := <-aDone:
		if err != nil || aRows.Load() != int64(12*rcRowsPerFile) {
			t.Fatalf("query A: %d rows, err %v; want every row", aRows.Load(), err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("query A did not finish once its consumer resumed")
	}
}

// info is the manifest entry the estimate reads (key and stored size) of object i.
func (fx *rcFixture) info(i int) manifest.FileInfo {
	return manifest.FileInfo{Key: fx.keys[i], Size: int64(len(fx.datas[i]))}
}
