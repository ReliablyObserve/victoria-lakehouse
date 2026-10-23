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
	smithy "github.com/aws/smithy-go"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
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
	rcRowsPerFile = 700 // > one 256-row read buffer: every file emits several blocks
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
	t.Helper()
	fs := newFaultS3(t)
	s := testStorageWithS3(t, fs.srv.URL)
	s.cfg.Query.FileWorkers = workers
	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	fx := &rcFixture{fs: fs, s: s, now: now, start: now.Add(-time.Hour).UnixNano(), end: now.Add(time.Hour).UnixNano()}
	for i := 0; i < rcFiles; i++ {
		rows := make([]rcLogRow, rcRowsPerFile)
		for j := range rows {
			svc := fmt.Sprintf("svc-%d", i)
			rows[j] = rcLogRow{
				TimestampUnixNano: now.Add(time.Duration(i*rcRowsPerFile+j) * time.Millisecond).UnixNano(),
				Body:              fmt.Sprintf("file=%d row=%d", i, j),
				SeverityText:      "INFO",
				ServiceName:       svc,
				Stream:            fmt.Sprintf(`{service.name=%q}`, svc),
				StreamID:          fmt.Sprintf("%048x", i),
			}
		}
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[rcLogRow](&buf, parquet.Compression(&parquet.Zstd))
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
			fx.s.cfg.Query.MaxLiveBytes = 1 // every block in flight reaches the ceiling

			start := metrics.QueryMemoryBudgetExceeded.Get()
			var first sync.Once
			rows, err := fx.query(t, context.Background(), "*", func(db *logstorage.DataBlock) {
				first.Do(func() {
					// Hold the consumer so the other workers pile up behind it with
					// blocks of every object left. A worker that reaches the ceiling
					// waits (and counts); one that got past it before the block in
					// flight was counted waits on the consumer instead, and reaches
					// the ceiling with its next block once the consumer is released.
					deadline := time.Now().Add(100 * time.Millisecond)
					for metrics.QueryMemoryBudgetExceeded.Get() <= start && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
				})
				time.Sleep(25 * time.Millisecond) // a slow consumer: the other workers' next blocks arrive while this one is consumed
			})
			if err != nil {
				t.Fatalf("RunQuery: %v, the budget must make workers wait, not fail the read", err)
			}
			if want := rcFiles * rcRowsPerFile; rows != want {
				t.Fatalf("rows = %d, want %d: the budget dropped blocks", rows, want)
			}
			if metrics.QueryMemoryBudgetExceeded.Get() <= start {
				t.Fatal("the budget never tripped: the test does not exercise it")
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
func TestRowRead_LiveBytesBudget_BoundsHeapAndKeepsEveryRow(t *testing.T) {
	if testing.Short() {
		t.Skip("memory measurement: seconds, real S3 code paths")
	}
	fs := newFaultS3(t)
	s := testStorageWithS3(t, fs.srv.URL)
	s.cfg.Query.FileWorkers = 16
	s.cfg.Query.MaxLiveBytes = 256 * 1024
	const files, rowsPerFile = 40, 5000
	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	for i := 0; i < files; i++ {
		rows := make([]logRow, rowsPerFile)
		for j := range rows {
			rows[j] = logRow{
				TimestampUnixNano: now.Add(time.Duration(i*rowsPerFile+j) * time.Microsecond).UnixNano(),
				Body:              fmt.Sprintf("file=%d row=%d a body of realistic length so a block weighs something: %s", i, j, strings.Repeat("x", 120)),
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
	t.Logf("MEASURED files=%d rows/file=%d workers=16 ceiling=256KiB: rows=%d/%d err=%v budget_waits=%d peak_heap_growth=%.1f MiB max_block=%.2f MiB elapsed=%s",
		files, rowsPerFile, rows.Load(), files*rowsPerFile, err, metrics.QueryMemoryBudgetExceeded.Get()-waits,
		float64(peak.Load())/(1<<20), float64(maxBlock.Load())/(1<<20), time.Since(started).Round(time.Millisecond))
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	if rows.Load() != files*rowsPerFile {
		t.Fatalf("rows = %d, want %d: the budget dropped rows", rows.Load(), files*rowsPerFile)
	}
	if peak.Load() > 384<<20 {
		t.Fatalf("peak heap growth %.1f MiB exceeds the 384 MiB production ceiling of the memory-budget tests", float64(peak.Load())/(1<<20))
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

// --- liveBytesGate --------------------------------------------------------

func TestLiveBytesGate_WaitsForRelease(t *testing.T) {
	var g liveBytesGate
	var logged atomic.Bool
	g.add(100)
	done := make(chan bool)
	go func() { done <- g.waitBelow(context.Background(), 100, &logged) }()
	select {
	case <-done:
		t.Fatal("waitBelow returned while the bytes in flight reached the ceiling")
	case <-time.After(50 * time.Millisecond):
	}
	g.release(100)
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("waitBelow gave up although room was made")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waitBelow was not woken by the release")
	}
}

func TestLiveBytesGate_ContextEndReleasesWaiter(t *testing.T) {
	var g liveBytesGate
	var logged atomic.Bool
	g.add(100)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() { done <- g.waitBelow(ctx, 10, &logged) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("waitBelow reported room after the context ended")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled query stayed blocked at the budget")
	}
}

// A block larger than the ceiling is admitted as soon as nothing else is in
// flight: no block can wait for room that never comes.
func TestLiveBytesGate_OversizeBlockAdmittedWhenIdle(t *testing.T) {
	var g liveBytesGate
	var logged atomic.Bool
	if !g.waitBelow(context.Background(), 10, &logged) {
		t.Fatal("an idle gate refused a block")
	}
	g.add(1 << 20) // larger than the ceiling
	g.release(1 << 20)
	if !g.waitBelow(context.Background(), 10, &logged) {
		t.Fatal("the gate stayed closed after the oversize block was released")
	}
}

func TestLiveBytesGate_ConcurrentWorkersAllFinish(t *testing.T) {
	var g liveBytesGate
	var logged atomic.Bool
	const workers, blocks, size, ceiling = 8, 200, 40, 100
	var inFlight, peak atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := 0; b < blocks; b++ {
				if !g.waitBelow(context.Background(), ceiling, &logged) {
					t.Error("waitBelow gave up on a live context")
					return
				}
				g.add(size)
				cur := inFlight.Add(size)
				for {
					p := peak.Load()
					if cur <= p || peak.CompareAndSwap(p, cur) {
						break
					}
				}
				inFlight.Add(-size)
				g.release(size)
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
		t.Fatalf("live = %d after every block was released", g.live.Load())
	}
	// Bounded by the ceiling plus one block per worker (the check and the add are not atomic).
	if limit := int64(ceiling + workers*size); peak.Load() > limit {
		t.Fatalf("peak in flight %d exceeds ceiling + one block per worker (%d)", peak.Load(), limit)
	}
}
