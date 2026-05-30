package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/parquet-go/parquet-go"
)

// instrumentedS3Server tracks every served request's byte count so tests
// can lock in a hard upper bound on S3 traffic for endpoints that should
// only read parquet footers (~16 KB per file) instead of full file bodies.
type instrumentedS3Server struct {
	mu          sync.RWMutex
	files       map[string][]byte
	srv         *httptest.Server
	bytesServed atomic.Int64
	rangeReqs   atomic.Int64
	fullReqs    atomic.Int64
}

func newInstrumentedS3Server() *instrumentedS3Server {
	m := &instrumentedS3Server{files: make(map[string][]byte)}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handler))
	return m
}

func (m *instrumentedS3Server) putFile(key string, data []byte) {
	m.mu.Lock()
	m.files[key] = data
	m.mu.Unlock()
}

func (m *instrumentedS3Server) handler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		if r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}
	key := parts[1]
	if r.Method == http.MethodPut {
		data, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		m.putFile(key, data)
		w.WriteHeader(http.StatusOK)
		return
	}
	m.mu.RLock()
	data, ok := m.files[key]
	m.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	rangeHdr := r.Header.Get("Range")
	if strings.HasPrefix(rangeHdr, "bytes=") {
		bounds := strings.SplitN(strings.TrimPrefix(rangeHdr, "bytes="), "-", 2)
		start, _ := strconv.ParseInt(bounds[0], 10, 64)
		end, _ := strconv.ParseInt(bounds[1], 10, 64)
		if start >= int64(len(data)) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(int(end-start+1)))
		w.WriteHeader(http.StatusPartialContent)
		n, _ := w.Write(data[start : end+1])
		m.bytesServed.Add(int64(n))
		m.rangeReqs.Add(1)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	n, _ := w.Write(data)
	m.bytesServed.Add(int64(n))
	m.fullReqs.Add(1)
}

func (m *instrumentedS3Server) close()      { m.srv.Close() }
func (m *instrumentedS3Server) url() string { return m.srv.URL }

// makeLargeParquet generates a Parquet file at least minBytes long by
// appending unique-content rows (so ZSTD cannot compress them away).
// Used to make footer-only reads visibly cheaper than full-file reads
// in regression tests.
func makeLargeParquet(t *testing.T, baseTime time.Time, minBytes int) []byte {
	t.Helper()
	rows := make([]logRow, 0, 4096)
	i := 0
	for {
		// Use a unique body per row to defeat ZSTD compression and
		// keep the file growing predictably.
		rows = append(rows, logRow{
			TimestampUnixNano: baseTime.Add(time.Duration(i) * time.Microsecond).UnixNano(),
			Body:              fmt.Sprintf("row-%d-payload-%x-%x-%x-%x", i, i*2654435761, i*1442695040, i*8675309, i*0xdeadbeef),
			SeverityText:      []string{"INFO", "WARN", "ERROR", "DEBUG"}[i%4],
			ServiceName:       fmt.Sprintf("service-%d", i%32),
		})
		i++
		if i%200 == 0 {
			data := writeParquetToBytes(t, rows)
			if len(data) >= minBytes {
				return data
			}
		}
		if i > 200000 {
			return writeParquetToBytes(t, rows) // safety stop
		}
	}
}

// TestGetFieldNames_ServesOnlyFooterBytes locks in the regression guard
// that `GetFieldNames` reads only the parquet footers (~16 KB per file)
// instead of downloading every full file body.
//
// Before this guard, GetFieldNames called `s.getFileData(...)` for every
// file in the time range — a 612-file manifest at ~1 MB average meant
// ~600 MB of sequential S3 downloads per `/select/logsql/field_names`
// call, which caused OOM kills on lakehouse-logs under Grafana load.
//
// We assert against an instrumented mock S3 server that the bytes served
// during one GetFieldNames invocation are at most
// `5 * footerPrefetchSize + slack`, and that zero full-file (no-Range)
// downloads are issued.
func TestGetFieldNames_ServesOnlyFooterBytes(t *testing.T) {
	mock := newInstrumentedS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	const fileBytes = 200 * 1024
	data := makeLargeParquet(t, baseTime, fileBytes)

	const numFiles = 5
	for i := 0; i < numFiles; i++ {
		key := fmt.Sprintf("logs/dt=2026-05-28/hour=%02d/file%d.parquet", 10+i, i)
		mock.putFile(key, data)
		s.manifest.AddFile(fmt.Sprintf("dt=2026-05-28/hour=%02d", 10+i), manifest.FileInfo{
			Key:       key,
			Size:      int64(len(data)),
			MinTimeNs: baseTime.Add(time.Duration(i)*time.Hour - time.Minute).UnixNano(),
			MaxTimeNs: baseTime.Add(time.Duration(i)*time.Hour + time.Minute).UnixNano(),
		})
	}

	q := mustParseQueryWithTime(t, `*`,
		baseTime.Add(-time.Hour).UnixNano(),
		baseTime.Add(10*time.Hour).UnixNano(),
	)

	fields, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatalf("GetFieldNames: %v", err)
	}
	if len(fields) == 0 {
		t.Fatal("expected non-empty field names")
	}

	served := mock.bytesServed.Load()
	rangeReqs := mock.rangeReqs.Load()
	fullReqs := mock.fullReqs.Load()
	maxAllowed := int64(numFiles*footerPrefetchSize) + 8192 // slack for HTTP

	t.Logf("served=%d bytes (range=%d, full=%d), %d files x %d bytes = %d full-download total",
		served, rangeReqs, fullReqs, numFiles, len(data), numFiles*len(data))

	if fullReqs > 0 {
		t.Errorf("GetFieldNames issued %d full-file downloads; expected only range reads (footer-only)", fullReqs)
	}
	if served > maxAllowed {
		t.Errorf("GetFieldNames served %d bytes; expected <= %d. Regression: not using footer-only path.", served, maxAllowed)
	}
}

// TestGetFieldNames_HitsRemainCorrect verifies that the footer-only
// optimization preserves the per-field hit count semantics. We check
// non-zero hits for the columns we wrote.
func TestGetFieldNames_HitsRemainCorrect(t *testing.T) {
	mock := newInstrumentedS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: baseTime.UnixNano(), Body: "a", SeverityText: "INFO", ServiceName: "svc-a"},
		{TimestampUnixNano: baseTime.Add(time.Second).UnixNano(), Body: "b", SeverityText: "ERROR", ServiceName: "svc-b"},
		{TimestampUnixNano: baseTime.Add(2 * time.Second).UnixNano(), Body: "c", SeverityText: "WARN", ServiceName: "svc-a"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-28/hour=10/test.parquet"
	mock.putFile(key, data)
	s.manifest.AddFile("dt=2026-05-28/hour=10", manifest.FileInfo{
		Key:       key,
		Size:      int64(len(data)),
		MinTimeNs: baseTime.Add(-time.Minute).UnixNano(),
		MaxTimeNs: baseTime.Add(time.Minute).UnixNano(),
	})

	q := mustParseQueryWithTime(t, `*`,
		baseTime.Add(-time.Hour).UnixNano(),
		baseTime.Add(time.Hour).UnixNano(),
	)

	fields, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatalf("GetFieldNames: %v", err)
	}

	want := map[string]uint64{
		"_time":        3,
		"_msg":         3,
		"level":        3,
		"service.name": 3,
	}
	got := make(map[string]uint64)
	for _, f := range fields {
		got[f.Value] = f.Hits
	}
	for name, wantHits := range want {
		if got[name] == 0 {
			t.Errorf("field %q: got Hits=0, want >= %d (footer-only path must preserve hit counts)", name, wantHits)
		}
	}
}

// writeFullLogParquetToBytes writes fullLogRow records to an in-memory
// parquet buffer. The 8-column schema is needed for tests that require
// projecting fewer than half the columns (so shouldUseRangeRead returns
// true and the range-read path actually engages).
func writeFullLogParquetToBytes(t *testing.T, rows []fullLogRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[fullLogRow](&buf, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// makeLargeFullLogParquet generates an 8-column logs Parquet file at
// least minBytes long, padding with unique-content rows so ZSTD cannot
// compress it down to below the prefetch threshold.
func makeLargeFullLogParquet(t *testing.T, baseTime time.Time, minBytes int) []byte {
	t.Helper()
	rows := make([]fullLogRow, 0, 4096)
	i := 0
	for {
		rows = append(rows, fullLogRow{
			TimestampUnixNano: baseTime.Add(time.Duration(i) * time.Microsecond).UnixNano(),
			Body:              fmt.Sprintf("row-%d-payload-%x-%x-%x-%x-%x", i, i*2654435761, i*1442695040, i*8675309, i*0xdeadbeef, i*0xbadf00d),
			SeverityText:      []string{"INFO", "WARN", "ERROR", "DEBUG"}[i%4],
			ServiceName:       fmt.Sprintf("service-%d", i%32),
			Stream:            fmt.Sprintf(`{svc="service-%d"}`, i%32),
			StreamID:          fmt.Sprintf("sid-%x", i),
			TraceID:           fmt.Sprintf("trace-%016x", i),
			SpanID:            fmt.Sprintf("span-%016x", i),
		})
		i++
		if i%200 == 0 {
			data := writeFullLogParquetToBytes(t, rows)
			if len(data) >= minBytes {
				return data
			}
		}
		if i > 200000 {
			return writeFullLogParquetToBytes(t, rows) // safety stop
		}
	}
}

// TestGetFieldValues_UsesColumnProjectedRead locks in the regression
// guard that GetFieldValues fetches only (target column + filter
// columns) bytes from S3 — not every column in the file.
//
// Before this guard, GetFieldValues called `s.getFileData(fi)` per
// file, downloading the full body (~hundreds of KB) just to scan a
// single column. Under Grafana drilldown load — where field_values
// is called repeatedly with various filters — that compounded into
// hundreds of MB of redundant S3 traffic per panel render.
//
// We assert against an instrumented mock S3 server that bytes served
// stay well under what a full-download path would issue (50% of total
// file bytes), with all reads being range reads (no full GETs).
func TestGetFieldValues_UsesColumnProjectedRead(t *testing.T) {
	mock := newInstrumentedS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	const fileBytes = 400 * 1024
	// 8-column schema so projecting 2 columns yields a 25% ratio
	// (below the 50% threshold) and the range-read path engages.
	data := makeLargeFullLogParquet(t, baseTime, fileBytes)

	const numFiles = 5
	for i := 0; i < numFiles; i++ {
		key := fmt.Sprintf("logs/dt=2026-05-28/hour=%02d/file%d.parquet", 10+i, i)
		mock.putFile(key, data)
		s.manifest.AddFile(fmt.Sprintf("dt=2026-05-28/hour=%02d", 10+i), manifest.FileInfo{
			Key:       key,
			Size:      int64(len(data)),
			MinTimeNs: baseTime.Add(time.Duration(i)*time.Hour - time.Minute).UnixNano(),
			MaxTimeNs: baseTime.Add(time.Duration(i)*time.Hour + time.Minute).UnixNano(),
		})
	}

	// Filter on `level` bypasses the label-index fast path (which
	// only fires when filter == nil), forcing the per-file scan that
	// is the actual production hot path under Grafana drilldown load.
	q := mustParseQueryWithTime(t, `level:="INFO"`,
		baseTime.Add(-time.Hour).UnixNano(),
		baseTime.Add(10*time.Hour).UnixNano(),
	)

	vals, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	if len(vals) == 0 {
		t.Fatal("expected non-empty values")
	}

	served := mock.bytesServed.Load()
	rangeReqs := mock.rangeReqs.Load()
	fullReqs := mock.fullReqs.Load()
	totalFileBytes := int64(numFiles) * int64(len(data))
	maxAllowed := totalFileBytes / 2 // <50% of full-download path

	t.Logf("served=%d bytes (range=%d, full=%d), %d files x %d bytes each = %d total. Max allowed: %d (50%% of full).",
		served, rangeReqs, fullReqs, numFiles, len(data), totalFileBytes, maxAllowed)

	if fullReqs > 0 {
		t.Errorf("GetFieldValues issued %d full-file downloads; expected only range reads (column-projected)", fullReqs)
	}
	if served > maxAllowed {
		t.Errorf("GetFieldValues served %d bytes; expected <= %d (50%% of full-download). "+
			"Regression: not using column-projected reads.", served, maxAllowed)
	}
}

// ensure parquet import is used
var _ = parquet.Compression(&parquet.Zstd)
