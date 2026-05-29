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

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
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
func makeLargeParquet(t *testing.T, baseTime time.Time, minBytes int) []byte {
	t.Helper()
	rows := make([]logRow, 0, 4096)
	i := 0
	for {
		rows = append(rows, logRow{
			TimestampUnixNano: baseTime.Add(time.Duration(i) * time.Microsecond).UnixNano(),
			Body:              fmt.Sprintf("row-%d-payload-%x-%x-%x-%x", i, i*2654435761, i*1442695040, i*8675309, i*0xdeadbeef),
			SeverityText:      []string{"INFO", "WARN", "ERROR", "DEBUG"}[i%4],
			ServiceName:       fmt.Sprintf("service-%d", i%32),
		})
		i++
		if i%200 == 0 {
			data := writeParquetToBytesLocal(t, rows)
			if len(data) >= minBytes {
				return data
			}
		}
		if i > 200000 {
			return writeParquetToBytesLocal(t, rows) // safety stop
		}
	}
}

// writeParquetToBytesLocal writes the logRow records to an in-memory
// parquet buffer. Suffixed "Local" because the traces module's existing
// writeParquetToBytes helper has a different signature.
func writeParquetToBytesLocal(t *testing.T, rows []logRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[logRow](&buf, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestGetFieldNames_ServesOnlyFooterBytes_Traces locks in the regression
// guard that traces' `GetFieldNames` reads only the parquet footer
// (~16 KB) instead of downloading the full file body.
//
// Before this guard, GetFieldNames called `s.getFileData(files[0])` —
// for a 1 MB file that meant a full 1 MB S3 download just to read the
// schema. Mirrors the equivalent guard added to the logs module so the
// two signals stay aligned on this behaviour.
func TestGetFieldNames_ServesOnlyFooterBytes_Traces(t *testing.T) {
	mock := newInstrumentedS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	baseTime := time.Date(2026, 5, 28, 10, 0, 0, 0, time.UTC)
	const fileBytes = 200 * 1024
	data := makeLargeParquet(t, baseTime, fileBytes)
	if len(data) < fileBytes {
		t.Fatalf("generated parquet too small: got %d, want >= %d", len(data), fileBytes)
	}

	key := "traces/dt=2026-05-28/hour=10/file0.parquet"
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
	if len(fields) == 0 {
		t.Fatal("expected non-empty field names")
	}

	served := mock.bytesServed.Load()
	rangeReqs := mock.rangeReqs.Load()
	fullReqs := mock.fullReqs.Load()
	maxAllowed := int64(footerPrefetchSize) + 4096 // single file footer + slack

	t.Logf("served=%d bytes (range=%d, full=%d), file_size=%d",
		served, rangeReqs, fullReqs, len(data))

	if fullReqs > 0 {
		t.Errorf("GetFieldNames issued %d full-file downloads; expected only range reads (footer-only)", fullReqs)
	}
	if served > maxAllowed {
		t.Errorf("GetFieldNames served %d bytes; expected <= %d. Regression: not using footer-only path.", served, maxAllowed)
	}
}
