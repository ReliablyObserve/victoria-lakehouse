package parquets3

import (
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
	// Unique content per row defeats ZSTD, so the file grows predictably.
	return growParquetTo(t, minBytes, func(i int) logRow {
		return logRow{
			TimestampUnixNano: baseTime.Add(time.Duration(i) * time.Microsecond).UnixNano(),
			Body:              fmt.Sprintf("row-%d-payload-%x-%x-%x-%x", i, i*2654435761, i*1442695040, i*8675309, i*0xdeadbeef),
			SeverityText:      []string{"INFO", "WARN", "ERROR", "DEBUG"}[i%4],
			ServiceName:       fmt.Sprintf("service-%d", i%32),
		}
	}, parquet.Compression(&parquet.Zstd))
}
