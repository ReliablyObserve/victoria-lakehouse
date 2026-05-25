package buffer

import (
	"net/http/httptest"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func bufForceGC() {
	runtime.GC()
	runtime.GC()
}

func bufHeapInUse() uint64 {
	var m runtime.MemStats
	bufForceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// fixedQuerier returns a fixed set of rows for testing.
type fixedQuerier struct {
	logRows   []schema.LogRow
	traceRows []schema.TraceRow
}

func (f *fixedQuerier) BufferedLogRows(startNs, endNs int64) []schema.LogRow {
	var result []schema.LogRow
	for _, r := range f.logRows {
		if r.TimestampUnixNano >= startNs && r.TimestampUnixNano < endNs {
			result = append(result, r)
		}
	}
	return result
}

func (f *fixedQuerier) BufferedTraceRows(startNs, endNs int64) []schema.TraceRow {
	var result []schema.TraceRow
	for _, r := range f.traceRows {
		if r.TimestampUnixNano >= startNs && r.TimestampUnixNano < endNs {
			result = append(result, r)
		}
	}
	return result
}

func TestMemLeak_Handler_ServeHTTPLogCycles(t *testing.T) {
	now := time.Now().UnixNano()
	store := &fixedQuerier{
		logRows: []schema.LogRow{
			{TimestampUnixNano: now, Body: "hello world", ServiceName: "svc-a"},
			{TimestampUnixNano: now + 1000, Body: "another log", ServiceName: "svc-b"},
		},
	}
	h := NewHandler(store, "")

	// Warm up
	for i := 0; i < 50; i++ {
		req := httptest.NewRequest("GET", "/?start="+strconv.FormatInt(now-1e9, 10)+"&end="+strconv.FormatInt(now+1e10, 10)+"&mode=logs", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
	}
	bufForceGC()

	before := bufHeapInUse()

	const iterations = 10000
	startStr := strconv.FormatInt(now-1e9, 10)
	endStr := strconv.FormatInt(now+1e10, 10)
	for i := 0; i < iterations; i++ {
		req := httptest.NewRequest("GET", "/?start="+startStr+"&end="+endStr+"&mode=logs", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
	}

	bufForceGC()
	after := bufHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d Handler.ServeHTTP log cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Handler_ServeHTTPTraceCycles(t *testing.T) {
	now := time.Now().UnixNano()
	store := &fixedQuerier{
		traceRows: []schema.TraceRow{
			{TimestampUnixNano: now, TraceID: "trace-abc", SpanName: "my-span"},
			{TimestampUnixNano: now + 1000, TraceID: "trace-def", SpanName: "other-span"},
		},
	}
	h := NewHandler(store, "")

	// Warm up
	for i := 0; i < 50; i++ {
		req := httptest.NewRequest("GET", "/?start="+strconv.FormatInt(now-1e9, 10)+"&end="+strconv.FormatInt(now+1e10, 10)+"&mode=traces", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
	}
	bufForceGC()

	before := bufHeapInUse()

	const iterations = 10000
	startStr := strconv.FormatInt(now-1e9, 10)
	endStr := strconv.FormatInt(now+1e10, 10)
	for i := 0; i < iterations; i++ {
		req := httptest.NewRequest("GET", "/?start="+startStr+"&end="+endStr+"&mode=traces", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
	}

	bufForceGC()
	after := bufHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(10 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d Handler.ServeHTTP trace cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Handler_ServeHTTPEmptyStore(t *testing.T) {
	store := &fixedQuerier{}
	h := NewHandler(store, "")

	// Warm up
	now := time.Now().UnixNano()
	for i := 0; i < 50; i++ {
		req := httptest.NewRequest("GET", "/?start=0&end="+strconv.FormatInt(now, 10)+"&mode=logs", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
	}
	bufForceGC()

	before := bufHeapInUse()

	const iterations = 20000
	endStr := strconv.FormatInt(now, 10)
	for i := 0; i < iterations; i++ {
		req := httptest.NewRequest("GET", "/?start=0&end="+endStr+"&mode=logs", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
	}

	bufForceGC()
	after := bufHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d empty-store ServeHTTP cycles (max %d)", growth, iterations, maxAllowed)
	}
}
