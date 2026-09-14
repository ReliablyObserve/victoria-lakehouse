package main

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	lhdelete "github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/traceindex"
)

// memPool is the smallest RewriterPool: an in-memory bucket.
type memPool struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (p *memPool) Upload(_ context.Context, key string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.objects[key] = append([]byte(nil), data...)
	return nil
}

func (p *memPool) Download(_ context.Context, key string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	d, ok := p.objects[key]
	if !ok {
		return nil, context.DeadlineExceeded
	}
	return d, nil
}

func (p *memPool) Delete(_ context.Context, key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.objects, key)
	return nil
}

// TestNewDeleteRewriter_ReplacementKeepsBloomsAndTraceIndex pins the traces
// binary's wiring: a span file rewritten to remove deleted spans must keep the
// SBBF column blooms AND the `_trace_idx` footer index — recomputed for the
// spans that survive, never copied, so the index neither loses the kept traces
// (trace-by-ID would fall back to a full scan) nor keeps advertising the
// deleted ones.
func TestNewDeleteRewriter_ReplacementKeepsBloomsAndTraceIndex(t *testing.T) {
	cfg := config.Default()
	cfg.Mode = config.ModeTraces

	pool := &memPool{objects: map[string][]byte{}}
	rw := newDeleteRewriter(pool, cfg, "traces")
	if !rw.HasProductionWriters() {
		t.Fatal("the binary's rewriter must be built with the compactor's writers")
	}

	spans := []schema.TraceRow{
		{TimestampUnixNano: 1000, TraceID: "keep-trace", SpanID: "s1", SpanName: "GET /a", ServiceName: "api"},
		{TimestampUnixNano: 2000, TraceID: "drop-trace", SpanID: "s2", SpanName: "POST /b", ServiceName: "leaky"},
	}
	var src bytes.Buffer
	w := parquet.NewGenericWriter[schema.TraceRow](&src)
	if _, err := w.Write(spans); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}
	key := "traces/dt=2026-09-01/hour=00/src.parquet"
	pool.objects[key] = src.Bytes()

	res, err := rw.RewriteFile(context.Background(), key, []lhdelete.Tombstone{
		{ID: "t", Query: `service.name:="leaky"`, StartNs: 0, EndNs: 9000},
	})
	if err != nil {
		t.Fatalf("RewriteFile: %v", err)
	}
	if res.RowsKept != 1 {
		t.Fatalf("kept %d spans, want 1", res.RowsKept)
	}
	if res.BloomBytes <= 0 {
		t.Fatalf("replacement carries %d bloom bytes; it must keep the SBBF column blooms", res.BloomBytes)
	}

	data := pool.objects[res.NewKey]
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open replacement: %v", err)
	}
	var idx string
	for _, kv := range f.Metadata().KeyValueMetadata {
		if kv.Key == traceindex.MetadataKey {
			idx = kv.Value
		}
	}
	if idx == "" {
		t.Fatal("replacement has no _trace_idx footer index; trace-by-ID lookups lose their fast path")
	}
	entries, err := traceindex.Unmarshal([]byte(idx))
	if err != nil {
		t.Fatalf("replacement _trace_idx does not decode: %v", err)
	}
	seen := map[string]bool{}
	for _, e := range entries {
		seen[e.TraceID] = true
	}
	if !seen["keep-trace"] {
		t.Errorf("_trace_idx lost the kept trace: %v", seen)
	}
	if seen["drop-trace"] {
		t.Errorf("_trace_idx still advertises the deleted trace: %v", seen)
	}
}
