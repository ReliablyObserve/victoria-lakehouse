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

// TestNewDeleteRewriter_ReplacementKeepsTheInFileBlooms pins the binary's
// wiring: the rewriter this binary runs must write replacements with the
// compactor's format — SBBF column blooms included — not the minimal fallback
// writer. A delete that strips a file's blooms makes it unprunable for every
// external Parquet reader and for LH's own row-group pruning, for as long as
// the file lives.
func TestNewDeleteRewriter_ReplacementKeepsTheInFileBlooms(t *testing.T) {
	cfg := config.Default()
	cfg.Mode = config.ModeLogs

	pool := &memPool{objects: map[string][]byte{}}
	rw := newDeleteRewriter(pool, cfg, "logs")
	if !rw.HasProductionWriters() {
		t.Fatal("the binary's rewriter must be built with the compactor's writers")
	}

	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "keep", SeverityText: "info", ServiceName: "web", TraceID: "t1"},
		{TimestampUnixNano: 2000, Body: "drop", SeverityText: "error", ServiceName: "web", TraceID: "t2"},
	}
	var src bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&src)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}
	key := "logs/dt=2026-09-01/hour=00/src.parquet"
	pool.objects[key] = src.Bytes()

	res, err := rw.RewriteFile(context.Background(), key, []lhdelete.Tombstone{
		{ID: "t", Query: `severity_text:="error"`, StartNs: 0, EndNs: 9000},
	})
	if err != nil {
		t.Fatalf("RewriteFile: %v", err)
	}
	if res.RowsKept != 1 {
		t.Fatalf("kept %d rows, want 1", res.RowsKept)
	}
	if res.BloomBytes <= 0 {
		t.Fatalf("replacement carries %d bloom bytes; it must keep the SBBF column blooms", res.BloomBytes)
	}

	data := pool.objects[res.NewKey]
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open replacement: %v", err)
	}
	bloomed := map[string]bool{}
	for _, rg := range f.RowGroups() {
		for i, cc := range rg.ColumnChunks() {
			if cc.BloomFilter() != nil {
				bloomed[f.Schema().Columns()[i][0]] = true
			}
		}
	}
	for _, col := range []string{"service.name", "trace_id"} {
		if !bloomed[col] {
			t.Errorf("replacement has no bloom filter on %q (bloomed: %v)", col, bloomed)
		}
	}
}
