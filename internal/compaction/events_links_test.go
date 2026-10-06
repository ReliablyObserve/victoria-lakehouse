package compaction

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// legacyTraceRow is the span row shape of a file written before the span
// events and links columns existed (the columns simply are not there).
type legacyTraceRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano,delta"`
	TraceID           string `parquet:"trace_id"`
	SpanID            string `parquet:"span_id"`
	SpanName          string `parquet:"span.name,dict"`
	ServiceName       string `parquet:"service.name,dict"`
}

// Compacting a file written before the columns existed together with files
// that carry events and links keeps every event and link, and gives the old
// spans none: a missing column is empty, never an error.
func TestCompactor_KeepsEventsLinksAndMergesWithOldFiles(t *testing.T) {
	s := setupTraceCompactor(t)

	var oldRows []legacyTraceRow
	for i := 0; i < 4; i++ {
		oldRows = append(oldRows, legacyTraceRow{int64(1000 + i), fmt.Sprintf("old-%d", i), fmt.Sprintf("o%d", i), "old-op", "svc"})
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[legacyTraceRow](&buf, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(oldRows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	oldKey := fmt.Sprintf("traces/%s/batch-000.parquet", s.partition)
	if err := s.pool.Upload(context.Background(), oldKey, buf.Bytes()); err != nil {
		t.Fatal(err)
	}
	oldFi := manifest.FileInfo{Key: oldKey, Size: int64(buf.Len()), RowCount: 4, MinTimeNs: 1000, MaxTimeNs: 1003, SchemaFingerprint: "fp-traces-v1"}
	s.manifest.AddFile(s.partition, oldFi)

	var newRows []schema.TraceRow
	want := map[string]string{}
	for i := 0; i < 6; i++ {
		var c schema.SpanSubFieldCollector
		r := schema.TraceRow{TimestampUnixNano: int64(2000 + i), TraceID: fmt.Sprintf("new-%d", i), SpanID: fmt.Sprintf("n%d", i), SpanName: "new-op", ServiceName: "svc"}
		if i%2 == 0 {
			c.Add("event:event_name:0", "exception")
			c.Add("event:event_attr:exception.type:0", "IOError")
			c.Add("link:link_span_id:0", fmt.Sprintf("%016x", i))
			r.ScopeAttributes = map[string]string{"k": fmt.Sprintf("v%d", i)}
		}
		c.Apply(&r)
		newRows = append(newRows, r)
		want[r.SpanID] = r.EventsJSON + "|" + r.LinksJSON
	}
	fi2 := s.addTraceFile(t, 1, newRows[:3])
	fi3 := s.addTraceFile(t, 2, newRows[3:])
	// addTraceFile stamps its own fingerprint; the old file must match it.
	oldFi.SchemaFingerprint = fi2.SchemaFingerprint

	res, err := s.compactor.Compact(context.Background(), s.partition, []manifest.FileInfo{oldFi, fi2, fi3}, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	s.pool.mu.Lock()
	out := s.pool.uploaded[res.OutputFile]
	s.pool.mu.Unlock()
	rows, err := readTraceRows(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 10 {
		t.Fatalf("got %d rows, want 10", len(rows))
	}
	withEvents := 0
	for _, r := range rows {
		if exp, ok := want[r.SpanID]; ok {
			if got := r.EventsJSON + "|" + r.LinksJSON; got != exp {
				t.Errorf("span %s: events/links %q, want %q", r.SpanID, got, exp)
			}
			if r.EventsJSON != "" {
				withEvents++
				if len(r.ScopeAttributes) != 1 {
					t.Errorf("span %s lost its scope attributes: %v", r.SpanID, r.ScopeAttributes)
				}
			}
			continue
		}
		if r.EventsJSON != "" || r.LinksJSON != "" {
			t.Errorf("old span %s gained events/links: %q %q", r.SpanID, r.EventsJSON, r.LinksJSON)
		}
	}
	if withEvents != 3 {
		t.Errorf("%d compacted spans carry events, want 3", withEvents)
	}

	// External readers see the columns as plain optional strings.
	f, err := parquet.OpenFile(bytes.NewReader(out), int64(len(out)))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{schema.ColSpanEventsJSON, schema.ColSpanLinksJSON} {
		col := f.Root().Column(name)
		if col == nil || !col.Leaf() || col.Type().Kind() != parquet.ByteArray || !col.Optional() {
			t.Errorf("column %s is not an optional BYTE_ARRAY leaf: %v", name, col)
		}
	}
}
