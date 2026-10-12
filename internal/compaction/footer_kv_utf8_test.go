package compaction

import (
	"context"
	"fmt"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/traceindex"
)

func footerKVLogRows(n int, base int64) []schema.LogRow {
	rows := make([]schema.LogRow, n)
	for i := range rows {
		rows[i] = schema.LogRow{TimestampUnixNano: base + int64(i), Body: fmt.Sprintf("line %d from handler", i), ServiceName: "svc", SeverityText: "info"}
	}
	return rows
}

func footerKVTraceRows(n int, base int64) []schema.TraceRow {
	rows := make([]schema.TraceRow, n)
	for i := range rows {
		rows[i] = schema.TraceRow{
			TimestampUnixNano: base + int64(i), TraceID: fmt.Sprintf("%032x", base+int64(i%4)), SpanID: fmt.Sprintf("%016x", i),
			SpanName: "op", ServiceName: "svc", Body: "span body",
			StartTimeUnixNano: schema.Int64Ptr(base + int64(i)), DurationNs: schema.Int64Ptr(100),
		}
	}
	return rows
}

// Compaction outputs (logs and traces) and the delete rewriter's replacements
// keep every footer key-value entry valid UTF-8 (issue #340): Arrow-rs readers
// refuse a file with a raw-bytes entry, and a compaction or rewrite must not
// turn a readable flushed file into an unreadable one.
func TestCompactedAndRewrittenFooterKVAllUTF8(t *testing.T) {
	const partition = "dt=2026-05-04/hour=10"
	ctx := context.Background()

	t.Run("compactor merge traces", func(t *testing.T) {
		pool := newMockPool()
		m := manifest.New("b", "traces/")
		var infos []manifest.FileInfo
		for i, rows := range [][]schema.TraceRow{footerKVTraceRows(6, 1000), footerKVTraceRows(6, 5000)} {
			data := makeTestTraceParquet(t, rows)
			key := fmt.Sprintf("traces/%s/src-%d.parquet", partition, i)
			if err := pool.Upload(ctx, key, data); err != nil {
				t.Fatal(err)
			}
			fi := manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: int64(len(rows)), MinTimeNs: 1000, MaxTimeNs: 6000, SchemaFingerprint: "fp"}
			m.AddFile(partition, fi)
			infos = append(infos, fi)
		}
		c := NewCompactor(CompactorConfig{Pool: pool, Manifest: m, Prefix: "traces/", Mode: config.ModeTraces, RowGroupSize: 100, CompressionLevel: 3})
		res, err := c.Compact(ctx, partition, infos, 0)
		if err != nil {
			t.Fatal(err)
		}
		kv := testutil.RequireFooterKVUTF8(t, pool.get(res.OutputFile))
		if _, ok := traceindex.FromMetadata(kv); !ok {
			t.Fatal("compacted traces object lost a decodable _trace_idx")
		}
	})

	t.Run("compactor merge logs", func(t *testing.T) {
		pool := newMockPool()
		m := manifest.New("b", "logs/")
		var infos []manifest.FileInfo
		for i, rows := range [][]schema.LogRow{footerKVLogRows(6, 1000), footerKVLogRows(6, 5000)} {
			data := makeTestParquet(t, rows)
			key := fmt.Sprintf("logs/%s/src-%d.parquet", partition, i)
			if err := pool.Upload(ctx, key, data); err != nil {
				t.Fatal(err)
			}
			fi := manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: int64(len(rows)), MinTimeNs: 1000, MaxTimeNs: 6000, SchemaFingerprint: "fp"}
			m.AddFile(partition, fi)
			infos = append(infos, fi)
		}
		c := NewCompactor(CompactorConfig{Pool: pool, Manifest: m, Prefix: "logs/", Mode: config.ModeLogs, RowGroupSize: 100, CompressionLevel: 3})
		res, err := c.Compact(ctx, partition, infos, 0)
		if err != nil {
			t.Fatal(err)
		}
		testutil.RequireFooterKVUTF8(t, pool.get(res.OutputFile))
	})

	t.Run("exported writers", func(t *testing.T) {
		data, err := WriteLogs(footerKVLogRows(10, 1000), 100, 3)
		if err != nil {
			t.Fatal(err)
		}
		testutil.RequireFooterKVUTF8(t, data)
		data, err = WriteTraces(footerKVTraceRows(10, 1000), 100, 3)
		if err != nil {
			t.Fatal(err)
		}
		kv := testutil.RequireFooterKVUTF8(t, data)
		if _, ok := traceindex.FromMetadata(kv); !ok {
			t.Fatal("WriteTraces output lost a decodable _trace_idx")
		}
	})

	// The delete rewriter wired like production (compaction.WriteLogs/WriteTraces).
	t.Run("delete rewriter traces", func(t *testing.T) {
		pool := newMockPool()
		rows := footerKVTraceRows(8, 1000)
		rows[0].ServiceName = "victim"
		key := "traces/" + partition + "/00001.parquet"
		if err := pool.Upload(ctx, key, makeTestTraceParquet(t, rows)); err != nil {
			t.Fatal(err)
		}
		rw := delete.NewRewriter(pool, "traces/", 100, "traces", delete.WithParquetWriters(delete.ParquetWriters{Logs: WriteLogs, Traces: WriteTraces, CompressionLevel: 3}))
		res, err := rw.RewriteFile(ctx, key, []delete.Tombstone{{Tenants: []delete.TenantRef{{}}, ID: "t", Query: `service.name:="victim"`, StartNs: 0, EndNs: 100000}})
		if err != nil {
			t.Fatal(err)
		}
		if res.RowsRemoved != 1 {
			t.Fatalf("removed %d rows, want 1", res.RowsRemoved)
		}
		kv := testutil.RequireFooterKVUTF8(t, pool.get(res.NewKey))
		entries, ok := traceindex.FromMetadata(kv)
		if !ok || len(entries) == 0 {
			t.Fatal("rewritten traces object lost a decodable _trace_idx")
		}
	})

	t.Run("delete rewriter logs", func(t *testing.T) {
		pool := newMockPool()
		rows := footerKVLogRows(8, 1000)
		rows[0].SeverityText = "victim"
		key := "logs/" + partition + "/00001.parquet"
		if err := pool.Upload(ctx, key, makeTestParquet(t, rows)); err != nil {
			t.Fatal(err)
		}
		rw := delete.NewRewriter(pool, "logs/", 100, "logs", delete.WithParquetWriters(delete.ParquetWriters{Logs: WriteLogs, Traces: WriteTraces, CompressionLevel: 3}))
		res, err := rw.RewriteFile(ctx, key, []delete.Tombstone{{Tenants: []delete.TenantRef{{}}, ID: "t", Query: `severity_text:="victim"`, StartNs: 0, EndNs: 100000}})
		if err != nil {
			t.Fatal(err)
		}
		if res.RowsRemoved != 1 {
			t.Fatalf("removed %d rows, want 1", res.RowsRemoved)
		}
		testutil.RequireFooterKVUTF8(t, pool.get(res.NewKey))
	})
}
