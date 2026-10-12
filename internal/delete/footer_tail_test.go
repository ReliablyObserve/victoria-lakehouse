package delete

import (
	"context"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/parquetfooter"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestRewriteFile_RecordsFooterTail: a delete rewrite records, in the result and
// the manifest entry of the replacement, what the replacement's footer says: the
// tail length a footer read needs, the bloom bytes and the per-column bytes, all
// from one parse. Both signals.
func TestRewriteFile_RecordsFooterTail(t *testing.T) {
	for _, mode := range []string{"logs", "traces"} {
		t.Run(mode, func(t *testing.T) {
			pool := newMockRewriterPool()
			key := mode + "/dt=2026-01-15/hour=10/00001.parquet"
			var data []byte
			if mode == "logs" {
				data = buildTestParquet(t, []schema.LogRow{
					{TimestampUnixNano: 1000, Body: "a", ServiceName: "drop"},
					{TimestampUnixNano: 2000, Body: "b", ServiceName: "keep"},
					{TimestampUnixNano: 3000, Body: "c", ServiceName: "keep"},
				})
			} else {
				data = buildTestTraceParquet(t, []schema.TraceRow{
					{TimestampUnixNano: 1000, TraceID: "0af7651916cd43dd8448eb211c80319c", SpanID: "s1", ServiceName: "drop"},
					{TimestampUnixNano: 2000, TraceID: "0af7651916cd43dd8448eb211c80319d", SpanID: "s2", ServiceName: "keep"},
					{TimestampUnixNano: 3000, TraceID: "0af7651916cd43dd8448eb211c80319e", SpanID: "s3", ServiceName: "keep"},
				})
			}
			pool.Put(key, data)
			rw := NewRewriter(pool, mode+"/", 100, mode)
			res, err := rw.RewriteFile(context.Background(), key, []Tombstone{
				{Tenants: []TenantRef{{}}, ID: "t", Query: `service.name:="drop"`, StartNs: 0, EndNs: 10000},
			})
			if err != nil {
				t.Fatalf("RewriteFile: %v", err)
			}
			want := parquetfooter.Inspect(mustGet(t, pool, res.NewKey))
			if want.Tail <= 0 || res.FooterTail != want.Tail || res.BloomBytes != want.BloomBytes {
				t.Fatalf("result: FooterTail=%d BloomBytes=%d, the replacement's footer says %d and %d", res.FooterTail, res.BloomBytes, want.Tail, want.BloomBytes)
			}
			if fi := rewrittenFileInfo(manifest.FileInfo{Key: key}, res); fi.FooterTail != want.Tail {
				t.Fatalf("manifest entry FooterTail=%d, want %d", fi.FooterTail, want.Tail)
			}
		})
	}
}
