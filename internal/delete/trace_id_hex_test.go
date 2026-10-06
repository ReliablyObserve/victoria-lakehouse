package delete

import (
	"bytes"
	"context"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// footerKV returns the lh.trace_id_hex footer value of a Parquet object.
func footerKV(t *testing.T, data []byte) (string, bool) {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	return f.Lookup(schema.TraceIDHexMetaKey)
}

// TestRewriteFile_TraceIDHexFollowsKeptRows: a delete rewrite records
// lh.trace_id_hex for the rows it keeps (both signals, the fallback writer):
// dropping the only non-hex row makes the replacement attested, dropping the
// hex row leaves it unattested. The result (and so the manifest entry) carries
// what the footer says.
func TestRewriteFile_TraceIDHexFollowsKeptRows(t *testing.T) {
	for _, mode := range []string{"logs", "traces"} {
		for _, tc := range []struct {
			name, query string
			want        bool
		}{
			{"drop_non_hex", `service.name:="nonhex"`, true},
			{"drop_hex", `service.name:="hex"`, false},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				pool := newMockRewriterPool()
				key := mode + "/dt=2026-01-15/hour=10/00001.parquet"
				var data []byte
				if mode == "logs" {
					data = buildTestParquet(t, []schema.LogRow{
						{TimestampUnixNano: 1000, Body: "a", ServiceName: "hex", TraceID: "0af7651916cd43dd8448eb211c80319c"},
						{TimestampUnixNano: 2000, Body: "b", ServiceName: "nonhex", TraceID: "4bf92f35-77b3-4da6-a3ce-929d0e0bf736"},
						{TimestampUnixNano: 3000, Body: "c", ServiceName: "keep"},
					})
				} else {
					data = buildTestTraceParquet(t, []schema.TraceRow{
						{TimestampUnixNano: 1000, TraceID: "0af7651916cd43dd8448eb211c80319c", SpanID: "s1", ServiceName: "hex"},
						{TimestampUnixNano: 2000, TraceID: "abc-def-ghi", SpanID: "s2", ServiceName: "nonhex"},
						{TimestampUnixNano: 3000, TraceID: "", SpanID: "s3", ServiceName: "keep"},
					})
				}
				pool.Put(key, data)
				rw := NewRewriter(pool, mode+"/", 100, mode)
				res, err := rw.RewriteFile(context.Background(), key, []Tombstone{
					{Tenants: []TenantRef{{}}, ID: "t", Query: tc.query, StartNs: 0, EndNs: 10000},
				})
				if err != nil {
					t.Fatalf("RewriteFile: %v", err)
				}
				kv, ok := footerKV(t, mustGet(t, pool, res.NewKey))
				if !ok || (kv != "0" && kv != "1") {
					t.Fatalf("replacement footer %s = %q (present=%v), want ASCII 0 or 1", schema.TraceIDHexMetaKey, kv, ok)
				}
				if (kv == "1") != tc.want || res.TraceIDHex != tc.want {
					t.Fatalf("replacement attests footer=%q result=%v, want %v", kv, res.TraceIDHex, tc.want)
				}
				fi := rewrittenFileInfo(manifest.FileInfo{Key: key}, res)
				if fi.TraceIDHex != tc.want {
					t.Fatalf("manifest entry TraceIDHex=%v, want %v", fi.TraceIDHex, tc.want)
				}
			})
		}
	}
}
