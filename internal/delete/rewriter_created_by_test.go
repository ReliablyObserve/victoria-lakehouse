package delete

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestRewriteFile_WritesLakehouseCreatedBy holds the delete rewriter to the
// created_by every Lakehouse writer records: a rewritten file must not fall
// back to parquet-go's build-information-dependent default, or its bytes would
// differ between toolchains.
func TestRewriteFile_WritesLakehouseCreatedBy(t *testing.T) {
	want := schema.ParquetWriterApp + " version "
	cases := []struct {
		name, prefix, mode, key string
		data                    func(t *testing.T) []byte
		query                   string
	}{
		{
			name: "logs", prefix: "logs/", mode: "logs", key: "logs/dt=2026-01-15/hour=10/00001.parquet",
			data: func(t *testing.T) []byte {
				return buildTestParquet(t, []schema.LogRow{
					{TimestampUnixNano: 1000, Body: "error", SeverityText: "error", ServiceName: "web"},
					{TimestampUnixNano: 2000, Body: "ok", SeverityText: "info", ServiceName: "web"},
				})
			},
			query: `severity_text:="error"`,
		},
		{
			name: "traces", prefix: "traces/", mode: "traces", key: "traces/dt=2026-05-02/hour=10/batch-01.parquet",
			data: func(t *testing.T) []byte {
				return buildTestTraceParquet(t, []schema.TraceRow{
					{TimestampUnixNano: 1000, TraceID: "t1", SpanID: "s1", SpanName: "GET /a", ServiceName: "a"},
					{TimestampUnixNano: 2000, TraceID: "t2", SpanID: "s2", SpanName: "GET /b", ServiceName: "b"},
				})
			},
			query: `service.name:="b"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := newMockRewriterPool()
			pool.Put(tc.key, tc.data(t))
			rw := NewRewriter(pool, tc.prefix, 100, tc.mode)
			res, err := rw.RewriteFile(context.Background(), tc.key, []Tombstone{
				{Tenants: []TenantRef{{}}, ID: "t", Query: tc.query, StartNs: 0, EndNs: 10000},
			})
			if err != nil {
				t.Fatalf("RewriteFile: %v", err)
			}
			data := mustGet(t, pool, res.NewKey)
			f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			if got := f.Metadata().CreatedBy; !strings.HasPrefix(got, want) {
				t.Fatalf("rewritten file created_by = %q, want prefix %q", got, want)
			}
		})
	}
}
