package delete

import (
	"context"
	"fmt"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil"
)

// A traces object rewritten by a delete keeps DELTA_BINARY_PACKED on the
// optional start_time_unix_nano column through the rewriter's own writer (the one
// used when no writers are injected). The production wiring with the compactor's
// writers is covered in internal/compaction.
func TestRewriteFile_Traces_StartTimeKeepsDeltaEncoding(t *testing.T) {
	rows := make([]schema.TraceRow, 400)
	for i := range rows {
		ts := int64(1700000000000000000) + int64(i)*1000
		svc := "keep"
		if i%4 == 0 {
			svc = "drop"
		}
		rows[i] = schema.TraceRow{TimestampUnixNano: ts, StartTimeUnixNano: schema.Int64Ptr(ts), TraceID: fmt.Sprintf("%032x", i), SpanID: fmt.Sprintf("%016x", i), ServiceName: svc}
	}
	in := buildTestTraceParquet(t, rows) // plain struct writer: no delta on start_time
	if testutil.HasEncoding(testutil.ColumnEncodings(t, in), "start_time_unix_nano", "DELTA_BINARY_PACKED") {
		t.Fatal("fixture unexpectedly delta encoded")
	}
	pool := newMockRewriterPool()
	key := "traces/dt=2026-05-02/hour=10/batch-01.parquet"
	pool.Put(key, in)
	rw := NewRewriter(pool, "traces/", 1000, "traces")
	res, err := rw.RewriteFile(context.Background(), key, []Tombstone{
		{Tenants: []TenantRef{{}}, ID: "t", Query: `service.name:="drop"`, StartNs: 0, EndNs: 1 << 62},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsRemoved != 100 {
		t.Fatalf("removed %d, want 100", res.RowsRemoved)
	}
	encs := testutil.ColumnEncodings(t, mustGet(t, pool, res.NewKey))
	if !testutil.HasEncoding(encs, "start_time_unix_nano", "DELTA_BINARY_PACKED") {
		t.Fatalf("rewritten object start_time_unix_nano encodings: %v", encs["start_time_unix_nano"])
	}
}
