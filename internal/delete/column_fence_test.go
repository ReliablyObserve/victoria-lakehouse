package delete

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

type futureLogRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano,delta"`
	Body              string `parquet:"body"`
	ServiceName       string `parquet:"service.name,dict"`
	Future            string `parquet:"future.column,optional"`
}

type futureTraceRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano,delta"`
	TraceID           string `parquet:"trace_id"`
	ServiceName       string `parquet:"service.name,dict"`
	Future            string `parquet:"future.column,optional"`
}

func writeRows[T any](t *testing.T, rows []T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// The forward fence: a delete rewrite of an object with a column the running
// code does not model would drop that column. It fails instead, leaving the
// object (and the pending tombstone) alone.
func TestRewriter_Fence_UnknownColumnsAreNotRewritten(t *testing.T) {
	tomb := []Tombstone{{Tenants: []TenantRef{{}}, ID: "ts1", Query: `service.name:="svc"`, StartNs: 0, EndNs: 1 << 62}}
	for _, signal := range []string{"logs", "traces"} {
		t.Run(signal, func(t *testing.T) {
			pool := newMockRewriterPool()
			var data []byte
			if signal == "logs" {
				data = writeRows(t, []futureLogRow{{1000, "a", "svc", "x"}, {2000, "b", "other", "y"}})
			} else {
				data = writeRows(t, []futureTraceRow{{1000, "t1", "svc", "x"}, {2000, "t2", "other", "y"}})
			}
			key := signal + "/dt=2026-05-02/hour=10/future.parquet"
			pool.Put(key, data)
			rw := NewRewriter(pool, signal+"/", 1000, signal)
			before := metrics.SkippedUnknownColumns(signal, "delete_rewrite").Get()

			res, err := rw.RewriteFile(context.Background(), key, tomb)
			if !errors.Is(err, ErrUnknownColumns) || res != nil {
				t.Fatalf("got res=%v err=%v, want ErrUnknownColumns", res, err)
			}
			if got, _ := pool.Get(key); !bytes.Equal(got, data) || pool.Count() != 1 {
				t.Fatal("the object was changed or a replacement was written")
			}
			if metrics.SkippedUnknownColumns(signal, "delete_rewrite").Get() != before+1 {
				t.Error("lakehouse_compaction_skipped_unknown_columns_total did not count the skip")
			}
		})
	}
}

// A known-schema object is rewritten as before.
func TestRewriter_Fence_KnownSchemaRewritesAsBefore(t *testing.T) {
	pool := newMockRewriterPool()
	key := "logs/dt=2026-05-02/hour=10/known.parquet"
	pool.Put(key, buildTestParquet(t, []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "a", ServiceName: "svc"},
		{TimestampUnixNano: 2000, Body: "b", ServiceName: "other"},
	}))
	rw := NewRewriter(pool, "logs/", 1000, "logs")
	res, err := rw.RewriteFile(context.Background(), key, []Tombstone{{Tenants: []TenantRef{{}}, ID: "ts1", Query: `service.name:="svc"`, StartNs: 0, EndNs: 1 << 62}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsRemoved != 1 || res.RowsKept != 1 {
		t.Fatalf("removed %d kept %d", res.RowsRemoved, res.RowsKept)
	}
}

// Property: for random mixes of known and unknown-column objects, an object
// with unknown columns is never replaced, and a known one is.
func TestRewriter_Fence_PropertyNoUnknownInputIsRewritten(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	tomb := []Tombstone{{Tenants: []TenantRef{{}}, ID: "ts1", Query: `service.name:="svc"`, StartNs: 0, EndNs: 1 << 62}}
	for iter := 0; iter < 40; iter++ {
		signal := []string{"logs", "traces"}[r.Intn(2)]
		unknown := r.Intn(2) == 0
		pool := newMockRewriterPool()
		var data []byte
		switch {
		case signal == "logs" && unknown:
			data = writeRows(t, []futureLogRow{{1000, "a", "svc", "x"}, {2000, "b", "other", ""}})
		case signal == "logs":
			data = buildTestParquet(t, []schema.LogRow{{TimestampUnixNano: 1000, Body: "a", ServiceName: "svc"}, {TimestampUnixNano: 2000, Body: "b", ServiceName: "other"}})
		case unknown:
			data = writeRows(t, []futureTraceRow{{1000, "t1", "svc", "x"}, {2000, "t2", "other", ""}})
		default:
			data = buildTestTraceParquet(t, []schema.TraceRow{{TimestampUnixNano: 1000, TraceID: "t1", ServiceName: "svc"}, {TimestampUnixNano: 2000, TraceID: "t2", ServiceName: "other"}})
		}
		key := signal + "/dt=2026-05-02/hour=10/p.parquet"
		pool.Put(key, data)
		res, err := NewRewriter(pool, signal+"/", 1000, signal).RewriteFile(context.Background(), key, tomb)
		if unknown {
			if !errors.Is(err, ErrUnknownColumns) {
				t.Fatalf("iter %d: unknown-column object not fenced: %v", iter, err)
			}
			if got, _ := pool.Get(key); !bytes.Equal(got, data) || pool.Count() != 1 {
				t.Fatalf("iter %d: unknown-column object rewritten", iter)
			}
		} else if err != nil || res.NewKey == "" {
			t.Fatalf("iter %d: known object not rewritten: %v", iter, err)
		}
	}
}
