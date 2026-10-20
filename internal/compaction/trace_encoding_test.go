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
)

func encodingSpans(n int) []schema.TraceRow {
	rows := make([]schema.TraceRow, n)
	for i := range rows {
		ts := int64(1700000000000000000) + int64(i)*1000
		rows[i] = schema.TraceRow{
			TimestampUnixNano: ts, StartTimeUnixNano: schema.Int64Ptr(ts),
			TraceID: fmt.Sprintf("%032x", i/4), SpanID: fmt.Sprintf("%016x", i), ServiceName: "svc",
		}
	}
	return rows
}

// A compacted traces object keeps DELTA_BINARY_PACKED on the optional
// start_time_unix_nano column, whether the inputs had it or not (inputs written by
// a plain struct writer are the worst case: an older build or a test fixture).
func TestWriteTraces_StartTimeKeepsDeltaEncoding(t *testing.T) {
	data, err := WriteTraces(encodingSpans(500), 100, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !testutil.HasEncoding(testutil.ColumnEncodings(t, data), "start_time_unix_nano", "DELTA_BINARY_PACKED") {
		t.Fatalf("WriteTraces start_time_unix_nano encodings: %v", testutil.ColumnEncodings(t, data)["start_time_unix_nano"])
	}
}

func TestCompactor_MergedTracesKeepStartTimeDelta(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "traces/")
	const partition = "dt=2026-05-04/hour=10"
	var infos []manifest.FileInfo
	for i := 0; i < 2; i++ {
		rows := encodingSpans(300)
		for j := range rows {
			rows[j].TimestampUnixNano += int64(i) * 1_000_000
			rows[j].StartTimeUnixNano = schema.Int64Ptr(rows[j].TimestampUnixNano)
			rows[j].SpanID = fmt.Sprintf("%d-%d", i, j)
		}
		data := makeTestTraceParquet(t, rows)
		key := fmt.Sprintf("traces/%s/batch-%03d.parquet", partition, i)
		if err := pool.Upload(context.Background(), key, data); err != nil {
			t.Fatal(err)
		}
		fi := manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 300, MinTimeNs: rows[0].TimestampUnixNano, MaxTimeNs: rows[299].TimestampUnixNano, SchemaFingerprint: "fp"}
		m.AddFile(partition, fi)
		infos = append(infos, fi)
	}
	// The fixture writer is a plain struct writer: it has no delta on start_time.
	in := pool.get(infos[0].Key)
	if testutil.HasEncoding(testutil.ColumnEncodings(t, in), "start_time_unix_nano", "DELTA_BINARY_PACKED") {
		t.Fatal("fixture unexpectedly delta encoded: the test would not prove the compactor re-encodes")
	}
	c := NewCompactor(CompactorConfig{Pool: pool, Manifest: m, Prefix: "traces/", Mode: config.ModeTraces, RowGroupSize: 1000, CompressionLevel: 7})
	res, err := c.Compact(context.Background(), partition, infos, 0)
	if err != nil {
		t.Fatal(err)
	}
	encs := testutil.ColumnEncodings(t, pool.get(res.OutputFile))
	if !testutil.HasEncoding(encs, "start_time_unix_nano", "DELTA_BINARY_PACKED") {
		t.Fatalf("compacted object start_time_unix_nano encodings: %v", encs["start_time_unix_nano"])
	}
}

// Spans a tombstone hides are still merged forward, and the merged object keeps
// the encoding: compaction wired with the production writers delta-encodes
// start_time_unix_nano and drops no row.
func TestCompaction_HiddenSpansKeepStartTimeDelta(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "traces/")
	const partition = "dt=2026-07-01/hour=03"
	var infos []manifest.FileInfo
	for i := 0; i < 2; i++ {
		rows := encodingSpans(200)
		for j := range rows {
			rows[j].TimestampUnixNano += int64(i) * 1_000_000
			rows[j].StartTimeUnixNano = schema.Int64Ptr(rows[j].TimestampUnixNano)
			rows[j].SpanID = fmt.Sprintf("%d-%d", i, j)
			if j%2 == 0 {
				rows[j].ServiceName = "drop"
			}
		}
		data := makeTestTraceParquet(t, rows)
		key := fmt.Sprintf("traces/%s/src-%d.parquet", partition, i)
		pool.put(key, data)
		fi := manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 200, MinTimeNs: rows[0].TimestampUnixNano, MaxTimeNs: rows[199].TimestampUnixNano, SchemaFingerprint: "fp"}
		m.AddFile(partition, fi)
		infos = append(infos, fi)
	}
	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{ID: "t", Query: `service.name:="drop"`, StartNs: 0, EndNs: 1 << 62, Mode: "permanent", Tenants: []delete.TenantRef{{}}})
	c := NewCompactor(CompactorConfig{Pool: pool, Manifest: m, Prefix: "traces/", Mode: config.ModeTraces, RowGroupSize: 100, CompressionLevel: 1})
	res, err := c.Compact(context.Background(), partition, infos, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := pool.get(res.OutputFile)
	if !testutil.HasEncoding(testutil.ColumnEncodings(t, out), "start_time_unix_nano", "DELTA_BINARY_PACKED") {
		t.Fatalf("compacted start_time_unix_nano encodings: %v", testutil.ColumnEncodings(t, out)["start_time_unix_nano"])
	}
	if res.RowsMerged != 400 {
		t.Fatalf("the merged object holds %d rows, want all 400 (hidden rows are copied forward)", res.RowsMerged)
	}
	if _, ok := store.Get("t"); !ok {
		t.Fatal("compaction retired the tombstone")
	}
}
