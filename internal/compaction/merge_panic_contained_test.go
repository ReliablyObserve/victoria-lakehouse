package compaction

import (
	"context"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A merge that panics (here: an input object the Parquet reader panics on, which
// readLogRows documents) is contained at the compaction group: Compact returns an
// error, uploads nothing, deletes nothing and leaves every source registered, so
// the process that also acknowledges ingest keeps running and the plan backs off
// like any failed merge.
func TestCompact_PanickingMergeIsContainedAndLeavesSourcesUntouched(t *testing.T) {
	for _, mode := range []config.Mode{config.ModeLogs, config.ModeTraces} {
		t.Run(string(mode), func(t *testing.T) {
			pool := newMockPool()
			m := manifest.New("test-bucket", "data/")
			const partition = "dt=2026-05-04/hour=10"

			good := makeTestParquet(t, []schema.LogRow{{TimestampUnixNano: 1000, Body: "first", ServiceName: "svc"}})
			if mode == config.ModeTraces {
				good = makeTestTraceParquet(t, []schema.TraceRow{{TimestampUnixNano: 1000, TraceID: "t1", SpanID: "s1", SpanName: "op"}})
			}
			// A footer-valid object whose pages are junk would be ideal; the
			// documented panic input is not a Parquet file at all.
			bad := []byte("PAR1 this is not a parquet object PAR1")

			var files []manifest.FileInfo
			for i, data := range [][]byte{good, bad} {
				key := "data/" + partition + "/batch-" + string(rune('a'+i)) + ".parquet"
				pool.put(key, data)
				fi := manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 1, MinTimeNs: 1000, MaxTimeNs: 1000, SchemaFingerprint: "fp"}
				m.AddFile(partition, fi)
				files = append(files, fi)
			}
			before := len(pool.Keys())

			c := NewCompactor(CompactorConfig{Pool: pool, Manifest: m, Prefix: "data/", Mode: mode, RowGroupSize: 100, CompressionLevel: 3})
			res, err := c.Compact(context.Background(), partition, files, 0)
			if err == nil {
				t.Fatalf("a panicking merge must fail the group, got result %+v", res)
			}
			if !strings.Contains(err.Error(), "merge panicked") {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := len(pool.Keys()); got != before {
				t.Errorf("objects after the failed merge: %d, before: %d (nothing may be uploaded or deleted)", got, before)
			}
			for _, f := range files {
				if pool.get(f.Key) == nil {
					t.Errorf("source %s was deleted by a failed merge", f.Key)
				}
			}
			if live := m.FilesForPartition(partition); len(live) != len(files) {
				t.Errorf("manifest holds %d files after the failed merge, want %d", len(live), len(files))
			}
		})
	}
}
