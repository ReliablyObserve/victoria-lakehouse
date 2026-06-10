package parquets3

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// TestInteg_CountPushdown_EqualsScan is the correctness gate for the manifest
// count-pushdown fast path: `* | stats count() by (service.name)` answered from
// manifest aggregates must emit the EXACT same service.name distribution (incl.
// the empty-value group) as a real Parquet scan of the same rows.
func TestInteg_CountPushdown_EqualsScan(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), ServiceName: "api-gw", Body: "a"},
		{TimestampUnixNano: now.Add(1).UnixNano(), ServiceName: "api-gw", Body: "b"},
		{TimestampUnixNano: now.Add(2).UnixNano(), ServiceName: "api-gw", Body: "c"},
		{TimestampUnixNano: now.Add(3).UnixNano(), ServiceName: "worker", Body: "d"},
		{TimestampUnixNano: now.Add(4).UnixNano(), ServiceName: "worker", Body: "e"},
		{TimestampUnixNano: now.Add(5).UnixNano(), ServiceName: "", Body: "f"}, // empty group
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/cp.parquet"
	mock.putFile(key, data)

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()
	const partition = "dt=2026-05-10/hour=14"
	fiBase := manifest.FileInfo{
		Key: key, Size: int64(len(data)), RowCount: int64(len(rows)),
		MinTimeNs: rows[0].TimestampUnixNano, MaxTimeNs: rows[len(rows)-1].TimestampUnixNano,
	}

	svcCol := "service.name"
	if m := s.registry.ResolveFromParquet("service.name"); m != nil {
		svcCol = m.InternalName
	}

	q := mustParseQueryWithTime(t, "* | stats by (service.name) count()", startNs, endNs)
	collect := func() map[string]int {
		got := map[string]int{}
		var mu sync.Mutex
		if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			defer mu.Unlock()
			for _, c := range db.GetColumns(false) {
				if c.Name == svcCol {
					for _, v := range c.Values {
						got[v]++
					}
				}
			}
		}); err != nil {
			t.Fatalf("RunQuery: %v", err)
		}
		return got
	}

	// Baseline: no aggregate on the file → it is scanned from Parquet.
	s.manifest = manifest.New("test-bucket", "logs/")
	s.manifest.AddFile(partition, fiBase)
	scan := collect()
	if scan["api-gw"] != 3 || scan["worker"] != 2 || scan[""] != 1 {
		t.Fatalf("scan baseline distribution unexpected: %v", scan)
	}

	// Fast path: aggregate present + file fully in range → served from metadata.
	s.manifest = manifest.New("test-bucket", "logs/")
	fiAgg := fiBase
	fiAgg.LabelAggregates = map[string]map[string]int64{"service.name": {"api-gw": 3, "worker": 2}}
	s.manifest.AddFile(partition, fiAgg)

	before := getCounterValue(t, metrics.MetadataOnlyFiles)
	fast := collect()
	if getCounterValue(t, metrics.MetadataOnlyFiles) == before {
		t.Fatal("fast path did not trigger (MetadataOnlyFiles unchanged) — query not detected as count-pushdown")
	}

	if !reflect.DeepEqual(scan, fast) {
		t.Fatalf("count-by-service mismatch (fast path != scan):\n scan=%v\n fast=%v", scan, fast)
	}
}

// TestInteg_CountPushdown_FilteredQuerySkipsFastPath guards the soundness gate: a
// query WITH a row filter must NOT use the whole-file aggregate (which counts
// every row), or it would ignore the filter and over-count. The fast path must
// stay dormant and the scan must apply the filter.
// TestInteg_CountPushdown_FilteredServedFromAggregates: a count filtered on
// the AGGREGATED field is now served from manifest aggregates — the synthetic
// rows reproduce the field's full distribution and preFilter applies the real
// filter to them downstream, so the result is exact (worker:1, api-gw:0) while
// the file never touches S3. This used to skip the fast path entirely (the old
// TestInteg_CountPushdown_FilteredQuerySkipsFastPath codified that); the gate
// extension (countPushdownFilterFields) made single-field filters sound.
func TestInteg_CountPushdown_FilteredServedFromAggregates(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), ServiceName: "api-gw", Body: "a"},
		{TimestampUnixNano: now.Add(1).UnixNano(), ServiceName: "api-gw", Body: "b"},
		{TimestampUnixNano: now.Add(2).UnixNano(), ServiceName: "worker", Body: "c"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/cpf.parquet"
	mock.putFile(key, data)

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()
	fiAgg := manifest.FileInfo{
		Key: key, Size: int64(len(data)), RowCount: int64(len(rows)),
		MinTimeNs: rows[0].TimestampUnixNano, MaxTimeNs: rows[len(rows)-1].TimestampUnixNano,
		LabelAggregates: map[string]map[string]int64{"service.name": {"api-gw": 2, "worker": 1}},
	}
	s.manifest.AddFile("dt=2026-05-10/hour=14", fiAgg)

	svcCol := "service.name"
	if m := s.registry.ResolveFromParquet("service.name"); m != nil {
		svcCol = m.InternalName
	}

	// Filter to worker only — the aggregate says api-gw:2, worker:1, but the
	// filtered count must be worker:1 and nothing else.
	q := mustParseQueryWithTime(t, `service.name:="worker" | stats by (service.name) count()`, startNs, endNs)

	got := map[string]int{}
	before := getCounterValue(t, metrics.MetadataOnlyFiles)
	if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		for _, c := range db.GetColumns(false) {
			if c.Name == svcCol {
				for _, v := range c.Values {
					got[v]++
				}
			}
		}
	}); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	if getCounterValue(t, metrics.MetadataOnlyFiles) == before {
		t.Fatal("fast path did NOT fire on a single-field filtered query — filtered counts should be metadata-served")
	}
	if got["worker"] != 1 || got["api-gw"] != 0 {
		t.Fatalf("filtered count wrong: %v (want only worker:1) — the downstream filter must prune synthetic rows exactly", got)
	}
}

// A filter referencing a DIFFERENT field than the pipe's group-by field must
// still skip the fast path: synthetic rows fabricate every column except the
// aggregated one, so evaluating severity_text against them would be wrong.
func TestInteg_CountPushdown_CrossFieldFilterSkipsFastPath(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), ServiceName: "api-gw", Body: "a"},
		{TimestampUnixNano: now.Add(1).UnixNano(), ServiceName: "worker", Body: "b"},
	}
	data := writeParquetToBytes(t, rows)
	key := "logs/dt=2026-05-10/hour=14/cpx.parquet"
	mock.putFile(key, data)

	fi := manifest.FileInfo{
		Key: key, Size: int64(len(data)), RowCount: int64(len(rows)),
		MinTimeNs: rows[0].TimestampUnixNano, MaxTimeNs: rows[1].TimestampUnixNano,
		LabelAggregates: map[string]map[string]int64{"service.name": {"api-gw": 1, "worker": 1}},
	}
	s.manifest.AddFile("dt=2026-05-10/hour=14", fi)

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()
	q := mustParseQueryWithTime(t, `severity_text:ERROR | stats by (service.name) count()`, startNs, endNs)

	before := getCounterValue(t, metrics.MetadataOnlyFiles)
	if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {}); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	if getCounterValue(t, metrics.MetadataOnlyFiles) != before {
		t.Fatal("fast path fired with a cross-field filter — synthetic rows fabricate severity_text; results would be wrong")
	}
}
