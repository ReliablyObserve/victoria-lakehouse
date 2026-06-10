package parquets3

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func pmValues(vs []logstorage.ValueWithHits) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = v.Value
	}
	sort.Strings(out)
	return out
}

// TestInteg_PmetaCatalog_TracesCrossPathParity is the Level-2 gate for the TRACES
// module: drive a real trace flush with --pmeta on (feeding the catalog via the
// trace flush path / extractTraceLabels), then assert the catalog returns exactly
// the ingested service.name AND span.name values, and that GetFieldValues matches
// the legacy scan path.
func TestInteg_PmetaCatalog_TracesCrossPathParity(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	catalog := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	s.catalog = catalog
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	bw.catalogObserver = &catalogObserver{store: catalog}

	now := time.Now()
	bw.AddTraceRows([]schema.TraceRow{
		{TimestampUnixNano: now.UnixNano(), ServiceName: "api-gateway", SpanName: "GET /a"},
		{TimestampUnixNano: now.UnixNano(), ServiceName: "order-service", SpanName: "POST /b"},
		{TimestampUnixNano: now.UnixNano(), ServiceName: "api-gateway", SpanName: "GET /a"},
	})
	bw.triggerFlush()

	startNs := now.Add(-time.Hour).UnixNano()
	endNs := now.Add(time.Hour).UnixNano()
	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		t.Fatal("no files registered after trace flush")
	}
	part := manifest.ExtractPartition(files[0].Key)

	// Catalog fed correctly for both trace facet fields.
	if got, want := catalog.FieldValues(part, "service.name", "", 0),
		[]string{"api-gateway", "order-service"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("catalog service.name = %v, want %v", got, want)
	}
	if got, want := catalog.FieldValues(part, "span.name", "", 0),
		[]string{"GET /a", "POST /b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("catalog span.name = %v, want %v", got, want)
	}

	// GetFieldValues catalog path == ground truth.
	q := mustParseQueryWithTime(t, "*", startNs, endNs)
	on, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues(pmeta on): %v", err)
	}
	gotOn := pmValues(on)
	if !reflect.DeepEqual(gotOn, []string{"api-gateway", "order-service"}) {
		t.Fatalf("GetFieldValues(pmeta on) = %v", gotOn)
	}

	// Cross-path: pmeta off (labelIndex empty → scan) returns the same.
	s.catalog = nil
	off, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues(pmeta off): %v", err)
	}
	if gotOff := pmValues(off); !reflect.DeepEqual(gotOn, gotOff) {
		t.Fatalf("cross-path mismatch: catalog=%v legacy=%v", gotOn, gotOff)
	}
}

// TestInteg_PmetaFlip_TracesBloomFacet exercises the traces bloom read-flip facet
// path (the AND-across-columns / OR-within-column check via Store.BloomMayContain):
// the facet is fed at flush, and a file that holds the queried value is never
// excluded (blooms have no false negatives).
func TestInteg_PmetaFlip_TracesBloomFacet(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	bw.catalogObserver = &catalogObserver{store: s.catalog}

	now := time.Now()
	bw.AddTraceRows([]schema.TraceRow{
		{TimestampUnixNano: now.UnixNano(), ServiceName: "api-gateway", SpanName: "GET /a"},
		{TimestampUnixNano: now.UnixNano(), ServiceName: "order-service", SpanName: "POST /b"},
	})
	bw.triggerFlush()

	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	if len(files) == 0 {
		t.Fatal("no files after trace flush")
	}
	fi := files[0]
	part := manifest.ExtractPartition(fi.Key)

	// Facet was fed at flush: a present value is found.
	got, ok := s.catalog.BloomMayContain(part, []string{fi.Key}, "service.name", "api-gateway")
	found := false
	for _, k := range got {
		if k == fi.Key {
			found = true
		}
	}
	if !ok || !found {
		t.Fatalf("traces bloom facet missing service.name=api-gateway (ok=%v got=%v)", ok, got)
	}

	// Facet bloom path must NOT exclude a file that contains the value.
	if s.checkFileBloom(context.Background(), fi, `service.name:="api-gateway"`) {
		t.Fatal("traces facet bloom wrongly excluded a file containing service.name=api-gateway")
	}
}

// TestInteg_PmetaFlip_BloomHybridColdRestart covers the post-sidecar-retirement cold
// restart: s.bloomIdx is EMPTY (no _bloom.bin persisted anymore) and the bloom state
// lives only in the bundle-warmed facet. The pre-filter hybrid must (a) still prune
// via the facet, (b) never drop a file holding the value, and (c) keep files of a
// partition neither side knows (can't exclude the unknown).
func TestInteg_PmetaFlip_BloomHybridColdRestart(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	bw.catalogObserver = &catalogObserver{store: s.catalog, pool: s.pool}

	now := time.Now()
	bw.AddTraceRows([]schema.TraceRow{
		{TimestampUnixNano: now.UnixNano(), ServiceName: "api-gateway", SpanName: "GET /a"},
	})
	bw.triggerFlush()

	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	if len(files) == 0 {
		t.Fatal("no file after flush")
	}
	fi := files[0]

	// Simulate the cold restart: persist bundles, then a FRESH catalog warmed only
	// from S3 — and an empty legacy bloomIdx (nothing persisted it).
	if _, err := s.catalog.PersistDirty(context.Background(), poolObjectStore{s.pool}); err != nil {
		t.Fatal(err)
	}
	part := manifest.ExtractPartition(fi.Key)
	fresh := newCatalogStore(s.cfg.Pmeta, "logs/")
	if res := fresh.WarmPartitions(context.Background(), poolObjectStore{s.pool}, []string{part}, 2); res.Loaded == 0 {
		t.Fatalf("bundle not warmed (NeedsRebuild=%v)", res.NeedsRebuild)
	}
	s.catalog = fresh

	present := []bloomindex.ColumnCheck{{Column: "service.name", Value: "api-gateway"}}
	absent := []bloomindex.ColumnCheck{{Column: "service.name", Value: "no-such-service-xyz"}}

	// (a)+(b): present value kept, absent value pruned — via facet only.
	if got := s.bloomMayContainAll([]string{fi.Key}, present); len(got) != 1 || got[0] != fi.Key {
		t.Fatalf("hybrid dropped a file holding the value: %v", got)
	}
	if got := s.bloomMayContainAll([]string{fi.Key}, absent); len(got) != 0 {
		t.Fatalf("hybrid kept a file the facet can prune: %v", got)
	}

	// (c): a key in a partition neither facet nor bloomIdx knows is KEPT.
	unknown := "traces/dt=1999-01-01/hour=00/x.parquet"
	if got := s.bloomMayContainAll([]string{unknown}, present); len(got) != 1 || got[0] != unknown {
		t.Fatalf("hybrid dropped an unknown-partition file (must keep what it can't exclude): %v", got)
	}
}
