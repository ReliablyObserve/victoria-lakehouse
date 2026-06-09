package parquets3

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func valueStrings(vs []logstorage.ValueWithHits) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = v.Value
	}
	sort.Strings(out)
	return out
}

// TestInteg_PmetaCatalog_CrossPathParity is the Level-2 gate: drive a REAL writer
// flush with --pmeta on (so the catalogObserver fires), then assert the catalog
// field/value path returns exactly what the data contains AND what the legacy
// scan path returns. This proves the live flush→catalog→GetFieldValues path is
// correct before the flag is enabled anywhere.
func TestInteg_PmetaCatalog_CrossPathParity(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url()) // base read storage (catalog nil, labelIndex empty)

	// Enable pmeta on this storage and a writer that shares its manifest+pool.
	catalog := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	s.catalog = catalog
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: catalog}

	now := time.Now()
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", ServiceName: "api-gateway"},
		{TimestampUnixNano: now.UnixNano(), Body: "b", ServiceName: "order-service"},
		{TimestampUnixNano: now.UnixNano(), Body: "c", ServiceName: "api-gateway"},
	})
	bw.triggerFlush() // upload to mock S3 + manifest.AddFile + catalogObserver.OnFileFlush

	startNs := now.Add(-time.Hour).UnixNano()
	endNs := now.Add(time.Hour).UnixNano()
	want := []string{"api-gateway", "order-service"}

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		t.Fatal("no files registered after flush")
	}
	part := manifest.ExtractPartition(files[0].Key)

	// (1) The catalog was fed correctly by the live writer flush.
	if got := catalog.FieldValues(part, "service.name", "", 0); !reflect.DeepEqual(got, want) {
		t.Fatalf("catalog (fed by flush) FieldValues = %v, want %v", got, want)
	}

	// (2) GetFieldValues catalog fast-path returns the ground truth.
	q := mustParseQueryWithTime(t, "*", startNs, endNs)
	on, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues(pmeta on): %v", err)
	}
	gotOn := valueStrings(on)
	if !reflect.DeepEqual(gotOn, want) {
		t.Fatalf("GetFieldValues(pmeta on) = %v, want %v (catalog must serve exact values)", gotOn, want)
	}

	// (3) Cross-path parity: with pmeta OFF the legacy labelIndex/scan path must
	// return the SAME values.
	s.catalog = nil
	off, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues(pmeta off): %v", err)
	}
	if gotOff := valueStrings(off); !reflect.DeepEqual(gotOn, gotOff) {
		t.Fatalf("cross-path mismatch: catalog=%v legacy=%v", gotOn, gotOff)
	}
}

// TestInteg_PmetaCatalog_WarmFromManifest verifies the cold-start path: a pod
// whose manifest is loaded but whose catalog is empty rebuilds the catalog from
// the manifest's per-file Labels (no S3), so the FIRST dropdown query is fast.
func TestInteg_PmetaCatalog_WarmFromManifest(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")

	base := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC).UnixNano()
	s.manifest.AddFile("dt=2026-06-09/hour=10", manifest.FileInfo{
		Key:       "logs/dt=2026-06-09/hour=10/a.parquet",
		MinTimeNs: base, MaxTimeNs: base + 1,
		Labels: map[string][]string{"service.name": {"api-gateway", "order-service"}, "level": {"ERROR"}},
	})
	s.manifest.AddFile("dt=2026-06-09/hour=11", manifest.FileInfo{
		Key:       "logs/dt=2026-06-09/hour=11/b.parquet",
		MinTimeNs: base + int64(time.Hour), MaxTimeNs: base + int64(time.Hour) + 1,
		Labels: map[string][]string{"service.name": {"user-service"}},
	})

	if v := s.catalog.FieldValues("dt=2026-06-09/hour=10", "service.name", "", 0); len(v) != 0 {
		t.Fatalf("catalog should be empty before warm, got %v", v)
	}

	s.WarmCatalog(context.Background())

	if got := s.catalog.FieldValues("dt=2026-06-09/hour=10", "service.name", "", 0); !reflect.DeepEqual(got, []string{"api-gateway", "order-service"}) {
		t.Fatalf("hour=10 after warm = %v", got)
	}
	if got := s.catalog.FieldValues("dt=2026-06-09/hour=11", "service.name", "", 0); !reflect.DeepEqual(got, []string{"user-service"}) {
		t.Fatalf("hour=11 after warm = %v", got)
	}
}
