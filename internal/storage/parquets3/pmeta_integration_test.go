package parquets3

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
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

// TestInteg_PmetaCatalog_BundlePersistWarmRoundTrip verifies the flip prerequisite:
// after a flush, persisting bundles to S3 and warming a FRESH store from them (a
// simulated cold restart) restores the bloom facet — which, unlike catalog/file-meta,
// CANNOT be re-derived from the manifest. catalog + file-meta round-trip too.
func TestInteg_PmetaCatalog_BundlePersistWarmRoundTrip(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog, pool: s.pool}

	now := time.Now()
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", ServiceName: "api-gateway"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", ServiceName: "order-service"},
	})
	bw.triggerFlush()

	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	if len(files) == 0 {
		t.Fatal("no file after flush")
	}
	fi := files[0]
	part := manifest.ExtractPartition(fi.Key)

	// Persist the dirty bundles to (mock) S3.
	if _, err := s.catalog.PersistDirty(context.Background(), poolObjectStore{s.pool}); err != nil {
		t.Fatal(err)
	}

	// Simulate a cold restart: a fresh store warmed only from the S3 bundles.
	fresh := newCatalogStore(s.cfg.Pmeta, "logs/")
	res := fresh.WarmPartitions(context.Background(), poolObjectStore{s.pool}, []string{part}, 4)
	if res.Loaded == 0 {
		t.Fatalf("no bundle loaded from S3 (NeedsRebuild=%v)", res.NeedsRebuild)
	}

	// Bloom survived (the whole point — it can't be rebuilt from the manifest).
	got, ok := fresh.BloomMayContain(part, []string{fi.Key}, "service.name", "api-gateway")
	found := false
	for _, k := range got {
		if k == fi.Key {
			found = true
		}
	}
	if !ok || !found {
		t.Fatalf("bloom did not survive persist→warm (ok=%v got=%v)", ok, got)
	}
	// catalog + file-meta round-tripped too.
	if _, ok := fresh.FileMeta(part, fi.Key); !ok {
		t.Fatal("file-meta did not survive persist→warm")
	}
	if v := fresh.FieldValues(part, "service.name", "", 0); len(v) == 0 {
		t.Fatal("catalog values did not survive persist→warm")
	}
}

// TestInteg_PmetaCatalog_NoLimitUsesIndex guards the dropdown-slowness fix: a
// no-limit (limit==0) field_values request must serve from the catalog (in-RAM)
// and return the values, NOT fall through to a full scan or get zeroed by the cap.
func TestInteg_PmetaCatalog_NoLimitUsesIndex(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog, sketch: sketchSet(nil)}

	now := time.Now()
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", ServiceName: "api-gateway"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", ServiceName: "order-service"},
	})
	bw.triggerFlush()

	q := mustParseQueryWithTime(t, "*", now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	before := metrics.CatalogValueLookups.Get("catalog")
	got, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 0) // limit==0
	if err != nil {
		t.Fatal(err)
	}
	if g := valueStrings(got); !reflect.DeepEqual(g, []string{"api-gateway", "order-service"}) {
		t.Fatalf("limit==0 GetFieldValues = %v (want the catalog values, not empty/scan)", g)
	}
	if metrics.CatalogValueLookups.Get("catalog") <= before {
		t.Fatal("limit==0 did not increment the catalog hit counter — it scanned instead of using the index")
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

// TestInteg_PmetaCatalog_RefuseSketchEnumeration asserts the INTENDED divergence:
// for an always-sketch field, refuse_sketch_enumeration=on returns empty (no scan)
// while off enumerates via the legacy scan. Uses service.name as the forced-sketch
// field (so it's actually present in the data) to make the divergence observable.
func TestInteg_PmetaCatalog_RefuseSketchEnumeration(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true, AlwaysSketchFields: []string{"service.name"}}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog}

	now := time.Now()
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", ServiceName: "api-gateway"},
		{TimestampUnixNano: now.UnixNano(), Body: "b", ServiceName: "order-service"},
	})
	bw.triggerFlush()

	q := mustParseQueryWithTime(t, "*", now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())

	// refuse OFF → the forced-sketch field still enumerates via the legacy scan.
	s.cfg.Pmeta.RefuseSketchEnumeration = false
	off, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(off) == 0 {
		t.Fatal("refuse off: forced-sketch field must still enumerate via scan")
	}

	// refuse ON → empty (no scan), the intended divergence.
	s.cfg.Pmeta.RefuseSketchEnumeration = true
	on, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(on) != 0 {
		t.Fatalf("refuse on: must return empty for an always-sketch field, got %v", valueStrings(on))
	}

	// A non-sketch field is never refused.
	if s.refuseEnumeration("level") {
		t.Fatal("non-sketch field must not be refused")
	}
}

// TestInteg_PmetaCatalog_CardinalityTapE2E is the full e2e: a real BatchWriter
// flush of rows carrying trace_id, with trace_id declared always-sketch, must feed
// the per-field HLL via the flush tap so Store.Cardinality + the gauge report the
// distinct count — closing ingest → sketch → readout.
func TestInteg_PmetaCatalog_CardinalityTapE2E(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true, AlwaysSketchFields: []string{"trace_id"}}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog, sketch: sketchSet(s.cfg.Pmeta.AlwaysSketchFields)}

	const n = 5000
	now := time.Now()
	rows := make([]schema.LogRow, n)
	for i := 0; i < n; i++ {
		rows[i] = schema.LogRow{
			TimestampUnixNano: now.UnixNano(),
			Body:              "msg",
			ServiceName:       "api-gateway",
			TraceID:           fmt.Sprintf("%032x", i),
		}
	}
	bw.AddLogRows(rows)
	bw.triggerFlush()

	// The flush tap fed the trace_id HLL: cardinality ≈ n.
	got := s.catalog.Cardinality("trace_id")
	if e := math.Abs(float64(got)-float64(n)) / float64(n); e > 0.03 {
		t.Fatalf("Cardinality(trace_id)=%d relErr=%.3f%% (true %d)", got, e*100, n)
	}
	// The per-field cardinality gauge was published.
	if g := metrics.CatalogFieldCardinality.Get("trace_id"); g == 0 {
		t.Fatal("lakehouse_catalog_field_cardinality{trace_id} not published")
	}
	// service.name (low-card, not always-sketch) is enumerable, not tapped.
	if c := s.catalog.Cardinality("service.name"); c != 0 {
		t.Fatalf("service.name should not be sketched, got cardinality %d", c)
	}
}

// TestInteg_PmetaCatalog_FileMetaFacetParity is the dual-write parity gate for the
// fileMetaFacet fold: after a real flush, the facet's per-file metadata must equal
// manifest.FileInfoToMeta(fi) — i.e. the _file_metadata.json sidecar content —
// byte-for-byte, so the sidecar can later be retired without losing data.
func TestInteg_PmetaCatalog_FileMetaFacetParity(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog, sketch: sketchSet(nil)}

	now := time.Now()
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", ServiceName: "api-gateway"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", ServiceName: "order-service"},
	})
	bw.triggerFlush()

	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	if len(files) == 0 {
		t.Fatal("no file registered after flush")
	}
	fi := files[0]
	part := manifest.ExtractPartition(fi.Key)

	got, ok := s.catalog.FileMeta(part, fi.Key)
	if !ok {
		t.Fatal("fileMetaFacet has no entry for the flushed file")
	}
	want := manifest.FileInfoToMeta(fi) // == the _file_metadata.json sidecar entry
	if got.RowCount != want.RowCount || got.MinTimeNs != want.MinTimeNs ||
		got.MaxTimeNs != want.MaxTimeNs || got.RawBytes != want.RawBytes ||
		got.SchemaFingerprint != want.SchemaFingerprint {
		t.Fatalf("fileMeta facet != sidecar:\n facet=%+v\n sidecar=%+v", got, want)
	}
	if !reflect.DeepEqual(got.Labels, want.Labels) {
		t.Fatalf("fileMeta labels mismatch: facet=%v sidecar=%v", got.Labels, want.Labels)
	}
}

// TestInteg_PmetaCatalog_FileMetaWarmParity covers the cold-start path: a pod whose
// manifest is loaded rebuilds the fileMetaFacet from the manifest's FileInfo (full
// metadata, not just labels) via WarmCatalog, so Store.FileMeta matches the sidecar
// even before any local flush.
func TestInteg_PmetaCatalog_FileMetaWarmParity(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")

	base := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC).UnixNano()
	fi := manifest.FileInfo{
		Key:               "logs/dt=2026-06-09/hour=10/a.parquet",
		RowCount:          4242,
		MinTimeNs:         base,
		MaxTimeNs:         base + int64(time.Minute),
		RawBytes:          987654,
		SchemaFingerprint: "sf-abc123",
		Labels:            map[string][]string{"service.name": {"api-gateway"}},
	}
	s.manifest.AddFile("dt=2026-06-09/hour=10", fi)

	s.WarmCatalog(context.Background())

	got, ok := s.catalog.FileMeta("dt=2026-06-09/hour=10", fi.Key)
	if !ok {
		t.Fatal("fileMetaFacet not populated by WarmCatalog")
	}
	want := manifest.FileInfoToMeta(fi)
	if got.RowCount != want.RowCount || got.MinTimeNs != want.MinTimeNs ||
		got.MaxTimeNs != want.MaxTimeNs || got.RawBytes != want.RawBytes ||
		got.SchemaFingerprint != want.SchemaFingerprint ||
		!reflect.DeepEqual(got.Labels, want.Labels) {
		t.Fatalf("warm fileMeta != sidecar:\n facet=%+v\n sidecar=%+v", got, want)
	}
}

// TestInteg_PmetaCatalog_AllFacetsE2E exercises EVERY pmeta facet through ONE real
// BatchWriter flush with --pmeta fully on, then asserts each in one place:
//
//	catalog (low-card dropdown) · HLL (cardinality) · file-meta (dual-write) ·
//	bloom (dual-write) · cold-start warm (catalog + file-meta rebuilt from manifest).
func TestInteg_PmetaCatalog_AllFacetsE2E(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true, AlwaysSketchFields: []string{"trace_id", "span_id"}}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog, sketch: sketchSet(s.cfg.Pmeta.AlwaysSketchFields)}

	const n = 2000
	now := time.Now()
	rows := make([]schema.LogRow, n)
	for i := 0; i < n; i++ {
		svc := "api-gateway"
		if i%2 == 1 {
			svc = "order-service"
		}
		rows[i] = schema.LogRow{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Body:              "msg",
			ServiceName:       svc,
			TraceID:           fmt.Sprintf("%032x", i),
			SpanID:            fmt.Sprintf("%016x", i),
		}
	}
	bw.AddLogRows(rows)
	bw.triggerFlush()

	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	if len(files) == 0 {
		t.Fatal("no file registered after flush")
	}
	fi := files[0]
	part := manifest.ExtractPartition(fi.Key)
	q := mustParseQueryWithTime(t, "*", now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())

	// 1. CATALOG — low-card dropdown returns exactly the distinct values.
	cat, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := valueStrings(cat); !reflect.DeepEqual(got, []string{"api-gateway", "order-service"}) {
		t.Fatalf("[catalog] service.name = %v", got)
	}

	// 2. HLL — trace_id AND span_id cardinality ≈ n (both tap branches).
	if card := s.catalog.Cardinality("trace_id"); math.Abs(float64(card)-float64(n))/float64(n) > 0.03 {
		t.Fatalf("[hll] trace_id cardinality = %d (true %d)", card, n)
	}
	if card := s.catalog.Cardinality("span_id"); math.Abs(float64(card)-float64(n))/float64(n) > 0.03 {
		t.Fatalf("[hll] span_id cardinality = %d (true %d)", card, n)
	}

	// 3. FILE-META — dual-write parity with the sidecar content.
	fm, ok := s.catalog.FileMeta(part, fi.Key)
	if !ok || fm.RowCount != fi.RowCount || fm.SchemaFingerprint != fi.SchemaFingerprint {
		t.Fatalf("[file-meta] facet=%+v vs fi(rc=%d sf=%s)", fm, fi.RowCount, fi.SchemaFingerprint)
	}

	// 4. BLOOM — a value present in the file is found.
	bloomGot, ok := s.catalog.BloomMayContain(part, []string{fi.Key}, "service.name", "api-gateway")
	found := false
	for _, k := range bloomGot {
		if k == fi.Key {
			found = true
		}
	}
	if !ok || !found {
		t.Fatalf("[bloom] service.name=api-gateway not found for %s (ok=%v got=%v)", fi.Key, ok, bloomGot)
	}

	// 5. COLD-START WARM — a fresh catalog rebuilds catalog + file-meta from the
	// manifest (bloom needs the bundle persist/warm, A3 — not asserted here).
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	s.WarmCatalog(context.Background())
	warmCat, _ := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if got := valueStrings(warmCat); !reflect.DeepEqual(got, []string{"api-gateway", "order-service"}) {
		t.Fatalf("[warm catalog] service.name = %v", got)
	}
	if _, ok := s.catalog.FileMeta(part, fi.Key); !ok {
		t.Fatal("[warm file-meta] facet missing after WarmCatalog")
	}
}

// TestInteg_PmetaCatalog_TraceRowTap covers the trace-row flush path (tapTraceRows):
// flushing trace rows feeds trace_id + span_id into their per-field HLLs.
func TestInteg_PmetaCatalog_TraceRowTap(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true, AlwaysSketchFields: []string{"trace_id", "span_id"}}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	bw.catalogObserver = &catalogObserver{store: s.catalog, sketch: sketchSet(s.cfg.Pmeta.AlwaysSketchFields)}

	const n = 1500
	now := time.Now()
	rows := make([]schema.TraceRow, n)
	for i := 0; i < n; i++ {
		rows[i] = schema.TraceRow{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			ServiceName:       "api-gateway",
			SpanName:          "GET /x",
			TraceID:           fmt.Sprintf("%032x", i),
			SpanID:            fmt.Sprintf("%016x", i),
		}
	}
	bw.AddTraceRows(rows)
	bw.triggerFlush()

	if c := s.catalog.Cardinality("trace_id"); math.Abs(float64(c)-float64(n))/float64(n) > 0.03 {
		t.Fatalf("trace_id cardinality = %d (true %d)", c, n)
	}
	if c := s.catalog.Cardinality("span_id"); math.Abs(float64(c)-float64(n))/float64(n) > 0.03 {
		t.Fatalf("span_id cardinality = %d (true %d)", c, n)
	}
}

// TestInteg_PmetaCatalog_LabelsParityWithLabelIndex is the labels-fold parity gate:
// the catalog facet IS the fold of cache.LabelIndex, so fed the same label data
// both must return the same field names and field values (the dual-write contract,
// before _label_index.json is retired at the flip).
func TestInteg_PmetaCatalog_LabelsParityWithLabelIndex(t *testing.T) {
	data := map[string][]string{
		"service.name":       {"api-gateway", "order-service", "user-service"},
		"severity_text":      {"ERROR", "INFO", "WARN"},
		"k8s.namespace.name": {"prod", "staging"},
	}

	li := cache.NewLabelIndex()
	for field, vals := range data {
		li.Add(field, vals)
	}

	store := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	// Two files contributing overlapping values — the catalog unions, like the index.
	store.OnFileFlush(pmeta.FileContribution{Partition: "p", FileKey: "f1", Labels: data})
	store.OnFileFlush(pmeta.FileContribution{Partition: "p", FileKey: "f2", Labels: map[string][]string{
		"service.name": {"api-gateway", "payments"},
	}})
	li.Add("service.name", []string{"api-gateway", "payments"})

	// Field names parity.
	gotNames := append([]string(nil), store.FieldNames("p")...)
	wantNames := append([]string(nil), li.GetFieldNames()...)
	sort.Strings(gotNames)
	sort.Strings(wantNames)
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("field names: catalog=%v labelIndex=%v", gotNames, wantNames)
	}

	// Field values parity, per field.
	for field := range data {
		gotVals := append([]string(nil), store.FieldValues("p", field, "", 100)...)
		wantVals := append([]string(nil), li.GetFieldValues(field, 100)...)
		sort.Strings(gotVals)
		sort.Strings(wantVals)
		if !reflect.DeepEqual(gotVals, wantVals) {
			t.Fatalf("field %q values: catalog=%v labelIndex=%v", field, gotVals, wantVals)
		}
	}
}

// TestInteg_PmetaFlip_FieldNamesAndBloom covers the two read-flips added on top of
// the file-meta flip: (1) labels field_names served from the catalog, and (2) the
// bloom flip keeping a file whose bloom-indexed value is present (the
// no-false-negative safety property — the facet path must never exclude a file that
// actually holds the queried value).
func TestInteg_PmetaFlip_FieldNamesAndBloom(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog}

	now := time.Now()
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "msg", ServiceName: "api-gateway", TraceID: "t1", SpanID: "s1"},
		{TimestampUnixNano: now.Add(time.Millisecond).UnixNano(), Body: "msg", ServiceName: "order-service", TraceID: "t2", SpanID: "s2"},
	})
	bw.triggerFlush()

	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	if len(files) == 0 {
		t.Fatal("no file after flush")
	}
	fi := files[0]
	q := mustParseQueryWithTime(t, "*", now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())

	// (1) labels field_names flip: the catalog serves field names, incl. service.name.
	names := s.catalogFieldNames(q)
	hasSvc := false
	for _, n := range names {
		if n == "service.name" {
			hasSvc = true
		}
	}
	if !hasSvc {
		t.Fatalf("catalogFieldNames missing service.name: %v", names)
	}

	// (2) bloom flip: a file whose bloom-indexed value IS present must never be
	// excluded by the facet path (blooms have no false negatives).
	if s.checkFileBloom(context.Background(), fi, "service.name:api-gateway") {
		t.Fatal("checkFileBloom wrongly excluded a file containing service.name=api-gateway")
	}
}

// TestInteg_PmetaRetire_SkipsFileMetaSidecar verifies the retire-sidecar-writes flag:
// with it on, no _file_metadata.json is written to S3, yet the file metadata is still
// available from the in-RAM facet (the footer is the cold-restart fallback).
func TestInteg_PmetaRetire_SkipsFileMetaSidecar(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true, RetireSidecarWrites: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog}
	bw.retireSidecars = true

	now := time.Now()
	bw.AddLogRows([]schema.LogRow{{TimestampUnixNano: now.UnixNano(), Body: "m", ServiceName: "api-gateway"}})
	bw.triggerFlush()

	// retire on → the _file_metadata.json sidecar is NOT written (the gate is
	// synchronous: no goroutine is spawned, so this is race-free).
	mock.mu.RLock()
	for k := range mock.files {
		if strings.HasSuffix(k, metadataSidecarSuffix) {
			mock.mu.RUnlock()
			t.Fatalf("retire-sidecars on but _file_metadata.json was written: %s", k)
		}
	}
	mock.mu.RUnlock()

	// ...yet the file metadata is still available from the facet.
	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	if len(files) == 0 {
		t.Fatal("no file after flush")
	}
	fi := files[0]
	if _, ok := s.catalog.FileMeta(manifest.ExtractPartition(fi.Key), fi.Key); !ok {
		t.Fatal("facet missing file metadata after retire flush")
	}
}

const metadataSidecarSuffix = "_file_metadata.json"

// TestInteg_PmetaFlip_ORBranchFacet covers the OR-branch bloom read-flip: the
// facet-based union match keeps a file whose bloom-indexed value is present (blooms
// never false-negate, so the facet path must never drop a matching file), and
// reports ok=false for a partition the facet doesn't carry (→ caller falls back).
func TestInteg_PmetaFlip_ORBranchFacet(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog}

	now := time.Now()
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "m", ServiceName: "api-gateway"},
		{TimestampUnixNano: now.Add(time.Millisecond).UnixNano(), Body: "m", ServiceName: "order-service"},
	})
	bw.triggerFlush()

	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	if len(files) == 0 {
		t.Fatal("no file after flush")
	}
	fi := files[0]
	part := manifest.ExtractPartition(fi.Key)
	checks := [][]bloomindex.ColumnCheck{{{Column: "service.name", Value: "api-gateway"}}}

	// present value → file is in the union (never dropped).
	union, ok := s.facetBloomUnionMatch(part, []string{fi.Key}, checks)
	if !ok {
		t.Fatal("facetBloomUnionMatch ok=false for a partition the facet carries")
	}
	if !union[fi.Key] {
		t.Fatalf("OR-branch facet dropped a file containing service.name=api-gateway: %v", union)
	}

	// a partition the facet does not carry → ok=false so the caller falls back.
	if _, ok := s.facetBloomUnionMatch("dt=1999-01-01/hour=00", []string{"x"}, checks); ok {
		t.Fatal("facetBloomUnionMatch should report ok=false for an unknown partition")
	}
}

// TestInteg_PmetaFlip_LogsBloomColdRestart is the logs twin of the traces
// cold-restart test (the absent-value assertions are the ones that catch a
// silently-empty facet — a present-value-only check passes vacuously because
// unknown keys are kept). Under retire-sidecar-writes after a cold restart the
// bundle-warmed facet is the ONLY bloom source: both pre-filter helpers must
// (a) prune an absent value, (b) never drop a file holding the value, and
// (c) keep files of partitions no bloom knows.
func TestInteg_PmetaFlip_LogsBloomColdRestart(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true, RetireSidecarWrites: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog, pool: s.pool}
	// retire mode: NO legacy bloom observer wired — the facet is the only feed.

	now := time.Now()
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "m", ServiceName: "api-gateway"},
	})
	bw.triggerFlush()

	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	if len(files) == 0 {
		t.Fatal("no file after flush")
	}
	fi := files[0]
	part := manifest.ExtractPartition(fi.Key)

	// Cold restart: bundles persisted, fresh catalog warmed ONLY from S3.
	if _, err := s.catalog.PersistDirty(context.Background(), poolObjectStore{s.pool}); err != nil {
		t.Fatal(err)
	}
	fresh := newCatalogStore(s.cfg.Pmeta, "logs/")
	if res := fresh.WarmPartitions(context.Background(), poolObjectStore{s.pool}, []string{part}, 2); res.Loaded == 0 {
		t.Fatalf("bundle not warmed (NeedsRebuild=%v)", res.NeedsRebuild)
	}
	s.catalog = fresh

	ctx := context.Background()
	keys := []string{fi.Key}
	presentCol := []bloomColumnValues{{Column: "service.name", Values: []string{"api-gateway"}}}
	absentCol := []bloomColumnValues{{Column: "service.name", Values: []string{"no-such-service-xyz"}}}
	presentBranch := [][]bloomindex.ColumnCheck{{{Column: "service.name", Value: "api-gateway"}}}
	absentBranch := [][]bloomindex.ColumnCheck{{{Column: "service.name", Value: "no-such-service-xyz"}}}

	// single-set path (bloomColumnIntersect)
	if m, ok := s.bloomColumnIntersect(ctx, part, keys, presentCol); !ok || !m[fi.Key] {
		t.Fatalf("[single-set] dropped a file holding the value (ok=%v m=%v)", ok, m)
	}
	if m, ok := s.bloomColumnIntersect(ctx, part, keys, absentCol); !ok || m[fi.Key] {
		t.Fatalf("[single-set] absent value not pruned (ok=%v m=%v) — facet empty?", ok, m)
	}

	// OR-branch path (bloomUnionMatch)
	if m, ok := s.bloomUnionMatch(ctx, part, keys, presentBranch); !ok || !m[fi.Key] {
		t.Fatalf("[or-branch] dropped a file holding the value (ok=%v m=%v)", ok, m)
	}
	if m, ok := s.bloomUnionMatch(ctx, part, keys, absentBranch); !ok || m[fi.Key] {
		t.Fatalf("[or-branch] absent value not pruned (ok=%v m=%v) — facet empty?", ok, m)
	}

	// Unknown partition → no bloom anywhere → ok=false → caller keeps the files.
	if _, ok := s.bloomColumnIntersect(ctx, "dt=1999-01-01/hour=00", []string{"x"}, presentCol); ok {
		t.Fatal("[single-set] unknown partition should report ok=false (keep files)")
	}
	if _, ok := s.bloomUnionMatch(ctx, "dt=1999-01-01/hour=00", []string{"x"}, presentBranch); ok {
		t.Fatal("[or-branch] unknown partition should report ok=false (keep files)")
	}
}
