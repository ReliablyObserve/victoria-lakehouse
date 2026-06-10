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

// TestInteg_PmetaRetire_SkipsFileMetaSidecar verifies the sidecar-write retirement:
// no _file_metadata.json is ever written to S3 (the writer is gone), yet the file
// metadata is still available from the in-RAM facet (the footer is the cold-restart
// fallback).
func TestInteg_PmetaRetire_SkipsFileMetaSidecar(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog}

	now := time.Now()
	bw.AddLogRows([]schema.LogRow{{TimestampUnixNano: now.UnixNano(), Body: "m", ServiceName: "api-gateway"}})
	bw.triggerFlush()

	// The _file_metadata.json sidecar is NOT written — unconditionally (no
	// sidecar writer exists anymore, so no goroutine is spawned; race-free).
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
// unknown keys are kept). With sidecar writes retired, after a cold restart the
// bundle-warmed facet is the ONLY bloom source: both pre-filter helpers must
// (a) prune an absent value, (b) never drop a file holding the value, and
// (c) keep files of partitions no bloom knows.
func TestInteg_PmetaFlip_LogsBloomColdRestart(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog, pool: s.pool}
	// The legacy bloom observer no longer exists — the facet is the only feed.

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

// TestInteg_PmetaFlip_WarmMetadataViaRealAdapter exercises the PRODUCTION file-meta
// flip path end-to-end: catalogFileMetaProvider (the real facet→manifest adapter,
// not the unit-test mock) + Storage.WarmCatalogFromS3 (the production bundle warm).
// Flush → persist bundles → fresh storage with empty catalog + zeroed manifest meta
// → WarmCatalogFromS3 → EnrichFromProvider must fill FileInfo from the facet alone.
func TestInteg_PmetaFlip_WarmMetadataViaRealAdapter(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog, pool: s.pool}

	now := time.Now()
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "m", ServiceName: "api-gateway"},
	})
	bw.triggerFlush()
	if _, err := s.catalog.PersistDirty(context.Background(), poolObjectStore{s.pool}); err != nil {
		t.Fatal(err)
	}

	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	if len(files) == 0 {
		t.Fatal("no file after flush")
	}
	fi := files[0]
	wantRows := fi.RowCount

	// Cold restart: same manifest keys but ZEROED metadata (what a bare S3 list
	// reconstruction yields), an EMPTY catalog, then the production warm.
	s.manifest = manifest.New("bucket", "logs/")
	s.manifest.AddFile(manifest.ExtractPartition(fi.Key), manifest.FileInfo{Key: fi.Key, Bucket: fi.Bucket, Size: fi.Size})
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	s.WarmCatalogFromS3(context.Background()) // production wrapper (poolObjectStore inside)

	// The real adapter serves the facet's metadata.
	fm, ok := catalogFileMetaProvider{store: s.catalog}.FileMeta(manifest.ExtractPartition(fi.Key), fi.Key)
	if !ok || fm.RowCount != wantRows {
		t.Fatalf("real adapter FileMeta = (%+v, %v), want RowCount=%d", fm, ok, wantRows)
	}

	// And the manifest enrich-from-facet path fills the zeroed FileInfo.
	enriched, uncovered := s.manifest.EnrichFromProvider(catalogFileMetaProvider{store: s.catalog})
	if enriched != 1 || len(uncovered) != 0 {
		t.Fatalf("EnrichFromProvider via real adapter = (%d, %v), want (1, [])", enriched, uncovered)
	}
	got := s.manifest.GetFilesForRange(0, 1<<62)
	if len(got) != 1 || got[0].RowCount != wantRows {
		t.Fatalf("manifest not enriched from facet: %+v", got)
	}
}

// TestInteg_EnrichEquivalence_ProviderVsSidecar is the sidecar-retirement
// equivalence gate for cold-start metadata: enriching a zeroed manifest from the
// in-RAM facet (EnrichFromProvider via the real catalogFileMetaProvider) must
// reconstruct EXACTLY the same FileInfo metadata as enriching it from the legacy
// _file_metadata.json sidecars (LoadSidecars). If these ever diverge, retiring the
// sidecar would silently change what a restarted pod believes about its files.
func TestInteg_EnrichEquivalence_ProviderVsSidecar(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog}

	// Two partitions (2h apart) → two files, so the equivalence is asserted
	// across more than one sidecar/bundle.
	base := time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: base.UnixNano(), Body: "a", ServiceName: "api-gateway"},
		{TimestampUnixNano: base.Add(time.Second).UnixNano(), Body: "b", ServiceName: "order-service"},
		{TimestampUnixNano: base.Add(2 * time.Hour).UnixNano(), Body: "c", ServiceName: "user-service"},
	})
	bw.triggerFlush()

	files := s.manifest.GetFilesForRange(base.Add(-time.Hour).UnixNano(), base.Add(3*time.Hour).UnixNano())
	if len(files) < 2 {
		t.Fatalf("want >= 2 files across two partitions, got %d", len(files))
	}

	// Build the _file_metadata.json sidecars directly from the flushed files and
	// put them in the mock S3 (the production sidecar writer is retired; this is
	// the same content it used to upload).
	parts := map[string]map[string]manifest.FileMeta{}
	for _, fi := range files {
		p := manifest.ExtractPartition(fi.Key)
		if parts[p] == nil {
			parts[p] = map[string]manifest.FileMeta{}
		}
		parts[p][fi.Key] = manifest.FileInfoToMeta(fi)
	}
	for p, fm := range parts {
		data, err := manifest.MarshalFileMetaSidecar(&manifest.FileMetaSidecar{Files: fm})
		if err != nil {
			t.Fatal(err)
		}
		mock.putFile(manifest.MetadataSidecarKey("logs/", p), data)
	}

	// TWO fresh manifests with the same keys but ZEROED metadata (what a bare S3
	// list reconstruction yields): one enriched from the facet, one from sidecars.
	mProv := manifest.New("test-bucket", "logs/")
	mSide := manifest.New("test-bucket", "logs/")
	for _, fi := range files {
		p := manifest.ExtractPartition(fi.Key)
		mProv.AddFile(p, manifest.FileInfo{Key: fi.Key, Bucket: fi.Bucket, Size: fi.Size})
		mSide.AddFile(p, manifest.FileInfo{Key: fi.Key, Bucket: fi.Bucket, Size: fi.Size})
	}

	enriched, uncovered := mProv.EnrichFromProvider(catalogFileMetaProvider{store: s.catalog})
	if enriched != len(files) || len(uncovered) != 0 {
		t.Fatalf("EnrichFromProvider = (%d, %v), want (%d, [])", enriched, uncovered, len(files))
	}
	if n := mSide.LoadSidecars(context.Background(), s.pool.S3Client(), 4); n != len(files) {
		t.Fatalf("LoadSidecars enriched %d files, want %d", n, len(files))
	}

	bySide := make(map[string]manifest.FileInfo, len(files))
	for _, fi := range mSide.GetFilesForRange(0, 1<<62) {
		bySide[fi.Key] = fi
	}
	byProv := mProv.GetFilesForRange(0, 1<<62)
	if len(byProv) != len(files) || len(bySide) != len(files) {
		t.Fatalf("enriched manifests lost files: provider=%d sidecar=%d want=%d", len(byProv), len(bySide), len(files))
	}
	for _, p := range byProv {
		sc, ok := bySide[p.Key]
		if !ok {
			t.Fatalf("sidecar-enriched manifest missing %s", p.Key)
		}
		if p.RowCount == 0 {
			t.Fatalf("file %s not actually enriched (RowCount=0)", p.Key)
		}
		if p.RowCount != sc.RowCount || p.MinTimeNs != sc.MinTimeNs || p.MaxTimeNs != sc.MaxTimeNs ||
			p.RawBytes != sc.RawBytes || p.SchemaFingerprint != sc.SchemaFingerprint {
			t.Fatalf("provider vs sidecar enrichment diverged for %s:\n provider=%+v\n sidecar=%+v", p.Key, p, sc)
		}
	}
}

// TestInteg_GetFieldNames_CatalogFlip drives the field_names read-flip END-TO-END
// through s.GetFieldNames (not the catalogFieldNames helper): after a real flush
// with --pmeta on the result must include service.name, and flipping the catalog
// off (s.catalog=nil) must return the SAME name set via the legacy path (parity).
// The degraded twin then deletes the parquet objects and clears every cache so the
// footer-hits path yields nothing — proving the catalog fallback branch (pmeta on)
// and the labelIndex branch (pmeta off) both still serve the names.
func TestInteg_GetFieldNames_CatalogFlip(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog}

	now := time.Now()
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", ServiceName: "api-gateway"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", ServiceName: "order-service"},
	})
	bw.triggerFlush()

	q := mustParseQueryWithTime(t, "*", now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	hasName := func(vs []logstorage.ValueWithHits, name string) bool {
		for _, v := range vs {
			if v.Value == name {
				return true
			}
		}
		return false
	}

	on, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	if !hasName(on, "service.name") {
		t.Fatalf("GetFieldNames(pmeta on) missing service.name: %v", valueStrings(on))
	}

	catalog := s.catalog
	s.catalog = nil
	off, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(valueStrings(on), valueStrings(off)) {
		t.Fatalf("field-names cross-path mismatch:\n catalog on=%v\n legacy=%v", valueStrings(on), valueStrings(off))
	}

	// Degraded twin: parquet objects gone + footer/mem caches cleared → the
	// footer-hits path yields nothing, so GetFieldNames must take the fallbacks.
	s.catalog = catalog
	mock.mu.Lock()
	for k := range mock.files {
		if strings.HasSuffix(k, ".parquet") {
			delete(mock.files, k)
		}
	}
	mock.mu.Unlock()
	s.footerCache = NewFooterCache(16)
	s.memCache = cache.NewLRU(1024 * 1024)

	on2, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	if !hasName(on2, "service.name") {
		t.Fatalf("catalog fallback branch (footers gone) missing service.name: %v", valueStrings(on2))
	}
	s.catalog = nil
	off2, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	if !hasName(off2, "service.name") {
		t.Fatalf("legacy labelIndex branch (footers gone) missing service.name: %v", valueStrings(off2))
	}
}

// TestInteg_CatalogFieldValues_MultiPartitionUnion: a flush spanning TWO partitions
// (2h apart → different hour=…) with different service.name values per partition
// must answer a whole-range GetFieldValues with the deduped UNION, sorted — the
// multi-partition union semantics of the catalog fast-path (catalogFieldValues
// walks every partition in the query range, not just the first).
func TestInteg_CatalogFieldValues_MultiPartitionUnion(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog}

	t1 := time.Date(2026, 6, 9, 10, 15, 0, 0, time.UTC)
	t2 := t1.Add(2 * time.Hour) // different hour ⇒ different partition
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: t1.UnixNano(), Body: "a", ServiceName: "checkout"},
		{TimestampUnixNano: t1.Add(time.Second).UnixNano(), Body: "b", ServiceName: "shared-svc"},
		{TimestampUnixNano: t2.UnixNano(), Body: "c", ServiceName: "billing"},
		{TimestampUnixNano: t2.Add(time.Second).UnixNano(), Body: "d", ServiceName: "shared-svc"},
	})
	bw.triggerFlush()

	p1 := partitionFromNano(t1.UnixNano())
	p2 := partitionFromNano(t2.UnixNano())
	if p1 == p2 {
		t.Fatalf("test rows must land in two partitions, both got %s", p1)
	}
	// Each partition's catalog holds ONLY its own values (no cross-partition bleed).
	if got := s.catalog.FieldValues(p1, "service.name", "", 0); !reflect.DeepEqual(got, []string{"checkout", "shared-svc"}) {
		t.Fatalf("partition %s catalog = %v", p1, got)
	}
	if got := s.catalog.FieldValues(p2, "service.name", "", 0); !reflect.DeepEqual(got, []string{"billing", "shared-svc"}) {
		t.Fatalf("partition %s catalog = %v", p2, got)
	}

	q := mustParseQueryWithTime(t, "*", t1.Add(-time.Hour).UnixNano(), t2.Add(time.Hour).UnixNano())
	before := metrics.CatalogValueLookups.Get("catalog")
	got, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatal(err)
	}
	// Raw order, NOT re-sorted: asserts the union is deduped (shared-svc once)
	// AND already sorted as returned.
	raw := make([]string, len(got))
	for i, v := range got {
		raw[i] = v.Value
	}
	if want := []string{"billing", "checkout", "shared-svc"}; !reflect.DeepEqual(raw, want) {
		t.Fatalf("whole-range GetFieldValues = %v, want deduped sorted union %v", raw, want)
	}
	if metrics.CatalogValueLookups.Get("catalog") <= before {
		t.Fatal("union was not served from the catalog fast-path")
	}
}

// TestInteg_CatalogTruncatedField_FallsToScan: a flushed file with more distinct
// values for one label field than the extractor cap (maxLabelsPerField) arrives
// with a CAPPED — i.e. possibly incomplete — value list, so the catalog must mark
// the field high-card and serve NOTHING for it (never a silently truncated list),
// and GetFieldValues must fall through to the scan, which returns the TRUE values.
func TestInteg_CatalogTruncatedField_FallsToScan(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
	s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.catalogObserver = &catalogObserver{store: s.catalog}

	const n = 120 // > maxLabelsPerField (100) distinct pod names → extractor caps the list
	base := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	rows := make([]schema.LogRow, n)
	for i := range rows {
		rows[i] = schema.LogRow{
			TimestampUnixNano: base.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Body:              "msg",
			ServiceName:       "api-gateway",
			K8sPodName:        fmt.Sprintf("pod-%03d", i),
		}
	}
	bw.AddLogRows(rows)
	bw.triggerFlush()

	files := s.manifest.GetFilesForRange(base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	if len(files) == 0 {
		t.Fatal("no file after flush")
	}
	if got := len(files[0].Labels["k8s.pod.name"]); got != maxLabelsPerField {
		t.Fatalf("extractor cap: manifest labels hold %d pod names, want exactly %d", got, maxLabelsPerField)
	}

	// (a) The catalog must NOT serve the truncated field (high-card behavior)…
	part := partitionFromNano(base.UnixNano())
	if got := s.catalog.FieldValues(part, "k8s.pod.name", "", 0); got != nil {
		t.Fatalf("catalog served %d values for a truncated field — a capped list must never be authoritative", len(got))
	}
	// …but the field NAME stays known, and an untruncated sibling is still served.
	names := s.catalog.FieldNames(part)
	hasPod := false
	for _, n := range names {
		if n == "k8s.pod.name" {
			hasPod = true
		}
	}
	if !hasPod {
		t.Fatalf("high-card field must remain a known field NAME, got %v", names)
	}
	if got := s.catalog.FieldValues(part, "service.name", "", 0); !reflect.DeepEqual(got, []string{"api-gateway"}) {
		t.Fatalf("untruncated sibling field = %v, want [api-gateway]", got)
	}

	// (b) GetFieldValues falls through to the scan and returns the TRUE values.
	q := mustParseQueryWithTime(t, "*", base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	beforeScan := metrics.CatalogValueLookups.Get("scan")
	got, err := s.GetFieldValues(context.Background(), nil, q, "k8s.pod.name", 0)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]string, n)
	for i := range want {
		want[i] = fmt.Sprintf("pod-%03d", i)
	}
	if vals := valueStrings(got); !reflect.DeepEqual(vals, want) {
		t.Fatalf("scan fallback returned %d values (want all %d true pod names): %v", len(vals), n, vals)
	}
	if metrics.CatalogValueLookups.Get("scan") <= beforeScan {
		t.Fatal("catalog miss not recorded — was the truncated field served from the catalog?")
	}
}

// TestInteg_PmetaFlip_CheckFileBloomFacetExcludes covers checkFileBloom's facet
// EXCLUDE branch (the prior tests only asserted the keep side): a value absent from
// the file's bloom must skip the file via the facet, with NO legacy .bloom present
// (the per-file .bloom writer is retired), and the metric label must be the facet one.
func TestInteg_PmetaFlip_CheckFileBloomFacetExcludes(t *testing.T) {
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
	})
	bw.triggerFlush()
	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	if len(files) == 0 {
		t.Fatal("no file after flush")
	}
	fi := files[0]

	if !s.checkFileBloom(context.Background(), fi, `service.name:="no-such-service-xyz"`) {
		t.Fatal("facet bloom should exclude a file lacking the value (facet empty or check bypassed?)")
	}
	if s.checkFileBloom(context.Background(), fi, `service.name:="api-gateway"`) {
		t.Fatal("facet bloom wrongly excluded a file containing the value")
	}
}
