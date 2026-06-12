package parquets3

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

const pmwPartition = "dt=2026-06-01/hour=10"

// pmwTenantPartition is the tenant-isolated partition the production code keys
// facets/bundles by (the full key dir) — the test keys are "logs/"+pmwPartition+
// "/…", so the facet lives under "logs/"+pmwPartition while the manifest keys
// files by the pure pmwPartition.
const pmwTenantPartition = "logs/" + pmwPartition

// pmwContribution returns a FileContribution for the test partition. The facet is
// keyed by the tenant-isolated partition (derived from the file key), matching the
// live writer flush, NOT the manifest's pure dt=/hour= partition.
func pmwContribution(key string, labels map[string][]string) pmeta.FileContribution {
	return pmeta.FileContribution{
		Partition:         manifest.ExtractTenantPartition(key),
		FileKey:           key,
		RowCount:          42,
		MinTimeNs:         1_000,
		MaxTimeNs:         2_000,
		RawBytes:          4096,
		SchemaFingerprint: "fp-1",
		Labels:            labels,
	}
}

// TestCatalogFileMetaProvider_HitAndMiss verifies the manifest-side
// adapter: a flushed file's metadata is served field-for-field from the
// facet; an unknown file reports a clean miss so the manifest falls
// back to the legacy sidecar path.
func TestCatalogFileMetaProvider_HitAndMiss(t *testing.T) {
	st := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	st.OnFileFlush(pmwContribution("logs/"+pmwPartition+"/f1.parquet",
		map[string][]string{"service.name": {"api"}}))

	p := catalogFileMetaProvider{store: st}

	got, ok := p.FileMeta(pmwPartition, "logs/"+pmwPartition+"/f1.parquet")
	if !ok {
		t.Fatal("expected FileMeta hit for flushed file")
	}
	want := manifest.FileMeta{
		RowCount:          42,
		MinTimeNs:         1_000,
		MaxTimeNs:         2_000,
		RawBytes:          4096,
		SchemaFingerprint: "fp-1",
		Labels:            map[string][]string{"service.name": {"api"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("FileMeta = %+v, want %+v", got, want)
	}

	if _, ok := p.FileMeta(pmwPartition, "logs/"+pmwPartition+"/absent.parquet"); ok {
		t.Error("expected miss for unknown file key")
	}
	if _, ok := p.FileMeta("dt=1999-01-01/hour=00", "x"); ok {
		t.Error("expected miss for unknown partition")
	}
}

func TestSketchSet(t *testing.T) {
	// The built-in id columns (schema.DefaultSketchIDColumns) are always present,
	// even with no operator config, so the promoted id columns are sketched +
	// persisted out of the box regardless of always_sketch_fields.
	for _, in := range [][]string{nil, {}} {
		got := sketchSet(in)
		for _, f := range schema.DefaultSketchIDColumns {
			if !got[f] {
				t.Errorf("sketchSet(%v) = %v, missing default %q", in, got, f)
			}
		}
		if len(got) != len(schema.DefaultSketchIDColumns) {
			t.Errorf("sketchSet(%v) = %v, want only the %d defaults", in, got, len(schema.DefaultSketchIDColumns))
		}
	}
	// Operator-configured fields are unioned in alongside the defaults, deduped
	// (trace_id is already a default, so it must not double-count).
	got := sketchSet([]string{"trace_id", "request_id"})
	if !got["request_id"] {
		t.Errorf("sketchSet missing configured request_id: %v", got)
	}
	for _, f := range schema.DefaultSketchIDColumns {
		if !got[f] {
			t.Errorf("sketchSet missing default %q: %v", f, got)
		}
	}
	if len(got) != len(schema.DefaultSketchIDColumns)+1 {
		t.Errorf("sketchSet = %v, want defaults + request_id (trace_id deduped)", got)
	}
}

// TestCatalogObserver_TapRows verifies the always-sketch HLL feed for
// both row shapes: distinct ids must move the cardinality estimate,
// and the taps must be safe no-ops when the observer/sketch is absent.
func TestCatalogObserver_TapRows(t *testing.T) {
	t.Run("log rows feed trace_id and span_id sketches", func(t *testing.T) {
		st := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
		o := &catalogObserver{store: st, sketch: sketchSet([]string{"trace_id", "span_id"})}
		o.tapLogRows("p", []schema.LogRow{
			{TraceID: "t1", SpanID: "s1"},
			{TraceID: "t2", SpanID: "s2"},
			{TraceID: "t3", SpanID: "s3"},
		})
		if c := st.Cardinality("trace_id"); c < 2 || c > 4 {
			t.Errorf("trace_id cardinality = %d, want ~3", c)
		}
		if c := st.Cardinality("span_id"); c < 2 || c > 4 {
			t.Errorf("span_id cardinality = %d, want ~3", c)
		}
	})

	t.Run("trace rows feed sketches", func(t *testing.T) {
		st := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
		o := &catalogObserver{store: st, sketch: sketchSet([]string{"trace_id", "span_id"})}
		o.tapTraceRows("p", []schema.TraceRow{
			{TraceID: "t1", SpanID: "s1"},
			{TraceID: "t2", SpanID: "s2"},
		})
		if c := st.Cardinality("trace_id"); c < 1 || c > 3 {
			t.Errorf("trace_id cardinality = %d, want ~2", c)
		}
	})

	t.Run("promoted id columns sketched by default (container.id, service.instance.id)", func(t *testing.T) {
		st := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
		// sketchSet(nil) includes the built-in DefaultSketchIDColumns, so the
		// promoted id columns are sketched without any operator config.
		o := &catalogObserver{store: st, sketch: sketchSet(nil)}
		o.tapLogRows("p", []schema.LogRow{
			{ContainerID: "c1", ServiceInstanceID: "i1"},
			{ContainerID: "c2", ServiceInstanceID: "i2"},
			{ContainerID: "c3", ServiceInstanceID: "i3"},
		})
		if c := st.Cardinality("container.id"); c < 2 || c > 4 {
			t.Errorf("container.id cardinality = %d, want ~3", c)
		}
		if c := st.Cardinality("service.instance.id"); c < 2 || c > 4 {
			t.Errorf("service.instance.id cardinality = %d, want ~3", c)
		}
	})

	t.Run("trace rows feed promoted id columns too", func(t *testing.T) {
		st := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
		o := &catalogObserver{store: st, sketch: sketchSet(nil)}
		o.tapTraceRows("p", []schema.TraceRow{
			{ContainerID: "c1", ServiceInstanceID: "i1"},
			{ContainerID: "c2", ServiceInstanceID: "i2"},
		})
		if c := st.Cardinality("container.id"); c < 1 || c > 3 {
			t.Errorf("container.id cardinality = %d, want ~2", c)
		}
	})

	t.Run("no sketch set is a no-op", func(t *testing.T) {
		st := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
		o := &catalogObserver{store: st} // sketch nil
		o.tapLogRows("p", []schema.LogRow{{TraceID: "t1"}})
		o.tapTraceRows("p", []schema.TraceRow{{TraceID: "t1"}})
		if c := st.Cardinality("trace_id"); c != 0 {
			t.Errorf("cardinality must stay 0 without a sketch set, got %d", c)
		}
	})

	t.Run("nil observer and nil store are safe", func(t *testing.T) {
		var o *catalogObserver
		o.tapLogRows("p", []schema.LogRow{{TraceID: "t"}})
		o.tapTraceRows("p", []schema.TraceRow{{TraceID: "t"}})
		o2 := &catalogObserver{sketch: sketchSet([]string{"trace_id"})}
		o2.tapLogRows("p", []schema.LogRow{{TraceID: "t"}})
		o2.tapTraceRows("p", []schema.TraceRow{{TraceID: "t"}})
	})
}

// TestCatalogFieldNames_RangeUnion: field names are unioned across the
// partitions overlapping the query window and sorted; an empty catalog
// yields nil so the caller falls through to the legacy labelIndex.
func TestCatalogFieldNames_RangeUnion(t *testing.T) {
	s := testStorage()
	s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")

	now := time.Date(2026, 6, 1, 10, 30, 0, 0, time.UTC)
	key := "logs/dt=2026-06-01/hour=10/f1.parquet"
	s.manifest.AddFile("dt=2026-06-01/hour=10", manifest.FileInfo{
		Key:       key,
		Size:      100,
		MinTimeNs: now.Add(-time.Minute).UnixNano(),
		MaxTimeNs: now.Add(time.Minute).UnixNano(),
	})
	s.catalog.OnFileFlush(pmeta.FileContribution{
		Partition: manifest.ExtractTenantPartition(key),
		FileKey:   key,
		Labels:    map[string][]string{"service.name": {"api"}, "env": {"prod"}},
	})

	q := mustParseQueryWithTime(t, "*", now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	got := s.catalogFieldNames(q)
	if !reflect.DeepEqual(got, []string{"env", "service.name"}) {
		t.Errorf("catalogFieldNames = %v, want [env service.name] (sorted union)", got)
	}

	// Time range with no overlapping files → nil (fall through to legacy).
	qOld := mustParseQueryWithTime(t, "*",
		time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		time.Date(1999, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano())
	if got := s.catalogFieldNames(qOld); got != nil {
		t.Errorf("expected nil for non-overlapping range, got %v", got)
	}
}

// TestWarmCatalog_RebuildsFromManifest: the manifest's per-file label
// maps must repopulate the catalog at startup — and must NOT mark
// bundles dirty (replay, not flush), or every restart would re-PUT
// every partition bundle.
func TestWarmCatalog_RebuildsFromManifest(t *testing.T) {
	s := testStorage()
	s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")

	now := time.Date(2026, 6, 1, 10, 30, 0, 0, time.UTC)
	s.manifest.AddFile("dt=2026-06-01/hour=10", manifest.FileInfo{
		Key:       "logs/dt=2026-06-01/hour=10/f1.parquet",
		Size:      100,
		MinTimeNs: now.UnixNano(),
		MaxTimeNs: now.UnixNano(),
		Labels:    map[string][]string{"service.name": {"api", "worker"}},
	})

	s.WarmCatalog(context.Background())

	got := s.catalog.FieldValues(pmwTenantPartition, "service.name", "", 0)
	if !reflect.DeepEqual(got, []string{"api", "worker"}) {
		t.Fatalf("warm catalog service.name = %v, want [api worker]", got)
	}
	if dirty := s.catalog.DirtyPartitions(); len(dirty) != 0 {
		t.Errorf("WarmCatalog marked bundles dirty: %v (replay must not re-PUT)", dirty)
	}
}

func TestWarmCatalog_NilCatalogAndCancelledContext(t *testing.T) {
	s := testStorage()
	s.WarmCatalog(context.Background()) // catalog nil → no-op, no panic

	s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	s.manifest.AddFile("dt=2026-06-01/hour=10", manifest.FileInfo{
		Key:    "logs/dt=2026-06-01/hour=10/f1.parquet",
		Labels: map[string][]string{"service.name": {"api"}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.WarmCatalog(ctx)
	if got := s.catalog.FieldValues("dt=2026-06-01/hour=10", "service.name", "", 0); len(got) != 0 {
		t.Errorf("cancelled context must abort the warm, got %v", got)
	}
}

// TestWarmCatalogFromS3_RebuildMissingBundle: when no bundle exists in
// S3 for a manifest partition, the warm must self-heal by rebuilding
// the partition from the manifest's files — and mark it dirty so the
// repaired bundle persists.
func TestWarmCatalogFromS3_RebuildMissingBundle(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")

	now := time.Date(2026, 6, 1, 10, 30, 0, 0, time.UTC)
	s.manifest.AddFile("dt=2026-06-01/hour=10", manifest.FileInfo{
		Key:       "logs/dt=2026-06-01/hour=10/f1.parquet",
		Size:      100,
		MinTimeNs: now.UnixNano(),
		MaxTimeNs: now.UnixNano(),
		Labels:    map[string][]string{"env": {"prod"}},
	})

	s.WarmCatalogFromS3(context.Background())

	got := s.catalog.FieldValues(pmwTenantPartition, "env", "", 0)
	if !reflect.DeepEqual(got, []string{"prod"}) {
		t.Fatalf("rebuilt catalog env = %v, want [prod]", got)
	}
	if dirty := s.catalog.DirtyPartitions(); len(dirty) != 1 {
		t.Errorf("rebuilt partition must be dirty so the repaired bundle persists, got %v", dirty)
	}
}

// TestWarmCatalogFromS3_LoadsPersistedBundle: the happy path — a bundle
// persisted by a previous process is loaded back and serves values.
func TestWarmCatalogFromS3_LoadsPersistedBundle(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	// Previous process: flush + persist.
	prev := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	prev.OnFileFlush(pmwContribution("logs/"+pmwPartition+"/f1.parquet",
		map[string][]string{"service.name": {"api"}}))
	if _, err := prev.PersistDirty(context.Background(), poolObjectStore{s.pool}); err != nil {
		t.Fatalf("PersistDirty: %v", err)
	}

	// Next process: manifest knows the partition, catalog is cold.
	s.manifest.AddFile(pmwPartition, manifest.FileInfo{
		Key:    "logs/" + pmwPartition + "/f1.parquet",
		Labels: map[string][]string{"service.name": {"api"}},
	})
	s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	s.WarmCatalogFromS3(context.Background())

	got := s.catalog.FieldValues(pmwTenantPartition, "service.name", "", 0)
	if !reflect.DeepEqual(got, []string{"api"}) {
		t.Errorf("warmed catalog = %v, want [api]", got)
	}
}

func TestWarmCatalogFromS3_NoOps(t *testing.T) {
	// catalog nil → no-op.
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.WarmCatalogFromS3(context.Background())

	// pool nil → no-op.
	s2 := testStorage()
	s2.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	s2.WarmCatalogFromS3(context.Background())

	// empty manifest → early return.
	s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	s.WarmCatalogFromS3(context.Background())
}

// TestPmetaOnCompacted: the output file's contribution is added, the
// merged-away inputs are removed (no dead keys), and the output gets
// catalog values from its labels.
func TestPmetaOnCompacted(t *testing.T) {
	s := testStorage()
	s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")

	in1 := "logs/" + pmwPartition + "/in1.parquet"
	in2 := "logs/" + pmwPartition + "/in2.parquet"
	s.catalog.OnFileFlush(pmwContribution(in1, map[string][]string{"service.name": {"api"}}))
	s.catalog.OnFileFlush(pmwContribution(in2, map[string][]string{"service.name": {"worker"}}))

	out := manifest.FileInfo{
		Key:       "logs/" + pmwPartition + "/compacted.parquet",
		RowCount:  84,
		MinTimeNs: 1_000,
		MaxTimeNs: 2_000,
		Labels:    map[string][]string{"service.name": {"api", "worker"}},
	}
	// The compactor extracts the COMBINED bloom (union of the merged inputs) from
	// the merged rows and passes it in keyed by the output file.
	combined := map[string]map[string][]string{
		out.Key: {"trace_id": {"tA", "tB"}, "service.name": {"api", "worker"}},
	}
	s.PmetaOnCompacted([]manifest.FileInfo{out}, []string{in1, in2}, combined)

	if _, ok := s.catalog.FileMeta(pmwTenantPartition, in1); ok {
		t.Error("compaction input in1 must be removed from the facet")
	}
	if _, ok := s.catalog.FileMeta(pmwTenantPartition, in2); ok {
		t.Error("compaction input in2 must be removed from the facet")
	}
	got, ok := s.catalog.FileMeta(pmwTenantPartition, out.Key)
	if !ok {
		t.Fatal("compaction output must be present in the facet")
	}
	if got.RowCount != 84 {
		t.Errorf("output RowCount = %d, want 84", got.RowCount)
	}

	// The combined bloom must be retained on the compacted output (file-level
	// pruning kept): a value from the union is found on the output key. The bloom
	// facet lives in the tenant-isolated bundle (pmwTenantPartition), same as the
	// FileMeta facet asserted above.
	if keys, ok := s.catalog.BloomMayContain(pmwTenantPartition, []string{out.Key}, "trace_id", "tA"); !ok || len(keys) != 1 || keys[0] != out.Key {
		t.Errorf("combined bloom must contain trace_id tA on the compacted output; ok=%v keys=%v", ok, keys)
	}

	// nil catalog → no-op, no panic.
	s2 := testStorage()
	s2.PmetaOnCompacted([]manifest.FileInfo{out}, []string{in1}, nil)
}

// TestPmetaOnFileExpired: removing the last file of a partition must
// evict the whole bundle from RAM (and attempt the S3 bundle delete).
func TestPmetaOnFileExpired(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")

	k1 := "logs/" + pmwPartition + "/f1.parquet"
	k2 := "logs/" + pmwPartition + "/f2.parquet"
	s.catalog.OnFileFlush(pmwContribution(k1, map[string][]string{"env": {"prod"}}))
	s.catalog.OnFileFlush(pmwContribution(k2, map[string][]string{"env": {"dev"}}))
	// Manifest still holds f2 → bundle must survive the first expiry.
	s.manifest.AddFile(pmwPartition, manifest.FileInfo{Key: k2})

	s.PmetaOnFileExpired(pmwPartition, k1)
	if _, ok := s.catalog.FileMeta(pmwTenantPartition, k1); ok {
		t.Error("expired file must be removed from the facet")
	}
	if _, ok := s.catalog.FileMeta(pmwTenantPartition, k2); !ok {
		t.Error("remaining file must survive a sibling's expiry")
	}

	// Expire the last file: manifest partition empties → bundle evicted.
	s.manifest.RemoveFile(pmwPartition, k2)
	s.PmetaOnFileExpired(pmwPartition, k2)
	if _, ok := s.catalog.FileMeta(pmwTenantPartition, k2); ok {
		t.Error("last expired file must be removed")
	}
	for _, p := range s.catalog.Partitions() {
		if p == pmwTenantPartition {
			t.Error("empty partition's bundle must be evicted from RAM")
		}
	}

	// nil catalog → no-op.
	s2 := testStorage()
	s2.PmetaOnFileExpired(pmwPartition, k1)
}

func TestRefuseEnumeration(t *testing.T) {
	s := testStorage()

	// catalog nil → never refuse.
	s.cfg.Pmeta.RefuseSketchEnumeration = true
	s.cfg.Pmeta.AlwaysSketchFields = []string{"trace_id"}
	if s.refuseEnumeration("trace_id") {
		t.Error("must not refuse without a catalog")
	}

	s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")

	// Flag off → never refuse.
	s.cfg.Pmeta.RefuseSketchEnumeration = false
	if s.refuseEnumeration("trace_id") {
		t.Error("must not refuse with the flag off")
	}

	// Flag on + declared sketch field → refuse.
	s.cfg.Pmeta.RefuseSketchEnumeration = true
	if !s.refuseEnumeration("trace_id") {
		t.Error("must refuse enumeration of a declared always-sketch field")
	}

	// Flag on + any other field (even a high-cardinality crosser) → scan.
	if s.refuseEnumeration("service.name") {
		t.Error("threshold-crossers must NOT be refused — they fall through to the scan")
	}
}

// TestCatalogObserver_OnFileFlush_TruncatedLabels: a label list at the
// extractor cap may be incomplete — the observer must report the field
// as truncated so the catalog marks it high-cardinality instead of
// serving the capped list as authoritative.
func TestCatalogObserver_OnFileFlush_TruncatedLabels(t *testing.T) {
	st := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	o := &catalogObserver{store: st}

	vals := make([]string, maxLabelsPerField) // exactly at cap → truncated
	for i := range vals {
		vals[i] = "v" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('a'+i%10))
	}
	o.OnFileFlush(pmwPartition, manifest.FileInfo{Key: "logs/" + pmwPartition + "/big.parquet"},
		map[string][]string{"user_id": vals}, nil)

	// A truncated field must not be served from the catalog (high-card mark).
	if got := st.FieldValues(pmwPartition, "user_id", "", 0); got != nil {
		t.Errorf("capped label list must not be served as authoritative, got %d values", len(got))
	}

	// persistDirty without a pool is a safe no-op; nil observer too.
	o.persistDirty(context.Background())
	var nilObs *catalogObserver
	nilObs.persistDirty(context.Background())
	nilObs.OnFileFlush(pmwPartition, manifest.FileInfo{}, nil, nil)
}
