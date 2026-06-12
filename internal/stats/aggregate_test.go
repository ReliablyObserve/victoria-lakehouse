package stats

import (
	"context"
	"errors"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func aggFile(key string, size, rows int64, cols map[string]int64) manifest.FileInfo {
	return manifest.FileInfo{Key: key, Size: size, RowCount: rows, RawBytes: size * 5, ColumnBytes: cols}
}

func TestStatsAggregate_AddRemove(t *testing.T) {
	a := NewStatsAggregate()
	a.OnAdd("p", aggFile("100/200/a.parquet", 1000, 10, map[string]int64{"service.name": 300, "k8s.pod.name": 700}))
	a.OnAdd("p", aggFile("100/200/b.parquet", 500, 5, map[string]int64{"service.name": 200, "k8s.pod.name": 300}))

	if got := a.StorageBytesOf("service.name"); got != 500 {
		t.Errorf("service.name storage = %d, want 500", got)
	}
	if got := a.StorageBytesOf("k8s.pod.name"); got != 1000 {
		t.Errorf("k8s.pod.name storage = %d, want 1000", got)
	}
	if ts := a.TenantSizes()["100:200"]; ts.StorageBytes != 1500 || ts.Files != 2 || ts.Rows != 15 {
		t.Errorf("tenant 100:200 = %+v, want storage 1500 files 2 rows 15", ts)
	}

	a.OnRemove("p", aggFile("100/200/b.parquet", 500, 5, map[string]int64{"service.name": 200, "k8s.pod.name": 300}))
	if got := a.StorageBytesOf("service.name"); got != 300 {
		t.Errorf("after remove, service.name = %d, want 300", got)
	}
	if got := a.TenantSizes()["100:200"].Files; got != 1 {
		t.Errorf("after remove, tenant files = %d, want 1", got)
	}
}

func TestStatsAggregate_CompactionDiff(t *testing.T) {
	a := NewStatsAggregate()
	in1 := aggFile("1/1/i1.parquet", 1000, 10, map[string]int64{"f": 1000})
	in2 := aggFile("1/1/i2.parquet", 1000, 10, map[string]int64{"f": 1000})
	a.OnAdd("p", in1)
	a.OnAdd("p", in2)
	// Compaction: remove inputs, add merged (better compression → fewer bytes than the sum).
	a.OnRemove("p", in1)
	a.OnRemove("p", in2)
	a.OnAdd("p", aggFile("1/1/merged.parquet", 1600, 20, map[string]int64{"f": 1600}))
	if got := a.StorageBytesOf("f"); got != 1600 {
		t.Errorf("after compaction, f = %d, want 1600 (the merged file, not the 2000 input sum)", got)
	}
	if got := a.FieldSizes()["f"].Files; got != 1 {
		t.Errorf("after compaction, f files = %d, want 1", got)
	}
}

func TestStatsAggregate_RecomputeMatchesIncremental(t *testing.T) {
	files := map[string][]manifest.FileInfo{
		"p1": {aggFile("1/1/a.parquet", 1000, 10, map[string]int64{"f": 600, "g": 400})},
		"p2": {aggFile("1/2/b.parquet", 500, 5, map[string]int64{"f": 500})},
	}
	inc := NewStatsAggregate()
	for p, fs := range files {
		for _, f := range fs {
			inc.OnAdd(p, f)
		}
	}
	rec := NewStatsAggregate()
	rec.Recompute(files)
	if inc.StorageBytesOf("f") != rec.StorageBytesOf("f") || rec.StorageBytesOf("f") != 1100 {
		t.Errorf("f: incremental=%d recompute=%d, want 1100", inc.StorageBytesOf("f"), rec.StorageBytesOf("f"))
	}
}

func TestStatsAggregate_MarshalLoad(t *testing.T) {
	a := NewStatsAggregate()
	a.OnAdd("p", aggFile("1/1/a.parquet", 1000, 10, map[string]int64{"f": 1000}))
	data, err := a.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	b := NewStatsAggregate()
	if err := b.Load(data); err != nil {
		t.Fatal(err)
	}
	if b.StorageBytesOf("f") != 1000 || b.TenantSizes()["1:1"].StorageBytes != 1000 {
		t.Errorf("round-trip mismatch: f=%d tenant=%+v", b.StorageBytesOf("f"), b.TenantSizes()["1:1"])
	}
}

func TestStatsAggregate_SuffixMatch(t *testing.T) {
	a := NewStatsAggregate()
	a.OnAdd("p", aggFile("1/1/a.parquet", 100, 1, map[string]int64{"service.name": 100}))
	if got := a.StorageBytesOf("resource_attr:service.name"); got != 100 {
		t.Errorf("suffix match = %d, want 100", got)
	}
}

// TestStatsAggregate_ScaledStorage is the regression guard for the Cardinality
// Explorer "Storage column read in KB" bug: older files carry no ColumnBytes, so
// covered < total and the API scales covered per-field bytes up to the real
// on-S3 total. Asserts the covered/total inputs the scale is derived from.
func TestStatsAggregate_ScaledStorage(t *testing.T) {
	a := NewStatsAggregate()
	a.OnAdd("p", aggFile("1/1/new.parquet", 1000, 10, map[string]int64{"f": 1000}))   // carries ColumnBytes
	a.OnAdd("p", manifest.FileInfo{Key: "1/1/old.parquet", Size: 3000, RowCount: 30}) // pre-feature, no ColumnBytes
	if cov := a.CoveredStorage(); cov != 1000 {
		t.Errorf("CoveredStorage = %d, want 1000 (only the file with ColumnBytes)", cov)
	}
	if tot := a.TotalStorage(); tot != 4000 {
		t.Errorf("TotalStorage = %d, want 4000 (all files' Size)", tot)
	}
	// scale = total/covered = 4 → the API renders f as 1000*4 = 4000 (real magnitude).
}

func TestStatsAggregate_MetaS3NilSafe(t *testing.T) {
	a := NewStatsAggregate()
	a.SetMetaS3(123)
	if got := a.MetaS3(); got != 123 {
		t.Errorf("MetaS3 = %d, want 123", got)
	}
	var nilAgg *StatsAggregate
	if got := nilAgg.MetaS3(); got != 0 {
		t.Errorf("nil-receiver MetaS3 = %d, want 0 (must be nil-safe for the APIConfig func value)", got)
	}
}

// fakePool is an in-memory stats.S3Pool for the sidecar round-trip test.
type fakePool struct{ objs map[string][]byte }

func (p *fakePool) Upload(_ context.Context, key string, data []byte) error {
	if p.objs == nil {
		p.objs = map[string][]byte{}
	}
	p.objs[key] = append([]byte(nil), data...)
	return nil
}
func (p *fakePool) Download(_ context.Context, key string) ([]byte, error) {
	if d, ok := p.objs[key]; ok {
		return d, nil
	}
	return nil, errors.New("not found")
}

func TestStatsAggregate_SaveLoadS3(t *testing.T) {
	a := NewStatsAggregate()
	a.OnAdd("p", aggFile("1/1/a.parquet", 1000, 10, map[string]int64{"f": 1000}))
	pool := &fakePool{}
	if err := a.SaveToS3(context.Background(), pool, "k"); err != nil {
		t.Fatal(err)
	}
	b := NewStatsAggregate()
	if err := b.LoadFromS3(context.Background(), pool, "k"); err != nil {
		t.Fatal(err)
	}
	if b.StorageBytesOf("f") != 1000 || b.TenantSizes()["1:1"].StorageBytes != 1000 {
		t.Errorf("sidecar round-trip mismatch: f=%d tenant=%+v", b.StorageBytesOf("f"), b.TenantSizes()["1:1"])
	}
	// A missing object is a non-fatal cold-start miss (caller log-and-continues).
	if err := b.LoadFromS3(context.Background(), pool, "missing"); err == nil {
		t.Error("LoadFromS3 of a missing key should return an error so the caller skips it")
	}
}
