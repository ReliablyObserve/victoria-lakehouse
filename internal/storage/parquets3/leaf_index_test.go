package parquets3

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// A MAP column is one top-level column but two leaf column chunks (key, value).
// A scalar column placed after a map therefore sits at a later chunk than its
// position among the top-level columns. The field reads must index by leaf, or
// they read another column's chunk. The real logs schema keeps its maps last
// today, so this fixture puts the maps in front the way the traces schema
// does (twin of the fix in lakehouse-traces for #409).
type mapsThenScalars struct {
	TimestampUnixNano int64             `parquet:"timestamp_unix_nano"`
	Body              string            `parquet:"body"`
	ResourceAttrs     map[string]string `parquet:"resource.attributes,optional"`
	LogAttrs          map[string]string `parquet:"log.attributes,optional"`
	SeverityText      string            `parquet:"severity_text"`
	ServiceName       string            `parquet:"service.name"`
}

func mapsThenScalarsFile(t *testing.T) []byte {
	t.Helper()
	rows := []mapsThenScalars{
		{1000, "a", map[string]string{"rk": "rv1"}, map[string]string{"lk": "lv1"}, "INFO", "svc-a"},
		{2000, "b", map[string]string{"rk": "rv2"}, nil, "ERROR", "svc-a"},
		{3000, "c", nil, map[string]string{"lk": "lv3"}, "INFO", "svc-b"},
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[mapsThenScalars](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestScanProjectedFieldValues_ScalarAfterMapsIsIndexedByLeaf(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	data := mapsThenScalarsFile(t)
	key := "logs/dt=2026-06-09/hour=10/maps.parquet"
	mock.putFile(key, data)
	fi := manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 3, MinTimeNs: 1000, MaxTimeNs: 3000}

	for _, col := range []string{"severity_text", "service.name"} {
		seen := map[string]uint64{}
		if err := s.scanProjectedFieldValues(context.Background(), fi, col, col, false, nil, nil, seen, math.MinInt64, math.MaxInt64); err != nil {
			t.Fatal(err)
		}
		want := map[string]uint64{"INFO": 2, "ERROR": 1}
		if col == "service.name" {
			want = map[string]uint64{"svc-a": 2, "svc-b": 1}
		}
		if len(seen) != len(want) {
			t.Fatalf("%s: values %v, want %v", col, seen, want)
		}
		for v, n := range want {
			if seen[v] != n {
				t.Errorf("%s: %q hits %d, want %d (all: %v)", col, v, seen[v], n, seen)
			}
		}
	}
}

func TestAccumulateFieldHits_ScalarAfterMapsIsIndexedByLeaf(t *testing.T) {
	data := mapsThenScalarsFile(t)
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	s := testStorageWithS3(t, "http://127.0.0.1:1")
	hits := map[string]uint64{}
	s.accumulateFieldHits(f, hits, nil)
	// Every scalar column has 3 non-null values. Read through the wrong chunk
	// (a map's key or value leaf) the counts are the map entries, 2 and 2.
	for _, c := range []string{"body", "severity_text", "service.name"} {
		name := c
		if m := s.registry.ResolveFromParquet(c); m != nil {
			name = m.InternalName
		}
		if hits[name] != 3 {
			t.Errorf("%s: %d hits, want 3 (all: %v)", name, hits[name], hits)
		}
	}
}
