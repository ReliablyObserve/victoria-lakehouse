package parquets3

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"

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
	return mapsThenScalarsFileAt(t, 0)
}

// mapsThenScalarsFileAt is the fixture with its three rows at base+1000,
// base+2000 and base+3000 ns.
func mapsThenScalarsFileAt(t *testing.T, base int64) []byte {
	t.Helper()
	rows := []mapsThenScalars{
		{base + 1000, "a", map[string]string{"rk": "rv1"}, map[string]string{"lk": "lv1"}, "INFO", "svc-a"},
		{base + 2000, "b", map[string]string{"rk": "rv2"}, map[string]string{"lk": "lv2"}, "ERROR", "svc-a"},
		{base + 3000, "c", nil, map[string]string{"lk": "lv3"}, "INFO", "svc-b"},
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

// Twin of the scan test above for field_names, which now reads the rows
// (storage.FieldNamesViaQuery over RunQuery) instead of the footers: the
// scalar columns that follow the maps must be read from their own chunks, or the
// rows carry another column's values. The level of the rows is the proof: the
// filter picks the one ERROR row, so reading severity_text through a map's chunk
// would match no row (or the wrong rows) and the hits would not be 1.
func TestAccumulateFieldHits_ScalarAfterMapsIsIndexedByLeaf(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	base := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC).UnixNano()
	data := mapsThenScalarsFileAt(t, base)
	key := "logs/dt=2026-06-09/hour=10/maps-names.parquet"
	mock.putFile(key, data)
	s.manifest.AddFile("dt=2026-06-09/hour=10", manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 3, MinTimeNs: base + 1000, MaxTimeNs: base + 3000})

	hits := func(query string) map[string]uint64 {
		t.Helper()
		got, err := s.GetFieldNames(context.Background(), nil, mustParseQueryWithTime(t, query, base, base+4000))
		if err != nil {
			t.Fatal(err)
		}
		return namesToHits(got)
	}
	all := hits("*")
	if all["level"] != 3 || all["service.name"] != 3 || all["_msg"] != 3 {
		t.Errorf("level, service.name and _msg are set on all 3 rows, got %v", all)
	}
	// The rows carry no stream column, so they are one block: a map key some row
	// has is listed and credited with every row of the block. A map read through
	// another map's chunk would list the wrong keys or credit another number of
	// rows (rk is set on two rows, lk on three, but both credit all three).
	if all["rk"] != 3 || all["lk"] != 3 {
		t.Errorf("rk/lk hits = %d/%d, want 3/3 (resource and log attribute keys of the one block): %v", all["rk"], all["lk"], all)
	}
	errRow := hits("level:=ERROR")
	if errRow["_msg"] != 1 || errRow["service.name"] != 1 {
		t.Errorf("level:=ERROR must match the one ERROR row, got %v", errRow)
	}
	// The ERROR row carries both keys, and the block lists them: credited with
	// the one matching row each.
	if errRow["rk"] != 1 || errRow["lk"] != 1 {
		t.Errorf("level:=ERROR rk/lk hits = %d/%d, want 1/1: %v", errRow["rk"], errRow["lk"], errRow)
	}
	svcB := hits("service.name:=svc-b")
	if svcB["_msg"] != 1 {
		t.Errorf("service.name:=svc-b must match the one row of svc-b, got %v", svcB)
	}
}
