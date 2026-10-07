package parquets3

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// fvMutantFile registers one object (a fvPropFile) with a caller-shaped
// manifest entry, so a test controls RowCount and the label aggregates.
func fvMutantFile(t *testing.T, s *Storage, mock *mockS3Server, name string, data []byte, base time.Time, rowCount int64, agg map[string]map[string]int64) {
	t.Helper()
	key := fmt.Sprintf("logs/dt=%s/hour=%02d/%s.parquet", base.Format("2006-01-02"), base.Hour(), name)
	mock.putFile(key, data)
	s.manifest.AddFile(partitionFromKey(key), manifest.FileInfo{
		Key: key, Size: int64(len(data)), RowCount: rowCount,
		MinTimeNs: base.UnixNano(), MaxTimeNs: base.Add(time.Minute).UnixNano(),
		LabelAggregates: agg,
	})
}

func fvMutantRows(n int, level string) []fvPropRow {
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	rows := make([]fvPropRow, n)
	for i := range rows {
		rows[i] = fvPropRow{ts: base.Add(time.Duration(i) * time.Millisecond).UnixNano(), level: level, svc: "a",
			maps: map[string]map[string]string{}, hasMap: map[string]bool{}}
	}
	return rows
}

// A manifest entry with label aggregates but no row count cannot give the empty
// bucket (it is the row count minus the counted values): the object is scanned
// instead, and the rows without the value are not lost.
func TestFieldValuesEmpty_AggregateWithoutRowCountIsScanned(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	rows := fvMutantRows(6, "INFO")
	for i := range rows[:2] {
		rows[i].level = ""
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "severity_text", "service.name", "resource.attributes", "log.attributes"}, rows, 0)
	fvMutantFile(t, s, mock, "agg0", data, base, 0, map[string]map[string]int64{"severity_text": {"INFO": 4}})
	lo, hi := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	got := fvEmptyValues(t, s, "*", lo, hi, "level", false)
	if want := map[string]uint64{"INFO": 4, "": 2}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("field_values(level) with aggregates and RowCount 0 = %v, want %v", got, want)
	}
}

// With the row count known, the aggregate path adds the rows without a value
// under "" — and equals the scan of the same object.
func TestFieldValuesEmpty_AggregateWithRowCountEqualsScan(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	rows := fvMutantRows(6, "INFO")
	for i := range rows[:2] {
		rows[i].level = ""
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "severity_text", "service.name", "resource.attributes", "log.attributes"}, rows, 0)
	fvMutantFile(t, s, mock, "agg6", data, base, 6, map[string]map[string]int64{"severity_text": {"INFO": 4}})
	lo, hi := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	agg := fvEmptyValues(t, s, "*", lo, hi, "level", false)
	scan := fvEmptyValues(t, s, "service.name:=a", lo, hi, "level", false)
	want := map[string]uint64{"INFO": 4, "": 2}
	if fmt.Sprint(agg) != fmt.Sprint(want) || fmt.Sprint(scan) != fmt.Sprint(want) {
		t.Errorf("aggregate=%v scan=%v, want %v", agg, scan, want)
	}
}

// An object that lacks the column altogether, lying inside the window: its rows
// are all "" — answered from the row count with no data page read when nothing
// narrows them, and by reading the rows when a filter or a tombstone does.
func TestFieldValuesEmpty_MissingColumnInsideWindow(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	// Enough payload that the footer read-ahead does not cover the object.
	const n = 4000
	rows := fvMutantRows(n, "")
	for i := range rows {
		if i%2 == 0 {
			rows[i].svc = "b"
		}
		rows[i].hasMap["resource.attributes"] = true
		rows[i].maps["resource.attributes"] = map[string]string{"k": fmt.Sprintf("value-%d-%d", i, i*7919%100003)}
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "service.name", "resource.attributes"}, rows, 3)
	fvMutantFile(t, s, mock, "nolevel", data, base, n+7, nil)
	lo, hi := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()

	// The manifest says one row more than the object holds: an answer from
	// the row count (no data page read) shows it, a scan of the rows does not.
	got := fvEmptyValues(t, s, "*", lo, hi, "level", false)
	if want := map[string]uint64{"": n + 7}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("unfiltered = %v, want the manifest row count %v", got, want)
	}
	filtered := fvEmptyValues(t, s, "service.name:=b", lo, hi, "level", false)
	if want := map[string]uint64{"": n / 2}; fmt.Sprint(filtered) != fmt.Sprint(want) {
		t.Errorf("filtered = %v, want %v", filtered, want)
	}
	// stream_field_values never lists the empty value.
	if sv := fvEmptyValues(t, s, "*", lo, hi, "level", true); len(sv) != 0 {
		t.Errorf("stream_field_values = %v, want none", sv)
	}
}

// An object that lacks the column and straddles the window is read row by row,
// never answered from its row count: only the rows inside the window count.
func TestFieldValuesEmpty_MissingColumnStraddlingWindowWithRowCount(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	rows := make([]fvPropRow, 10)
	for i := range rows {
		rows[i] = fvPropRow{ts: base.Add(time.Duration(i) * time.Second).UnixNano(), svc: "a", maps: map[string]map[string]string{}, hasMap: map[string]bool{}}
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "service.name", "resource.attributes"}, rows, 4)
	key := fmt.Sprintf("logs/dt=%s/hour=%02d/straddle.parquet", base.Format("2006-01-02"), base.Hour())
	mock.putFile(key, data)
	s.manifest.AddFile(partitionFromKey(key), manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 10,
		MinTimeNs: base.UnixNano(), MaxTimeNs: base.Add(9 * time.Second).UnixNano()})
	lo, hi := base.Add(3*time.Second).UnixNano(), base.Add(6*time.Second).UnixNano()
	got := fvEmptyValues(t, s, "*", lo, hi, "level", false)
	if want := map[string]uint64{"": 4}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("window [3s,6s] over a 10-row object without the column = %v, want %v", got, want)
	}
}

// The MAP reader expands only the keys a request names: the keys of the target,
// of its filter and of its tombstones, in their bare and prefixed spellings.
func TestMapScanKeys_OnlyTheRequestedKeys(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	filter := parseFilterFromQuery(mustParseQueryWithTime(t, `user:=u1 level:=ERROR`, 0, 1))
	got := s.mapScanKeys("lk", filter, nil)
	for _, want := range []string{"lk", "user", "level", "_time"} {
		if _, ok := got[want]; !ok {
			t.Errorf("mapScanKeys misses %q: %v", want, got)
		}
	}
	if len(got) != 4 {
		t.Errorf("mapScanKeys = %v, want exactly lk, user, level, _time", got)
	}
	pre := s.mapScanKeys("log_attr:lk", nil, nil)
	if _, ok := pre["lk"]; !ok {
		t.Errorf("a prefixed field must also name its bare key: %v", pre)
	}
	if k := s.mapScanKeys("*", nil, nil); k != nil {
		t.Errorf("a wildcard cannot be bounded, got %v", k)
	}
}

// Expanding one key of two wide maps must not stringify every other key: the
// allocations stay far below one per attribute per row.
func TestFieldValuesEmpty_MapReadExpandsOnlyTheRequestedKey(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = fvPropMode()
	base := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Second)
	const n, width = 300, 40
	rows := make([]fvPropRow, n)
	for i := range rows {
		attrs := map[string]string{"lk": fmt.Sprintf("v%d", i%5)}
		for k := 0; k < width; k++ {
			attrs[fmt.Sprintf("other%d", k)] = fmt.Sprintf("x%d-%d", k, i)
		}
		rows[i] = fvPropRow{ts: base.Add(time.Duration(i) * time.Millisecond).UnixNano(), level: "INFO", svc: "a",
			maps: map[string]map[string]string{"log.attributes": attrs}, hasMap: map[string]bool{"log.attributes": true}}
	}
	data := fvPropFile(t, []string{"timestamp_unix_nano", "severity_text", "service.name", "resource.attributes", "log.attributes"}, rows, 0)
	fvMutantFile(t, s, mock, "wide", data, base, n, nil)
	lo, hi := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	got := fvEmptyValues(t, s, "*", lo, hi, "lk", false)
	want := map[string]uint64{"v0": 60, "v1": 60, "v2": 60, "v3": 60, "v4": 60}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("field_values(lk) = %v, want %v", got, want)
	}
	q := mustParseQueryWithTime(t, "*", lo, hi)
	allocs := testing.AllocsPerRun(5, func() { _, _ = s.GetFieldValues(context.Background(), nil, q, "lk", 0) })
	if limit := float64(n * 8); allocs > limit {
		t.Errorf("field_values(lk) over %d rows x %d keys made %.0f allocations, want at most %.0f (one key expanded)", n, width, allocs, limit)
	}
}
