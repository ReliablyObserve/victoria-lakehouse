package parquets3

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Negative control for the field_names counting read. Only a field_names
// enumeration carries the counter (storage.WithFieldNamesQuery + the counter in
// the context), and only it gives up the pruning the row filter allows: every
// other read keeps its file pre-filter (label counts, statistics, blooms) and
// its row-group skips. A change that leaks the counting mode into RunQuery, or
// that turns the footer bloom off for every query, makes an ordinary filtered
// read open the object or row group a filter excludes, which these tests catch
// next to the counting read that must open it.

// skippedRowGroups is every skip reason of lakehouse_parquet_row_groups_skipped_total.
func skippedRowGroups() uint64 {
	var n uint64
	for _, r := range metrics.RowGroupSkipReasons {
		n += metrics.ParquetRowGroupsSkipped.Get(r)
	}
	return n
}

// twoObjectStream is one stream in two objects whose level differs: the filter
// level:="WARN-2" excludes the first object (a quoted value is what the label
// pruning reads, hence the hyphens).
func twoObjectStream(t *testing.T) (*Storage, *mockS3Server) {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", s.cfg.Mode)
	row := func(at time.Duration, level, traceID string) schema.LogRow {
		r := fvcRow(at, "x")
		r.ServiceName, r.Stream = "svc", `{service.name="svc"}`
		r.SeverityText, r.TraceID = level, traceID
		return r
	}
	fvcUpload(t, bw, "", []schema.LogRow{row(1*time.Minute, "INFO-1", "ta")})
	fvcUpload(t, bw, "", []schema.LogRow{row(2*time.Minute, "WARN-2", "tb")})
	return s, mock
}

func discard(uint, *logstorage.DataBlock) {}

func TestFieldNames_OnlyTheCountingReadOpensObjectsTheFilterExcludes(t *testing.T) {
	const filter = `level:="WARN-2"`
	ctx := context.Background()
	for name, run := range map[string]func(s *Storage) error{
		"RunQuery": func(s *Storage) error { return s.RunQuery(ctx, nil, fnWindow(t, filter), discard) },
		"hits shape": func(s *Storage) error {
			return s.RunQuery(ctx, nil, fnWindow(t, filter+" | stats by (_time:1h) count() hits"), discard)
		},
		"stats": func(s *Storage) error {
			return s.RunQuery(ctx, nil, fnWindow(t, filter+" | stats count() n"), discard)
		},
	} {
		s, mock := twoObjectStream(t)
		before := metrics.ParquetRowGroupsSkipped.Get("label_index") + metrics.ParquetRowGroupsSkipped.Get("column_stats")
		if err := run(s); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if n := mock.readObjects(); n != 1 {
			t.Errorf("%s: %d objects opened, want 1 (the object the filter excludes is skipped)", name, n)
		}
		if d := metrics.ParquetRowGroupsSkipped.Get("label_index") + metrics.ParquetRowGroupsSkipped.Get("column_stats") - before; d == 0 {
			t.Errorf("%s: no object-level skip recorded", name)
		}
	}
	// next to it: the field_names count of the same filter opens both objects
	s, mock := twoObjectStream(t)
	if _, err := s.GetFieldNames(ctx, nil, fnWindow(t, filter)); err != nil {
		t.Fatal(err)
	}
	if n := mock.readObjects(); n != 2 {
		t.Errorf("field_names count: %d objects opened, want 2 (an excluded object still lists its columns)", n)
	}
}

// rowGroupStore is two objects of four row groups of ten rows, one trace id per
// row group.
func rowGroupStore(t *testing.T) *Storage {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	pad := strings.Repeat("x", 1500)
	for f := 0; f < 2; f++ {
		var rows []schema.LogRow
		for i := 0; i < 40; i++ {
			k := i / 10
			rows = append(rows, schema.LogRow{
				TimestampUnixNano: base.Add(time.Duration(f*40+i) * time.Second).UnixNano(),
				Body:              fmt.Sprintf("w%d%d common %s%d", f, k, pad, i),
				ServiceName:       []string{"alpha", "beta"}[f],
				SeverityText:      []string{"INFO", "ERROR"}[k%2],
				TraceID:           tid(f, k),
				Stream:            fmt.Sprintf(`{service.name=%q}`, []string{"alpha", "beta"}[f]),
				StreamID:          fmt.Sprintf("%048x", 3+f),
			})
		}
		res, err := writeLogsParquet(rows, 10, 3)
		if err != nil {
			t.Fatal(err)
		}
		registerFileInMockS3(t, s, mock, fmt.Sprintf("logs/dt=%s/hour=%02d/f%d.parquet", base.Format("2006-01-02"), base.Hour(), f), res.Data, base.Add(time.Duration(f)*40*time.Second))
	}
	return s
}

func TestFieldNames_OnlyTheCountingReadReadsRowGroupsTheFilterExcludes(t *testing.T) {
	ctx := context.Background()
	window := func(query string) *logstorage.Query {
		now := time.Now()
		return mustParseQueryWithTime(t, query, now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	}
	for _, tc := range []struct {
		filter string
		rows   int
		reason string // the skip the filter earns an ordinary read
		skips  uint64 // at least
	}{
		{"trace_id:=" + tid(1, 0), 10, "bloom", 7}, // the footer bloom of the id
		{"w10", 10, "token_bloom", 1},              // the token bloom of a body word
		{`service.name:="beta"`, 40, "bloom", 1},   // the footer bloom of a service
	} {
		s := rowGroupStore(t)
		before := metrics.ParquetRowGroupsSkipped.Get(tc.reason)
		rows := 0
		if err := s.RunQuery(ctx, nil, window(tc.filter), func(_ uint, db *logstorage.DataBlock) { rows += db.RowsCount() }); err != nil {
			t.Fatal(err)
		}
		if rows != tc.rows {
			t.Fatalf("%s: RunQuery returned %d rows, want %d", tc.filter, rows, tc.rows)
		}
		if d := metrics.ParquetRowGroupsSkipped.Get(tc.reason) - before; d < tc.skips {
			t.Errorf("%s: RunQuery skipped %d row groups by %s, want at least %d", tc.filter, d, tc.reason, tc.skips)
		}

		// next to it: the field_names count of the same filter skips none
		s = rowGroupStore(t)
		all := skippedRowGroups()
		if _, err := s.GetFieldNames(ctx, nil, window(tc.filter)); err != nil {
			t.Fatal(err)
		}
		if d := skippedRowGroups() - all; d != 0 {
			t.Errorf("%s: the field_names count skipped %d row groups for the row filter's sake", tc.filter, d)
		}
	}
}
