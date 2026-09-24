package parquets3

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

var fvcBase = time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)

func fvcRow(at time.Duration, level string) schema.LogRow {
	return schema.LogRow{
		TimestampUnixNano: fvcBase.Add(at).UnixNano(), Body: "x", SeverityText: level,
		ServiceName: "svc-" + level, Stream: `{service.name="svc-` + level + `"}`,
	}
}

// fvcStorage flushes each batch as its own object in the same partition.
func fvcStorage(t *testing.T, batches ...[]schema.LogRow) *Storage {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	for _, b := range batches {
		bw.AddLogRows(b)
		bw.triggerFlush()
	}
	return s
}

// One object wholly inside the window is answered from its label counts, one
// that straddles the window's end is scanned for its in-window rows only; the
// two answers add up to the exact hits.
func TestFieldValues_LabelCountsAndScanAddUpExactly(t *testing.T) {
	inside := []schema.LogRow{fvcRow(5*time.Minute, "INFO"), fvcRow(6*time.Minute, "INFO")}
	straddling := []schema.LogRow{
		fvcRow(10*time.Minute, "INFO"), fvcRow(11*time.Minute, "WARN"),
		fvcRow(40*time.Minute, "INFO"), fvcRow(41*time.Minute, "INFO"), fvcRow(42*time.Minute, "ERROR"),
	}
	s := fvcStorage(t, inside, straddling)
	q := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(30*time.Minute).UnixNano())

	agg0, scan0 := metrics.FieldValuesFiles.Get("aggregate"), metrics.FieldValuesFiles.Get("scan")
	got, err := s.GetFieldValues(context.Background(), nil, q, "level", 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []logstorage.ValueWithHits{{Value: "INFO", Hits: 3}, {Value: "WARN", Hits: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("field_values level = %v, want %v", got, want)
	}
	if d := metrics.FieldValuesFiles.Get("aggregate") - agg0; d != 1 {
		t.Errorf("objects answered from label counts = %d, want 1", d)
	}
	if d := metrics.FieldValuesFiles.Get("scan") - scan0; d != 1 {
		t.Errorf("objects scanned = %d, want 1", d)
	}
}

// The response is ordered and limited the way VictoriaLogs orders and limits
// it: descending hits, then values; past the limit hits are zeroed and the
// first values in natural order are kept — not an arbitrary subset.
func TestFieldValues_LimitFollowsUpstream(t *testing.T) {
	rows := []schema.LogRow{
		fvcRow(1*time.Minute, "WARN"), fvcRow(2*time.Minute, "WARN"),
		fvcRow(3*time.Minute, "INFO"), fvcRow(4*time.Minute, "INFO"), fvcRow(5*time.Minute, "INFO"),
		fvcRow(6*time.Minute, "ERROR"),
	}
	s := fvcStorage(t, rows)
	q := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano())
	for _, tc := range []struct {
		limit uint64
		want  []logstorage.ValueWithHits
	}{
		{0, []logstorage.ValueWithHits{{Value: "INFO", Hits: 3}, {Value: "WARN", Hits: 2}, {Value: "ERROR", Hits: 1}}},
		{3, []logstorage.ValueWithHits{{Value: "INFO", Hits: 3}, {Value: "WARN", Hits: 2}, {Value: "ERROR", Hits: 1}}},
		{2, []logstorage.ValueWithHits{{Value: "ERROR", Hits: 0}, {Value: "INFO", Hits: 0}}},
		{1, []logstorage.ValueWithHits{{Value: "ERROR", Hits: 0}}},
	} {
		for _, ep := range []string{"level", "streams"} {
			var got []logstorage.ValueWithHits
			var err error
			want := tc.want
			if ep == "streams" {
				got, err = s.GetStreams(context.Background(), nil, q, tc.limit)
				want = make([]logstorage.ValueWithHits, len(tc.want))
				for i, v := range tc.want {
					want[i] = logstorage.ValueWithHits{Value: `{service.name="svc-` + v.Value + `"}`, Hits: v.Hits}
				}
			} else {
				got, err = s.GetFieldValues(context.Background(), nil, q, ep, tc.limit)
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s limit=%d = %v, want %v", ep, tc.limit, got, want)
			}
		}
	}
}

// A cancelled request is an error on both paths, never a partial answer.
func TestFieldValues_CancelledRequestIsAnError(t *testing.T) {
	s := fvcStorage(t, []schema.LogRow{fvcRow(time.Minute, "INFO"), fvcRow(50*time.Minute, "WARN")})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	whole := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano())
	cut := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(30*time.Minute).UnixNano())
	for name, q := range map[string]*logstorage.Query{"label counts": whole, "scan": cut} {
		if _, err := s.GetFieldValues(ctx, nil, q, "level", 0); !errors.Is(err, context.Canceled) {
			t.Errorf("%s: err = %v, want context.Canceled", name, err)
		}
	}
	if _, err := s.GetStreams(ctx, nil, whole, 0); !errors.Is(err, context.Canceled) {
		t.Errorf("streams: err = %v, want context.Canceled", err)
	}
}

// Scans run on the file-worker pool; a pool of one and a pool wider than the
// file list give the same exact answer.
func TestGetStreams_WorkerPoolSizeDoesNotChangeTheAnswer(t *testing.T) {
	var batches [][]schema.LogRow
	for i := 0; i < 6; i++ {
		batches = append(batches, []schema.LogRow{fvcRow(time.Duration(i)*time.Minute, "INFO"), fvcRow(time.Duration(i)*time.Minute+time.Second, "WARN")})
	}
	s := fvcStorage(t, batches...)
	q := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano())
	want := []logstorage.ValueWithHits{{Value: `{service.name="svc-INFO"}`, Hits: 6}, {Value: `{service.name="svc-WARN"}`, Hits: 6}}
	for _, workers := range []int{1, 64} {
		s.cfg.Query.FileWorkers = workers
		got, err := s.GetStreams(context.Background(), nil, q, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("workers=%d: streams = %v, want %v", workers, got, want)
		}
	}
}

// Scans skip row groups whose time range misses the window, and the window
// end is inclusive: a row exactly at the end that opens a row group is
// counted (the query path's end-exclusive check would drop it).
func TestFieldValues_RowGroupPruningKeepsTheWindowEnd(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Insert.RowGroupSize = 2
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	bw.AddLogRows([]schema.LogRow{
		fvcRow(1*time.Minute, "INFO"), fvcRow(2*time.Minute, "INFO"),
		fvcRow(3*time.Minute, "WARN"), fvcRow(4*time.Minute, "WARN"),
		fvcRow(5*time.Minute, "ERROR"), fvcRow(6*time.Minute, "ERROR"),
	})
	bw.triggerFlush()

	skipped0 := metrics.ParquetRowGroupsSkipped.Get("stats")
	// [2m, 3m]: the second row of the first group and the row opening the
	// second group, exactly at the window end; the third group is outside.
	q := mustParseQueryWithTime(t, "*", fvcBase.Add(2*time.Minute).UnixNano(), fvcBase.Add(3*time.Minute).UnixNano())
	got, err := s.GetFieldValues(context.Background(), nil, q, "level", 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := []logstorage.ValueWithHits{{Value: "INFO", Hits: 1}, {Value: "WARN", Hits: 1}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("field_values level = %v, want %v", got, want)
	}
	if metrics.ParquetRowGroupsSkipped.Get("stats") <= skipped0 {
		t.Error("the row group outside the window was read")
	}
}
