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

func fvcSpan(at time.Duration, name string) schema.TraceRow {
	ts := fvcBase.Add(at).UnixNano()
	return schema.TraceRow{
		TimestampUnixNano: ts, StartTimeUnixNano: ts, TraceID: "t" + name, SpanID: "s" + name,
		SpanName: name, ServiceName: "svc-" + name,
		Stream: `{resource_attr:service.name="svc-` + name + `"}`, StreamID: "id-" + name,
	}
}

// fvcStorage flushes each batch as its own object in the same partition.
func fvcStorage(t *testing.T, batches ...[]schema.TraceRow) *Storage {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces // as the traces binary runs (the buffer bridge decodes spans)
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	for _, b := range batches {
		bw.AddTraceRows(b)
		bw.triggerFlush()
	}
	return s
}

// Label counts of the object inside the window plus the in-window rows of the
// straddling object add up to the exact hits.
func TestTraceFieldValues_LabelCountsAndScanAddUpExactly(t *testing.T) {
	inside := []schema.TraceRow{fvcSpan(5*time.Minute, "GET"), fvcSpan(6*time.Minute, "GET")}
	straddling := []schema.TraceRow{
		fvcSpan(10*time.Minute, "GET"), fvcSpan(11*time.Minute, "PUT"),
		fvcSpan(40*time.Minute, "GET"), fvcSpan(41*time.Minute, "GET"), fvcSpan(42*time.Minute, "POST"),
	}
	s := fvcStorage(t, inside, straddling)
	q := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(30*time.Minute).UnixNano())

	agg0, scan0 := metrics.FieldValuesFiles.Get("aggregate"), metrics.FieldValuesFiles.Get("scan")
	got, err := s.GetFieldValues(context.Background(), nil, q, "name", 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []logstorage.ValueWithHits{{Value: "GET", Hits: 3}, {Value: "PUT", Hits: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("field_values name = %v, want %v", got, want)
	}
	if d := metrics.FieldValuesFiles.Get("aggregate") - agg0; d != 1 {
		t.Errorf("objects answered from label counts = %d, want 1", d)
	}
	if d := metrics.FieldValuesFiles.Get("scan") - scan0; d != 1 {
		t.Errorf("objects scanned = %d, want 1", d)
	}
}

// Ordered and limited the way VictoriaTraces (VictoriaLogs underneath) does it.
func TestTraceFieldValues_LimitFollowsUpstream(t *testing.T) {
	s := fvcStorage(t, []schema.TraceRow{
		fvcSpan(1*time.Minute, "PUT"), fvcSpan(2*time.Minute, "PUT"),
		fvcSpan(3*time.Minute, "GET"), fvcSpan(4*time.Minute, "GET"), fvcSpan(5*time.Minute, "GET"),
		fvcSpan(6*time.Minute, "DELETE"),
	})
	q := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano())
	for _, tc := range []struct {
		limit uint64
		want  []logstorage.ValueWithHits
	}{
		{0, []logstorage.ValueWithHits{{Value: "GET", Hits: 3}, {Value: "PUT", Hits: 2}, {Value: "DELETE", Hits: 1}}},
		{2, []logstorage.ValueWithHits{{Value: "DELETE", Hits: 0}, {Value: "GET", Hits: 0}}},
	} {
		got, err := s.GetFieldValues(context.Background(), nil, q, "name", tc.limit)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("limit=%d: %v, want %v", tc.limit, got, tc.want)
		}
		streams, err := s.GetStreams(context.Background(), nil, q, tc.limit)
		if err != nil {
			t.Fatal(err)
		}
		if len(streams) != len(tc.want) || streams[0].Hits != tc.want[0].Hits {
			t.Errorf("streams limit=%d: %v, want the same shape as %v", tc.limit, streams, tc.want)
		}
	}
}

func TestTraceFieldValues_CancelledRequestIsAnError(t *testing.T) {
	s := fvcStorage(t, []schema.TraceRow{fvcSpan(time.Minute, "GET"), fvcSpan(50*time.Minute, "PUT")})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	whole := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano())
	cut := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(30*time.Minute).UnixNano())
	for name, q := range map[string]*logstorage.Query{"label counts": whole, "scan": cut} {
		if _, err := s.GetFieldValues(ctx, nil, q, "name", 0); !errors.Is(err, context.Canceled) {
			t.Errorf("%s: err = %v, want context.Canceled", name, err)
		}
	}
	if _, err := s.GetStreams(ctx, nil, whole, 0); !errors.Is(err, context.Canceled) {
		t.Errorf("streams: err = %v, want context.Canceled", err)
	}
}

// Twin of the logs test: row groups outside the window are skipped, and a
// span exactly at the window end that opens a row group is counted.
func TestTraceFieldValues_RowGroupPruningKeepsTheWindowEnd(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	s.cfg.Insert.RowGroupSize = 2
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	bw.AddTraceRows([]schema.TraceRow{
		fvcSpan(1*time.Minute, "GET"), fvcSpan(2*time.Minute, "GET"),
		fvcSpan(3*time.Minute, "PUT"), fvcSpan(4*time.Minute, "PUT"),
		fvcSpan(5*time.Minute, "POST"), fvcSpan(6*time.Minute, "POST"),
	})
	bw.triggerFlush()

	skipped0 := metrics.ParquetRowGroupsSkipped.Get("stats")
	q := mustParseQueryWithTime(t, "*", fvcBase.Add(2*time.Minute).UnixNano(), fvcBase.Add(3*time.Minute).UnixNano())
	got, err := s.GetFieldValues(context.Background(), nil, q, "name", 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := []logstorage.ValueWithHits{{Value: "GET", Hits: 1}, {Value: "PUT", Hits: 1}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("field_values name = %v, want %v", got, want)
	}
	if metrics.ParquetRowGroupsSkipped.Get("stats") <= skipped0 {
		t.Error("the row group outside the window was read")
	}
}
