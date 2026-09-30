package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// VictoriaTraces v0.12.0 hides spans younger than -search.latencyOffset from
// LogsQL query APIs unless the request says disable_latency_offset=true; the
// traces select handler expresses that as a "_time up to now-offset" filter on
// the query (lakehouse-traces/internal/selectapi.applyLatencyOffset). The cold
// tier has to answer such a query exactly as hot VictoriaTraces does, so these
// tests hand the real Storage the query the handler builds.

func offsetFilter(t *testing.T, now time.Time, offset time.Duration) *logstorage.Filter {
	t.Helper()
	f, err := logstorage.ParseFilter("_time:<=" + now.Add(-offset).UTC().Format("2006-01-02T15:04:05.000000000Z07:00"))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func coldLatencyStorage(t *testing.T, now time.Time) (*Storage, *mockS3Server) {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())

	streamLit := `{resource_attr:service.name="api"}`
	rows := []traceParityRow{
		{TimestampUnixNano: now.Add(-5 * time.Second).UnixNano(), TraceID: "t-young", ServiceName: "api", SpanName: "young", Stream: streamLit},
		{TimestampUnixNano: now.Add(-2 * time.Minute).UnixNano(), TraceID: "t-old", ServiceName: "api", SpanName: "old", Stream: streamLit},
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[traceParityRow](&buf, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	base := now.Add(-time.Minute) // the file spans [now-2m, now]
	key := fmt.Sprintf("traces/dt=%s/hour=%02d/latency.parquet", base.Format("2006-01-02"), base.Hour())
	registerFileInMockS3(t, s, mock, key, buf.Bytes(), base)
	return s, mock
}

func traceIDsOf(t *testing.T, s *Storage, q *logstorage.Query) map[string]int {
	t.Helper()
	var mu sync.Mutex
	got := map[string]int{}
	err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range db.GetColumns(false) {
			if c.Name != "trace_id" {
				continue
			}
			for _, v := range c.Values {
				got[v]++
			}
		}
	})
	if err != nil {
		t.Fatalf("RunQuery(%s): %v", q, err)
	}
	return got
}

func TestColdLatencyOffset_HidesYoungRowsLikeHotVT(t *testing.T) {
	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	s, _ := coldLatencyStorage(t, now)
	start, end := now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano()

	build := func(query string, withOffset bool) *logstorage.Query {
		q := mustParseQueryWithTime(t, query, start, end)
		if withOffset {
			q.AddExtraFilters(offsetFilter(t, now, 30*time.Second))
		}
		return q
	}

	for _, query := range []string{`*`, `trace_id:in(t-young,t-old)`, `_stream:{resource_attr:service.name="api"}`} {
		// Default (offset on): only the row older than 30s.
		got := traceIDsOf(t, s, build(query, true))
		if got["t-young"] != 0 || got["t-old"] != 1 {
			t.Errorf("query %q with the offset: got %v, want only t-old", query, got)
		}
		// disable_latency_offset=true: nothing added, both rows.
		got = traceIDsOf(t, s, build(query, false))
		if got["t-young"] != 1 || got["t-old"] != 1 {
			t.Errorf("query %q without the offset: got %v, want both rows", query, got)
		}
	}

	// A trace_id lookup for the young trace: hot VT (offset applied) does not
	// return it; cold must not either, whatever widenTraceIDQueryToNow does to
	// the scan window.
	if got := traceIDsOf(t, s, build(`trace_id:t-young`, true)); len(got) != 0 {
		t.Errorf("trace_id:t-young with the offset: got %v, want nothing", got)
	}
	if got := traceIDsOf(t, s, build(`trace_id:t-young`, false)); got["t-young"] != 1 {
		t.Errorf("trace_id:t-young without the offset: got %v, want the row", got)
	}
}

// The buffer bridge is asked for the query's effective time range, so it
// honours the same end time as the parquet scan: with the offset the bridge is
// never asked for rows younger than now-offset, and the opt-out reaches now.
func TestColdLatencyOffset_BridgeAsksForEffectiveRange(t *testing.T) {
	now := time.Now()
	s, _ := coldLatencyStorage(t, now)

	var mu sync.Mutex
	var ends []int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var end int64
		_, _ = fmt.Sscan(r.URL.Query().Get("end"), &end)
		mu.Lock()
		ends = append(ends, end)
		mu.Unlock()
		w.Header().Set(buffer.TenantScopeHeader, "0:0")
	}))
	defer srv.Close()
	bridge := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 2 * time.Second}, config.ModeTraces)
	bridge.SetEndpoints([]string{srv.URL})
	s.bufferBridge = bridge

	lastEnd := func() int64 {
		mu.Lock()
		defer mu.Unlock()
		if len(ends) == 0 {
			t.Fatal("the bridge was never asked")
		}
		return ends[len(ends)-1]
	}

	start, end := now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano()
	q := mustParseQueryWithTime(t, `*`, start, end)
	q.AddExtraFilters(offsetFilter(t, now, 30*time.Second))
	traceIDsOf(t, s, q)
	if got := lastEnd(); got > now.Add(-29*time.Second).UnixNano() {
		t.Errorf("with the offset the bridge was asked up to %s, want <= now-30s", time.Unix(0, got).UTC())
	}

	q = mustParseQueryWithTime(t, `*`, start, end)
	traceIDsOf(t, s, q)
	if got := lastEnd(); got < now.UnixNano() {
		t.Errorf("without the offset the bridge was asked up to %s, want >= now", time.Unix(0, got).UTC())
	}
}
