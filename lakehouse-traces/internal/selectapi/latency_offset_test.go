package selectapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtselect/traces/tracecommon"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// VictoriaTraces v0.12.0 applies -search.latencyOffset (default 30s) to every
// LogsQL query API except live tailing, and lets a request opt out with
// disable_latency_offset=true. Lakehouse serves LogsQL through VictoriaLogs'
// handlers, so wrapVL adds the equivalent "_time up to now-offset" filter in
// traces mode. These tests drive the real VictoriaLogs handlers against a
// storage that behaves like a tier: it returns only the rows inside the time
// range of the query it is handed, which is what the cold tier, the buffer
// bridge and hot VictoriaTraces all do.

type timedRow struct {
	ts  time.Time
	msg string
}

// tierStore serves rows filtered by the query's own time range, and records
// the range it was asked for.
type tierStore struct {
	mockStore
	mu     sync.Mutex
	rows   []timedRow
	ranges [][2]int64
}

func (s *tierStore) record(q *logstorage.Query) (int64, int64) {
	start, end := q.GetFilterTimeRange()
	s.mu.Lock()
	s.ranges = append(s.ranges, [2]int64{start, end})
	s.mu.Unlock()
	return start, end
}

func (s *tierStore) RunQuery(_ context.Context, _ []logstorage.TenantID, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	start, end := s.record(q)

	var times, msgs []string
	for _, r := range s.rows {
		if n := r.ts.UnixNano(); n >= start && n <= end {
			times = append(times, r.ts.UTC().Format(time.RFC3339Nano))
			msgs = append(msgs, r.msg)
		}
	}
	if len(times) == 0 {
		return nil
	}
	var db logstorage.DataBlock
	db.SetColumns([]logstorage.BlockColumn{{Name: "_time", Values: times}, {Name: "_msg", Values: msgs}})
	writeBlock(0, &db)
	return nil
}

func (s *tierStore) GetFieldNames(_ context.Context, _ []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	s.record(q)
	return nil, nil
}

func (s *tierStore) GetFieldValues(_ context.Context, _ []logstorage.TenantID, q *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	s.record(q)
	return nil, nil
}

func (s *tierStore) GetStreamFieldNames(_ context.Context, _ []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	s.record(q)
	return nil, nil
}

func (s *tierStore) GetStreamFieldValues(_ context.Context, _ []logstorage.TenantID, q *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	s.record(q)
	return nil, nil
}

func (s *tierStore) GetStreams(_ context.Context, _ []logstorage.TenantID, q *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	s.record(q)
	return nil, nil
}

func (s *tierStore) GetStreamIDs(_ context.Context, _ []logstorage.TenantID, q *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	s.record(q)
	return nil, nil
}

func (s *tierStore) lastRange(t *testing.T) [2]int64 {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ranges) == 0 {
		t.Fatal("the storage was never queried")
	}
	return s.ranges[len(s.ranges)-1]
}

var _ storage.Storage = (*tierStore)(nil)

func latencyServer(t *testing.T, mode config.Mode, rows []timedRow) (*tierStore, *http.ServeMux) {
	t.Helper()
	st := &tierStore{rows: rows}
	internalvlstorage.SetStorage(st, delete.NewTombstoneStore())
	mux := http.NewServeMux()
	NewHandler(st, testConfig(mode)).Register(mux)
	return st, mux
}

func get(mux http.Handler, path string, args url.Values) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path+"?"+args.Encode(), nil))
	return rec
}

func TestLatencyOffset_HidesRowsYoungerThanTheOffset(t *testing.T) {
	now := time.Now()
	_, mux := latencyServer(t, config.ModeTraces, []timedRow{
		{now.Add(-5 * time.Second), "young"},
		{now.Add(-2 * time.Minute), "old"},
	})
	args := func(extra ...string) url.Values {
		v := url.Values{"query": {"*"}, "start": {"-1h"}}
		for i := 0; i+1 < len(extra); i += 2 {
			v.Set(extra[i], extra[i+1])
		}
		return v
	}

	// Default: the 5s-old row is inside the 30s offset and stays hidden.
	rec := get(mux, "/select/logsql/query", args())
	if rec.Code != http.StatusOK {
		t.Fatalf("default: status %d %q", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "young") || !strings.Contains(rec.Body.String(), "old") {
		t.Errorf("default query must return only the row older than the offset, got %q", rec.Body.String())
	}

	// Opt-out: everything is visible, as on a VictoriaTraces with the opt-out.
	rec = get(mux, "/select/logsql/query", args("disable_latency_offset", "true"))
	if !strings.Contains(rec.Body.String(), "young") || !strings.Contains(rec.Body.String(), "old") {
		t.Errorf("disable_latency_offset=true must return both rows, got %q", rec.Body.String())
	}

	// An explicit false is the default.
	rec = get(mux, "/select/logsql/query", args("disable_latency_offset", "false"))
	if strings.Contains(rec.Body.String(), "young") {
		t.Errorf("disable_latency_offset=false must keep the offset, got %q", rec.Body.String())
	}

	// A malformed value is a 400 with upstream's message, not a silent default.
	rec = get(mux, "/select/logsql/query", args("disable_latency_offset", "maybe"))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `cannot parse disable_latency_offset="maybe" as bool`) {
		t.Errorf("malformed opt-out: got %d %q, want 400 with upstream's message", rec.Code, rec.Body.String())
	}
}

func TestLatencyOffset_EndTimeIsNowMinusFlag(t *testing.T) {
	old := *tracecommon.LatencyOffset
	t.Cleanup(func() { *tracecommon.LatencyOffset = old })
	*tracecommon.LatencyOffset = 10 * time.Minute

	st, mux := latencyServer(t, config.ModeTraces, nil)
	before := time.Now()
	rec := get(mux, "/select/logsql/query", url.Values{"query": {"*"}})
	after := time.Now()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %q", rec.Code, rec.Body.String())
	}
	end := st.lastRange(t)[1]
	lo, hi := before.Add(-10*time.Minute).UnixNano(), after.Add(-10*time.Minute).UnixNano()
	if end < lo || end > hi {
		t.Errorf("effective end = %s, want within [now-10m] = [%s, %s]", time.Unix(0, end).UTC(), time.Unix(0, lo).UTC(), time.Unix(0, hi).UTC())
	}

	// The opt-out leaves the range open.
	rec = get(mux, "/select/logsql/query", url.Values{"query": {"*"}, "disable_latency_offset": {"1"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %q", rec.Code, rec.Body.String())
	}
	if end := st.lastRange(t)[1]; end < time.Now().Add(time.Hour).UnixNano() {
		t.Errorf("opt-out end = %s, want unbounded", time.Unix(0, end).UTC())
	}
}

// A caller's own end (or a query's own _time filter) is intersected with the
// offset, never widened by it: the smaller wins, as upstream's AND of filters.
func TestLatencyOffset_IntersectsWithCallerRange(t *testing.T) {
	st, mux := latencyServer(t, config.ModeTraces, nil)
	long := time.Now().Add(-time.Hour).UTC()
	rec := get(mux, "/select/logsql/query", url.Values{"query": {"*"}, "end": {long.Format(time.RFC3339Nano)}})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %q", rec.Code, rec.Body.String())
	}
	if end := st.lastRange(t)[1]; end >= time.Now().Add(-30*time.Minute).UnixNano() {
		t.Errorf("caller end one hour ago was widened to %s", time.Unix(0, end).UTC())
	}

	rec = get(mux, "/select/logsql/query", url.Values{"query": {"_time:1d"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %q", rec.Code, rec.Body.String())
	}
	if end := st.lastRange(t)[1]; end > time.Now().Add(-25*time.Second).UnixNano() {
		t.Errorf("query _time:1d end = %s, want capped at now-30s", time.Unix(0, end).UTC())
	}
}

// Every LogsQL query API gets the offset; /select/tenant_ids does not, matching
// upstream, whose ProcessTenantIDsRequest never goes through parseCommonArgs.
func TestLatencyOffset_AppliesToEveryQueryAPI(t *testing.T) {
	now := time.Now()
	apis := []struct {
		path string
		args url.Values
	}{
		{"/select/logsql/query", url.Values{"query": {"*"}, "start": {"-1h"}}},
		{"/select/logsql/hits", url.Values{"query": {"*"}, "start": {"-1h"}, "step": {"5m"}}},
		{"/select/logsql/facets", url.Values{"query": {"*"}, "start": {"-1h"}}},
		{"/select/logsql/field_names", url.Values{"query": {"*"}, "start": {"-1h"}}},
		{"/select/logsql/field_values", url.Values{"query": {"*"}, "start": {"-1h"}, "field": {"f"}}},
		{"/select/logsql/stream_field_names", url.Values{"query": {"*"}, "start": {"-1h"}}},
		{"/select/logsql/stream_field_values", url.Values{"query": {"*"}, "start": {"-1h"}, "field": {"f"}}},
		{"/select/logsql/streams", url.Values{"query": {"*"}, "start": {"-1h"}}},
		{"/select/logsql/stream_ids", url.Values{"query": {"*"}, "start": {"-1h"}}},
		{"/select/logsql/stats_query", url.Values{"query": {"* | stats count()"}}},
		{"/select/logsql/stats_query_range", url.Values{"query": {"* | stats count()"}, "start": {"-1h"}, "step": {"5m"}}},
	}
	for _, api := range apis {
		t.Run(strings.TrimPrefix(api.path, "/select/logsql/"), func(t *testing.T) {
			st, mux := latencyServer(t, config.ModeTraces, nil)
			if rec := get(mux, api.path, api.args); rec.Code != http.StatusOK {
				t.Fatalf("status %d %q", rec.Code, rec.Body.String())
			}
			if end := st.lastRange(t)[1]; end > now.Add(-25*time.Second).UnixNano() {
				t.Errorf("end = %s, want capped at now-30s", time.Unix(0, end).UTC())
			}

			args := url.Values{}
			for k, v := range api.args {
				args[k] = v
			}
			args.Set("disable_latency_offset", "true")
			st, mux = latencyServer(t, config.ModeTraces, nil)
			if rec := get(mux, api.path, args); rec.Code != http.StatusOK {
				t.Fatalf("opt-out status %d %q", rec.Code, rec.Body.String())
			}
			if end := st.lastRange(t)[1]; end < now.Add(time.Hour).UnixNano() {
				t.Errorf("opt-out end = %s, want the offset gone", time.Unix(0, end).UTC())
			}
		})
	}

	t.Run("tenant_ids", func(t *testing.T) {
		st, mux := latencyServer(t, config.ModeTraces, nil)
		rec := get(mux, "/select/tenant_ids", url.Values{})
		if rec.Code >= 500 {
			t.Fatalf("status %d %q", rec.Code, rec.Body.String())
		}
		if len(st.ranges) != 0 {
			t.Errorf("tenant_ids must not run a query")
		}
		if got := rec.Header().Get("Content-Type"); got == "" && rec.Code == http.StatusOK {
			t.Log("tenant_ids answered without a content type")
		}
	})
}

// Logs mode is VictoriaLogs, which has no latency offset: nothing is added.
func TestLatencyOffset_NotAppliedInLogsMode(t *testing.T) {
	now := time.Now()
	_, mux := latencyServer(t, config.ModeLogs, []timedRow{{now.Add(-time.Second), "fresh"}})
	rec := get(mux, "/select/logsql/query", url.Values{"query": {"*"}, "start": {"-1h"}})
	if !strings.Contains(rec.Body.String(), "fresh") {
		t.Errorf("logs mode must not hide recent rows, got %d %q", rec.Code, rec.Body.String())
	}
}

func TestApplyLatencyOffset_DirectCases(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 123456789, time.UTC)
	cases := []struct {
		name, query, wantFilter string
		wantErr                 bool
	}{
		{"default", "query=*", "_time:<=2026-09-30T11:59:30.123456789Z", false},
		{"opt out", "query=*&disable_latency_offset=true", "", false},
		{"opt out 1", "query=*&disable_latency_offset=1", "", false},
		{"explicit false", "query=*&disable_latency_offset=false", "_time:<=2026-09-30T11:59:30.123456789Z", false},
		{"bad value", "query=*&disable_latency_offset=x", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/select/logsql/query?"+tc.query, nil)
			err := applyLatencyOffset(r, now)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			got := r.Form["extra_filters"]
			if tc.wantFilter == "" {
				if len(got) != 0 {
					t.Errorf("extra_filters = %v, want none", got)
				}
				return
			}
			if len(got) != 1 || got[0] != tc.wantFilter {
				t.Errorf("extra_filters = %v, want [%s]", got, tc.wantFilter)
			}
			if _, err := logstorage.ParseFilter(got[0]); err != nil {
				t.Errorf("the added filter does not parse: %v", err)
			}
		})
	}
}
