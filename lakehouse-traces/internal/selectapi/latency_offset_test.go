package selectapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtselect/traces/tracecommon"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

// VictoriaTraces v0.12.0 applies -search.latencyOffset (default 30s) to every
// LogsQL query API except live tailing, and lets a request opt out with
// disable_latency_offset=true. The traces binary serves LogsQL through
// VictoriaTraces' own app/vtselect/logsql handlers, so the offset is upstream's
// code, not ours. These tests drive those handlers over the Lakehouse storage
// adapter against a store that behaves like a tier: it evaluates the query's
// own filter (time filters included) on every row, which is what the cold tier,
// the buffer bridge and hot VictoriaTraces all do.

type tierRow struct {
	ts     time.Time
	fields map[string]string
}

func row(ts time.Time, msg string, kv ...string) tierRow {
	f := map[string]string{"_msg": msg}
	for i := 0; i+1 < len(kv); i += 2 {
		f[kv[i]] = kv[i+1]
	}
	return tierRow{ts: ts, fields: f}
}

// tierStore serves rows filtered by the query it is handed and records the time
// range it was asked for.
type tierStore struct {
	mockStore
	mu     sync.Mutex
	rows   []tierRow
	ranges [][2]int64
	// dyn, when set, replaces rows: it is handed the query's effective range,
	// and the rows it returns are filtered by the query's filter only (not by the
	// range), so the filter's own bound decides what is visible.
	dyn func(start, end int64) []tierRow
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
	rows := s.rows
	rangeFilter := true
	if s.dyn != nil {
		rows = s.dyn(start, end)
		rangeFilter = false
	}
	f := logstorage.QueryFilter(q)

	var kept []tierRow
	names := map[string]bool{"_time": true}
	for _, r := range rows {
		n := r.ts.UnixNano()
		if rangeFilter && (n < start || n > end) {
			continue
		}
		fields := []logstorage.Field{{Name: "_time", Value: r.ts.UTC().Format(time.RFC3339Nano)}}
		for k, v := range r.fields {
			fields = append(fields, logstorage.Field{Name: k, Value: v})
		}
		if f != nil && !f.MatchRow(fields) {
			continue
		}
		kept = append(kept, r)
		for k := range r.fields {
			names[k] = true
		}
	}
	if len(kept) == 0 {
		return nil
	}
	sorted := make([]string, 0, len(names))
	for k := range names {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	cols := make([]logstorage.BlockColumn, 0, len(sorted))
	for _, name := range sorted {
		vals := make([]string, len(kept))
		for i, r := range kept {
			if name == "_time" {
				vals[i] = r.ts.UTC().Format(time.RFC3339Nano)
			} else {
				vals[i] = r.fields[name]
			}
		}
		cols = append(cols, logstorage.BlockColumn{Name: name, Values: vals})
	}
	var db logstorage.DataBlock
	db.SetColumns(cols)
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

func latencyServer(t *testing.T, rows []tierRow) (*tierStore, *http.ServeMux) {
	t.Helper()
	st := &tierStore{rows: rows}
	vtstorageadapter.Init(st)
	mux := http.NewServeMux()
	NewHandler(st, testConfig(config.ModeTraces)).Register(mux)
	return st, mux
}

func get(mux http.Handler, path string, args url.Values) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path+"?"+args.Encode(), nil))
	return rec
}

func msgs(t *testing.T, rec *httptest.ResponseRecorder) []string {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(rec.Body.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]string
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("not a JSON line %q: %v", line, err)
		}
		out = append(out, m["_msg"])
	}
	sort.Strings(out)
	return out
}

func q(extra ...string) url.Values {
	v := url.Values{"query": {"*"}, "start": {"-2h"}}
	for i := 0; i+1 < len(extra); i += 2 {
		v.Set(extra[i], extra[i+1])
	}
	return v
}

func TestLatencyOffset_HidesRowsYoungerThanTheOffset(t *testing.T) {
	now := time.Now()
	_, mux := latencyServer(t, []tierRow{row(now.Add(-5*time.Second), "young"), row(now.Add(-2*time.Minute), "old")})

	if got := msgs(t, get(mux, "/select/logsql/query", q())); strings.Join(got, ",") != "old" {
		t.Errorf("default: got %v, want only the row older than the offset", got)
	}
	if got := msgs(t, get(mux, "/select/logsql/query", q("disable_latency_offset", "true"))); strings.Join(got, ",") != "old,young" {
		t.Errorf("disable_latency_offset=true: got %v, want both rows", got)
	}
	if got := msgs(t, get(mux, "/select/logsql/query", q("disable_latency_offset", "false"))); strings.Join(got, ",") != "old" {
		t.Errorf("disable_latency_offset=false: got %v, want the default", got)
	}
	rec := get(mux, "/select/logsql/query", q("disable_latency_offset", "maybe"))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `cannot parse disable_latency_offset="maybe" as bool`) {
		t.Errorf("malformed opt-out: got %d %q, want 400 with upstream's message", rec.Code, rec.Body.String())
	}
}

// The bound is inclusive at the nanosecond, as upstream's AddTimeFilter: a row
// exactly at now-offset is visible, one nanosecond newer is not, one older is.
// The store hands back rows at E-1, E and E+1 for the query's own end E and
// leaves the decision to the query's filter, so this fails if the bound turns
// into a strict "<" or moves by a nanosecond.
func TestLatencyOffset_BoundaryIsInclusiveAtTheNanosecond(t *testing.T) {
	st, mux := latencyServer(t, nil)
	st.dyn = func(_, end int64) []tierRow {
		if end == 1<<63-1 {
			return nil
		}
		return []tierRow{
			row(time.Unix(0, end-1), "before"),
			row(time.Unix(0, end), "exact"),
			row(time.Unix(0, end+1), "after"),
		}
	}
	got := msgs(t, get(mux, "/select/logsql/query", q()))
	if strings.Join(got, ",") != "before,exact" {
		t.Errorf("rows at end-1ns, end, end+1ns: got %v, want [before exact]", got)
	}
}

func TestLatencyOffset_EndTimeIsNowMinusFlag(t *testing.T) {
	old := *tracecommon.LatencyOffset
	t.Cleanup(func() { *tracecommon.LatencyOffset = old })
	*tracecommon.LatencyOffset = 10 * time.Minute

	st, mux := latencyServer(t, nil)
	before := time.Now()
	if rec := get(mux, "/select/logsql/query", q()); rec.Code != http.StatusOK {
		t.Fatalf("status %d %q", rec.Code, rec.Body.String())
	}
	after := time.Now()
	end := st.lastRange(t)[1]
	lo, hi := before.Add(-10*time.Minute).UnixNano(), after.Add(-10*time.Minute).UnixNano()
	if end < lo || end > hi {
		t.Errorf("effective end = %s, want within now-10m [%s, %s]", time.Unix(0, end).UTC(), time.Unix(0, lo).UTC(), time.Unix(0, hi).UTC())
	}
	if rec := get(mux, "/select/logsql/query", q("disable_latency_offset", "1")); rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if end := st.lastRange(t)[1]; end < time.Now().Add(time.Hour).UnixNano() {
		t.Errorf("opt-out end = %s, want unbounded", time.Unix(0, end).UTC())
	}
}

// The offset composes with what the caller sends: the smaller end wins, and the
// caller's extra_filters and extra_stream_filters narrow the result further
// without ever lifting the offset.
func TestLatencyOffset_ComposesWithCallerArguments(t *testing.T) {
	now := time.Now()
	rows := []tierRow{
		row(now.Add(-5*time.Second), "young", "_stream", `{app="a"}`),
		row(now.Add(-2*time.Minute), "old-a", "_stream", `{app="a"}`),
		row(now.Add(-3*time.Minute), "old-b", "_stream", `{app="b"}`),
	}
	st, mux := latencyServer(t, rows)

	// The caller's own earlier end is not widened.
	early := now.Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	if rec := get(mux, "/select/logsql/query", q("end", early)); rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if end := st.lastRange(t)[1]; end >= now.Add(-30*time.Minute).UnixNano() {
		t.Errorf("caller end one hour ago was widened to %s", time.Unix(0, end).UTC())
	}
	// A query's own _time filter is capped, not extended.
	if rec := get(mux, "/select/logsql/query", url.Values{"query": {"_time:1d"}}); rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if end := st.lastRange(t)[1]; end > now.Add(-25*time.Second).UnixNano() {
		t.Errorf("query _time:1d end = %s, want capped at now-30s", time.Unix(0, end).UTC())
	}

	for _, tc := range []struct {
		name string
		args url.Values
		want string
	}{
		{"extra_filters narrows", q("extra_filters", "_msg:old-a"), "old-a"},
		{"extra_filters cannot lift the offset", q("extra_filters", "_msg:young"), ""},
		{"extra_filters with the opt-out", q("extra_filters", "_msg:young", "disable_latency_offset", "true"), "young"},
		{"extra_stream_filters narrows", q("extra_stream_filters", `{app="b"}`), "old-b"},
		{"extra_stream_filters cannot lift the offset", q("extra_stream_filters", `{app="a"}`), "old-a"},
		{"both", q("extra_filters", "_msg:old-a", "extra_stream_filters", `{app="a"}`), "old-a"},
	} {
		if got := strings.Join(msgs(t, get(mux, "/select/logsql/query", tc.args)), ","); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

// Every LogsQL query API gets the offset unless the caller opts out; the two
// that never parse the common arguments upstream (tenant_ids,
// query_time_range) neither apply it nor parse disable_latency_offset, so a
// malformed value is not an error there (LogsQLRoutes is held to upstream by
// TestLogsQLRouteTableMatchesVendoredVT).
func TestLatencyOffset_PerAPI(t *testing.T) {
	now := time.Now()
	apis := map[string]url.Values{
		"query":               q(),
		"hits":                q("step", "5m"),
		"facets":              q(),
		"field_names":         q(),
		"field_values":        q("field", "f"),
		"stream_field_names":  q(),
		"stream_field_values": q("field", "f"),
		"streams":             q(),
		"stream_ids":          q(),
		"stats_query":         url.Values{"query": {"* | stats count()"}},
		"stats_query_range":   q("query", "* | stats count()", "step", "5m"),
	}
	for name, args := range apis {
		t.Run(name, func(t *testing.T) {
			path := "/select/logsql/" + name
			st, mux := latencyServer(t, nil)
			if rec := get(mux, path, args); rec.Code != http.StatusOK {
				t.Fatalf("status %d %q", rec.Code, rec.Body.String())
			}
			if end := st.lastRange(t)[1]; end > now.Add(-25*time.Second).UnixNano() {
				t.Errorf("end = %s, want capped at now-30s", time.Unix(0, end).UTC())
			}
			args2 := url.Values{}
			for k, v := range args {
				args2[k] = v
			}
			args2.Set("disable_latency_offset", "true")
			st, mux = latencyServer(t, nil)
			if rec := get(mux, path, args2); rec.Code != http.StatusOK {
				t.Fatalf("opt-out status %d %q", rec.Code, rec.Body.String())
			}
			if end := st.lastRange(t)[1]; end < now.Add(time.Hour).UnixNano() {
				t.Errorf("opt-out end = %s, want the offset gone", time.Unix(0, end).UTC())
			}
		})
	}

	t.Run("query_time_range ignores the argument", func(t *testing.T) {
		_, mux := latencyServer(t, []tierRow{row(now.Add(-5*time.Second), "young")})
		for _, v := range []string{"true", "false", "x"} {
			rec := get(mux, "/select/logsql/query_time_range", q("disable_latency_offset", v))
			if rec.Code != http.StatusOK {
				t.Errorf("disable_latency_offset=%s: got %d %q, want 200 (upstream never parses it here)", v, rec.Code, rec.Body.String())
			}
		}
	})
	t.Run("tenant_ids ignores the argument", func(t *testing.T) {
		st, mux := latencyServer(t, nil)
		rec := get(mux, "/select/tenant_ids", url.Values{"disable_latency_offset": {"x"}})
		if rec.Code >= 500 || rec.Code == http.StatusBadRequest && strings.Contains(rec.Body.String(), "disable_latency_offset") {
			t.Errorf("tenant_ids: %d %q", rec.Code, rec.Body.String())
		}
		if len(st.ranges) != 0 {
			t.Errorf("tenant_ids must not run a query")
		}
	})
}
