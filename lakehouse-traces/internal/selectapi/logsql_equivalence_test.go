package selectapi

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

// The same VictoriaTraces LogsQL handler answers a query over two different
// storages: a real VictoriaTraces local logstorage ("hot", which is what
// upstream ships) and the Lakehouse adapter over a tier store. For the same
// rows and the same request the two answers must be equal, including for the
// cases where a hand-built time filter drifts from upstream's: stats rate()
// (whose step comes from the query's time range), options(time_offset=...),
// options(ignore_global_time_filter=true), in() subqueries, join and union.

func addHot(t *testing.T, r tierRow) {
	t.Helper()
	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	fields := make([]logstorage.Field, 0, len(r.fields))
	for k, v := range r.fields {
		fields = append(fields, logstorage.Field{Name: k, Value: v})
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
	lr.MustAdd(logstorage.TenantID{}, r.ts.UnixNano(), fields, 0)
	(&vtstorage.Storage{}).MustAddRows(lr)
}

func flushHot(t *testing.T) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/internal/force_flush", nil)
	rec := httptest.NewRecorder()
	if !vtstorage.RequestHandler(rec, req) || rec.Code != http.StatusOK {
		t.Fatalf("force_flush: handled, code %d %q", rec.Code, rec.Body.String())
	}
}

// answer runs one request against a handler and returns its rows: JSON lines
// with every value kept as a string.
func answer(t *testing.T, h http.Handler, path string, args url.Values) ([]map[string]string, int) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path+"?"+args.Encode(), nil))
	var out []map[string]string
	for _, line := range strings.Split(strings.TrimSpace(rec.Body.String()), "\n") {
		if line == "" {
			continue
		}
		m := map[string]string{}
		var raw map[string]any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			// A non-JSON body (an error text) compares as one opaque row.
			return []map[string]string{{"body": strings.TrimSpace(rec.Body.String())}}, rec.Code
		}
		for k, v := range raw {
			// The reference storage adds its stream bookkeeping columns; the
			// tier store has no streams.
			if k == "_stream" || k == "_stream_id" {
				continue
			}
			switch x := v.(type) {
			case string:
				m[k] = x
			default:
				b, _ := json.Marshal(x)
				m[k] = string(b)
			}
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return rowKey(out[i]) < rowKey(out[j]) })
	return out, rec.Code
}

func rowKey(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k + "=" + m[k] + ";")
	}
	return b.String()
}

// sameRows compares two answers; numeric values must agree within relTol (the
// two runs compute "now" a few milliseconds apart, which moves a rate by parts
// per million and nothing else).
func sameRows(a, b []map[string]string, relTol float64) (bool, string) {
	if len(a) != len(b) {
		return false, "row counts differ"
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false, "column sets differ: " + rowKey(a[i]) + " vs " + rowKey(b[i])
		}
		for k, va := range a[i] {
			vb, ok := b[i][k]
			if !ok {
				return false, "missing column " + k
			}
			if va == vb {
				continue
			}
			if k == "data" {
				if ok, why := sameData(va, vb, relTol); !ok {
					return false, why
				}
				continue
			}
			fa, ea := strconv.ParseFloat(va, 64)
			fb, eb := strconv.ParseFloat(vb, 64)
			if ea != nil || eb != nil || k == "_time" {
				return false, k + ": " + va + " vs " + vb
			}
			if math.Abs(fa-fb) > relTol*math.Max(math.Abs(fa), math.Abs(fb)) {
				return false, k + ": " + va + " vs " + vb
			}
		}
	}
	return true, ""
}

var (
	dataStampRe = regexp.MustCompile(`\[\d+(\.\d+)?,"`)
	dataNumRe   = regexp.MustCompile(`"([-+0-9.eE]+|NaN|Inf)"`)
)

// sameData compares two Prometheus-style bodies: same shape, sample timestamps
// ignored, sample values within relTol.
func sameData(a, b string, relTol float64) (bool, string) {
	skel := func(s string) (string, []float64) {
		s = dataStampRe.ReplaceAllString(s, `[0,"`)
		var nums []float64
		s = dataNumRe.ReplaceAllStringFunc(s, func(m string) string {
			f, err := strconv.ParseFloat(strings.Trim(m, `"`), 64)
			if err != nil {
				return m
			}
			nums = append(nums, f)
			return `"#"`
		})
		return s, nums
	}
	sa, na := skel(a)
	sb, nb := skel(b)
	if sa != sb || len(na) != len(nb) {
		return false, "data shape: " + a + " vs " + b
	}
	for i := range na {
		if math.Abs(na[i]-nb[i]) > relTol*math.Max(math.Abs(na[i]), math.Abs(nb[i])) {
			return false, "data value: " + a + " vs " + b
		}
	}
	return true, ""
}

func TestLogsQLServedEqualsUpstreamOnTheSameData(t *testing.T) {
	t.Chdir(t.TempDir()) // vtstorage's default -storageDataPath is relative
	vtstorage.Init()
	t.Cleanup(vtstorage.Stop)

	now := time.Now()
	ago := func(d time.Duration) time.Time { return now.Add(-d) }
	rows := []tierRow{
		row(ago(5*time.Second), "m-5s", "level", "error", "app", "a"),
		row(ago(25*time.Second), "m-25s", "level", "warn", "app", "a"),
		row(ago(35*time.Second), "m-35s", "level", "error", "app", "b"),
		row(ago(2*time.Minute), "m-2m", "level", "error", "app", "b"),
		row(ago(10*time.Minute), "m-10m", "level", "warn", "app", "a"),
		row(ago(time.Hour+5*time.Second), "m-1h5s", "level", "error", "app", "a"),
		row(ago(time.Hour+10*time.Minute), "m-1h10m", "level", "warn", "app", "b"),
	}
	for _, r := range rows {
		addHot(t, r)
	}
	flushHot(t)

	// Served: the traces binary's mux over the Lakehouse adapter.
	st := &tierStore{rows: rows}
	served := http.NewServeMux()
	NewHandler(st, testConfig(config.ModeTraces)).Register(served)

	// Upstream: VictoriaTraces' handler straight over its own storage.
	upstream := func(path string) http.Handler {
		for _, rt := range LogsQLRoutes {
			if rt.Path == path {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					logsqlHandlers[rt.Handler](context.Background(), w, r)
				})
			}
		}
		t.Fatalf("no route %s", path)
		return nil
	}

	start := now.Add(-3 * time.Hour).UTC().Format(time.RFC3339Nano)
	cases := []struct {
		name  string
		path  string
		args  url.Values
		empty bool // the case must return rows, or it proves nothing
		// gap, when set, names a documented cold-tier difference from upstream
		// that has nothing to do with the latency offset. The case then asserts
		// the difference is still there, so whoever closes it also deletes the
		// entry.
		gap string
	}{
		{"all rows", "/select/logsql/query", url.Values{"query": {"*"}, "start": {start}}, true, ""},
		{"count", "/select/logsql/query", url.Values{"query": {"* | stats count() n"}, "start": {start}}, true, ""},
		{"rate()", "/select/logsql/query", url.Values{"query": {"* | stats rate() r"}, "start": {start}}, true, ""},
		{"rate_sum()", "/select/logsql/query", url.Values{"query": {"* | stats rate_sum(1) r"}, "start": {start}}, false, ""},
		{"rate() on stats_query", "/select/logsql/stats_query", url.Values{"query": {"* | stats rate() r"}, "start": {start}}, true, ""},
		{"rate() by level on stats_query_range", "/select/logsql/stats_query_range", url.Values{"query": {"* | stats by (level) rate() r"}, "start": {start}, "step": {"30m"}}, true, ""},
		{"hits", "/select/logsql/hits", url.Values{"query": {"*"}, "start": {start}, "step": {"30m"}}, true, ""},
		{"time_offset", "/select/logsql/query", url.Values{"query": {"options(time_offset=1h) * | stats count() n"}, "start": {start}}, false, ""},
		{"time_offset rows", "/select/logsql/query", url.Values{"query": {"options(time_offset=1h) *"}, "start": {start}}, true,
			"hot storage adds the time_offset to the _time it returns (storage_search.go, subTimeOffsetToTimestamps); the cold tier filters by the shifted range but returns _time unshifted"},
		{"in() subquery without pipes", "/select/logsql/query", url.Values{"query": {"_msg:in(level:error | fields _msg)"}, "start": {start}}, true,
			"a query with no pipes never reaches RunQueryExternalWithSubqueries, so an in() filter in it stays unresolved (a documented cold-tier gap, patches/vl-*/external_query.go.src)"},
		{"ignore_global_time_filter", "/select/logsql/query", url.Values{"query": {"options(ignore_global_time_filter=true) * | stats count() n"}, "start": {start}}, true, ""},
		{"in() subquery", "/select/logsql/query", url.Values{"query": {"_msg:in(level:error | fields _msg) | stats count() n"}, "start": {start}}, true, ""},
		{"in() subquery, ignore_global_time_filter", "/select/logsql/query", url.Values{"query": {"options(ignore_global_time_filter=true) _msg:in(level:error | fields _msg) | stats count() n"}, "start": {start}}, true, ""},
		{"join", "/select/logsql/query", url.Values{"query": {"level:error | join by (app) (* | stats by (app) count() c)"}, "start": {start}}, true, ""},
		{"union", "/select/logsql/query", url.Values{"query": {"level:error | union (level:warn)"}, "start": {start}}, true, ""},
		{"union, ignore_global_time_filter", "/select/logsql/query", url.Values{"query": {"options(ignore_global_time_filter=true) level:error | union (level:warn)"}, "start": {start}}, true, ""},
		{"with the opt-out", "/select/logsql/query", url.Values{"query": {"*"}, "start": {start}, "disable_latency_offset": {"true"}}, true, ""},
		{"rate() with the opt-out", "/select/logsql/query", url.Values{"query": {"* | stats rate() r"}, "start": {start}, "disable_latency_offset": {"true"}}, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The hot storage is the reference: no external hook installed.
			vtstorage.SetExternalStorage(nil)
			want, wantCode := answer(t, upstream(tc.path), tc.path, tc.args)
			vtstorageadapter.Init(st)
			got, gotCode := answer(t, served, tc.path, tc.args)
			vtstorage.SetExternalStorage(nil)

			if gotCode != wantCode {
				t.Fatalf("status: served %d, upstream %d (%v)", gotCode, wantCode, want)
			}
			if tc.empty && len(want) == 0 {
				t.Fatalf("the upstream answer is empty: the case proves nothing")
			}
			if tc.gap != "" {
				if ok, _ := sameRows(got, want, 1e-3); ok {
					t.Errorf("the documented gap is closed (%s): delete the gap entry", tc.gap)
				}
				return
			}
			if ok, why := sameRows(got, want, 1e-3); !ok {
				t.Errorf("served != upstream: %s\nserved:   %v\nupstream: %v", why, got, want)
			}
		})
	}
}

// An end-to-end rate() through the served handler: 10 minutes of data, the offset
// on, and the value is the row count over the range upstream uses (start to
// now minus the offset), not over a range that ends at now.
func TestRateThroughTheServedHandler(t *testing.T) {
	now := time.Now()
	var rows []tierRow
	for i := 1; i <= 20; i++ { // older than the offset, inside the range
		rows = append(rows, row(now.Add(-time.Duration(60+i)*time.Second), "old"))
	}
	for i := 0; i < 5; i++ { // younger than the offset: hidden
		rows = append(rows, row(now.Add(-time.Duration(1+i)*time.Second), "young"))
	}
	st, mux := latencyServer(t, rows)
	start := now.Add(-time.Hour)
	args := url.Values{"query": {"* | stats rate() r"}, "start": {start.UTC().Format(time.RFC3339Nano)}}

	got, code := answer(t, mux, "/select/logsql/query", args)
	if code != http.StatusOK || len(got) != 1 {
		t.Fatalf("rate(): %d %v", code, got)
	}
	rate, err := strconv.ParseFloat(got[0]["r"], 64)
	if err != nil {
		t.Fatal(err)
	}
	end := time.Unix(0, st.lastRange(t)[1])
	want := 20 / end.Sub(start).Seconds()
	if math.Abs(rate-want) > 1e-3*want {
		t.Errorf("rate() = %g, want 20 rows over [start, now-offset] = %g", rate, want)
	}
	// The same query with the opt-out sees all 25 rows, and a range with no
	// upper bound, for which upstream's rate() divides by one second.
	args.Set("disable_latency_offset", "true")
	got, _ = answer(t, mux, "/select/logsql/query", args)
	if rate2, _ := strconv.ParseFloat(got[0]["r"], 64); rate2 != 25 {
		t.Errorf("opt-out rate() = %g, want 25", rate2)
	}
}
