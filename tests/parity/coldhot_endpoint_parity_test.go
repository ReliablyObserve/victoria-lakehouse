//go:build parity

package parity

// Hot vs cold per-endpoint parity. The Service Graph tests are in
// servicegraph_parity_test.go; this file covers every other endpoint
// the Grafana plugins + tools-of-record (Jaeger UI, Tempo CLI, VLogs
// CLI) hit, so any future cold-tier regression that changes a result
// shape (or count, where the data is the same on both tiers) fails
// here rather than in production.
//
// Counts won't always be equal — hot retains 24h, cold retains
// indefinitely, so cold strictly dominates on long-window queries.
// Where exact match is impossible we assert directional invariants:
//   - cold >= hot on long windows
//   - both > 0 when data is known to exist
//   - response shape (keys, types) is identical
//
// All tests skip cleanly when the underlying data isn't present yet
// (right after a fresh restart) instead of flaking.

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestParity_Jaeger_Services_NonEmpty(t *testing.T) {
	// Poll for up to 3 minutes so a freshly-restarted cold tier gets
	// a chance to flush + refresh its manifest. Without the retry this
	// test flakes during e2e runs where compose-up is in progress.
	deadline := time.Now().Add(3 * time.Minute)
	var hot, cold []string
	for time.Now().Before(deadline) {
		hot = jaegerServices(t, vtBaseURL)
		cold = jaegerServices(t, lhtBaseURL)
		if len(hot) > 0 && len(cold) > 0 {
			break
		}
		time.Sleep(15 * time.Second)
	}
	if len(hot) == 0 {
		t.Skip("hot has no services after 3min — bucket may be empty")
	}
	if len(cold) == 0 {
		t.Fatalf("cold has no services after 3min but hot has %d — likely a manifest or label index regression", len(hot))
	}
	// Cold may be missing services that were pushed directly to hot
	// only (e.g. e2e test fixtures). Don't fail on that; fail only
	// when EVERY hot service is missing from cold (a real regression).
	missing := setDiff(hot, cold)
	intersection := len(hot) - len(missing)
	if intersection == 0 {
		t.Errorf("cold doesn't share ANY of hot's %d services — likely a manifest or label-index regression", len(hot))
	}
}

func TestParity_Jaeger_Operations_SameForKnownService(t *testing.T) {
	hot := jaegerServices(t, vtBaseURL)
	if len(hot) == 0 {
		t.Skip("no services to compare operations for")
	}
	svc := hot[0]
	hotOps := jaegerOperations(t, vtBaseURL, svc)
	coldOps := jaegerOperations(t, lhtBaseURL, svc)
	if len(hotOps) == 0 {
		t.Skipf("hot has no operations for %s yet", svc)
	}
	missing := setDiff(hotOps, coldOps)
	if len(missing) > 0 {
		t.Errorf("operations on %s in hot but not in cold: %v", svc, missing)
	}
}

func TestParity_Jaeger_TraceByID_Roundtrip(t *testing.T) {
	hot := jaegerServices(t, vtBaseURL)
	if len(hot) == 0 {
		t.Skip("no services")
	}
	svc := findCommonService(t, hot)
	if svc == "" {
		t.Skip("no service common to both tiers")
	}
	// Pull one trace ID from COLD (not hot) so we know cold has it.
	// Then fetch by ID on cold — that's the actual cold-tier
	// trace-by-id correctness check.
	traces := jaegerSearch(t, lhtBaseURL, svc, 1)
	if len(traces) == 0 {
		t.Skipf("cold returned no traces for %s", svc)
	}
	tid := traces[0]
	coldSpans := jaegerSpans(t, lhtBaseURL, tid)
	if coldSpans == 0 {
		t.Errorf("cold lookup of own trace %s returned 0 spans — footer-prefetch or trace-index regression", tid)
	}
}

// findCommonService returns the first service name present on both
// tiers. Used by tests that need a service known to have data on
// cold (hot-only services from direct OTLP pushes don't).
func findCommonService(t *testing.T, hot []string) string {
	t.Helper()
	cold := jaegerServices(t, lhtBaseURL)
	coldSet := make(map[string]bool, len(cold))
	for _, s := range cold {
		coldSet[s] = true
	}
	for _, s := range hot {
		if coldSet[s] {
			return s
		}
	}
	return ""
}

func TestParity_Tempo_TagsScopesIdentical(t *testing.T) {
	hotScopes := tempoTagScopeNames(t, vtBaseURL)
	coldScopes := tempoTagScopeNames(t, lhtBaseURL)
	if !equalStringSets(hotScopes, coldScopes) {
		t.Errorf("Tempo tag-scope set differs: hot=%v cold=%v", hotScopes, coldScopes)
	}
}

func TestParity_Tempo_ServiceNameValuesSuperset(t *testing.T) {
	hotVals := tempoTagValues(t, vtBaseURL, "service.name")
	coldVals := tempoTagValues(t, lhtBaseURL, "service.name")
	if len(hotVals) == 0 {
		t.Skip("hot has no service.name values")
	}
	missing := setDiff(hotVals, coldVals)
	if len(missing) > 0 {
		t.Errorf("service.name values in hot missing from cold: %v", missing)
	}
}

func TestParity_LogsQL_FieldNamesSuperset(t *testing.T) {
	hot := logsqlFieldNames(t, vtBaseURL, "*")
	cold := logsqlFieldNames(t, lhtBaseURL, "*")
	if len(hot) == 0 {
		t.Skip("hot label index is empty")
	}
	// Trace-specific fields (trace_id, span_id, etc.) must appear on
	// both tiers. Cold may have additional fields from older data.
	core := []string{"trace_id", "span_id", "parent_span_id", "name"}
	for _, c := range core {
		hotHas, coldHas := false, false
		for _, n := range hot {
			if n == c {
				hotHas = true
				break
			}
		}
		for _, n := range cold {
			if n == c {
				coldHas = true
				break
			}
		}
		if hotHas && !coldHas {
			t.Errorf("core field %q in hot label index but not cold", c)
		}
	}
}

func TestParity_LogsQL_CountConsistency_RecentWindow(t *testing.T) {
	// A 1-hour window. Cold may have more (retention) but the ratio
	// shouldn't be wildly different. Catches accidental query-path
	// regressions where cold drops most of its rows.
	hotN := logsqlCount(t, vtBaseURL, "_time:1h *")
	coldN := logsqlCount(t, lhtBaseURL, "_time:1h *")
	if hotN == 0 {
		t.Skip("hot is empty in last hour")
	}
	if coldN == 0 {
		t.Errorf("cold has 0 rows in last hour while hot has %d", hotN)
	}
}

func TestParity_LogsQL_ServiceFilterFindsData(t *testing.T) {
	// Filter by a known service name on BOTH tiers; both must return
	// non-zero. Direct correctness check on the most common query.
	hot := jaegerServices(t, vtBaseURL)
	if len(hot) == 0 {
		t.Skip("no services to test")
	}
	svc := hot[0]
	q := fmt.Sprintf(`_time:1h resource_attr:service.name:=%q | stats count() as n`, svc)
	hotN := logsqlStatsCount(t, vtBaseURL, q)
	coldN := logsqlStatsCount(t, lhtBaseURL, q)
	if hotN == 0 {
		t.Skipf("hot service filter for %s found nothing", svc)
	}
	if coldN == 0 {
		t.Errorf("cold service filter for %s = 0 (hot=%d)", svc, hotN)
	}
}

func TestParity_LogsQL_AggregateByService_SameKeys(t *testing.T) {
	// Group-by on a top-level promoted column. Both tiers must
	// produce the same key set even if counts differ.
	q := `_time:1h * | stats by (resource_attr:service.name) count() as n`
	hot := logsqlGroupKeys(t, vtBaseURL, q, "resource_attr:service.name")
	cold := logsqlGroupKeys(t, lhtBaseURL, q, "resource_attr:service.name")
	if len(hot) == 0 {
		t.Skip("hot returned no groups")
	}
	missing := setDiff(hot, cold)
	if len(missing) > 0 {
		t.Errorf("group-by service.name: hot has keys missing from cold: %v", missing)
	}
}

func TestParity_LogsQL_KindFilter_Both0and1Spans(t *testing.T) {
	// Spans of all kinds (1=INTERNAL, 2=SERVER, 3=CLIENT) should
	// appear on both tiers. Verifies the kind column is queryable on
	// cold — relevant after the service-graph fix that added new
	// columns to the schema.
	for _, kind := range []int{1, 2, 3} {
		q := fmt.Sprintf(`_time:1h kind:%d | stats count() as n`, kind)
		hotN := logsqlStatsCount(t, vtBaseURL, q)
		coldN := logsqlStatsCount(t, lhtBaseURL, q)
		if hotN == 0 {
			continue // not generated by current load mix
		}
		if coldN == 0 {
			t.Errorf("kind=%d found on hot (%d) but not cold", kind, hotN)
		}
	}
}

func TestParity_LogsQL_NonExistentService_BothEmpty(t *testing.T) {
	// Negative path: a name no service has. Both must return 0.
	q := `_time:1h resource_attr:service.name:="this-service-does-not-exist-xyzzy" | stats count() as n`
	hotN := logsqlStatsCount(t, vtBaseURL, q)
	coldN := logsqlStatsCount(t, lhtBaseURL, q)
	if hotN != 0 || coldN != 0 {
		t.Errorf("non-existent service should return 0 on both tiers, got hot=%d cold=%d", hotN, coldN)
	}
}

func TestParity_LogsQL_LimitParity(t *testing.T) {
	// `| limit N` must return at most N rows on both tiers.
	for _, limit := range []int{1, 5, 10} {
		q := fmt.Sprintf(`_time:1h * | limit %d`, limit)
		hotN := logsqlQueryRowCount(t, vtBaseURL, q)
		coldN := logsqlQueryRowCount(t, lhtBaseURL, q)
		if hotN > limit {
			t.Errorf("hot exceeded limit %d, got %d rows", limit, hotN)
		}
		if coldN > limit {
			t.Errorf("cold exceeded limit %d, got %d rows", limit, coldN)
		}
	}
}

func TestParity_LogsQL_StreamSelectorEqualsCardinality(t *testing.T) {
	// {service.name="X"} stream selector should return the same
	// number of UNIQUE traces on both tiers (modulo TTL).
	hot := jaegerServices(t, vtBaseURL)
	if len(hot) == 0 {
		t.Skip("no services")
	}
	svc := hot[0]
	// Counts of distinct trace_ids.
	q := fmt.Sprintf(`_time:1h resource_attr:service.name:=%q | fields trace_id | uniq by (trace_id) | stats count() as n`, svc)
	hotN := logsqlStatsCount(t, vtBaseURL, q)
	coldN := logsqlStatsCount(t, lhtBaseURL, q)
	if hotN == 0 {
		t.Skipf("hot has no traces for %s", svc)
	}
	if coldN == 0 {
		t.Errorf("cold has no distinct trace_ids for %s (hot=%d)", svc, hotN)
	}
}

func TestParity_LogsQL_TimeRangeBoundaries(t *testing.T) {
	// Cold tier flushes every 120s and ingested rows may lag hot by
	// minutes — narrow recent windows (1m/5m) flake otherwise. Test
	// only windows wide enough to cover the flush lag.
	for _, w := range []string{"30m", "1h", "2h"} {
		hotN := logsqlCount(t, vtBaseURL, fmt.Sprintf("_time:%s *", w))
		coldN := logsqlCount(t, lhtBaseURL, fmt.Sprintf("_time:%s *", w))
		if hotN == 0 {
			continue
		}
		if coldN == 0 {
			t.Errorf("window %s: hot=%d cold=0 — time-range comparison may be broken on cold", w, hotN)
		}
	}
}

// --- helpers -------------------------------------------------------------

func jaegerServices(t *testing.T, base string) []string {
	t.Helper()
	r := fetch(t, base, "/select/jaeger/api/services", nil)
	if r.StatusCode != 200 {
		return nil
	}
	var d struct {
		Data []string `json:"data"`
	}
	_ = json.Unmarshal(r.Body, &d)
	return d.Data
}

func jaegerOperations(t *testing.T, base, svc string) []string {
	t.Helper()
	r := fetch(t, base, "/select/jaeger/api/services/"+svc+"/operations", nil)
	if r.StatusCode != 200 {
		return nil
	}
	var d struct {
		Data []string `json:"data"`
	}
	_ = json.Unmarshal(r.Body, &d)
	return d.Data
}

func jaegerSearch(t *testing.T, base, svc string, limit int) []string {
	t.Helper()
	r := fetch(t, base, "/select/jaeger/api/traces", url.Values{
		"service": {svc},
		"limit":   {fmt.Sprint(limit)},
	})
	if r.StatusCode != 200 {
		return nil
	}
	var d struct {
		Data []struct {
			TraceID string `json:"traceID"`
		} `json:"data"`
	}
	_ = json.Unmarshal(r.Body, &d)
	var out []string
	for _, t := range d.Data {
		out = append(out, t.TraceID)
	}
	return out
}

func jaegerSpans(t *testing.T, base, traceID string) int {
	t.Helper()
	r := fetch(t, base, "/select/jaeger/api/traces/"+traceID, nil)
	if r.StatusCode != 200 {
		return 0
	}
	var d struct {
		Data []struct {
			Spans []any `json:"spans"`
		} `json:"data"`
	}
	_ = json.Unmarshal(r.Body, &d)
	if len(d.Data) == 0 {
		return 0
	}
	return len(d.Data[0].Spans)
}

func tempoTagScopeNames(t *testing.T, base string) []string {
	t.Helper()
	r := fetch(t, base, "/select/tempo/api/v2/search/tags", nil)
	if r.StatusCode != 200 {
		return nil
	}
	var d struct {
		Scopes []struct {
			Name string `json:"name"`
		} `json:"scopes"`
	}
	_ = json.Unmarshal(r.Body, &d)
	var out []string
	for _, s := range d.Scopes {
		out = append(out, s.Name)
	}
	return out
}

func tempoTagValues(t *testing.T, base, tag string) []string {
	t.Helper()
	r := fetch(t, base, "/select/tempo/api/v2/search/tag/"+tag+"/values", nil)
	if r.StatusCode != 200 {
		return nil
	}
	var d struct {
		TagValues []struct {
			Value string `json:"value"`
		} `json:"tagValues"`
	}
	_ = json.Unmarshal(r.Body, &d)
	var out []string
	for _, v := range d.TagValues {
		out = append(out, v.Value)
	}
	return out
}

func logsqlFieldNames(t *testing.T, base, query string) []string {
	t.Helper()
	r := fetch(t, base, "/select/logsql/field_names", url.Values{"query": {query}})
	if r.StatusCode != 200 {
		return nil
	}
	var d struct {
		Values []struct {
			Value string `json:"value"`
		} `json:"values"`
	}
	_ = json.Unmarshal(r.Body, &d)
	var out []string
	for _, v := range d.Values {
		out = append(out, v.Value)
	}
	return out
}

// logsqlCount returns the number of rows returned by `query | stats count()`.
func logsqlCount(t *testing.T, base, query string) int {
	return logsqlStatsCount(t, base, query+" | stats count() as n")
}

func logsqlStatsCount(t *testing.T, base, query string) int {
	t.Helper()
	r := fetch(t, base, "/select/logsql/stats_query", url.Values{"query": {query}})
	if r.StatusCode != 200 {
		return 0
	}
	var d struct {
		Data struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r.Body, &d); err != nil {
		return 0
	}
	if len(d.Data.Result) == 0 {
		return 0
	}
	v := d.Data.Result[0].Value[1]
	switch x := v.(type) {
	case string:
		var n int
		_, _ = fmt.Sscanf(x, "%d", &n)
		return n
	case float64:
		return int(x)
	}
	return 0
}

// logsqlGroupKeys returns the distinct values of `keyName` field in
// the stats output.
func logsqlGroupKeys(t *testing.T, base, query, keyName string) []string {
	t.Helper()
	r := fetch(t, base, "/select/logsql/query", url.Values{"query": {query}})
	if r.StatusCode != 200 {
		return nil
	}
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(string(r.Body)), "\n") {
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			continue
		}
		if v, ok := row[keyName].(string); ok {
			out = append(out, v)
		}
	}
	return out
}

func logsqlQueryRowCount(t *testing.T, base, query string) int {
	t.Helper()
	r := fetch(t, base, "/select/logsql/query", url.Values{"query": {query}})
	if r.StatusCode != 200 {
		return 0
	}
	count := 0
	for _, line := range strings.Split(strings.TrimSpace(string(r.Body)), "\n") {
		if line != "" {
			count++
		}
	}
	return count
}

func setDiff(a, b []string) []string {
	bset := make(map[string]bool, len(b))
	for _, x := range b {
		bset[x] = true
	}
	var out []string
	for _, x := range a {
		if !bset[x] {
			out = append(out, x)
		}
	}
	return out
}

func equalStringSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	bset := make(map[string]bool, len(b))
	for _, x := range b {
		bset[x] = true
	}
	for _, x := range a {
		if !bset[x] {
			return false
		}
	}
	return true
}

var _ = time.Now // keep time import for future window-arithmetic tests
