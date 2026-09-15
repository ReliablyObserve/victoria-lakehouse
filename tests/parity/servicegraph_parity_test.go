//go:build parity

package parity

// Service-graph parity coverage. Pins the cold-tier behaviors fixed in
// PR #121 so future refactors can't silently re-introduce any of these
// six bugs:
//
//   1. demo generator producing same-service traces only          (cmd/datagen/main.go)
//   2. VT-hot servicegraph task disabled by default               (compose flag)
//   3. LH cold servicegraph goroutine killed by stray defer       (lakehouse-traces/main.go)
//   4. RunQueryExternal skips join-pipe preprocessing             (patches/vl-*/external_query.go.src)
//   5. SG edge fields dropped at insert / read                    (schema + insert + traceRowToFields)
//   6. SG columns absent from registry → stats-by collapses       (schema/registry.go)
//
// Each test compares the LH cold tier's behavior against either hot VT
// (where applicable) or against an absolute correctness assertion the
// upstream task guarantees.

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestServiceGraphParity_DependenciesAPI asserts the canonical end-to-end
// invariant the entire fix chain enabled: cold-tier
// /select/jaeger/api/dependencies returns the same shape and a non-empty
// edge set, equivalent to what hot VT returns. This is the user-visible
// proof that bugs #2 through #6 are closed simultaneously — any one of
// them being re-introduced makes this test fail.
func TestServiceGraphParity_DependenciesAPI(t *testing.T) {
	params := url.Values{"lookback": {"1800000"}}

	hotResp := waitForServiceGraphEdges(t, vtBaseURL, params, serviceGraphFirstTickTimeout)
	if hotResp.Total == 0 {
		t.Fatalf("hot VT produced no service-graph edges within %s — the servicegraph task "+
			"is not running on victoriatraces (see TestServiceGraphParity_HotTaskMustBeEnabled)",
			serviceGraphFirstTickTimeout)
	}
	coldResp := waitForServiceGraphEdges(t, lhtBaseURL, params, serviceGraphFirstTickTimeout)
	if coldResp.Total == 0 {
		t.Fatalf("cold LH has zero service-graph edges but hot has %d — regression "+
			"in one of: (a) servicegraph goroutine staying alive (defer fix), "+
			"(b) RunQueryExternal handling joins (external_query.go.src patch), "+
			"(c) SG columns landing in TraceRow (insert.go), "+
			"(d) traceRowToFields exposing the columns (storage_query.go), "+
			"(e) registry knowing the columns (registry.go)", hotResp.Total)
	}

	// Both tiers must produce edges only between distinct services. The
	// `NOT parent:eq_field(child)` filter in the upstream task guarantees
	// this; if it stops working we'd see self-loops which are nonsense
	// in a dependency graph.
	for _, e := range coldResp.Data {
		if e.Parent == e.Child {
			t.Errorf("cold-tier self-loop edge %q→%q (callCount=%d) — generator "+
				"or join filter broke", e.Parent, e.Child, e.CallCount)
		}
		if e.Parent == "" || e.Child == "" || e.CallCount <= 0 {
			t.Errorf("cold-tier empty/zero edge: parent=%q child=%q callCount=%d "+
				"— the Jaeger handler filters these out, so seeing them means "+
				"the writer / registry / projection chain is leaking empty rows",
				e.Parent, e.Child, e.CallCount)
		}
	}

	// Topology overlap: every cold edge's services must also appear on
	// the hot side. Edge counts will differ (cold's task interval is
	// longer so it aggregates more spans per tick) but the *set* of
	// services involved must match the active traffic pattern.
	hotServices := edgeServiceSet(hotResp.Data)
	coldServices := edgeServiceSet(coldResp.Data)
	for s := range coldServices {
		if !hotServices[s] {
			t.Errorf("cold edge references service %q that hot doesn't — "+
				"check whether the demo generator's RPC chain is propagating "+
				"correctly across both tiers", s)
		}
	}

	t.Logf("hot=%d edges, cold=%d edges, services_overlap=%d/%d",
		hotResp.Total, coldResp.Total, len(coldServices), len(hotServices))
}

// TestServiceGraphParity_GeneratorProducesCrossServiceEdges pins bug #1.
// The original generator picked one random service per trace and gave
// every span the same service.name, so the upstream task's join always
// produced self-edges that got dropped by NOT parent:eq_field(child).
// Fix: the generator now builds an RPC call-chain. The hot tier reflects
// this directly — if any edge is self-referential, the generator is back
// to its old behavior.
func TestServiceGraphParity_GeneratorProducesCrossServiceEdges(t *testing.T) {
	hot := fetch(t, vtBaseURL, "/select/jaeger/api/dependencies", url.Values{"lookback": {"1800000"}})
	if hot.StatusCode != 200 {
		t.Fatalf("hot Jaeger dependencies: %d", hot.StatusCode)
	}
	resp := parseDependenciesResponse(t, hot.Body)
	if resp.Total == 0 {
		t.Skip("no edges yet")
	}
	for _, e := range resp.Data {
		if e.Parent == e.Child {
			t.Errorf("generator regression: self-edge %s→%s (callCount=%d). "+
				"cmd/datagen/main.go must build a chain of DISTINCT services per trace.",
				e.Parent, e.Child, e.CallCount)
		}
	}
}

// TestServiceGraphParity_HotTaskMustBeEnabled pins bug #2. If a future
// compose change disables -servicegraph.enableTask=true on victoriatraces,
// hot won't generate edges and the parity baseline collapses.
func TestServiceGraphParity_HotTaskMustBeEnabled(t *testing.T) {
	// Wait up to one hot tick interval (1m + buffer) for the metric to update.
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		body := fetch(t, vtBaseURL, "/metrics", nil).Body
		if strings.Contains(string(body), `flag{name="servicegraph.enableTask", value="true", is_set="true"}`) {
			return
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("hot VT does not have -servicegraph.enableTask=true set. " +
		"docker-compose-e2e.yml's victoriatraces service must include this flag.")
}

// TestServiceGraphParity_ColdTaskGoroutineLives pins bug #3. The fix
// removed a stray defer servicegraph.Stop() from newMux() that fired
// immediately. If a future refactor re-adds that defer, the task
// goroutine dies in microseconds and we never see any duration buckets.
func TestServiceGraphParity_ColdTaskGoroutineLives(t *testing.T) {
	deadline := time.Now().Add(7 * time.Minute) // cold ticks every 5m + buffer
	for time.Now().Before(deadline) {
		body := string(fetch(t, lhtBaseURL, "/metrics", nil).Body)
		if strings.Contains(body, "vt_servicegraph_task_duration_seconds_count") {
			// Histogram only registers after the goroutine has actually
			// run the function body at least once — defer-killed goroutine
			// would skip this metric entirely.
			return
		}
		time.Sleep(15 * time.Second)
	}
	t.Fatal("cold LH never registered vt_servicegraph_task_duration_seconds_count — " +
		"the task goroutine likely died before its first tick. Check for a stray " +
		"defer servicegraph.Stop() in lakehouse-traces/main.go.")
}

// TestServiceGraphParity_JoinPipeWorksOnCold pins bug #4. The upstream
// task issues a JOIN-bearing LogsQL query. RunQueryExternal must
// preprocess the inner subquery via initJoinMaps, otherwise the result
// is silently empty even though both sides exist independently.
func TestServiceGraphParity_JoinPipeWorksOnCold(t *testing.T) {
	// Run the exact upstream-task query shape against the cold LogsQL
	// endpoint. Two things this query has to get right or it tests nothing:
	//
	//   - The window must cover the seeded data. `_time:10m` is empty by
	//     the time the suite runs (datagen backfills 1-24h ago), so both
	//     the outer query and the join subquery returned zero rows and the
	//     "join is broken" failure was really a "window is empty" failure.
	//     The subquery cannot inherit the request's start/end, so the
	//     filter is inlined on both sides.
	//   - `resource_attr:service.name` contains a ':' and must be
	//     backtick-quoted, otherwise LogsQL parses it as field
	//     `resource_attr` with a bucket and the projection is empty.
	window := seedWindowFilter()
	q := window + ` (NOT parent_span_id:"") AND (kind:~"2|5") ` +
		"| fields parent_span_id, `resource_attr:service.name` " +
		"| rename parent_span_id as span_id, `resource_attr:service.name` as child " +
		`| join by (span_id) (` +
		window + ` (NOT span_id:"") AND (kind:~"3|4") ` +
		"| fields span_id, `resource_attr:service.name` " +
		"| rename `resource_attr:service.name` as parent" +
		`) inner ` +
		// `filter` is not optional: upstream builds this step as
		// "| filter NOT <parent>:eq_field(<child>)"
		// (VictoriaTraces app/vtselect/traces/query/query.go), and LogsQL has
		// no bare `NOT` pipe — without it BOTH tiers answer 400, so the case
		// failed on a malformed query rather than on a cold-tier divergence.
		`| filter NOT parent:eq_field(child) ` +
		`| stats by (parent, child) count() callCount`

	// Both tiers answer the SAME query. Checking only that cold returns
	// well-shaped rows would pass on a join that silently dropped or
	// duplicated edges; hot VictoriaTraces is the reference for what this
	// query means, so the edge sets must be identical, call counts included.
	hot := serviceGraphEdges(t, vtBaseURL, q)
	if len(hot) == 0 {
		t.Fatal("the hot tier produced no service-graph edges — seed or query " +
			"defect, not parity: an empty reference makes the comparison below " +
			"vacuous. Check that datagen seeded parent/child spans in the window.")
	}
	cold := serviceGraphEdges(t, lhtBaseURL, q)
	if len(cold) == 0 {
		t.Fatal("cold join query returned empty output — RunQueryExternal is " +
			"not preprocessing the inner subquery via initJoinMaps. Check " +
			"patches/vl-traces/external_query.go.src and the 6 caller sites.")
	}

	for edge, hotCount := range hot {
		coldCount, ok := cold[edge]
		if !ok {
			t.Errorf("cold lost the service-graph edge %s that hot reports with callCount=%s", edge, hotCount)
			continue
		}
		if coldCount != hotCount {
			t.Errorf("service-graph edge %s: cold callCount=%s, hot callCount=%s", edge, coldCount, hotCount)
		}
	}
	for edge, coldCount := range cold {
		if _, ok := hot[edge]; !ok {
			t.Errorf("cold invented the service-graph edge %s (callCount=%s) that hot does not report", edge, coldCount)
		}
	}
	t.Logf("service-graph edges compared: %d (hot) vs %d (cold)", len(hot), len(cold))
}

// serviceGraphEdges runs the upstream service-graph task query against one
// tier and returns "parent->child" => callCount. Every output row is one JSON
// object per (parent, child) pair; a row missing either key is a malformed
// answer, not an edge, and fails here rather than being skipped into an
// accidentally-equal set.
func serviceGraphEdges(t *testing.T, baseURL, query string) map[string]string {
	t.Helper()
	res := fetch(t, baseURL, "/select/logsql/query", url.Values{"query": {query}})
	if res.StatusCode != 200 {
		t.Fatalf("%s LogsQL join query: %d, %s", baseURL, res.StatusCode, string(res.Body))
	}
	edges := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(res.Body)), "\n") {
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("%s: join output line is not JSON: %s", baseURL, line)
		}
		parent, pok := rowValue(row, "parent")
		child, cok := rowValue(row, "child")
		if !pok || !cok {
			t.Errorf("%s: join output row missing parent or child: %s", baseURL, line)
			continue
		}
		count, _ := rowValue(row, "callCount")
		edges[parent+"->"+child] = count
	}
	return edges
}

// TestServiceGraphParity_TraceQLSearch pins the related TraceQL gap that
// was incidentally fixed by the SG cascade (specifically the registry
// registration of cold-tier columns). The Tempo /api/search endpoint
// issues a two-phase query: phase 1 finds trace IDs via the upstream
// task's LogsQL shape (works through the join-aware adapter); phase 2
// fetches spans with `trace_id:in(...)`. The handler's writeBlock
// callback aborts on `db.GetTimestamps(nil)` returning false, so cold
// previously returned 0 traces even though spans existed. Parity now
// matches hot — this test catches a regression in either layer.
func TestServiceGraphParity_TraceQLSearch(t *testing.T) {
	now := time.Now().Unix()
	start := now - 600
	for _, q := range []string{`{}`, `{.service.name="api-gateway"}`} {
		ref := fetch(t, vtBaseURL, "/select/tempo/api/search", url.Values{
			"q":     {q},
			"limit": {"5"},
			"start": {fmt.Sprint(start)},
			"end":   {fmt.Sprint(now)},
		})
		sut := fetch(t, lhtBaseURL, "/select/tempo/api/search", url.Values{
			"q":     {q},
			"limit": {"5"},
			"start": {fmt.Sprint(start)},
			"end":   {fmt.Sprint(now)},
		})
		var refResp, sutResp struct {
			Traces []map[string]any `json:"traces"`
		}
		_ = json.Unmarshal(ref.Body, &refResp)
		_ = json.Unmarshal(sut.Body, &sutResp)
		if len(refResp.Traces) > 0 && len(sutResp.Traces) == 0 {
			t.Errorf("TraceQL parity gap for q=%s: hot=%d cold=0. "+
				"Cold-tier reader DataBlock missing _time as RFC3339Nano string column? "+
				"Check storage_query.go traceRowToFields and registry _time formatting.",
				q, len(refResp.Traces))
		}
	}
}

// TestServiceGraphParity_StatsByOnSGFields pins bug #6. Even after the
// SG fields land in Parquet and the read path exposes them, the column
// registry must list them so projection.go adds them to the DataBlock
// reaching `stats by`. Without that, every row's group key is empty and
// the whole stream collapses into one (empty, empty) bucket with
// callCount=NaN.
func TestServiceGraphParity_StatsByOnSGFields(t *testing.T) {
	q := `{trace_service_graph_stream="-"} NOT parent:"" ` +
		`| fields parent, child, callCount ` +
		`| stats by (parent, child) sum(callCount) as callCount`
	var body string
	deadline := time.Now().Add(serviceGraphFirstTickTimeout)
	for {
		res := fetch(t, lhtBaseURL, "/select/logsql/query", url.Values{"query": {q}})
		if res.StatusCode != 200 {
			t.Fatalf("cold stats-by query: %d, %s", res.StatusCode, string(res.Body))
		}
		body = strings.TrimSpace(string(res.Body))
		if body != "" || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Second)
	}
	if body == "" {
		t.Fatalf("cold persisted no service-graph rows within %s — the servicegraph task "+
			"never wrote a snapshot, so the stats-by path under test was never exercised",
			serviceGraphFirstTickTimeout)
	}

	collapsed := 0
	distinct := 0
	for _, line := range strings.Split(body, "\n") {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			continue
		}
		parent, _ := row["parent"].(string)
		child, _ := row["child"].(string)
		callCount, _ := row["callCount"].(string)
		if parent == "" && child == "" {
			collapsed++
			if callCount == "NaN" {
				t.Errorf("stats-by collapsed all rows into empty (parent, child) " +
					"bucket with callCount=NaN — registry doesn't know about " +
					"parent/child columns, so projection.go dropped them. " +
					"Add the SG fields to TracesProfile.Promoted in registry.go.")
			}
			continue
		}
		distinct++
	}
	if distinct == 0 {
		t.Errorf("zero (parent, child) groups produced — projection or registry " +
			"is dropping the columns before stats-by sees them")
	}
}

type depEdge struct {
	Parent    string `json:"parent"`
	Child     string `json:"child"`
	CallCount int    `json:"callCount"`
}

type depResp struct {
	Data  []depEdge `json:"data"`
	Total int       `json:"total"`
}

// serviceGraphFirstTickTimeout bounds the wait for a tier's first
// service-graph snapshot. The task first runs one interval after the binary
// starts (1m on cold, tests/parity/docker-compose.yml) and its rows are
// visible only once the writer has flushed them. A suite that reaches the
// service-graph tests within a minute of the stack starting would otherwise
// read the empty graph from before that tick and fail or skip on timing
// alone.
const serviceGraphFirstTickTimeout = 3 * time.Minute

// waitForServiceGraphEdges polls a tier's Jaeger dependencies endpoint until
// it reports at least one edge or the timeout passes, and returns the last
// answer either way — the caller decides what an empty graph means.
func waitForServiceGraphEdges(t *testing.T, base string, params url.Values, timeout time.Duration) depResp {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		r := fetch(t, base, "/select/jaeger/api/dependencies", params)
		if r.StatusCode != 200 {
			t.Fatalf("%s Jaeger dependencies returned %d: %s", base, r.StatusCode, string(r.Body))
		}
		resp := parseDependenciesResponse(t, r.Body)
		if resp.Total > 0 || time.Now().After(deadline) {
			return resp
		}
		time.Sleep(10 * time.Second)
	}
}

func parseDependenciesResponse(t *testing.T, body []byte) depResp {
	t.Helper()
	var r depResp
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("parse Jaeger dependencies: %v\nbody=%s", err, string(body))
	}
	return r
}

func edgeServiceSet(edges []depEdge) map[string]bool {
	out := make(map[string]bool, len(edges)*2)
	for _, e := range edges {
		out[e.Parent] = true
		out[e.Child] = true
	}
	return out
}

// fail-fast format helper — tests above use t.Errorf for soft failures
// where one row being wrong shouldn't drown other diagnostics.
var _ = fmt.Sprintf
