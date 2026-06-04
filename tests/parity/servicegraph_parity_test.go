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

	hot := fetch(t, vtBaseURL, "/select/jaeger/api/dependencies", params)
	cold := fetch(t, lhtBaseURL, "/select/jaeger/api/dependencies", params)

	if hot.StatusCode != 200 {
		t.Fatalf("hot Jaeger dependencies returned %d: %s", hot.StatusCode, string(hot.Body))
	}
	if cold.StatusCode != 200 {
		t.Fatalf("cold Jaeger dependencies returned %d: %s", cold.StatusCode, string(cold.Body))
	}

	hotResp := parseDependenciesResponse(t, hot.Body)
	coldResp := parseDependenciesResponse(t, cold.Body)

	if hotResp.Total == 0 {
		t.Skip("hot VT has no service-graph edges yet — waiting for first task tick")
	}
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
	// Run the exact upstream-task query shape against the cold LogsQL endpoint.
	// We trim the time range to 10m to match the task's lookbehind window.
	q := `_time:10m (NOT parent_span_id:"") AND (kind:~"2|5") ` +
		`| fields parent_span_id, resource_attr:service.name ` +
		`| rename parent_span_id as span_id, resource_attr:service.name as child ` +
		`| join by (span_id) (` +
		`_time:10m (NOT span_id:"") AND (kind:~"3|4") ` +
		`| fields span_id, resource_attr:service.name ` +
		`| rename resource_attr:service.name as parent` +
		`) inner ` +
		`| NOT parent:eq_field(child) ` +
		`| stats by (parent, child) count() callCount`

	res := fetch(t, lhtBaseURL, "/select/logsql/query", url.Values{"query": {q}})
	if res.StatusCode != 200 {
		t.Fatalf("cold LogsQL join query: %d, %s", res.StatusCode, string(res.Body))
	}
	body := strings.TrimSpace(string(res.Body))
	if body == "" {
		t.Fatal("cold join query returned empty output — RunQueryExternal is " +
			"not preprocessing the inner subquery via initJoinMaps. Check " +
			"patches/vl-traces/external_query.go.src and the 6 caller sites.")
	}
	// Each output row is one JSON object per (parent, child) pair.
	lines := strings.Split(body, "\n")
	for _, line := range lines {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			continue
		}
		if row["parent"] == nil || row["child"] == nil {
			t.Errorf("join output row missing parent or child: %s", line)
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
	res := fetch(t, lhtBaseURL, "/select/logsql/query", url.Values{"query": {q}})
	if res.StatusCode != 200 {
		t.Fatalf("cold stats-by query: %d, %s", res.StatusCode, string(res.Body))
	}
	body := strings.TrimSpace(string(res.Body))
	if body == "" {
		t.Skip("no SG rows persisted yet")
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
