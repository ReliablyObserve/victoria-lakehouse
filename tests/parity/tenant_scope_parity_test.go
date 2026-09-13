//go:build parity

package parity

// Tenant-scoped behavior of the traces tier beyond the row, hits and
// field-value reads TestTenantIsolation_Traces_PerTenantParity pins:
//
//   - the manifest's per-tenant file and byte totals add up to the bucket
//     overview (TestTenantParity_TenantCountsSumToTotal);
//   - a tenant the stack never saw reads as empty on both tiers instead of
//     falling through to another tenant's data
//     (TestTenantParity_UnknownTenantReturnsEmpty);
//   - the Jaeger dependencies graph and the Tempo trace search answer each
//     seeded tenant with exactly what hot VictoriaTraces answers that tenant
//     with (TestTenantParity_DependenciesAPI_RespectsScope,
//     TestTenantParity_TraceQLPerTenant).
//
// Every assertion is an exact expectation against a non-empty reference. The
// two seeded tenants share service names, so the only thing that separates a
// scoped answer from one that leaks the other tenant's data is a count or an
// id set — never a status code, a self-consistency check or a `>=`.

import (
	"encoding/json"
	"maps"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// TestTenantParity_TenantCountsSumToTotal pins the manifest accounting: the
// per-tenant file and byte totals must add up to the bucket overview.
func TestTenantParity_TenantCountsSumToTotal(t *testing.T) {
	tenants := requireSeededTenants(t)

	var sumFiles, sumBytes int64
	for _, te := range tenants {
		sumFiles += te.TotalFiles
		sumBytes += te.TotalBytes
	}
	if sumFiles == 0 {
		t.Fatalf("the manifest lists %d tenants and no files — flush defect, not an "+
			"accounting result", len(tenants))
	}

	overview := fetchOverview(t, lhtBaseURL)
	if overview.TotalFiles != sumFiles {
		t.Errorf("file count drift: sum(per-tenant)=%d, overview.total_files=%d",
			sumFiles, overview.TotalFiles)
	}
	if overview.TotalBytes != sumBytes {
		t.Errorf("byte count drift: sum(per-tenant)=%d, overview.total_bytes=%d",
			sumBytes, overview.TotalBytes)
	}
}

// TestTenantParity_UnknownTenantReturnsEmpty pins that a read for a tenant
// with no data answers 0 rather than falling through to a global scan, which
// would let a made-up header read another tenant's spans. The same read for
// the seeded default tenant is the control: without it, a 0 would only prove
// that the window is empty.
func TestTenantParity_UnknownTenantReturnsEmpty(t *testing.T) {
	const query = "span_id:* | stats count() rows"
	for _, tier := range []struct{ name, base string }{
		{"VT", vtBaseURL},
		{"LHT", lhtBaseURL},
	} {
		t.Run(tier.name, func(t *testing.T) {
			seeded := tenantStatsCount(t, tier.base, query, "0", "0")
			if seeded <= 0 {
				t.Fatalf("control read for the seeded tenant 0:0 returned %d spans — seed "+
					"or query defect: a zero for the unknown tenant would prove nothing", seeded)
			}
			if got := tenantStatsCount(t, tier.base, query, "99999", "99999"); got != 0 {
				t.Errorf("tenant 99999:99999 read %d spans, want 0 — a tenant with no data "+
					"must not fall through to another tenant's (tenant 0:0 holds %d)", got, seeded)
			}
		})
	}
}

// TestTenantParity_DependenciesAPI_RespectsScope asserts that the Jaeger
// dependencies graph each tier serves a tenant carries exactly the call
// counts hot VictoriaTraces derives from that tenant's spans.
//
// Each service-graph task tick persists one snapshot covering its whole
// lookbehind, stamped with the tick's window end truncated to the task
// interval: 30s on hot and 1m on cold (tests/parity/docker-compose.yml). A
// whole minute is therefore a stamp both tiers write, and a snapshot stamped
// after this test started was computed after the stack settled, over the
// complete corpus. The request is pinned to that one snapshot; reading a
// longer lookback would sum every snapshot in it and scale each count by how
// many ticks the tier happened to run.
func TestTenantParity_DependenciesAPI_RespectsScope(t *testing.T) {
	tenants := requireSeededTenants(t)

	notBefore := time.Now()
	deadline := notBefore.Add(4 * time.Minute)
	var tick time.Time
	var graphs map[string]map[string]map[depEdgeKey]int
	for {
		tick, graphs = latestCommonServiceGraphTick(t, tenants, notBefore)
		if graphs != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no service-graph snapshot stamped after %s reached both tiers for every "+
				"tenant within 4m — the servicegraph task is not producing per-tenant edges "+
				"on one tier", notBefore.UTC().Format(time.RFC3339))
		}
		time.Sleep(10 * time.Second)
	}

	first, second := tenants[0].AccountID, tenants[1].AccountID
	if maps.Equal(graphs["VT"][first], graphs["VT"][second]) {
		t.Fatalf("hot answers tenants %s and %s with the same graph — the seed cannot tell "+
			"a scoped graph from a leaking one", first, second)
	}

	for _, te := range tenants {
		t.Run("account_"+te.AccountID, func(t *testing.T) {
			hot := graphs["VT"][te.AccountID]
			cold := graphs["LHT"][te.AccountID]
			for edge, want := range hot {
				if got, ok := cold[edge]; !ok {
					t.Errorf("edge %s -> %s: missing on cold, hot callCount %d", edge.parent, edge.child, want)
				} else if got != want {
					t.Errorf("edge %s -> %s: cold callCount %d, hot %d — a graph scoped to "+
						"one tenant must count that tenant's calls only (see B6 in "+
						"docs/parity-and-gaps.md)", edge.parent, edge.child, got, want)
				}
			}
			for edge, got := range cold {
				if _, ok := hot[edge]; !ok {
					t.Errorf("edge %s -> %s (callCount %d) is on cold only", edge.parent, edge.child, got)
				}
			}
			t.Logf("snapshot %s, account %s: hot %d edges / %d calls, cold %d edges / %d calls",
				tick.UTC().Format(time.RFC3339), te.AccountID, len(hot), sumCalls(hot), len(cold), sumCalls(cold))
		})
	}
}

// TestTenantParity_TraceQLPerTenant asserts that a Tempo search scoped to a
// tenant returns exactly the traces hot VictoriaTraces returns for it. Trace
// ids are random per seed, so a search that leaks the other tenant's traces
// shows up as extra ids rather than hiding inside a count.
func TestTenantParity_TraceQLPerTenant(t *testing.T) {
	tenants := requireSeededTenants(t)

	// Three hours from the middle of the seeded day: tens of traces per
	// tenant, far below the search limit, so each answer is the complete set
	// rather than whichever traces a limit happened to keep.
	mid := seedWindowMidpoint()
	start, end := mid.Add(-90*time.Minute), mid.Add(90*time.Minute)

	hot := make(map[string][]string, len(tenants))
	owner := map[string]string{}
	for _, te := range tenants {
		ids := tempoTraceIDs(t, vtBaseURL, start, end, te.AccountID, te.ProjectID)
		if len(ids) == 0 {
			t.Fatalf("hot reference for account %s returned no traces between %s and %s — "+
				"seed or query defect, not parity", te.AccountID, start.UTC().Format(time.RFC3339),
				end.UTC().Format(time.RFC3339))
		}
		for _, id := range ids {
			if other, seen := owner[id]; seen {
				t.Fatalf("hot returns trace %s for both account %s and account %s — the seed "+
					"cannot tell a scoped search from a leaking one", id, other, te.AccountID)
			}
			owner[id] = te.AccountID
		}
		hot[te.AccountID] = ids
	}

	for _, te := range tenants {
		t.Run("account_"+te.AccountID, func(t *testing.T) {
			want := hot[te.AccountID]
			got := tempoTraceIDs(t, lhtBaseURL, start, end, te.AccountID, te.ProjectID)
			wantSet, gotSet := stringSet(want), stringSet(got)
			for _, id := range want {
				if !gotSet[id] {
					t.Errorf("trace %s is missing on cold", id)
				}
			}
			for _, id := range got {
				if !wantSet[id] {
					if other, ok := owner[id]; ok {
						t.Errorf("trace %s belongs to account %s — a search scoped to account "+
							"%s must not return it (see B6 in docs/parity-and-gaps.md)", id, other, te.AccountID)
					} else {
						t.Errorf("trace %s is on cold only", id)
					}
				}
			}
			t.Logf("account %s: hot %d traces, cold %d", te.AccountID, len(want), len(got))
		})
	}
}

// --- helpers -------------------------------------------------------------

type tenantSummary struct {
	AccountID  string `json:"account_id"`
	ProjectID  string `json:"project_id"`
	TotalFiles int64  `json:"total_files"`
	TotalBytes int64  `json:"total_bytes"`
	TotalRows  int64  `json:"total_rows"`
}

// requireSeededTenants returns the tenants the cold traces manifest lists and
// fails unless it lists at least the two the parity compose seeds.
func requireSeededTenants(t *testing.T) []tenantSummary {
	t.Helper()
	r := fetch(t, lhtBaseURL, "/lakehouse/api/v1/tenants", nil)
	if r.StatusCode != 200 {
		t.Fatalf("GET /lakehouse/api/v1/tenants returned %d: %s", r.StatusCode, string(r.Body))
	}
	var d struct {
		Tenants []tenantSummary `json:"tenants"`
	}
	if err := json.Unmarshal(r.Body, &d); err != nil {
		t.Fatalf("parse /lakehouse/api/v1/tenants: %v", err)
	}
	if len(d.Tenants) < 2 {
		t.Fatalf("need the two seeded tenants, the manifest lists %d — check the "+
			"datagen-seed-tenant2 service in tests/parity/docker-compose.yml", len(d.Tenants))
	}
	return d.Tenants
}

type overviewResp struct {
	TotalFiles  int64 `json:"total_files"`
	TotalBytes  int64 `json:"total_bytes"`
	TenantCount int   `json:"tenant_count"`
}

func fetchOverview(t *testing.T, base string) overviewResp {
	t.Helper()
	r := fetch(t, base, "/lakehouse/api/v1/stats/overview", nil)
	if r.StatusCode != 200 {
		t.Fatalf("GET /lakehouse/api/v1/stats/overview returned %d: %s", r.StatusCode, string(r.Body))
	}
	var o overviewResp
	if err := json.Unmarshal(r.Body, &o); err != nil {
		t.Fatalf("parse /lakehouse/api/v1/stats/overview: %v", err)
	}
	return o
}

// tenantStatsCount runs a single-value stats query over the seeded window with
// tenant headers. Every failure is fatal: a reader that turns a failed request
// into 0 cannot tell "no data" from "request failed", and 0 is exactly what
// a tenant-scope assertion is looking for.
func tenantStatsCount(t *testing.T, base, query, account, project string) int {
	t.Helper()
	params := seedWindowParams()
	params.Set("query", query)
	r := tenantFetch(t, base, "/select/logsql/stats_query", params, account, project)
	if r.StatusCode != 200 {
		t.Fatalf("%s stats_query for tenant %s:%s returned %d: %s",
			base, account, project, r.StatusCode, string(r.Body))
	}
	v, err := extractVectorCount(r.Body)
	if err != nil {
		t.Fatalf("%s stats_query for tenant %s:%s: %v", base, account, project, err)
	}
	return int(v)
}

type depEdgeKey struct{ parent, child string }

// dependenciesAtTick reads a tenant's Jaeger dependencies graph pinned to the
// snapshot stamped at tick: a 1 ms lookback ending on the stamp admits that
// snapshot and no earlier one.
func dependenciesAtTick(t *testing.T, base string, tick time.Time, account, project string) map[depEdgeKey]int {
	t.Helper()
	params := url.Values{
		"endTs":    {strconv.FormatInt(tick.UnixMilli(), 10)},
		"lookback": {"1"},
	}
	r := tenantFetch(t, base, "/select/jaeger/api/dependencies", params, account, project)
	if r.StatusCode != 200 {
		t.Fatalf("%s dependencies for tenant %s:%s returned %d: %s",
			base, account, project, r.StatusCode, string(r.Body))
	}
	edges := map[depEdgeKey]int{}
	for _, e := range parseDependenciesResponse(t, r.Body).Data {
		edges[depEdgeKey{e.Parent, e.Child}] += e.CallCount
	}
	return edges
}

// latestCommonServiceGraphTick walks back from the current minute to
// notBefore and returns the newest whole-minute snapshot that both tiers
// hold for every tenant, with the graphs read at it keyed by tier name and
// account. It returns nil graphs while no such snapshot exists yet.
func latestCommonServiceGraphTick(t *testing.T, tenants []tenantSummary, notBefore time.Time) (time.Time, map[string]map[string]map[depEdgeKey]int) {
	t.Helper()
	tiers := []struct{ name, base string }{{"VT", vtBaseURL}, {"LHT", lhtBaseURL}}
	for tick := time.Now().Truncate(time.Minute); !tick.Before(notBefore); tick = tick.Add(-time.Minute) {
		graphs := map[string]map[string]map[depEdgeKey]int{}
		complete := true
		for _, tier := range tiers {
			graphs[tier.name] = map[string]map[depEdgeKey]int{}
			for _, te := range tenants {
				edges := dependenciesAtTick(t, tier.base, tick, te.AccountID, te.ProjectID)
				if len(edges) == 0 {
					complete = false
					break
				}
				graphs[tier.name][te.AccountID] = edges
			}
			if !complete {
				break
			}
		}
		if complete {
			return tick, graphs
		}
	}
	return time.Time{}, nil
}

func sumCalls(edges map[depEdgeKey]int) int {
	total := 0
	for _, n := range edges {
		total += n
	}
	return total
}

// tempoSearchLimit is the largest `limit` VictoriaTraces accepts on
// /select/tempo/api/search; larger values are clamped to it.
const tempoSearchLimit = 1000

// tempoTraceIDs returns the sorted trace ids a tenant-scoped Tempo search for
// every trace (`{}`) answers with between start and end. An answer that fills
// the limit may be truncated, so it fails instead of being compared.
func tempoTraceIDs(t *testing.T, base string, start, end time.Time, account, project string) []string {
	t.Helper()
	params := url.Values{
		"q":     {"{}"},
		"limit": {strconv.Itoa(tempoSearchLimit)},
		"start": {strconv.FormatInt(start.Unix(), 10)},
		"end":   {strconv.FormatInt(end.Unix(), 10)},
	}
	r := tenantFetch(t, base, "/select/tempo/api/search", params, account, project)
	if r.StatusCode != 200 {
		t.Fatalf("%s tempo search for tenant %s:%s returned %d: %s",
			base, account, project, r.StatusCode, string(r.Body))
	}
	var resp struct {
		Traces []struct {
			TraceID string `json:"traceID"`
		} `json:"traces"`
	}
	if err := json.Unmarshal(r.Body, &resp); err != nil {
		t.Fatalf("parse %s tempo search for tenant %s:%s: %v", base, account, project, err)
	}
	if len(resp.Traces) >= tempoSearchLimit {
		t.Fatalf("%s tempo search for tenant %s:%s filled the %d-trace limit — the answer "+
			"may be truncated, so narrow the window", base, account, project, tempoSearchLimit)
	}
	ids := make([]string, 0, len(resp.Traces))
	for _, tr := range resp.Traces {
		ids = append(ids, tr.TraceID)
	}
	return sortedStrings(ids)
}
