//go:build parity

package parity

// LogsQL latency-offset parity (VictoriaTraces v0.12.0).
//
// VictoriaTraces >= v0.12.0 applies -search.latencyOffset (default 30s) to the
// LogsQL query APIs except live tailing, so a span younger than the offset is
// hidden from /select/logsql/* unless the request says
// disable_latency_offset=true. Before v0.12.0 only the Jaeger and Tempo APIs
// had it. The traces binary serves LogsQL through VictoriaTraces' own handlers,
// so hot VT and the cold tier must answer the same for a span younger than the
// offset, with and without the opt-out. The rest of the suite opts out
// (withoutLatencyOffset) because it means "compare everything the seed wrote";
// these cases set the argument explicitly so the helper leaves them alone.
//
// The case writes two spans, so it writes them into a tenant of its own
// (latencyProbeAccount): the other tests read tenant 0:0 and the seeded
// tenants, never this one, and requireSeededTenants leaves it out of the list
// they iterate. That is what lets the file sort and run anywhere (the suite is
// run with -shuffle=on). The case ends only once the cold tier has flushed the
// probe tenant's spans, so no manifest change of its making lands in another
// test.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"
)

// latencyProbeAccount is the tenant (AccountID, with ProjectID 0) the
// latency-offset case owns. No other writer in the stack uses it.
const (
	latencyProbeAccount = "7301"
	latencyProbeProject = "0"
)

// pushOTLPSpan writes one span, ending at endAt, to a tier's OTLP endpoint for
// the probe tenant.
func pushOTLPSpan(t *testing.T, base, traceID, spanID, service string, endAt time.Time) {
	t.Helper()
	h := http.Header{}
	h.Set("AccountID", latencyProbeAccount)
	h.Set("ProjectID", latencyProbeProject)
	pushSpanAs(t, base, h, traceID, spanID, service, endAt)
}

// probeTraceIDs returns the sorted trace_ids the tier's LogsQL query API
// returns for the probe tenant's two traces.
func probeTraceIDs(t *testing.T, base string, ids []string, disable string) []string {
	t.Helper()
	params := url.Values{
		"query":                  {"trace_id:in(" + strings.Join(ids, ",") + ")"},
		"start":                  {"-1h"},
		"disable_latency_offset": {disable},
	}
	r := tenantFetch(t, base, "/select/logsql/query", params, latencyProbeAccount, latencyProbeProject)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("%s logsql/query returned %d: %s", base, r.StatusCode, string(r.Body))
	}
	seen := map[string]bool{}
	for _, row := range parseNDJSON(r.Body) {
		if id, _ := row["trace_id"].(string); id != "" {
			seen[id] = true
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func TestParity_Traces_LogsQLLatencyOffset(t *testing.T) {
	stamp := time.Now().UnixNano()
	oldID := fmt.Sprintf("%016x%016x", stamp, 1)
	youngID := fmt.Sprintf("%016x%016x", stamp, 2)
	ids := []string{oldID, youngID}
	sort.Strings(ids)

	youngAt := time.Now()
	for _, base := range []string{vtBaseURL, lhtBaseURL} {
		pushOTLPSpan(t, base, oldID, "0000000000000001", "latency-offset-probe", youngAt.Add(-3*time.Minute))
		pushOTLPSpan(t, base, youngID, "0000000000000002", "latency-offset-probe", youngAt)
	}

	// Both tiers must first serve both spans once the offset is disabled: the
	// hot tier flushes its in-memory parts every few seconds, the cold tier
	// serves the unflushed span from its buffer.
	want := strings.Join(ids, ",")
	deadline := time.Now().Add(25 * time.Second)
	for {
		hot := probeTraceIDs(t, vtBaseURL, ids, "true")
		cold := probeTraceIDs(t, lhtBaseURL, ids, "true")
		if strings.Join(hot, ",") == want && strings.Join(cold, ",") == want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("with disable_latency_offset=true both tiers must return both probe spans: hot=%v cold=%v want=%v", hot, cold, ids)
		}
		time.Sleep(time.Second)
	}

	// Default (the argument set to its default, false): a span younger than
	// the offset is hidden on both tiers, the older one is not.
	hot := probeTraceIDs(t, vtBaseURL, ids, "false")
	cold := probeTraceIDs(t, lhtBaseURL, ids, "false")
	age := time.Since(youngAt)
	if age > 25*time.Second {
		// The probe span is about to cross the offset: its visibility is a
		// race on both tiers, so only the older span can be asserted.
		t.Logf("probe span is %s old, too close to the 30s offset to assert on it", age.Round(time.Second))
		for name, got := range map[string][]string{"hot": hot, "cold": cold} {
			if !containsString(got, oldID) {
				t.Errorf("%s tier lost the span older than the offset: %v", name, got)
			}
		}
	} else {
		if fmt.Sprint(hot) != fmt.Sprint([]string{oldID}) {
			t.Errorf("hot VT with the default offset returned %v, want only the older span %s (VT >= v0.12.0 hides spans younger than 30s from LogsQL)", hot, oldID)
		}
		if fmt.Sprint(cold) != fmt.Sprint(hot) {
			t.Errorf("cold tier with the default offset returned %v, hot VT returned %v: the tiers must agree", cold, hot)
		}
	}

	waitProbeTenantFlushed(t)
}

// waitProbeTenantFlushed returns once the cold manifest lists the probe tenant
// with a file: the flush its spans cause is then over, and no later test can
// see it happen.
func waitProbeTenantFlushed(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		r := fetch(t, lhtBaseURL, "/lakehouse/api/v1/tenants", nil)
		var d struct {
			Tenants []tenantSummary `json:"tenants"`
		}
		if r.StatusCode == http.StatusOK && json.Unmarshal(r.Body, &d) == nil {
			for _, te := range d.Tenants {
				if te.AccountID == latencyProbeAccount && te.TotalFiles > 0 {
					return
				}
			}
		}
		time.Sleep(time.Second)
	}
	t.Logf("the probe tenant %s:%s did not appear in the cold manifest within 45s; a later manifest read may see its flush", latencyProbeAccount, latencyProbeProject)
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
