//go:build parity

package parity

// LogsQL latency-offset parity (VictoriaTraces v0.12.0).
//
// VictoriaTraces >= v0.12.0 applies -search.latencyOffset (default 30s) to the
// LogsQL query APIs except live tailing, so a span younger than the offset is
// hidden from /select/logsql/* unless the request says
// disable_latency_offset=true. Before v0.12.0 only the Jaeger and Tempo APIs
// had it. The traces binary serves LogsQL through VictoriaLogs' handlers and
// adds the same "_time up to now-offset" filter itself
// (lakehouse-traces/internal/selectapi.applyLatencyOffset), so hot VT and the
// cold tier must answer the same for a span younger than the offset, with and
// without the opt-out. The rest of the suite opts out (withoutLatencyOffset)
// because it means "compare everything the seed wrote"; these cases set the
// argument explicitly so the helper leaves them alone.
//
// This file writes two spans into the shared stack, so it is named to sort (and
// therefore run) after every other file of the package: the suite's counts, its
// service lists and its tenant totals are read before the probe spans exist.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"
)

// pushOTLPSpan writes one span, ending at endAt, to a tier's OTLP endpoint.
func pushOTLPSpan(t *testing.T, base, traceID, spanID, service string, endAt time.Time) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"resourceSpans": []map[string]any{{
		"resource": map[string]any{"attributes": []map[string]any{
			{"key": "service.name", "value": map[string]any{"stringValue": service}},
		}},
		"scopeSpans": []map[string]any{{
			"scope": map[string]any{"name": "latency-offset-parity"},
			"spans": []map[string]any{{
				"traceId":           traceID,
				"spanId":            spanID,
				"name":              "latency-offset-probe",
				"kind":              2,
				"startTimeUnixNano": fmt.Sprintf("%d", endAt.Add(-time.Second).UnixNano()),
				"endTimeUnixNano":   fmt.Sprintf("%d", endAt.UnixNano()),
			}},
		}},
	}}})
	req, err := http.NewRequest(http.MethodPost, base+"/insert/opentelemetry/v1/traces", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("push span to %s: %v", base, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("push span to %s: status %d", base, resp.StatusCode)
	}
}

// probeTraceIDs returns the sorted trace_ids the tier's LogsQL query API
// returns for the two probe traces.
func probeTraceIDs(t *testing.T, base string, ids []string, disable string) []string {
	t.Helper()
	params := url.Values{
		"query":                  {"trace_id:in(" + strings.Join(ids, ",") + ")"},
		"start":                  {"-1h"},
		"disable_latency_offset": {disable},
	}
	r := fetch(t, base, "/select/logsql/query", params)
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
	want := ids
	deadline := time.Now().Add(25 * time.Second)
	for {
		hot := probeTraceIDs(t, vtBaseURL, ids, "true")
		cold := probeTraceIDs(t, lhtBaseURL, ids, "true")
		if strings.Join(hot, ",") == strings.Join(want, ",") && strings.Join(cold, ",") == strings.Join(want, ",") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("with disable_latency_offset=true both tiers must return both probe spans: hot=%v cold=%v want=%v", hot, cold, want)
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
		return
	}
	if fmt.Sprint(hot) != fmt.Sprint([]string{oldID}) {
		t.Errorf("hot VT with the default offset returned %v, want only the older span %s (VT >= v0.12.0 hides spans younger than 30s from LogsQL)", hot, oldID)
	}
	if fmt.Sprint(cold) != fmt.Sprint(hot) {
		t.Errorf("cold tier with the default offset returned %v, hot VT returned %v: the tiers must agree", cold, hot)
	}
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
