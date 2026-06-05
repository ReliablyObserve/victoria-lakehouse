//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

// TestServiceGraph_ColdTierGeneratesEdges verifies that VT's
// upstream servicegraph background task — running inside the LH
// process and routed through our adapter for both query and
// persist — produces edges that Jaeger's /api/dependencies reader
// can serve from cold storage.
//
// Test approach: push a small set of parent/child span pairs that
// span SRV-A → SRV-B → SRV-C; wait for one task tick (compose runs
// it every 2 minutes); then assert the Jaeger dependencies endpoint
// returns at least one (parent, child) edge.
//
// Patience window matches `-servicegraph.taskInterval=2m` + flush
// lag. Test is tagged e2e + long-running on purpose.
func TestServiceGraph_ColdTierGeneratesEdges(t *testing.T) {
	stamp := time.Now().UnixNano()
	traceID := fmt.Sprintf("%032x", stamp)

	// One trace, three spans with parent/child relationships across
	// three distinct services. kind=3 (CLIENT) on the parent side
	// and kind=2 (SERVER) on the child side — the exact pair VT's
	// service-graph aggregator joins on.
	pushTrace(t, tracesBaseURL, traceID, []traceSpan{
		{spanID: "aaaa000000000001", parentSpanID: "", service: "service-a", kind: 3, name: "GET /a"},
		{spanID: "aaaa000000000002", parentSpanID: "aaaa000000000001", service: "service-b", kind: 2, name: "GET /b"},
		{spanID: "aaaa000000000003", parentSpanID: "aaaa000000000002", service: "service-c", kind: 2, name: "GET /c"},
	})

	// Wait for: (a) writer flush (120s) (b) one servicegraph task
	// tick (120s). 5 minutes is the safe upper bound.
	deadline := time.Now().Add(5 * time.Minute)
	var lastBody []byte
	for time.Now().Before(deadline) {
		body, err := fetchJaegerDependencies(t)
		if err == nil && hasEdgeContaining(body, "service-a", "service-b") {
			t.Logf("service-graph picked up service-a→service-b edge after %v", time.Since(time.Unix(0, stamp)))
			return
		}
		lastBody = body
		time.Sleep(30 * time.Second)
	}
	t.Fatalf("service-graph task never produced the expected edge within 5min; last response: %s", string(lastBody))
}

type traceSpan struct {
	spanID, parentSpanID, service, name string
	kind                                int
}

func pushTrace(t *testing.T, base, traceID string, spans []traceSpan) {
	t.Helper()
	now := time.Now()
	resourceSpans := make([]map[string]any, 0, len(spans))
	for _, s := range spans {
		resourceSpans = append(resourceSpans, map[string]any{
			"resource": map[string]any{
				"attributes": []map[string]any{
					{"key": "service.name", "value": map[string]any{"stringValue": s.service}},
				},
			},
			"scopeSpans": []map[string]any{{
				"scope": map[string]any{"name": "service-graph-e2e"},
				"spans": []map[string]any{{
					"traceId":           traceID,
					"spanId":            s.spanID,
					"parentSpanId":      s.parentSpanID,
					"name":              s.name,
					"kind":              s.kind,
					"startTimeUnixNano": fmt.Sprintf("%d", now.Add(-1*time.Second).UnixNano()),
					"endTimeUnixNano":   fmt.Sprintf("%d", now.UnixNano()),
				}},
			}},
		})
	}
	body, _ := json.Marshal(map[string]any{"resourceSpans": resourceSpans})

	req, _ := http.NewRequest("POST", base+"/insert/opentelemetry/v1/traces", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("push trace: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("push trace: status %d", resp.StatusCode)
	}
}

func fetchJaegerDependencies(t *testing.T) ([]byte, error) {
	t.Helper()
	endTs := time.Now().UnixMilli()
	url := fmt.Sprintf("%s/select/jaeger/api/dependencies?endTs=%d&lookback=600000", tracesBaseURL, endTs)
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(mustNewRequest("GET", url))
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return body, fmt.Errorf("status %d", resp.StatusCode)
	}
	return body, nil
}

func mustNewRequest(method, url string) *http.Request {
	req, _ := http.NewRequest(method, url, nil)
	return req
}

// hasEdgeContaining looks for {parent, child, callCount} entries in
// Jaeger's /api/dependencies response shape:
//
//	{"data": [{"parent":"…","child":"…","callCount": N}, ...]}
//
// Returns true when an entry matches the requested (parent, child)
// pair regardless of callCount.
func hasEdgeContaining(body []byte, parent, child string) bool {
	var resp struct {
		Data []struct {
			Parent    string `json:"parent"`
			Child     string `json:"child"`
			CallCount int    `json:"callCount"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return false
	}
	for _, e := range resp.Data {
		if e.Parent == parent && e.Child == child {
			return true
		}
	}
	return false
}

// TestServiceGraph_SmokeOfDependenciesEndpoint just exercises the
// endpoint to make sure it's mounted and returns valid JSON, even
// when there are no edges (e.g. fresh stack). Catches a misrouted
// handler or a regression where vtstorage_adapter doesn't translate
// the LogsQL aggregation pipe.
func TestServiceGraph_SmokeOfDependenciesEndpoint(t *testing.T) {
	body, err := fetchJaegerDependencies(t)
	if err != nil {
		t.Fatalf("dependencies endpoint: %v body=%s", err, string(body))
	}
	var probe struct {
		Data []any `json:"data"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		t.Fatalf("dependencies response is not valid JSON: %v body=%s", err, string(body))
	}
	t.Logf("dependencies endpoint OK — %d edges currently visible", len(probe.Data))
}
