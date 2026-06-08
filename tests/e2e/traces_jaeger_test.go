//go:build e2e

package e2e

import (
	"encoding/json"
	"net/url"
	"testing"
)

func TestJaeger_Services(t *testing.T) {
	body := httpGetBody(t, tracesBaseURL, "/api/services", nil)
	resp := mustParseJSON(t, body)

	dataRaw, ok := resp["data"]
	if !ok {
		t.Fatal("response missing 'data' field")
	}

	dataArr, ok := dataRaw.([]any)
	if !ok {
		t.Fatalf("expected data to be an array, got %T", dataRaw)
	}

	if len(dataArr) == 0 {
		t.Fatal("expected at least one service")
	}

	services := make([]string, 0, len(dataArr))
	for _, v := range dataArr {
		if s, ok := v.(string); ok {
			services = append(services, s)
		}
	}

	expectedServices := []string{
		"api-gateway", "user-service", "order-service",
		"payment-service", "notification-service",
	}

	for _, svc := range expectedServices {
		if !containsString(services, svc) {
			t.Errorf("services missing %q; got: %v", svc, services)
		}
	}

	t.Logf("Jaeger services: %v", services)
}

func TestJaeger_Operations(t *testing.T) {
	body := httpGetBody(t, tracesBaseURL, "/api/services/api-gateway/operations", nil)
	resp := mustParseJSON(t, body)

	dataRaw, ok := resp["data"]
	if !ok {
		t.Fatal("response missing 'data' field")
	}

	dataArr, ok := dataRaw.([]any)
	if !ok {
		t.Fatalf("expected data to be an array, got %T", dataRaw)
	}

	if len(dataArr) == 0 {
		t.Fatal("expected at least one operation for api-gateway")
	}

	ops := make([]string, 0, len(dataArr))
	for _, v := range dataArr {
		if s, ok := v.(string); ok {
			ops = append(ops, s)
		}
	}

	t.Logf("api-gateway operations: %v", ops)
}

func TestJaeger_Search(t *testing.T) {
	params := url.Values{
		"service":  {"api-gateway"},
		"lookback": {"72h"},
		"limit":    {"5"},
	}

	body := httpGetBody(t, tracesBaseURL, "/api/traces", params)
	resp := assertJaegerResponse(t, body)

	dataArr := resp["data"].([]any)
	t.Logf("Jaeger search returned %d traces", len(dataArr))

	// Each trace should have spans and processes
	for i, entry := range dataArr {
		obj := entry.(map[string]any)
		spans := obj["spans"].([]any)
		procs := obj["processes"].(map[string]any)

		if len(spans) == 0 {
			t.Errorf("trace[%d] has no spans", i)
		}
		if len(procs) == 0 {
			t.Errorf("trace[%d] has no processes", i)
		}
	}
}

func TestJaeger_TraceDetail(t *testing.T) {
	// First, search for a trace ID
	params := url.Values{
		"service":  {"api-gateway"},
		"lookback": {"72h"},
		"limit":    {"1"},
	}

	searchBody := httpGetBody(t, tracesBaseURL, "/api/traces", params)
	searchResp := mustParseJSON(t, searchBody)

	dataArr := searchResp["data"].([]any)
	if len(dataArr) == 0 {
		t.Skip("no traces found for api-gateway")
	}

	firstTrace := dataArr[0].(map[string]any)
	traceID := firstTrace["traceID"].(string)
	if traceID == "" {
		t.Fatal("first trace has empty traceID")
	}

	// Fetch trace detail
	detailBody := httpGetBody(t, tracesBaseURL, "/api/traces/"+traceID, nil)
	detailResp := assertJaegerResponse(t, detailBody)

	detailData := detailResp["data"].([]any)
	if len(detailData) == 0 {
		t.Fatal("trace detail returned no data")
	}

	trace := detailData[0].(map[string]any)
	if trace["traceID"] != traceID {
		t.Errorf("expected traceID=%s, got %v", traceID, trace["traceID"])
	}

	spans := trace["spans"].([]any)
	if len(spans) == 0 {
		t.Fatal("trace detail has no spans")
	}

	procs := trace["processes"].(map[string]any)
	if len(procs) == 0 {
		t.Fatal("trace detail has no processes")
	}

	t.Logf("trace %s: %d spans, %d processes", traceID, len(spans), len(procs))
}

func TestJaeger_TraceDetail_SpanTags(t *testing.T) {
	traceID := getAnyTraceID(t)
	if traceID == "" {
		t.Skip("no traces available")
	}

	detailBody := httpGetBody(t, tracesBaseURL, "/api/traces/"+traceID, nil)
	detailResp := assertJaegerResponse(t, detailBody)
	detailData := detailResp["data"].([]any)
	trace := detailData[0].(map[string]any)
	spans := trace["spans"].([]any)

	// Check that at least one span has tags
	foundTags := false
	foundSpanKind := false
	foundScopeAttr := false

	for _, spanRaw := range spans {
		span := spanRaw.(map[string]any)
		tags, ok := span["tags"].([]any)
		if !ok || len(tags) == 0 {
			continue
		}
		foundTags = true

		for _, tagRaw := range tags {
			tag := tagRaw.(map[string]any)
			key, _ := tag["key"].(string)
			if key == "span.kind" {
				foundSpanKind = true
			}
			if key == "scope_attr:otel.library.name" {
				foundScopeAttr = true
			}
		}
	}

	if !foundTags {
		t.Error("no spans have tags in trace detail")
	}
	if !foundSpanKind {
		t.Log("span.kind tag not found in any span — may depend on span kind value")
	}
	if !foundScopeAttr {
		t.Log("scope_attr:otel.library.name tag not found — scope attribute mapping may vary")
	}
}

func TestJaeger_TraceDetail_ProcessTags(t *testing.T) {
	traceID := getAnyTraceID(t)
	if traceID == "" {
		t.Skip("no traces available")
	}

	detailBody := httpGetBody(t, tracesBaseURL, "/api/traces/"+traceID, nil)
	detailResp := assertJaegerResponse(t, detailBody)
	detailData := detailResp["data"].([]any)
	trace := detailData[0].(map[string]any)
	procs := trace["processes"].(map[string]any)

	// At least one process should have tags (resource attributes)
	foundProcessWithTags := false
	foundResourceAttrs := make(map[string]bool)

	for pid, procRaw := range procs {
		proc := procRaw.(map[string]any)
		tags, ok := proc["tags"].([]any)
		if !ok || len(tags) == 0 {
			continue
		}
		foundProcessWithTags = true

		for _, tagRaw := range tags {
			tag := tagRaw.(map[string]any)
			key, _ := tag["key"].(string)
			foundResourceAttrs[key] = true
		}

		_ = pid
	}

	if !foundProcessWithTags {
		t.Log("no processes have tags — resource attributes may not be included in search results")
	}

	// Check for expected resource attributes
	expectedAttrs := []string{
		"deployment.environment", "cloud.region", "host.name",
		"k8s.namespace.name", "k8s.deployment.name", "k8s.node.name",
	}
	for _, attr := range expectedAttrs {
		if foundResourceAttrs[attr] {
			t.Logf("found resource attribute: %s", attr)
		}
	}
}

func TestJaeger_TraceDetail_SpanAttributes(t *testing.T) {
	traceID := getAnyTraceID(t)
	if traceID == "" {
		t.Skip("no traces available")
	}

	detailBody := httpGetBody(t, tracesBaseURL, "/api/traces/"+traceID, nil)
	detailResp := assertJaegerResponse(t, detailBody)
	detailData := detailResp["data"].([]any)
	trace := detailData[0].(map[string]any)
	spans := trace["spans"].([]any)

	foundHTTPMethod := false
	foundHTTPStatusCode := false
	foundDBSystem := false

	for _, spanRaw := range spans {
		span := spanRaw.(map[string]any)
		tags, ok := span["tags"].([]any)
		if !ok {
			continue
		}

		for _, tagRaw := range tags {
			tag := tagRaw.(map[string]any)
			key, _ := tag["key"].(string)
			switch key {
			case "span_attr:http.method":
				foundHTTPMethod = true
			case "span_attr:http.status_code":
				foundHTTPStatusCode = true
			case "span_attr:db.system":
				foundDBSystem = true
			}
		}
	}

	if !foundHTTPMethod {
		t.Log("span_attr:http.method not found — trace may not include HTTP spans")
	}
	if !foundHTTPStatusCode {
		t.Log("span_attr:http.status_code not found — trace may not include HTTP spans")
	}
	if !foundDBSystem {
		t.Log("span_attr:db.system not found — trace may not include DB spans")
	}
}

func TestJaeger_TraceDetail_ParentRefs(t *testing.T) {
	traceID := getAnyTraceID(t)
	if traceID == "" {
		t.Skip("no traces available")
	}

	detailBody := httpGetBody(t, tracesBaseURL, "/api/traces/"+traceID, nil)
	detailResp := assertJaegerResponse(t, detailBody)
	detailData := detailResp["data"].([]any)
	trace := detailData[0].(map[string]any)
	spans := trace["spans"].([]any)

	if len(spans) < 2 {
		t.Skip("trace has fewer than 2 spans, cannot test parent references")
	}

	// Non-root spans should have references
	foundReference := false
	for _, spanRaw := range spans {
		span := spanRaw.(map[string]any)
		refs, ok := span["references"].([]any)
		if !ok || len(refs) == 0 {
			continue
		}
		foundReference = true

		for _, refRaw := range refs {
			ref := refRaw.(map[string]any)
			refType, _ := ref["refType"].(string)
			if refType != "CHILD_OF" {
				t.Errorf("unexpected reference type: %s", refType)
			}
			refTraceID, _ := ref["traceID"].(string)
			if refTraceID != traceID {
				t.Errorf("reference traceID %s != span traceID %s", refTraceID, traceID)
			}
			refSpanID, _ := ref["spanID"].(string)
			if refSpanID == "" {
				t.Error("reference has empty spanID")
			}
		}
	}

	if !foundReference {
		t.Error("no spans have parent references in a multi-span trace")
	}
}

// TestJaeger_ColdRecentTrace_NotSilentlyEmpty pins the cold-Jaeger regression
// where a recently-flushed/cold partition returned 0 traces through the Jaeger
// API even though the underlying span rows existed in storage.
//
// The trap this test closes: existing Jaeger tests do t.Skip("no traces found")
// when /api/traces returns an empty data array, so a cold-returns-0 bug passes
// silently. Here we FIRST prove via LogsQL that api-gateway spans exist in the
// recent window, and only THEN exercise the Jaeger search + trace-by-id path.
// If the data demonstrably exists but Jaeger returns nothing, we t.Fatal.
//
// Invariant: if LogsQL shows >=1 api-gateway span row in the recent window,
// then the Jaeger /api/traces search MUST return >=1 trace, and a Jaeger
// /api/traces/<id> retrieval of the first result MUST return >=1 span.
// t.Skip is permitted ONLY for the genuine "no data ingested at all" case.
func TestJaeger_ColdRecentTrace_NotSilentlyEmpty(t *testing.T) {
	// (1) Prove data EXISTS via LogsQL against the traces store. queryTraces
	// queries tracesBaseURL /select/logsql/query over the recent (default
	// ~30min) window, which is the same cold/recent slice the Jaeger path hit.
	rows := queryTraces(t, `service.name:="api-gateway"`, 5)
	if len(rows) == 0 {
		// Genuine "nothing ingested at all" — the ONLY legitimate skip.
		t.Skip("no api-gateway span rows in the recent window; nothing ingested to verify against")
	}

	// Sanity: confirm the rows really are api-gateway spans with a trace_id, so
	// we don't pass off unrelated data as proof-of-existence.
	dataExists := false
	for _, row := range rows {
		svc, _ := row["service.name"].(string)
		traceID, _ := row["trace_id"].(string)
		if svc == "api-gateway" && traceID != "" {
			dataExists = true
			break
		}
	}
	if !dataExists {
		t.Skip("no api-gateway rows with a trace_id in the recent window; nothing to verify against")
	}
	t.Logf("LogsQL confirms api-gateway data exists: %d recent span row(s)", len(rows))

	// (2) Run the Jaeger search for api-gateway over a recent window. Use a 1h
	// lookback to comfortably cover the ~30min LogsQL window plus clock skew,
	// while still exercising the recent/cold path rather than the full 72h
	// range used elsewhere — that recent window is where the bug surfaced.
	params := url.Values{
		"service":  {"api-gateway"},
		"lookback": {"1h"},
		"limit":    {"5"},
	}
	searchBody := httpGetBody(t, tracesBaseURL, "/api/traces", params)
	searchResp := mustParseJSON(t, searchBody)

	dataRaw, ok := searchResp["data"]
	if !ok {
		t.Fatalf("Jaeger search response missing 'data' field; raw: %s", string(searchBody))
	}
	dataArr, ok := dataRaw.([]any)
	if !ok {
		t.Fatalf("Jaeger search 'data' is not an array, got %T", dataRaw)
	}

	// (3) Data is PROVEN to exist (step 1) — so an empty Jaeger search is the
	// cold-returns-0 regression, NOT a benign no-data case. Fatal, never skip.
	if len(dataArr) == 0 {
		t.Fatalf("cold Jaeger regression: LogsQL shows api-gateway spans exist in the recent window, "+
			"but Jaeger /api/traces?service=api-gateway&lookback=1h returned 0 traces; raw: %s",
			string(searchBody))
	}
	t.Logf("Jaeger search returned %d trace(s) over 1h lookback", len(dataArr))

	// First result must carry a non-empty traceID.
	firstTrace, ok := dataArr[0].(map[string]any)
	if !ok {
		t.Fatalf("Jaeger search data[0] is not an object, got %T", dataArr[0])
	}
	traceID, _ := firstTrace["traceID"].(string)
	if traceID == "" {
		t.Fatal("cold Jaeger regression: first search result has an empty traceID")
	}

	// Trace-by-id retrieval of that first result MUST return >=1 span. A cold
	// partial-hit bug could let search list the trace but return no spans on
	// detail fetch — that is still a regression, so fatal.
	detailBody := httpGetBody(t, tracesBaseURL, "/api/traces/"+traceID, nil)
	detailResp := mustParseJSON(t, detailBody)

	detailRaw, ok := detailResp["data"]
	if !ok {
		t.Fatalf("Jaeger trace-by-id response missing 'data' field; raw: %s", string(detailBody))
	}
	detailData, ok := detailRaw.([]any)
	if !ok {
		t.Fatalf("Jaeger trace-by-id 'data' is not an array, got %T", detailRaw)
	}
	if len(detailData) == 0 {
		t.Fatalf("cold Jaeger regression: trace-by-id retrieval of %s returned no trace data "+
			"despite the trace appearing in search; raw: %s", traceID, string(detailBody))
	}

	trace, ok := detailData[0].(map[string]any)
	if !ok {
		t.Fatalf("Jaeger trace-by-id data[0] is not an object, got %T", detailData[0])
	}
	spans, ok := trace["spans"].([]any)
	if !ok {
		t.Fatalf("Jaeger trace-by-id data[0] missing or invalid 'spans'; raw: %s", string(detailBody))
	}
	if len(spans) == 0 {
		t.Fatalf("cold Jaeger regression: trace-by-id retrieval of %s returned 0 spans "+
			"despite api-gateway data existing; raw: %s", traceID, string(detailBody))
	}

	t.Logf("cold-recent invariant holds: trace %s retrieved with %d span(s)", traceID, len(spans))
}

// getAnyTraceID searches for a trace and returns its ID.
func getAnyTraceID(t *testing.T) string {
	t.Helper()

	params := url.Values{
		"service":  {"api-gateway"},
		"lookback": {"72h"},
		"limit":    {"1"},
	}

	body := httpGetBody(t, tracesBaseURL, "/api/traces", params)

	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("failed to parse Jaeger search response: %v", err)
	}

	dataArr, ok := resp["data"].([]any)
	if !ok || len(dataArr) == 0 {
		return ""
	}

	first := dataArr[0].(map[string]any)
	traceID, _ := first["traceID"].(string)
	return traceID
}
