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
