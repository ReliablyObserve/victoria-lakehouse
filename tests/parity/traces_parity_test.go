//go:build parity

package parity

import (
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"
)

func getTraceID(t *testing.T, baseURL string) string {
	t.Helper()
	params := url.Values{
		"service":  {"api-gateway"},
		"lookback": {"48h"},
		"limit":    {"1"},
	}
	r := fetch(t, baseURL, "/select/jaeger/api/traces", params)
	if r.StatusCode != 200 {
		t.Fatalf("Jaeger search returned %d", r.StatusCode)
	}
	var resp map[string]any
	if err := json.Unmarshal(r.Body, &resp); err != nil {
		t.Fatalf("parse: %v", err)
	}
	dataArr, _ := resp["data"].([]any)
	if len(dataArr) == 0 {
		t.Skip("no traces found")
	}
	first, _ := dataArr[0].(map[string]any)
	id, _ := first["traceID"].(string)
	if id == "" {
		t.Fatal("empty traceID")
	}
	return id
}

func TestParity_Traces_Jaeger(t *testing.T) {
	t.Run("jaeger_services", func(t *testing.T) {
		ref := fetch(t, vtBaseURL, "/select/jaeger/api/services", nil)
		sut := fetch(t, lhtBaseURL, "/select/jaeger/api/services", nil)
		compareParity(t, ParityCase{Compare: SetEqual}, ref, sut)
	})

	t.Run("jaeger_operations", func(t *testing.T) {
		ref := fetch(t, vtBaseURL, "/select/jaeger/api/services/api-gateway/operations", nil)
		sut := fetch(t, lhtBaseURL, "/select/jaeger/api/services/api-gateway/operations", nil)
		compareParity(t, ParityCase{Compare: SetEqual}, ref, sut)
	})

	t.Run("jaeger_search_service", func(t *testing.T) {
		params := url.Values{"service": {"api-gateway"}, "lookback": {"48h"}, "limit": {"5"}}
		ref := fetch(t, vtBaseURL, "/select/jaeger/api/traces", params)
		sut := fetch(t, lhtBaseURL, "/select/jaeger/api/traces", params)
		compareParity(t, ParityCase{Compare: NonEmpty}, ref, sut)
	})

	t.Run("jaeger_search_limit", func(t *testing.T) {
		params := url.Values{"service": {"api-gateway"}, "lookback": {"48h"}, "limit": {"5"}}
		ref := fetch(t, vtBaseURL, "/select/jaeger/api/traces", params)
		sut := fetch(t, lhtBaseURL, "/select/jaeger/api/traces", params)
		var refResp, sutResp map[string]any
		json.Unmarshal(ref.Body, &refResp)
		json.Unmarshal(sut.Body, &sutResp)
		refData, _ := refResp["data"].([]any)
		sutData, _ := sutResp["data"].([]any)
		if len(refData) > 5 {
			t.Errorf("ref returned %d traces, expected <= 5", len(refData))
		}
		if len(sutData) > 5 {
			t.Errorf("sut returned %d traces, expected <= 5", len(sutData))
		}
		t.Logf("jaeger_search_limit: ref=%d sut=%d", len(refData), len(sutData))
	})

	t.Run("jaeger_trace_detail", func(t *testing.T) {
		traceID := getTraceID(t, vtBaseURL)
		ref := fetch(t, vtBaseURL, "/select/jaeger/api/traces/"+traceID, nil)
		sut := fetch(t, lhtBaseURL, "/select/jaeger/api/traces/"+traceID, nil)
		compareParity(t, ParityCase{Compare: StructureMatch}, ref, sut)
	})

	t.Run("jaeger_dependencies", func(t *testing.T) {
		params := url.Values{"lookback": {"48h"}}
		ref := fetch(t, vtBaseURL, "/select/jaeger/api/dependencies", params)
		sut := fetch(t, lhtBaseURL, "/select/jaeger/api/dependencies", params)
		compareParity(t, ParityCase{Compare: NonEmpty}, ref, sut)
	})
}

func TestParity_Traces_LogsQL(t *testing.T) {
	tracesFullRange := func() url.Values {
		now := time.Now()
		return url.Values{
			"start": {fmt.Sprintf("%d", now.Add(-48*time.Hour).UnixNano())},
			"end":   {fmt.Sprintf("%d", now.UnixNano())},
		}
	}

	t.Run("traces_field_names", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "*")
		ref := fetch(t, vtBaseURL, "/select/logsql/field_names", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/field_names", params)
		compareParity(t, ParityCase{Compare: SetSuperset}, ref, sut)
	})

	t.Run("traces_field_values", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "*")
		params.Set("field", "service.name")
		ref := fetch(t, vtBaseURL, "/select/logsql/field_values", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/field_values", params)
		compareParity(t, ParityCase{Compare: SetEqual}, ref, sut)
	})

	t.Run("traces_query_wildcard", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "*")
		params.Set("limit", "10")
		ref := fetch(t, vtBaseURL, "/select/logsql/query", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/query", params)
		compareParity(t, ParityCase{Compare: NonEmpty}, ref, sut)
	})

	t.Run("traces_stats_count", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "* | stats count() rows")
		ref := fetch(t, vtBaseURL, "/select/logsql/stats_query", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/stats_query", params)
		compareParity(t, ParityCase{Compare: CountEqual}, ref, sut)
	})

	t.Run("traces_hits", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "*")
		params.Set("step", "3600s")
		ref := fetch(t, vtBaseURL, "/select/logsql/hits", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/hits", params)
		compareParity(t, ParityCase{Compare: BucketMatch}, ref, sut)
	})

	t.Run("traces_filter_service", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", `service.name:="api-gateway" | stats count() rows`)
		ref := fetch(t, vtBaseURL, "/select/logsql/stats_query", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/stats_query", params)
		compareParity(t, ParityCase{Compare: CountEqual}, ref, sut)
	})

	t.Run("traces_trace_id_lookup", func(t *testing.T) {
		traceID := getTraceID(t, vtBaseURL)
		params := tracesFullRange()
		params.Set("query", fmt.Sprintf(`trace_id:="%s"`, traceID))
		params.Set("limit", "100")
		ref := fetch(t, vtBaseURL, "/select/logsql/query", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/query", params)
		compareParity(t, ParityCase{Compare: RowsMatch}, ref, sut)
	})

	t.Run("traces_stats_by_service", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "* | stats by(service.name) count() rows")
		ref := fetch(t, vtBaseURL, "/select/logsql/stats_query", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/stats_query", params)
		compareParity(t, ParityCase{Compare: StructureMatch}, ref, sut)
	})

	t.Run("traces_empty_range", func(t *testing.T) {
		now := time.Now()
		future := now.Add(365 * 24 * time.Hour)
		params := url.Values{
			"query": {"* | stats count() rows"},
			"start": {fmt.Sprintf("%d", future.UnixNano())},
			"end":   {fmt.Sprintf("%d", future.Add(time.Hour).UnixNano())},
		}
		ref := fetch(t, vtBaseURL, "/select/logsql/stats_query", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/stats_query", params)
		compareParity(t, ParityCase{Compare: CountEqual}, ref, sut)
	})
}
