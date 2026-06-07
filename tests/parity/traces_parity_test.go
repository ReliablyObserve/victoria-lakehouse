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
	// VT's Jaeger handler panics when reading LH trace data because
	// start_time_unix_nano is stored as a formatted timestamp string.
	// The Jaeger handler rewrite is tracked as a separate task.
	t.Skip("Jaeger handler panics on LH data format — requires handler rewrite (Task 6)")

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

// TestParity_Traces_ColdHot_RecentTraceID pins the recently-flushed parity
// invariant that had NO gate: cold tier must not return 0 traces when hot
// VT returns N for the same recent-lookback query. This is the exact failure
// shape of the cold-Jaeger-0-traces-at-12h / smartCache partial-hit bug
// (commit bd838e9) — hot VT served the data, cold LH silently narrowed the
// recently-flushed range to empty. compareNonEmpty alone is too weak here
// (a `{"data":[]}` body is non-empty bytes), so we parse the array/traces
// length explicitly and assert: hot>0 implies cold>0.
//
// Unlike TestParity_Traces_Jaeger (skipped, broad structural diff), this is
// targeted at presence-parity only and runs against the same live VT/LHT
// stack as the rest of this file.
func TestParity_Traces_ColdHot_RecentTraceID(t *testing.T) {
	// jaegerTraceCount returns the number of traces in a Jaeger search
	// response (the `data` array, same shape getTraceID reads).
	jaegerTraceCount := func(t *testing.T, body []byte) int {
		t.Helper()
		var resp map[string]any
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("parse jaeger response: %v", err)
		}
		dataArr, _ := resp["data"].([]any)
		return len(dataArr)
	}

	// tempoTraceCount returns the number of traces in a Tempo /api/search
	// response (the `traces` array, same shape servicegraph_parity_test reads).
	tempoTraceCount := func(t *testing.T, body []byte) int {
		t.Helper()
		var resp struct {
			Traces []map[string]any `json:"traces"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("parse tempo response: %v", err)
		}
		return len(resp.Traces)
	}

	// Recent lookback: matches the recently-flushed window the bug zeroed out.
	t.Run("jaeger_recent_lookback", func(t *testing.T) {
		params := url.Values{
			"service":  {"api-gateway"},
			"lookback": {"1h"},
			"limit":    {"20"},
		}
		ref := fetch(t, vtBaseURL, "/select/jaeger/api/traces", params)
		sut := fetch(t, lhtBaseURL, "/select/jaeger/api/traces", params)
		// NonEmpty-style: both must be 200 and parseable.
		compareParity(t, ParityCase{Compare: NonEmpty}, ref, sut)
		nHot := jaegerTraceCount(t, ref.Body)
		nCold := jaegerTraceCount(t, sut.Body)
		t.Logf("jaeger_recent_lookback: hot=%d cold=%d", nHot, nCold)
		// The recently-flushed parity invariant: if hot has data, cold must too.
		if nHot > 0 && nCold == 0 {
			t.Errorf("recently-flushed parity gap (Jaeger): hot=%d traces but cold=0 "+
				"for service=api-gateway lookback=1h. Cold tier zeroed a recently-flushed "+
				"window (smartCache partial-hit narrowing / cold-Jaeger-0-at-12h regression).", nHot)
		}
	})

	// Tempo TraceQL by resource service name — same recently-flushed window.
	t.Run("tempo_recent_resource_service_name", func(t *testing.T) {
		now := time.Now().Unix()
		start := now - 3600 // 1h, matches Jaeger lookback above
		params := url.Values{
			"q":     {`{resource.service.name="api-gateway"}`},
			"limit": {"20"},
			"start": {fmt.Sprint(start)},
			"end":   {fmt.Sprint(now)},
		}
		ref := fetch(t, vtBaseURL, "/select/tempo/api/search", params)
		sut := fetch(t, lhtBaseURL, "/select/tempo/api/search", params)
		compareParity(t, ParityCase{Compare: NonEmpty}, ref, sut)
		nHot := tempoTraceCount(t, ref.Body)
		nCold := tempoTraceCount(t, sut.Body)
		t.Logf("tempo_recent_resource_service_name: hot=%d cold=%d", nHot, nCold)
		if nHot > 0 && nCold == 0 {
			t.Errorf("recently-flushed parity gap (Tempo TraceQL): hot=%d traces but cold=0 "+
				"for q={resource.service.name=\"api-gateway\"} over last 1h. "+
				"Cold tier narrowed a recently-flushed window to empty.", nHot)
		}
	})

	// Tempo root-span / comparison shape — q={nestedSetParent<0} selects root
	// spans; this exercises the comparison-operator query path that the
	// two-phase cold reader must keep non-empty when hot is non-empty.
	t.Run("tempo_recent_root_span", func(t *testing.T) {
		now := time.Now().Unix()
		start := now - 3600
		params := url.Values{
			"q":     {`{nestedSetParent<0}`},
			"limit": {"20"},
			"start": {fmt.Sprint(start)},
			"end":   {fmt.Sprint(now)},
		}
		ref := fetch(t, vtBaseURL, "/select/tempo/api/search", params)
		sut := fetch(t, lhtBaseURL, "/select/tempo/api/search", params)
		compareParity(t, ParityCase{Compare: NonEmpty}, ref, sut)
		nHot := tempoTraceCount(t, ref.Body)
		nCold := tempoTraceCount(t, sut.Body)
		t.Logf("tempo_recent_root_span: hot=%d cold=%d", nHot, nCold)
		if nHot > 0 && nCold == 0 {
			t.Errorf("recently-flushed parity gap (Tempo root-span): hot=%d traces but cold=0 "+
				"for q={nestedSetParent<0} over last 1h. Cold-tier comparison-operator "+
				"query path zeroed a recently-flushed window.", nHot)
		}
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
		params.Set("query", "span_id:*")
		ref := fetch(t, vtBaseURL, "/select/logsql/field_names", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/field_names", params)
		compareParity(t, ParityCase{Compare: SetSuperset}, ref, sut)
	})

	t.Run("traces_field_values", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "span_id:*")
		params.Set("field", "resource_attr:service.name")
		ref := fetch(t, vtBaseURL, "/select/logsql/field_values", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/field_values", params)
		compareParity(t, ParityCase{Compare: SetSuperset}, ref, sut)
	})

	t.Run("traces_query_wildcard", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "span_id:*")
		params.Set("limit", "10")
		ref := fetch(t, vtBaseURL, "/select/logsql/query", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/query", params)
		compareParity(t, ParityCase{Compare: NonEmpty}, ref, sut)
	})

	t.Run("traces_stats_count", func(t *testing.T) {
		// Use span_id:* to exclude VT internal index entries (trace_id_idx, service_graph)
		// that VT stores as regular LogRows but LHT filters at insert time.
		params := tracesFullRange()
		params.Set("query", "span_id:* | stats count() rows")
		ref := fetch(t, vtBaseURL, "/select/logsql/stats_query", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/stats_query", params)
		compareParity(t, ParityCase{Compare: CountEqual}, ref, sut)
	})

	t.Run("traces_hits", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "span_id:*")
		params.Set("step", "3600s")
		ref := fetch(t, vtBaseURL, "/select/logsql/hits", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/hits", params)
		compareParity(t, ParityCase{Compare: BucketMatch}, ref, sut)
	})

	t.Run("traces_filter_service", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", `span_id:* service.name:="api-gateway" | stats count() rows`)
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
		// Skip _msg (VT stores "-", LHT body is empty) and start_time
		// (LHT promoted column alias not present in VT).
		compareParity(t, ParityCase{
			Compare:    RowsMatch,
			SkipFields: []string{"_msg", "start_time"},
		}, ref, sut)
	})

	t.Run("traces_stats_by_service", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "span_id:* | stats by(service.name) count() rows")
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

	// VT metadata fields must appear without span_attr: prefix.
	t.Run("traces_field_names_vt_metadata", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "span_id:*")
		ref := fetch(t, vtBaseURL, "/select/logsql/field_names", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/field_names", params)
		if ref.StatusCode != 200 || sut.StatusCode != 200 {
			t.Fatalf("status ref=%d sut=%d", ref.StatusCode, sut.StatusCode)
		}
		refSet := stringSet(extractValuesStrings(ref.Body))
		sutSet := stringSet(extractValuesStrings(sut.Body))
		vtMetadata := []string{
			"end_time_unix_nano", "start_time_unix_nano", "flags",
			"dropped_attributes_count", "dropped_events_count",
			"dropped_links_count", "scope_version",
		}
		for _, field := range vtMetadata {
			if !refSet[field] {
				continue
			}
			if !sutSet[field] {
				t.Errorf("missing VT metadata field %q (must appear without span_attr: prefix)", field)
			}
			prefixed := "span_attr:" + field
			if sutSet[prefixed] {
				t.Errorf("VT metadata field %q must NOT have span_attr: prefix, found %q", field, prefixed)
			}
		}
	})

	// Span attributes must have span_attr: prefix.
	t.Run("traces_field_names_span_attr_prefix", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "span_id:*")
		sut := fetch(t, lhtBaseURL, "/select/logsql/field_names", params)
		if sut.StatusCode != 200 {
			t.Fatalf("SUT returned status %d", sut.StatusCode)
		}
		vals := extractValuesStrings(sut.Body)
		nameSet := make(map[string]bool, len(vals))
		for _, v := range vals {
			nameSet[v] = true
		}
		spanAttrs := []string{"rpc.system", "db.system", "http.method"}
		for _, attr := range spanAttrs {
			prefixed := "span_attr:" + attr
			if nameSet[prefixed] {
				continue
			}
			if nameSet[attr] {
				t.Errorf("span attribute %q must have span_attr: prefix", attr)
			}
		}
	})

	// Resource attributes must have resource_attr: prefix.
	t.Run("traces_field_names_resource_attr_prefix", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "span_id:*")
		sut := fetch(t, lhtBaseURL, "/select/logsql/field_names", params)
		if sut.StatusCode != 200 {
			t.Fatalf("SUT returned status %d", sut.StatusCode)
		}
		vals := extractValuesStrings(sut.Body)
		nameSet := make(map[string]bool, len(vals))
		for _, v := range vals {
			nameSet[v] = true
		}
		if !nameSet["resource_attr:service.name"] {
			t.Error("missing resource_attr:service.name")
		}
		if nameSet["service.name"] {
			t.Error("service.name must have resource_attr: prefix in traces")
		}
	})

	// start_time_unix_nano must have raw epoch nanoseconds (not formatted timestamp).
	t.Run("traces_start_time_unix_nano_format", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "span_id:*")
		params.Set("limit", "1")
		sut := fetch(t, lhtBaseURL, "/select/logsql/query", params)
		if sut.StatusCode != 200 {
			t.Fatalf("SUT returned status %d", sut.StatusCode)
		}
		rows := parseNDJSON(sut.Body)
		if len(rows) == 0 {
			t.Skip("no rows returned")
		}
		val, ok := rows[0]["start_time_unix_nano"]
		if !ok {
			t.Fatal("start_time_unix_nano field missing from query result")
		}
		valStr := fmt.Sprintf("%v", val)
		if len(valStr) < 10 {
			t.Errorf("start_time_unix_nano value %q looks too short for epoch nanoseconds", valStr)
		}
		if len(valStr) > 0 && (valStr[0] < '0' || valStr[0] > '9') {
			t.Errorf("start_time_unix_nano value %q should be numeric epoch, not formatted timestamp", valStr)
		}
	})

	// VT index entries must not appear in LHT queries.
	t.Run("traces_no_index_entries", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "* | stats count() rows")
		ref := fetch(t, vtBaseURL, "/select/logsql/stats_query", params)
		params.Set("query", "span_id:* | stats count() rows")
		refFiltered := fetch(t, vtBaseURL, "/select/logsql/stats_query", params)
		sutAll := fetch(t, lhtBaseURL, "/select/logsql/stats_query", params)
		refAllCount := extractCount(t, ref.Body)
		refFilteredCount := extractCount(t, refFiltered.Body)
		sutCount := extractCount(t, sutAll.Body)
		indexEntries := refAllCount - refFilteredCount
		if indexEntries <= 0 {
			t.Skip("no VT index entries detected")
		}
		t.Logf("VT index entries: %d (total=%d filtered=%d)", indexEntries, refAllCount, refFilteredCount)
		if sutCount != refFilteredCount {
			t.Errorf("LHT count %d != VT filtered count %d (LHT should not have index entries)", sutCount, refFilteredCount)
		}
	})

	// Filter by VT metadata field value.
	t.Run("traces_filter_metadata_field", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", `span_id:* kind:="1" | stats count() rows`)
		ref := fetch(t, vtBaseURL, "/select/logsql/stats_query", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/stats_query", params)
		compareParity(t, ParityCase{Compare: CountEqual}, ref, sut)
	})

	// Field values for VT metadata field.
	t.Run("traces_field_values_kind", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "span_id:*")
		params.Set("field", "kind")
		ref := fetch(t, vtBaseURL, "/select/logsql/field_values", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/field_values", params)
		compareParity(t, ParityCase{Compare: SetSuperset}, ref, sut)
	})

	// Stats grouped by VT metadata field (kind).
	t.Run("traces_stats_by_kind", func(t *testing.T) {
		params := tracesFullRange()
		params.Set("query", "span_id:* | stats by(kind) count() rows")
		ref := fetch(t, vtBaseURL, "/select/logsql/stats_query", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/stats_query", params)
		compareParity(t, ParityCase{Compare: StructureMatch}, ref, sut)
	})

	// Multi-service filter with VT metadata.
	t.Run("traces_multi_service_count", func(t *testing.T) {
		for _, svc := range []string{"api-gateway", "order-service", "user-service"} {
			t.Run(svc, func(t *testing.T) {
				params := tracesFullRange()
				params.Set("query", fmt.Sprintf(`span_id:* service.name:="%s" | stats count() rows`, svc))
				ref := fetch(t, vtBaseURL, "/select/logsql/stats_query", params)
				sut := fetch(t, lhtBaseURL, "/select/logsql/stats_query", params)
				compareParity(t, ParityCase{Compare: CountEqual}, ref, sut)
			})
		}
	})
}
