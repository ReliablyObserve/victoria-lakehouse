//go:build e2e

package e2e

import (
	"encoding/json"
	"strings"
	"testing"
)

// assertValidNDJSON validates that data is valid newline-delimited JSON.
// Returns the parsed lines as []map[string]any.
func assertValidNDJSON(t *testing.T, data []byte) []map[string]any {
	t.Helper()

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 || (len(lines) == 1 && lines[0] == "") {
		return nil
	}

	var results []map[string]any
	for i, line := range lines {
		if line == "" {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("line %d is not valid JSON: %v\nline: %s", i+1, err, line)
		}
		results = append(results, obj)
	}
	return results
}

// assertVectorResponse validates a Prometheus-style instant vector response.
// Expected shape: {"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[ts,"val"]}]}}
func assertVectorResponse(t *testing.T, data []byte) map[string]any {
	t.Helper()

	resp := mustParseJSON(t, data)

	status, ok := resp["status"].(string)
	if !ok || status != "success" {
		t.Fatalf("expected status=success, got %v", resp["status"])
	}

	dataObj, ok := resp["data"].(map[string]any)
	if !ok {
		t.Fatalf("expected data to be an object, got %T", resp["data"])
	}

	resultType, ok := dataObj["resultType"].(string)
	if !ok || resultType != "vector" {
		t.Fatalf("expected resultType=vector, got %v", dataObj["resultType"])
	}

	result, ok := dataObj["result"].([]any)
	if !ok {
		t.Fatalf("expected result to be an array, got %T", dataObj["result"])
	}

	if len(result) == 0 {
		t.Fatal("expected at least one result in vector response")
	}

	// Validate first result entry
	entry, ok := result[0].(map[string]any)
	if !ok {
		t.Fatalf("expected result[0] to be an object, got %T", result[0])
	}

	if _, ok := entry["metric"]; !ok {
		t.Fatal("expected result[0] to have 'metric' field")
	}

	value, ok := entry["value"].([]any)
	if !ok || len(value) != 2 {
		t.Fatalf("expected result[0].value to be [timestamp, value], got %v", entry["value"])
	}

	return resp
}

// assertMatrixResponse validates a Prometheus-style range vector (matrix) response.
// Expected shape: {"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[ts,"val"],...]}]}}
// Note: the current implementation aliases stats_query_range to stats_query,
// so it returns a vector. This helper accepts both matrix and vector.
func assertMatrixResponse(t *testing.T, data []byte) map[string]any {
	t.Helper()

	resp := mustParseJSON(t, data)

	status, ok := resp["status"].(string)
	if !ok || status != "success" {
		t.Fatalf("expected status=success, got %v", resp["status"])
	}

	dataObj, ok := resp["data"].(map[string]any)
	if !ok {
		t.Fatalf("expected data to be an object, got %T", resp["data"])
	}

	resultType, _ := dataObj["resultType"].(string)
	if resultType != "matrix" && resultType != "vector" {
		t.Fatalf("expected resultType=matrix or vector, got %v", dataObj["resultType"])
	}

	return resp
}

// assertHitsResponse validates the hits endpoint response.
// Expected shape: {"hits":[{"fields":{},"timestamps":["..."],"values":[...]},...]}
func assertHitsResponse(t *testing.T, data []byte) map[string]any {
	t.Helper()

	resp := mustParseJSON(t, data)

	hitsRaw, ok := resp["hits"]
	if !ok {
		t.Fatal("response missing 'hits' field")
	}

	hits, ok := hitsRaw.([]any)
	if !ok {
		t.Fatalf("expected hits to be an array, got %T", hitsRaw)
	}

	for i, entry := range hits {
		obj, ok := entry.(map[string]any)
		if !ok {
			t.Fatalf("hits[%d] is not an object", i)
		}

		if _, ok := obj["fields"]; !ok {
			t.Fatalf("hits[%d] missing 'fields'", i)
		}

		ts, ok := obj["timestamps"].([]any)
		if !ok {
			t.Fatalf("hits[%d] missing or invalid 'timestamps'", i)
		}

		vals, ok := obj["values"].([]any)
		if !ok {
			t.Fatalf("hits[%d] missing or invalid 'values'", i)
		}

		if len(ts) != len(vals) {
			t.Fatalf("hits[%d] timestamps length (%d) != values length (%d)", i, len(ts), len(vals))
		}
	}

	return resp
}

// assertJaegerResponse validates a Jaeger search or trace detail response.
// Expected shape: {"data":[{"traceID":"...","spans":[...],"processes":{...}},...], "total":N, ...}
func assertJaegerResponse(t *testing.T, data []byte) map[string]any {
	t.Helper()

	resp := mustParseJSON(t, data)

	dataRaw, ok := resp["data"]
	if !ok {
		t.Fatal("response missing 'data' field")
	}

	dataArr, ok := dataRaw.([]any)
	if !ok {
		t.Fatalf("expected data to be an array, got %T", dataRaw)
	}

	for i, entry := range dataArr {
		obj, ok := entry.(map[string]any)
		if !ok {
			t.Fatalf("data[%d] is not an object", i)
		}

		if _, ok := obj["traceID"]; !ok {
			t.Fatalf("data[%d] missing 'traceID'", i)
		}

		spans, ok := obj["spans"].([]any)
		if !ok {
			t.Fatalf("data[%d] missing or invalid 'spans'", i)
		}
		if len(spans) == 0 {
			t.Fatalf("data[%d] has no spans", i)
		}

		procs, ok := obj["processes"].(map[string]any)
		if !ok {
			t.Fatalf("data[%d] missing or invalid 'processes'", i)
		}
		if len(procs) == 0 {
			t.Fatalf("data[%d] has no processes", i)
		}
	}

	return resp
}

// assertValuesResponse validates the field_names / field_values / streams response.
// Expected shape: {"values":[{"value":"...","hits":N},...]}
func assertValuesResponse(t *testing.T, data []byte) []map[string]any {
	t.Helper()

	resp := mustParseJSON(t, data)

	valuesRaw, ok := resp["values"]
	if !ok {
		t.Fatal("response missing 'values' field")
	}

	valuesArr, ok := valuesRaw.([]any)
	if !ok {
		t.Fatalf("expected values to be an array, got %T", valuesRaw)
	}

	var result []map[string]any
	for i, entry := range valuesArr {
		obj, ok := entry.(map[string]any)
		if !ok {
			t.Fatalf("values[%d] is not an object", i)
		}
		if _, ok := obj["value"]; !ok {
			t.Fatalf("values[%d] missing 'value' field", i)
		}
		result = append(result, obj)
	}

	return result
}

// extractValueStrings extracts the "value" string from each entry in a values response.
func extractValueStrings(t *testing.T, entries []map[string]any) []string {
	t.Helper()
	var result []string
	for _, e := range entries {
		if v, ok := e["value"].(string); ok {
			result = append(result, v)
		}
	}
	return result
}

// containsString checks if a string slice contains the given string.
func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
