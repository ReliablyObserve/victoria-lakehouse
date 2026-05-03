//go:build e2e

package e2e

import (
	"strconv"
	"testing"
)

func TestStatsQuery_Count(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/stats_query", params)
	resp := assertVectorResponse(t, body)

	// Extract the count value
	data := resp["data"].(map[string]any)
	result := data["result"].([]any)
	first := result[0].(map[string]any)
	value := first["value"].([]any)
	countStr, ok := value[1].(string)
	if !ok {
		t.Fatalf("expected count as string, got %T: %v", value[1], value[1])
	}

	count, err := strconv.Atoi(countStr)
	if err != nil {
		t.Fatalf("cannot parse count %q: %v", countStr, err)
	}

	if count <= 0 {
		t.Fatalf("expected count > 0, got %d", count)
	}

	t.Logf("stats_query wildcard count: %d", count)
}

func TestStatsQuery_Filtered(t *testing.T) {
	// Get wildcard count
	wildcardParams := defaultTimeParams()
	wildcardParams.Set("query", "*")

	wildcardBody := httpGetBody(t, logsBaseURL, "/select/logsql/stats_query", wildcardParams)
	wildcardResp := assertVectorResponse(t, wildcardBody)
	wildcardData := wildcardResp["data"].(map[string]any)
	wildcardResult := wildcardData["result"].([]any)
	wildcardFirst := wildcardResult[0].(map[string]any)
	wildcardValue := wildcardFirst["value"].([]any)
	wildcardCountStr := wildcardValue[1].(string)
	wildcardCount, _ := strconv.Atoi(wildcardCountStr)

	// Get filtered count
	errorParams := defaultTimeParams()
	errorParams.Set("query", `level:="ERROR"`)

	errorBody := httpGetBody(t, logsBaseURL, "/select/logsql/stats_query", errorParams)
	errorResp := assertVectorResponse(t, errorBody)
	errorData := errorResp["data"].(map[string]any)
	errorResult := errorData["result"].([]any)
	errorFirst := errorResult[0].(map[string]any)
	errorValue := errorFirst["value"].([]any)
	errorCountStr := errorValue[1].(string)
	errorCount, _ := strconv.Atoi(errorCountStr)

	if errorCount >= wildcardCount {
		t.Errorf("expected ERROR count (%d) < wildcard count (%d)", errorCount, wildcardCount)
	}

	t.Logf("wildcard count: %d, ERROR count: %d", wildcardCount, errorCount)
}

func TestStatsQueryRange(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("step", "3600s")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/stats_query_range", params)

	resp := assertMatrixResponse(t, body)

	data := resp["data"].(map[string]any)
	resultType := data["resultType"].(string)

	if resultType != "matrix" {
		t.Fatalf("expected resultType=matrix, got %s", resultType)
	}

	result := data["result"].([]any)
	if len(result) == 0 {
		t.Fatal("expected at least one series in matrix result")
	}

	first := result[0].(map[string]any)
	values := first["values"].([]any)
	if len(values) == 0 {
		t.Fatal("expected at least one time bucket in matrix values")
	}

	t.Logf("stats_query_range: %d time buckets", len(values))
}
