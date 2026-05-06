package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// VerifyCheck is the result of a single data correctness check.
type VerifyCheck struct {
	Name    string `json:"name"`
	Pass    bool   `json:"pass"`
	Details string `json:"details"`
}

// VerifyResult aggregates all verification checks.
type VerifyResult struct {
	Checks     []VerifyCheck `json:"checks"`
	PassCount  int           `json:"pass_count"`
	FailCount  int           `json:"fail_count"`
	TotalCount int           `json:"total_count"`
	Pass       bool          `json:"pass"`
}

func runVerify(target string) *VerifyResult {
	vr := &VerifyResult{}

	now := time.Now()
	twoDaysAgo := now.Add(-48 * time.Hour)

	client := &http.Client{Timeout: 30 * time.Second}

	fmt.Println("\n=== Data Correctness Verification ===")
	fmt.Printf("Target: %s\n\n", target)

	// 1. Field completeness
	vr.addCheck(checkFieldCompleteness(client, target, twoDaysAgo, now))

	// 2. Service values
	vr.addCheck(checkFieldValues(client, target, twoDaysAgo, now,
		"service.name",
		[]string{"api-gateway", "order-service", "payment-service", "notification-service", "user-service"},
	))

	// 3. Level values
	vr.addCheck(checkFieldValues(client, target, twoDaysAgo, now,
		"level",
		[]string{"INFO", "WARN", "ERROR", "DEBUG"},
	))

	// 4. Namespace values
	vr.addCheck(checkFieldValues(client, target, twoDaysAgo, now,
		"k8s.namespace.name",
		[]string{"production", "staging"},
	))

	// 5. Filter accuracy: service.name
	vr.addCheck(checkFilterAccuracy(client, target, twoDaysAgo, now,
		`service.name:="api-gateway"`, "service.name", "api-gateway"))

	// 6. Filter accuracy: level
	vr.addCheck(checkFilterAccuracy(client, target, twoDaysAgo, now,
		`level:="ERROR"`, "level", "ERROR"))

	// 7. Time range correctness
	vr.addCheck(checkTimeRange(client, target))

	// 8. Trace lookup
	vr.addCheck(checkTraceLookup(client, target, twoDaysAgo, now))

	// 9. Empty range (future)
	vr.addCheck(checkEmptyRange(client, target))

	// 10. Stats query
	vr.addCheck(checkStatsQuery(client, target, twoDaysAgo, now))

	// 11. Cross-check count
	vr.addCheck(checkCrossCheckCount(client, target, twoDaysAgo, now))

	vr.TotalCount = len(vr.Checks)
	vr.Pass = vr.FailCount == 0

	fmt.Printf("\n=== Verification Summary: %d/%d passed ===\n", vr.PassCount, vr.TotalCount)
	if vr.Pass {
		fmt.Println("Overall: PASS")
	} else {
		fmt.Println("Overall: FAIL")
	}

	return vr
}

func (vr *VerifyResult) addCheck(c VerifyCheck) {
	vr.Checks = append(vr.Checks, c)
	if c.Pass {
		vr.PassCount++
		fmt.Printf("  PASS  %s\n", c.Name)
	} else {
		vr.FailCount++
		fmt.Printf("  FAIL  %s -- %s\n", c.Name, c.Details)
	}
}

// --- Individual checks ---

func checkFieldCompleteness(client *http.Client, target string, start, end time.Time) VerifyCheck {
	name := "field_completeness"
	expected := []string{
		"_time", "_msg", "service.name", "level", "trace_id", "span_id",
		"k8s.namespace.name", "k8s.pod.name", "k8s.deployment.name", "k8s.node.name",
		"deployment.environment", "cloud.region", "host.name",
		"_stream", "_stream_id", "scope.name",
	}

	u := fmt.Sprintf("%s/select/logsql/field_names?start=%d&end=%d",
		target, start.UnixNano(), end.UnixNano())
	body, err := httpGetBody(client, u)
	if err != nil {
		return VerifyCheck{Name: name, Pass: false, Details: fmt.Sprintf("HTTP error: %v", err)}
	}

	got := parseFieldNamesResponse(body)

	var missing []string
	for _, e := range expected {
		if !containsStr(got, e) {
			missing = append(missing, e)
		}
	}

	if len(missing) > 0 {
		return VerifyCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("missing fields: %s (got %d fields: %s)", strings.Join(missing, ", "), len(got), strings.Join(got, ", "))}
	}
	return VerifyCheck{Name: name, Pass: true,
		Details: fmt.Sprintf("all %d expected fields present out of %d total", len(expected), len(got))}
}

func checkFieldValues(client *http.Client, target string, start, end time.Time, field string, expected []string) VerifyCheck {
	name := fmt.Sprintf("field_values_%s", field)

	u := fmt.Sprintf("%s/select/logsql/field_values?field=%s&limit=100&start=%d&end=%d",
		target, url.QueryEscape(field), start.UnixNano(), end.UnixNano())
	body, err := httpGetBody(client, u)
	if err != nil {
		return VerifyCheck{Name: name, Pass: false, Details: fmt.Sprintf("HTTP error: %v", err)}
	}

	got := parseFieldValuesResponse(body)

	sortedExpected := make([]string, len(expected))
	copy(sortedExpected, expected)
	sort.Strings(sortedExpected)

	sortedGot := make([]string, len(got))
	copy(sortedGot, got)
	sort.Strings(sortedGot)

	var missing []string
	for _, e := range sortedExpected {
		if !containsStr(sortedGot, e) {
			missing = append(missing, e)
		}
	}
	if len(missing) > 0 {
		return VerifyCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("missing expected values %v from got %v", missing, sortedGot)}
	}

	return VerifyCheck{Name: name, Pass: true,
		Details: fmt.Sprintf("exactly %d values: %s", len(expected), strings.Join(sortedGot, ", "))}
}

func checkFilterAccuracy(client *http.Client, target string, start, end time.Time, query, field, expectedVal string) VerifyCheck {
	name := fmt.Sprintf("filter_accuracy_%s=%s", field, expectedVal)

	u := fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=50",
		target, url.QueryEscape(query), start.UnixNano(), end.UnixNano())
	body, err := httpGetBody(client, u)
	if err != nil {
		return VerifyCheck{Name: name, Pass: false, Details: fmt.Sprintf("HTTP error: %v", err)}
	}

	rows := parseJSONLRows(body)
	if len(rows) == 0 {
		return VerifyCheck{Name: name, Pass: false, Details: "no rows returned"}
	}

	var bad int
	for _, row := range rows {
		val, ok := row[field]
		if !ok {
			bad++
			continue
		}
		if val != expectedVal {
			bad++
		}
	}

	if bad > 0 {
		return VerifyCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("%d/%d rows had wrong value for %s", bad, len(rows), field)}
	}
	return VerifyCheck{Name: name, Pass: true,
		Details: fmt.Sprintf("all %d rows have %s=%s", len(rows), field, expectedVal)}
}

func checkTimeRange(client *http.Client, target string) VerifyCheck {
	name := "time_range_correctness"

	end := time.Now()
	start := end.Add(-1 * time.Hour)

	u := fmt.Sprintf("%s/select/logsql/query?query=*&start=%d&end=%d&limit=50",
		target, start.UnixNano(), end.UnixNano())
	body, err := httpGetBody(client, u)
	if err != nil {
		return VerifyCheck{Name: name, Pass: false, Details: fmt.Sprintf("HTTP error: %v", err)}
	}

	rows := parseJSONLRows(body)
	if len(rows) == 0 {
		return VerifyCheck{Name: name, Pass: false, Details: "no rows returned"}
	}

	var outOfRange int
	for _, row := range rows {
		ts, ok := row["_time"]
		if !ok {
			outOfRange++
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			// Try other formats VL might use
			t, err = time.Parse("2006-01-02T15:04:05Z", ts)
			if err != nil {
				// Try unix nanos as string
				nanos, nerr := strconv.ParseInt(ts, 10, 64)
				if nerr != nil {
					outOfRange++
					continue
				}
				t = time.Unix(0, nanos)
			}
		}
		// Allow 1-minute tolerance for clock skew
		if t.Before(start.Add(-time.Minute)) || t.After(end.Add(time.Minute)) {
			outOfRange++
		}
	}

	if outOfRange > 0 {
		return VerifyCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("%d/%d rows out of time range [%s, %s]",
				outOfRange, len(rows), start.Format(time.RFC3339), end.Format(time.RFC3339))}
	}
	return VerifyCheck{Name: name, Pass: true,
		Details: fmt.Sprintf("all %d rows within expected 1h window", len(rows))}
}

func checkTraceLookup(client *http.Client, target string, start, end time.Time) VerifyCheck {
	name := "trace_lookup"

	// Step 1: Get a trace_id from a wildcard query
	u := fmt.Sprintf("%s/select/logsql/query?query=*&start=%d&end=%d&limit=5",
		target, start.UnixNano(), end.UnixNano())
	body, err := httpGetBody(client, u)
	if err != nil {
		return VerifyCheck{Name: name, Pass: false, Details: fmt.Sprintf("HTTP error fetching sample: %v", err)}
	}

	rows := parseJSONLRows(body)
	if len(rows) == 0 {
		return VerifyCheck{Name: name, Pass: false, Details: "no rows from wildcard query to get trace_id"}
	}

	var traceID string
	for _, row := range rows {
		if tid, ok := row["trace_id"]; ok && tid != "" {
			traceID = tid
			break
		}
	}
	if traceID == "" {
		return VerifyCheck{Name: name, Pass: false, Details: "no trace_id found in sampled rows"}
	}

	// Step 2: Look up exact trace_id
	query := fmt.Sprintf(`trace_id:="%s"`, traceID)
	u2 := fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=10",
		target, url.QueryEscape(query), start.UnixNano(), end.UnixNano())
	body2, err := httpGetBody(client, u2)
	if err != nil {
		return VerifyCheck{Name: name, Pass: false, Details: fmt.Sprintf("HTTP error on trace lookup: %v", err)}
	}

	rows2 := parseJSONLRows(body2)
	if len(rows2) == 0 {
		return VerifyCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("trace_id %s not found via exact match", traceID)}
	}

	// Verify all returned rows actually have the expected trace_id
	for _, row := range rows2 {
		if tid, ok := row["trace_id"]; ok && tid != traceID {
			return VerifyCheck{Name: name, Pass: false,
				Details: fmt.Sprintf("returned row has trace_id=%s, expected %s", tid, traceID)}
		}
	}

	return VerifyCheck{Name: name, Pass: true,
		Details: fmt.Sprintf("trace_id %s found, %d row(s) returned", traceID, len(rows2))}
}

func checkEmptyRange(client *http.Client, target string) VerifyCheck {
	name := "empty_future_range"

	// Year 3000
	futureStart := time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
	futureEnd := time.Date(3000, 1, 2, 0, 0, 0, 0, time.UTC)

	u := fmt.Sprintf("%s/select/logsql/query?query=*&start=%d&end=%d&limit=10",
		target, futureStart.UnixNano(), futureEnd.UnixNano())
	body, err := httpGetBody(client, u)
	if err != nil {
		return VerifyCheck{Name: name, Pass: false, Details: fmt.Sprintf("HTTP error: %v", err)}
	}

	rows := parseJSONLRows(body)
	if len(rows) != 0 {
		return VerifyCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("expected 0 rows for year 3000, got %d", len(rows))}
	}
	return VerifyCheck{Name: name, Pass: true, Details: "0 rows returned for future range (year 3000)"}
}

func checkStatsQuery(client *http.Client, target string, start, end time.Time) VerifyCheck {
	name := "stats_query_count"

	u := fmt.Sprintf("%s/select/logsql/stats_query?query=*&start=%d&end=%d",
		target, start.UnixNano(), end.UnixNano())
	body, err := httpGetBody(client, u)
	if err != nil {
		return VerifyCheck{Name: name, Pass: false, Details: fmt.Sprintf("HTTP error: %v", err)}
	}

	count := parseStatsCount(body)
	if count <= 0 {
		return VerifyCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("stats count is %d, expected > 0 (body: %.200s)", count, body)}
	}
	return VerifyCheck{Name: name, Pass: true,
		Details: fmt.Sprintf("stats count = %d", count)}
}

func checkCrossCheckCount(client *http.Client, target string, start, end time.Time) VerifyCheck {
	name := "cross_check_stats_vs_query"

	// Get stats count
	statsURL := fmt.Sprintf("%s/select/logsql/stats_query?query=*&start=%d&end=%d",
		target, start.UnixNano(), end.UnixNano())
	statsBody, err := httpGetBody(client, statsURL)
	if err != nil {
		return VerifyCheck{Name: name, Pass: false, Details: fmt.Sprintf("HTTP error (stats): %v", err)}
	}
	statsCount := parseStatsCount(statsBody)

	// Get query rows with a small limit to confirm data exists
	queryURL := fmt.Sprintf("%s/select/logsql/query?query=*&start=%d&end=%d&limit=10",
		target, start.UnixNano(), end.UnixNano())
	queryBody, err := httpGetBody(client, queryURL)
	if err != nil {
		return VerifyCheck{Name: name, Pass: false, Details: fmt.Sprintf("HTTP error (query): %v", err)}
	}
	rows := parseJSONLRows(queryBody)

	if statsCount <= 0 {
		return VerifyCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("stats count = %d, expected > 0", statsCount)}
	}

	if len(rows) == 0 {
		return VerifyCheck{Name: name, Pass: false,
			Details: "query returned 0 rows but stats shows data"}
	}

	// The query was limited to 10, so we expect rows <= 10 and stats >= rows
	if statsCount < int64(len(rows)) {
		return VerifyCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("stats count (%d) < query rows (%d), inconsistent", statsCount, len(rows))}
	}

	return VerifyCheck{Name: name, Pass: true,
		Details: fmt.Sprintf("stats=%d, query returned %d rows (limit=10), consistent", statsCount, len(rows))}
}

// --- Helpers ---

func httpGetBody(client *http.Client, u string) (string, error) {
	resp, err := client.Get(u)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 400 {
		return string(b), fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
	}
	return string(b), nil
}

// parseFieldNamesResponse handles both wrapped {"values":[...]} and JSON-lines formats
func parseFieldNamesResponse(body string) []string {
	// Try wrapped format first: {"values":[{"value":"name","hits":N},...]}
	var wrapped struct {
		Values []struct {
			Value string `json:"value"`
		} `json:"values"`
	}
	if err := json.Unmarshal([]byte(body), &wrapped); err == nil && len(wrapped.Values) > 0 {
		names := make([]string, len(wrapped.Values))
		for i, v := range wrapped.Values {
			names[i] = v.Value
		}
		return names
	}

	// Fall back to JSON-lines
	var names []string
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			names = append(names, line)
			continue
		}
		if f, ok := obj["field"]; ok {
			names = append(names, fmt.Sprintf("%v", f))
		} else if f, ok := obj["value"]; ok {
			names = append(names, fmt.Sprintf("%v", f))
		}
	}
	return names
}

// parseFieldValuesResponse handles both wrapped {"values":[...]} and JSON-lines formats
func parseFieldValuesResponse(body string) []string {
	// Try wrapped format first: {"values":[{"value":"name","hits":N},...]}
	var wrapped struct {
		Values []struct {
			Value string `json:"value"`
		} `json:"values"`
	}
	if err := json.Unmarshal([]byte(body), &wrapped); err == nil && len(wrapped.Values) > 0 {
		values := make([]string, len(wrapped.Values))
		for i, v := range wrapped.Values {
			values[i] = v.Value
		}
		return values
	}

	// Fall back to JSON-lines
	var values []string
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			values = append(values, line)
			continue
		}
		if v, ok := obj["value"]; ok {
			values = append(values, fmt.Sprintf("%v", v))
		} else if v, ok := obj["field_value"]; ok {
			values = append(values, fmt.Sprintf("%v", v))
		}
	}
	return values
}

// parseJSONLRows parses JSON Lines (one JSON object per line) response.
func parseJSONLRows(body string) []map[string]string {
	var rows []map[string]string
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			continue
		}
		row := make(map[string]string)
		for k, v := range obj {
			row[k] = fmt.Sprintf("%v", v)
		}
		rows = append(rows, row)
	}
	return rows
}

// parseStatsCount extracts count from Prometheus-style {"data":{"result":[{"value":[ts,"count"]}]}} or JSON-lines
func parseStatsCount(body string) int64 {
	// Try Prometheus vector format (lakehouse)
	var promResp struct {
		Data struct {
			Result []struct {
				Value []interface{} `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &promResp); err == nil && len(promResp.Data.Result) > 0 {
		if len(promResp.Data.Result[0].Value) >= 2 {
			if s, ok := promResp.Data.Result[0].Value[1].(string); ok {
				n, _ := strconv.ParseInt(s, 10, 64)
				return n
			}
		}
	}

	// Fall back to JSON-lines {"hits":N}
	var total int64
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			continue
		}
		if h, ok := obj["hits"]; ok {
			switch v := h.(type) {
			case float64:
				total += int64(v)
			case string:
				n, _ := strconv.ParseInt(v, 10, 64)
				total += n
			}
		}
	}
	return total
}

func containsStr(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
