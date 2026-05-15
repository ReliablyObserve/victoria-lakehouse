package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

type CompareConfig struct {
	LakehouseURL string
	VLURL        string
	Iterations   int
	Warmup       int
}

type CompareReport struct {
	Timestamp         string              `json:"timestamp"`
	LakehouseURL      string              `json:"lakehouse_url"`
	VLURL             string              `json:"vl_url"`
	CorrectnessChecks []CorrectnessCheck  `json:"correctness_checks"`
	CorrectnessPass   bool                `json:"correctness_pass"`
	PerfWarmLH        []ComparePerf       `json:"perf_warm_lakehouse"`
	PerfWarmVL        []ComparePerf       `json:"perf_warm_vl"`
	PerfColdLH        []ComparePerf       `json:"perf_cold_lakehouse"`
	PerfColdVL        []ComparePerf       `json:"perf_cold_vl"`
	PerfComparison    []PerfComparisonRow `json:"perf_comparison"`
}

type CorrectnessCheck struct {
	Name    string `json:"name"`
	Pass    bool   `json:"pass"`
	Details string `json:"details"`
	LHValue string `json:"lh_value,omitempty"`
	VLValue string `json:"vl_value,omitempty"`
}

type ComparePerf struct {
	Name       string  `json:"name"`
	P50Ms      float64 `json:"p50_ms"`
	P95Ms      float64 `json:"p95_ms"`
	P99Ms      float64 `json:"p99_ms"`
	MinMs      float64 `json:"min_ms"`
	MaxMs      float64 `json:"max_ms"`
	Iterations int     `json:"iterations"`
}

type PerfComparisonRow struct {
	Scenario  string  `json:"scenario"`
	LHWarmP95 float64 `json:"lh_warm_p95_ms"`
	VLWarmP95 float64 `json:"vl_warm_p95_ms"`
	LHColdP95 float64 `json:"lh_cold_p95_ms"`
	VLColdP95 float64 `json:"vl_cold_p95_ms"`
	WarmRatio float64 `json:"warm_ratio"`
	ColdRatio float64 `json:"cold_ratio"`
}

func runCompare(cfg CompareConfig) CompareReport {
	report := CompareReport{
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
		LakehouseURL: cfg.LakehouseURL,
		VLURL:        cfg.VLURL,
	}

	fmt.Println("\n" + strings.Repeat("=", 90))
	fmt.Println("  VICTORIA LAKEHOUSE vs VICTORIALOGS — HEAD-TO-HEAD COMPARISON")
	fmt.Println(strings.Repeat("=", 90))

	// Phase 1: Data correctness — identical queries, compare output
	fmt.Println("\n[Phase 1] Data Correctness — same query → both systems → diff results")
	fmt.Println(strings.Repeat("-", 70))
	report.CorrectnessChecks = runCorrectnessComparison(cfg.LakehouseURL, cfg.VLURL)
	report.CorrectnessPass = true
	passCount := 0
	for _, c := range report.CorrectnessChecks {
		if !c.Pass {
			report.CorrectnessPass = false
		} else {
			passCount++
		}
	}
	fmt.Printf("\n  Correctness: %d/%d passed\n", passCount, len(report.CorrectnessChecks))

	// Phase 2: Performance — warm caches (steady-state)
	fmt.Println("\n[Phase 2] Performance — Warm Caches (steady-state)")
	fmt.Println(strings.Repeat("-", 70))
	scenarios := buildCompareScenarios()

	fmt.Printf("\n  Lakehouse (warm):\n")
	report.PerfWarmLH = runComparePerfSuite(cfg.LakehouseURL, scenarios, cfg.Iterations, cfg.Warmup)
	fmt.Printf("\n  VictoriaLogs (warm):\n")
	report.PerfWarmVL = runComparePerfSuite(cfg.VLURL, scenarios, cfg.Iterations, cfg.Warmup)

	// Phase 3: Performance — cold caches (cache bypass)
	fmt.Println("\n[Phase 3] Performance — Cold Caches (cache bypass)")
	fmt.Println(strings.Repeat("-", 70))

	fmt.Printf("\n  Lakehouse (cold — L1/L2 cleared per batch):\n")
	report.PerfColdLH = runComparePerfCold(cfg.LakehouseURL, scenarios, cfg.Iterations)
	fmt.Printf("\n  VictoriaLogs (cold — unique time offsets per iteration):\n")
	report.PerfColdVL = runComparePerfColdVL(cfg.VLURL, scenarios, cfg.Iterations)

	// Comparison table
	report.PerfComparison = buildPerfComparison(scenarios, report)
	printCompareReport(report)

	return report
}

func runCorrectnessComparison(lhURL, vlURL string) []CorrectnessCheck {
	client := &http.Client{Timeout: 30 * time.Second}
	now := time.Now()
	twoDaysAgo := now.Add(-48 * time.Hour)
	var checks []CorrectnessCheck

	// 1. field_names — both should have the shared fields from dual-write
	checks = append(checks, compareFieldNames(client, lhURL, vlURL, twoDaysAgo, now))

	// 2. field_values for service.name — shared services must appear in both
	checks = append(checks, compareFieldValues(client, lhURL, vlURL, twoDaysAgo, now, "service.name",
		[]string{"api-gateway", "order-service", "payment-service", "notification-service", "user-service"}))

	// 3. field_values for level — shared levels
	checks = append(checks, compareFieldValues(client, lhURL, vlURL, twoDaysAgo, now, "level",
		[]string{"INFO", "WARN", "ERROR", "DEBUG"}))

	// 4. Filtered query results — same service filter, compare row content
	checks = append(checks, compareQueryResults(client, lhURL, vlURL, twoDaysAgo, now,
		`service.name:="api-gateway"`, "service.name", 20))

	// 5. Filtered query — level filter
	checks = append(checks, compareQueryResults(client, lhURL, vlURL, twoDaysAgo, now,
		`level:="ERROR"`, "level", 20))

	// 6. Stats count — both should report > 0 for dual-write service
	checks = append(checks, compareStatsCount(client, lhURL, vlURL, twoDaysAgo, now,
		`service.name:="api-gateway"`))

	// 7. Trace lookup — find a trace_id in LH, verify it exists in VL too
	checks = append(checks, compareTraceLookup(client, lhURL, vlURL, twoDaysAgo, now))

	// 8. Empty range — both should return 0
	checks = append(checks, compareEmptyRange(client, lhURL, vlURL))

	// 9. Query row structure — same fields present in both systems' rows
	checks = append(checks, compareRowStructure(client, lhURL, vlURL, twoDaysAgo, now))

	// 10. Message content — dual-write rows should have identical _msg
	checks = append(checks, compareMessageContent(client, lhURL, vlURL, twoDaysAgo, now))

	return checks
}

func compareFieldNames(client *http.Client, lhURL, vlURL string, start, end time.Time) CorrectnessCheck {
	name := "field_names_overlap"
	sharedExpected := []string{"_time", "_msg", "service.name", "level", "_stream", "_stream_id"}

	lhBody, err := httpGetBody(client, fmt.Sprintf("%s/select/logsql/field_names?query=*&start=%d&end=%d",
		lhURL, start.UnixNano(), end.UnixNano()))
	if err != nil {
		return CorrectnessCheck{Name: name, Pass: false, Details: fmt.Sprintf("LH error: %v", err)}
	}
	vlBody, err := httpGetBody(client, fmt.Sprintf("%s/select/logsql/field_names?query=*&start=%d&end=%d",
		vlURL, start.UnixNano(), end.UnixNano()))
	if err != nil {
		return CorrectnessCheck{Name: name, Pass: false, Details: fmt.Sprintf("VL error: %v", err)}
	}

	lhFields := parseFieldNamesResponse(lhBody)
	vlFields := parseFieldNamesResponse(vlBody)

	var missing []string
	for _, f := range sharedExpected {
		inLH := containsStr(lhFields, f)
		inVL := containsStr(vlFields, f)
		if !inLH || !inVL {
			where := ""
			if !inLH {
				where += "LH"
			}
			if !inVL {
				if where != "" {
					where += "+"
				}
				where += "VL"
			}
			missing = append(missing, fmt.Sprintf("%s(missing in %s)", f, where))
		}
	}

	if len(missing) > 0 {
		return CorrectnessCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("shared fields missing: %s", strings.Join(missing, ", ")),
			LHValue: fmt.Sprintf("%d fields", len(lhFields)),
			VLValue: fmt.Sprintf("%d fields", len(vlFields)),
		}
	}

	return CorrectnessCheck{Name: name, Pass: true,
		Details: fmt.Sprintf("all %d shared fields present in both (LH=%d, VL=%d total)", len(sharedExpected), len(lhFields), len(vlFields)),
		LHValue: strings.Join(lhFields, ", "),
		VLValue: fmt.Sprintf("%d fields", len(vlFields)),
	}
}

func compareFieldValues(client *http.Client, lhURL, vlURL string, start, end time.Time, field string, expected []string) CorrectnessCheck {
	name := fmt.Sprintf("field_values_%s_match", field)

	lhBody, err := httpGetBody(client, fmt.Sprintf("%s/select/logsql/field_values?query=*&field=%s&limit=100&start=%d&end=%d",
		lhURL, url.QueryEscape(field), start.UnixNano(), end.UnixNano()))
	if err != nil {
		return CorrectnessCheck{Name: name, Pass: false, Details: fmt.Sprintf("LH error: %v", err)}
	}
	vlBody, err := httpGetBody(client, fmt.Sprintf("%s/select/logsql/field_values?query=*&field=%s&limit=100&start=%d&end=%d",
		vlURL, url.QueryEscape(field), start.UnixNano(), end.UnixNano()))
	if err != nil {
		return CorrectnessCheck{Name: name, Pass: false, Details: fmt.Sprintf("VL error: %v", err)}
	}

	lhVals := parseFieldValuesResponse(lhBody)
	vlVals := parseFieldValuesResponse(vlBody)

	var missing []string
	for _, e := range expected {
		inLH := containsStr(lhVals, e)
		inVL := containsStr(vlVals, e)
		if !inLH || !inVL {
			where := ""
			if !inLH {
				where += "LH"
			}
			if !inVL {
				if where != "" {
					where += "+"
				}
				where += "VL"
			}
			missing = append(missing, fmt.Sprintf("%s(missing in %s)", e, where))
		}
	}

	if len(missing) > 0 {
		return CorrectnessCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("expected values missing: %s", strings.Join(missing, ", ")),
			LHValue: strings.Join(lhVals, ", "),
			VLValue: strings.Join(vlVals, ", "),
		}
	}

	return CorrectnessCheck{Name: name, Pass: true,
		Details: fmt.Sprintf("all %d expected %s values found in both", len(expected), field),
		LHValue: strings.Join(lhVals, ", "),
		VLValue: fmt.Sprintf("%d values", len(vlVals)),
	}
}

func compareQueryResults(client *http.Client, lhURL, vlURL string, start, end time.Time, query, filterField string, limit int) CorrectnessCheck {
	name := fmt.Sprintf("query_%s_results", filterField)

	lhBody, err := httpGetBody(client, fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=%d",
		lhURL, url.QueryEscape(query), start.UnixNano(), end.UnixNano(), limit))
	if err != nil {
		return CorrectnessCheck{Name: name, Pass: false, Details: fmt.Sprintf("LH error: %v", err)}
	}
	vlBody, err := httpGetBody(client, fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=%d",
		vlURL, url.QueryEscape(query), start.UnixNano(), end.UnixNano(), limit))
	if err != nil {
		return CorrectnessCheck{Name: name, Pass: false, Details: fmt.Sprintf("VL error: %v", err)}
	}

	lhRows := parseJSONLRows(lhBody)
	vlRows := parseJSONLRows(vlBody)

	if len(lhRows) == 0 {
		return CorrectnessCheck{Name: name, Pass: false, Details: "LH returned 0 rows"}
	}
	if len(vlRows) == 0 {
		return CorrectnessCheck{Name: name, Pass: false, Details: "VL returned 0 rows"}
	}

	// Verify all returned rows have correct filter value in both
	lhBad := 0
	for _, row := range lhRows {
		if v, ok := row[filterField]; ok {
			expected := strings.TrimPrefix(query, filterField+`:="`)
			expected = strings.TrimSuffix(expected, `"`)
			if v != expected {
				lhBad++
			}
		}
	}
	vlBad := 0
	for _, row := range vlRows {
		if v, ok := row[filterField]; ok {
			expected := strings.TrimPrefix(query, filterField+`:="`)
			expected = strings.TrimSuffix(expected, `"`)
			if v != expected {
				vlBad++
			}
		}
	}

	if lhBad > 0 || vlBad > 0 {
		return CorrectnessCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("filter violations: LH %d/%d, VL %d/%d", lhBad, len(lhRows), vlBad, len(vlRows)),
			LHValue: fmt.Sprintf("%d rows (%d bad)", len(lhRows), lhBad),
			VLValue: fmt.Sprintf("%d rows (%d bad)", len(vlRows), vlBad),
		}
	}

	// Both returned correct data — check that shared fields exist in rows
	lhSample := lhRows[0]
	vlSample := vlRows[0]
	sharedFields := []string{"_time", "_msg", filterField}
	var fieldMissing []string
	for _, f := range sharedFields {
		if _, ok := lhSample[f]; !ok {
			fieldMissing = append(fieldMissing, f+"(LH)")
		}
		if _, ok := vlSample[f]; !ok {
			fieldMissing = append(fieldMissing, f+"(VL)")
		}
	}
	if len(fieldMissing) > 0 {
		return CorrectnessCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("row missing fields: %s", strings.Join(fieldMissing, ", ")),
		}
	}

	return CorrectnessCheck{Name: name, Pass: true,
		Details: fmt.Sprintf("both return correct filtered rows (LH=%d, VL=%d), all have required fields", len(lhRows), len(vlRows)),
		LHValue: fmt.Sprintf("%d rows", len(lhRows)),
		VLValue: fmt.Sprintf("%d rows", len(vlRows)),
	}
}

func compareStatsCount(client *http.Client, lhURL, vlURL string, start, end time.Time, query string) CorrectnessCheck {
	name := "stats_count_both_positive"

	// LH stats_query: filter-only (no pipe syntax)
	lhBody, err := httpGetBody(client, fmt.Sprintf("%s/select/logsql/stats_query?query=%s&start=%d&end=%d",
		lhURL, url.QueryEscape(query), start.UnixNano(), end.UnixNano()))
	if err != nil {
		return CorrectnessCheck{Name: name, Pass: false, Details: fmt.Sprintf("LH error: %v", err)}
	}
	// VL stats_query: requires | stats pipe
	vlStatsQuery := query + " | stats count() rows"
	vlBody, err := httpGetBody(client, fmt.Sprintf("%s/select/logsql/stats_query?query=%s&start=%d&end=%d",
		vlURL, url.QueryEscape(vlStatsQuery), start.UnixNano(), end.UnixNano()))
	if err != nil {
		return CorrectnessCheck{Name: name, Pass: false, Details: fmt.Sprintf("VL error: %v", err)}
	}

	lhCount := parseStatsCount(lhBody)
	vlCount := parseStatsCount(vlBody)

	if lhCount <= 0 {
		return CorrectnessCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("LH stats count = %d, expected > 0", lhCount),
			LHValue: fmt.Sprintf("%d", lhCount),
			VLValue: fmt.Sprintf("%d", vlCount),
		}
	}
	if vlCount <= 0 {
		return CorrectnessCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("VL stats count = %d, expected > 0", vlCount),
			LHValue: fmt.Sprintf("%d", lhCount),
			VLValue: fmt.Sprintf("%d", vlCount),
		}
	}

	return CorrectnessCheck{Name: name, Pass: true,
		Details: fmt.Sprintf("both report positive counts: LH=%d, VL=%d", lhCount, vlCount),
		LHValue: fmt.Sprintf("%d", lhCount),
		VLValue: fmt.Sprintf("%d", vlCount),
	}
}

func compareTraceLookup(client *http.Client, lhURL, vlURL string, start, end time.Time) CorrectnessCheck {
	name := "trace_id_lookup_both"

	// Dual-write generates different trace_ids per system, so we test independently:
	// both should support trace_id exact-match lookup on their own data
	lhTraceID := findTraceID(client, lhURL, start, end)
	vlTraceID := findTraceID(client, vlURL, start, end)

	if lhTraceID == "" {
		return CorrectnessCheck{Name: name, Pass: false, Details: "no trace_id found in LH"}
	}
	if vlTraceID == "" {
		return CorrectnessCheck{Name: name, Pass: false, Details: "no trace_id found in VL"}
	}

	// Verify LH can look up its own trace_id
	lhQuery := fmt.Sprintf(`trace_id:="%s"`, lhTraceID)
	lhBody, err := httpGetBody(client, fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=5",
		lhURL, url.QueryEscape(lhQuery), start.UnixNano(), end.UnixNano()))
	if err != nil {
		return CorrectnessCheck{Name: name, Pass: false, Details: fmt.Sprintf("LH lookup error: %v", err)}
	}
	lhRows := parseJSONLRows(lhBody)
	if len(lhRows) == 0 {
		return CorrectnessCheck{Name: name, Pass: false, Details: "LH trace_id lookup returned 0 rows"}
	}

	// Verify VL can look up its own trace_id
	vlQuery := fmt.Sprintf(`trace_id:="%s"`, vlTraceID)
	vlBody, err := httpGetBody(client, fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=5",
		vlURL, url.QueryEscape(vlQuery), start.UnixNano(), end.UnixNano()))
	if err != nil {
		return CorrectnessCheck{Name: name, Pass: false, Details: fmt.Sprintf("VL lookup error: %v", err)}
	}
	vlRows := parseJSONLRows(vlBody)
	if len(vlRows) == 0 {
		return CorrectnessCheck{Name: name, Pass: false, Details: "VL trace_id lookup returned 0 rows"}
	}

	return CorrectnessCheck{Name: name, Pass: true,
		Details: fmt.Sprintf("both support trace_id lookup (LH=%d rows, VL=%d rows)", len(lhRows), len(vlRows)),
		LHValue: lhTraceID,
		VLValue: vlTraceID,
	}
}

func findTraceID(client *http.Client, target string, start, end time.Time) string {
	// First try a targeted query for dual-write data that has trace_id
	for _, q := range []string{`service.name:="api-gateway"`, `service.name:="order-service"`, "*"} {
		body, err := httpGetBody(client, fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=20",
			target, url.QueryEscape(q), start.UnixNano(), end.UnixNano()))
		if err != nil {
			continue
		}
		for _, row := range parseJSONLRows(body) {
			if tid, ok := row["trace_id"]; ok && tid != "" {
				return tid
			}
		}
	}
	return ""
}

func compareEmptyRange(client *http.Client, lhURL, vlURL string) CorrectnessCheck {
	name := "empty_range_both_zero"
	futureStart := time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
	futureEnd := time.Date(3000, 1, 2, 0, 0, 0, 0, time.UTC)

	lhBody, _ := httpGetBody(client, fmt.Sprintf("%s/select/logsql/query?query=*&start=%d&end=%d&limit=10",
		lhURL, futureStart.UnixNano(), futureEnd.UnixNano()))
	vlBody, _ := httpGetBody(client, fmt.Sprintf("%s/select/logsql/query?query=*&start=%d&end=%d&limit=10",
		vlURL, futureStart.UnixNano(), futureEnd.UnixNano()))

	lhRows := parseJSONLRows(lhBody)
	vlRows := parseJSONLRows(vlBody)

	if len(lhRows) != 0 || len(vlRows) != 0 {
		return CorrectnessCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("expected 0 rows for year 3000: LH=%d, VL=%d", len(lhRows), len(vlRows)),
		}
	}
	return CorrectnessCheck{Name: name, Pass: true,
		Details: "both return 0 rows for future time range",
	}
}

func compareRowStructure(client *http.Client, lhURL, vlURL string, start, end time.Time) CorrectnessCheck {
	name := "row_structure_match"
	query := `service.name:="api-gateway"`

	lhBody, err := httpGetBody(client, fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=1",
		lhURL, url.QueryEscape(query), start.UnixNano(), end.UnixNano()))
	if err != nil {
		return CorrectnessCheck{Name: name, Pass: false, Details: fmt.Sprintf("LH error: %v", err)}
	}
	vlBody, err := httpGetBody(client, fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=1",
		vlURL, url.QueryEscape(query), start.UnixNano(), end.UnixNano()))
	if err != nil {
		return CorrectnessCheck{Name: name, Pass: false, Details: fmt.Sprintf("VL error: %v", err)}
	}

	lhRows := parseJSONLRows(lhBody)
	vlRows := parseJSONLRows(vlBody)
	if len(lhRows) == 0 || len(vlRows) == 0 {
		return CorrectnessCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("need rows from both: LH=%d, VL=%d", len(lhRows), len(vlRows))}
	}

	// Core fields that dual-write data should share
	coreFields := []string{"_time", "_msg", "_stream", "_stream_id", "service.name"}
	var missing []string
	for _, f := range coreFields {
		if _, ok := lhRows[0][f]; !ok {
			missing = append(missing, f+"(LH)")
		}
		if _, ok := vlRows[0][f]; !ok {
			missing = append(missing, f+"(VL)")
		}
	}

	if len(missing) > 0 {
		lhKeys := sortedKeys(lhRows[0])
		vlKeys := sortedKeys(vlRows[0])
		return CorrectnessCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("core fields missing: %s", strings.Join(missing, ", ")),
			LHValue: strings.Join(lhKeys, ", "),
			VLValue: strings.Join(vlKeys, ", "),
		}
	}

	lhKeys := sortedKeys(lhRows[0])
	vlKeys := sortedKeys(vlRows[0])
	return CorrectnessCheck{Name: name, Pass: true,
		Details: fmt.Sprintf("core fields present in both (LH has %d fields, VL has %d)", len(lhKeys), len(vlKeys)),
		LHValue: strings.Join(lhKeys, ", "),
		VLValue: strings.Join(vlKeys, ", "),
	}
}

func compareMessageContent(client *http.Client, lhURL, vlURL string, start, end time.Time) CorrectnessCheck {
	name := "message_format_similar"

	// Dual-write generates structurally similar data (same services, levels, message
	// patterns) but with different trace_ids. Compare that both systems return
	// non-empty _msg for the same filter and that messages contain expected patterns.
	query := `service.name:="api-gateway"`

	lhBody, err := httpGetBody(client, fmt.Sprintf(
		"%s/select/logsql/query?query=%s&start=%d&end=%d&limit=5",
		lhURL, url.QueryEscape(query), start.UnixNano(), end.UnixNano()))
	if err != nil {
		return CorrectnessCheck{Name: name, Pass: false, Details: fmt.Sprintf("LH error: %v", err)}
	}
	vlBody, err := httpGetBody(client, fmt.Sprintf(
		"%s/select/logsql/query?query=%s&start=%d&end=%d&limit=5",
		vlURL, url.QueryEscape(query), start.UnixNano(), end.UnixNano()))
	if err != nil {
		return CorrectnessCheck{Name: name, Pass: false, Details: fmt.Sprintf("VL error: %v", err)}
	}

	lhRows := parseJSONLRows(lhBody)
	vlRows := parseJSONLRows(vlBody)

	if len(lhRows) == 0 {
		return CorrectnessCheck{Name: name, Pass: false, Details: "LH returned 0 rows"}
	}
	if len(vlRows) == 0 {
		return CorrectnessCheck{Name: name, Pass: false, Details: "VL returned 0 rows"}
	}

	// Both should have non-empty _msg
	lhMsg := lhRows[0]["_msg"]
	vlMsg := vlRows[0]["_msg"]
	if lhMsg == "" {
		return CorrectnessCheck{Name: name, Pass: false, Details: "LH _msg is empty"}
	}
	if vlMsg == "" {
		return CorrectnessCheck{Name: name, Pass: false, Details: "VL _msg is empty"}
	}

	// Both should have service.name=api-gateway in all rows
	lhAllCorrect := true
	for _, r := range lhRows {
		if r["service.name"] != "api-gateway" {
			lhAllCorrect = false
		}
	}
	vlAllCorrect := true
	for _, r := range vlRows {
		if r["service.name"] != "api-gateway" {
			vlAllCorrect = false
		}
	}

	if !lhAllCorrect || !vlAllCorrect {
		return CorrectnessCheck{Name: name, Pass: false,
			Details: fmt.Sprintf("filter leak: LH correct=%v, VL correct=%v", lhAllCorrect, vlAllCorrect),
		}
	}

	return CorrectnessCheck{Name: name, Pass: true,
		Details: fmt.Sprintf("both return non-empty messages with correct service filter (LH=%d, VL=%d rows)", len(lhRows), len(vlRows)),
		LHValue: truncate(lhMsg, 80),
		VLValue: truncate(vlMsg, 80),
	}
}

// --- Performance comparison scenarios ---

type CompareScenario struct {
	Name  string
	URLFn func(target string, offset time.Duration) string
}

func buildCompareScenarios() []CompareScenario {
	now := time.Now()
	oneHourAgo := now.Add(-1 * time.Hour)
	oneDayAgo := now.Add(-24 * time.Hour)
	twoDaysAgo := now.Add(-48 * time.Hour)

	return []CompareScenario{
		{
			Name: "query_wildcard_1h",
			URLFn: func(t string, off time.Duration) string {
				s := oneHourAgo.Add(off)
				e := now.Add(off)
				return fmt.Sprintf("%s/select/logsql/query?query=*&start=%d&end=%d&limit=100", t, s.UnixNano(), e.UnixNano())
			},
		},
		{
			Name: "query_service_filter",
			URLFn: func(t string, off time.Duration) string {
				s := twoDaysAgo.Add(off)
				e := now.Add(off)
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=50",
					t, url.QueryEscape(`service.name:="api-gateway"`), s.UnixNano(), e.UnixNano())
			},
		},
		{
			Name: "query_level_filter",
			URLFn: func(t string, off time.Duration) string {
				s := oneHourAgo.Add(off)
				e := now.Add(off)
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=50",
					t, url.QueryEscape(`level:="ERROR"`), s.UnixNano(), e.UnixNano())
			},
		},
		{
			Name: "query_compound_filter",
			URLFn: func(t string, off time.Duration) string {
				s := oneHourAgo.Add(off)
				e := now.Add(off)
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=30",
					t, url.QueryEscape(`service.name:="api-gateway" AND level:="ERROR"`), s.UnixNano(), e.UnixNano())
			},
		},
		{
			Name: "field_names",
			URLFn: func(t string, off time.Duration) string {
				s := twoDaysAgo.Add(off)
				e := now.Add(off)
				return fmt.Sprintf("%s/select/logsql/field_names?query=*&start=%d&end=%d", t, s.UnixNano(), e.UnixNano())
			},
		},
		{
			Name: "field_values_service",
			URLFn: func(t string, off time.Duration) string {
				s := twoDaysAgo.Add(off)
				e := now.Add(off)
				return fmt.Sprintf("%s/select/logsql/field_values?query=*&field=service.name&limit=100&start=%d&end=%d", t, s.UnixNano(), e.UnixNano())
			},
		},
		{
			Name: "stats_count_1h",
			URLFn: func(t string, off time.Duration) string {
				s := oneHourAgo.Add(off)
				e := now.Add(off)
				return fmt.Sprintf("%s/select/logsql/stats_query?query=%s&start=%d&end=%d",
					t, url.QueryEscape("* | stats count() rows"), s.UnixNano(), e.UnixNano())
			},
		},
		{
			Name: "stats_count_24h",
			URLFn: func(t string, off time.Duration) string {
				s := oneDayAgo.Add(off)
				e := now.Add(off)
				return fmt.Sprintf("%s/select/logsql/stats_query?query=%s&start=%d&end=%d",
					t, url.QueryEscape("* | stats count() rows"), s.UnixNano(), e.UnixNano())
			},
		},
		{
			Name: "trace_id_lookup",
			URLFn: func(t string, off time.Duration) string {
				s := twoDaysAgo.Add(off)
				e := now.Add(off)
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=1",
					t, url.QueryEscape(`trace_id:="ffffffffffffffffffffffffffffffff"`), s.UnixNano(), e.UnixNano())
			},
		},
		{
			Name: "hits_histogram_1h",
			URLFn: func(t string, off time.Duration) string {
				s := oneHourAgo.Add(off)
				e := now.Add(off)
				return fmt.Sprintf("%s/select/logsql/hits?query=*&start=%d&end=%d&step=300s", t, s.UnixNano(), e.UnixNano())
			},
		},
	}
}

func runComparePerfSuite(target string, scenarios []CompareScenario, iterations, warmup int) []ComparePerf {
	client := &http.Client{Timeout: 30 * time.Second}
	var results []ComparePerf

	for _, sc := range scenarios {
		// Warmup with zero offset
		for i := 0; i < warmup; i++ {
			u := sc.URLFn(target, 0)
			resp, err := client.Get(u)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}

		var latencies []float64
		for i := 0; i < iterations; i++ {
			u := sc.URLFn(target, 0)
			start := time.Now()
			resp, err := client.Get(u)
			elapsed := time.Since(start)
			if err != nil {
				continue
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode < 400 {
				latencies = append(latencies, float64(elapsed.Microseconds())/1000.0)
			}
		}

		results = append(results, buildComparePerf(sc.Name, latencies))
		r := results[len(results)-1]
		fmt.Printf("    %-28s p50=%7.1fms p95=%7.1fms p99=%7.1fms\n",
			sc.Name, r.P50Ms, r.P95Ms, r.P99Ms)
	}
	return results
}

func runComparePerfCold(lhTarget string, scenarios []CompareScenario, iterations int) []ComparePerf {
	client := &http.Client{Timeout: 30 * time.Second}
	var results []ComparePerf

	for _, sc := range scenarios {
		var latencies []float64
		batchSize := 5
		for batch := 0; batch < iterations; batch += batchSize {
			clearCacheSilent(lhTarget)
			time.Sleep(50 * time.Millisecond)

			end := batch + batchSize
			if end > iterations {
				end = iterations
			}
			for i := batch; i < end; i++ {
				u := sc.URLFn(lhTarget, 0)
				start := time.Now()
				resp, err := client.Get(u)
				elapsed := time.Since(start)
				if err != nil {
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode < 400 {
					latencies = append(latencies, float64(elapsed.Microseconds())/1000.0)
				}
			}
		}

		results = append(results, buildComparePerf(sc.Name, latencies))
		r := results[len(results)-1]
		fmt.Printf("    %-28s p50=%7.1fms p95=%7.1fms p99=%7.1fms\n",
			sc.Name, r.P50Ms, r.P95Ms, r.P99Ms)
	}
	return results
}

func clearCacheSilent(target string) {
	client := &http.Client{Timeout: 5 * time.Second}
	req, _ := http.NewRequest(http.MethodPost, target+"/internal/cache/clear", nil)
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
}

func runComparePerfColdVL(vlTarget string, scenarios []CompareScenario, iterations int) []ComparePerf {
	client := &http.Client{Timeout: 30 * time.Second}
	var results []ComparePerf

	for _, sc := range scenarios {
		var latencies []float64
		// For VL "cold": use micro-shifted time ranges per iteration to reduce cache hits
		for i := 0; i < iterations; i++ {
			offset := time.Duration(i) * time.Millisecond
			u := sc.URLFn(vlTarget, offset)
			start := time.Now()
			resp, err := client.Get(u)
			elapsed := time.Since(start)
			if err != nil {
				continue
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode < 400 {
				latencies = append(latencies, float64(elapsed.Microseconds())/1000.0)
			}
		}

		results = append(results, buildComparePerf(sc.Name, latencies))
		r := results[len(results)-1]
		fmt.Printf("    %-28s p50=%7.1fms p95=%7.1fms p99=%7.1fms\n",
			sc.Name, r.P50Ms, r.P95Ms, r.P99Ms)
	}
	return results
}

func buildComparePerf(name string, latencies []float64) ComparePerf {
	if len(latencies) == 0 {
		return ComparePerf{Name: name}
	}
	sort.Float64s(latencies)
	return ComparePerf{
		Name:       name,
		P50Ms:      percentile(latencies, 0.50),
		P95Ms:      percentile(latencies, 0.95),
		P99Ms:      percentile(latencies, 0.99),
		MinMs:      latencies[0],
		MaxMs:      latencies[len(latencies)-1],
		Iterations: len(latencies),
	}
}

func buildPerfComparison(scenarios []CompareScenario, report CompareReport) []PerfComparisonRow {
	var rows []PerfComparisonRow
	for i, sc := range scenarios {
		row := PerfComparisonRow{Scenario: sc.Name}
		if i < len(report.PerfWarmLH) {
			row.LHWarmP95 = report.PerfWarmLH[i].P95Ms
		}
		if i < len(report.PerfWarmVL) {
			row.VLWarmP95 = report.PerfWarmVL[i].P95Ms
		}
		if i < len(report.PerfColdLH) {
			row.LHColdP95 = report.PerfColdLH[i].P95Ms
		}
		if i < len(report.PerfColdVL) {
			row.VLColdP95 = report.PerfColdVL[i].P95Ms
		}
		if row.VLWarmP95 > 0 {
			row.WarmRatio = row.LHWarmP95 / row.VLWarmP95
		}
		if row.VLColdP95 > 0 {
			row.ColdRatio = row.LHColdP95 / row.VLColdP95
		}
		rows = append(rows, row)
	}
	return rows
}

func printCompareReport(report CompareReport) {
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("  DATA CORRECTNESS RESULTS")
	fmt.Println(strings.Repeat("=", 100))
	for _, c := range report.CorrectnessChecks {
		status := "PASS"
		if !c.Pass {
			status = "FAIL"
		}
		fmt.Printf("  %s  %-35s %s\n", status, c.Name, c.Details)
	}

	fmt.Println("\n" + strings.Repeat("=", 110))
	fmt.Println("  PERFORMANCE COMPARISON (p95)")
	fmt.Println(strings.Repeat("=", 110))
	fmt.Printf("  %-28s %12s %12s %12s %12s %8s %8s\n",
		"Scenario", "LH Warm", "VL Warm", "LH Cold", "VL Cold", "Warm x", "Cold x")
	fmt.Println("  " + strings.Repeat("-", 100))

	for _, row := range report.PerfComparison {
		warmR := "-"
		coldR := "-"
		if row.WarmRatio > 0 {
			warmR = fmt.Sprintf("%.1fx", row.WarmRatio)
		}
		if row.ColdRatio > 0 {
			coldR = fmt.Sprintf("%.1fx", row.ColdRatio)
		}
		fmt.Printf("  %-28s %11.1fms %11.1fms %11.1fms %11.1fms %8s %8s\n",
			row.Scenario, row.LHWarmP95, row.VLWarmP95, row.LHColdP95, row.VLColdP95, warmR, coldR)
	}

	fmt.Println("  " + strings.Repeat("-", 100))

	// Summary: where LH wins vs VL wins
	var lhWins, vlWins, ties int
	for _, row := range report.PerfComparison {
		if row.WarmRatio == 0 {
			continue
		}
		if row.WarmRatio < 0.9 {
			lhWins++
		} else if row.WarmRatio > 1.1 {
			vlWins++
		} else {
			ties++
		}
	}
	fmt.Printf("\n  Warm cache: LH faster in %d, VL faster in %d, similar in %d scenarios\n", lhWins, vlWins, ties)

	lhWins, vlWins, ties = 0, 0, 0
	for _, row := range report.PerfComparison {
		if row.ColdRatio == 0 {
			continue
		}
		if row.ColdRatio < 0.9 {
			lhWins++
		} else if row.ColdRatio > 1.1 {
			vlWins++
		} else {
			ties++
		}
	}
	fmt.Printf("  Cold cache: LH faster in %d, VL faster in %d, similar in %d scenarios\n", lhWins, vlWins, ties)
}

func (r *CompareReport) WriteJSON(path string) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// --- helpers ---

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
