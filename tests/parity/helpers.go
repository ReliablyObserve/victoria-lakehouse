//go:build parity

package parity

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

var (
	vlBaseURL  = envOrDefault("VL_BASE_URL", "http://localhost:39428")
	lhBaseURL  = envOrDefault("LH_BASE_URL", "http://localhost:39429")
	vtBaseURL  = envOrDefault("VT_BASE_URL", "http://localhost:39430")
	lhtBaseURL = envOrDefault("LHT_BASE_URL", "http://localhost:39431")
)

var httpClient = &http.Client{Timeout: 60 * time.Second}
var shortClient = &http.Client{Timeout: 3 * time.Second}

// seedWindowHours must match --hours-back in the datagen-seed service of
// tests/parity/docker-compose.yml. cmd/datagen places each trace at
// `now - rand[1..hours-back]h + rand(3600)s`, so the seeded rows sit
// anywhere in the last day and nothing at all lands in the last few
// minutes. Any test that asks for a short relative window (`_time:10m`)
// therefore reads an empty range on BOTH tiers and asserts nothing.
const seedWindowHours = 24

// seedWindowStart/seedWindowEnd bracket the seeded data with a margin on
// each side: datagen runs before the tests, and the tests themselves take
// minutes, so the window has to outlive both.
func seedWindowStart() time.Time {
	return time.Now().Add(-(seedWindowHours + 2) * time.Hour)
}

func seedWindowEnd() time.Time {
	return time.Now().Add(time.Hour)
}

// seedWindowParams returns the start/end query parameters covering the whole
// seeded window, in epoch nanoseconds.
func seedWindowParams() url.Values {
	return url.Values{
		"start": {strconv.FormatInt(seedWindowStart().UnixNano(), 10)},
		"end":   {strconv.FormatInt(seedWindowEnd().UnixNano(), 10)},
	}
}

// seedWindowFilter returns the same window as an inline LogsQL `_time:[...]`
// filter, for queries that carry their own time filter (join subqueries
// cannot inherit the request's start/end).
func seedWindowFilter() string {
	return fmt.Sprintf("_time:[%s, %s]",
		seedWindowStart().Format(time.RFC3339Nano),
		seedWindowEnd().Format(time.RFC3339Nano))
}

// requireNonEmptyReference fails when the reference tier returned nothing to
// compare against. Every set / row / bucket comparison is vacuously true
// against an empty reference, so a silent pass there means the seed or the
// query is broken — not that the two tiers agree. Fix the query; never
// relax this guard.
func requireNonEmptyReference(t *testing.T, mode CompareMode, n int, what string) {
	t.Helper()
	if n == 0 {
		t.Fatalf("%s: reference returned nothing (%s) — seed or query defect, "+
			"not parity: a comparison against an empty reference passes "+
			"vacuously. Fix the query or the seed rather than the comparison.",
			mode, what)
	}
}

type fetchResult struct {
	StatusCode int
	Body       []byte
}

func fetch(t *testing.T, baseURL, path string, params url.Values) fetchResult {
	return fetchWith(t, httpClient, baseURL, path, params)
}

func fetchShort(t *testing.T, baseURL, path string, params url.Values) fetchResult {
	return fetchWith(t, shortClient, baseURL, path, params)
}

func fetchWith(t *testing.T, client *http.Client, baseURL, path string, params url.Values) fetchResult {
	t.Helper()
	u := baseURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	resp, err := client.Get(u)
	if err != nil {
		return fetchResult{StatusCode: 0, Body: []byte(err.Error())}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		// A streaming endpoint (/select/logsql/tail) sends headers and then
		// never closes the body, so whether the client timeout lands on the
		// request or on the body read is a race — the same call flaked
		// between a clean StatusCode 0 and a t.Fatalf here. Both outcomes
		// mean "timed out" to the caller, so report them identically.
		return fetchResult{StatusCode: 0, Body: []byte(err.Error())}
	}
	return fetchResult{StatusCode: resp.StatusCode, Body: body}
}

func parseNDJSON(data []byte) []map[string]any {
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	var results []map[string]any
	for _, line := range lines {
		if line == "" {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			continue
		}
		results = append(results, obj)
	}
	return results
}

func parseJSON(data []byte) (map[string]any, error) {
	var obj map[string]any
	err := json.Unmarshal(data, &obj)
	return obj, err
}

func extractVectorCount(data []byte) (float64, error) {
	obj, err := parseJSON(data)
	if err != nil {
		return 0, fmt.Errorf("parse JSON: %w", err)
	}
	dataObj, _ := obj["data"].(map[string]any)
	if dataObj == nil {
		return 0, fmt.Errorf("missing data field")
	}
	result, _ := dataObj["result"].([]any)
	if len(result) == 0 {
		return 0, nil
	}
	first, _ := result[0].(map[string]any)
	if first == nil {
		return 0, fmt.Errorf("result[0] is not object")
	}
	value, _ := first["value"].([]any)
	if len(value) < 2 {
		return 0, fmt.Errorf("value array too short")
	}
	s, _ := value[1].(string)
	if s == "" {
		return 0, fmt.Errorf("value[1] is not string")
	}
	return strconv.ParseFloat(s, 64)
}

func extractCount(t *testing.T, data []byte) int {
	t.Helper()
	v, err := extractVectorCount(data)
	if err != nil {
		t.Fatalf("extractCount: %v", err)
	}
	return int(v)
}

// extractValuesStrings is extractValuesForField without a field hint. Use
// extractValuesForField whenever the query names the output column (any
// `| uniq by(x)` or `| fields x` pipe), because those rows carry no "value"
// key and would otherwise extract as an empty set — which compares equal to
// anything.
func extractValuesStrings(data []byte) []string {
	return extractValuesForField(data, "")
}

// extractValuesForField pulls the comparable value list out of a VL/VT
// response. It understands every shape the parity suite queries:
//
//	NDJSON rows            {"value":"x"}                 — field_values
//	NDJSON rows            {"level":"ERROR"}             — `| uniq by(level)`
//	{"values":[{"value"}]} — field_names / stream_field_*
//	{"data":["x", ...]}    — Jaeger services / operations
//	{"facets":[{"field_name":f,"values":[{"field_value":v}]}]}
//	[{...}, ...]           — /select/tenant_ids
//
// `field` names the column to read out of NDJSON rows. It is required for
// pipes that rename or project the output column; for a single-column row
// the sole key is used as a fallback so a missing hint degrades to the right
// answer instead of to silence.
func extractValuesForField(data []byte, field string) []string {
	var vals []string
	for _, row := range parseNDJSON(data) {
		if v, ok := rowValue(row, field); ok {
			vals = append(vals, v)
		}
	}
	if len(vals) > 0 {
		return vals
	}
	obj, err := parseJSON(data)
	if err != nil {
		// A top-level JSON array (/select/tenant_ids) is not an object.
		return extractArrayValues(data)
	}
	if dataArr, ok := obj["data"].([]any); ok {
		for _, entry := range dataArr {
			if s, ok := entry.(string); ok {
				vals = append(vals, s)
			}
		}
		if len(vals) > 0 {
			return vals
		}
	}
	if facets, ok := obj["facets"].([]any); ok {
		return extractFacetPairs(facets)
	}
	valuesRaw, _ := obj["values"].([]any)
	for _, entry := range valuesRaw {
		m, _ := entry.(map[string]any)
		if m == nil {
			continue
		}
		if v, ok := m["value"].(string); ok {
			vals = append(vals, v)
		}
	}
	return vals
}

// rowValue reads the comparable value out of one NDJSON row. Only scalars
// count: a whole-response envelope such as {"values": [...]} or
// {"facets": [...]} also parses as a single-key "row", and rendering its
// array as one string would turn the entire response into a single bogus
// set member.
func rowValue(row map[string]any, field string) (string, bool) {
	if field != "" {
		return scalarString(row[field])
	}
	if v, ok := row["value"]; ok {
		return scalarString(v)
	}
	// `| uniq by(x)` and `| fields x` produce single-column rows keyed by
	// the field the query named.
	if len(row) == 1 {
		for _, v := range row {
			return scalarString(v)
		}
	}
	return "", false
}

func scalarString(v any) (string, bool) {
	switch v.(type) {
	case nil, []any, map[string]any:
		return "", false
	}
	return fmt.Sprintf("%v", v), true
}

// extractFacetPairs flattens /select/logsql/facets into "field=value"
// strings. Hit counts are deliberately excluded: the two tiers count the
// same rows, so any difference in the pair set is a real content gap while a
// difference in hits alone would be noise.
func extractFacetPairs(facets []any) []string {
	var vals []string
	for _, entry := range facets {
		f, _ := entry.(map[string]any)
		if f == nil {
			continue
		}
		name, _ := f["field_name"].(string)
		values, _ := f["values"].([]any)
		for _, v := range values {
			m, _ := v.(map[string]any)
			if m == nil {
				continue
			}
			vals = append(vals, fmt.Sprintf("%s=%v", name, m["field_value"]))
		}
	}
	return vals
}

// extractArrayValues renders a top-level JSON array as comparable strings.
// Objects are rendered with sorted keys so two tiers that emit the same
// object in a different key order still compare equal.
func extractArrayValues(data []byte) []string {
	var arr []any
	if err := json.Unmarshal(data, &arr); err != nil {
		return nil
	}
	var vals []string
	for _, entry := range arr {
		switch v := entry.(type) {
		case string:
			vals = append(vals, v)
		case map[string]any:
			keys := make([]string, 0, len(v))
			for k := range v {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			parts := make([]string, 0, len(keys))
			for _, k := range keys {
				parts = append(parts, fmt.Sprintf("%s=%v", k, v[k]))
			}
			vals = append(vals, strings.Join(parts, ","))
		default:
			vals = append(vals, fmt.Sprintf("%v", v))
		}
	}
	return vals
}

func extractHitsBuckets(data []byte) (timestamps []string, counts []float64) {
	obj, _ := parseJSON(data)
	if obj == nil {
		return
	}
	hitsRaw, _ := obj["hits"].([]any)
	for _, entry := range hitsRaw {
		m, _ := entry.(map[string]any)
		if m == nil {
			continue
		}
		ts, _ := m["timestamps"].([]any)
		vs, _ := m["values"].([]any)
		for _, t := range ts {
			if s, ok := t.(string); ok {
				timestamps = append(timestamps, s)
			}
		}
		for _, v := range vs {
			if s, ok := v.(string); ok {
				f, _ := strconv.ParseFloat(s, 64)
				counts = append(counts, f)
			} else if f, ok := v.(float64); ok {
				counts = append(counts, f)
			}
		}
	}
	return
}

func sortedStrings(s []string) []string {
	cp := make([]string, len(s))
	copy(cp, s)
	sort.Strings(cp)
	return cp
}

func stringSet(s []string) map[string]bool {
	m := make(map[string]bool, len(s))
	for _, v := range s {
		m[v] = true
	}
	return m
}

func extractRowKeys(rows []map[string]any, skipFields []string) []string {
	skip := stringSet(skipFields)
	skip["_stream"] = true
	skip["_stream_id"] = true
	var keys []string
	for _, row := range rows {
		var parts []string
		timeStr, _ := row["_time"].(string)
		parts = append(parts, "t="+timeStr)
		if !skip["_msg"] {
			msgStr, _ := row["_msg"].(string)
			parts = append(parts, "m="+msgStr)
		}
		sortStart := len(parts)
		for k, v := range row {
			if skip[k] || k == "_time" || k == "_msg" {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(parts[sortStart:])
		keys = append(keys, strings.Join(parts, "|"))
	}
	sort.Strings(keys)
	return keys
}
