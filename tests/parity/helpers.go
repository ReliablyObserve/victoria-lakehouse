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
	vlBaseURL  = envOrDefault("VL_BASE_URL", "http://localhost:19428")
	lhBaseURL  = envOrDefault("LH_BASE_URL", "http://localhost:29428")
	vtBaseURL  = envOrDefault("VT_BASE_URL", "http://localhost:10428")
	lhtBaseURL = envOrDefault("LHT_BASE_URL", "http://localhost:20428")
)

var httpClient = &http.Client{Timeout: 60 * time.Second}
var shortClient = &http.Client{Timeout: 3 * time.Second}

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
		t.Fatalf("reading body from %s: %v", u, err)
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

func extractValuesStrings(data []byte) []string {
	lines := parseNDJSON(data)
	var vals []string
	for _, line := range lines {
		if v, ok := line["value"].(string); ok {
			vals = append(vals, v)
		}
	}
	if len(vals) > 0 {
		return vals
	}
	obj, err := parseJSON(data)
	if err != nil {
		return nil
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
