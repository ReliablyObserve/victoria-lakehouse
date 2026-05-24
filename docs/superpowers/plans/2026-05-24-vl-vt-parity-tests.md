# VL/VT Parity Test Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Comprehensive LogsQL API parity testing between Victoria Lakehouse and VictoriaLogs/VictoriaTraces as reference implementations — 115 test cases across 9 families covering all endpoints, filters, pipes, stats, time ranges, field metadata, response formats, edge cases, and traces.

**Architecture:** Dual-write parity testing. A dedicated lightweight Docker Compose stack dual-writes identical data to both LH and VL/VT. A Go test binary sends identical queries to both systems and diffs results using configurable comparison modes (count_equal, set_equal, rows_match, etc.).

**Tech Stack:** Go 1.24, Docker Compose, `//go:build parity` build tag, existing datagen for data seeding

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Create | `tests/parity/docker-compose.yml` | Lightweight 6-service stack (minio, VL, LH-logs, VT, LH-traces, datagen) |
| Create | `tests/parity/helpers.go` | HTTP client, response parsers, comparison engine, diff logic |
| Create | `tests/parity/parity_test.go` | ParityCase type, RunParity test runner, time param helpers |
| Create | `tests/parity/logs_endpoints_test.go` | Family 1: 14 endpoint smoke tests |
| Create | `tests/parity/logs_filters_test.go` | Family 2: 25 filter type tests |
| Create | `tests/parity/logs_pipes_test.go` | Family 3: 20 pipe operation tests |
| Create | `tests/parity/logs_stats_test.go` | Family 4: 15 stats query tests |
| Create | `tests/parity/logs_timerange_test.go` | Family 5: 10 time range tests |
| Create | `tests/parity/logs_fields_test.go` | Family 6: 10 field metadata tests |
| Create | `tests/parity/logs_response_test.go` | Family 7: 10 response format tests |
| Create | `tests/parity/logs_edge_test.go` | Family 8: 10 edge case tests |
| Create | `tests/parity/traces_parity_test.go` | Traces: 15 Jaeger API + LogsQL tests |

---

### Task 1: Docker Compose Stack

**Files:**
- Create: `tests/parity/docker-compose.yml`

- [ ] **Step 1: Create the docker-compose.yml**

```yaml
# tests/parity/docker-compose.yml
name: parity

networks:
  parity-net:
    driver: bridge

services:
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    ports:
      - "19000:9000"
    networks: [parity-net]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      timeout: 5s
      retries: 5

  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    networks: [parity-net]
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin &&
      mc mb local/parity-bucket --ignore-existing &&
      echo 'MinIO bucket ready'
      "

  victorialogs:
    image: victoriametrics/victoria-logs:v1.50.0
    command:
      - "-storageDataPath=/data"
      - "-retentionPeriod=48h"
      - "-loggerLevel=INFO"
    networks: [parity-net]
    ports:
      - "19428:9428"
    volumes:
      - vl-data:/data
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://127.0.0.1:9428/health"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 5s

  lakehouse-logs:
    build:
      context: ../../
      dockerfile: Dockerfile.logs
    depends_on:
      minio-init:
        condition: service_completed_successfully
    ports:
      - "29428:9428"
    networks: [parity-net]
    command:
      - "-lakehouse.s3.bucket=parity-bucket"
      - "-lakehouse.s3.endpoint=http://minio:9000"
      - "-lakehouse.s3.access-key=minioadmin"
      - "-lakehouse.s3.secret-key=minioadmin"
      - "-lakehouse.s3.force-path-style=true"
      - "-lakehouse.manifest.refresh-interval=5s"
      - "-lakehouse.insert.flush-interval=5s"
      - "-lakehouse.compaction.leader-election=s3"
      - "-lakehouse.compaction.interval=15s"
      - "-lakehouse.query.file-workers=64"
      - "-lakehouse.cache.memory-mb=256"
      - "-loggerLevel=INFO"
    healthcheck:
      test: ["CMD", "/usr/local/bin/healthcheck", "http://localhost:9428/health"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 10s

  victoriatraces:
    image: victoriametrics/victoria-traces:v0.9.0
    command:
      - "-storageDataPath=/data"
      - "-retentionPeriod=48h"
      - "-loggerLevel=INFO"
    networks: [parity-net]
    ports:
      - "10428:10428"
    volumes:
      - vt-data:/data
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://127.0.0.1:10428/health"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 10s

  lakehouse-traces:
    build:
      context: ../../
      dockerfile: Dockerfile.traces
    depends_on:
      minio-init:
        condition: service_completed_successfully
    ports:
      - "20428:10428"
    networks: [parity-net]
    command:
      - "-lakehouse.s3.bucket=parity-bucket"
      - "-lakehouse.s3.endpoint=http://minio:9000"
      - "-lakehouse.s3.access-key=minioadmin"
      - "-lakehouse.s3.secret-key=minioadmin"
      - "-lakehouse.s3.force-path-style=true"
      - "-lakehouse.manifest.refresh-interval=5s"
      - "-lakehouse.insert.flush-interval=5s"
      - "-lakehouse.compaction.leader-election=s3"
      - "-lakehouse.compaction.interval=15s"
      - "-lakehouse.query.file-workers=64"
      - "-lakehouse.cache.memory-mb=256"
      - "-loggerLevel=INFO"
    healthcheck:
      test: ["CMD", "/usr/local/bin/healthcheck", "http://localhost:10428/health"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 10s

  datagen-seed:
    build:
      context: ../../
      dockerfile: Dockerfile.datagen
    depends_on:
      victorialogs:
        condition: service_healthy
      victoriatraces:
        condition: service_healthy
      lakehouse-logs:
        condition: service_healthy
      lakehouse-traces:
        condition: service_healthy
    networks: [parity-net]
    command:
      - "--logs=10000"
      - "--traces=2000"
      - "--hours-back=24"
      - "--vl-endpoint=http://victorialogs:9428"
      - "--vt-endpoint=http://victoriatraces:10428"
      - "--lh-logs-endpoint=http://lakehouse-logs:9428"
      - "--lh-traces-endpoint=http://lakehouse-traces:10428"

volumes:
  vl-data: {}
  vt-data: {}
```

- [ ] **Step 2: Verify compose starts successfully**

Run:
```bash
cd tests/parity && docker compose up -d --build --wait
```
Expected: All 6 services healthy, datagen-seed completes with "Generated" log.

- [ ] **Step 3: Verify both systems have data**

Run:
```bash
curl -s "http://localhost:19428/select/logsql/stats_query?query=*+|+stats+count()+rows&start=0&end=$(date +%s)000000000" | jq .
curl -s "http://localhost:29428/select/logsql/stats_query?query=*+|+stats+count()+rows&start=0&end=$(date +%s)000000000" | jq .
```
Expected: Both return count > 0.

- [ ] **Step 4: Tear down**

Run:
```bash
cd tests/parity && docker compose down -v
```

- [ ] **Step 5: Commit**

```bash
git add tests/parity/docker-compose.yml
git commit -m "feat(parity): add lightweight 6-service docker-compose for parity tests"
```

---

### Task 2: Helpers — HTTP Client, Response Parsers, Comparison Engine

**Files:**
- Create: `tests/parity/helpers.go`

- [ ] **Step 1: Create helpers.go with HTTP client and response parsers**

```go
//go:build parity

package parity

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
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

type fetchResult struct {
	StatusCode int
	Body       []byte
}

func fetch(t *testing.T, baseURL, path string, params url.Values) fetchResult {
	t.Helper()
	u := baseURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	resp, err := httpClient.Get(u)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
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
		msgStr, _ := row["_msg"].(string)
		parts = append(parts, "t="+timeStr, "m="+msgStr)
		for k, v := range row {
			if skip[k] || k == "_time" || k == "_msg" {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(parts[2:])
		keys = append(keys, strings.Join(parts, "|"))
	}
	sort.Strings(keys)
	return keys
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && GOWORK=off go build -tags=parity ./tests/parity/...
```
Expected: No errors (package has no test files yet, build only checks helpers.go compiles).

- [ ] **Step 3: Commit**

```bash
git add tests/parity/helpers.go
git commit -m "feat(parity): add HTTP client, response parsers, and comparison helpers"
```

---

### Task 3: Test Runner — ParityCase Type + RunParity + Comparison Engine

**Files:**
- Create: `tests/parity/parity_test.go`

- [ ] **Step 1: Create parity_test.go with ParityCase, CompareMode, and RunParity**

```go
//go:build parity

package parity

import (
	"fmt"
	"math"
	"net/url"
	"strings"
	"testing"
	"time"
)

type CompareMode string

const (
	CountEqual     CompareMode = "count_equal"
	CountTolerance CompareMode = "count_tolerance"
	SetEqual       CompareMode = "set_equal"
	SetSuperset    CompareMode = "set_superset"
	RowsMatch      CompareMode = "rows_match"
	StatusEqual    CompareMode = "status_equal"
	StructureMatch CompareMode = "structure_match"
	BucketMatch    CompareMode = "bucket_match"
	NonEmpty       CompareMode = "non_empty"
)

type ParityCase struct {
	Name       string
	Endpoint   string
	Params     map[string]string
	Compare    CompareMode
	SkipFields []string
	Tolerance  float64
}

func fullRangeParams() url.Values {
	now := time.Now()
	return url.Values{
		"start": {fmt.Sprintf("%d", now.Add(-48*time.Hour).UnixNano())},
		"end":   {fmt.Sprintf("%d", now.UnixNano())},
	}
}

func rangeParams(dur time.Duration) url.Values {
	now := time.Now()
	return url.Values{
		"start": {fmt.Sprintf("%d", now.Add(-dur).UnixNano())},
		"end":   {fmt.Sprintf("%d", now.UnixNano())},
	}
}

func buildParams(pc ParityCase, base url.Values) url.Values {
	params := url.Values{}
	if base != nil {
		for k, v := range base {
			params[k] = v
		}
	}
	for k, v := range pc.Params {
		params.Set(k, v)
	}
	return params
}

func RunParity(t *testing.T, refBase, sutBase string, cases []ParityCase) {
	t.Helper()
	for _, pc := range cases {
		t.Run(pc.Name, func(t *testing.T) {
			params := buildParams(pc, fullRangeParams())
			ref := fetch(t, refBase, pc.Endpoint, params)
			sut := fetch(t, sutBase, pc.Endpoint, params)
			compareParity(t, pc, ref, sut)
		})
	}
}

func RunParityWithRange(t *testing.T, refBase, sutBase string, dur time.Duration, cases []ParityCase) {
	t.Helper()
	for _, pc := range cases {
		t.Run(pc.Name, func(t *testing.T) {
			params := buildParams(pc, rangeParams(dur))
			ref := fetch(t, refBase, pc.Endpoint, params)
			sut := fetch(t, sutBase, pc.Endpoint, params)
			compareParity(t, pc, ref, sut)
		})
	}
}

func compareParity(t *testing.T, pc ParityCase, ref, sut fetchResult) {
	t.Helper()
	switch pc.Compare {
	case CountEqual:
		compareCountEqual(t, ref, sut, 0)
	case CountTolerance:
		tol := pc.Tolerance
		if tol == 0 {
			tol = 0.01
		}
		compareCountEqual(t, ref, sut, tol)
	case SetEqual:
		compareSetEqual(t, ref, sut)
	case SetSuperset:
		compareSetSuperset(t, ref, sut)
	case RowsMatch:
		compareRowsMatch(t, ref, sut, pc.SkipFields)
	case StatusEqual:
		compareStatusEqual(t, ref, sut)
	case StructureMatch:
		compareStructureMatch(t, ref, sut)
	case BucketMatch:
		compareBucketMatch(t, ref, sut)
	case NonEmpty:
		compareNonEmpty(t, ref, sut)
	default:
		t.Fatalf("unknown compare mode: %s", pc.Compare)
	}
}

func compareCountEqual(t *testing.T, ref, sut fetchResult, tolerance float64) {
	t.Helper()
	if ref.StatusCode != 200 {
		t.Fatalf("reference returned status %d: %s", ref.StatusCode, string(ref.Body))
	}
	if sut.StatusCode != 200 {
		t.Fatalf("SUT returned status %d: %s", sut.StatusCode, string(sut.Body))
	}
	refCount, err := extractVectorCount(ref.Body)
	if err != nil {
		refLines := parseNDJSON(ref.Body)
		sutLines := parseNDJSON(sut.Body)
		refCount = float64(len(refLines))
		sutCount := float64(len(sutLines))
		if tolerance == 0 {
			if refCount != sutCount {
				t.Errorf("count mismatch: ref=%v sut=%v", refCount, sutCount)
			}
		} else {
			if refCount > 0 && math.Abs(refCount-sutCount)/refCount > tolerance {
				t.Errorf("count outside tolerance %.1f%%: ref=%v sut=%v", tolerance*100, refCount, sutCount)
			}
		}
		t.Logf("count_equal (NDJSON): ref=%v sut=%v", refCount, sutCount)
		return
	}
	sutCount, err := extractVectorCount(sut.Body)
	if err != nil {
		t.Fatalf("SUT extractVectorCount: %v", err)
	}
	if tolerance == 0 {
		if refCount != sutCount {
			t.Errorf("count mismatch: ref=%v sut=%v", refCount, sutCount)
		}
	} else {
		if refCount > 0 && math.Abs(refCount-sutCount)/refCount > tolerance {
			t.Errorf("count outside tolerance %.1f%%: ref=%v sut=%v", tolerance*100, refCount, sutCount)
		}
	}
	t.Logf("count_equal: ref=%v sut=%v", refCount, sutCount)
}

func compareSetEqual(t *testing.T, ref, sut fetchResult) {
	t.Helper()
	if ref.StatusCode != 200 {
		t.Fatalf("reference returned status %d", ref.StatusCode)
	}
	if sut.StatusCode != 200 {
		t.Fatalf("SUT returned status %d", sut.StatusCode)
	}
	refVals := sortedStrings(extractValuesStrings(ref.Body))
	sutVals := sortedStrings(extractValuesStrings(sut.Body))
	refSet := stringSet(refVals)
	sutSet := stringSet(sutVals)
	for _, v := range refVals {
		if !sutSet[v] {
			t.Errorf("SUT missing value %q present in reference", v)
		}
	}
	for _, v := range sutVals {
		if !refSet[v] {
			t.Errorf("SUT has extra value %q not in reference", v)
		}
	}
	t.Logf("set_equal: ref=%d sut=%d", len(refVals), len(sutVals))
}

func compareSetSuperset(t *testing.T, ref, sut fetchResult) {
	t.Helper()
	if ref.StatusCode != 200 {
		t.Fatalf("reference returned status %d", ref.StatusCode)
	}
	if sut.StatusCode != 200 {
		t.Fatalf("SUT returned status %d", sut.StatusCode)
	}
	refVals := extractValuesStrings(ref.Body)
	sutSet := stringSet(extractValuesStrings(sut.Body))
	for _, v := range refVals {
		if !sutSet[v] {
			t.Errorf("SUT missing value %q present in reference (superset check)", v)
		}
	}
	t.Logf("set_superset: ref=%d sut=%d", len(refVals), len(sutSet))
}

func compareRowsMatch(t *testing.T, ref, sut fetchResult, skipFields []string) {
	t.Helper()
	if ref.StatusCode != 200 {
		t.Fatalf("reference returned status %d", ref.StatusCode)
	}
	if sut.StatusCode != 200 {
		t.Fatalf("SUT returned status %d", sut.StatusCode)
	}
	refRows := parseNDJSON(ref.Body)
	sutRows := parseNDJSON(sut.Body)
	if len(refRows) != len(sutRows) {
		t.Errorf("row count mismatch: ref=%d sut=%d", len(refRows), len(sutRows))
		return
	}
	refKeys := extractRowKeys(refRows, skipFields)
	sutKeys := extractRowKeys(sutRows, skipFields)
	mismatches := 0
	for i := range refKeys {
		if i >= len(sutKeys) {
			break
		}
		if refKeys[i] != sutKeys[i] {
			mismatches++
			if mismatches <= 3 {
				t.Errorf("row %d mismatch:\n  ref: %s\n  sut: %s", i, refKeys[i], sutKeys[i])
			}
		}
	}
	if mismatches > 3 {
		t.Errorf("... and %d more mismatches", mismatches-3)
	}
	t.Logf("rows_match: %d rows, %d mismatches", len(refRows), mismatches)
}

func compareStatusEqual(t *testing.T, ref, sut fetchResult) {
	t.Helper()
	if ref.StatusCode != sut.StatusCode {
		t.Errorf("status mismatch: ref=%d sut=%d", ref.StatusCode, sut.StatusCode)
	}
	t.Logf("status_equal: ref=%d sut=%d", ref.StatusCode, sut.StatusCode)
}

func compareStructureMatch(t *testing.T, ref, sut fetchResult) {
	t.Helper()
	if ref.StatusCode != 200 {
		t.Fatalf("reference returned status %d", ref.StatusCode)
	}
	if sut.StatusCode != 200 {
		t.Fatalf("SUT returned status %d", sut.StatusCode)
	}
	refObj, err := parseJSON(ref.Body)
	if err != nil {
		t.Fatalf("ref parse: %v", err)
	}
	sutObj, err := parseJSON(sut.Body)
	if err != nil {
		t.Fatalf("sut parse: %v", err)
	}
	refStatus, _ := refObj["status"].(string)
	sutStatus, _ := sutObj["status"].(string)
	if refStatus != sutStatus {
		t.Errorf("status field mismatch: ref=%q sut=%q", refStatus, sutStatus)
	}
	refData, _ := refObj["data"].(map[string]any)
	sutData, _ := sutObj["data"].(map[string]any)
	if refData == nil || sutData == nil {
		t.Logf("structure_match: one or both missing data field")
		return
	}
	refType, _ := refData["resultType"].(string)
	sutType, _ := sutData["resultType"].(string)
	if refType != sutType {
		t.Errorf("resultType mismatch: ref=%q sut=%q", refType, sutType)
	}
	refResult, _ := refData["result"].([]any)
	sutResult, _ := sutData["result"].([]any)
	if len(refResult) != len(sutResult) {
		t.Errorf("result array length mismatch: ref=%d sut=%d", len(refResult), len(sutResult))
	}
	t.Logf("structure_match: type=%s ref_results=%d sut_results=%d", refType, len(refResult), len(sutResult))
}

func compareBucketMatch(t *testing.T, ref, sut fetchResult) {
	t.Helper()
	if ref.StatusCode != 200 {
		t.Fatalf("reference returned status %d", ref.StatusCode)
	}
	if sut.StatusCode != 200 {
		t.Fatalf("SUT returned status %d", sut.StatusCode)
	}
	refTS, refCounts := extractHitsBuckets(ref.Body)
	sutTS, sutCounts := extractHitsBuckets(sut.Body)
	if len(refTS) != len(sutTS) {
		t.Errorf("bucket count mismatch: ref=%d sut=%d", len(refTS), len(sutTS))
		return
	}
	mismatches := 0
	for i := range refCounts {
		if i >= len(sutCounts) {
			break
		}
		if math.Abs(refCounts[i]-sutCounts[i]) > 1 {
			mismatches++
			if mismatches <= 3 {
				t.Errorf("bucket %d (%s) count mismatch: ref=%v sut=%v", i, refTS[i], refCounts[i], sutCounts[i])
			}
		}
	}
	totalRef := 0.0
	totalSut := 0.0
	for _, c := range refCounts {
		totalRef += c
	}
	for _, c := range sutCounts {
		totalSut += c
	}
	t.Logf("bucket_match: %d buckets, %d mismatches, ref_total=%v sut_total=%v", len(refTS), mismatches, totalRef, totalSut)
}

func compareNonEmpty(t *testing.T, ref, sut fetchResult) {
	t.Helper()
	if ref.StatusCode != 200 {
		t.Fatalf("reference returned status %d", ref.StatusCode)
	}
	if sut.StatusCode != 200 {
		t.Fatalf("SUT returned status %d", sut.StatusCode)
	}
	refLen := len(strings.TrimSpace(string(ref.Body)))
	sutLen := len(strings.TrimSpace(string(sut.Body)))
	if refLen == 0 {
		t.Error("reference returned empty response")
	}
	if sutLen == 0 {
		t.Error("SUT returned empty response")
	}
	t.Logf("non_empty: ref=%d bytes sut=%d bytes", refLen, sutLen)
}

func statsEndpoint() string {
	return "/select/logsql/stats_query"
}

func statsRangeEndpoint() string {
	return "/select/logsql/stats_query_range"
}

func queryEndpoint() string {
	return "/select/logsql/query"
}

func hitsEndpoint() string {
	return "/select/logsql/hits"
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && GOWORK=off go vet -tags=parity ./tests/parity/...
```
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add tests/parity/parity_test.go
git commit -m "feat(parity): add ParityCase type, RunParity runner, and comparison engine"
```

---

### Task 4: Family 1 — Endpoint Coverage (14 cases)

**Files:**
- Create: `tests/parity/logs_endpoints_test.go`

- [ ] **Step 1: Create logs_endpoints_test.go**

```go
//go:build parity

package parity

import "testing"

func TestParity_Endpoints(t *testing.T) {
	cases := []ParityCase{
		{
			Name:     "query_wildcard",
			Endpoint: "/select/logsql/query",
			Params:   map[string]string{"query": "*", "limit": "10"},
			Compare:  RowsMatch,
		},
		{
			Name:     "query_time_range",
			Endpoint: "/select/logsql/query_time_range",
			Params:   map[string]string{"query": "*"},
			Compare:  StructureMatch,
		},
		{
			Name:     "facets",
			Endpoint: "/select/logsql/facets",
			Params:   map[string]string{"query": "*"},
			Compare:  SetEqual,
		},
		{
			Name:     "field_names",
			Endpoint: "/select/logsql/field_names",
			Params:   map[string]string{"query": "*"},
			Compare:  SetSuperset,
		},
		{
			Name:     "field_values_level",
			Endpoint: "/select/logsql/field_values",
			Params:   map[string]string{"query": "*", "field": "level"},
			Compare:  SetEqual,
		},
		{
			Name:     "stream_field_names",
			Endpoint: "/select/logsql/stream_field_names",
			Params:   map[string]string{"query": "*"},
			Compare:  SetSuperset,
		},
		{
			Name:     "stream_field_values_service",
			Endpoint: "/select/logsql/stream_field_values",
			Params:   map[string]string{"query": "*", "field": "service.name"},
			Compare:  SetEqual,
		},
		{
			Name:     "streams",
			Endpoint: "/select/logsql/streams",
			Params:   map[string]string{"query": "*"},
			Compare:  NonEmpty,
		},
		{
			Name:     "stream_ids",
			Endpoint: "/select/logsql/stream_ids",
			Params:   map[string]string{"query": "*"},
			Compare:  NonEmpty,
		},
		{
			Name:     "hits_1h",
			Endpoint: "/select/logsql/hits",
			Params:   map[string]string{"query": "*", "step": "3600s"},
			Compare:  BucketMatch,
		},
		{
			Name:     "stats_count",
			Endpoint: "/select/logsql/stats_query",
			Params:   map[string]string{"query": "* | stats count() rows"},
			Compare:  CountEqual,
		},
		{
			Name:     "stats_range",
			Endpoint: "/select/logsql/stats_query_range",
			Params:   map[string]string{"query": "* | stats count() rows", "step": "3600s"},
			Compare:  StructureMatch,
		},
		{
			Name:     "tail_not_supported",
			Endpoint: "/select/logsql/tail",
			Params:   map[string]string{"query": "*"},
			Compare:  StatusEqual,
		},
		{
			Name:     "tenant_ids",
			Endpoint: "/select/tenant_ids",
			Params:   map[string]string{},
			Compare:  SetEqual,
		},
	}
	RunParity(t, vlBaseURL, lhBaseURL, cases)
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && GOWORK=off go vet -tags=parity ./tests/parity/...
```
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add tests/parity/logs_endpoints_test.go
git commit -m "feat(parity): add Family 1 — 14 endpoint coverage tests"
```

---

### Task 5: Family 2 — Filter Types (25 cases)

**Files:**
- Create: `tests/parity/logs_filters_test.go`

- [ ] **Step 1: Create logs_filters_test.go**

```go
//go:build parity

package parity

import "testing"

func TestParity_Filters(t *testing.T) {
	cases := []ParityCase{
		{Name: "wildcard", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: CountEqual},
		{Name: "exact_service", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:="api-gateway" | stats count() rows`}, Compare: CountEqual},
		{Name: "exact_level", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="ERROR" | stats count() rows`}, Compare: CountEqual},
		{Name: "exact_namespace", Endpoint: statsEndpoint(), Params: map[string]string{"query": `k8s.namespace.name:="production" | stats count() rows`}, Compare: CountEqual},
		{Name: "substring_msg", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:timeout | stats count() rows`}, Compare: CountEqual},
		{Name: "substring_case", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:Error | stats count() rows`}, Compare: CountEqual},
		{Name: "regexp_msg", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:~"timeout|deadline" | stats count() rows`}, Compare: CountEqual},
		{Name: "regexp_service", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:~"api-.*" | stats count() rows`}, Compare: CountEqual},
		{Name: "not_level", Endpoint: statsEndpoint(), Params: map[string]string{"query": `NOT level:="DEBUG" | stats count() rows`}, Compare: CountEqual},
		{Name: "not_service", Endpoint: statsEndpoint(), Params: map[string]string{"query": `NOT service.name:="api-gateway" | stats count() rows`}, Compare: CountEqual},
		{Name: "and_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:="api-gateway" AND level:="ERROR" | stats count() rows`}, Compare: CountEqual},
		{Name: "or_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="ERROR" OR level:="WARN" | stats count() rows`}, Compare: CountEqual},
		{Name: "and_or_combined", Endpoint: statsEndpoint(), Params: map[string]string{"query": `(level:="ERROR" OR level:="WARN") AND service.name:="api-gateway" | stats count() rows`}, Compare: CountEqual},
		{Name: "field_exists", Endpoint: statsEndpoint(), Params: map[string]string{"query": `trace_id:* | stats count() rows`}, Compare: CountEqual},
		{Name: "field_not_exists", Endpoint: statsEndpoint(), Params: map[string]string{"query": `NOT nonexistent_field:* | stats count() rows`}, Compare: CountEqual},
		{Name: "exact_msg", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:="specific log message" | stats count() rows`}, Compare: CountEqual},
		{Name: "in_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:in("ERROR", "WARN") | stats count() rows`}, Compare: CountEqual},
		{Name: "range_numeric", Endpoint: statsEndpoint(), Params: map[string]string{"query": `http.status_code:range[400, 599] | stats count() rows`}, Compare: CountEqual},
		{Name: "seq_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:seq("connection" "refused") | stats count() rows`}, Compare: CountEqual},
		{Name: "ipv4_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:ipv4_range("10.0.0.0/8") | stats count() rows`}, Compare: CountEqual},
		{Name: "len_range", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:len_range(100, 500) | stats count() rows`}, Compare: CountEqual},
		{Name: "multi_exact", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:="api-gateway" level:="ERROR" k8s.namespace.name:="production" | stats count() rows`}, Compare: CountEqual},
		{Name: "negated_regexp", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:!~"debug|trace" | stats count() rows`}, Compare: CountEqual},
		{Name: "empty_value", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="" | stats count() rows`}, Compare: CountEqual},
		{Name: "stream_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `{service.name="api-gateway"} | stats count() rows`}, Compare: CountEqual},
	}
	RunParity(t, vlBaseURL, lhBaseURL, cases)
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && GOWORK=off go vet -tags=parity ./tests/parity/...
```
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add tests/parity/logs_filters_test.go
git commit -m "feat(parity): add Family 2 — 25 filter type parity tests"
```

---

### Task 6: Family 3 — Pipe Operations (20 cases)

**Files:**
- Create: `tests/parity/logs_pipes_test.go`

- [ ] **Step 1: Create logs_pipes_test.go**

```go
//go:build parity

package parity

import "testing"

func TestParity_Pipes(t *testing.T) {
	cases := []ParityCase{
		{Name: "stats_count", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: CountEqual},
		{Name: "stats_count_by_level", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats by(level) count() rows"}, Compare: StructureMatch},
		{Name: "stats_count_by_service", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats by(service.name) count() rows"}, Compare: StructureMatch},
		{Name: "stats_count_uniq", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count_uniq(service.name) services"}, Compare: CountEqual},
		{Name: "stats_min_max", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats min(_time) earliest, max(_time) latest"}, Compare: StructureMatch},
		{Name: "fields_projection", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | fields _time, _msg, level", "limit": "10"}, Compare: RowsMatch},
		{Name: "fields_single", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | fields _msg", "limit": "10"}, Compare: RowsMatch},
		{Name: "sort_time_asc", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | sort by(_time)", "limit": "10"}, Compare: RowsMatch},
		{Name: "sort_time_desc", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | sort by(_time) desc", "limit": "10"}, Compare: RowsMatch},
		{Name: "limit_10", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | limit 10", "limit": "10"}, Compare: RowsMatch},
		{Name: "limit_1", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | limit 1", "limit": "1"}, Compare: RowsMatch},
		{Name: "uniq_level", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | uniq by(level)"}, Compare: SetEqual},
		{Name: "uniq_service", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | uniq by(service.name)"}, Compare: SetEqual},
		{Name: "top_services", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | top 5 by(service.name)"}, Compare: StructureMatch},
		{Name: "pipe_chain_fields_sort", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | fields _time, level | sort by(_time) | limit 5", "limit": "5"}, Compare: RowsMatch},
		{Name: "pipe_chain_filter_stats", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="ERROR" | stats by(service.name) count() rows`}, Compare: StructureMatch},
		{Name: "stats_by_two_fields", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats by(level, service.name) count() rows"}, Compare: StructureMatch},
		{Name: "stats_sum", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats sum(duration) total"}, Compare: CountTolerance, Tolerance: 0.05},
		{Name: "stats_avg", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats avg(duration) mean"}, Compare: CountTolerance, Tolerance: 0.05},
		{Name: "copy_pipe", Endpoint: queryEndpoint(), Params: map[string]string{"query": "* | copy level AS severity", "limit": "10"}, Compare: RowsMatch},
	}
	RunParity(t, vlBaseURL, lhBaseURL, cases)
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && GOWORK=off go vet -tags=parity ./tests/parity/...
```
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add tests/parity/logs_pipes_test.go
git commit -m "feat(parity): add Family 3 — 20 pipe operation parity tests"
```

---

### Task 7: Family 4 — Stats Query & Range (15 cases)

**Files:**
- Create: `tests/parity/logs_stats_test.go`

- [ ] **Step 1: Create logs_stats_test.go**

```go
//go:build parity

package parity

import (
	"fmt"
	"testing"
	"time"
)

func TestParity_Stats(t *testing.T) {
	now := time.Now()

	cases := []ParityCase{
		{Name: "count_1h", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: CountEqual},
		{Name: "count_6h", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: CountEqual},
		{Name: "count_24h", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: CountEqual},
		{Name: "count_full", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: CountEqual},
		{Name: "filtered_count", Endpoint: statsEndpoint(), Params: map[string]string{"query": `service.name:="api-gateway" | stats count() rows`}, Compare: CountEqual},
		{Name: "filtered_level", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="ERROR" | stats count() rows`}, Compare: CountEqual},
		{Name: "group_by_level", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats by(level) count() rows"}, Compare: StructureMatch},
		{Name: "range_rate_1h", Endpoint: statsRangeEndpoint(), Params: map[string]string{"query": "* | stats count() rows", "step": "300s"}, Compare: StructureMatch},
		{Name: "range_rate_6h", Endpoint: statsRangeEndpoint(), Params: map[string]string{"query": "* | stats count() rows", "step": "600s"}, Compare: StructureMatch},
		{Name: "range_rate_24h", Endpoint: statsRangeEndpoint(), Params: map[string]string{"query": "* | stats count() rows", "step": "3600s"}, Compare: StructureMatch},
		{Name: "range_filtered", Endpoint: statsRangeEndpoint(), Params: map[string]string{"query": `level:="ERROR" | stats count() rows`, "step": "3600s"}, Compare: StructureMatch},
		{Name: "range_grouped", Endpoint: statsRangeEndpoint(), Params: map[string]string{"query": "* | stats by(level) count() rows", "step": "3600s"}, Compare: StructureMatch},
		{Name: "multi_stat", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() total, count_uniq(level) levels"}, Compare: StructureMatch},
		{Name: "count_over_subrange", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", now.Add(-12*time.Hour).UnixNano()),
			"end":   fmt.Sprintf("%d", now.Add(-6*time.Hour).UnixNano()),
		}, Compare: CountEqual},
		{Name: "empty_range_stats", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", now.Add(24*time.Hour).UnixNano()),
			"end":   fmt.Sprintf("%d", now.Add(48*time.Hour).UnixNano()),
		}, Compare: CountEqual},
	}

	durations := map[string]time.Duration{
		"count_1h":  1 * time.Hour,
		"count_6h":  6 * time.Hour,
		"count_24h": 24 * time.Hour,
	}

	for _, pc := range cases {
		t.Run(pc.Name, func(t *testing.T) {
			var params = fullRangeParams()
			if dur, ok := durations[pc.Name]; ok {
				params = rangeParams(dur)
			}
			for k, v := range pc.Params {
				params.Set(k, v)
			}
			ref := fetch(t, vlBaseURL, pc.Endpoint, params)
			sut := fetch(t, lhBaseURL, pc.Endpoint, params)
			compareParity(t, pc, ref, sut)
		})
	}
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && GOWORK=off go vet -tags=parity ./tests/parity/...
```
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add tests/parity/logs_stats_test.go
git commit -m "feat(parity): add Family 4 — 15 stats query parity tests"
```

---

### Task 8: Family 5 — Time Range Handling (10 cases)

**Files:**
- Create: `tests/parity/logs_timerange_test.go`

- [ ] **Step 1: Create logs_timerange_test.go**

```go
//go:build parity

package parity

import (
	"fmt"
	"testing"
	"time"
)

func TestParity_TimeRange(t *testing.T) {
	now := time.Now()
	dataStart := now.Add(-24 * time.Hour)

	t.Run("ns_epoch", func(t *testing.T) {
		pc := ParityCase{Name: "ns_epoch", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", dataStart.UnixNano()),
			"end":   fmt.Sprintf("%d", now.UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("sec_epoch", func(t *testing.T) {
		pc := ParityCase{Name: "sec_epoch", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", dataStart.Unix()),
			"end":   fmt.Sprintf("%d", now.Unix()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("ms_epoch", func(t *testing.T) {
		pc := ParityCase{Name: "ms_epoch", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", dataStart.UnixMilli()),
			"end":   fmt.Sprintf("%d", now.UnixMilli()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("missing_end", func(t *testing.T) {
		pc := ParityCase{Name: "missing_end", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", dataStart.UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("missing_start", func(t *testing.T) {
		pc := ParityCase{Name: "missing_start", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"end":   fmt.Sprintf("%d", now.UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("future_range", func(t *testing.T) {
		future := now.Add(365 * 24 * time.Hour)
		pc := ParityCase{Name: "future_range", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", future.UnixNano()),
			"end":   fmt.Sprintf("%d", future.Add(time.Hour).UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("zero_width", func(t *testing.T) {
		ts := fmt.Sprintf("%d", now.Add(-6*time.Hour).UnixNano())
		pc := ParityCase{Name: "zero_width", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": ts,
			"end":   ts,
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("narrow_1min", func(t *testing.T) {
		mid := now.Add(-12 * time.Hour)
		pc := ParityCase{Name: "narrow_1min", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", mid.UnixNano()),
			"end":   fmt.Sprintf("%d", mid.Add(time.Minute).UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("full_range", func(t *testing.T) {
		pc := ParityCase{Name: "full_range", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": "0",
			"end":   fmt.Sprintf("%d", now.UnixNano()),
		}, Compare: CountEqual}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})

	t.Run("boundary_ns", func(t *testing.T) {
		pc := ParityCase{Name: "boundary_ns", Endpoint: statsEndpoint(), Params: map[string]string{
			"query": "* | stats count() rows",
			"start": fmt.Sprintf("%d", dataStart.UnixNano()),
			"end":   fmt.Sprintf("%d", dataStart.Add(time.Second).UnixNano()),
		}, Compare: CountTolerance, Tolerance: 0.1}
		params := buildParams(pc, nil)
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		compareParity(t, pc, ref, sut)
	})
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && GOWORK=off go vet -tags=parity ./tests/parity/...
```
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add tests/parity/logs_timerange_test.go
git commit -m "feat(parity): add Family 5 — 10 time range handling parity tests"
```

---

### Task 9: Family 6 — Field Metadata (10 cases)

**Files:**
- Create: `tests/parity/logs_fields_test.go`

- [ ] **Step 1: Create logs_fields_test.go**

```go
//go:build parity

package parity

import "testing"

func TestParity_Fields(t *testing.T) {
	cases := []ParityCase{
		{Name: "field_names_all", Endpoint: "/select/logsql/field_names", Params: map[string]string{"query": "*"}, Compare: SetSuperset},
		{Name: "field_names_filtered", Endpoint: "/select/logsql/field_names", Params: map[string]string{"query": `service.name:="api-gateway"`}, Compare: SetSuperset},
		{Name: "field_values_level", Endpoint: "/select/logsql/field_values", Params: map[string]string{"query": "*", "field": "level"}, Compare: SetEqual},
		{Name: "field_values_service", Endpoint: "/select/logsql/field_values", Params: map[string]string{"query": "*", "field": "service.name"}, Compare: SetEqual},
		{Name: "field_values_namespace", Endpoint: "/select/logsql/field_values", Params: map[string]string{"query": "*", "field": "k8s.namespace.name"}, Compare: SetEqual},
		{Name: "field_values_limit", Endpoint: "/select/logsql/field_values", Params: map[string]string{"query": "*", "field": "service.name", "limit": "2"}, Compare: NonEmpty},
		{Name: "stream_field_names", Endpoint: "/select/logsql/stream_field_names", Params: map[string]string{"query": "*"}, Compare: SetSuperset},
		{Name: "stream_field_values", Endpoint: "/select/logsql/stream_field_values", Params: map[string]string{"query": "*", "field": "service.name"}, Compare: SetEqual},
		{Name: "streams_list", Endpoint: "/select/logsql/streams", Params: map[string]string{"query": "*"}, Compare: NonEmpty},
		{Name: "stream_ids", Endpoint: "/select/logsql/stream_ids", Params: map[string]string{"query": "*"}, Compare: NonEmpty},
	}
	RunParity(t, vlBaseURL, lhBaseURL, cases)
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && GOWORK=off go vet -tags=parity ./tests/parity/...
```
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add tests/parity/logs_fields_test.go
git commit -m "feat(parity): add Family 6 — 10 field metadata parity tests"
```

---

### Task 10: Family 7 — Response Format & Pagination (10 cases)

**Files:**
- Create: `tests/parity/logs_response_test.go`

- [ ] **Step 1: Create logs_response_test.go**

```go
//go:build parity

package parity

import (
	"testing"
)

func TestParity_Response(t *testing.T) {
	t.Run("jsonl_structure", func(t *testing.T) {
		pc := ParityCase{Name: "jsonl_structure", Endpoint: queryEndpoint(), Params: map[string]string{"query": "*", "limit": "10"}, Compare: RowsMatch}
		params := buildParams(pc, fullRangeParams())
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		refRows := parseNDJSON(ref.Body)
		sutRows := parseNDJSON(sut.Body)
		if len(refRows) == 0 {
			t.Fatal("reference returned 0 JSONL rows")
		}
		if len(sutRows) == 0 {
			t.Fatal("SUT returned 0 JSONL rows")
		}
		compareParity(t, pc, ref, sut)
	})

	t.Run("limit_respected", func(t *testing.T) {
		pc := ParityCase{Name: "limit_respected", Endpoint: queryEndpoint(), Params: map[string]string{"query": "*", "limit": "5"}, Compare: RowsMatch}
		params := buildParams(pc, fullRangeParams())
		ref := fetch(t, vlBaseURL, pc.Endpoint, params)
		sut := fetch(t, lhBaseURL, pc.Endpoint, params)
		refRows := parseNDJSON(ref.Body)
		sutRows := parseNDJSON(sut.Body)
		if len(refRows) > 5 {
			t.Errorf("reference returned %d rows, expected <= 5", len(refRows))
		}
		if len(sutRows) > 5 {
			t.Errorf("SUT returned %d rows, expected <= 5", len(sutRows))
		}
		t.Logf("limit=5: ref=%d sut=%d", len(refRows), len(sutRows))
	})

	t.Run("limit_zero", func(t *testing.T) {
		pc := ParityCase{Name: "limit_zero", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: CountEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("hits_bucket_keys", func(t *testing.T) {
		pc := ParityCase{Name: "hits_bucket_keys", Endpoint: hitsEndpoint(), Params: map[string]string{"query": "*", "step": "1800s"}, Compare: BucketMatch}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("hits_sum_equals_count", func(t *testing.T) {
		params := fullRangeParams()
		params.Set("query", "*")
		params.Set("step", "3600s")
		hitsRef := fetch(t, vlBaseURL, hitsEndpoint(), params)
		_, refCounts := extractHitsBuckets(hitsRef.Body)
		totalHits := 0.0
		for _, c := range refCounts {
			totalHits += c
		}

		statsParams := fullRangeParams()
		statsParams.Set("query", "* | stats count() rows")
		statsRef := fetch(t, vlBaseURL, statsEndpoint(), statsParams)
		statsCount, err := extractVectorCount(statsRef.Body)
		if err != nil {
			t.Fatalf("extract stats count: %v", err)
		}
		diff := totalHits - statsCount
		if diff < 0 {
			diff = -diff
		}
		pct := 0.0
		if statsCount > 0 {
			pct = diff / statsCount
		}
		if pct > 0.02 {
			t.Errorf("hits sum (%.0f) != stats count (%.0f), diff=%.1f%%", totalHits, statsCount, pct*100)
		}
		t.Logf("hits_sum=%.0f stats_count=%.0f diff=%.1f%%", totalHits, statsCount, pct*100)
	})

	t.Run("stats_vector_format", func(t *testing.T) {
		pc := ParityCase{Name: "stats_vector_format", Endpoint: statsEndpoint(), Params: map[string]string{"query": "* | stats count() rows"}, Compare: StructureMatch}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("stats_range_matrix", func(t *testing.T) {
		pc := ParityCase{Name: "stats_range_matrix", Endpoint: statsRangeEndpoint(), Params: map[string]string{"query": "* | stats count() rows", "step": "3600s"}, Compare: StructureMatch}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("field_names_jsonl", func(t *testing.T) {
		pc := ParityCase{Name: "field_names_jsonl", Endpoint: "/select/logsql/field_names", Params: map[string]string{"query": "*"}, Compare: SetSuperset}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("field_values_jsonl", func(t *testing.T) {
		pc := ParityCase{Name: "field_values_jsonl", Endpoint: "/select/logsql/field_values", Params: map[string]string{"query": "*", "field": "level"}, Compare: SetEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("large_limit", func(t *testing.T) {
		pc := ParityCase{Name: "large_limit", Endpoint: queryEndpoint(), Params: map[string]string{"query": "*", "limit": "100000"}, Compare: NonEmpty}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && GOWORK=off go vet -tags=parity ./tests/parity/...
```
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add tests/parity/logs_response_test.go
git commit -m "feat(parity): add Family 7 — 10 response format and pagination parity tests"
```

---

### Task 11: Family 8 — Edge Cases & Errors (10 cases)

**Files:**
- Create: `tests/parity/logs_edge_test.go`

- [ ] **Step 1: Create logs_edge_test.go**

```go
//go:build parity

package parity

import (
	"strings"
	"sync"
	"testing"
)

func TestParity_EdgeCases(t *testing.T) {
	t.Run("empty_filter", func(t *testing.T) {
		pc := ParityCase{Name: "empty_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `nonexistent_service:="xxx" | stats count() rows`}, Compare: CountEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("tail_501", func(t *testing.T) {
		pc := ParityCase{Name: "tail_501", Endpoint: "/select/logsql/tail", Params: map[string]string{"query": "*"}, Compare: StatusEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("invalid_query", func(t *testing.T) {
		pc := ParityCase{Name: "invalid_query", Endpoint: queryEndpoint(), Params: map[string]string{"query": ")))invalid"}, Compare: StatusEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("missing_query", func(t *testing.T) {
		pc := ParityCase{Name: "missing_query", Endpoint: queryEndpoint(), Params: map[string]string{}, Compare: StatusEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("special_chars", func(t *testing.T) {
		pc := ParityCase{Name: "special_chars", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:="hello \"world\"" | stats count() rows`}, Compare: CountEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("unicode_msg", func(t *testing.T) {
		pc := ParityCase{Name: "unicode_msg", Endpoint: statsEndpoint(), Params: map[string]string{"query": `_msg:="日本語" | stats count() rows`}, Compare: CountEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("empty_string_filter", func(t *testing.T) {
		pc := ParityCase{Name: "empty_string_filter", Endpoint: statsEndpoint(), Params: map[string]string{"query": `level:="" | stats count() rows`}, Compare: CountEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("very_long_query", func(t *testing.T) {
		longFilter := `_msg:="` + strings.Repeat("a", 1000) + `" | stats count() rows`
		pc := ParityCase{Name: "very_long_query", Endpoint: statsEndpoint(), Params: map[string]string{"query": longFilter}, Compare: StatusEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})

	t.Run("concurrent_queries", func(t *testing.T) {
		params := fullRangeParams()
		params.Set("query", "* | stats count() rows")

		var wg sync.WaitGroup
		results := make([]fetchResult, 10)
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				results[idx] = fetch(t, lhBaseURL, statsEndpoint(), params)
			}(i)
		}
		wg.Wait()

		refResult := fetch(t, vlBaseURL, statsEndpoint(), params)
		refCount, err := extractVectorCount(refResult.Body)
		if err != nil {
			t.Fatalf("ref count: %v", err)
		}

		for i, r := range results {
			if r.StatusCode != 200 {
				t.Errorf("concurrent[%d] status=%d", i, r.StatusCode)
				continue
			}
			sutCount, err := extractVectorCount(r.Body)
			if err != nil {
				t.Errorf("concurrent[%d] parse: %v", i, err)
				continue
			}
			if sutCount != refCount {
				t.Errorf("concurrent[%d] count=%v expected=%v", i, sutCount, refCount)
			}
		}
		t.Logf("concurrent: 10 queries all returned %v", refCount)
	})

	t.Run("stats_no_pipe", func(t *testing.T) {
		pc := ParityCase{Name: "stats_no_pipe", Endpoint: statsEndpoint(), Params: map[string]string{"query": "*"}, Compare: StatusEqual}
		RunParity(t, vlBaseURL, lhBaseURL, []ParityCase{pc})
	})
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && GOWORK=off go vet -tags=parity ./tests/parity/...
```
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add tests/parity/logs_edge_test.go
git commit -m "feat(parity): add Family 8 — 10 edge case and error parity tests"
```

---

### Task 12: Traces Parity (15 cases)

**Files:**
- Create: `tests/parity/traces_parity_test.go`

- [ ] **Step 1: Create traces_parity_test.go**

```go
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
	r := fetch(t, baseURL, "/api/traces", params)
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
		ref := fetch(t, vtBaseURL, "/api/services", nil)
		sut := fetch(t, lhtBaseURL, "/api/services", nil)
		compareParity(t, ParityCase{Compare: SetEqual}, ref, sut)
	})

	t.Run("jaeger_operations", func(t *testing.T) {
		ref := fetch(t, vtBaseURL, "/api/services/api-gateway/operations", nil)
		sut := fetch(t, lhtBaseURL, "/api/services/api-gateway/operations", nil)
		compareParity(t, ParityCase{Compare: SetEqual}, ref, sut)
	})

	t.Run("jaeger_search_service", func(t *testing.T) {
		params := url.Values{"service": {"api-gateway"}, "lookback": {"48h"}, "limit": {"5"}}
		ref := fetch(t, vtBaseURL, "/api/traces", params)
		sut := fetch(t, lhtBaseURL, "/api/traces", params)
		compareParity(t, ParityCase{Compare: NonEmpty}, ref, sut)
	})

	t.Run("jaeger_search_limit", func(t *testing.T) {
		params := url.Values{"service": {"api-gateway"}, "lookback": {"48h"}, "limit": {"5"}}
		ref := fetch(t, vtBaseURL, "/api/traces", params)
		sut := fetch(t, lhtBaseURL, "/api/traces", params)
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
		ref := fetch(t, vtBaseURL, "/api/traces/"+traceID, nil)
		sut := fetch(t, lhtBaseURL, "/api/traces/"+traceID, nil)
		compareParity(t, ParityCase{Compare: StructureMatch}, ref, sut)
	})

	t.Run("jaeger_dependencies", func(t *testing.T) {
		params := url.Values{"lookback": {"48h"}}
		ref := fetch(t, vtBaseURL, "/api/dependencies", params)
		sut := fetch(t, lhtBaseURL, "/api/dependencies", params)
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
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && GOWORK=off go vet -tags=parity ./tests/parity/...
```
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add tests/parity/traces_parity_test.go
git commit -m "feat(parity): add traces parity — 15 Jaeger API and LogsQL tests"
```

---

### Task 13: Integration Test — Full Suite on Compose Stack

**Files:**
- All files from Tasks 1-12

- [ ] **Step 1: Start the compose stack**

Run:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/tests/parity && docker compose up -d --build --wait
```
Expected: All services healthy, datagen-seed completes.

- [ ] **Step 2: Wait for LH flush to S3**

Run:
```bash
sleep 15
```
LH flush-interval is 5s, manifest refresh is 5s. 15s ensures at least one flush+manifest cycle.

- [ ] **Step 3: Run the full parity test suite**

Run:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && GOWORK=off go test -tags=parity -v -timeout=300s ./tests/parity/...
```
Expected: All tests pass. Failures indicate real parity gaps to investigate.

- [ ] **Step 4: Review failures and fix**

For any failing tests:
1. Check if it's a known acceptable difference (see spec section "Known Acceptable Differences")
2. If LH returns wrong results, investigate the storage/query code
3. If VL/VT behavior changed between versions, update the test expectations
4. Update comparison mode if needed (e.g., switch from SetEqual to SetSuperset)

- [ ] **Step 5: Re-run after fixes**

Run:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && GOWORK=off go test -tags=parity -v -timeout=300s ./tests/parity/...
```
Expected: All 115 tests pass.

- [ ] **Step 6: Tear down**

Run:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/tests/parity && docker compose down -v
```

- [ ] **Step 7: Commit any fixes**

```bash
git add tests/parity/
git commit -m "fix(parity): adjust tests for actual VL/LH behavior"
```
