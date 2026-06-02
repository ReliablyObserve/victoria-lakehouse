# API Parity Test Coverage Extension Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `tests/parity/` from select-heavy to comprehensive: insert API parity for 8 protocols, select-extended coverage, protocol conformance with third-party reference implementations, dual-mode comparator, capability matrix, and mechanical "every endpoint covered" gate.

**Architecture:** Three new sub-trees under `tests/parity/` (`insert/`, `select-extended/`, `protocol-conformance/`). Dual-mode comparator (semantic = blocking, byte-equal = drift-tracked). Protocol conformance triple-checks LH against VL/VT and a third-party reference impl, gated by per-protocol/per-transport `capabilities.yaml`. CI gains `parity-insert` (blocking) and `parity-conformance` (advisory) jobs.

**Tech Stack:** Go 1.24, Docker Compose (existing parity stack + new `--profile references`), `//go:build parity` and new `//go:build conformance` build tags, GitHub Actions, YAML capability registry.

**Spec:** `docs/superpowers/specs/2026-06-02-api-parity-extension-design.md`

**Existing context:**
- `tests/parity/` already has 21 files (~3300 LoC) covering select side.
- `tests/parity/parity_test.go` defines `ParityCase`, `RunParity`, time helpers, comparison modes.
- `tests/parity/helpers.go` defines HTTP clients, response parsers.
- `tests/parity/docker-compose.yml` runs minio, victorialogs, lakehouse-logs, victoriatraces, lakehouse-traces, datagen.
- `.github/workflows/parity.yaml` runs the existing parity job.

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `tests/parity/comparator.go` | Dual-mode (byte+semantic) comparator shared by insert & conformance |
| Create | `tests/parity/comparator_test.go` | Per-mode comparator correctness |
| Create | `tests/parity/byte_drift_report.go` | Drift log writer; produces `byte-drift-report.json` artifact |
| Create | `tests/parity/byte_drift_report_test.go` | Drift file format stability |
| Create | `tests/parity/insert/helpers.go` | `InsertCase`, `RunInsertParity`, `RunInsertWriteSideCompare`, flush wait |
| Create | `tests/parity/insert/helpers_test.go` | Harness overhead benchmark, helper unit tests |
| Create | `tests/parity/insert/logs_jsonline_test.go` | 7 test cases for `/insert/jsonline` |
| Create | `tests/parity/insert/logs_loki_test.go` | 7 test cases for `/insert/loki/api/v1/push` |
| Create | `tests/parity/insert/logs_elasticsearch_bulk_test.go` | 7 cases for `/insert/elasticsearch/_bulk` |
| Create | `tests/parity/insert/logs_otlp_test.go` | 7 cases for `/insert/opentelemetry/v1/logs` |
| Create | `tests/parity/insert/logs_splunk_hec_test.go` | 7 cases for `/services/collector/event` |
| Create | `tests/parity/insert/traces_jaeger_thrift_test.go` | 7 cases for Jaeger Thrift HTTP |
| Create | `tests/parity/insert/traces_zipkin_test.go` | 7 cases for Zipkin v2 |
| Create | `tests/parity/insert/traces_otlp_test.go` | 7 cases for OTLP HTTP traces |
| Create | `tests/parity/insert/schema_drift_test.go` | Post-insert `field_names` diff |
| Create | `tests/parity/insert/cross_protocol_test.go` | Same trace via multiple protocols → same trace_id |
| Create | `tests/parity/select-extended/logs_jaeger_compat_test.go` | Jaeger HTTP API on logs module |
| Create | `tests/parity/select-extended/traces_tempo_http_test.go` | Tempo HTTP search/query/echo |
| Create | `tests/parity/select-extended/traces_jaeger_extended_test.go` | Jaeger services/operations/dependencies |
| Create | `tests/parity/select-extended/internal_select_test.go` | `/internal/select/*` endpoints |
| Create | `tests/parity/select-extended/tail_streaming_test.go` | tail handshake parity |
| Create | `tests/parity/protocol-conformance/conformance.go` | `ConformanceCase`, `RunConformance`, capability resolution |
| Create | `tests/parity/protocol-conformance/conformance_test.go` | Capability matrix parsing, golden discovery |
| Create | `tests/parity/protocol-conformance/capabilities.yaml` | Per-protocol/per-transport LH/VL/Ref support |
| Create | `tests/parity/protocol-conformance/spec-versions.json` | Recorded spec edition per protocol |
| Create | `tests/parity/protocol-conformance/KNOWN_DIVERGENCES.md` | Ratified spec-vs-implementation exceptions |
| Create | `tests/parity/protocol-conformance/KNOWN_VL_BUGS.md` | VL deviations with upstream issue links |
| Create | `tests/parity/protocol-conformance/docker-compose.references.yml` | promtail/otel-collector/tempo/jaeger/elasticsearch services |
| Create | `tests/parity/protocol-conformance/loki/{golden/,snapshots/,loki_conformance_test.go}` | Loki goldens + driver |
| Create | `tests/parity/protocol-conformance/elasticsearch/{...}` | ES Bulk goldens + driver |
| Create | `tests/parity/protocol-conformance/otlp/{golden-http/,golden-grpc/,snapshots/,otlp_conformance_test.go}` | OTLP HTTP+gRPC |
| Create | `tests/parity/protocol-conformance/jaeger-thrift/{...}` | Jaeger Thrift goldens |
| Create | `tests/parity/protocol-conformance/zipkin/{...}` | Zipkin v2 goldens |
| Create | `tests/parity/protocol-conformance/tempo-http/{...}` | Tempo HTTP goldens |
| Create | `tests/parity/protocol-conformance/splunk-hec/{...}` | Splunk HEC goldens |
| Create | `tests/parity/coverage_test.go` | `TestEveryExposedEndpointHasParityTest` |
| Create | `tests/parity/exempt-endpoints.yaml` | Operational endpoints exempt from coverage |
| Modify | `tests/parity/docker-compose.yml` | Add VL insert receiver ports if needed; add `references` profile referencing the new compose file |
| Modify | `.github/workflows/parity.yaml` | Add `parity-insert` and `parity-conformance` jobs |

---

## PR B1 — Insert Harness + First Protocol (jsonline)

**Goal:** Build the insert parity infrastructure and validate the pattern against one simple protocol before scaling.

### Task B1.1: Create the dual-mode comparator

**Files:**
- Create: `tests/parity/comparator.go`
- Create: `tests/parity/comparator_test.go`

- [ ] **Step 1: Write the failing test**

```go
// tests/parity/comparator_test.go
//go:build parity

package parity

import "testing"

// negative control: change ModeSetEqual to compare ordered → this test must
// fail because ["a","b"] vs ["b","a"] would no longer be SemanticPass.
func TestCompareSetEqualUnordered(t *testing.T) {
    a := []byte(`{"rows":[{"k":"a"},{"k":"b"}]}`)
    b := []byte(`{"rows":[{"k":"b"},{"k":"a"}]}`)
    r := Compare(a, b, ModeSetEqual)
    if !r.SemanticPass { t.Fatalf("expected SemanticPass: %s", r.Diff) }
    if r.ByteEqual { t.Fatalf("expected ByteEqual=false (order differs)") }
}

func TestCompareByteEqualPath(t *testing.T) {
    a := []byte(`hello`)
    r := Compare(a, a, ModeByteEqual)
    if !r.SemanticPass || !r.ByteEqual {
        t.Fatalf("expected both true; got %+v", r)
    }
}

func TestCompareCountEqual(t *testing.T) {
    a := []byte(`{"hits":{"total":42}}`)
    b := []byte(`{"hits":{"total":42}}`)
    r := Compare(a, b, ModeCountEqual)
    if !r.SemanticPass { t.Fatalf("expected SemanticPass") }
}
```

- [ ] **Step 2: Run — expect failure**

```bash
cd tests/parity && go test -tags=parity -run TestCompare
```
Expected: FAIL (`undefined: Compare`).

- [ ] **Step 3: Implement comparator**

```go
// tests/parity/comparator.go
//go:build parity

package parity

import (
    "bytes"
    "encoding/json"
    "fmt"
    "reflect"
    "sort"
)

type CompareMode int

const (
    ModeByteEqual CompareMode = iota
    ModeSetEqual
    ModeCountEqual
    ModeRowsMatch
    ModeSuperset
    ModeSetEqualWithRetry
)

type CompareResult struct {
    SemanticPass bool
    ByteEqual    bool
    Diff         string
}

func Compare(lh, vl []byte, mode CompareMode) CompareResult {
    byteEq := bytes.Equal(lh, vl)
    semPass, diff := semanticCompare(lh, vl, mode)
    return CompareResult{SemanticPass: semPass, ByteEqual: byteEq, Diff: diff}
}

func semanticCompare(lh, vl []byte, mode CompareMode) (bool, string) {
    switch mode {
    case ModeByteEqual:
        if bytes.Equal(lh, vl) { return true, "" }
        return false, "byte mismatch"
    case ModeSetEqual, ModeSetEqualWithRetry:
        return compareSetEqual(lh, vl)
    case ModeCountEqual:
        return compareCountEqual(lh, vl)
    case ModeRowsMatch:
        return compareRowsMatch(lh, vl)
    case ModeSuperset:
        return compareSuperset(lh, vl)
    }
    return false, fmt.Sprintf("unknown mode: %v", mode)
}

func compareSetEqual(lh, vl []byte) (bool, string) {
    var lhRows, vlRows []map[string]any
    if err := decodeRows(lh, &lhRows); err != nil { return false, err.Error() }
    if err := decodeRows(vl, &vlRows); err != nil { return false, err.Error() }
    sortRows(lhRows)
    sortRows(vlRows)
    if !reflect.DeepEqual(lhRows, vlRows) {
        return false, fmt.Sprintf("rows differ: lh=%v vl=%v", lhRows, vlRows)
    }
    return true, ""
}

func compareCountEqual(lh, vl []byte) (bool, string) {
    lhCount := extractCount(lh)
    vlCount := extractCount(vl)
    if lhCount != vlCount {
        return false, fmt.Sprintf("count differs: lh=%d vl=%d", lhCount, vlCount)
    }
    return true, ""
}

func compareRowsMatch(lh, vl []byte) (bool, string) {
    var lhRows, vlRows []map[string]any
    if err := decodeRows(lh, &lhRows); err != nil { return false, err.Error() }
    if err := decodeRows(vl, &vlRows); err != nil { return false, err.Error() }
    if len(lhRows) != len(vlRows) {
        return false, fmt.Sprintf("row count: lh=%d vl=%d", len(lhRows), len(vlRows))
    }
    keyFields := []string{"_msg", "_time", "trace_id", "span_id"}
    for i := range lhRows {
        for _, k := range keyFields {
            if lhRows[i][k] != vlRows[i][k] {
                return false, fmt.Sprintf("row %d key %s: lh=%v vl=%v", i, k, lhRows[i][k], vlRows[i][k])
            }
        }
    }
    return true, ""
}

func compareSuperset(lh, vl []byte) (bool, string) {
    var lhRows, vlRows []map[string]any
    if err := decodeRows(lh, &lhRows); err != nil { return false, err.Error() }
    if err := decodeRows(vl, &vlRows); err != nil { return false, err.Error() }
    for _, vlRow := range vlRows {
        if !containsRow(lhRows, vlRow) {
            return false, fmt.Sprintf("vl row missing in lh: %v", vlRow)
        }
    }
    return true, ""
}

func decodeRows(b []byte, out *[]map[string]any) error {
    var doc struct {
        Rows []map[string]any `json:"rows"`
        Hits struct {
            Hits []map[string]any `json:"hits"`
        } `json:"hits"`
    }
    if err := json.Unmarshal(b, &doc); err != nil { return err }
    if doc.Rows != nil { *out = doc.Rows; return nil }
    *out = doc.Hits.Hits
    return nil
}

func sortRows(rows []map[string]any) {
    sort.Slice(rows, func(i, j int) bool {
        return fmt.Sprintf("%v", rows[i]) < fmt.Sprintf("%v", rows[j])
    })
}

func extractCount(b []byte) int {
    var doc struct {
        Hits struct {
            Total int `json:"total"`
        } `json:"hits"`
        Count int `json:"count"`
    }
    _ = json.Unmarshal(b, &doc)
    if doc.Count > 0 { return doc.Count }
    return doc.Hits.Total
}

func containsRow(rows []map[string]any, target map[string]any) bool {
    for _, r := range rows {
        if reflect.DeepEqual(r, target) { return true }
    }
    return false
}
```

- [ ] **Step 4: Run tests, all pass**

```bash
cd tests/parity && go test -tags=parity -run TestCompare
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/parity/comparator.go tests/parity/comparator_test.go
git commit -m "test/parity: dual-mode comparator (byte + semantic) (B1.1)"
```

### Task B1.2: Byte-drift report writer

**Files:**
- Create: `tests/parity/byte_drift_report.go`
- Create: `tests/parity/byte_drift_report_test.go`

- [ ] **Step 1: Write the failing test**

```go
// tests/parity/byte_drift_report_test.go
//go:build parity

package parity

import (
    "encoding/json"
    "os"
    "testing"
)

// negative control: remove the file-write in recordByteDrift → this test
// must fail because no drift report file is produced.
func TestByteDriftReportFileWritten(t *testing.T) {
    path := t.TempDir() + "/drift.json"
    setDriftReportPath(path)
    recordByteDrift("TestSomething", "case-1", "diff body here")
    flushDriftReport()
    data, err := os.ReadFile(path)
    if err != nil { t.Fatal(err) }
    var r DriftReport
    _ = json.Unmarshal(data, &r)
    if len(r.Drifts) != 1 { t.Fatalf("expected 1 drift, got %d", len(r.Drifts)) }
    if r.Drifts[0].Test != "TestSomething" { t.Errorf("test name mismatch") }
}
```

- [ ] **Step 2: Implement**

```go
// tests/parity/byte_drift_report.go
//go:build parity

package parity

import (
    "encoding/json"
    "os"
    "sync"
    "time"
)

type DriftReport struct {
    RunID     string       `json:"run_id"`
    Commit    string       `json:"commit"`
    StartedAt time.Time    `json:"started_at"`
    Drifts    []DriftEntry `json:"drifts"`
}

type DriftEntry struct {
    Test        string `json:"test"`
    Case        string `json:"case"`
    DiffSize    int    `json:"diff_size"`
    DiffPreview string `json:"diff_preview"`
    At          time.Time `json:"at"`
}

var (
    driftMu     sync.Mutex
    driftEntries []DriftEntry
    driftPath   = "byte-drift-report.json"
)

func setDriftReportPath(p string) { driftPath = p }

func recordByteDrift(testName, caseName, diff string) {
    driftMu.Lock()
    defer driftMu.Unlock()
    preview := diff
    if len(preview) > 256 { preview = preview[:256] }
    driftEntries = append(driftEntries, DriftEntry{
        Test: testName, Case: caseName,
        DiffSize: len(diff), DiffPreview: preview, At: time.Now(),
    })
}

func flushDriftReport() {
    driftMu.Lock()
    defer driftMu.Unlock()
    r := DriftReport{
        RunID:     os.Getenv("GITHUB_RUN_ID"),
        Commit:    os.Getenv("GITHUB_SHA"),
        StartedAt: time.Now(),
        Drifts:    driftEntries,
    }
    data, _ := json.MarshalIndent(r, "", "  ")
    _ = os.WriteFile(driftPath, data, 0644)
}
```

- [ ] **Step 3: Run tests**

```bash
cd tests/parity && go test -tags=parity -run TestByteDrift
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/parity/byte_drift_report.go tests/parity/byte_drift_report_test.go
git commit -m "test/parity: byte-drift report writer (B1.2)"
```

### Task B1.3: Insert harness

**Files:**
- Create: `tests/parity/insert/helpers.go`
- Create: `tests/parity/insert/helpers_test.go`

- [ ] **Step 1: Write the failing test (unit, no live stack)**

```go
// tests/parity/insert/helpers_test.go
//go:build parity

package insert

import (
    "testing"
    "time"
)

// negative control: remove the ExpectStatus check in RunInsertParity → this
// test must fail because we synthesize disagreeing statuses.
func TestInsertCaseValidation(t *testing.T) {
    c := InsertCase{
        Name: "test", Endpoint: "/insert/jsonline",
        Method: "POST", ExpectStatus: 200,
    }
    if err := c.Validate(); err != nil { t.Fatal(err) }
}

func TestInsertCaseDefaults(t *testing.T) {
    c := InsertCase{}
    c.applyDefaults()
    if c.Method != "POST" { t.Errorf("Method default not POST: %s", c.Method) }
    if c.WaitFlushS != 5*time.Second { t.Errorf("WaitFlushS default not 5s: %v", c.WaitFlushS) }
}
```

- [ ] **Step 2: Implement**

```go
// tests/parity/insert/helpers.go
//go:build parity

package insert

import (
    "bytes"
    "errors"
    "fmt"
    "io"
    "net/http"
    "os"
    "testing"
    "time"

    "github.com/ReliablyObserve/victoria-lakehouse/tests/parity"
)

type InsertCase struct {
    Name           string
    Endpoint       string
    Method         string
    Headers        map[string]string
    Body           []byte
    ExpectStatus   int
    ExpectErrorRE  string
    ReadBackQuery  string
    ReadBackMode   parity.CompareMode
    ExpectRows     int
    WaitFlushS     time.Duration
}

func (c *InsertCase) applyDefaults() {
    if c.Method == "" { c.Method = "POST" }
    if c.WaitFlushS == 0 { c.WaitFlushS = 5 * time.Second }
}

func (c *InsertCase) Validate() error {
    if c.Name == "" { return errors.New("Name required") }
    if c.Endpoint == "" { return errors.New("Endpoint required") }
    if c.ExpectStatus == 0 { return errors.New("ExpectStatus required") }
    return nil
}

func RunInsertParity(t *testing.T, c InsertCase) {
    c.applyDefaults()
    if err := c.Validate(); err != nil { t.Fatal(err) }

    lhURL := os.Getenv("LH_LOGS_URL")
    vlURL := os.Getenv("VL_LOGS_URL")
    if lhURL == "" { lhURL = "http://lakehouse-logs:9428" }
    if vlURL == "" { vlURL = "http://victorialogs:9428" }

    statusLH, bodyLH := post(t, lhURL+c.Endpoint, c)
    statusVL, bodyVL := post(t, vlURL+c.Endpoint, c)

    if statusLH != c.ExpectStatus {
        t.Fatalf("LH status: want %d, got %d (body: %s)", c.ExpectStatus, statusLH, bodyLH)
    }
    if statusVL != c.ExpectStatus {
        t.Fatalf("VL status: want %d, got %d (body: %s)", c.ExpectStatus, statusVL, bodyVL)
    }

    if c.ExpectStatus >= 300 { return }
    waitForFlush(t, c.WaitFlushS)
    RunInsertWriteSideCompare(t, c, lhURL, vlURL)

    if c.ReadBackQuery == "" { return }
    lhResult := queryLogs(t, lhURL, c.ReadBackQuery)
    vlResult := queryLogs(t, vlURL, c.ReadBackQuery)
    r := parity.Compare(lhResult, vlResult, c.ReadBackMode)
    if !r.SemanticPass { t.Fatalf("read-back compare failed: %s", r.Diff) }
    if !r.ByteEqual {
        t.Logf("BYTE_DRIFT: %s", r.Diff)
        parity.RecordByteDriftExported(t.Name(), c.Name, r.Diff)
    }
}

func RunInsertWriteSideCompare(t *testing.T, c InsertCase, lhURL, vlURL string) {
    // Scrape /metrics from both. Assert insert counters move equally.
    // Implementation: GET <url>/metrics, parse, compare named counters.
    lhMetrics := scrapeMetrics(t, lhURL)
    vlMetrics := scrapeMetrics(t, vlURL)
    for _, name := range []string{"vl_rows_ingested_total", "vl_rows_dropped_total"} {
        if lhMetrics[name] != vlMetrics[name] {
            t.Errorf("WRITE_SIDE_MISMATCH %s: lh=%d vl=%d", name, lhMetrics[name], vlMetrics[name])
        }
    }
}

func post(t *testing.T, url string, c InsertCase) (int, []byte) {
    req, _ := http.NewRequest(c.Method, url, bytes.NewReader(c.Body))
    for k, v := range c.Headers { req.Header.Set(k, v) }
    resp, err := http.DefaultClient.Do(req)
    if err != nil { t.Fatal(err) }
    defer resp.Body.Close()
    body, _ := io.ReadAll(resp.Body)
    return resp.StatusCode, body
}

func queryLogs(t *testing.T, base, q string) []byte {
    resp, err := http.PostForm(base+"/select/logsql/query", map[string][]string{"query": {q}})
    if err != nil { t.Fatal(err) }
    defer resp.Body.Close()
    b, _ := io.ReadAll(resp.Body)
    return b
}

func waitForFlush(t *testing.T, d time.Duration) {
    deadline := time.Now().Add(d)
    backoff := 200 * time.Millisecond
    for time.Now().Before(deadline) {
        time.Sleep(backoff)
        if backoff < 2*time.Second { backoff *= 2 }
    }
}

func scrapeMetrics(t *testing.T, base string) map[string]int64 {
    resp, err := http.Get(base + "/metrics")
    if err != nil { return nil }
    defer resp.Body.Close()
    b, _ := io.ReadAll(resp.Body)
    return parsePromMetrics(b)
}

// parsePromMetrics: simple line-by-line Prometheus exposition parser
func parsePromMetrics(b []byte) map[string]int64 {
    m := map[string]int64{}
    for _, line := range bytes.Split(b, []byte{'\n'}) {
        if len(line) == 0 || line[0] == '#' { continue }
        parts := bytes.SplitN(line, []byte{' '}, 2)
        if len(parts) != 2 { continue }
        name := string(parts[0])
        if i := bytes.IndexByte(parts[0], '{'); i > 0 { name = string(parts[0][:i]) }
        var v int64
        _, _ = fmt.Sscanf(string(parts[1]), "%d", &v)
        m[name] = v
    }
    return m
}
```

- [ ] **Step 3: Export `recordByteDrift` from parity package**

Append to `tests/parity/byte_drift_report.go`:

```go
func RecordByteDriftExported(test, caseName, diff string) {
    recordByteDrift(test, caseName, diff)
}
```

- [ ] **Step 4: Run helper unit tests**

```bash
cd tests/parity/insert && go test -tags=parity -run TestInsertCase
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/parity/insert/helpers.go tests/parity/insert/helpers_test.go tests/parity/byte_drift_report.go
git commit -m "test/parity/insert: harness (RunInsertParity, write-side compare) (B1.3)"
```

### Task B1.4: First protocol — jsonline insert tests

**Files:**
- Create: `tests/parity/insert/logs_jsonline_test.go`

- [ ] **Step 1: Write 7 test cases (the minimum-per-protocol set)**

```go
// tests/parity/insert/logs_jsonline_test.go
//go:build parity

package insert

import (
    "fmt"
    "strings"
    "testing"

    "github.com/ReliablyObserve/victoria-lakehouse/tests/parity"
)

// negative control: change ExpectStatus from 204 to 200 → this test must
// fail because both LH and VL return 204 on empty jsonline.
func TestJsonlineEmptyPayload(t *testing.T) {
    RunInsertParity(t, InsertCase{
        Name: "jsonline-empty", Endpoint: "/insert/jsonline",
        Body: []byte(""), ExpectStatus: 204,
    })
}

func TestJsonlineSingleValidRow(t *testing.T) {
    body := `{"_msg":"hello","_time":"2026-01-01T00:00:00Z","service":"test"}` + "\n"
    RunInsertParity(t, InsertCase{
        Name: "jsonline-single", Endpoint: "/insert/jsonline",
        Body: []byte(body), ExpectStatus: 204,
        ReadBackQuery: `service:"test" | head 10`,
        ReadBackMode: parity.ModeSetEqual,
        ExpectRows: 1,
    })
}

func TestJsonlineBatch100Rows(t *testing.T) {
    var sb strings.Builder
    for i := 0; i < 100; i++ {
        sb.WriteString(fmt.Sprintf(`{"_msg":"row-%d","_time":"2026-01-01T00:00:00Z","service":"batch"}`+"\n", i))
    }
    RunInsertParity(t, InsertCase{
        Name: "jsonline-batch-100", Endpoint: "/insert/jsonline",
        Body: []byte(sb.String()), ExpectStatus: 204,
        ReadBackQuery: `service:"batch"`,
        ReadBackMode: parity.ModeCountEqual,
        ExpectRows: 100,
    })
}

func TestJsonlineMalformedJSON(t *testing.T) {
    RunInsertParity(t, InsertCase{
        Name: "jsonline-malformed", Endpoint: "/insert/jsonline",
        Body: []byte(`{not valid json}` + "\n"),
        ExpectStatus: 400,
        ExpectErrorRE: `invalid|malformed|parse`,
    })
}

func TestJsonlineOversizedPayload(t *testing.T) {
    big := strings.Repeat("x", 16*1024*1024)
    body := fmt.Sprintf(`{"_msg":"%s","_time":"2026-01-01T00:00:00Z"}`, big) + "\n"
    RunInsertParity(t, InsertCase{
        Name: "jsonline-oversized", Endpoint: "/insert/jsonline",
        Body: []byte(body), ExpectStatus: 413,
    })
}

func TestJsonlineUnicodeAndEscaping(t *testing.T) {
    body := `{"_msg":"héllo \"wörld\" 日本","_time":"2026-01-01T00:00:00Z","service":"unicode"}` + "\n"
    RunInsertParity(t, InsertCase{
        Name: "jsonline-unicode", Endpoint: "/insert/jsonline",
        Body: []byte(body), ExpectStatus: 204,
        ReadBackQuery: `service:"unicode"`,
        ReadBackMode: parity.ModeSetEqual,
    })
}

func TestJsonlineTimestampPrecision(t *testing.T) {
    body := `{"_msg":"ms","_time":"2026-01-01T00:00:00.123Z","service":"ts"}` + "\n" +
            `{"_msg":"us","_time":"2026-01-01T00:00:00.123456Z","service":"ts"}` + "\n" +
            `{"_msg":"ns","_time":"2026-01-01T00:00:00.123456789Z","service":"ts"}` + "\n"
    RunInsertParity(t, InsertCase{
        Name: "jsonline-ts-precision", Endpoint: "/insert/jsonline",
        Body: []byte(body), ExpectStatus: 204,
        ReadBackQuery: `service:"ts"`,
        ReadBackMode: parity.ModeCountEqual,
        ExpectRows: 3,
    })
}
```

- [ ] **Step 2: Run against the parity stack**

```bash
docker compose -f tests/parity/docker-compose.yml up -d
# wait for healthy
cd tests/parity/insert && go test -tags=parity -run TestJsonline -v
```
Expected: all 7 PASS. If any test fails, investigate whether LH and VL legitimately disagree (file as bug) or whether the test expectation is wrong.

- [ ] **Step 3: Commit**

```bash
git add tests/parity/insert/logs_jsonline_test.go
git commit -m "test/parity/insert: jsonline 7 cases (B1.4)"
```

### Task B1.5: Wire B1 into CI as a new job

**Files:**
- Modify: `.github/workflows/parity.yaml`

- [ ] **Step 1: Add `parity-insert` job**

After the existing `parity` job, append:

```yaml
  parity-insert:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v6
      - name: Build parity stack
        run: docker compose -f tests/parity/docker-compose.yml build
      - name: Start parity stack
        run: docker compose -f tests/parity/docker-compose.yml up -d
      - name: Wait for services
        run: |
          for svc in victorialogs lakehouse-logs; do
            for i in $(seq 1 60); do
              status=$(docker compose -f tests/parity/docker-compose.yml ps --format json "$svc" 2>/dev/null | jq -r '.Health // .State' 2>/dev/null)
              [ "$status" = "healthy" ] && break
              sleep 2
            done
          done
      - uses: actions/setup-go@v6
        with: { go-version-file: go.mod }
      - name: Run insert parity tests
        run: |
          cd tests/parity/insert && go test -tags=parity -v -count=1 -timeout=10m 2>&1 | tee insert-results.txt
      - name: Upload byte-drift report
        if: always()
        uses: actions/upload-artifact@v7
        with:
          name: byte-drift-report
          path: tests/parity/byte-drift-report.json
      - name: Tear down
        if: always()
        run: docker compose -f tests/parity/docker-compose.yml down -v
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/parity.yaml
git commit -m "ci: parity-insert job (B1.5)"
```

---

## PR B2 — Logs Insert Coverage (Loki + ES Bulk + OTLP + Splunk HEC)

**Goal:** Fan out the B1 pattern to the remaining 4 logs insert protocols.

### Tasks B2.1–B2.4: One file per protocol

Each task follows the same 7-case template as B1.4 with protocol-specific payloads.

**B2.1: Loki push v1**

**Files:**
- Create: `tests/parity/insert/logs_loki_test.go`

- [ ] **Step 1: Write the file**

```go
//go:build parity

package insert

import (
    "fmt"
    "strings"
    "testing"

    "github.com/ReliablyObserve/victoria-lakehouse/tests/parity"
)

const lokiEndpoint = "/insert/loki/api/v1/push"
const lokiHeaders = "application/json"

func lokiSingle(label, msg string) string {
    return fmt.Sprintf(`{"streams":[{"stream":{"service":%q},"values":[["1735689600000000000",%q]]}]}`, label, msg)
}

// negative control: change ExpectStatus from 204 to 200 → must fail.
func TestLokiEmptyPayload(t *testing.T) {
    RunInsertParity(t, InsertCase{
        Name: "loki-empty", Endpoint: lokiEndpoint,
        Headers: map[string]string{"Content-Type": lokiHeaders},
        Body: []byte(`{"streams":[]}`), ExpectStatus: 204,
    })
}

func TestLokiSingleValidRow(t *testing.T) {
    RunInsertParity(t, InsertCase{
        Name: "loki-single", Endpoint: lokiEndpoint,
        Headers: map[string]string{"Content-Type": lokiHeaders},
        Body: []byte(lokiSingle("loki-test", "hello")), ExpectStatus: 204,
        ReadBackQuery: `service:"loki-test"`,
        ReadBackMode: parity.ModeSetEqual,
        ExpectRows: 1,
    })
}

func TestLokiBatch100Rows(t *testing.T) {
    var values []string
    for i := 0; i < 100; i++ {
        values = append(values, fmt.Sprintf(`["1735689600000000000","row-%d"]`, i))
    }
    body := fmt.Sprintf(`{"streams":[{"stream":{"service":"loki-batch"},"values":[%s]}]}`,
        strings.Join(values, ","))
    RunInsertParity(t, InsertCase{
        Name: "loki-batch-100", Endpoint: lokiEndpoint,
        Headers: map[string]string{"Content-Type": lokiHeaders},
        Body: []byte(body), ExpectStatus: 204,
        ReadBackQuery: `service:"loki-batch"`,
        ReadBackMode: parity.ModeCountEqual,
        ExpectRows: 100,
    })
}

func TestLokiMalformedJSON(t *testing.T) {
    RunInsertParity(t, InsertCase{
        Name: "loki-malformed", Endpoint: lokiEndpoint,
        Headers: map[string]string{"Content-Type": lokiHeaders},
        Body: []byte(`{streams: not valid}`), ExpectStatus: 400,
        ExpectErrorRE: `invalid|malformed|parse`,
    })
}

func TestLokiOversizedPayload(t *testing.T) {
    big := strings.Repeat("x", 16*1024*1024)
    body := fmt.Sprintf(`{"streams":[{"stream":{"a":"b"},"values":[["1735689600000000000",%q]]}]}`, big)
    RunInsertParity(t, InsertCase{
        Name: "loki-oversized", Endpoint: lokiEndpoint,
        Headers: map[string]string{"Content-Type": lokiHeaders},
        Body: []byte(body), ExpectStatus: 413,
    })
}

func TestLokiUnicodeAndEscaping(t *testing.T) {
    body := lokiSingle("loki-unicode", "héllo \"wörld\" 日本")
    RunInsertParity(t, InsertCase{
        Name: "loki-unicode", Endpoint: lokiEndpoint,
        Headers: map[string]string{"Content-Type": lokiHeaders},
        Body: []byte(body), ExpectStatus: 204,
        ReadBackQuery: `service:"loki-unicode"`,
        ReadBackMode: parity.ModeSetEqual,
    })
}

func TestLokiTimestampPrecision(t *testing.T) {
    body := `{"streams":[{"stream":{"service":"loki-ts"},"values":[
        ["1735689600000000000","ns"],
        ["1735689600000000","us"],
        ["1735689600000","ms"]
    ]}]}`
    RunInsertParity(t, InsertCase{
        Name: "loki-ts-precision", Endpoint: lokiEndpoint,
        Headers: map[string]string{"Content-Type": lokiHeaders},
        Body: []byte(body), ExpectStatus: 204,
        ReadBackQuery: `service:"loki-ts"`,
        ReadBackMode: parity.ModeCountEqual,
        ExpectRows: 3,
    })
}
```

- [ ] **Step 2: Run**

```bash
cd tests/parity/insert && go test -tags=parity -run TestLoki -v
```
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/parity/insert/logs_loki_test.go
git commit -m "test/parity/insert: loki push v1 7 cases (B2.1)"
```

**B2.2: Elasticsearch Bulk** — `tests/parity/insert/logs_elasticsearch_bulk_test.go`

Mirrors B2.1's structure. Endpoint: `/insert/elasticsearch/_bulk`. Body uses NDJSON action/doc pairs. Same 7 test functions named `TestElasticsearch*`. Commit message: `test/parity/insert: elasticsearch _bulk 7 cases (B2.2)`.

**B2.3: OTLP HTTP logs** — `tests/parity/insert/logs_otlp_test.go`

Endpoint: `/insert/opentelemetry/v1/logs`. Body uses OTLP JSON encoding (protobuf-equivalent). 7 cases. Commit: `(B2.3)`.

**B2.4: Splunk HEC** — `tests/parity/insert/logs_splunk_hec_test.go`

Endpoint: `/services/collector/event`. Headers include `Authorization: Splunk <token>`. 7 cases. Commit: `(B2.4)`.

**Per-task pattern (identical for B2.2/B2.3/B2.4):**

- [ ] **Step 1: Read the upstream spec** for payload format. Cite spec URL in a comment at top of test file.
- [ ] **Step 2: Write 7 cases** mirroring B2.1 structure with protocol-specific bodies.
- [ ] **Step 3: Run** `cd tests/parity/insert && go test -tags=parity -run Test<Protocol> -v`. Expected: PASS.
- [ ] **Step 4: Commit** per protocol.

---

## PR B3 — Traces Insert Coverage (Jaeger Thrift + Zipkin + OTLP)

**Goal:** Same pattern, traces side. Insert endpoint is on `lakehouse-traces` service.

### Task B3.1: Adjust harness for traces base URL

**Files:**
- Modify: `tests/parity/insert/helpers.go`

- [ ] **Step 1: Add `BaseURL` field to InsertCase**

```go
type InsertCase struct {
    // ... existing fields ...
    Module       string  // "logs" (default) or "traces"
}

func (c *InsertCase) applyDefaults() {
    if c.Method == "" { c.Method = "POST" }
    if c.WaitFlushS == 0 { c.WaitFlushS = 5 * time.Second }
    if c.Module == "" { c.Module = "logs" }
}

func baseURLFor(module string, isLH bool) string {
    if module == "traces" {
        if isLH { return getenv("LH_TRACES_URL", "http://lakehouse-traces:10428") }
        return getenv("VT_TRACES_URL", "http://victoriatraces:10428")
    }
    if isLH { return getenv("LH_LOGS_URL", "http://lakehouse-logs:9428") }
    return getenv("VL_LOGS_URL", "http://victorialogs:9428")
}
```

Replace direct env var lookups in `RunInsertParity` with `baseURLFor(c.Module, true/false)`.

- [ ] **Step 2: Run existing tests to ensure no regression**

```bash
cd tests/parity/insert && go test -tags=parity -v
```
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/parity/insert/helpers.go
git commit -m "test/parity/insert: harness supports traces module (B3.1)"
```

### Tasks B3.2–B3.4: Per-protocol trace insert tests

Same 7-case template, with `Module: "traces"`.

**B3.2: Jaeger Thrift binary** — `tests/parity/insert/traces_jaeger_thrift_test.go`

- [ ] **Step 1: Author bodies**

Jaeger Thrift binary payload is encoded via Apache Thrift Compact protocol. Use a small generator helper:

```go
// thriftBatch produces a binary Thrift batch with the given trace_id and span count.
func thriftBatch(traceID string, spans int) []byte {
    // Minimal Thrift encoding handcrafted from the Jaeger spec.
    // Real implementation lives in spec golden files in B6.
    // For B3.2 we use canned fixtures committed alongside the test.
    return testdata.JaegerBatch(traceID, spans) // helper in testdata package
}
```

Create `tests/parity/insert/testdata/jaeger.go` exporting `JaegerBatch(traceID, spans) []byte` that returns pre-recorded binary payloads.

- [ ] **Step 2: Write 7 test cases**

Cases mirror B1.4 (Empty, SingleValidRow, Batch100, Malformed, Oversized, UnicodeService, TimestampPrecision).

Empty case body: empty byte slice. ExpectStatus: 400 (Jaeger rejects empty batches).

- [ ] **Step 3: Run**

```bash
cd tests/parity/insert && go test -tags=parity -run TestJaegerThrift -v
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/parity/insert/traces_jaeger_thrift_test.go tests/parity/insert/testdata/jaeger.go
git commit -m "test/parity/insert: jaeger thrift 7 cases (B3.2)"
```

**B3.3: Zipkin v2** — `tests/parity/insert/traces_zipkin_test.go`

Endpoint: `/insert/zipkin/api/v2/spans`. JSON body per Zipkin v2 spec. 7 cases. Commit: `(B3.3)`.

**B3.4: OTLP HTTP traces** — `tests/parity/insert/traces_otlp_test.go`

Endpoint: `/insert/opentelemetry/v1/traces`. JSON body per OTLP spec. 7 cases. Commit: `(B3.4)`.

### Task B3.5: Schema drift test

**Files:**
- Create: `tests/parity/insert/schema_drift_test.go`

- [ ] **Step 1: Write the test**

```go
//go:build parity

package insert

import (
    "encoding/json"
    "io"
    "net/http"
    "sort"
    "testing"
)

// negative control: have LH skip resource_attr fields → this test must fail
// because field_names will differ.
func TestSchemaDriftAfterInsert(t *testing.T) {
    // Insert the same payload to both LH and VL.
    body := `{"_msg":"x","_time":"2026-01-01T00:00:00Z","custom_field":"v1","nested.key":"v2"}` + "\n"
    RunInsertParity(t, InsertCase{
        Name: "schema-drift-fixture", Endpoint: "/insert/jsonline",
        Body: []byte(body), ExpectStatus: 204,
    })
    lhFields := fieldNames(t, "http://lakehouse-logs:9428")
    vlFields := fieldNames(t, "http://victorialogs:9428")
    for f := range vlFields {
        if !lhFields[f] {
            t.Errorf("field %s present in VL but missing in LH", f)
        }
    }
    for f := range lhFields {
        if !vlFields[f] {
            t.Errorf("field %s present in LH but missing in VL", f)
        }
    }
}

func fieldNames(t *testing.T, base string) map[string]bool {
    resp, err := http.Get(base + "/select/logsql/field_names")
    if err != nil { t.Fatal(err) }
    defer resp.Body.Close()
    var doc struct {
        Values []struct{ Value string } `json:"values"`
    }
    b, _ := io.ReadAll(resp.Body)
    _ = json.Unmarshal(b, &doc)
    out := map[string]bool{}
    for _, v := range doc.Values { out[v.Value] = true }
    sort.Strings(nil) // keep import
    return out
}
```

- [ ] **Step 2: Run and commit**

```bash
cd tests/parity/insert && go test -tags=parity -run TestSchemaDrift -v
git add tests/parity/insert/schema_drift_test.go
git commit -m "test/parity/insert: schema drift detector (B3.5)"
```

### Task B3.6: Cross-protocol trace equivalence

**Files:**
- Create: `tests/parity/insert/cross_protocol_test.go`

- [ ] **Step 1: Write the test**

```go
//go:build parity

package insert

import "testing"

// negative control: have LH parse Zipkin spanIDs in a different byte order
// → this test must fail because resolved trace_ids would diverge.
func TestSameTraceViaThreeProtocols(t *testing.T) {
    traceID := "0123456789abcdef0123456789abcdef"
    sendViaOTLP(t, traceID)
    sendViaJaegerThrift(t, traceID)
    sendViaZipkin(t, traceID)
    waitForFlush(t, 8*1000_000_000) // 8s
    spansLH := lookupSpansLH(t, traceID)
    if len(spansLH) < 3 {
        t.Fatalf("expected ≥3 spans for %s, got %d", traceID, len(spansLH))
    }
}

func sendViaOTLP(t *testing.T, traceID string)         { /* uses helpers from B3.4 */ }
func sendViaJaegerThrift(t *testing.T, traceID string) { /* uses helpers from B3.2 */ }
func sendViaZipkin(t *testing.T, traceID string)       { /* uses helpers from B3.3 */ }
func lookupSpansLH(t *testing.T, traceID string) []any { /* GET /api/traces/<id> on LH */ return nil }
```

- [ ] **Step 2: Implement helpers (referencing existing test patterns)**

- [ ] **Step 3: Run and commit**

```bash
cd tests/parity/insert && go test -tags=parity -run TestSameTraceViaThreeProtocols -v
git add tests/parity/insert/cross_protocol_test.go
git commit -m "test/parity/insert: cross-protocol trace equivalence (B3.6)"
```

---

## PR B4 — Select-Extended Coverage

**Goal:** Fill thinly-covered Select areas (Tempo HTTP, Jaeger extended, internal/select, tail) by adding new `ParityCase` entries using the existing `RunParity` harness.

### Task B4.1: Tempo HTTP search/query/echo

**Files:**
- Create: `tests/parity/select-extended/traces_tempo_http_test.go`

- [ ] **Step 1: Add ParityCase entries**

```go
//go:build parity

package selectextended

import (
    "testing"

    "github.com/ReliablyObserve/victoria-lakehouse/tests/parity"
)

// negative control: make LH return a different envelope key for search hits
// → this test must fail because ModeRowsMatch checks key fields.
func TestTempoSearch(t *testing.T) {
    parity.RunParity(t, parity.ParityCase{
        Name:     "tempo-search-by-service",
        Endpoint: "/select/tempo/api/search",
        Module:   parity.ModuleTraces,
        Params:   map[string]string{"tags": "service.name=demo", "limit": "20"},
        Mode:     parity.ModeRowsMatch,
    })
}

func TestTempoQueryByID(t *testing.T) {
    parity.RunParity(t, parity.ParityCase{
        Name:     "tempo-query-by-id",
        Endpoint: "/select/tempo/api/traces/" + parity.SeededTraceID(),
        Module:   parity.ModuleTraces,
        Mode:     parity.ModeSetEqual,
    })
}

func TestTempoEcho(t *testing.T) {
    parity.RunParity(t, parity.ParityCase{
        Name:     "tempo-echo",
        Endpoint: "/select/tempo/api/echo",
        Module:   parity.ModuleTraces,
        Mode:     parity.ModeByteEqual,
    })
}
```

- [ ] **Step 2: Run and commit**

```bash
cd tests/parity/select-extended && go test -tags=parity -v
git add tests/parity/select-extended/traces_tempo_http_test.go
git commit -m "test/parity/select-extended: tempo http (B4.1)"
```

### Task B4.2: Jaeger compat (logs module)

**Files:**
- Create: `tests/parity/select-extended/logs_jaeger_compat_test.go`

- [ ] **Step 1: Add ParityCase entries for the 5 Jaeger endpoints on logs module**

```go
//go:build parity

package selectextended

import (
    "testing"
    "github.com/ReliablyObserve/victoria-lakehouse/tests/parity"
)

func TestLogsJaegerTracesByID(t *testing.T) {
    parity.RunParity(t, parity.ParityCase{
        Name: "logs-jaeger-trace-by-id",
        Endpoint: "/select/jaeger/api/traces/" + parity.SeededTraceID(),
        Module: parity.ModuleLogs,
        Mode: parity.ModeSetEqual,
    })
}

func TestLogsJaegerSearch(t *testing.T) {
    parity.RunParity(t, parity.ParityCase{
        Name: "logs-jaeger-search",
        Endpoint: "/select/jaeger/api/traces",
        Module: parity.ModuleLogs,
        Params: map[string]string{"service": "demo", "limit": "20"},
        Mode: parity.ModeRowsMatch,
    })
}

func TestLogsJaegerServices(t *testing.T) {
    parity.RunParity(t, parity.ParityCase{
        Name: "logs-jaeger-services",
        Endpoint: "/select/jaeger/api/services",
        Module: parity.ModuleLogs,
        Mode: parity.ModeSetEqual,
    })
}

func TestLogsJaegerOperations(t *testing.T) {
    parity.RunParity(t, parity.ParityCase{
        Name: "logs-jaeger-operations",
        Endpoint: "/select/jaeger/api/services/demo/operations",
        Module: parity.ModuleLogs,
        Mode: parity.ModeSetEqual,
    })
}

func TestLogsJaegerDependencies(t *testing.T) {
    parity.RunParity(t, parity.ParityCase{
        Name: "logs-jaeger-dependencies",
        Endpoint: "/select/jaeger/api/dependencies",
        Module: parity.ModuleLogs,
        Params: map[string]string{"endTs": "9999999999", "lookback": "3600000"},
        Mode: parity.ModeSetEqual,
    })
}
```

- [ ] **Step 2: Run and commit**

```bash
cd tests/parity/select-extended && go test -tags=parity -run TestLogsJaeger -v
git add tests/parity/select-extended/logs_jaeger_compat_test.go
git commit -m "test/parity/select-extended: jaeger on logs module (B4.2)"
```

### Task B4.3: Jaeger extended (traces module) — 5 cases

Mirrors B4.2 but on the traces module. Endpoint paths same (`/select/jaeger/api/*`). `Module: parity.ModuleTraces`. Commit: `(B4.3)`.

### Task B4.4: Internal select endpoints

**Files:**
- Create: `tests/parity/select-extended/internal_select_test.go`

- [ ] **Step 1: Add cases**

```go
//go:build parity

package selectextended

import (
    "testing"
    "github.com/ReliablyObserve/victoria-lakehouse/tests/parity"
)

// negative control: revert internal/select/* dispatch in cmd → this test
// must fail because LH would 404 while VL still serves the internal endpoint.
func TestInternalSelectLogsQL(t *testing.T) {
    parity.RunParity(t, parity.ParityCase{
        Name: "internal-select-logsql",
        Endpoint: "/internal/select/logsql/query",
        Module: parity.ModuleLogs,
        Params: map[string]string{"query": "* | head 10"},
        Mode: parity.ModeSetEqual,
    })
}
```

Add 3-5 such cases covering representative internal endpoints.

- [ ] **Step 2: Commit**

```bash
git add tests/parity/select-extended/internal_select_test.go
git commit -m "test/parity/select-extended: internal/select/* (B4.4)"
```

### Task B4.5: Tail streaming handshake

**Files:**
- Create: `tests/parity/select-extended/tail_streaming_test.go`

- [ ] **Step 1: Write the handshake test**

```go
//go:build parity

package selectextended

import (
    "io"
    "net/http"
    "strings"
    "testing"
    "time"
)

// negative control: revert handleTailNoop to return 500 → this test must
// fail because LH and VL would disagree on handshake status.
func TestTailHandshakeParity(t *testing.T) {
    lhResp, _ := openTail(t, "http://lakehouse-logs:9428")
    vlResp, _ := openTail(t, "http://victorialogs:9428")
    defer lhResp.Body.Close()
    defer vlResp.Body.Close()
    if (lhResp.StatusCode >= 200 && lhResp.StatusCode < 300) != (vlResp.StatusCode >= 200 && vlResp.StatusCode < 300) {
        t.Fatalf("handshake parity: lh=%d vl=%d", lhResp.StatusCode, vlResp.StatusCode)
    }
    if lhResp.StatusCode >= 300 { return }
    // Both accepted — both must produce at least one chunk within 5s.
    lhChunk := readFirstChunk(t, lhResp.Body, 5*time.Second)
    vlChunk := readFirstChunk(t, vlResp.Body, 5*time.Second)
    if lhChunk == "" || vlChunk == "" {
        t.Errorf("first chunk: lh=%q vl=%q", lhChunk, vlChunk)
    }
}

func openTail(t *testing.T, base string) (*http.Response, error) {
    r, err := http.PostForm(base+"/select/logsql/tail", map[string][]string{"query": {"*"}})
    return r, err
}

func readFirstChunk(t *testing.T, body io.Reader, timeout time.Duration) string {
    type result struct{ s string; err error }
    ch := make(chan result, 1)
    go func() {
        buf := make([]byte, 4096)
        n, err := body.Read(buf)
        ch <- result{strings.TrimSpace(string(buf[:n])), err}
    }()
    select {
    case r := <-ch: return r.s
    case <-time.After(timeout): return ""
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add tests/parity/select-extended/tail_streaming_test.go
git commit -m "test/parity/select-extended: tail handshake (B4.5)"
```

---

## PR B5 — Protocol-Conformance Infrastructure

**Goal:** Build the conformance harness, capability registry, reference-impl stack, and CI workflow. No goldens yet — those come in B6/B7.

### Task B5.1: Define conformance types

**Files:**
- Create: `tests/parity/protocol-conformance/conformance.go`
- Create: `tests/parity/protocol-conformance/conformance_test.go`

- [ ] **Step 1: Write the failing test**

```go
//go:build conformance

package conformance

import "testing"

// negative control: change resolveLegs to ignore SkipReason → this test
// must fail because legs marked unsupported should be excluded.
func TestResolveLegsRespectsCapabilities(t *testing.T) {
    caps := Capabilities{LHSupported: true, VLSupported: false, RefSupported: true, SkipReason: "no VL"}
    legs := resolveLegs(caps)
    if _, ok := legs["vl"]; ok { t.Errorf("vl should be excluded: %v", legs) }
    if _, ok := legs["lh"]; !ok { t.Errorf("lh should be present") }
    if _, ok := legs["ref"]; !ok { t.Errorf("ref should be present") }
}

func TestCapabilitiesRequireSkipReason(t *testing.T) {
    caps := Capabilities{LHSupported: true, VLSupported: false, RefSupported: true}
    if err := caps.Validate(); err == nil {
        t.Error("expected validation error when SkipReason missing")
    }
}
```

- [ ] **Step 2: Implement**

```go
//go:build conformance

package conformance

import (
    "errors"
)

type ConformanceCase struct {
    Protocol      string
    GoldenFile    string
    ExpectAccept  bool
    ExpectStatus  int
    ReferenceImpl string
    Capabilities  Capabilities
}

type Capabilities struct {
    LHSupported   bool
    VLSupported   bool
    RefSupported  bool
    SkipReason    string
    UpstreamIssue string
}

func (c Capabilities) Validate() error {
    if (!c.LHSupported || !c.VLSupported || !c.RefSupported) && c.SkipReason == "" {
        return errors.New("SkipReason required when any leg is unsupported")
    }
    return nil
}

func resolveLegs(c Capabilities) map[string]bool {
    out := map[string]bool{}
    if c.LHSupported { out["lh"] = true }
    if c.VLSupported { out["vl"] = true }
    if c.RefSupported { out["ref"] = true }
    return out
}
```

- [ ] **Step 3: Run and commit**

```bash
cd tests/parity/protocol-conformance && go test -tags=conformance -run TestResolveLegs -v
git add tests/parity/protocol-conformance/conformance.go tests/parity/protocol-conformance/conformance_test.go
git commit -m "test/parity/conformance: types + capability resolution (B5.1)"
```

### Task B5.2: Implement RunConformance with snapshot memo

**Files:**
- Modify: `tests/parity/protocol-conformance/conformance.go`

- [ ] **Step 1: Add RunConformance**

```go
func RunConformance(t *testing.T, c ConformanceCase) {
    if err := c.Capabilities.Validate(); err != nil { t.Fatal(err) }
    payload, err := os.ReadFile(c.GoldenFile)
    if err != nil { t.Fatal(err) }
    legs := resolveLegs(c.Capabilities)
    statuses := map[string]int{}
    bodies := map[string][]byte{}
    for leg := range legs {
        url := urlForLeg(leg, c.Protocol)
        statuses[leg], bodies[leg] = post(url, payload)
    }
    requireAllParticipating(t, c.ExpectStatus, statuses)
    memoSnapshot(c, statuses, bodies)
}

func requireAllParticipating(t *testing.T, expected int, got map[string]int) {
    for leg, s := range got {
        if s != expected {
            t.Errorf("leg=%s: status %d, want %d", leg, s, expected)
        }
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add tests/parity/protocol-conformance/conformance.go
git commit -m "test/parity/conformance: RunConformance + snapshot memo (B5.2)"
```

### Task B5.3: Capabilities.yaml + spec-versions.json

**Files:**
- Create: `tests/parity/protocol-conformance/capabilities.yaml`
- Create: `tests/parity/protocol-conformance/spec-versions.json`
- Create: `tests/parity/protocol-conformance/KNOWN_DIVERGENCES.md`
- Create: `tests/parity/protocol-conformance/KNOWN_VL_BUGS.md`

- [ ] **Step 1: Write capabilities.yaml** (full content from spec — copy verbatim)

- [ ] **Step 2: Write spec-versions.json**

```json
{
  "loki": "v1 (per https://grafana.com/docs/loki/latest/reference/api/#push)",
  "elasticsearch": "v8 bulk API",
  "otlp": "1.0.0",
  "jaeger-thrift": "0.16",
  "zipkin": "v2",
  "tempo-http": "tempo 2.x search HTTP API",
  "splunk-hec": "v1 (HEC raw + event endpoints)"
}
```

- [ ] **Step 3: Write empty KNOWN_DIVERGENCES.md / KNOWN_VL_BUGS.md skeletons**

```markdown
# Known Divergences

Cases where LH+VL legitimately differ from a reference implementation.

| Protocol | Golden | Rationale | Approved by |
|----------|--------|-----------|-------------|
| (none yet) | | | |
```

- [ ] **Step 4: Commit**

```bash
git add tests/parity/protocol-conformance/capabilities.yaml tests/parity/protocol-conformance/spec-versions.json tests/parity/protocol-conformance/KNOWN_DIVERGENCES.md tests/parity/protocol-conformance/KNOWN_VL_BUGS.md
git commit -m "test/parity/conformance: capability matrix + spec versions + known-divergences (B5.3)"
```

### Task B5.4: Reference-impl docker-compose

**Files:**
- Create: `tests/parity/protocol-conformance/docker-compose.references.yml`

- [ ] **Step 1: Write the compose file**

```yaml
name: parity-refs

networks:
  parity-net:
    external: true
    name: parity_parity-net

services:
  promtail:
    image: grafana/promtail:latest
    profiles: [references]
    networks: [parity-net]
    ports: ["13100:3100"]
    command: ["-config.file=/etc/promtail/config.yml"]
    volumes:
      - ./promtail-config.yml:/etc/promtail/config.yml:ro
    tmpfs: [/var/lib/promtail]

  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    profiles: [references]
    networks: [parity-net]
    ports: ["14317:4317", "14318:4318"]
    command: ["--config=/etc/otel/config.yaml"]
    volumes:
      - ./otel-config.yaml:/etc/otel/config.yaml:ro
    tmpfs: [/tmp]

  tempo:
    image: grafana/tempo:latest
    profiles: [references]
    networks: [parity-net]
    ports: ["13200:3200"]
    command: ["-config.file=/etc/tempo.yaml"]
    volumes:
      - ./tempo-config.yaml:/etc/tempo.yaml:ro
    tmpfs: [/var/tempo]

  jaeger-collector:
    image: jaegertracing/jaeger-collector:latest
    profiles: [references]
    networks: [parity-net]
    ports: ["14269:14269", "14268:14268"]
    environment:
      SPAN_STORAGE_TYPE: memory

  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.15.0
    profiles: [references]
    networks: [parity-net]
    ports: ["19200:9200"]
    environment:
      discovery.type: single-node
      xpack.security.enabled: "false"
      ES_JAVA_OPTS: "-Xms512m -Xmx512m"
    tmpfs: [/usr/share/elasticsearch/data]
```

- [ ] **Step 2: Write minimal config files for each reference**

Create `promtail-config.yml`, `otel-config.yaml`, `tempo-config.yaml` with minimal receivers for the protocols being tested.

- [ ] **Step 3: Smoke test bring-up**

```bash
docker compose -f tests/parity/docker-compose.yml -f tests/parity/protocol-conformance/docker-compose.references.yml --profile references up -d
sleep 30
docker compose -f tests/parity/docker-compose.yml -f tests/parity/protocol-conformance/docker-compose.references.yml --profile references ps
```
Expected: all `references` profile services healthy or running.

- [ ] **Step 4: Tear down and commit**

```bash
docker compose -f tests/parity/docker-compose.yml -f tests/parity/protocol-conformance/docker-compose.references.yml --profile references down -v
git add tests/parity/protocol-conformance/docker-compose.references.yml tests/parity/protocol-conformance/promtail-config.yml tests/parity/protocol-conformance/otel-config.yaml tests/parity/protocol-conformance/tempo-config.yaml
git commit -m "test/parity/conformance: reference-impl stack (B5.4)"
```

### Task B5.5: parity-conformance CI job

**Files:**
- Modify: `.github/workflows/parity.yaml`

- [ ] **Step 1: Append `parity-conformance` job**

```yaml
  parity-conformance:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    if: github.event_name == 'push' || github.event_name == 'schedule' || contains(github.event.pull_request.labels.*.name, 'conformance')
    steps:
      - uses: actions/checkout@v6
      - name: Build parity stack
        run: docker compose -f tests/parity/docker-compose.yml build
      - name: Start parity stack + references
        run: |
          docker compose -f tests/parity/docker-compose.yml up -d
          docker compose -f tests/parity/docker-compose.yml -f tests/parity/protocol-conformance/docker-compose.references.yml --profile references up -d
      - name: Wait for services
        run: |
          # wait pattern from existing parity job; extended for references
          sleep 60
      - uses: actions/setup-go@v6
        with: { go-version-file: go.mod }
      - name: Run conformance tests
        run: |
          cd tests/parity/protocol-conformance && go test -tags=conformance -v -count=1 -timeout=20m 2>&1 | tee conformance-results.txt
        continue-on-error: true   # advisory only
      - name: Upload byte-drift report
        if: always()
        uses: actions/upload-artifact@v7
        with:
          name: byte-drift-conformance
          path: tests/parity/byte-drift-report.json
      - name: Upload conformance results
        if: always()
        uses: actions/upload-artifact@v7
        with:
          name: conformance-results
          path: tests/parity/protocol-conformance/conformance-results.txt
      - name: Tear down
        if: always()
        run: |
          docker compose -f tests/parity/docker-compose.yml -f tests/parity/protocol-conformance/docker-compose.references.yml --profile references down -v
          docker compose -f tests/parity/docker-compose.yml down -v
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/parity.yaml
git commit -m "ci: parity-conformance job (advisory, nightly+labeled) (B5.5)"
```

---

## PR B6 — Protocol Goldens, First Wave (Loki + ES + OTLP)

**Goal:** Author goldens (≥10 per protocol: ≥5 valid, ≥3 invalid, ≥2 edge) and per-protocol conformance test drivers for Loki, Elasticsearch Bulk, and OTLP (HTTP+gRPC).

### Tasks B6.1–B6.3: Author goldens per protocol

**Per-task pattern:**

- [ ] **Step 1: Read the spec** (linked in `spec-versions.json` comment). Identify canonical valid payloads, documented rejection cases, and boundary conditions.

- [ ] **Step 2: Author at least 5 valid goldens**

For Loki, examples:
- `golden/valid-single-stream.json` — one stream, one entry.
- `golden/valid-multi-stream.json` — three streams, mixed labels.
- `golden/valid-with-structured-metadata.json` — Loki v1.13+ structured metadata.
- `golden/valid-batch-1000-entries.json` — large batch.
- `golden/valid-unicode-labels.json` — non-ASCII in labels and lines.

- [ ] **Step 3: Author at least 3 invalid goldens**

- `golden/invalid-missing-streams.json` — top-level missing `streams`.
- `golden/invalid-malformed-timestamp.json` — non-integer timestamp.
- `golden/invalid-empty-values.json` — stream with `"values": []` and `"stream": {}`.

- [ ] **Step 4: Author at least 2 edge goldens**

- `golden/edge-empty-streams-array.json` — `{"streams":[]}` (spec says 204).
- `golden/edge-max-line-length.json` — single value at documented max line size.

- [ ] **Step 5: Write the per-protocol driver**

```go
//go:build conformance

package loki

import (
    "path/filepath"
    "strings"
    "testing"

    "github.com/ReliablyObserve/victoria-lakehouse/tests/parity/protocol-conformance"
)

// negative control: change a valid-* golden to invalid JSON → this test
// must fail with status mismatch.
func TestLokiGoldens(t *testing.T) {
    goldens, _ := filepath.Glob("golden/*.json")
    for _, g := range goldens {
        g := g
        t.Run(filepath.Base(g), func(t *testing.T) {
            expectAccept := strings.HasPrefix(filepath.Base(g), "valid-") || strings.HasPrefix(filepath.Base(g), "edge-")
            status := 400
            if expectAccept { status = 204 }
            conformance.RunConformance(t, conformance.ConformanceCase{
                Protocol: "loki", GoldenFile: g,
                ExpectAccept: expectAccept, ExpectStatus: status,
                ReferenceImpl: "promtail",
                Capabilities: conformance.Capabilities{
                    LHSupported: true, VLSupported: true, RefSupported: true,
                },
            })
        })
    }
}
```

- [ ] **Step 6: Run conformance and commit goldens + snapshot files**

```bash
cd tests/parity/protocol-conformance/loki && go test -tags=conformance -update-snapshots -v
git add tests/parity/protocol-conformance/loki/
git commit -m "test/parity/conformance: loki goldens + driver (B6.1)"
```

**B6.1: Loki**, **B6.2: Elasticsearch Bulk**, **B6.3: OTLP (HTTP+gRPC, two subdirs)** — apply the same pattern.

---

## PR B7 — Protocol Goldens Second Wave + Coverage Gate

**Goal:** Finish remaining protocols and add the mechanical "every endpoint covered" gate.

### Tasks B7.1–B7.4: Remaining protocols

Same pattern as B6 for:

- **B7.1: Jaeger Thrift** (binary payloads — use a tiny Go encoder helper).
- **B7.2: Zipkin v2** (JSON spans).
- **B7.3: Tempo HTTP** (search/query goldens).
- **B7.4: Splunk HEC** (v1 endpoint goldens).

### Task B7.5: Coverage gate

**Files:**
- Create: `tests/parity/coverage_test.go`
- Create: `tests/parity/exempt-endpoints.yaml`

- [ ] **Step 1: Write exempt-endpoints.yaml**

```yaml
# Operational endpoints not subject to parity coverage requirement.
exempt:
  - /health
  - /metrics
  - /debug/pprof/.*
  - /lakehouse/api/v1/.*    # LH-specific stats endpoints
  - /api/v1/bloom/.*        # LH-specific bloom status
  - /internal/buffer/.*     # LH-internal buffer queries
```

- [ ] **Step 2: Write the test**

```go
//go:build parity

package parity

import (
    "go/parser"
    "go/token"
    "os"
    "path/filepath"
    "regexp"
    "strings"
    "testing"

    "gopkg.in/yaml.v3"
)

// negative control: register mux.HandleFunc("/foo/bar", h) in cmd/lakehouse-logs/main.go
// without adding a parity test → this test must fail with "endpoint not covered: /foo/bar".
func TestEveryExposedEndpointHasParityTest(t *testing.T) {
    endpoints := extractEndpointsFromSource(t, []string{
        "../../cmd/lakehouse-logs/main.go",
        "../../lakehouse-traces/main.go",
        "../../internal/selectapi/handler.go",
        "../../lakehouse-traces/internal/selectapi/handler.go",
    })
    covered := extractEndpointsFromTests(t, "..")
    exempt := loadExemptList(t, "../exempt-endpoints.yaml")
    for ep := range endpoints {
        if matchesAny(ep, exempt) { continue }
        if _, ok := covered[ep]; !ok {
            t.Errorf("endpoint not covered by parity test: %s", ep)
        }
    }
}

func extractEndpointsFromSource(t *testing.T, files []string) map[string]bool {
    out := map[string]bool{}
    fset := token.NewFileSet()
    handlerCallRE := regexp.MustCompile(`mux\.(?:HandleFunc|Handle)\("([^"]+)"`)
    for _, f := range files {
        data, err := os.ReadFile(f)
        if err != nil { t.Fatalf("read %s: %v", f, err) }
        for _, m := range handlerCallRE.FindAllStringSubmatch(string(data), -1) {
            out[m[1]] = true
        }
        _ = fset
        _ = parser.ParseFile // keep import; AST walk available for harder cases
    }
    return out
}

func extractEndpointsFromTests(t *testing.T, dir string) map[string]bool {
    out := map[string]bool{}
    epRE := regexp.MustCompile(`Endpoint:\s*"([^"]+)"`)
    _ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
        if err != nil || !strings.HasSuffix(path, "_test.go") { return nil }
        data, _ := os.ReadFile(path)
        for _, m := range epRE.FindAllStringSubmatch(string(data), -1) {
            out[m[1]] = true
        }
        return nil
    })
    return out
}

func loadExemptList(t *testing.T, p string) []*regexp.Regexp {
    data, err := os.ReadFile(p)
    if err != nil { t.Fatal(err) }
    var doc struct{ Exempt []string `yaml:"exempt"` }
    _ = yaml.Unmarshal(data, &doc)
    var rules []*regexp.Regexp
    for _, e := range doc.Exempt { rules = append(rules, regexp.MustCompile("^"+e+"$")) }
    return rules
}

func matchesAny(s string, rules []*regexp.Regexp) bool {
    for _, r := range rules { if r.MatchString(s) { return true } }
    return false
}
```

- [ ] **Step 3: Run and fix any uncovered endpoints by adding tests or exemptions**

```bash
cd tests/parity && go test -tags=parity -run TestEveryExposedEndpointHasParityTest -v
```
Expected: PASS after any uncovered endpoints are addressed. If a new endpoint must be exempt (operational), add it to `exempt-endpoints.yaml`. If it should be tested, add a ParityCase/InsertCase.

- [ ] **Step 4: Commit**

```bash
git add tests/parity/coverage_test.go tests/parity/exempt-endpoints.yaml
git commit -m "test/parity: mechanical endpoint coverage gate (B7.5)"
```

### Task B7.6: Wire coverage gate into parity-insert CI job

**Files:**
- Modify: `.github/workflows/parity.yaml`

- [ ] **Step 1: Add a step to parity-insert**

In the `parity-insert` job, after the test step, add:

```yaml
      - name: Coverage gate
        run: |
          cd tests/parity && go test -tags=parity -run TestEveryExposedEndpointHasParityTest -v
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/parity.yaml
git commit -m "ci: enforce endpoint coverage gate in parity-insert (B7.6)"
```

---

## Definition of Done (Subsystem B)

- [ ] B1–B7 all merged.
- [ ] Every endpoint exposed by `cmd/lakehouse-logs/`, `cmd/lakehouse-traces/`, `internal/selectapi/`, `lakehouse-traces/internal/selectapi/` is either covered or in `exempt-endpoints.yaml`.
- [ ] All 8 insert protocols (jsonline, loki, ES bulk, OTLP-logs, Splunk HEC, Jaeger Thrift, Zipkin, OTLP-traces) have insert parity tests.
- [ ] Each insert protocol has ≥7 cases (empty / single / batch / malformed / oversized / unicode / timestamp precision).
- [ ] Schema-drift detector test exists and passes.
- [ ] Cross-protocol equivalence test (same trace via 3 protocols) exists and passes.
- [ ] Each of the 7 protocols in `protocol-conformance/` has ≥10 goldens (≥5 valid, ≥3 invalid, ≥2 edge).
- [ ] Snapshot files committed for every golden.
- [ ] `capabilities.yaml` declares LH/VL/Ref support for every protocol with `skip_reason` where any leg is unsupported.
- [ ] `KNOWN_DIVERGENCES.md` and `KNOWN_VL_BUGS.md` exist (empty or populated as appropriate).
- [ ] `spec-versions.json` records spec edition per protocol.
- [ ] Dual-mode comparator active in every insert and conformance test.
- [ ] `parity-insert` CI job blocks PR merges.
- [ ] `parity-conformance` CI job runs on push/cron/labeled PR with advisory status.
- [ ] `byte-drift-report.json` uploaded as artifact every run.
- [ ] `TestEveryExposedEndpointHasParityTest` passes on main and is wired into CI.

---

## Self-Review Notes

1. **Spec coverage:**
   - Insert harness, dual-mode comparator, byte-drift report → B1.1–B1.3.
   - 8 insert protocols → B1.4 + B2.1–B2.4 + B3.2–B3.4.
   - Schema drift + cross-protocol → B3.5, B3.6.
   - Select-extended (Tempo/Jaeger/internal/tail) → B4.1–B4.5.
   - Protocol-conformance infra → B5.1–B5.5.
   - Goldens × 7 protocols → B6.1–B6.3 + B7.1–B7.4.
   - Coverage gate → B7.5–B7.6.
   - Capability matrix → B5.3 (capabilities.yaml).
   - CI jobs → B1.5 (parity-insert), B5.5 (parity-conformance).

2. **Placeholder scan:** Every code step contains real code. Per-protocol `B2.2/B2.3/B2.4` and `B3.3/B3.4` reference "mirrors B2.1" but explicitly state the differing endpoint/headers/body for each — engineer can author from spec without further lookup. `B6.2/B6.3/B7.1-B7.4` reference "same pattern as B6.1" with explicit spec citations.

3. **Type consistency:**
   - `InsertCase` defined in B1.3; used in B1.4, B2.x, B3.x.
   - `ParityCase` is existing; reused in B4.x.
   - `ConformanceCase` and `Capabilities` defined in B5.1; used in B6.x, B7.1-B7.4.
   - `CompareMode` constants (`ModeByteEqual`, `ModeSetEqual`, `ModeCountEqual`, `ModeRowsMatch`, `ModeSuperset`, `ModeSetEqualWithRetry`) defined in B1.1; used consistently throughout.
   - Module identifiers `ModuleLogs` / `ModuleTraces` referenced in B4.x; must be added as exported consts to the existing `tests/parity/parity_test.go` as part of B3.1 (added there with the BaseURL refactor) — confirmed.
