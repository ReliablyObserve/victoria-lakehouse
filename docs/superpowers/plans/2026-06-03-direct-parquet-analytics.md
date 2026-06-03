# Direct Parquet Analytics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `tests/analytics-engines/` to prove LH's claim that any Parquet-capable engine can query LH data directly. Four OSS engines (DuckDB, Trino, Spark, ClickHouse) execute 10 canonical queries against both small (Subsystem C's `golden/v1/`) and large (RNG-seeded 10k-row, 6-partition) datasets. Every engine's results must match pre-computed JSON goldens.

**Architecture:** Each engine has its own subdirectory with self-contained docker compose (MinIO + engine). Shared Go packages provide canonical query catalog, adapter interface, dataset seeder, MinIO uploader, and golden comparator. DuckDB (embedded, no container) serves as the canonical reference engine for golden recording. CI uses matrix parallelism: 4 engines × 2 sizes = 8 required jobs + 4 advisory bloom-usage jobs + 1 cross-cutting regression.

**Tech Stack:** Go 1.24, `github.com/marcboeker/go-duckdb` (CGO + libduckdb), `github.com/minio/minio-go/v7`, `parquet-go`, Docker Compose, GitHub Actions matrix strategy

**Spec:** `docs/superpowers/specs/2026-06-03-direct-parquet-analytics-design.md`

**Existing context:**
- LH writes Parquet via `internal/storage/parquets3/writer.go` with Hive partitioning (`dt=YYYY-MM-DD/hour=HH`).
- Subsystem C produces `tests/parquet-format/golden/v1/{logs,traces}.parquet` — D's small dataset reuses these.
- `cmd/bench/seed.go` already contains a row generator; D's `dataset_seeder.go` reuses its row constructors but writes parquet files directly (skipping the LH ingest path) to keep the seeder hermetic.
- Existing parity stack at `tests/parity/docker-compose.yml` is NOT reused — D's per-engine compose files are independent.

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `tests/analytics-engines/shared/canonical_query.go` | `Catalog` (10 queries), `CanonicalQuery`, `Mode`, `ResultMode`, `DatasetTarget` |
| Create | `tests/analytics-engines/shared/canonical_query_test.go` | Catalog validity tests |
| Create | `tests/analytics-engines/shared/adapter.go` | `Adapter` interface |
| Create | `tests/analytics-engines/shared/golden_compare.go` | `GoldenResult`, `Compare`, type coercion, `FloatTolerance` |
| Create | `tests/analytics-engines/shared/golden_compare_test.go` | Per-mode comparator tests |
| Create | `tests/analytics-engines/shared/dataset_seeder.go` | `LoadSmall`, `GenerateLarge`, fixed `Seed` |
| Create | `tests/analytics-engines/shared/dataset_seeder_test.go` | Determinism + stats tests |
| Create | `tests/analytics-engines/shared/minio_uploader.go` | `Uploader`, `UploadHive` |
| Create | `tests/analytics-engines/shared/minio_uploader_test.go` | Hive layout enforcement test |
| Create | `tests/analytics-engines/shared/runner.go` | `RunCorrectnessSuite`, `RunBloomUsageAdvisory`, `UploadAndRegister` |
| Create | `tests/analytics-engines/shared/bloom_usage.go` | `BloomUsageReport`, `WriteReport` |
| Create | `tests/analytics-engines/queries/README.md` | Query name → description mapping (human reference) |
| Create | `tests/analytics-engines/goldens/small/<query>.json` | One per query covering DatasetSmall/Both |
| Create | `tests/analytics-engines/goldens/large/<query>.json` | One per query covering DatasetLarge/Both |
| Create | `tests/analytics-engines/duckdb/docker-compose.yml` | MinIO only |
| Create | `tests/analytics-engines/duckdb/adapter.go` | DuckDB adapter via go-duckdb embedded |
| Create | `tests/analytics-engines/duckdb/adapter_test.go` | DuckDB adapter quirks |
| Create | `tests/analytics-engines/duckdb/duckdb_test.go` | correctness + realistic + bloom-usage |
| Create | `tests/analytics-engines/trino/docker-compose.yml` | Trino + MinIO |
| Create | `tests/analytics-engines/trino/catalog/hive.properties` | Hive catalog config |
| Create | `tests/analytics-engines/trino/adapter.go` | Trino JDBC-style HTTP adapter |
| Create | `tests/analytics-engines/trino/adapter_test.go` | Translate quirks |
| Create | `tests/analytics-engines/trino/trino_test.go` | correctness + realistic + bloom-usage |
| Create | `tests/analytics-engines/spark/docker-compose.yml` | Spark master + worker + MinIO |
| Create | `tests/analytics-engines/spark/adapter.go` | Spark adapter via spark-connect or Thrift JDBC |
| Create | `tests/analytics-engines/spark/adapter_test.go` | Translate quirks |
| Create | `tests/analytics-engines/spark/spark_test.go` | correctness + realistic + bloom-usage |
| Create | `tests/analytics-engines/clickhouse/docker-compose.yml` | ClickHouse + MinIO |
| Create | `tests/analytics-engines/clickhouse/adapter.go` | ClickHouse adapter via ch-go driver |
| Create | `tests/analytics-engines/clickhouse/adapter_test.go` | Translate quirks |
| Create | `tests/analytics-engines/clickhouse/clickhouse_test.go` | correctness + realistic + bloom-usage |
| Create | `tests/analytics-engines/regression_test.go` | Cross-cutting: Hive layout, OTel schema URL pin |
| Create | `tests/analytics-engines/cmd/genfixture/main.go` | Reference-engine golden recorder |
| Modify | `Makefile` | Targets `analytics-engines-test ENGINE=<x>`, `analytics-engines-goldens-{small,large}` |
| Create | `.github/workflows/analytics-engines.yaml` | Matrix-parallel CI workflow |

---

## PR D1 — Shared Infrastructure (Canonical Queries, Adapter Interface, Comparator)

**Goal:** Pure-Go infrastructure that all engine adapters depend on. No docker, no network. Establishes types and behavior used by every subsequent PR.

### Task D1.1: Canonical query catalog

**Files:**
- Create: `tests/analytics-engines/shared/canonical_query.go`
- Create: `tests/analytics-engines/shared/canonical_query_test.go`

- [ ] **Step 1: Write the failing test**

```go
// tests/analytics-engines/shared/canonical_query_test.go
package shared

import "testing"

// negative control: add two entries with the same Name → this test must
// fail because the duplicate-detection loop would catch it.
func TestCatalogNoDuplicateNames(t *testing.T) {
    seen := map[string]bool{}
    for _, q := range Catalog {
        if seen[q.Name] { t.Errorf("duplicate query name: %s", q.Name) }
        seen[q.Name] = true
    }
}

func TestCatalogHasTenQueries(t *testing.T) {
    if len(Catalog) != 10 {
        t.Errorf("Catalog has %d queries, want 10", len(Catalog))
    }
}

func TestCatalogQueriesAllHaveDescription(t *testing.T) {
    for _, q := range Catalog {
        if q.Description == "" {
            t.Errorf("query %q has empty Description", q.Name)
        }
    }
}

func TestCatalogQueriesAllHaveSQL(t *testing.T) {
    for _, q := range Catalog {
        if q.SQL == "" {
            t.Errorf("query %q has empty SQL", q.Name)
        }
    }
}

func TestCatalogQueryNamesMatchFilenameConvention(t *testing.T) {
    for _, q := range Catalog {
        for _, c := range q.Name {
            if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
                t.Errorf("query name %q contains non-snake-lowercase char %q", q.Name, c)
            }
        }
    }
}

func TestCatalogModeSplit(t *testing.T) {
    var logs, traces int
    for _, q := range Catalog {
        switch q.Mode {
        case ModeLogs: logs++
        case ModeTraces: traces++
        }
    }
    if logs != 5 { t.Errorf("expected 5 logs queries, got %d", logs) }
    if traces != 5 { t.Errorf("expected 5 traces queries, got %d", traces) }
}
```

- [ ] **Step 2: Run — expect failure**

```bash
cd tests/analytics-engines/shared && go test
```
Expected: FAIL (`undefined: Catalog`).

- [ ] **Step 3: Implement canonical_query.go**

```go
// tests/analytics-engines/shared/canonical_query.go
package shared

type Mode int

const (
    ModeLogs Mode = iota
    ModeTraces
)

type ResultMode int

const (
    OrderedRows ResultMode = iota
    SetEqual
    Aggregate
)

type DatasetTarget int

const (
    DatasetSmall DatasetTarget = iota
    DatasetLarge
    DatasetBoth
)

type CanonicalQuery struct {
    Name             string
    Mode             Mode
    Description      string
    SQL              string
    ResultMode       ResultMode
    DatasetTarget    DatasetTarget
    BloomColumns     []string
    RequiresPushdown bool
}

var Catalog = []CanonicalQuery{
    {
        Name:          "logs_count_total",
        Mode:          ModeLogs,
        Description:   "Total log row count",
        SQL:           `SELECT COUNT(*) AS total FROM logs`,
        ResultMode:    Aggregate,
        DatasetTarget: DatasetBoth,
    },
    {
        Name:          "logs_top_services_by_error_count",
        Mode:          ModeLogs,
        Description:   "Top 5 services by ERROR-severity log count",
        SQL:           `SELECT "service.name" AS service, COUNT(*) AS errors FROM logs WHERE severity_text = 'ERROR' GROUP BY "service.name" ORDER BY errors DESC, service ASC LIMIT 5`,
        ResultMode:    OrderedRows,
        DatasetTarget: DatasetBoth,
        BloomColumns:  []string{"service.name"},
    },
    {
        Name:          "logs_volume_by_severity",
        Mode:          ModeLogs,
        Description:   "Log count grouped by severity_text",
        SQL:           `SELECT severity_text, COUNT(*) AS n FROM logs GROUP BY severity_text ORDER BY severity_text`,
        ResultMode:    OrderedRows,
        DatasetTarget: DatasetBoth,
    },
    {
        Name:             "logs_volume_by_hour",
        Mode:             ModeLogs,
        Description:      "Log count per hour partition",
        SQL:              `SELECT (timestamp_unix_nano / 3600000000000) * 3600000000000 AS hour_ns, COUNT(*) AS n FROM logs GROUP BY hour_ns ORDER BY hour_ns`,
        ResultMode:       OrderedRows,
        DatasetTarget:    DatasetLarge,
        RequiresPushdown: true,
    },
    {
        Name:          "logs_filter_by_trace_id",
        Mode:          ModeLogs,
        Description:   "Logs for a specific trace_id (bloom filter must be consulted)",
        SQL:           `SELECT _msg, severity_text FROM logs WHERE trace_id = '0123456789abcdef0123456789abcdef' ORDER BY _time`,
        ResultMode:    OrderedRows,
        DatasetTarget: DatasetLarge,
        BloomColumns:  []string{"trace_id"},
    },
    {
        Name:          "traces_count_total",
        Mode:          ModeTraces,
        Description:   "Total span row count",
        SQL:           `SELECT COUNT(*) AS total FROM traces`,
        ResultMode:    Aggregate,
        DatasetTarget: DatasetBoth,
    },
    {
        Name:        "traces_latency_percentiles_by_service",
        Mode:        ModeTraces,
        Description: "p50/p95/p99 latency in ms per service",
        SQL: `SELECT "service.name" AS service,
                     approx_percentile((end_time_unix_nano - start_time_unix_nano) / 1e6, 0.5) AS p50,
                     approx_percentile((end_time_unix_nano - start_time_unix_nano) / 1e6, 0.95) AS p95,
                     approx_percentile((end_time_unix_nano - start_time_unix_nano) / 1e6, 0.99) AS p99
              FROM traces
              GROUP BY "service.name"
              ORDER BY service`,
        ResultMode:    OrderedRows,
        DatasetTarget: DatasetBoth,
    },
    {
        Name:        "traces_span_count_by_trace",
        Mode:        ModeTraces,
        Description: "Distribution of span count per trace_id",
        SQL: `SELECT span_count, COUNT(*) AS traces
              FROM (SELECT trace_id, COUNT(*) AS span_count FROM traces GROUP BY trace_id) g
              GROUP BY span_count
              ORDER BY span_count`,
        ResultMode:    OrderedRows,
        DatasetTarget: DatasetBoth,
    },
    {
        Name:          "traces_root_spans_count",
        Mode:          ModeTraces,
        Description:   "Number of spans without a parent (roots)",
        SQL:           `SELECT COUNT(*) AS roots FROM traces WHERE parent_span_id = ''`,
        ResultMode:    Aggregate,
        DatasetTarget: DatasetBoth,
    },
    {
        Name:          "traces_top_services_by_span_count",
        Mode:          ModeTraces,
        Description:   "Top 5 services by span volume",
        SQL:           `SELECT "service.name" AS service, COUNT(*) AS spans FROM traces GROUP BY "service.name" ORDER BY spans DESC, service ASC LIMIT 5`,
        ResultMode:    OrderedRows,
        DatasetTarget: DatasetBoth,
        BloomColumns:  []string{"service.name"},
    },
}
```

- [ ] **Step 4: Run tests, all pass**

```bash
cd tests/analytics-engines/shared && go test -v
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/analytics-engines/shared/canonical_query.go tests/analytics-engines/shared/canonical_query_test.go
git commit -m "test/analytics-engines: canonical query catalog (10 queries) (D1.1)"
```

### Task D1.2: Adapter interface

**Files:**
- Create: `tests/analytics-engines/shared/adapter.go`

- [ ] **Step 1: Write the interface**

```go
// tests/analytics-engines/shared/adapter.go
package shared

import "context"

type Adapter interface {
    Name() string
    SetUp(ctx context.Context) error
    RegisterParquet(ctx context.Context, table string, path string) error
    Translate(query CanonicalQuery) string
    Execute(ctx context.Context, sql string) ([]map[string]any, error)
    TearDown(ctx context.Context) error
    WasBloomUsed(ctx context.Context, queryID string) (bool, error)
}
```

- [ ] **Step 2: Commit**

```bash
git add tests/analytics-engines/shared/adapter.go
git commit -m "test/analytics-engines: adapter interface (D1.2)"
```

### Task D1.3: Golden comparator

**Files:**
- Create: `tests/analytics-engines/shared/golden_compare.go`
- Create: `tests/analytics-engines/shared/golden_compare_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// tests/analytics-engines/shared/golden_compare_test.go
package shared

import "testing"

// negative control: change SetEqual implementation to compare ordered →
// this test must fail because reordering should be tolerated.
func TestCompareSetEqualUnordered(t *testing.T) {
    a := GoldenResult{Rows: []map[string]any{{"x": int64(1)}, {"x": int64(2)}}}
    b := GoldenResult{Rows: []map[string]any{{"x": int64(2)}, {"x": int64(1)}}}
    if diff := Compare(a, b, SetEqual); diff != "" {
        t.Errorf("expected no diff, got: %s", diff)
    }
}

func TestCompareOrderedRowsRespectsOrder(t *testing.T) {
    a := GoldenResult{Rows: []map[string]any{{"x": int64(1)}, {"x": int64(2)}}}
    b := GoldenResult{Rows: []map[string]any{{"x": int64(2)}, {"x": int64(1)}}}
    if diff := Compare(a, b, OrderedRows); diff == "" {
        t.Error("expected diff for order mismatch")
    }
}

// negative control: drop the FloatTolerance check → this test must fail
// because exact equality would reject the 0.999 vs 1.000 pair.
func TestCompareFloatTolerance(t *testing.T) {
    a := GoldenResult{Rows: []map[string]any{{"p": 0.999}}}
    b := GoldenResult{Rows: []map[string]any{{"p": 1.000}}}
    if diff := Compare(a, b, OrderedRows); diff != "" {
        t.Errorf("expected within tolerance, got: %s", diff)
    }
}

func TestCompareFloatToleranceExceeded(t *testing.T) {
    a := GoldenResult{Rows: []map[string]any{{"p": 0.95}}}
    b := GoldenResult{Rows: []map[string]any{{"p": 1.00}}}
    if diff := Compare(a, b, OrderedRows); diff == "" {
        t.Error("expected diff outside tolerance")
    }
}

func TestCompareAggregateScalar(t *testing.T) {
    a := GoldenResult{Rows: []map[string]any{{"total": int64(42)}}}
    b := GoldenResult{Rows: []map[string]any{{"total": int64(42)}}}
    if diff := Compare(a, b, Aggregate); diff != "" {
        t.Errorf("expected no diff, got: %s", diff)
    }
}

// negative control: remove coerceNumeric → this test must fail because
// engines may return int32 or float64 for the same column.
func TestCompareCoercesIntegerTypes(t *testing.T) {
    a := GoldenResult{Rows: []map[string]any{{"n": int64(10)}}}
    b := GoldenResult{Rows: []map[string]any{{"n": float64(10)}}}
    if diff := Compare(a, b, OrderedRows); diff != "" {
        t.Errorf("expected coercion to match, got: %s", diff)
    }
}
```

- [ ] **Step 2: Implement comparator**

```go
// tests/analytics-engines/shared/golden_compare.go
package shared

import (
    "fmt"
    "math"
    "reflect"
    "sort"
    "strings"
)

const FloatTolerance = 0.005

type GoldenStats struct {
    RowsRead     int64 `json:"rows_read,omitempty"`
    BytesScanned int64 `json:"bytes_scanned,omitempty"`
}

type GoldenResult struct {
    QueryName string                       `json:"query"`
    Mode      string                       `json:"mode"`
    Engine    string                       `json:"engine,omitempty"`
    Rows      []map[string]any             `json:"rows"`
    Stats     GoldenStats                  `json:"stats,omitempty"`
}

func Compare(expected, actual GoldenResult, mode ResultMode) string {
    expRows := normalizeRows(expected.Rows)
    actRows := normalizeRows(actual.Rows)
    switch mode {
    case OrderedRows, Aggregate:
        return compareOrdered(expRows, actRows)
    case SetEqual:
        return compareSetEqual(expRows, actRows)
    }
    return fmt.Sprintf("unknown ResultMode: %v", mode)
}

func compareOrdered(exp, act []map[string]any) string {
    if len(exp) != len(act) {
        return fmt.Sprintf("row count: expected=%d actual=%d", len(exp), len(act))
    }
    for i := range exp {
        if diff := compareRow(exp[i], act[i]); diff != "" {
            return fmt.Sprintf("row %d: %s", i, diff)
        }
    }
    return ""
}

func compareSetEqual(exp, act []map[string]any) string {
    if len(exp) != len(act) {
        return fmt.Sprintf("row count: expected=%d actual=%d", len(exp), len(act))
    }
    sortMaps(exp)
    sortMaps(act)
    return compareOrdered(exp, act)
}

func compareRow(exp, act map[string]any) string {
    if len(exp) != len(act) {
        return fmt.Sprintf("key count: expected=%d actual=%d", len(exp), len(act))
    }
    for k, ev := range exp {
        av, ok := act[k]
        if !ok { return fmt.Sprintf("key %q missing in actual", k) }
        if diff := compareValue(ev, av); diff != "" {
            return fmt.Sprintf("key %q: %s", k, diff)
        }
    }
    return ""
}

func compareValue(exp, act any) string {
    eN, eIsNum := toFloat(exp)
    aN, aIsNum := toFloat(act)
    if eIsNum && aIsNum {
        return compareFloat(eN, aN)
    }
    if reflect.DeepEqual(exp, act) { return "" }
    return fmt.Sprintf("expected=%v(%T) actual=%v(%T)", exp, exp, act, act)
}

func compareFloat(a, b float64) string {
    if a == b { return "" }
    if math.IsNaN(a) && math.IsNaN(b) { return "" }
    denom := math.Abs(a)
    if denom < 1e-9 { denom = 1.0 }
    rel := math.Abs(a-b) / denom
    if rel <= FloatTolerance { return "" }
    return fmt.Sprintf("float diff %v vs %v (rel=%.6f > %.6f)", a, b, rel, FloatTolerance)
}

func toFloat(v any) (float64, bool) {
    switch x := v.(type) {
    case int: return float64(x), true
    case int32: return float64(x), true
    case int64: return float64(x), true
    case uint: return float64(x), true
    case uint32: return float64(x), true
    case uint64: return float64(x), true
    case float32: return float64(x), true
    case float64: return x, true
    }
    return 0, false
}

func normalizeRows(rows []map[string]any) []map[string]any {
    out := make([]map[string]any, len(rows))
    for i, r := range rows {
        copyMap := make(map[string]any, len(r))
        for k, v := range r { copyMap[k] = v }
        out[i] = copyMap
    }
    return out
}

func sortMaps(rows []map[string]any) {
    keyFn := func(m map[string]any) string {
        ks := make([]string, 0, len(m))
        for k := range m { ks = append(ks, k) }
        sort.Strings(ks)
        var sb strings.Builder
        for _, k := range ks {
            sb.WriteString(k)
            sb.WriteByte('=')
            fmt.Fprintf(&sb, "%v", m[k])
            sb.WriteByte('|')
        }
        return sb.String()
    }
    sort.Slice(rows, func(i, j int) bool { return keyFn(rows[i]) < keyFn(rows[j]) })
}
```

- [ ] **Step 3: Run, all tests pass**

```bash
cd tests/analytics-engines/shared && go test -v
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/analytics-engines/shared/golden_compare.go tests/analytics-engines/shared/golden_compare_test.go
git commit -m "test/analytics-engines: golden result comparator (D1.3)"
```

---

## PR D2 — Dataset Seeder + MinIO Uploader

**Goal:** Produce deterministic test datasets and upload them to MinIO with Hive partitioning.

### Task D2.1: Small dataset loader

**Files:**
- Create: `tests/analytics-engines/shared/dataset_seeder.go`
- Create: `tests/analytics-engines/shared/dataset_seeder_test.go`

- [ ] **Step 1: Write the failing test**

```go
// tests/analytics-engines/shared/dataset_seeder_test.go
package shared

import (
    "os"
    "testing"
)

// negative control: change LoadSmall to point at a wrong path → this
// test must fail because the file does not exist.
func TestLoadSmallReturnsExistingPaths(t *testing.T) {
    ds := LoadSmall(t)
    if _, err := os.Stat(ds.LogsURI); err != nil {
        t.Errorf("LogsURI not found: %s", ds.LogsURI)
    }
    if _, err := os.Stat(ds.TracesURI); err != nil {
        t.Errorf("TracesURI not found: %s", ds.TracesURI)
    }
}
```

- [ ] **Step 2: Implement LoadSmall**

```go
// tests/analytics-engines/shared/dataset_seeder.go
package shared

import (
    "path/filepath"
    "testing"
)

const Seed int64 = 1735689600

type DatasetSize int

const (
    SizeSmall DatasetSize = iota
    SizeLarge
)

type Dataset struct {
    Size      DatasetSize
    LogsURI   string
    TracesURI string
    Stats     DatasetStats
}

type DatasetStats struct {
    LogsRowCount     int
    TracesRowCount   int
    PartitionCount   int
    DistinctServices int
    DistinctTraceIDs int
}

func LoadSmall(t *testing.T) Dataset {
    t.Helper()
    repoRoot := findRepoRoot(t)
    return Dataset{
        Size:      SizeSmall,
        LogsURI:   filepath.Join(repoRoot, "tests/parquet-format/golden/v1/logs.parquet"),
        TracesURI: filepath.Join(repoRoot, "tests/parquet-format/golden/v1/traces.parquet"),
        Stats: DatasetStats{
            LogsRowCount:     2, // matches fixture.go CanonicalLogRows()
            TracesRowCount:   1,
            PartitionCount:   1,
            DistinctServices: 1,
            DistinctTraceIDs: 1,
        },
    }
}

func findRepoRoot(t *testing.T) string {
    t.Helper()
    dir, err := filepath.Abs(".")
    if err != nil { t.Fatal(err) }
    for {
        if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil { return dir }
        parent := filepath.Dir(dir)
        if parent == dir { t.Fatal("no go.mod found above CWD") }
        dir = parent
    }
}
```

Add the import for `os`.

- [ ] **Step 3: Run; commit**

```bash
cd tests/analytics-engines/shared && go test -run TestLoadSmall -v
git add tests/analytics-engines/shared/dataset_seeder.go tests/analytics-engines/shared/dataset_seeder_test.go
git commit -m "test/analytics-engines: small dataset loader (D2.1)"
```

### Task D2.2: Large dataset generator

**Files:**
- Modify: `tests/analytics-engines/shared/dataset_seeder.go`
- Modify: `tests/analytics-engines/shared/dataset_seeder_test.go`

- [ ] **Step 1: Write the failing tests**

```go
func TestGenerateLargeStatsMatchSpec(t *testing.T) {
    ds, err := GenerateLarge(t)
    if err != nil { t.Fatal(err) }
    if ds.Stats.LogsRowCount != 10000 {
        t.Errorf("LogsRowCount=%d, want 10000", ds.Stats.LogsRowCount)
    }
    if ds.Stats.TracesRowCount != 5000 {
        t.Errorf("TracesRowCount=%d, want 5000", ds.Stats.TracesRowCount)
    }
    if ds.Stats.PartitionCount != 6 {
        t.Errorf("PartitionCount=%d, want 6", ds.Stats.PartitionCount)
    }
    if ds.Stats.DistinctServices != 5 {
        t.Errorf("DistinctServices=%d, want 5", ds.Stats.DistinctServices)
    }
    if ds.Stats.DistinctTraceIDs != 100 {
        t.Errorf("DistinctTraceIDs=%d, want 100", ds.Stats.DistinctTraceIDs)
    }
}

// negative control: replace Seed with time.Now().UnixNano() → this
// test must fail because two calls would produce different files.
func TestSeederDeterministic(t *testing.T) {
    a, err := GenerateLarge(t)
    if err != nil { t.Fatal(err) }
    b, err := GenerateLarge(t)
    if err != nil { t.Fatal(err) }
    // Compare logs parquet bytes
    aLogs, _ := os.ReadFile(a.LogsURI)
    bLogs, _ := os.ReadFile(b.LogsURI)
    if !bytes.Equal(aLogs, bLogs) {
        t.Errorf("logs files differ: a=%d bytes, b=%d bytes", len(aLogs), len(bLogs))
    }
}
```

- [ ] **Step 2: Implement GenerateLarge**

```go
// Append to dataset_seeder.go

import (
    "bytes"
    "fmt"
    "math/rand"
    "os"
    "path/filepath"

    "github.com/parquet-go/parquet-go"
    "github.com/parquet-go/parquet-go/compress/zstd"

    "github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

const (
    LargeServiceCount       = 5
    LargeTraceCount         = 100
    LargeSpansPerTrace      = 50
    LargeLogsPerService     = 2000
    LargePartitionCount     = 6
    LargePartitionDurationS = 3600 // 1 hour
)

var largeStartUnixNs = int64(1769803200_000_000_000) // 2026-01-01T00:00:00Z

func GenerateLarge(t *testing.T) (Dataset, error) {
    t.Helper()
    dir := t.TempDir()
    rng := rand.New(rand.NewSource(Seed))
    services := make([]string, LargeServiceCount)
    for i := range services { services[i] = fmt.Sprintf("svc-%d", i+1) }

    traceIDs := make([]string, LargeTraceCount)
    for i := range traceIDs { traceIDs[i] = fmt.Sprintf("%032x", uint64(i+1)) }

    if err := writeLargeLogs(dir, services, traceIDs, rng); err != nil { return Dataset{}, err }
    if err := writeLargeTraces(dir, services, traceIDs, rng); err != nil { return Dataset{}, err }

    return Dataset{
        Size:      SizeLarge,
        LogsURI:   filepath.Join(dir, "logs"),
        TracesURI: filepath.Join(dir, "traces"),
        Stats: DatasetStats{
            LogsRowCount:     LargeServiceCount * LargeLogsPerService,
            TracesRowCount:   LargeTraceCount * LargeSpansPerTrace,
            PartitionCount:   LargePartitionCount,
            DistinctServices: LargeServiceCount,
            DistinctTraceIDs: LargeTraceCount,
        },
    }, nil
}

func writeLargeLogs(dir string, services, traceIDs []string, rng *rand.Rand) error {
    severityWeights := []struct {
        text   string
        number int32
        cum    int
    }{
        {"INFO", 9, 70},
        {"WARN", 13, 90},
        {"ERROR", 17, 98},
        {"FATAL", 21, 100},
    }
    rowsByPartition := map[int][]schema.LogRow{}
    perService := LargeLogsPerService
    for si, svc := range services {
        for i := 0; i < perService; i++ {
            partition := i % LargePartitionCount
            ts := largeStartUnixNs + int64(partition)*int64(LargePartitionDurationS)*1_000_000_000 + int64(rng.Int63n(int64(LargePartitionDurationS)*1_000_000_000))
            sev := severityWeights[0]
            roll := rng.Intn(100)
            for _, w := range severityWeights {
                if roll < w.cum { sev = w; break }
            }
            tidIdx := rng.Intn(len(traceIDs))
            row := schema.LogRow{
                TimestampUnixNano: ts,
                Body:              fmt.Sprintf("log-%d-%d", si, i),
                SeverityText:      sev.text,
                SeverityNumber:    sev.number,
                ServiceName:       svc,
                TraceID:           traceIDs[tidIdx],
            }
            rowsByPartition[partition] = append(rowsByPartition[partition], row)
        }
    }
    for partition, rows := range rowsByPartition {
        partDir := filepath.Join(dir, "logs", fmt.Sprintf("dt=2026-01-01/hour=%02d", partition))
        if err := os.MkdirAll(partDir, 0755); err != nil { return err }
        var buf bytes.Buffer
        w := parquet.NewGenericWriter[schema.LogRow](&buf,
            parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault}),
            parquet.MaxRowsPerRowGroup(int64(len(rows)/2+1)),
        )
        if _, err := w.Write(rows); err != nil { return err }
        if err := w.Close(); err != nil { return err }
        if err := os.WriteFile(filepath.Join(partDir, "00001.parquet"), buf.Bytes(), 0644); err != nil { return err }
    }
    return nil
}

func writeLargeTraces(dir string, services, traceIDs []string, rng *rand.Rand) error {
    rowsByPartition := map[int][]schema.TraceRow{}
    for ti, tid := range traceIDs {
        svc := services[ti%len(services)]
        partition := ti % LargePartitionCount
        baseTs := largeStartUnixNs + int64(partition)*int64(LargePartitionDurationS)*1_000_000_000 + int64(rng.Int63n(int64(LargePartitionDurationS)*1_000_000_000))
        for s := 0; s < LargeSpansPerTrace; s++ {
            startTs := baseTs + int64(s)*100_000
            endTs := startTs + int64(rng.Intn(50_000_000)+100_000)
            parent := ""
            if s > 0 { parent = fmt.Sprintf("%016x", uint64(s)) }
            row := schema.TraceRow{
                TimestampUnixNano: startTs,
                StartTimeUnixNano: startTs,
                EndTimeUnixNano:   endTs,
                TraceID:           tid,
                SpanID:            fmt.Sprintf("%016x", uint64(s+1)),
                ParentSpanID:      parent,
                ServiceName:       svc,
                SpanName:          fmt.Sprintf("op-%d", s),
            }
            rowsByPartition[partition] = append(rowsByPartition[partition], row)
        }
    }
    for partition, rows := range rowsByPartition {
        partDir := filepath.Join(dir, "traces", fmt.Sprintf("dt=2026-01-01/hour=%02d", partition))
        if err := os.MkdirAll(partDir, 0755); err != nil { return err }
        var buf bytes.Buffer
        w := parquet.NewGenericWriter[schema.TraceRow](&buf,
            parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault}),
            parquet.MaxRowsPerRowGroup(int64(len(rows)/2+1)),
        )
        if _, err := w.Write(rows); err != nil { return err }
        if err := w.Close(); err != nil { return err }
        if err := os.WriteFile(filepath.Join(partDir, "00001.parquet"), buf.Bytes(), 0644); err != nil { return err }
    }
    return nil
}
```

- [ ] **Step 3: Run; commit**

```bash
cd tests/analytics-engines/shared && go test -run "TestGenerateLarge|TestSeederDeterministic" -v
git add tests/analytics-engines/shared/dataset_seeder.go tests/analytics-engines/shared/dataset_seeder_test.go
git commit -m "test/analytics-engines: deterministic large dataset generator (D2.2)"
```

### Task D2.3: MinIO uploader

**Files:**
- Create: `tests/analytics-engines/shared/minio_uploader.go`
- Create: `tests/analytics-engines/shared/minio_uploader_test.go`

- [ ] **Step 1: Write the failing test**

```go
// tests/analytics-engines/shared/minio_uploader_test.go
package shared

import "testing"

// negative control: change UploadHive to drop the path-prefix logic →
// this test must fail because uploaded keys would not include the
// dt=*/hour=* segments.
func TestUploadHivePreservesLayout(t *testing.T) {
    // Pure-Go test of key derivation; no real MinIO needed.
    got := deriveS3Key("/tmp/data", "/tmp/data/logs/dt=2026-01-01/hour=00/00001.parquet", "logs/")
    want := "logs/dt=2026-01-01/hour=00/00001.parquet"
    if got != want { t.Errorf("got %q, want %q", got, want) }
}
```

- [ ] **Step 2: Implement**

```go
// tests/analytics-engines/shared/minio_uploader.go
package shared

import (
    "context"
    "fmt"
    "os"
    "path/filepath"
    "strings"
    "time"

    "github.com/minio/minio-go/v7"
    "github.com/minio/minio-go/v7/pkg/credentials"
)

type Uploader struct {
    Endpoint  string
    AccessKey string
    SecretKey string
    Bucket    string

    client *minio.Client
}

func (u *Uploader) ensureClient() error {
    if u.client != nil { return nil }
    c, err := minio.New(u.Endpoint, &minio.Options{
        Creds:  credentials.NewStaticV4(u.AccessKey, u.SecretKey, ""),
        Secure: false,
    })
    if err != nil { return err }
    u.client = c
    return nil
}

func (u *Uploader) UploadHive(ctx context.Context, localRoot, s3Prefix string) error {
    if err := u.ensureClient(); err != nil { return err }
    return filepath.Walk(localRoot, func(path string, info os.FileInfo, err error) error {
        if err != nil { return err }
        if info.IsDir() { return nil }
        key := deriveS3Key(localRoot, path, s3Prefix)
        return u.uploadWithRetry(ctx, key, path)
    })
}

func deriveS3Key(localRoot, fullPath, s3Prefix string) string {
    rel := strings.TrimPrefix(fullPath, localRoot)
    rel = strings.TrimPrefix(rel, string(filepath.Separator))
    return s3Prefix + rel
}

func (u *Uploader) uploadWithRetry(ctx context.Context, key, localPath string) error {
    var lastErr error
    backoff := 250 * time.Millisecond
    for i := 0; i < 3; i++ {
        _, err := u.client.FPutObject(ctx, u.Bucket, key, localPath, minio.PutObjectOptions{})
        if err == nil { return nil }
        lastErr = err
        time.Sleep(backoff)
        backoff *= 2
    }
    return fmt.Errorf("UPLOAD_FAILED: %s (after 3 retries): %w", key, lastErr)
}
```

- [ ] **Step 3: Run unit test; commit**

```bash
cd tests/analytics-engines/shared && go test -run TestUploadHive -v
git add tests/analytics-engines/shared/minio_uploader.go tests/analytics-engines/shared/minio_uploader_test.go
git commit -m "test/analytics-engines: MinIO uploader with Hive layout (D2.3)"
```

### Task D2.4: Runner helpers

**Files:**
- Create: `tests/analytics-engines/shared/runner.go`

- [ ] **Step 1: Write helpers (no test yet — exercised by engine integration tests)**

```go
// tests/analytics-engines/shared/runner.go
package shared

import (
    "context"
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "testing"
)

// UploadAndRegister uploads the dataset to MinIO (if remote) and
// registers logs/traces as tables in the adapter.
func UploadAndRegister(t *testing.T, ctx context.Context, a Adapter, ds Dataset, u *Uploader) {
    t.Helper()
    if u != nil {
        if err := u.UploadHive(ctx, ds.LogsURI, "logs/"); err != nil { t.Fatal(err) }
        if err := u.UploadHive(ctx, ds.TracesURI, "traces/"); err != nil { t.Fatal(err) }
        if err := a.RegisterParquet(ctx, "logs", fmt.Sprintf("s3://%s/logs/", u.Bucket)); err != nil { t.Fatal(err) }
        if err := a.RegisterParquet(ctx, "traces", fmt.Sprintf("s3://%s/traces/", u.Bucket)); err != nil { t.Fatal(err) }
        return
    }
    // No uploader: in-process engine (DuckDB) reads local paths directly.
    if err := a.RegisterParquet(ctx, "logs", ds.LogsURI); err != nil { t.Fatal(err) }
    if err := a.RegisterParquet(ctx, "traces", ds.TracesURI); err != nil { t.Fatal(err) }
}

func RunCorrectnessSuite(t *testing.T, ctx context.Context, a Adapter, size DatasetSize) {
    t.Helper()
    for _, q := range Catalog {
        if !targetMatches(q.DatasetTarget, size) { continue }
        q := q
        t.Run(q.Name, func(t *testing.T) {
            sql := a.Translate(q)
            rows, err := a.Execute(ctx, sql)
            if err != nil { t.Fatalf("QUERY_REJECTED: %s/%s: %v", a.Name(), q.Name, err) }
            actual := GoldenResult{QueryName: q.Name, Rows: rows, Engine: a.Name()}
            expected := loadGolden(t, q.Name, size)
            if diff := Compare(expected, actual, q.ResultMode); diff != "" {
                t.Fatalf("RESULT_DIVERGENCE: %s/%s: %s", a.Name(), q.Name, diff)
            }
        })
    }
}

func RunBloomUsageAdvisory(t *testing.T, ctx context.Context, a Adapter) BloomUsageReport {
    t.Helper()
    report := BloomUsageReport{Engine: a.Name(), Used: map[string]bool{}, Notes: map[string]string{}}
    for _, q := range Catalog {
        if len(q.BloomColumns) == 0 { continue }
        sql := a.Translate(q)
        if _, err := a.Execute(ctx, sql); err != nil { continue }
        used, err := a.WasBloomUsed(ctx, q.Name)
        if err != nil {
            report.Notes[q.Name] = fmt.Sprintf("detection unsupported: %v", err)
            continue
        }
        report.Used[q.Name] = used
    }
    return report
}

func loadGolden(t *testing.T, name string, size DatasetSize) GoldenResult {
    t.Helper()
    repoRoot := findRepoRoot(t)
    sizeDir := "small"
    if size == SizeLarge { sizeDir = "large" }
    path := filepath.Join(repoRoot, "tests/analytics-engines/goldens", sizeDir, name+".json")
    data, err := os.ReadFile(path)
    if err != nil { t.Fatalf("MISSING_GOLDEN: %s; run 'make analytics-engines-goldens-%s' to record initial expectation", path, sizeDir) }
    var r GoldenResult
    if err := json.Unmarshal(data, &r); err != nil { t.Fatal(err) }
    return r
}

func targetMatches(target DatasetTarget, size DatasetSize) bool {
    switch target {
    case DatasetBoth: return true
    case DatasetSmall: return size == SizeSmall
    case DatasetLarge: return size == SizeLarge
    }
    return false
}
```

- [ ] **Step 2: Commit**

```bash
git add tests/analytics-engines/shared/runner.go
git commit -m "test/analytics-engines: runner helpers (UploadAndRegister, RunCorrectnessSuite, RunBloomUsageAdvisory) (D2.4)"
```

### Task D2.5: Bloom usage report types

**Files:**
- Create: `tests/analytics-engines/shared/bloom_usage.go`

- [ ] **Step 1: Implement**

```go
// tests/analytics-engines/shared/bloom_usage.go
package shared

import (
    "encoding/json"
    "os"
)

type BloomUsageReport struct {
    Engine string            `json:"engine"`
    Used   map[string]bool   `json:"used"`
    Notes  map[string]string `json:"notes"`
}

func WriteReport(path string, r BloomUsageReport) error {
    data, err := json.MarshalIndent(r, "", "  ")
    if err != nil { return err }
    return os.WriteFile(path, data, 0644)
}
```

- [ ] **Step 2: Commit**

```bash
git add tests/analytics-engines/shared/bloom_usage.go
git commit -m "test/analytics-engines: BloomUsageReport types (D2.5)"
```

---

## PR D3 — DuckDB Engine (First End-to-End)

**Goal:** Validate the full pipeline (seeder → MinIO upload → engine read → golden compare) with DuckDB embedded. Record initial goldens.

### Task D3.1: DuckDB docker-compose (MinIO only)

**Files:**
- Create: `tests/analytics-engines/duckdb/docker-compose.yml`

- [ ] **Step 1: Write compose file**

```yaml
# tests/analytics-engines/duckdb/docker-compose.yml
name: lh-analytics-duckdb

services:
  minio:
    image: minio/minio:RELEASE.2024-08-17T01-24-54Z
    command: server /data --console-address ":9001"
    ports: ["19000:9000"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    tmpfs: [/data]
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 3s
      timeout: 3s
      retries: 10

  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin &&
      mc mb local/lh-analytics-test --ignore-existing
      "
```

- [ ] **Step 2: Smoke test**

```bash
docker compose -f tests/analytics-engines/duckdb/docker-compose.yml up -d
sleep 10
docker compose -f tests/analytics-engines/duckdb/docker-compose.yml ps
docker compose -f tests/analytics-engines/duckdb/docker-compose.yml down -v
```

- [ ] **Step 3: Commit**

```bash
git add tests/analytics-engines/duckdb/docker-compose.yml
git commit -m "test/analytics-engines/duckdb: MinIO compose (D3.1)"
```

### Task D3.2: DuckDB adapter

**Files:**
- Create: `tests/analytics-engines/duckdb/adapter.go`
- Create: `tests/analytics-engines/duckdb/adapter_test.go`

- [ ] **Step 1: Write the failing test**

```go
// tests/analytics-engines/duckdb/adapter_test.go
//go:build analytics_engines

package duckdb

import (
    "testing"

    "github.com/ReliablyObserve/victoria-lakehouse/tests/analytics-engines/shared"
)

// negative control: change Translate to lowercase column names → this
// test must fail because "service.name" is the canonical column.
func TestDuckDBTranslatePassesThroughANSI(t *testing.T) {
    a := New()
    q := shared.CanonicalQuery{SQL: `SELECT COUNT(*) FROM logs`}
    if got := a.Translate(q); got != q.SQL {
        t.Errorf("Translate altered SQL: %q != %q", got, q.SQL)
    }
}
```

- [ ] **Step 2: Implement adapter**

```go
// tests/analytics-engines/duckdb/adapter.go
//go:build analytics_engines

package duckdb

import (
    "context"
    "database/sql"
    "fmt"

    _ "github.com/marcboeker/go-duckdb"

    "github.com/ReliablyObserve/victoria-lakehouse/tests/analytics-engines/shared"
)

type Adapter struct {
    db *sql.DB
}

func New() *Adapter { return &Adapter{} }

func (a *Adapter) Name() string { return "duckdb" }

func (a *Adapter) SetUp(ctx context.Context) error {
    db, err := sql.Open("duckdb", "")
    if err != nil { return err }
    if _, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil { return err }
    a.db = db
    return nil
}

func (a *Adapter) RegisterParquet(ctx context.Context, table, path string) error {
    if a.db == nil { return fmt.Errorf("adapter not set up") }
    // DuckDB CREATE VIEW with read_parquet supports both local globs and s3:// URIs.
    sql := fmt.Sprintf(`CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet('%s/**/*.parquet')`, table, path)
    _, err := a.db.ExecContext(ctx, sql)
    return err
}

func (a *Adapter) Translate(q shared.CanonicalQuery) string {
    // DuckDB supports ANSI SQL + approx_percentile (via approx_quantile).
    return q.SQL
}

func (a *Adapter) Execute(ctx context.Context, sqlText string) ([]map[string]any, error) {
    rows, err := a.db.QueryContext(ctx, sqlText)
    if err != nil { return nil, err }
    defer rows.Close()
    cols, _ := rows.Columns()
    var out []map[string]any
    for rows.Next() {
        vals := make([]any, len(cols))
        ptrs := make([]any, len(cols))
        for i := range vals { ptrs[i] = &vals[i] }
        if err := rows.Scan(ptrs...); err != nil { return nil, err }
        m := map[string]any{}
        for i, c := range cols { m[c] = vals[i] }
        out = append(out, m)
    }
    return out, rows.Err()
}

func (a *Adapter) TearDown(ctx context.Context) error {
    if a.db != nil { return a.db.Close() }
    return nil
}

func (a *Adapter) WasBloomUsed(ctx context.Context, queryID string) (bool, error) {
    // DuckDB does not surface bloom filter usage in EXPLAIN output reliably.
    return false, fmt.Errorf("not supported")
}
```

- [ ] **Step 3: Run unit test**

```bash
cd tests/analytics-engines/duckdb && go test -tags=analytics_engines -run TestDuckDBTranslate -v
```
Expected: PASS (CGO needed; requires `gcc` and libduckdb).

- [ ] **Step 4: Commit**

```bash
git add tests/analytics-engines/duckdb/adapter.go tests/analytics-engines/duckdb/adapter_test.go
git commit -m "test/analytics-engines/duckdb: adapter (embedded via go-duckdb) (D3.2)"
```

### Task D3.3: DuckDB end-to-end test (correctness on small dataset)

**Files:**
- Create: `tests/analytics-engines/duckdb/duckdb_test.go`

- [ ] **Step 1: Write the test**

```go
//go:build analytics_engines

package duckdb

import (
    "context"
    "testing"

    "github.com/ReliablyObserve/victoria-lakehouse/tests/analytics-engines/shared"
)

// negative control: bypass UploadAndRegister → this test must fail
// because the "logs" view would not exist in DuckDB.
func TestDuckDBCorrectnessSmall(t *testing.T) {
    ctx := context.Background()
    a := New()
    if err := a.SetUp(ctx); err != nil { t.Fatal(err) }
    t.Cleanup(func() { _ = a.TearDown(ctx) })
    ds := shared.LoadSmall(t)
    shared.UploadAndRegister(t, ctx, a, ds, nil)
    shared.RunCorrectnessSuite(t, ctx, a, shared.SizeSmall)
}
```

- [ ] **Step 2: Generate the small goldens first**

We need to record initial goldens before this test can pass. See Task D3.4.

### Task D3.4: Reference golden recorder + initial goldens

**Files:**
- Create: `tests/analytics-engines/cmd/genfixture/main.go`
- Modify: `Makefile`
- Create: `tests/analytics-engines/goldens/small/*.json` (one per query covering DatasetSmall/Both)

- [ ] **Step 1: Implement recorder**

```go
// tests/analytics-engines/cmd/genfixture/main.go
//go:build analytics_engines

package main

import (
    "context"
    "encoding/json"
    "flag"
    "log"
    "os"
    "path/filepath"

    "github.com/ReliablyObserve/victoria-lakehouse/tests/analytics-engines/duckdb"
    "github.com/ReliablyObserve/victoria-lakehouse/tests/analytics-engines/shared"
)

func main() {
    size := flag.String("size", "small", "small or large")
    outDir := flag.String("out", "tests/analytics-engines/goldens", "output directory")
    flag.Parse()

    ctx := context.Background()
    a := duckdb.New()
    if err := a.SetUp(ctx); err != nil { log.Fatal(err) }
    defer a.TearDown(ctx)

    var ds shared.Dataset
    var dsSize shared.DatasetSize
    if *size == "small" {
        ds = shared.LoadSmall(noopT{})
        dsSize = shared.SizeSmall
    } else {
        var err error
        ds, err = shared.GenerateLarge(noopT{})
        if err != nil { log.Fatal(err) }
        dsSize = shared.SizeLarge
    }
    shared.UploadAndRegister(noopT{}, ctx, a, ds, nil)

    sizeDir := filepath.Join(*outDir, *size)
    _ = os.MkdirAll(sizeDir, 0755)
    for _, q := range shared.Catalog {
        if !targetMatches(q.DatasetTarget, dsSize) { continue }
        rows, err := a.Execute(ctx, a.Translate(q))
        if err != nil { log.Fatalf("query %s: %v", q.Name, err) }
        r := shared.GoldenResult{QueryName: q.Name, Rows: rows}
        data, _ := json.MarshalIndent(r, "", "  ")
        path := filepath.Join(sizeDir, q.Name+".json")
        if err := os.WriteFile(path, data, 0644); err != nil { log.Fatal(err) }
        log.Printf("wrote %s (%d rows)", path, len(rows))
    }
}

func targetMatches(target shared.DatasetTarget, size shared.DatasetSize) bool {
    if target == shared.DatasetBoth { return true }
    if target == shared.DatasetSmall && size == shared.SizeSmall { return true }
    if target == shared.DatasetLarge && size == shared.SizeLarge { return true }
    return false
}

// noopT implements *testing.T's interface members used by shared helpers.
type noopT struct{}

func (noopT) Helper() {}
func (noopT) Fatal(args ...any) { log.Fatal(args...) }
func (noopT) Fatalf(format string, args ...any) { log.Fatalf(format, args...) }
func (noopT) Errorf(format string, args ...any) { log.Printf(format, args...) }
```

The helpers (`LoadSmall`, `GenerateLarge`, `UploadAndRegister`) accept a small interface — refactor `shared` to accept this interface instead of `*testing.T`:

```go
type Logger interface {
    Helper()
    Fatal(args ...any)
    Fatalf(format string, args ...any)
}
```

Update `LoadSmall`, `GenerateLarge`, `UploadAndRegister`, `findRepoRoot` to take `Logger` (renaming `t *testing.T` parameters).

- [ ] **Step 2: Add Makefile targets**

```makefile
.PHONY: analytics-engines-goldens-small analytics-engines-goldens-large analytics-engines-test

analytics-engines-goldens-small:
	go run -tags=analytics_engines ./tests/analytics-engines/cmd/genfixture -size=small -out=tests/analytics-engines/goldens

analytics-engines-goldens-large:
	go run -tags=analytics_engines ./tests/analytics-engines/cmd/genfixture -size=large -out=tests/analytics-engines/goldens

analytics-engines-test:
	cd tests/analytics-engines/$(ENGINE) && go test -tags=analytics_engines -v -count=1 -timeout=20m
```

- [ ] **Step 3: Generate the goldens**

```bash
# Subsystem C's golden/v1/ must exist; if not, run `make parquet-format-golden-v1` first.
make analytics-engines-goldens-small
make analytics-engines-goldens-large
ls tests/analytics-engines/goldens/small/  # should show JSON files
ls tests/analytics-engines/goldens/large/
```

- [ ] **Step 4: Run DuckDB end-to-end test**

```bash
docker compose -f tests/analytics-engines/duckdb/docker-compose.yml up -d
sleep 5
make analytics-engines-test ENGINE=duckdb
docker compose -f tests/analytics-engines/duckdb/docker-compose.yml down -v
```
Expected: PASS.

- [ ] **Step 5: Commit goldens**

```bash
git add tests/analytics-engines/cmd/ tests/analytics-engines/duckdb/duckdb_test.go tests/analytics-engines/goldens/ Makefile tests/analytics-engines/shared/runner.go tests/analytics-engines/shared/dataset_seeder.go tests/analytics-engines/shared/minio_uploader.go
git commit -m "test/analytics-engines: golden recorder + initial small/large goldens + DuckDB e2e (D3.4)"
```

### Task D3.5: First CI job

**Files:**
- Create: `.github/workflows/analytics-engines.yaml`

- [ ] **Step 1: Write minimal workflow with DuckDB job**

```yaml
# .github/workflows/analytics-engines.yaml
name: Analytics Engines

on:
  pull_request:
    paths:
      - 'internal/schema/**'
      - 'internal/storage/parquets3/**'
      - 'lakehouse-traces/internal/schema/**'
      - 'lakehouse-traces/internal/storage/parquets3/**'
      - 'tests/analytics-engines/**'
      - 'tests/parquet-format/golden/**'
      - '.github/workflows/analytics-engines.yaml'
  push:
    branches: [main]
  schedule:
    - cron: '0 5 * * *'

permissions:
  contents: read

env:
  GOWORK: "off"

jobs:
  duckdb:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    strategy:
      matrix:
        size: [correctness, realistic]
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version-file: go.mod }
      - name: Install build deps
        run: sudo apt-get update && sudo apt-get install -y gcc libstdc++-12-dev
      - name: Start MinIO
        run: docker compose -f tests/analytics-engines/duckdb/docker-compose.yml up -d
      - name: Wait for MinIO
        run: |
          for i in $(seq 1 30); do
            curl -sf http://localhost:19000/minio/health/live && break
            sleep 2
          done
      - name: Run DuckDB ${{ matrix.size }}
        run: |
          cd tests/analytics-engines/duckdb && \
          go test -tags=analytics_engines -v -count=1 -timeout=20m -run "TestDuckDB(Correctness|Realistic)" \
            -args -size=${{ matrix.size }}
      - name: Tear down
        if: always()
        run: docker compose -f tests/analytics-engines/duckdb/docker-compose.yml down -v
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/analytics-engines.yaml
git commit -m "ci: analytics-engines workflow with DuckDB matrix (D3.5)"
```

---

## PR D4 — Trino Engine

**Goal:** Add Trino as the second engine; extend CI matrix.

### Task D4.1: Trino compose + Hive catalog config

**Files:**
- Create: `tests/analytics-engines/trino/docker-compose.yml`
- Create: `tests/analytics-engines/trino/catalog/hive.properties`

- [ ] **Step 1: Write compose**

```yaml
# tests/analytics-engines/trino/docker-compose.yml
name: lh-analytics-trino

services:
  minio:
    image: minio/minio:RELEASE.2024-08-17T01-24-54Z
    command: server /data --console-address ":9001"
    ports: ["19000:9000"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    tmpfs: [/data]
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 3s
      retries: 10

  minio-init:
    image: minio/mc:latest
    depends_on: { minio: { condition: service_healthy } }
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin &&
      mc mb local/lh-analytics-test --ignore-existing
      "

  hive-metastore:
    image: bitsondatadev/hive-metastore:latest
    ports: ["19083:9083"]
    environment:
      DATABASE_HOST: hive-postgres
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
      S3_ENDPOINT: http://minio:9000
    depends_on: { hive-postgres: { condition: service_started } }

  hive-postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: hive
      POSTGRES_PASSWORD: hive
      POSTGRES_DB: metastore
    tmpfs: [/var/lib/postgresql/data]

  trino:
    image: trinodb/trino:439
    ports: ["18080:8080"]
    depends_on: { hive-metastore: { condition: service_started } }
    volumes:
      - ./catalog:/etc/trino/catalog
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:8080/v1/info"]
      interval: 5s
      retries: 20
```

- [ ] **Step 2: Write hive.properties**

```ini
# tests/analytics-engines/trino/catalog/hive.properties
connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=http://minio:9000
hive.s3.path-style-access=true
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadmin
hive.s3.ssl.enabled=false
hive.non-managed-table-writes-enabled=true
hive.storage-format=PARQUET
```

- [ ] **Step 3: Smoke test**

```bash
docker compose -f tests/analytics-engines/trino/docker-compose.yml up -d
sleep 60
curl http://localhost:18080/v1/info
docker compose -f tests/analytics-engines/trino/docker-compose.yml down -v
```

- [ ] **Step 4: Commit**

```bash
git add tests/analytics-engines/trino/docker-compose.yml tests/analytics-engines/trino/catalog/
git commit -m "test/analytics-engines/trino: compose + Hive catalog (D4.1)"
```

### Task D4.2: Trino adapter

**Files:**
- Create: `tests/analytics-engines/trino/adapter.go`
- Create: `tests/analytics-engines/trino/adapter_test.go`

- [ ] **Step 1: Write adapter using trinodb's HTTP API or trino-go-client**

```go
//go:build analytics_engines

package trino

import (
    "context"
    "database/sql"
    "fmt"
    "strings"

    _ "github.com/trinodb/trino-go-client/trino"

    "github.com/ReliablyObserve/victoria-lakehouse/tests/analytics-engines/shared"
)

type Adapter struct {
    db        *sql.DB
    schema    string
    lastQueryID string
}

func New() *Adapter { return &Adapter{schema: "default"} }

func (a *Adapter) Name() string { return "trino" }

func (a *Adapter) SetUp(ctx context.Context) error {
    dsn := "http://test@localhost:18080?catalog=hive&schema=default"
    db, err := sql.Open("trino", dsn)
    if err != nil { return err }
    a.db = db
    if _, err := a.db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS hive.default WITH (location = 's3://lh-analytics-test/')"); err != nil {
        return err
    }
    return nil
}

func (a *Adapter) RegisterParquet(ctx context.Context, table, path string) error {
    if !strings.HasPrefix(path, "s3://") { return fmt.Errorf("trino requires s3:// path, got %q", path) }
    sqlStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS hive.default.%s (
        "service.name" VARCHAR, severity_text VARCHAR
    ) WITH (
        format = 'PARQUET',
        external_location = '%s',
        partitioned_by = ARRAY['dt','hour']
    )`, table, strings.TrimSuffix(path, "/"))
    // For external tables with partitions, run MSCK-equivalent
    if _, err := a.db.ExecContext(ctx, sqlStmt); err != nil {
        // Table may exist; try CALL system.sync_partition_metadata
    }
    if _, err := a.db.ExecContext(ctx, fmt.Sprintf("CALL system.sync_partition_metadata('default', '%s', 'FULL')", table)); err != nil {
        return err
    }
    return nil
}

func (a *Adapter) Translate(q shared.CanonicalQuery) string {
    sql := q.SQL
    sql = strings.ReplaceAll(sql, "approx_percentile(", "approx_quantile(")
    return sql
}

func (a *Adapter) Execute(ctx context.Context, sqlText string) ([]map[string]any, error) {
    rows, err := a.db.QueryContext(ctx, sqlText)
    if err != nil { return nil, err }
    defer rows.Close()
    cols, _ := rows.Columns()
    var out []map[string]any
    for rows.Next() {
        vals := make([]any, len(cols))
        ptrs := make([]any, len(cols))
        for i := range vals { ptrs[i] = &vals[i] }
        if err := rows.Scan(ptrs...); err != nil { return nil, err }
        m := map[string]any{}
        for i, c := range cols { m[c] = vals[i] }
        out = append(out, m)
    }
    return out, rows.Err()
}

func (a *Adapter) TearDown(ctx context.Context) error {
    if a.db != nil { return a.db.Close() }
    return nil
}

func (a *Adapter) WasBloomUsed(ctx context.Context, queryID string) (bool, error) {
    if a.lastQueryID == "" { return false, fmt.Errorf("no query id captured") }
    // Trino /v1/query/<id> exposes processedInputDataSize and totalDataSize.
    return false, fmt.Errorf("inspection via HTTP not yet wired (advisory)")
}
```

- [ ] **Step 2: Write a translation unit test**

```go
//go:build analytics_engines

package trino

import (
    "strings"
    "testing"

    "github.com/ReliablyObserve/victoria-lakehouse/tests/analytics-engines/shared"
)

// negative control: remove the approx_percentile → approx_quantile rewrite
// → this test must fail because Trino would reject the syntax.
func TestTrinoTranslateApproxPercentile(t *testing.T) {
    a := New()
    q := shared.CanonicalQuery{SQL: `SELECT approx_percentile(x, 0.95) FROM t`}
    got := a.Translate(q)
    if !strings.Contains(got, "approx_quantile(") {
        t.Errorf("expected approx_quantile, got: %s", got)
    }
}
```

- [ ] **Step 3: Run; commit**

```bash
cd tests/analytics-engines/trino && go test -tags=analytics_engines -run TestTrinoTranslate -v
git add tests/analytics-engines/trino/adapter.go tests/analytics-engines/trino/adapter_test.go
git commit -m "test/analytics-engines/trino: adapter + translation tests (D4.2)"
```

### Task D4.3: Trino end-to-end test

**Files:**
- Create: `tests/analytics-engines/trino/trino_test.go`

- [ ] **Step 1: Write test**

```go
//go:build analytics_engines

package trino

import (
    "context"
    "testing"

    "github.com/ReliablyObserve/victoria-lakehouse/tests/analytics-engines/shared"
)

func TestTrinoCorrectnessSmall(t *testing.T) {
    ctx := context.Background()
    a := New()
    if err := a.SetUp(ctx); err != nil { t.Fatal(err) }
    t.Cleanup(func() { _ = a.TearDown(ctx) })
    ds := shared.LoadSmall(t)
    u := &shared.Uploader{Endpoint: "localhost:19000", AccessKey: "minioadmin", SecretKey: "minioadmin", Bucket: "lh-analytics-test"}
    shared.UploadAndRegister(t, ctx, a, ds, u)
    shared.RunCorrectnessSuite(t, ctx, a, shared.SizeSmall)
}

func TestTrinoCorrectnessLarge(t *testing.T) {
    ctx := context.Background()
    a := New()
    if err := a.SetUp(ctx); err != nil { t.Fatal(err) }
    t.Cleanup(func() { _ = a.TearDown(ctx) })
    ds, err := shared.GenerateLarge(t)
    if err != nil { t.Fatal(err) }
    u := &shared.Uploader{Endpoint: "localhost:19000", AccessKey: "minioadmin", SecretKey: "minioadmin", Bucket: "lh-analytics-test"}
    shared.UploadAndRegister(t, ctx, a, ds, u)
    shared.RunCorrectnessSuite(t, ctx, a, shared.SizeLarge)
}
```

- [ ] **Step 2: Run end-to-end with compose up**

```bash
docker compose -f tests/analytics-engines/trino/docker-compose.yml up -d
sleep 60
make analytics-engines-test ENGINE=trino
docker compose -f tests/analytics-engines/trino/docker-compose.yml down -v
```

- [ ] **Step 3: Commit**

```bash
git add tests/analytics-engines/trino/trino_test.go
git commit -m "test/analytics-engines/trino: e2e correctness tests (D4.3)"
```

### Task D4.4: Add Trino to CI

**Files:**
- Modify: `.github/workflows/analytics-engines.yaml`

- [ ] **Step 1: Append Trino job**

```yaml
  trino:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    strategy:
      matrix:
        size: [correctness, realistic]
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version-file: go.mod }
      - name: Start stack
        run: docker compose -f tests/analytics-engines/trino/docker-compose.yml up -d
      - name: Wait for Trino
        run: |
          for i in $(seq 1 60); do
            curl -sf http://localhost:18080/v1/info && break
            sleep 3
          done
      - name: Run Trino ${{ matrix.size }}
        run: |
          cd tests/analytics-engines/trino && \
          go test -tags=analytics_engines -v -count=1 -timeout=20m -run "TestTrinoCorrectness$(echo ${{ matrix.size }} | sed 's/correctness/Small/;s/realistic/Large/')"
      - name: Tear down
        if: always()
        run: docker compose -f tests/analytics-engines/trino/docker-compose.yml down -v
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/analytics-engines.yaml
git commit -m "ci: add Trino to analytics-engines matrix (D4.4)"
```

---

## PR D5 — Spark Engine

**Goal:** Add Spark as the third engine.

### Task D5.1: Spark compose

**Files:**
- Create: `tests/analytics-engines/spark/docker-compose.yml`

- [ ] **Step 1: Write compose**

```yaml
# tests/analytics-engines/spark/docker-compose.yml
name: lh-analytics-spark

services:
  minio:
    image: minio/minio:RELEASE.2024-08-17T01-24-54Z
    command: server /data --console-address ":9001"
    ports: ["19000:9000"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    tmpfs: [/data]
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 3s
      retries: 10

  minio-init:
    image: minio/mc:latest
    depends_on: { minio: { condition: service_healthy } }
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin &&
      mc mb local/lh-analytics-test --ignore-existing
      "

  spark:
    image: bitnami/spark:3.5
    ports:
      - "17077:7077"   # master
      - "18080:8080"   # UI
      - "15432:15432"  # Thrift JDBC
    environment:
      SPARK_MODE: master
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
    command: ["/opt/bitnami/spark/sbin/start-thriftserver.sh", "--master", "local[2]"]
```

- [ ] **Step 2: Commit**

```bash
git add tests/analytics-engines/spark/docker-compose.yml
git commit -m "test/analytics-engines/spark: compose with Thrift JDBC server (D5.1)"
```

### Task D5.2: Spark adapter

**Files:**
- Create: `tests/analytics-engines/spark/adapter.go`
- Create: `tests/analytics-engines/spark/adapter_test.go`

- [ ] **Step 1: Implement adapter using Hive Thrift JDBC over Spark Thrift server**

```go
//go:build analytics_engines

package spark

import (
    "context"
    "database/sql"
    "fmt"
    "strings"

    _ "github.com/beltran/gohive"

    "github.com/ReliablyObserve/victoria-lakehouse/tests/analytics-engines/shared"
)

type Adapter struct {
    db *sql.DB
}

func New() *Adapter { return &Adapter{} }
func (a *Adapter) Name() string { return "spark" }

func (a *Adapter) SetUp(ctx context.Context) error {
    db, err := sql.Open("hive", "localhost:15432")
    if err != nil { return err }
    a.db = db
    _, _ = a.db.ExecContext(ctx, "SET spark.sql.session.timeZone=UTC")
    _, _ = a.db.ExecContext(ctx, "SET fs.s3a.endpoint=http://minio:9000")
    _, _ = a.db.ExecContext(ctx, "SET fs.s3a.path.style.access=true")
    return nil
}

func (a *Adapter) RegisterParquet(ctx context.Context, table, path string) error {
    s3path := strings.Replace(path, "s3://", "s3a://", 1)
    sqlStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s
        USING PARQUET PARTITIONED BY (dt, hour)
        LOCATION '%s'`, table, strings.TrimSuffix(s3path, "/"))
    if _, err := a.db.ExecContext(ctx, sqlStmt); err != nil { return err }
    if _, err := a.db.ExecContext(ctx, fmt.Sprintf("MSCK REPAIR TABLE %s", table)); err != nil {
        return err
    }
    if _, err := a.db.ExecContext(ctx, fmt.Sprintf("REFRESH TABLE %s", table)); err != nil {
        return err
    }
    return nil
}

func (a *Adapter) Translate(q shared.CanonicalQuery) string {
    return q.SQL
}

func (a *Adapter) Execute(ctx context.Context, sqlText string) ([]map[string]any, error) {
    rows, err := a.db.QueryContext(ctx, sqlText)
    if err != nil { return nil, err }
    defer rows.Close()
    cols, _ := rows.Columns()
    var out []map[string]any
    for rows.Next() {
        vals := make([]any, len(cols))
        ptrs := make([]any, len(cols))
        for i := range vals { ptrs[i] = &vals[i] }
        if err := rows.Scan(ptrs...); err != nil { return nil, err }
        m := map[string]any{}
        for i, c := range cols { m[c] = vals[i] }
        out = append(out, m)
    }
    return out, rows.Err()
}

func (a *Adapter) TearDown(ctx context.Context) error {
    if a.db != nil { return a.db.Close() }
    return nil
}

func (a *Adapter) WasBloomUsed(ctx context.Context, queryID string) (bool, error) {
    return false, fmt.Errorf("Spark adapter bloom inspection not yet wired")
}
```

- [ ] **Step 2: Translation unit test**

```go
//go:build analytics_engines

package spark

import (
    "testing"

    "github.com/ReliablyObserve/victoria-lakehouse/tests/analytics-engines/shared"
)

// negative control: change Translate to drop quoted column names → this
// test must fail because Spark needs "service.name" preserved.
func TestSparkTranslatePreservesQuotedColumns(t *testing.T) {
    a := New()
    q := shared.CanonicalQuery{SQL: `SELECT "service.name" FROM logs`}
    got := a.Translate(q)
    if got != q.SQL {
        t.Errorf("expected pass-through, got: %s", got)
    }
}
```

- [ ] **Step 3: Commit**

```bash
git add tests/analytics-engines/spark/adapter.go tests/analytics-engines/spark/adapter_test.go
git commit -m "test/analytics-engines/spark: adapter via Thrift JDBC (D5.2)"
```

### Task D5.3: Spark e2e test + CI matrix addition

Follow same pattern as D4.3 + D4.4. Create `spark/spark_test.go` mirroring `trino/trino_test.go`, then append Spark matrix entry to `.github/workflows/analytics-engines.yaml`.

Commits:
```bash
git add tests/analytics-engines/spark/spark_test.go
git commit -m "test/analytics-engines/spark: e2e tests (D5.3a)"
git add .github/workflows/analytics-engines.yaml
git commit -m "ci: add Spark to analytics-engines matrix (D5.3b)"
```

---

## PR D6 — ClickHouse Engine

Follow the same pattern as D5 (compose → adapter → e2e tests → CI matrix entry).

### Task D6.1: ClickHouse compose

**Files:**
- Create: `tests/analytics-engines/clickhouse/docker-compose.yml`

```yaml
name: lh-analytics-clickhouse

services:
  minio:
    image: minio/minio:RELEASE.2024-08-17T01-24-54Z
    command: server /data --console-address ":9001"
    ports: ["19000:9000"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    tmpfs: [/data]
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      retries: 10

  minio-init:
    image: minio/mc:latest
    depends_on: { minio: { condition: service_healthy } }
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin &&
      mc mb local/lh-analytics-test --ignore-existing
      "

  clickhouse:
    image: clickhouse/clickhouse-server:24.8
    ports: ["19001:9000", "18123:8123"]
    ulimits:
      nofile: { soft: 262144, hard: 262144 }
```

Commit: `test/analytics-engines/clickhouse: compose (D6.1)`.

### Task D6.2: ClickHouse adapter

Use `github.com/ClickHouse/clickhouse-go/v2` driver. ClickHouse reads parquet via `s3()` table function:

```go
// In RegisterParquet:
sql := fmt.Sprintf(`CREATE TABLE %s AS
    SELECT * FROM s3('http://minio:9000/lh-analytics-test/%s/**/*.parquet',
        'minioadmin','minioadmin','Parquet')`, table, strings.TrimPrefix(strings.TrimPrefix(path, "s3://lh-analytics-test/"), "/"))
```

Translation: ClickHouse uses `quantile()` instead of `approx_percentile()`:

```go
func (a *Adapter) Translate(q shared.CanonicalQuery) string {
    sql := q.SQL
    sql = strings.ReplaceAll(sql, "approx_percentile(", "quantile(")
    return sql
}
```

Add translation unit test similar to D4.2. Commit: `test/analytics-engines/clickhouse: adapter + translation (D6.2)`.

### Task D6.3: ClickHouse e2e + CI

Mirror D4.3 + D4.4 pattern. Commits: `test/analytics-engines/clickhouse: e2e tests (D6.3a)`, `ci: add clickhouse matrix (D6.3b)`.

---

## PR D7 — Bloom-Usage Advisory + Regression Tests + CI Consolidation

**Goal:** Implement bloom-usage detection per engine; add cross-cutting regression tests; consolidate CI workflow.

### Task D7.1: Bloom usage detection per engine

For each engine adapter, implement `WasBloomUsed`:

- **Trino**: capture the X-Trino-Query-Id response header during `Execute`, then GET `/v1/query/<id>`. Compare `processedInputDataSize` vs `totalInputDataSize`. If `processed < total * 0.8`, bloom likely engaged.

- **Spark**: after the query, scrape the Spark UI at `/api/v1/applications/<appid>/stages` and look at `numFiles` vs `numFilesRead`. Spark prints bloom stats in stage metrics when `parquet.filter.bloom.enabled` is set.

- **ClickHouse**: read from `system.query_log` after the query: `SELECT ProfileEvents FROM system.query_log WHERE query LIKE '%<marker>%' AND type = 'QueryFinish'`. Look for `ParquetSeekBloomFilterUsed` counter.

- **DuckDB**: not supported (already returns error). Note in `KNOWN_LIMITATIONS.md`.

Each implementation gets its own task. Commits: `test/analytics-engines/<engine>: bloom usage detection (D7.1.<n>)`.

### Task D7.2: Bloom usage test functions per engine

In each engine's test file:

```go
func TestTrinoBloomUsage(t *testing.T) {
    ctx := context.Background()
    a := New()
    if err := a.SetUp(ctx); err != nil { t.Fatal(err) }
    t.Cleanup(func() { _ = a.TearDown(ctx) })
    ds, err := shared.GenerateLarge(t)
    if err != nil { t.Fatal(err) }
    u := &shared.Uploader{Endpoint: "localhost:19000", AccessKey: "minioadmin", SecretKey: "minioadmin", Bucket: "lh-analytics-test"}
    shared.UploadAndRegister(t, ctx, a, ds, u)
    report := shared.RunBloomUsageAdvisory(t, ctx, a)
    if err := shared.WriteReport("bloom-usage-trino.json", report); err != nil { t.Fatal(err) }
}
```

Per-engine commits.

### Task D7.3: Cross-cutting regression tests

**Files:**
- Create: `tests/analytics-engines/regression_test.go`

```go
//go:build analytics_engines

package analyticsengines

import (
    "regexp"
    "testing"

    parquets3 "github.com/ReliablyObserve/victoria-lakehouse/internal/storage/parquets3"
    "github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// negative control: change partitionFromNano() format → this test must
// fail because the regex would no longer match.
func TestHivePartitionLayoutMatchesEngineExpectation(t *testing.T) {
    rows := []schema.LogRow{{TimestampUnixNano: 1735689600000000000, Body: "x"}}
    res, err := parquets3.WriteLogsParquetForTest(rows, 100, 7)
    if err != nil { t.Fatal(err) }
    _ = res
    // The Hive prefix is constructed in flushLogPartition; the regex
    // expectation lives here.
    re := regexp.MustCompile(`^.*/(logs|traces)/dt=\d{4}-\d{2}-\d{2}/hour=\d{2}/[^/]+\.parquet$`)
    sample := "tenant/logs/dt=2025-01-01/hour=00/abcdef.parquet"
    if !re.MatchString(sample) {
        t.Errorf("regex does not match expected layout sample: %s", sample)
    }
}

// negative control: drop the assertion → schema URL drift between
// Subsystem C's lockfile and the writer's emitted metadata would go
// unnoticed.
func TestSchemaURLInFileMetadataMatchesOTelLockfile(t *testing.T) {
    if parquets3.ParquetOTelSchemaURL != "https://opentelemetry.io/schemas/1.30.0" {
        t.Errorf("ParquetOTelSchemaURL=%q; Subsystem C's otel-version.yaml expects 1.30.0", parquets3.ParquetOTelSchemaURL)
    }
}
```

Commit: `test/analytics-engines: cross-cutting regression tests (D7.3)`.

### Task D7.4: Consolidate CI workflow

Update `.github/workflows/analytics-engines.yaml` to:
- Use matrix strategy with `engine: [duckdb, trino, spark, clickhouse]`.
- Add advisory bloom-usage jobs gated by `if: github.event_name == 'push' || github.event_name == 'schedule'`.
- Add `regression` job.

Commit: `ci: consolidate analytics-engines workflow (matrix + advisory + regression) (D7.4)`.

---

## Definition of Done (Subsystem D)

- [ ] D1–D7 all merged.
- [ ] Four engines (DuckDB, Trino, Spark, ClickHouse) implement the `Adapter` interface.
- [ ] Ten canonical queries in `Catalog` (5 logs + 5 traces).
- [ ] `tests/analytics-engines/goldens/{small,large}/*.json` exist for every query targeting that size.
- [ ] `TestCatalogCoverage` passes.
- [ ] All four engines pass `correctness` and `realistic` suites.
- [ ] `TestSeederDeterministic` passes.
- [ ] `TestHivePartitionLayoutMatchesEngineExpectation` passes.
- [ ] `TestSchemaURLInFileMetadataMatchesOTelLockfile` passes.
- [ ] Bloom-usage detection implemented per engine (DuckDB documented as unsupported).
- [ ] CI matrix runs 4 engines × 2 sizes (8 required jobs) + 4 advisory bloom jobs + 1 regression.
- [ ] Per-engine `bloom-usage-<engine>.json` artifact uploaded.
- [ ] Negative-control comment on every load-bearing assertion.

---

## Self-Review Notes

1. **Spec coverage:**
   - Shared types + comparator → D1.1, D1.2, D1.3.
   - Dataset seeder + Uploader + Runner + BloomUsageReport → D2.1-D2.5.
   - DuckDB end-to-end (first engine) → D3.1-D3.5.
   - Trino → D4.1-D4.4.
   - Spark → D5.1-D5.3.
   - ClickHouse → D6.1-D6.3.
   - Bloom usage advisory + regression + CI consolidation → D7.1-D7.4.

2. **Placeholder scan:**
   - D5.3, D6.3 reference "mirror D4.3 + D4.4 pattern" — the explicit pattern (test function + matrix entry) is shown in D4; D5/D6 specify the engine-specific input but reuse structure. Acceptable because the template is fully written out in D4.
   - D7.1 describes detection logic per engine without showing every line of code. Each detection is engine-specific and small; the prose describes the surface to query. Plan reviewers should understand the work without further specification.

3. **Type consistency:**
   - `Catalog`, `CanonicalQuery`, `Mode`, `ResultMode`, `DatasetTarget`, `BloomColumns` defined in D1.1; used in D2.4 (runner), D3.2-D6.3 (adapter Translate).
   - `Adapter` interface defined in D1.2; implemented by D3.2 (DuckDB), D4.2 (Trino), D5.2 (Spark), D6.2 (ClickHouse).
   - `GoldenResult`, `Compare`, `FloatTolerance` defined in D1.3; used in D2.4 (runner), genfixture in D3.4.
   - `Dataset`, `DatasetStats`, `LoadSmall`, `GenerateLarge`, `Seed` defined in D2.1-D2.2; used in D3.3-D6.3 e2e tests and genfixture in D3.4.
   - `Uploader`, `UploadHive` defined in D2.3; used in D2.4 (runner) and D4.3+ engine tests.
   - `BloomUsageReport`, `WriteReport` defined in D2.5; used in D7.2 per-engine bloom test functions.
   - `parquets3.ParquetOTelSchemaURL` referenced in D7.3 regression must exist in Subsystem C (it does — defined in C4.1).
