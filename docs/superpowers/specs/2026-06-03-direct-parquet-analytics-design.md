# Subsystem D — Direct Parquet Analytics

**Status:** Design

**Date:** 2026-06-03

**Continues:** Subsystem C (Parquet format compatibility) — D's small dataset reuses C's `golden/v1/{logs,traces}.parquet`.

**Successor:** Subsystem E (community standards, narrowed scope) — OTel column-name validation moved to C; Apache Arrow IPC and license/SBOM stay in E.

---

## Goal

Prove LH's published claim that any Parquet-capable engine can query LH data directly. Cover four OSS engines with reference-workload tests where every engine must produce identical results to a canonical reference (DuckDB).

This subsystem must:
1. Test four engines: DuckDB, Trino, Spark, ClickHouse.
2. Run 10 canonical queries (5 logs + 5 traces) against each engine.
3. Use two datasets: a small one (Subsystem C's `golden/v1/`) for correctness; a large deterministic one (10k rows across 6 partitions, fixed RNG seed) for realistic patterns.
4. Compare every engine's result against pre-computed golden JSON files.
5. Provide advisory checks for Parquet bloom filter usage per engine (operator visibility, not blocking).
6. Provide a regression test guaranteeing LH's Hive partition layout doesn't drift in a way that breaks the documented engine integrations.

---

## Architecture

Tests live under `tests/analytics-engines/`. Each engine has its own subdirectory with a self-contained docker compose stack. CI uses matrix parallelism so all four engines run concurrently on separate runners.

```
tests/analytics-engines/
├── queries/                                # SQL files (informational; canonical SQL lives in Go)
├── goldens/
│   ├── small/<query>.json                  # results from canonical reference (DuckDB) against C's golden/v1/
│   └── large/<query>.json                  # results from DuckDB against the 10k generated dataset
├── duckdb/{docker-compose.yml, adapter.go, adapter_test.go, duckdb_test.go}
├── trino/{docker-compose.yml, catalog/, adapter.go, adapter_test.go, trino_test.go}
├── spark/{docker-compose.yml, adapter.go, adapter_test.go, spark_test.go}
├── clickhouse/{docker-compose.yml, adapter.go, adapter_test.go, clickhouse_test.go}
├── shared/{canonical_query.go, adapter.go, golden_compare.go, dataset_seeder.go,
│           minio_uploader.go, bloom_usage.go}
└── regression_test.go                      # cross-cutting (Hive layout, OTel URL pin sync)
```

Each engine's compose brings up MinIO (object storage) and the engine itself. Tests:
1. Spin up the engine's compose stack.
2. Either load the small dataset (Subsystem C's goldens) or generate the large deterministic dataset.
3. Upload the parquet files to MinIO under the Hive-prefixed layout (`logs/dt=YYYY-MM-DD/hour=HH/...`).
4. Register the location as a table in the engine.
5. Run each canonical query through the engine; compare result with the matching golden JSON.
6. Tear down stack.

CI workflow `.github/workflows/analytics-engines.yaml` matrix-expands across engines and dataset sizes:

```yaml
strategy:
  matrix:
    engine: [duckdb, trino, spark, clickhouse]
    size:   [correctness, realistic]
```

= 8 required jobs. Plus 4 advisory bloom-usage jobs (nightly + push to main) and 1 cross-cutting regression job = 13 jobs total.

---

## Components

### Canonical query catalog (`shared/canonical_query.go`)

A canonical query is the "what" (analytical intent), not the engine SQL "how". Each adapter's `Translate` method may rewrite syntax for engine quirks; the canonical SQL is ANSI-leaning.

```go
type Mode int
const (
    ModeLogs Mode = iota
    ModeTraces
)

type ResultMode int
const (
    OrderedRows ResultMode = iota   // top-N or hourly buckets — preserve order
    SetEqual                        // any row order
    Aggregate                       // scalar / single-row
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
    // 5 logs queries + 5 traces queries; see implementation plan for full list.
}
```

The catalog ships 10 queries:

**Logs (5):**
- `logs_count_total` — total row count.
- `logs_top_services_by_error_count` — top 5 by ERROR severity (exercises `service.name` bloom).
- `logs_volume_by_severity` — group-by severity.
- `logs_volume_by_hour` — group-by hour (exercises partition pruning).
- `logs_filter_by_trace_id` — single trace_id filter (exercises `trace_id` bloom).

**Traces (5):**
- `traces_count_total`.
- `traces_latency_percentiles_by_service` — p50/p95/p99 per service.
- `traces_span_count_by_trace` — distribution of spans per trace.
- `traces_root_spans_count` — count of `parent_span_id = ''`.
- `traces_top_services_by_span_count` — top 5 by volume.

### Adapter interface (`shared/adapter.go`)

```go
type Adapter interface {
    Name() string
    SetUp(ctx context.Context) error
    RegisterParquet(ctx context.Context, table string, path string) error
    Translate(query CanonicalQuery) string
    Execute(ctx context.Context, sql string) ([]map[string]any, error)
    TearDown(ctx context.Context) error
    WasBloomUsed(ctx context.Context, queryID string) (bool, error) // engine-specific telemetry
}
```

Per-engine adapters live in `<engine>/adapter.go`.

### Dataset seeder (`shared/dataset_seeder.go`)

```go
type DatasetSize int
const (
    SizeSmall DatasetSize = iota
    SizeLarge
)

type Dataset struct {
    Size       DatasetSize
    LogsURI    string        // s3://bucket/logs/ for engines; local file path for DuckDB direct
    TracesURI  string
    Stats      DatasetStats
}

type DatasetStats struct {
    LogsRowCount     int
    TracesRowCount   int
    PartitionCount   int
    DistinctServices int
    DistinctTraceIDs int
}

const Seed int64 = 1735689600 // Unix nanos of 2025-01-01T00:00:00Z — canonical anchor

func LoadSmall(t *testing.T) Dataset
func GenerateLarge(t *testing.T) (Dataset, error)
```

`LoadSmall` points at `tests/parquet-format/golden/v1/{logs,traces}.parquet`.

`GenerateLarge` produces a controlled distribution using the fixed seed:
- 5 services (`svc-1`..`svc-5`).
- 100 trace_ids × 50 spans = 5000 spans total.
- 10000 logs (2000 per service); severity 70% INFO / 20% WARN / 8% ERROR / 2% FATAL.
- Time window: 2026-01-01T00:00 through 2026-01-01T06:00 → 6 partitions (`hour=00` through `hour=05`).
- 1 parquet file per partition per mode = 6 logs + 6 traces files.

### Golden comparator (`shared/golden_compare.go`)

```go
type GoldenResult struct {
    QueryName string                       `json:"query"`
    Mode      string                       `json:"mode"`
    Engine    string                       `json:"engine,omitempty"` // omitted in committed goldens
    Rows      []map[string]any             `json:"rows"`
    Stats     GoldenStats                  `json:"stats,omitempty"`
}

type GoldenStats struct {
    RowsRead    int64
    BytesScanned int64
}

const FloatTolerance = 0.005 // 0.5% relative tolerance

func Compare(expected, actual GoldenResult, mode ResultMode) string
```

Result coercion before compare:
- Integers → `int64`.
- Floats → `float64` (with tolerance).
- Strings → `string`.
- Timestamps → INT64 nanoseconds.

### MinIO uploader (`shared/minio_uploader.go`)

```go
type Uploader struct {
    Endpoint  string
    AccessKey string
    SecretKey string
    Bucket    string
}

func (u *Uploader) UploadHive(ctx context.Context, localRoot, s3Prefix string) error
```

Each engine job uses bucket `lh-analytics-test`. Uploader retries each PUT up to 3× with exponential backoff.

### Bloom usage advisory (`shared/bloom_usage.go`)

```go
type BloomUsageReport struct {
    Engine string                   `json:"engine"`
    Used   map[string]bool          `json:"used"`     // query name → used
    Notes  map[string]string        `json:"notes"`    // optional per-query explanation
}

func WriteReport(path string, r BloomUsageReport) error
```

Each adapter implements `WasBloomUsed` against its engine's telemetry:
- Trino: query info JSON's `processedInputDataSize` vs `totalDataSize`.
- Spark: `df.queryExecution.executedPlan.metrics["filesPruned"]`.
- ClickHouse: `system.query_log` filter rows.
- DuckDB: `EXPLAIN ANALYZE` output parse.

Detection is best-effort; report logs `unknown` when detection is not reliable.

### Per-engine compose files

Pinned image tags (no `:latest`):
- `duckdb`: MinIO only; DuckDB embedded via `github.com/marcboeker/go-duckdb`.
- `trino`: `trinodb/trino:439` + MinIO. Hive catalog config at `trino/catalog/hive.properties`.
- `spark`: `bitnami/spark:3.5` master + 1 worker + MinIO. Spark uses in-memory Hive catalog.
- `clickhouse`: `clickhouse/clickhouse-server:24.8` + MinIO.

### CI workflow (`.github/workflows/analytics-engines.yaml`)

| Job | Trigger | Required for merge |
|-----|---------|--------------------|
| `analytics-engines (duckdb, correctness)` | every PR | yes |
| `analytics-engines (duckdb, realistic)` | every PR | yes |
| `analytics-engines (trino, correctness)` | every PR | yes |
| `analytics-engines (trino, realistic)` | every PR | yes |
| `analytics-engines (spark, correctness)` | every PR | yes |
| `analytics-engines (spark, realistic)` | every PR | yes |
| `analytics-engines (clickhouse, correctness)` | every PR | yes |
| `analytics-engines (clickhouse, realistic)` | every PR | yes |
| `analytics-engines (<each>, bloom-usage)` | nightly + push to main | no (advisory) |
| `analytics-engines (regression)` | every PR | yes |

Path triggers:
- `internal/schema/**`
- `internal/storage/parquets3/**`
- `lakehouse-traces/internal/schema/**`
- `lakehouse-traces/internal/storage/parquets3/**`
- `tests/analytics-engines/**`
- `tests/parquet-format/golden/**`
- `.github/workflows/analytics-engines.yaml`

---

## Data Flow

### CI matrix run (per engine)

1. PR modifies a path under triggers.
2. Matrix expands to 4 engines × 2 sizes = 8 jobs, plus 1 regression job.
3. Per-engine job:
   a. Check out repo.
   b. Set up Go.
   c. `docker compose -f tests/analytics-engines/<engine>/docker-compose.yml up -d`.
   d. Wait for MinIO and engine healthy.
   e. Run `correctness` or `realistic` sub-suite.
   f. Upload per-query result JSONs + bloom-usage report as artifacts.
   g. `docker compose down -v`.
4. Status check `analytics-engines / matrix-coverage` aggregates the 8 required jobs.

### Correctness sub-suite (small dataset)

1. `LoadSmall` → paths to Subsystem C's `golden/v1/{logs,traces}.parquet`.
2. `Uploader.UploadHive` copies them into MinIO under `logs/dt=2025-01-01/hour=00/` and `traces/dt=2025-01-01/hour=00/`.
3. `RegisterParquet` registers the location as a table in the engine.
4. For each `CanonicalQuery` with `DatasetTarget ≠ DatasetLarge`:
   - Adapter's `Translate` returns engine-specific SQL.
   - Execute via engine driver.
   - Result coerced to canonical types.
   - `Compare` against `goldens/small/<query>.json`.
   - Mismatch → `t.Fatal`.

### Realistic sub-suite (large dataset)

1. `GenerateLarge(t)` produces 10k logs + 5k traces in a temp directory using fixed seed.
2. Uploader places files in MinIO Hive layout across 6 partitions.
3. Same query execution + comparison loop.
4. For queries with `RequiresPushdown: true`, additionally inspect engine telemetry; log to artifact if pushdown didn't engage.

### Bloom-usage advisory

After each query with non-empty `BloomColumns`, `adapter.WasBloomUsed` is called. Result aggregated into `BloomUsageReport` and written to `bloom-usage-report.json` as a CI artifact. Never `t.Fail`.

### Adding a new canonical query

1. Author the query in `shared/canonical_query.go` (`Catalog` slice).
2. Run `make analytics-engines-goldens-small` + `analytics-engines-goldens-large`.
3. Commit catalog change + two new JSON goldens.
4. CI matrix runs the query against all 4 engines; divergent results block merge.

### Adding a new engine

1. Create `tests/analytics-engines/<engine>/` with `docker-compose.yml`, `adapter.go`, `<engine>_test.go`.
2. Implement `Adapter` interface.
3. Add `<engine>` to CI matrix.
4. First CI run exercises all goldens against the new engine.

### Engine version bump

1. Update pinned image tag in `<engine>/docker-compose.yml`.
2. Re-run CI; goldens that diverge: triage (new version correct → re-record goldens; or regression → revert).

### Schema change in LH

1. Subsystem C's coverage gate fires first.
2. Subsystem D's small-dataset tests then exercise the new schema via C's `golden/v<N>/` files after C's bump workflow.
3. If a column is renamed: D's queries reference column names directly and would fail. Adapter's `Translate` may map names, but canonical SQL uses LH-emitted names.

---

## Error Handling

### Engine image unavailable / network outage
- Sentinel exit code 3 (infra failure), distinct from real test failure code 1.

### Engine startup timeout (>90s)
- `STARTUP_TIMEOUT: <engine> not healthy after 90s`; container logs included.

### Query rejected by engine
- `QUERY_REJECTED: <engine>/<query>: <error>`. Triage as adapter bug or LH writer bug.

### Cross-engine result divergence
- `RESULT_DIVERGENCE: <engine>/<query>: <diff>`. Triage manually; usually adapter SQL rewrite needed.

### Float precision mismatch
- `FloatTolerance = 0.005` (0.5%). Engines' `approx_percentile` implementations differ (Spark t-digest, DuckDB reservoir, Trino QDigest).
- If a query exceeds tolerance consistently, mark with custom tolerance or document divergence.

### Timestamp precision loss
- Comparator coerces timestamps to INT64 nanos before compare. Engine emitting floats → `TIMESTAMP_PRECISION_LOSS` failure.

### Dataset seeder non-determinism
- `TestSeederDeterministic` runs `GenerateLarge` twice; byte-compares output. Mismatch → seeder bug (likely time-of-day call).

### Hive partition layout drift
- `TestHivePartitionLayoutMatchesEngineExpectation` writes a file, asserts S3 key matches `^.*/(logs|traces)/dt=\d{4}-\d{2}-\d{2}/hour=\d{2}/[^/]+\.parquet$`.

### Bloom filter not consulted
- Advisory only. `BLOOM_USED` / `BLOOM_NOT_USED` logged.

### Predicate pushdown not engaged
- Advisory for queries with `RequiresPushdown: true`. Logged; doesn't fail.

### Engine caches stale data
- Pre-query: explicit cache invalidation per engine (Trino `CALL system.flush_metadata_cache()`, Spark `REFRESH TABLE`, ClickHouse `DETACH/ATTACH TABLE`, DuckDB re-attach).

### MinIO eventual consistency
- After upload, retry table re-registration up to 5s before failing.

### Missing golden file
- `MISSING_GOLDEN: run 'make analytics-engines-goldens-<size>' to record initial expectation`. Never auto-record in CI.

### Engine version pin yanked from registry
- Same as image unavailable (exit 3). Mitigation: separate GHCR image-mirroring cron (out of scope here).

### Disk space exhaustion
- 30-minute per-job timeout caps runaway processes. `t.TempDir()` auto-cleans. Volumes use `tmpfs` where possible.

### Network flakiness during MinIO upload
- 3 retries with exponential backoff. Failure → `UPLOAD_FAILED: <key>` (distinct from query failures).

### ClickHouse-specific quirks
- S3 table function requires explicit `Parquet` format argument.
- Some types need explicit `String` cast.
- Handled in CK adapter's `Translate`.

### Spark Hive catalog
- Uses in-memory catalog (no external Hive Metastore).
- `RegisterParquet` issues `CREATE TABLE ... USING PARQUET PARTITIONED BY (dt, hour) LOCATION '<minio_uri>'` then `MSCK REPAIR TABLE` to discover partitions.

### DuckDB embedded (no container)
- DuckDB adapter is special: no docker compose for the engine itself; embedded via `go-duckdb`.
- MinIO still in docker. DuckDB reads via S3 httpfs extension.

---

## Testing Strategy

### Unit tests (no docker, no network)

`shared/canonical_query_test.go`:
- `TestCatalogNoDuplicateNames`
- `TestCatalogQueriesAllHaveDescription`
- `TestCatalogQueriesHaveValidResultMode`
- `TestCatalogQueriesHaveValidDatasetTarget`
- `TestCatalogCoverage` — every query has the expected golden files.

`shared/golden_compare_test.go`:
- `TestCompareSetEqualUnordered`
- `TestCompareOrderedRowsRespectsOrder`
- `TestCompareFloatTolerance`
- `TestCompareFloatToleranceExceeded`
- `TestCompareTimestampNanosCoerced`

`shared/dataset_seeder_test.go`:
- `TestSeederDeterministic`
- `TestSeederStatsMatchSpec`
- `TestSeederSeverityDistribution`

### Integration tests (per engine, build tag `analytics_engines`)

Each engine has `correctness`, `realistic`, and `bloom-usage` test functions following a shared template that drives `Adapter`, `LoadSmall`/`GenerateLarge`, and `RunCorrectnessSuite`/`RunBloomUsageAdvisory`.

### Adapter-specific unit tests

Each engine has 2–3 unit tests covering SQL translation quirks specific to that engine (e.g., Trino `approx_percentile` → `approx_quantile`).

### Cross-cutting regression tests

`tests/analytics-engines/regression_test.go`:
- `TestHivePartitionLayoutMatchesEngineExpectation`
- `TestSchemaURLInFileMetadataMatchesOTelLockfile`

### Negative-control proofs

Every load-bearing assertion has a comment naming the production code that must break it.

---

## Migration Plan & Deliverables

Seven PRs sequentially. Each independently mergeable; each leaves CI green.

| PR | Scope | New files (approx LoC) | Risk |
|----|-------|------------------------|------|
| **D1** | Shared infrastructure | `shared/canonical_query.go`, `shared/adapter.go`, `shared/golden_compare.go`, plus unit tests | Low — Go-only, no docker |
| **D2** | Dataset seeder + MinIO uploader | `shared/dataset_seeder.go`, `shared/minio_uploader.go`, determinism tests | Low — depends on parquet-go and minio-go |
| **D3** | DuckDB engine (first end-to-end) | `duckdb/{adapter.go, adapter_test.go, duckdb_test.go, docker-compose.yml}`. Goldens generated. Makefile targets. First CI job. | Medium — CGO + libduckdb in CI |
| **D4** | Trino engine | `trino/` package; extends CI matrix | Medium — Trino startup ~30s |
| **D5** | Spark engine | `spark/` package | Medium — Spark image ~1GB, JVM startup |
| **D6** | ClickHouse engine | `clickhouse/` package | Low — CK Parquet support is well-understood |
| **D7** | Bloom-usage advisory + regression tests + CI consolidation | `shared/bloom_usage.go`, per-engine bloom test functions, `regression_test.go`, CI workflow restructuring | Low — additive, advisory |

**Ordering rationale:**
- D1 lays the shared interfaces.
- D2 produces dataset; determinism gate ensures stability before any engine consumes it.
- D3 is the simplest engine end-to-end (no separate container); validates full pipeline before adding heavier engines.
- D4–D6 fan out engines one at a time; each PR reviewable in isolation.
- D7 adds advisory layers and cross-cutting regression.

---

## Definition of Done

### Coverage
- [ ] Four engines covered: DuckDB, Trino, Spark, ClickHouse.
- [ ] Ten canonical queries (5 logs + 5 traces) in `Catalog`.
- [ ] Small + large datasets both exercised.
- [ ] `TestCatalogCoverage` passes.

### Correctness
- [ ] All 4 engines pass `correctness` on every query.
- [ ] All 4 engines pass `realistic` on every query.
- [ ] Cross-engine result equality within `FloatTolerance` for aggregates.
- [ ] `TestSeederDeterministic` passes.
- [ ] `TestHivePartitionLayoutMatchesEngineExpectation` passes.
- [ ] `TestSchemaURLInFileMetadataMatchesOTelLockfile` passes.

### Infrastructure
- [ ] Each engine has its own `docker-compose.yml` with pinned image tag.
- [ ] CI workflow matrix-parallels 4 engines × 2 sizes.
- [ ] 8 required jobs + 4 advisory bloom-usage jobs + 1 cross-cutting regression job.
- [ ] Job artifacts uploaded.
- [ ] Path triggers cover schema/storage/test directories + C's goldens.

### Engine adapters
- [ ] Each adapter implements `Adapter` interface.
- [ ] Each adapter has ≥2 syntax-translation unit tests.
- [ ] DuckDB adapter runs in-process.

### Advisory
- [ ] Bloom-usage detection implemented per engine.
- [ ] `bloom-usage-report.json` uploaded every advisory run.

### Negative-control proofs
- [ ] Every load-bearing assertion has a negative-control comment.

---

## Out of Scope

Deferred to other subsystems:
- **Apache Arrow IPC compatibility, license/SBOM audit** — narrowed Subsystem E.

Deferred indefinitely:
- **Cloud engines (Databricks, Snowflake)** — credential-gated; documented but not CI-tested.
- **Performance benchmarking across engines** — track via existing `cmd/bench/` if needed.
- **StarRocks, Doris, pandas** — OSS-core four cover dominant use cases; incremental adds possible later.
- **Iceberg/Delta Lake table format** — LH writes raw Parquet with Hive partitioning; format adoption is a separate decision.
- **Federated queries** — D validates "engine → LH", not "engine → engine → LH".

---

## Open Questions

None — all decisions resolved during brainstorming:
- Engine scope: OSS-only core (DuckDB, Trino, Spark, ClickHouse).
- Validation: reference workload with goldens; cross-engine result equality.
- Dataset: hybrid (small committed via C; large RNG-seeded 10k rows, 6 partitions).
- Harness: per-engine compose with CI matrix parallelism.
