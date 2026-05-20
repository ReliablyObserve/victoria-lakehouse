# Query Performance Optimization — Design Spec

**Date:** 2026-05-20
**Status:** Draft
**Scope:** Benchmark suite, OTEL instrumentation, query latency optimizations, correctness gate

## Competitive Position

Beat Loki (S3+TSDB index) by 2-3x on query latency. Accept 3-5x slower than VL/VT on local disk. This is the target at production-near scale (10K-50K files, 100GB+).

### Latency Targets

| Query type | Loki (S3+TSDB) | **Lakehouse target (cold)** | **Lakehouse target (warm)** | VL/VT (local disk) |
|---|---|---|---|---|
| Exact filter (trace_id) | 3-8s | <2s | <500ms | 100-300ms |
| Exact filter (service_name) | 2-5s | <1.5s | <400ms | 80-200ms |
| Wildcard `*` | 2-5s | <1s | <300ms | 50-200ms |
| `hits` volume graph | 2-5s | <1s | <300ms | 50-200ms |
| `field_names` / `field_values` | 1-3s | <500ms | <200ms | 20-80ms |
| `stats_query_range` | 3-10s | <2s | <1s | 100-400ms |

Cold = first query, no cache. Warm = file data in L1/L2 cache.

### Data Scale Tiers

| Tier | Files | Size | Tenants | Purpose |
|---|---|---|---|---|
| Small (current) | 500 | ~30MB | 1-2 | Fast iteration, CI-compatible |
| Medium (prod-near) | 10K | ~1GB | 5 | Realistic workload validation |
| Large (prod-target) | 50K | ~5GB | 10 | Scale limit testing |

Benchmark at current scale, extrapolate and validate at production-near and production-target.

### Benchmark Execution

Local-only, developer runs manually. Results tracked in `benchmarks/baseline-{signal}-{tier}.json`. No CI integration — GHA is for correctness, not perf.

---

## Phase 0: Correctness Gate

All verification, regression, and coverage work must complete before any performance optimization lands. This ensures every optimization can be validated without breaking existing behavior.

### Verification Test Suite

Tests that prove every output surface produces correct, expected data. Written TDD-style from this spec.

| Surface | What's verified | Test location |
|---|---|---|
| LogsQL endpoints | `/select/logsql/{query,hits,field_names,field_values,streams,stats_query,stats_query_range}` — correct JSON schema, correct row counts, correct filtering, correct time ranges | `internal/selectapi/verify_test.go` |
| Jaeger API | `/api/{traces,services,operations,dependencies}` — correct Jaeger JSON format, span structure, service discovery | `internal/selectapi/jaeger_verify_test.go` |
| Loki push compat | Insert via Loki push → select via LogsQL — field mapping preserved, structured metadata correct | `internal/insertapi/loki_verify_test.go` |
| OTLP insert | Insert via OTLP → select — resource/scope/span attributes mapped correctly | `internal/insertapi/otlp_verify_test.go` |
| ES bulk insert | Insert via ES bulk → select — field names, `_msg` mapping | `internal/insertapi/es_verify_test.go` |
| Prometheus metrics | All 147+ metrics registered, correct labels, counters increment, histograms observe | `internal/metrics/verify_test.go` |
| Stats API | `/lakehouse/api/v1/{stats,tenants,tenants/{id}}` — correct JSON, tenant data matches reality | `internal/stats/verify_test.go` |
| Manifest API | File counts, byte totals, time ranges match actual S3 state | `internal/manifest/verify_test.go` |
| Parquet output | Written parquet files have correct schema, column types, row group structure, bloom filters present | `internal/storage/parquets3/parquet_verify_test.go` |
| OTEL traces output | Lakehouse's own traces have correct span names, attributes, parent-child relationships | `internal/telemetry/verify_test.go` |
| Helm chart | Templates render valid YAML for all value combinations | `charts/victoria-lakehouse/verify_test.go` |

### Regression Test Suite

Golden-file tests that lock in current correct behavior and catch drift during optimization.

| Test type | How it works |
|---|---|
| JSON response snapshots | Insert known data → query → compare response JSON against golden file. Any field change = test failure. |
| Metric value assertions | Insert N rows → assert specific metric values (e.g., `lakehouse_insert_rows_total` = N, `lakehouse_parquet_files_flushed_total` > 0) |
| Round-trip fidelity | Insert log/trace with all fields → select → verify every field value matches exactly. Covers field name translation (OTel dots → underscores), timestamp precision, structured metadata. |
| Multi-tenant isolation | Insert to tenant A and B → query tenant A → verify zero rows from B leak |
| Time range correctness | Insert data at known timestamps → query with tight time range → verify only expected rows return |
| Bloom correctness | Insert known trace_ids → query one → verify only files containing that trace_id were scanned (via metrics) |

### Coverage Targets

| Package | Target |
|---|---|
| `internal/selectapi/` | 95% |
| `internal/storage/parquets3/` | 90% |
| `internal/manifest/` | 95% |
| `internal/bloomindex/` | 90% |
| `internal/stats/` | 90% |
| `internal/cache/` | 90% |
| `internal/config/` | 95% |
| `internal/tenant/` | 90% |
| `internal/schema/` | 90% |
| `internal/telemetry/` (new) | 90% |
| `lakehouse-traces/internal/*` | Same targets per package |
| **Overall** | **90%+** |

### TDD Workflow

Every change (correctness or performance) follows:

1. Write test from spec — expected behavior + assertions
2. Run test — verify it fails or shows current baseline
3. Implement the change
4. Run test — verify it passes, regression suite still green
5. Update golden files only if the change is intentional
6. For performance changes: record before/after in benchmark baseline

### Phase 0 Exit Criteria

- All verification tests pass for both logs and traces
- Regression suite green with golden files committed
- Coverage >90% overall
- No known correctness bugs in query or insert paths

---

## Phase 1: Instrumentation & Baselines

### OTEL SDK Integration

Add `go.opentelemetry.io/otel` to both `go.mod` (logs) and `lakehouse-traces/go.mod`. Both binaries get identical instrumentation.

#### Configuration

```yaml
telemetry:
  enabled: true
  endpoint: "http://victoriatraces:10428/api/v1/traces"
  sample_rate: 0.1        # 10% in production
  always_sample_slow: true # 100% for queries > slow_threshold
  profiling: false         # opt-in continuous profiling
```

`sample_rate: 1.0` during benchmarks. Head-based sampling at 10% + tail-based 100% for slow queries in production.

Overhead: ~2-5μs per span, ~8 spans per query = <50μs total. Negligible vs. ms-level query times.

### Full-Flow Tracing — HTTP to Store

Three injection layers, zero VL/VT modifications:

#### Layer 1: HTTP Middleware

`otelhttp.NewHandler()` wrapping the mux. Automatic spans for every request with method, path, status, duration.

#### Layer 2: `wrapVL()` Span

Child span around each VL handler call:

```go
func (h *Handler) wrapVL(fn func(ctx, w, r)) http.HandlerFunc {
    return func(w, r) {
        ctx, span := tracer.Start(r.Context(), "vl.handler."+endpointName)
        defer span.End()
        fn(ctx, w, r)  // VL receives traced context
    }
}
```

The time gap between `vl.handler` start and first `storage.*` child span = VL's internal overhead (parsing, filter compilation). Measured without modifying VL.

#### Layer 3: TracedStorage Decorator

Wrap the `ExternalStorage` interface. VL calls these methods — each gets a span:

```go
type TracedStorage struct {
    inner storage.Storage
}

func (t *TracedStorage) RunQuery(ctx, tenants, q, fn) error {
    ctx, span := tracer.Start(ctx, "storage.run_query",
        attribute.Int("tenant_count", len(tenants)),
        attribute.String("query", q.String()))
    defer span.End()
    return t.inner.RunQuery(ctx, tenants, q, fn)
}
```

Registered via `vlstorage.SetStorage(TracedStorage{inner: store}, manifest)`.

#### Per-Stage Spans (inside RunQuery)

```
lakehouse.query (root span)
├── manifest.lookup          {files_candidate, files_matched}
├── label.prefilter          {files_skipped}
├── bloom.filter             {partitions_checked, files_skipped, skip_rate}
├── file.fetch [repeated]    {key, cache_tier: "l1|l2|peer|s3", bytes}
├── parquet.open             {row_groups}
├── rowgroup.prune           {skipped_stats, skipped_bloom, skipped_pushdown}
├── row.read                 {rows, bytes}
└── filter.eval              {rows_in, rows_out}
```

#### Insert Path Tracing

```
http.request (root)
├── vl.handler.insert        (VL protocol parsing — black box)
├── storage.add_rows         {row_count, partition}
├── writer.buffer            {buffer_size, buffer_bytes}
├── writer.flush (async)     {partition, rows, compressed_bytes, ratio}
└── s3.put_object            {key, bytes, duration}
```

#### What This Reveals Without Modifying VL/VT

| Measurement | How |
|---|---|
| VL query parsing + filter compilation time | Gap between `vl.handler` start and first `storage.*` span |
| VL insert protocol parsing time | Gap between `http.request` start and `storage.add_rows` |
| Whether VL calls storage once or multiple times | Child span count under `vl.handler` |
| Total VL overhead vs. Lakehouse storage time | Compare `vl.handler` duration minus `storage.*` durations |
| End-to-end latency breakdown | Root span shows full picture |

### Benchmark CLI

**Binary:** `cmd/bench/main.go` — standalone CLI.

```
lakehouse-bench [flags]
  --tier small|medium|large
  --signal logs|traces|both
  --endpoint hits|query|all
  --profile
  --otel-endpoint
  --minio-endpoint
  --runs 3
  --output benchmarks/
```

#### Seed Phase — Via Real LH Insert APIs

```
1. Seed via LH insert endpoints
   ├── Logs: POST /insert/jsonline to lakehouse-logs:9428
   │   └── 20 service_names, 4 levels, 50 hosts, high trace_id cardinality
   ├── Traces: POST /insert/otlp to lakehouse-traces:10428
   │   └── 20 service_names, 30 span_names, 5 span_kinds, 3 status_codes
   └── Record write metrics

2. Wait for flush + manifest refresh
   └── Poll /lakehouse/api/v1/stats until expected file count

3. Query phase via LH select endpoints
   ├── Logs: /select/logsql/* on lakehouse-logs:9428
   └── Traces: /select/logsql/* + /select/jaeger/api/* on lakehouse-traces:10428
   └── Cold → warm → hot cycle per query

4. Write results — both write and read baselines
```

#### Write Benchmark Matrix

| Protocol | Signal | Batch sizes | Measures |
|---|---|---|---|
| jsonline | logs | 100, 1000, 10000 rows | Ingest throughput + flush latency |
| otlp | traces | 100, 1000, 10000 spans | OTLP parsing + write path |
| loki push | logs | 1000 streams | Loki compat overhead vs jsonline |
| elasticsearch bulk | logs | 1000 docs | ES compat overhead |

#### Read Benchmark Matrix

| Endpoint | Filters tested | Both signals |
|---|---|---|
| `/select/logsql/hits` | `*`, `trace_id:="X"`, `service_name:="Y"`, `level:="ERROR"` | yes |
| `/select/logsql/query` | same + `_msg:"keyword"` | yes |
| `/select/logsql/field_names` | no filter | yes |
| `/select/logsql/field_values` | `service_name`, `trace_id` | yes |
| `/select/logsql/stats_query_range` | `* \| count()` by service_name | yes |
| `/select/jaeger/api/traces/{id}` | single trace lookup | traces only |
| `/select/jaeger/api/services` | no filter | traces only |

#### Baseline File Format

`benchmarks/baseline-{signal}-{tier}.json`:

```json
{
  "timestamp": "2026-05-20T01:15:00Z",
  "git_sha": "dc5a596",
  "tier": "small",
  "signal": "logs",
  "file_count": 500,
  "total_bytes": 31457280,
  "write": {
    "jsonline_1000": {
      "rows_per_sec": 45000,
      "p50_ms": 12,
      "p95_ms": 28,
      "flush_ms": 340,
      "compression_ratio": 7.2
    }
  },
  "read": [
    {
      "endpoint": "/select/logsql/hits",
      "filter": "trace_id:=\"abc123\"",
      "cold_ms": 4850,
      "warm_ms": 1200,
      "hot_ms": 890,
      "stages": {
        "manifest_lookup_ms": 2,
        "label_prefilter_ms": 5,
        "bloom_filter_ms": 45,
        "file_fetch_ms": 3800,
        "parquet_open_ms": 120,
        "rowgroup_prune_ms": 15,
        "row_read_ms": 680,
        "filter_eval_ms": 180
      },
      "cache": {"l1_hits": 0, "l2_hits": 0, "s3_fetches": 45},
      "bloom": {"files_checked": 500, "files_skipped": 455, "skip_rate": 0.91}
    }
  ]
}
```

### Phase 1 Exit Criteria

- OTEL traces visible in Grafana/Jaeger for both logs and traces queries
- Full HTTP→VL→storage→S3 flow traced with per-stage spans
- Insert path traced from HTTP→VL→buffer→flush→S3
- Benchmark CLI runs against MinIO, produces baseline JSON for small tier
- Write and read baselines established for both signals

---

## Phase 2: Latency Optimizations

Each optimization follows the TDD cycle: write test → measure baseline → implement → verify improvement → regression suite green. Order may change based on Phase 1 profiling data.

### Priority 1: Hits-Specific Fast Path

`/select/logsql/hits` only needs timestamps bucketed into intervals. Currently runs full `RunQuery`, reading all columns.

**Implementation:**
- `RunHitsQuery` storage method — reads only `_time` column from parquet
- For unfiltered `*` queries: pure metadata path using manifest + row group stats, zero S3 reads
- For filtered queries: read `_time` + filter columns only

**Expected improvement:** 5-10x for hits queries. The 5s `trace_id` hits query should drop to <1s.

**Applies to:** logs and traces.

### Priority 2: Column Projection Pushdown

`queryFile()` reads ALL columns via `parquet.NewGenericRowGroupReader[LogRow]`. Most queries reference 2-3 fields out of 10+.

**Implementation:**
- Map LogsQL query's referenced fields to parquet column indices
- Pass column projection to parquet reader
- Fall back to full read for `*` or complex queries

**Expected improvement:** 2-4x reduction in I/O and decompression.

**Applies to:** logs and traces.

### Priority 3: Parallel Row Group Processing

Row groups within a single file are processed sequentially. Large files have 3-10 row groups.

**Implementation:**
- Fan out row group processing within `queryFile()` using sub-worker pool
- Cap at 2-3 goroutines per file to avoid over-subscription with the outer file worker pool

**Expected improvement:** 1.5-2x for queries hitting large files.

**Applies to:** logs and traces.

### Priority 4: Bloom Index Coverage — Full Field Matrix

Bloom indexes must cover high-cardinality and high-frequency filter fields for both signals.

#### Logs Fields

| Field | Cardinality | Filter pattern |
|---|---|---|
| `trace_id` | very high | exact match |
| `service_name` | medium (20-100) | exact match, `in()` |
| `host_name` | medium | exact match |
| `k8s_namespace_name` | low-medium | exact match |
| `k8s_pod_name` | high | exact match |
| `k8s_deployment_name` | medium | exact match |
| `deployment_environment` | low (3-5) | exact match |
| `level` / `severity` | very low (4) | exact match |

#### Traces Fields

| Field | Cardinality | Filter pattern |
|---|---|---|
| `trace_id` | very high | exact match |
| `service_name` / `service.name` | medium (20-100) | exact match, `in()` |
| `span_name` / `span.name` | high | exact match |
| `k8s_namespace_name` | low-medium | exact match |
| `k8s_pod_name` | high | exact match |
| `k8s_deployment_name` | medium | exact match |
| `deployment_environment` | low (3-5) | exact match |
| `span_kind` | very low (5) | exact match |
| `status_code` | very low (3) | exact match |

**Implementation:**
- Verify each field has `HasBloom=true` in schema registry for its signal
- Verify bloom indexes are built during flush for all listed fields
- Extend `buildBloomChecks()` to handle `field:in("a","b")` (not just `field:="value"`)
- Add per-field bloom effectiveness metrics to OTEL traces: `{field, files_checked, files_skipped, skip_rate}`
- Benchmark with realistic cardinality per field

**Applies to:** logs and traces with their respective field sets.

### Priority 5: Manifest Time-Range Index Upgrade

`GetFilesForRange()` iterates all partitions linearly. Negligible at current scale, meaningful at 10K+ files.

**Implementation:**
- Sort partitions by time, binary search for range lookups
- Add per-file min/max timestamp to FileInfo from parquet metadata enrichment
- Sub-hour precision pruning using file-level timestamps

**Expected improvement:** Negligible at current scale, measurable at production-near.

**Applies to:** logs and traces (shared `internal/manifest/`).

### Phase 2 Exit Criteria

- Each optimization has before/after benchmark comparison
- Regression suite green after each optimization
- Latency targets met at small tier
- Medium tier validates extrapolation

---

## Phase 3: Concurrency Stress Testing

### Concurrent Query Benchmark

- 10, 50, 100 parallel queries against medium tier
- Mix of endpoint types and filter patterns
- Measure: p50/p95/p99 latency, throughput (queries/sec), error rate

### Mixed Read/Write

- Concurrent ingest (continuous jsonline/otlp stream) + queries
- Measure write throughput degradation under query load
- Measure query latency degradation under write load

### Configuration Tuning

- Validate `MaxConcurrent=32` and `FileWorkers=8` defaults
- Test with 2x and 0.5x values, measure impact
- Document recommended settings per deployment size

### Scale Validation

- Run full benchmark at medium (10K files) and large (50K files) tiers
- Verify latency targets hold at production-near scale
- Identify scale-dependent bottlenecks (manifest size, bloom index loading, cache pressure)

### Phase 3 Exit Criteria

- Latency targets met at production-near scale (10K-50K files)
- Concurrency doesn't degrade beyond 2x vs. single query at 50 concurrent
- Mixed read/write shows <20% mutual interference
- Configuration recommendations documented

---

## Documentation Updates

Full docs upgrade as part of Phase 0:

| Document | Update |
|---|---|
| `docs/architecture.md` | Add query execution flow diagram, cache hierarchy, bloom index pipeline |
| `docs/performance.md` (new) | Latency targets, benchmark methodology, tuning guide |
| `docs/telemetry.md` (new) | OTEL configuration, trace span reference, metric catalog |
| `docs/docker-compose-setup.md` | Add OTEL trace visibility, benchmark CLI usage |
| `README.md` | Performance section with competitive positioning |
| `CHANGELOG.md` | Track each optimization with measured improvement |

---

## Applies to Both Signals

Every component in this spec applies to both `lakehouse-logs` and `lakehouse-traces` in parallel:

- Shared packages (`internal/selectapi/`, `internal/storage/parquets3/`, `internal/manifest/`, `internal/bloomindex/`, `internal/cache/`, `internal/stats/`, `internal/config/`, `internal/telemetry/`) are implemented once, used by both binaries.
- Signal-specific packages (`lakehouse-traces/internal/selectapi/`, `lakehouse-traces/internal/vlstorage/`) get the same instrumentation and testing.
- Benchmark CLI tests both signals with their respective insert protocols and query patterns.
- Bloom field matrices differ per signal (logs vs traces fields) but use the same index infrastructure.
