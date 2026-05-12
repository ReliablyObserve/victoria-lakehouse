# Changelog

All notable changes to Victoria Lakehouse will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.20.0] - 2026-05-12

### Added
- Multi-tenancy — single binary serves all tenants via header-based routing with S3 prefix isolation (`{AccountID}/{ProjectID}/`, default `0/0/`), matching Grafana Loki/Tempo pattern. Enterprise option for bucket-per-tenant isolation with separate IAM policies
- Global read mode — optional `--lakehouse.tenant.global-read-header` / `--lakehouse.tenant.global-read-value` for admin dashboards that query across all tenants (disabled by default, explicit opt-in)
- Analytics engines documentation — comprehensive guide covering 9 Parquet engines (DuckDB, ClickHouse, Trino, Databricks, Snowflake, StarRocks, Doris, Spark, pandas) with Grafana datasource status, query examples, and integration guides
- Tenant configuration flags — `--lakehouse.tenant.isolation` (prefix/bucket), `--lakehouse.tenant.bucket-template`, `--lakehouse.tenant.default-account`, `--lakehouse.tenant.default-project`, `--lakehouse.tenant.header-account`, `--lakehouse.tenant.header-project`, `--lakehouse.tenant.global-read-header`, `--lakehouse.tenant.global-read-value`
- Multi-level select architecture — vlselect/vtselect fan out queries to both hot (disk) and cold (lakehouse S3) storage nodes for unified hot+cold results
- VictoriaTraces hot tier in Docker Compose — standalone VT instance with 24h disk retention
- Datagen trace dual-write — `--vt-endpoint` flag pushes traces to VictoriaTraces via Zipkin `/api/v2/spans` alongside S3 Parquet writes
- Eleven Grafana datasources — Global VL/VT (via vlselect/vtselect), Hot VL/VT (direct disk), Cold logs/traces (lakehouse S3), Loki proxy (hot+cold), DuckDB analytics, ClickHouse analytics/logs/traces
- DuckDB Grafana datasource — in-memory DuckDB with `httpfs` extension for direct SQL on S3 Parquet files via `read_parquet()`
- ClickHouse analytics engine — pre-configured with `lakehouse.logs` and `lakehouse.traces` views querying MinIO Parquet via `s3()` table function, with dedicated Grafana Logs and Traces datasources for native log/trace panel visualization on raw Parquet
- Architecture diagram in Docker Compose docs showing full data flow across all tiers

### Changed
- Docker Compose hot tier retention reduced from 7d to 24h to match cold boundary
- Grafana default datasource changed to VictoriaLogs Global (via vlselect) for unified hot+cold queries
- Grafana image changed from Alpine to Ubuntu (`grafana/grafana:latest-ubuntu`) — required for DuckDB plugin (glibc dependency)

## [0.18.2] - 2026-05-12

### Fixed
- Fix Jaeger trace search returning null data — use VT-canonical field names (`"resource_attr:service.name"`, `name`, `duration`) with LogsQL quoting for colon-containing fields
- Fix loki-vl-proxy hot+cold routing — VictoriaLogs serves hot data (<24h), lakehouse-logs serves cold data via `-cold-enabled` with 1h overlap
- Add `external_query.go` patch to auto-release workflow — fixes binary build failure (`undefined: logstorage.QueryHasPipes`)
- Update e2e compose loki-vl-proxy from broken local build path to published GHCR image v1.31.2
- Format `_time` column as RFC3339Nano instead of raw nanoseconds — fixes VL handler timestamp parsing for all query endpoints
- Recover from `writeBlock` panics caused by unsupported VL pipe processors (e.g. `CountByTimePipe` in `/hits`) — prevents query crashes, returns partial results instead
- Add `filter.go` to traces module for metadata filter scoping — traces `GetFieldNames`/`GetFieldValues` now correctly apply LogsQL filters
- Apply LogsQL filter scope to metadata endpoints (`GetFieldNames`, `GetFieldValues`, `GetStreamFieldNames`, `GetStreamFieldValues`) — previously returned unfiltered results

### Changed
- Replace custom LogsQL filter parser with VL's native `Filter.MatchRow()` — full LogsQL parity including OR, AND, NOT, regex, ranges, case-insensitive matching, and all filter types VL supports
- Apply LogsQL filter evaluation in traces `RunQuery` (was missing) — traces now filter rows same as logs module
- Apply `filter` substring parameter in vlstorage adapter for `GetFieldNames`, `GetFieldValues`, `GetStreamFieldNames`, `GetStreamFieldValues` — was previously ignored, now matches VL behavior
- Improve loki-vl-proxy config for Grafana Loki Drilldown — switch to translated metadata mode, add structured metadata emission, expand stream fields (12 labels), add derived fields for trace-to-logs linking, enable patterns autodetect and label values indexed cache
- Split LOC badge into separate prod code and test code badges
- Add `GOWORK=off` to Makefile — prevents build failures from incompatible VL versions across modules

## [0.18.1] - 2026-05-11

### Added
- **Smart cache controller** — unified cache orchestrator wrapping L1 (memory), L2 (disk), L3 (peer), L4 (S3) with configurable TTL, hot access detection, pin tracking, and singleflight S3 deduplication (`internal/smartcache/`)
- **Cross-signal prefetch** — bidirectional hints between `lakehouse-logs` and `lakehouse-traces` deployments via HTTP (`/internal/prefetch/hint`, `/internal/cache/evict-hint`). Logs query for `service=checkout` automatically warms trace data for same time window, and vice versa (`internal/crosssignal/`)
- **LogsQL filter evaluation** — post-scan field matchers (exact, substring, regex, NOT) applied to DataBlock rows in RunQuery, ensuring cold queries respect LogsQL semantics (`internal/storage/parquets3/filter.go`)
- **max_rows enforcement** — `query.max_rows` (default 10M) caps emitted rows per query via atomic counter, preventing unbounded cold-query resource usage
- **Internal endpoint auth** — `/internal/cache/clear` and `/internal/cache/stats` require Bearer token (`peer.auth_key`) when configured, matching `/internal/manifest/update` pattern
- **Prefetch engine wiring** — cross-signal handler now creates and uses a `prefetch.Engine` to process incoming prefetch hints (was nil/inert)
- **Parallel query file workers** — configurable bounded worker pool for concurrent Parquet file processing during queries, replacing sequential file scanning (`query.file_workers`, default 8)
- **Cache sizing calculator** — adaptive cache budget estimation blending ingestion rate (early) and query pattern analysis (after 12h), with per-node fleet division (`internal/smartcache/sizing.go`)
- **Active query pinning** — files used by in-flight queries are pinned in cache with configurable grace period, preventing eviction under pressure
- **Connected data eviction** — trace IDs extracted from query results enable cross-signal cache deprioritization when traces are evicted
- **Hint batching** — cross-signal client accumulates trace ID hints and flushes on interval or batch size threshold, reducing HTTP overhead
- **Smart cache metrics** — 15 new Prometheus metrics: hit ratio, entries, bytes used/limit, evictions by reason, hot/pinned/owned entries, effective bytes, prefetch hit ratio, coverage hours
- **Cross-signal metrics** — 6 new metrics: eviction sent/received/pending/applied, prefetch sent/received
- Smart cache snapshot persistence — periodic metadata snapshots to disk for fast cache warmup on restart
- Smart cache eviction loop — background TTL enforcement with hot access detection and pin protection

### Changed
- `getFileData()` in storage now routes through SmartCacheController when available, with fallback to original L1→L2→L3→S3 chain
- `RunQuery` wraps `writeBlock` callback with filter evaluation, tombstone filtering, and max_rows enforcement before passing to caller
- `RunQuery` uses parallel file worker pool instead of sequential processing
- `queryFile` extracts trace IDs from result DataBlocks for prefetch and cross-signal hints
- Both `lakehouse-logs` and `lakehouse-traces` binaries wire up cross-signal handlers with active prefetch engine, eviction loop, and snapshot persistence
- Auto-release workflow now auto-merges metadata PRs to prevent version drift

## [0.17.0] - 2026-05-11

### Added
- Query rate limiting via `MaxConcurrent` semaphore — returns HTTP 429 when at capacity
- S3 retry with exponential backoff for all S3 operations (`ReadAt`, `Upload`, `Download`, `Delete`, `Exists`)
- Context propagation in S3 reader (replaces `context.TODO()`)
- Per-operation S3 metrics (requests, duration, errors, bytes read)
- Slow query logging with configurable threshold and query duration histograms
- VL/VT integration stubs: `GetStreamIDs`, `GetTenantIDs`, delete dispatch (`DeleteRunTask`/`DeleteStopTask`/`DeleteActiveTasks`)
- Tests: s3reader (Upload/Download/Delete/Exists), election (S3/K8s/auto), Jaeger handlers, selectapi, vlstorage adapters, S3 retry (+112 tests)
- Helm: `NOTES.txt` post-install guidance, `NetworkPolicy` template, `values.schema.json` validation
- CI: golangci-lint v2 config, Dependabot for Go/Actions/Docker, hardened security workflow
- Project logo

### Changed
- Replace custom `internalselect` handler (~960 lines) with VL's built-in `RequestHandler` for both modules
- Split `parquets3/storage.go` (1,383 lines) into `storage_query.go` and `storage_fields.go`
- Extract Jaeger handlers (~560 lines) from `handler.go` into dedicated `jaeger.go`

### Removed
- Dead code: empty `UpdatePerQueryStatsMetrics()`, unused `CircuitBreakerConfig`, `S3CircuitBreakerState` metric

### Fixed
- Replace custom internalselect encoding with VL's actual wire format — fixes vlselect panics (`growslice: len out of range`) caused by 4-byte uint32 block lengths instead of 8-byte uint64
- Add `internal/vlstorage/` thin dispatch layer bridging `storage.Storage` to VL's vlstorage function signatures (both logs and traces)
- Remove protocol-incompatible vlselect service from E2E compose
- Remove orphaned vlselect Grafana datasource pointing to removed service
- Fix traces-to-logs datasource uid reference (`victoria-lakehouse-logs` → `victoria-lakehouse-cold`)
- Delete dead `internal/protocol/` package in both logs and traces modules (replaced by VL encoding in #28)

### Architecture
- Split into two separate binaries: `lakehouse-logs` and `lakehouse-traces`
- Each binary has its own Go module with independent VL dependency versions
- Logs pins to VL v1.50.0, Traces pins to VL commit a408207c2242 (VT v0.8.2 compatible)
- Removed unified `cmd/lakehouse/` binary and `--lakehouse.mode` flag — mode is hardcoded per binary

### Logs (`lakehouse-logs`)
- Separate Dockerfile (`Dockerfile.logs`), Docker image (`ghcr.io/.../lakehouse-logs`)
- Default port `:9428`, bloom columns: `[service.name]`
- Delete API at `/delete/logsql/*`
- Mode-specific config section: `logs:` in YAML, `--lakehouse.logs.*` flags

### Traces (`lakehouse-traces`)
- Separate Go module (`lakehouse-traces/go.mod`) with VT-compatible VL dependency
- Separate Dockerfile (`Dockerfile.traces`), Docker image (`ghcr.io/.../lakehouse-traces`)
- Default port `:10428`, bloom columns: `[trace_id, service.name]`
- Delete API at `/delete/tracessql/*`
- Jaeger gRPC support: `--lakehouse.traces.jaeger-enabled`, `--lakehouse.traces.jaeger-grpc-addr`
- Mode-specific config section: `traces:` in YAML, `--lakehouse.traces.*` flags

### Shared
- Mode-specific config extension points (`logs:` / `traces:` sections) with accessor methods (`ActiveBloomColumns()`, `ActiveDeletePrefix()`, `ActiveCompatVersion()`)
- Discovery `defaultPort` parameter for mode-aware SRV resolution (9428 for logs, 10428 for traces)
- Helm chart: mode-aware image selection (`image.logs.repository` / `image.traces.repository`)
- CI: Fully parallel jobs for logs and traces (test, lint, build, docker, security, benchmarks)

## [0.14.0] - 2026-05-05

### Added
- `/lakehouse/info` endpoint now includes `build_time` field for operational visibility
- Traces delete support: mode-aware rewriter uses `schema.TraceRow` for traces mode, `schema.LogRow` for logs mode
- Delete handler registers at `/delete/tracessql/*` in traces mode, `/delete/logsql/*` in logs mode
- Docs: 5 new pages for Docusaurus site — read-path, kubernetes-deployment, docker-compose-setup, benchmarks, open-parquet-format
- Docs: Docusaurus YAML frontmatter on all 20 documentation pages
- CI: Changelog enforcement workflow — PRs with releasable changes require `[Unreleased]` entry

### Fixed
- Docs: Corrected false VL/VT compatibility claims — replaced "imports as Go module dependencies" with accurate "reimplements the VL/VT storage interface" (codebase is 100% clean-room, zero VL/VT Go imports)
- Docs: Removed non-existent `/insert/opentelemetry/v1/logs` endpoint from write-path documentation
- Docs: M7 Observability milestone updated from "Planned" to "Complete"
- Docs: Config count corrected from "65+ flags" to "110+ config options" (verified from code)

### Changed
- Docs: All cost tables corrected for 3 AZ replication (VL/VT runs 3 identical clusters, one per AZ)
- Docs: At 500GB/day 1yr 3 AZ — VL/VT $2,679/mo, Lakehouse $2,814/mo (within 5%), Loki $3,610/mo
- Docs: Compute scaled to 6× per component (3 AZ), storage × 3 for EBS, break-even and cumulative projections updated

## [0.12.0] - 2026-05-05

### Added
- Cost-aware deletion: VL-compatible `/delete/logsql/*` APIs with tombstone-based soft delete
- Three delete modes: `hide` (tombstone only), `permanent` (physical removal), `auto` (smart default)
- Tombstone query-time filtering across all query paths (zero-cost data suppression)
- Background rewriter for S3 Standard files with storage-class gating (never touches Glacier/IA)
- S3 storage class detection with lifecycle rule prediction (zero-cost age-based)
- Cost estimation endpoint (`/delete/logsql/estimate`) with per-class breakdown
- Delete verification endpoint (`/delete/logsql/verify`) for compliance auditing
- Un-delete support (remove tombstone to restore data visibility)
- Tombstone persistence to disk + S3 (survives full cluster recreation)

## [0.11.0] - 2026-05-05

### Added
- E2E: VictoriaLogs hot tier, multi-level vlselect, loki-vl-proxy in Docker Compose
- E2E: Internal Docker networking (only Grafana on port 3003)
- E2E: Loki proxy integration tests, vlselect multi-level tests, performance assertion tests
- Datagen: 5 realistic log patterns (JSON, logfmt, nginx, Java stacktrace, OTEL)
- Datagen: Dual-write to VL and S3 for hot/cold verification
- Loadtest: Benchmark mode for file size × row group × compression matrix
- Helm: Single YAML config blob in ConfigMap (no individual flag mapping)
- Helm: Common section deep-merged into components
- Helm: Separate toggleable headless services for discovery
- Helm: VPA support, extraManifests, vmauth Secret routing
- CI: Upstream sync tracks GitHub releases (not Go module versions)
- CI: Nightly benchmark workflow with artifact upload
- Docs: Performance documentation with benchmark methodology and cost projections

### Changed
- Helm: vmauth config stored as Secret instead of ConfigMap
- Helm: All components use generic HPA/VPA/PDB/ServiceMonitor/Ingress templates
- Grafana: 5 datasources (cold, hot, multi-level, Loki proxy, Jaeger)

### Removed
- Docker Compose: Host port mappings for non-Grafana services
- Helm: compaction-rbac.yaml (config in lakehouseConfig blob)

## [0.10.0] - 2026-05-04

### Added
- **Level-based Parquet compaction** — L0→L1→L2 with configurable thresholds, partition-level S3 sentinels, and structured logging (`internal/compaction/`)
- **Leader election** — K8s Lease (primary) with S3 lock + HTTP liveness detection (fallback), `auto`/`k8s`/`s3`/`none` modes (`internal/election/`)
- **Peer manifest push notifications** — fire-and-forget HTTP POST to all peers on flush/compaction, with S3 ListObjects poll as fallback (`internal/manifest/push.go`)
- **Manifest update receiver** — `POST /internal/manifest/update` handler for cross-instance manifest sync
- **Load testing binary** — `cmd/loadtest/` with latency benchmarks (6 tests against plan targets) and throughput stress tests (insert rate, query QPS, mixed workload)
- **Compaction metrics** — 11 new Prometheus metrics: runs, files, bytes, rows, duration, errors, skip reasons
- **Election metrics** — leader gauge, transition counter, health check outcomes
- **Manifest push metrics** — push total, errors, peer count, received updates
- **Helm RBAC** — K8s Role/RoleBinding for Lease-based leader election when `compaction.enabled=true`
- **Nightly CI load test** — GitHub Actions workflow running full benchmark suite on schedule

## [0.9.0] - 2026-05-04

### Added
- **Prometheus metrics instrumentation** — ~80 metrics under `lakehouse_*` prefix: HTTP RED, S3 operations, cache tiers, peer cache, manifest/discovery, Parquet engine, insert/writer, prefetch, startup/health, query
- **Grafana dashboards** — `victoria-lakehouse.json` (single-instance, 7 rows) and `victoria-lakehouse-cluster.json` (fleet, adds peer cache + per-instance)
- **Alerting rules** — 10 Prometheus alerting rules for critical operational conditions
- **Startup warmup sequence** — phased startup with readiness probe gating (init → disk recovery → S3 refresh → ready)
- **Circuit breaker** for S3 operations with configurable thresholds and recovery

## [0.8.0] - 2026-05-04

### Added
- **Write-ahead log (WAL)** — append-only crash recovery with gob-encoded log/trace entries, automatic replay on startup, atomic truncate after flush (`internal/wal/`)
- **VL-compatible insert APIs** — `/insert/jsonline`, `/insert/loki/api/v1/push`, `/insert/elasticsearch/_bulk` with full field mapping to Parquet schema (`internal/insertapi/`)
- **Adaptive file sizing** — per-partition byte estimates trigger flush when approaching `--lakehouse.insert.target-file-size` for optimal Parquet output
- **Buffer query bridge** — select pods fan out to insert pods via `/internal/buffer/query` for zero-delay reads of unflushed data (`internal/storage/parquets3/buffer_bridge.go`)
- **Manifest label pruning** — `FileInfo.Labels` field with `MatchesLabel()` for query-time file skipping without opening Parquet files
- **Manifest management** — `AllFiles()` snapshot and `RemoveFile()` for partition lifecycle
- **Label extraction** — automatic extraction of label values from log rows (10 fields) and trace rows (2 fields) during flush
- **WAL integration in BatchWriter** — entries written to WAL before buffering, WAL truncated on successful flush, replay on startup
- **Insert + select role separation** — `--lakehouse.role=all|insert|select` for independent scaling
- **Config extensions** — `TargetFileSize`, `WALMaxBytes`, `WALDir`, `WALEnabled`, `SelectConfig` with `BufferQueryEnabled`, `InsertHeadlessService`, `BufferQueryTimeout`

## [0.7.0] - 2026-05-03

### Added
- **Manifest partitions API** — `GET /manifest/partitions` with date-range filtering for per-date file/byte summaries
- **GetPartitions()** manifest method for partition inventory
- **PartitionsHandler** and **PartitionsResponse** types for HTTP layer

## [0.6.0] - 2026-05-03

### Added
- Filter AST engine with full LogsQL predicate support: exact match (`field:="value"`), substring (`field:value`), regex (`field:~"pattern"`), AND, OR, NOT, parenthesised grouping
- Playwright-based E2E UI tests validating Grafana Explore queries against live Lakehouse backend
- E2E integration tests for logs queries, Jaeger trace search, field enumeration, and stats aggregation
- Schema validation tests ensuring Parquet column mapping correctness

### Fixed
- Schema field mapping corrections for OTEL-standard column names

## [0.5.0] - 2026-05-03

### Added
- VL/VT internal select protocol (`/internal/select/*`) — 11 endpoints for cluster storage-node registration
- Binary DataBlock streaming with ZSTD compression for efficient cluster communication
- Prefetch engine with token-based row group read-ahead optimisation
- Register as `-storageNode` on vlselect/vtselect for transparent hot+cold fan-out

## [0.4.0] - 2026-05-02

### Added
- Distributed peer cache via consistent hash ring with headless DNS service discovery
- Peer HTTP protocol (`/internal/cache/fetch`, `/internal/cache/has`) with shared-secret auth
- Hot boundary auto-discovery from vlstorage/vtstorage `/internal/partition/list` endpoint
- Topology auto-detection: storage-node, direct, loki-proxy modes
- Static and headless service discovery for storage nodes and peers

## [0.3.0] - 2026-05-02

### Added
- L1 in-memory LRU cache for Parquet footers, bloom filters, and hot row groups
- L2 local disk cache with LRU eviction at configurable watermark
- Cache coalescence via `singleflight.Group` to deduplicate concurrent S3 fetches
- Label/attribute index with background scanning and disk persistence for sub-ms `field_names`/`field_values`
- Metadata persistence and recovery on restart (manifest, label index, footers)

## [0.2.0] - 2026-05-02

### Added
- Bloom filter checking for fast point lookups on `trace_id` and `service_name` columns
- Column projection — read only columns referenced by query, reducing I/O by 60-80%
- `GetStreamFieldNames`, `GetStreamFieldValues`, `GetStreams`, `GetStreamIDs` storage methods
- `GetFieldNames`, `GetFieldValues` from Parquet metadata with label index fallback
- No-op `Delete*` and `GetTenantIDs` methods for read-only cold storage

## [0.1.0] - 2026-05-02

### Added
- Initial project structure with Go module, CI/CD, Dockerfile, Helm chart skeleton
- Config namespace (`--lakehouse.*`) with YAML + flag parsing and production-ready defaults
- Mode selection: `--lakehouse.mode=logs` (port 9428) or `--lakehouse.mode=traces` (port 10428)
- S3 `io.ReaderAt` adapter for parquet-go with connection pooling and range reads
- ParquetS3Storage query engine: Hive partition pruning, row group statistics skipping, DataBlock emission
- SchemaRegistry mapping OTEL Parquet columns to VL/VT internal names (logs + traces profiles)
- Partition manifest with S3 ListObjects refresh and sub-ms "nothing here" fast path
- HTTP endpoints: `/health`, `/ready`, `/manifest/range`, `/manifest/partitions`, `/lakehouse/info`
- Public LogsQL API: all `/select/logsql/*` query endpoints (query, stats, hits, field/stream discovery)
- Jaeger API: `/select/jaeger/api/*` endpoints (traces, services, operations, dependencies)
- Phased startup warmup: init → disk recovery → S3 refresh → ready
- Distroless container image with multi-stage build
- GitHub Actions CI/CD: test, lint (golangci, gosec, gitleaks), build, security scanning, auto-release
- PR labeler, dependabot, CODEOWNERS configuration
- Documentation: architecture, configuration, cost estimates, getting started, observability, operations, performance, scaling, security
