# Changelog

All notable changes to Victoria Lakehouse will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.14.0] - 2026-05-05

### Added
- `/lakehouse/info` endpoint now includes `build_time` field for operational visibility
- Traces delete support: mode-aware rewriter uses `schema.TraceRow` for traces mode, `schema.LogRow` for logs mode
- Delete handler registers at `/delete/traceql/*` in traces mode, `/delete/logsql/*` in logs mode
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
