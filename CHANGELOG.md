# Changelog

All notable changes to Victoria Lakehouse will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
