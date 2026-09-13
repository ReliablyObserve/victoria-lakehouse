# Contributing to Victoria Lakehouse

Thank you for your interest in contributing to Victoria Lakehouse! This document provides guidelines and information for contributors.

## Getting Started

### Prerequisites

- Go 1.23+
- Docker (for local E2E testing)
- MinIO CLI (`mc`) for local S3 testing

### Development Setup

```bash
git clone git@github.com:ReliablyObserve/victoria-lakehouse.git
cd victoria-lakehouse
go mod download
go test ./...
```

### Running Locally

```bash
# Start MinIO for local S3
docker compose -f deployment/docker/docker-compose-e2e.yml up -d minio

# Run lakehouse-logs
go run ./cmd/lakehouse-logs --lakehouse.s3.bucket=obs-archive --lakehouse.s3.endpoint=http://localhost:9000

# Run lakehouse-traces
cd lakehouse-traces && go run . --lakehouse.s3.bucket=obs-archive --lakehouse.s3.endpoint=http://localhost:9000
```

## Development Workflow

1. **Fork** the repository and create a feature branch from `main`
2. **Write tests first** — we follow TDD practices
3. **Run the full test suite** before submitting: `go test -race ./...`
4. **Run linters**: `golangci-lint run ./...`
5. **Open a pull request** against `main`

## Code Standards

### Go Style

- Follow standard Go conventions and [Effective Go](https://go.dev/doc/effective_go)
- Use `gofmt` / `goimports` for formatting
- All exported types and functions must have doc comments
- Error messages should be lowercase and not end with punctuation

### Testing

- Minimum 90% test coverage for new code
- Use table-driven tests where appropriate
- Include both unit tests and integration tests
- Test files live alongside the code they test (`*_test.go`)

### File Permissions (Security)

- Files: `0o600` (owner read/write only)
- Directories: `0o750` (owner full, group read/execute)
- Never use `0o644` or `0o755` in new code

### Linting

We use `golangci-lint` with the following enabled linters:
- `errcheck` — all error returns must be checked
- `gosec` — security-focused linting
- `gosimple` — simplification suggestions
- `govet` — Go vet checks
- `staticcheck` — advanced static analysis

Run locally before pushing:
```bash
golangci-lint run ./...
```

## Pull Requests

### PR Guidelines

- Keep PRs focused — one feature or fix per PR
- Write a clear description of what changed and why
- Reference any related issues
- Ensure all CI checks pass before requesting review
- Squash commits if the history is noisy

### Adding or extending a feature

A PR that adds a Lakehouse feature or extends an existing one is not mergeable until it carries:

1. **Feature catalog entry** — add or update the feature in `tests/conformance/registry/features/<area>.yaml`:
   title, status, `since`, the registry rows that verify it, the regression tests (existing files/functions),
   docs links and a one-line highlight. Every `lh-addition`/`lh-shim` registry row must belong to a feature.
2. **Verification** — registry rows for every new endpoint, flag or behavior (native VL/VT rows first when the
   feature touches upstream surfaces), regression tests that exercise the feature, and benchmarks when it
   affects the read or write path (validated responses, compared against the recorded baseline).
3. **Generated docs** — run `make conformance-gen`; `docs/features.md`, `UPSTREAM_COVERAGE.md` and the README
   highlights block are generated from the registry and must be committed current.
4. **CHANGELOG** — an `[Unreleased]` `### Added`/`### Changed` bullet whose bold lead-in maps to the feature.

CI enforces this: the required `conformance-inventory` check fails when a PR adds a CHANGELOG `### Added`
bullet, a route, flag or handler, or an `lh.*` registry row without touching the feature catalog, or when
any generated file is stale.

### CI Checks

All PRs must pass:
- Unit tests with race detector (`go test -race ./...`)
- `golangci-lint` (errcheck, gosec, gosimple, govet, staticcheck)
- CodeQL security analysis
- Build verification
- Conformance: registry/inventory drift, feature catalog completeness, generated docs current
- Hot/cold parity suite (required once the known-failure allowlist is empty)

## Documentation Policy

`docs/` documents final decisions, full explanations of shipped behavior, operational
guidance, and reliability proofs (benchmarks + the code to rerun them, costs, audits,
measurements). Pre-development material — research, plans, specs, design explorations —
is not part of this repository.

When contributing, ship documentation updates together with the code change they
describe: new flags land in `docs/configuration.md`, behavior changes in the matching
architecture/operations doc, and measured results in the benchmark docs.

## Architecture

Victoria Lakehouse follows a modular architecture:

```
internal/
  cache/         — Multi-tier cache (L1 memory, L2 disk)
  config/        — Configuration and flag parsing
  discovery/     — DNS and hot-boundary discovery
  insertapi/     — Insert API handlers and buffer management
  manifest/      — Partition manifest (file registry)
  schema/        — Parquet schema and field mapping
  storage/       — Storage backends (parquets3)
  wal/           — Write-ahead log for durability
```

### Key Design Principles

- **Zero VL/VT modifications** — we import VL/VT as dependencies, never fork
- **Storage dispatch replacement only** — only the storage layer is ours
- **S3 is the storage node** — no separate storage nodes, insert+select roles only
- **Open Parquet format** — data readable by DuckDB, Spark, Trino, ClickHouse

### Main Rules (non-negotiable)

1. **Full VictoriaLogs/VictoriaTraces compatibility.** Every native VL/VT API, LogsQL/TraceQL feature, flag and
   Grafana datasource behavior works on Lakehouse exactly as it does upstream. Lakehouse only *extends* upstream
   behavior; it never changes it. Reuse upstream code in this order: import the upstream symbol → add a minimal
   export patch under `patches/` → write Lakehouse code only when upstream has nothing, and only for
   Lakehouse-specific features (S3/Parquet storage, pmeta, tenancy, compaction, cold-tier stats and UI).
2. **Verified, not assumed.** Every feature and every upstream version bump must pass the conformance machine
   (`tests/conformance/`): the registry declares every endpoint and feature (native VL/VT first, Lakehouse
   additions second), the inventory extracted from the vendored upstream sources fails the build on drift, and
   the executed rows compare Lakehouse against hot VL/VT as the oracle across six dimensions — native
   functionality, Lakehouse UI and API additions, Grafana datasources and Drilldown apps, ClickHouse readback of
   the Parquet Lakehouse writes for logs and traces, ingest-to-S3 delivery and durability, and performance.
   A change that flips any native row from pass to fail does not merge. Tests extend coverage with every PR.
3. **Performance is measured, never claimed.** Benchmarks validate every timed response (status, parse, exact
   equality against the hot baseline and ClickHouse over the same Parquet, membership for truncated scans) and
   report validity per system; invalid responses never count as latency. Lakehouse must beat ClickHouse-over-S3
   on every scenario and minimize its ratio to hot VL/VT per query class; a regression against the recorded
   baseline blocks the PR.
4. **Storage: fastest Lakehouse, fully open Parquet.** The storage layer targets the best performance achievable
   on S3, comparable with VL/VT, while every object stays standard Parquet that ClickHouse, DuckDB, Trino and
   Spark can read and prune efficiently in the long term: time-sorted, well-sized files with column statistics,
   page indexes and bloom filters; hive-style prefixes; Lakehouse metadata only in footer key/values, pmeta or
   sidecars that never break a `*.parquet` glob. A storage change is judged against both the Lakehouse benchmark
   and the external-reader readback and performance checks.

## Reporting Issues

- Use [GitHub Issues](https://github.com/ReliablyObserve/victoria-lakehouse/issues)
- Include reproduction steps, expected vs actual behavior
- For security vulnerabilities, see [SECURITY.md](SECURITY.md)

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
