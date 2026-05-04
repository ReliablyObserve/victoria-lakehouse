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
docker compose -f deploy/compose/docker-compose.yml up -d minio

# Run lakehouse in logs mode
go run ./cmd/lakehouse --lakehouse.mode=logs --lakehouse.s3.bucket=obs-archive --lakehouse.s3.endpoint=http://localhost:9000
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

### CI Checks

All PRs must pass:
- Unit tests with race detector (`go test -race ./...`)
- `golangci-lint` (errcheck, gosec, gosimple, govet, staticcheck)
- CodeQL security analysis
- Build verification

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

## Reporting Issues

- Use [GitHub Issues](https://github.com/ReliablyObserve/victoria-lakehouse/issues)
- Include reproduction steps, expected vs actual behavior
- For security vulnerabilities, see [SECURITY.md](SECURITY.md)

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
