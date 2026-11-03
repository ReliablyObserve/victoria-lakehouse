# Contributing to Victoria Lakehouse

Thank you for your interest in contributing to Victoria Lakehouse! This document provides guidelines and information for contributors.

## Getting Started

### Prerequisites

- Go 1.23+
- Docker (for local E2E testing)
- The `mc` client (any S3 client works) for local S3 testing

### Development Setup

```bash
git clone git@github.com:ReliablyObserve/victoria-lakehouse.git
cd victoria-lakehouse
go mod download
go test ./...
```

### Running Locally

```bash
# Start the S3 backend (RustFS) and create the buckets
docker compose -f deployment/docker/docker-compose-e2e.yml up -d s3 s3-init

# Run lakehouse-logs
go run ./cmd/lakehouse-logs --lakehouse.s3.bucket=obs-archive --lakehouse.s3.endpoint=http://localhost:9000

# Run lakehouse-traces
cd lakehouse-traces && go run . --lakehouse.s3.bucket=obs-archive --lakehouse.s3.endpoint=http://localhost:9000
```

## Development Workflow

1. **Fork** the repository and create a feature branch from `main`
2. **Write tests first** — we follow TDD practices
3. **Run the full test suite** before submitting: `go test -race ./...`, and `make pre-push` (a preview of the fast CI tier, not a replacement) before every push
4. **Run linters**: `golangci-lint run ./...`
5. **Open a pull request** against `main`

### Before you push: `make pre-push`

`make pre-push` is a **preview** of the fast, always-run tier of CI, so a red head is caught before it costs a CI
cycle. It calls the same tools and scripts the CI jobs call and re-implements none of them, but it is not CI: it
picks the Go test packages from the diff, runs without `-race` unless asked, does not shard, and cannot run the
jobs that need a Docker stack. Every row says what it covered, the headline says `PASS (N not verified)` when rows
were skipped or ran only in part, and the summary lists every always-run CI job it did not run. A green preview is
no promise that CI is green.

```bash
make pre-push                       # what the diff against origin/main touches
make pre-push PREPUSH_SCOPE=all     # every step, whatever the diff (the plain lint is never filtered to new findings; still not CI)
make pre-push RACE=1                # adds -race to go test
```

What each step runs, and the CI job and workflow line it mirrors (line numbers are as of the commit that last edited
this table; `test_pre_push.py` checks the job and step names against the workflows, not the numbers):

| Step | Runs when the diff touches | What it calls | CI job: workflow line | Differs from CI |
|---|---|---|---|---|
| `fetch-base`, `scope` | always | `git fetch origin main`; `pr_classify.py --ci-scope` against the merge base | `scope`: ci.yaml:36 | informational only: no Go step is skipped on `skip_go`; a changed classifier runs the copy of this branch, CI the base's |
| `contains-main`, `commits-signed`, `no-ai-attribution` | always | merge-base equals `origin/main`; every commit shows `G`; no co-author trailer or AI attribution | none: owner rules | local-only checks |
| `registry-gate` | always | `scripts/ci/gate_local.sh` | `registry-gate.yaml` (judges with the base branch's gate code) | reads HEAD, not the working tree; when the branch changes the gate the row is marked a preview (CI judges with the base's copy) |
| `changelog-gate` | always | `check_changelog_pr.py`, `changelog_fragments.py check` | `changelog-check`: changelog-pr.yaml:28 | `PR_AUTHOR_LOGIN`, `PR_HEAD_REPO` and `PR_BASE_REPO` are unset locally, so the release-metadata approver and Dependabot paths are not judged as in CI (the row says so) |
| `ci-script-tests` | `.github/`, `scripts/ci/`, Makefile, Dockerfiles, compose files, charts, `go.mod`/`go.sum`, requirements | the static tests plus the tests named after a changed `scripts/ci` file; `PREPUSH_SCOPE=all` runs the full `unittest discover` (about 400 s) | `changelog-tests`: changelog-pr.yaml:50 | CI discovers every test on every pull request; marked partial unless `PREPUSH_SCOPE=all` |
| `registry-gate-selftests` | `PREPUSH_SCOPE=all`; reported NOT VERIFIED when the diff touches `changelog.d/`, the registry or `docs/features.md` | `test_check_registry_touch.sh`, `test_gate_workflow_emulation.sh`, `test_pr_classify_toolchain.py` (about 8 min) | `conformance-inventory`: conformance.yaml:54-58 | not run by default: they clone the real repository |
| `lint-logs-selftests` | `patches/`; `PREPUSH_SCOPE=all` adds the four other self-tests | `check_patches_equal_test.sh` and `check_patches_equal.sh` against a pristine clone of the logs-pin VictoriaLogs tag (cached under `~/.cache/lakehouse-pre-push`) | `lint-logs`: ci.yaml:584-616 | needs the network once for the clone; NOT VERIFIED offline without a cache |
| `actionlint`, `zizmor` | `.github/` | the workflow-lint commands | `workflow-lint`: workflow-lint.yaml:38, :51 | zizmor's online audits run only with `GH_TOKEN`; the row is marked partial without it |
| `harness-tests` | `scripts/bench/ab/`, `scripts/proof/`, `deployment/docker/proof/`, the proof compose file, `tests/parity/testdata/tiecut_golden.json` | `pyflakes scripts/proof`; `pytest scripts/bench/ab/tests` (pr-proof.yaml:457); `pytest scripts/proof/tests` with the five `--cov` options and `--cov-fail-under=90`; `scripts.proof.metrics ... --check`; the fixture generator in a copy of HEAD | `proof-metrics`: proof-metrics.yaml:42-57 | `docker compose config` is not run (no Docker); a private `TMPDIR` and `AB_PROJECT` keep it off the lock and ports of a running proof or A/B stack |
| `chart-tests` | `charts/` | `helm lint --strict`, `test_images.sh`, `test_templates.sh`, `test_release_image_tags.py` | `helm`: ci.yaml:857-870 | none known |
| `deps`, `conformance-check` | always | `make deps-*`, `make conformance-check` (about 12-14 s) | `conformance-inventory`: conformance.yaml:50 | none known |
| `golangci-lint (logs)`, `golangci-lint (traces)` | a Go file, an embedded asset under a package, or `go.mod`, `go.sum`, `patches/`, `.golangci.yml`; a logs-module change lints both modules | whole packages as CI: `./internal/... ./cmd/...`, traces `./...`; the tagged `tests/` runs with findings new since the merge base | `lint-logs`: ci.yaml:633, :640, :651; `lint-traces`: ci.yaml:676, :691 | the linter is the local binary (the row notes a version mismatch with `GOLANGCI_LINT_VERSION`) |
| `go vet (logs)`, `go vet (traces)` | as above | `gofmt -s -l` and `go vet` of the whole module (`./internal/... ./cmd/...`; traces `./...`) | `test-logs`: ci.yaml:283, :335; `test-traces`: ci.yaml:356, :392 | none known |
| `go build (logs)`, `go build (traces)` | as above | `go build` of the whole module | `build-logs`: ci.yaml:717; `build-traces`: ci.yaml:749 | the host platform only (CI builds linux and darwin, amd64 and arm64); the row is marked partial |
| `go test (logs)`, `go test (traces)` | a changed Go package (any Go change also runs the three source-scanning packages of `GO_SOURCE_READERS`); a non-Go file under a package; a file a Go test reads (`GO_TEST_READERS` in `pre_push.py`: `docs/` -> `internal/config`, `internal/delete`; `alerts/`, `dashboards/` -> `internal/metrics`; `charts/` -> the compaction PDB test; `deployment/`, `README.md` -> `internal/config`; `benchmarks/` -> `cmd/bench`) | `go test -count=1 -p 2 -timeout=10m` on those packages, `-short` only for `parquets3` as CI; the module-wide inputs and unmapped paths are NOT VERIFIED | `test-logs`: ci.yaml:311; `test-traces`: ci.yaml:366 | marked partial in every mode: no `-race` unless `RACE=1`, no sharding, no 90% coverage gate; in diff mode also only the picked packages, and the packages that import a changed one are named as not tested (the row is NOT VERIFIED when nothing else ran) |

`PREPUSH_SCOPE=all` runs every step: the full discovery, the self-tests, `go test` of every package of both modules
(`-short` for `parquets3` only) and no `--new-from-rev` on the plain lint. It is still a preview: no coverage gate, no sharding,
no `-race` unless asked, host-only build. At the end the tool prints the 11 parts
of the PR evidence pack as a checklist (the PR body is not parsed).

Notes:

- The scope is the committed diff `merge-base(origin/main, HEAD)..HEAD`, what a push publishes. The gates read `HEAD`;
  vet, lint and test read the working tree, so uncommitted tracked changes and untracked files are reported in the `scope` row.
- Required Go jobs run on every pull request in CI, so none of these steps is skipped on the classifier's `skip_go`. A
  Go step is reported SKIPPED only when no changed path can change its result, and the summary still lists that CI job.
- A step skipped because a tool or input is missing is listed under `NOT VERIFIED locally`: CI still runs it. For the
  Python checks, make a venv once: `python3 -m venv ~/.cache/lakehouse-pre-push-venv` and
  `~/.cache/lakehouse-pre-push-venv/bin/pip install --require-hashes -r scripts/ci/requirements.txt -r scripts/proof/requirements.txt` (what the PR proof job installs),
  then `PRE_PUSH_PYTHON=~/.cache/lakehouse-pre-push-venv/bin/python make pre-push`. `GOLANGCI_LINT=<path>` selects a
  linter binary (a linter built with an older Go than `go.mod` targets cannot load the config: the row is then NOT VERIFIED
  with a hint, install the pinned `GOLANGCI_LINT_VERSION` built with the repository's Go version). zizmor is looked up on `PATH` and next to `PRE_PUSH_PYTHON`:
  install it into that venv with `pip install --require-hashes --only-binary=:all: -r .github/workflow-lint-requirements.txt`
  (the pinned file CI uses); set `GH_TOKEN` for its online audits. `PRE_PUSH_OFFLINE=1` skips the fetch (the verdict may then be against a stale base). `PRE_PUSH_CACHE=<dir>` moves the
  cache of the pristine logs-pin clone (default `~/.cache/lakehouse-pre-push`).
- `make deps-*` only checks that the dependency trees exist: after a change to `patches/` or a pin, refresh them as the
  Makefile does (a stale tree fails the Go steps).
- The full output of each step is kept in `<git dir>/pre-push-logs/` (overwritten by every run; the table shows the first
  error line of a failure and the failing step's last lines are printed under it). Run directly, `python3 scripts/ci/pre_push.py`
  exits 1 when a step FAILs and 2 on a usage error; through `make` a failure prints `Error 1` and `make` exits 2.
- The variable is `PREPUSH_SCOPE`, not `SCOPE`, so an exported `SCOPE` in your shell cannot switch it.
- Time (measured on one laptop, single runs, so indicative only): a diff that changes only `docs/` took 29 s, a
  comment-only edit of one Go file (every Go step passing) 45 s, and `PREPUSH_SCOPE=all` 19.0 min, which overlapped a
  2-minute unit-test run of another job (the `scripts/ci` discovery 3.7 min, the registry gate self-tests 7.1 min, the proof
  harness 1.1 min, `go test` of the logs module 3.4 min and of the traces module 2.6 min without `-race`, the rest under 1 min).
  Use `PREPUSH_SCOPE=all` before a large change, not before every push.
- The tests of a changed `scripts/ci` file are chosen by name (`foo.py` selects `test_foo*.py`), so a test of a script that
  only imports the changed one is not selected; `PREPUSH_SCOPE=all` runs them all.

What it **cannot catch** (CI will):

- Jobs that need a Docker stack or a service: `e2e`, `parity`, PR proof and its A/B, visual and quality comments, `parquet-readers`,
  `s3-compat`, `docker-logs`/`docker-traces`, `helm-config-drift`, `bench-selftest`, the benchmarks, the perf cells.
- Flaky tests and retries, Linux-only failures (this runs on your OS and architecture), `-race` runs it did not select
  (it never adds `-race` by itself), the sharding and time-budget gates, and the 90% coverage gate.
- Tests of packages that import a touched package (they are named in the row as not tested, not run), the
  release-metadata and Dependabot paths of the changelog gate (`PR_AUTHOR_LOGIN` and friends are unset locally), and tests that scan the Go sources of packages other than
  `internal/upstreamreuse`, `internal/metrics` and `internal/schema` (which run on every Go change, `GO_SOURCE_READERS`):
  they run only when their own package or a file listed in `GO_TEST_READERS` changed.
- Code scanning (CodeQL, Semgrep, govulncheck, Trivy), dependency review, `github-code-quality` review threads, the
  independent review, and the PR body itself.

### Test time budget

`internal/storage/parquets3` (in both modules) is the slowest package, and CI
holds it to a `-timeout`:

- The unit jobs (`test-logs`, `test-traces`) run it with `-short`, under
  `-timeout=8m`. A test that builds hundreds of files or a large deployment
  (memory-budget, production-shape, field-metadata exactness matrix) should
  start with `if testing.Short() { t.Skip(...) }`.
- Those `Short()`-gated tests run in the `test-parquets3-heavy` job on every PR
  (`scripts/ci/short_gated_tests.sh` derives the list from the source, so
  nothing is enumerated by hand). Run them locally with
  `go test ./internal/storage/parquets3/ -run "$(scripts/ci/short_gated_tests.sh internal/storage/parquets3)"`.
- Both jobs split the package into shards, each its own `go test` with its own
  `-timeout` and headroom gate, inside the one required job (so the required
  check names stay `test-logs` / `test-traces`). `scripts/ci/shard_tests.py`
  places every top-level test in exactly one shard, and a guard fails the job
  when a test ran in no shard or in two. Placement is a hash of the test name,
  except where a weights file lists the slow tests: for
  `lakehouse-traces/internal/storage/parquets3` (6 shards of the `-short` run,
  3 of the heavy run) `scripts/ci/shard_weights/*.json` gives the measured cost
  of each slow test, so the shards carry equal time rather than equal counts (3
  count-balanced shards ran 321-480 s of a 480 s budget). A test missing from
  the file still runs, at the default cost; a stale file costs balance, not
  coverage. Target: every shard below about 60% of its budget. When a shard
  warns, regenerate the file from fresh `go test -race -json` output
  (`python3 scripts/ci/shard_tests.py weights OUT <json>...`) before touching a
  timeout.
- `scripts/ci/gotest_report.py` warns at 70% and fails at 90% of each package's
  timeout and lists the slowest tests in the job summary. When it warns, make
  the slowest tests faster instead of raising the timeout: share fixtures,
  size datasets to the assertion, and never `time.Sleep` for real time. A test
  that points S3 at a server answering 500 should call `singleAttemptS3(t)` so
  the SDK's retry backoff does not turn it into a multi-second wait.

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

We use `golangci-lint` v2 (`.golangci.yml`) with these linters enabled:
- `errcheck` — all error returns must be checked
- `govet` — Go vet checks
- `staticcheck` — advanced static analysis (includes the former `gosimple` checks)
- `ineffassign`, `wastedassign`, `unused`, `unconvert` — dead and redundant code
- `bodyclose`, `durationcheck`, `errname` — HTTP body, duration and error-naming correctness
- `gocyclo`, `misspell` — complexity and spelling
- `gofmt -s` formatting

Security linting (`gosec`) runs as its own required job in the Security workflow.

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
3. **Generated docs** — run `make conformance-gen`; `docs/features.md`, `UPSTREAM_COVERAGE.md`,
   `docs/proof-coverage.md` (with its `.json` twin) and the README highlights block are generated from the
   registry and must be committed current.
4. **CHANGELOG** — a fragment, `changelog.d/<PR-or-slug>.added.md`, whose bullet has a bold lead-in that maps to
   the feature (the catalog gate matches `### Added` lead-ins, and an `added` fragment is one; an extension of an
   existing feature may use `changed`, and still updates the catalog when it adds a route, handler, flag or
   registry row). `CHANGELOG.md` itself is not edited by hand: see [changelog.d/README.md](changelog.d/README.md).

CI enforces this through the required `conformance-inventory` check: it fails a PR that adds a CHANGELOG
`### Added` bullet (or an `added` fragment), a route or handler, a `lakehouse.*` flag, or an `lh.*` registry row without changing
`tests/conformance/registry/features/`, and any PR whose generated files (`docs/features.md`,
`UPSTREAM_COVERAGE.md`, `docs/proof-coverage.md` and `.json`, the README highlights block, the upstream
inventory) are stale.

### CI Checks

All PRs must pass the required status checks on `main`:
- Unit tests with race detector and the 90% per-package coverage floor (`test-logs`, `test-traces`); every package under `internal/` is held to it by its full path, including `internal/storage/parquets3`, and the exclusions are named in the workflow (`scripts/ci/check_coverage.sh`)
- `golangci-lint` v2 (`lint-logs`, `lint-traces`)
- Security: `gosec`, `govulncheck`, `gitleaks`, `trivy`
- Build and image verification (`build-logs`/`build-traces` for linux amd64 and arm64, `docker-logs`,
  `docker-traces`, `helm`)
- CHANGELOG format (`changelog-check`, `changelog-tests`)
- Conformance (`conformance-inventory`): registry and upstream-inventory drift, feature catalog completeness,
  registry and catalog touch rules, generated files current

Advisory checks run on PRs but are not required: CodeQL, fuzz/stress/memleak, `parquet-readback`, `parity-unit`,
`benchmarks-*`, and the hot/cold parity suite (`parity`, on PRs touching `internal/`, `cmd/`, `tests/parity/` or
`lakehouse-traces/`): a failure not listed in `tests/parity/known_failures.txt` fails that job. It becomes a
required status check once the allowlist is empty.

## Documentation Policy

`docs/` documents final decisions, full explanations of shipped behavior, operational
guidance, and reliability proofs (benchmarks + the code to rerun them, costs, audits,
measurements). Pre-development material — research, plans, specs, design explorations —
is not part of this repository.

When contributing, ship documentation updates together with the code change they
describe: new flags land in `docs/configuration.md`, behavior changes in the matching
architecture/operations doc, and measured results in the benchmark docs.

## Upstream Releases

Victoria Lakehouse embeds VictoriaLogs and VictoriaTraces at pinned versions (the
`Makefile` is the source of truth). Moving to a new upstream release follows
[docs/upstream-sync.md](docs/upstream-sync.md): pins in all five places, fresh deps trees,
regenerated patches, tidy and build, vmui sync, registry rows, the query-grammar sweep, a
verification round and a benchmark. A daily workflow probes the newest releases and opens
a pull request with what the bump would cost; the same page explains how to grant the
token it needs.

## Release Notes

The GitHub release notes are generated from `CHANGELOG.md` in the VictoriaLogs style
(`SECURITY`, `FEATURE`, `BUGFIX` bullets). Behaviour an operator has to act on when upgrading
goes inside the bullet of the change, as an indented second paragraph that opens with a bold `**Update note.**`
(the release writes `CHANGELOG.md` from the fragments, so a hand-written `### Update notes` subsection is refused). The mapping and
the preview command are in [docs/release-notes.md](docs/release-notes.md).

## Architecture

Victoria Lakehouse follows a modular architecture:

```
internal/
  buffer/        — Insert buffer and the cross-pod buffer query handler
  cache/         — Multi-tier cache (L1 memory, L2 disk)
  compaction/    — Partition assignment, scheduling, compaction and orphan cleanup
  config/        — Configuration and flag parsing
  delete/        — Delete API, tombstones and file rewriting
  discovery/     — DNS and hot-boundary discovery
  lifecycle/     — Kubernetes lifecycle: drain, readiness, ring, staleness
  manifest/      — Partition manifest (file registry)
  membuffer/     — In-memory insert buffer backed by VictoriaLogs' logstorage
  pmeta/         — Unified per-partition metadata (catalog, bloom and HLL facets)
  schema/        — Parquet schema and field mapping
  selectapi/     — Select API wrappers around the upstream VictoriaLogs/VictoriaTraces handlers
  stats/         — Tenant stats, storage metrics and the stats API
  storage/       — Storage backends (parquets3)
  tenant/        — Tenant mapping, aliases and header middleware
  ui/            — Lakehouse UI and the embedded vmui
  vlstorage/     — Adapter between VictoriaLogs' storage interface and the Lakehouse backend
  (also: azdetect, bloomindex, crosssignal, metrics, peercache, prefetch, resourcebounds,
   retention, s3reader, smartcache, startup, telemetry, traceindex, upstreamreuse)
```

### Key Design Principles

- **Zero VL/VT modifications** — we import VL/VT as dependencies, never fork
- **Storage dispatch replacement only** — only the storage layer is ours
- **S3 is the storage node** — no separate storage nodes, insert+select roles only
- **Open Parquet format** — data readable by DuckDB, Spark, Trino, ClickHouse

### Main Rules (non-negotiable)

1. **Full VictoriaLogs/VictoriaTraces compatibility.** Every native VL/VT API, LogsQL/TraceQL feature, flag and
   Grafana datasource behavior must work on Lakehouse exactly as it does upstream. Lakehouse only *extends* upstream
   behavior; it never changes it. Reuse upstream code in this order: import the upstream symbol → add a minimal
   export patch under `patches/` → write Lakehouse code only when upstream has nothing, and only for
   Lakehouse-specific features (S3/Parquet storage, pmeta, tenancy, compaction, cold-tier stats and UI).
2. **Verified, not assumed.** Every feature and every upstream version bump must pass the conformance checks in
   `tests/conformance/`: the registry declares every endpoint and feature (native VL/VT first, Lakehouse
   additions second), the feature catalog links every Lakehouse feature to its rows, tests and docs, and the
   inventory extracted from the vendored upstream sources fails the build on drift. Verification spans six
   dimensions — native functionality (the hot/cold parity suite), Lakehouse UI and API additions, Grafana
   datasources and Drilldown apps, external readback of the Parquet Lakehouse writes (the `parquet-readback` job
   with pyarrow and DuckDB; ClickHouse in the benchmark stack), ingest-to-S3 delivery and durability (e2e), and
   performance (benchmarks). Registry rows are declared expectations until the row runner executes them; the
   parity suite is the executable oracle today. A change that turns a passing native parity test into a failure
   does not merge. Tests extend coverage with every PR. **Registry gate:** a PR that changes product
   code (any non-test file under `internal/`, `cmd/`, `lakehouse-traces/`, `patches/`, `charts/`, and
   the shipped `Dockerfile`, `Dockerfile.logs`, `Dockerfile.traces`, root `go.mod`/`go.sum`) must change
   registry rows or features (a real content change), every new `Test`/`Fuzz` function must be linked
   from a row or feature by name (`file#Test`), references to removed tests must go, a parity fix must ship a lock,
   and locks are never weakened (any change to an `expect: pass` row, or a build constraint or `t.Skip` in a
   lock's test file, fails the gate). There is no exemption label: the gate decides from the content of the
   diff (a Go toolchain bump is recognised as such), and the required review covers what a content check
   cannot judge. Run `make registry-gate` before every push; see
   [`tests/conformance/README.md`](tests/conformance/README.md#registry-gate-on-every-pr).
3. **Performance is measured, never claimed.** Benchmarks validate every timed response (status, parse, exact
   equality against the hot baseline and ClickHouse over the same Parquet, membership for truncated scans) and
   report validity per system; invalid responses never count as latency. Lakehouse is expected to beat
   ClickHouse-over-S3 on every scenario and to minimize its ratio to hot VL/VT per query class. A PR touching
   the read or write path re-runs `scripts/bench/run.sh` with the recorded baseline's profile
   (`docs/benchmarks/full-scope-s3.md`) and posts the table in the PR; a p90/p50 regression against the recorded
   baseline, or any cell ClickHouse wins, blocks the PR at review. CI's `benchmarks-*` jobs are advisory
   microbenchmarks, not this gate.
4. **Storage: fastest Lakehouse, fully open Parquet.** The storage layer targets the best performance achievable
   on S3, comparable with VL/VT, while every object stays standard Parquet that ClickHouse, DuckDB, Trino and
   Spark can read and prune efficiently in the long term: time-sorted, well-sized files with column statistics,
   page indexes and bloom filters; hive-style prefixes; Lakehouse metadata only in footer key/values, pmeta or
   sidecars that never break a `*.parquet` glob. A storage change is judged against both the Lakehouse benchmark
   and the external-reader checks (the `parquet-readback` job and the three-way benchmark).

## Reporting Issues

- Use [GitHub Issues](https://github.com/ReliablyObserve/victoria-lakehouse/issues)
- Include reproduction steps, expected vs actual behavior
- For security vulnerabilities, see [SECURITY.md](SECURITY.md)

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
