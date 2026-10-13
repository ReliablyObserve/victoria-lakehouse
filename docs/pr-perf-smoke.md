# PR perf smoke, API matrix and quality report

A pull request that changes runtime code can be measured against `main` and against hot VictoriaLogs / VictoriaTraces on one
isolated stack, from a laptop or a CI runner, with the same code. The harness lives in `scripts/bench/ab`. It produces the
data and the Markdown for three sticky pull request comments:

| comment | marker | what it answers |
|---|---|---|
| perf smoke | `<!-- lh-perf-smoke -->` | is the PR faster or slower than `main`, per signal, layer, shape and range; what does it cost in S3 requests and bytes; does any answer differ from hot VL/VT |
| API matrix | `<!-- lh-api-matrix -->` | does every read-only endpoint the conformance registry declares answer like `main` and like hot VL/VT: status, schema, rows, values, series, traces |
| PR quality | `<!-- lh-pr-quality -->` | tests, coverage, registry counts, parity, fuzz targets and lint, base vs PR |

Both of the first two end with **Known differences and gaps**, the list of every difference the run found and what records it.
Wiring the comments and checks into GitHub Actions, and the visual proof, are separate changes; this one is the harness and its
tests.

## Running it

```bash
python3 -m pip install -r scripts/bench/ab/requirements.txt          # PyYAML, pytest

# the whole smoke for the current branch against main: builds both Lakehouse images, starts the stack, measures, tears it down
python3 scripts/bench/ab/pr_smoke.py --base origin/main --out /tmp/ab-smoke

# another ref, a chosen set, A/A (the same build twice: the noise floor)
python3 scripts/bench/ab/pr_smoke.py --base <sha> --head origin/some-branch --sets control-logs,control-traces --out /tmp/ab
python3 scripts/bench/ab/pr_smoke.py --same-build --out /tmp/ab-aa

# the pieces
python3 scripts/bench/ab/selection.py --base origin/main              # which sets the changed files need, and why
python3 scripts/bench/ab/selection.py --check                         # shapes.json against the registry (also in make conformance-check)
python3 scripts/bench/ab/stack.py ports                               # the ports of the stack and whether they are free
python3 scripts/bench/ab/stack.py latency probe 50                    # set 50 ms of S3 latency and measure it
make ab-test                                                          # unit tests of the harness
```

The run needs Docker and about 6 GiB of memory for the stack. It writes, in `--out`: `selection.json`, `state.json`,
`raw-0ms.json`, `summary-0ms.json` (and one summary per extra latency), `api-matrix.json`, `components.json`, `known-perf.json`, `known-api.json`,
`perf-smoke.md`, `api-matrix.md`, `perf-verdict.json`, `api-verdict.json` and `meta.json`. The exit status is the larger of the two
verdicts: 0 passed or skipped, 1 a regression or a new difference, 2 the run is invalid, 3 it did not complete.

`pr_smoke.py` creates detached worktrees of the base and the head under `--out/trees` and removes them again with a plain
`git worktree remove` (never `--force`); a tree that is not clean is left and reported. `--base-dir`/`--head-dir` use trees you
already have, `--skip-build` reuses existing images, `--keep-stack` leaves the stack up for a second run. A run holds a per-project lock (a `flock` on `lh-ab-<project>.lock` in the temp
directory) from the preflight to the end of the teardown: a second run that cannot take it, or that finds the stack running, exits 4 without building, retagging or removing
anything; the operating system releases the lock when its holder dies, so a stale file does not wedge the next run. `--reuse-stack` uses a running stack and leaves it up, but
it still builds and retags the `lh-ab-*` images unless `--skip-build` is given (as requested). The compose project is
`lh-ab`; `stack.py down` removes its containers, volumes and the images it built, by exact tag, and nothing else.

## The stack

It is `scripts/proof`'s stack (`deployment/docker/docker-compose-proof.yml`, the same images, tenants and seeding as the API data
proof), with one overlay (`scripts/bench/ab/docker-compose.ab.yml`) and a few services only: RustFS (the pinned digest of the e2e
compose file), hot VictoriaLogs and VictoriaTraces at the versions the Makefile pins, and Lakehouse logs and traces built from the
base commit and from the PR. Grafana, the Jaeger UI and the proxies are not started.

- **Ports.** The block is derived from the project name by `scripts/proof` (the default `lh-ab` gets 45600 + offsets, 46028..46062)
  and checked free before anything starts. `stack.py ports` prints it. It stays clear of the ranges of other stacks on a development host
  (29xxx, 39xxx, 47xxx), the e2e stacks (20xxx-23xxx) and loki-vl-proxy's (3002, 13xxx, 18xxx, 19428, 33xxx, 34xxx).
- **S3 latency.** toxiproxy sits between every Lakehouse and RustFS. Latency is set through its HTTP API, the call must answer 2xx
  and the toxic is read back (`latency.py`). A probe then times the same request through the proxy and directly to RustFS,
  interleaved, and fails the run when the measured added delay is off the injected one by more than max(10 ms, 25 %). A second
  check reads Lakehouse's own `lakehouse_s3_request_duration_seconds` around one cold query at 0 ms and at the injected value and
  of each of the four Lakehouse processes (base and PR, logs and traces) and requires the mean to rise by at least 0.8 times the
  injected delay and at most 2.5 times it (on two runs of the same stack the mean rose by 85 ms and by
  50 ms for 50 ms injected; the spread is not explained), which proves that Lakehouse's S3 traffic crosses the proxy (the original
  bug was a wrong path). This replaces the 2026-09 injector, which exec'd into a container of another compose project, ignored the failure and measured
  nothing: its "20/50/100 ms" columns had no latency in them (the probe measured +0.2 ms to +0.6 ms while it printed "injected 50ms").
  `scripts/inject-s3-latency.sh` and `scripts/bench/with-s3-latency.sh` now fail loudly when the container is missing (exit 2, the
  wrapped command does not run); use `latency.py` for any other stack.
- **Seed.** `cmd/datagen` with a fixed `--seed` and `--now`, six chunks per tenant over six hours, sent once to hot + base and once
  to the PR, so all three hold the same rows; tenants `0:0` and `1001:0` (also reached as the alias `acme-corp`: `X-Scope-OrgID` for
  Lakehouse, `AccountID: 1001` for hot, which has no aliases). Before anything is compared, scripts/proof's seed-equality gate
  requires the same row count and the same hash of the row identities on hot, base and PR for every tenant form and layer.
- **Layers.** `cold`: flushed by the 5 s policy and not compacted (the newest two hours). `compacted`: the older four hours after a
  forced `POST /lakehouse/compaction/recompact` of every hour. `buffer`: rows ingested after the flush, held in the insert buffer by
  a one-hour flush policy. `after-restart`: the same rows after `docker restart` of the Lakehouse containers. While a layer is
  measured the manifest refresh and the compaction scheduler are parked (`LH_MANIFEST_REFRESH=1h`, `LH_COMPACTION_INTERVAL=24h`), so
  a per-query S3 delta is the query's.

## What is measured

`shapes.json` holds named sets of shapes. A shape is one request template with its answer validator (`kind`), the registry rows
and features it measures (`covers`), and optionally `smoke: true`.

| set | what it covers |
|---|---|
| `control-logs`, `control-traces` | always run: counts, filters, group-by, field values, scan, trace-by-ID; every layer; the `smoke` shapes run on every runtime change |
| `query-projection`, `stats`, `enumeration`, `hits` | column projection, stats pipes and endpoints, field/stream enumeration, the hits histogram |
| `buffer-restart`, `tenancy` | the insert buffer and its recovery; integer and alias tenants |
| `traces-jaeger`, `traces-tempo`, `traceql-metrics`, `trace-by-id` | the Jaeger and Tempo APIs, TraceQL metrics, the point lookup through every API |
| `compaction` | compacted vs freshly flushed layers asked the same questions |

Ranges are `15m`, `1h` and `full` (the whole layer window). Windows are absolute and end at the layer's end.

**Order.** For each cell (shape x layer x range x tenant form) every target is asked in turn, in a rotating order, on the same
window: hot (`ref`), Lakehouse base, Lakehouse PR. The first request of each target is the cold one; for Lakehouse the caches are
cleared first with `POST /internal/cache/clear` and the peer key, the answer asserted, and a cold run whose cache could not be
cleared invalidates the run. Hot VL/VT cannot clear its caches, so its cold column is a first request. The rest are warm.

**Validation.** Every answer goes through `validate.py`, a port of `scripts/bench/run.sh`'s `extract_result` and `_validate_iter`
(a test runs run.sh's own function on the same fixtures and requires the same strings): status 2xx, parseable, not empty (except
documented misses), identical to the cell's first valid result (else flapping), and for a truncated scan every row a member of the
reference window. A scalar count, a group-by (rows, total, hash of the pairs), field values (hash of value and hits), a scan (hash of
the row keys), spans, or a canonical JSON hash for Jaeger, Tempo and TraceQL metrics. **An invalid answer is never a latency.**

Jaeger, Tempo search and OTLP trace answers are compared with `scripts.proof`'s decoders (spans keyed by trace and span id, processes
resolved), so span order and `processID` labels are not differences. Tempo's tag list and tag values are samples on hot (it answers differently on identical calls; measured). Their shapes carry `vs_ref: false` (hot is not compared;
base is compared with PR, and the normal flapping rule applies to the Lakehouse side) and `ref_varies: true` (hot is also exempt from the flapping rule). The tag list
is compared by the scopes that carry tags, the tag values exactly, both decoded with `scripts.proof`; an empty answer, a body that is not a tag list or an
HTTP error is invalid on every target, so a broken answer that still returns 200 fails. In the API matrix the same paths are scored by status, shape and
non-emptiness (`facets` alone is `arbitrary`). Search shapes keep `limit 20` and are compared with hot by the
exact set of traces: on the seeded data hot and Lakehouse return the same 20 of 700 to 958 matching traces on every call (measured).
Each comment reports the differences its own run found.

**S3 cost.** From Lakehouse's `/metrics`: GETs (`lakehouse_s3_requests_total` for GET and GetObject), all non-write requests, and
`lakehouse_s3_bytes_read_total`. `/metrics` shows the state of about a second ago (measured: a counter turns up 1.0 s after the
request), so the counters are read per phase, only after that second has passed: once before the cold run, once after it, once after
the warm runs. The cold figure is the cold query's; the warm figure is the mean per warm request. This costs about 2.2 s per cell.

## Verdicts

| | rule |
|---|---|
| noise | a cell is `slower` or `faster` when the PR's warm p50 differs from the base's by more than max(15 %, 3 ms) |
| re-measure | a cell the first pass called `slower`, `faster` or `unstable` is measured again with 7 runs, every window moved 5 minutes earlier inside its layer (the data is static, so this is the nearest thing to a fresh window; it does not time the caches the first pass filled); only a move or a problem that holds is reported (marked with a footnote). A new difference from hot seen in either pass stays. See *Flaky policy* below |
| `broken` | the base answered validly and the PR does not |
| `unstable` | the PR gave an error, empty or flapping answer the base did not (any of its runs): measured again; fails if it reproduces; if it does not, see *Flaky policy* |
| result vs hot | the validated answer of the PR (and of the base) is compared with hot's on the same window: same, differs, invalid |
| new difference | the PR's answer differs from hot's **and** from the base's, or the base matched hot and the PR does not: fails the check |
| S3 | more GETs or bytes than the base beyond max(10 %, 2 requests / 64 KiB) on a control shape fails the check even when latency is flat; the comment names the metric and the phase (`more bytes (cold)`). Other shapes are listed only; `pr_smoke.py --gate-s3-all` (off by default, an owner decision) makes them fail too |
| other profiles | the cells of the other S3 latency profiles (50 ms) fail the check like the 0 ms ones, and their differences are in the known-differences list |
| invalid run | a cell with no valid answer on base or PR, hot VL/VT giving no valid answer in more than max(2, 10 %) of the cells of a signal, logs and traces counted separately (fewer are asked twice, then listed and compared base against PR; a shape that is empty on every target is `vacuous` and measures nothing), a cold run whose cache was not cleared, or a failed latency probe: exit 2 |

The comment shows the verdict first, then only the cells that moved, differ or cost more S3; everything within noise is one
collapsed table. Cold timings, a component summary (ingest rows/s and spans/s, forced compaction, Lakehouse CPU-seconds and peak
RSS, base vs PR and hot), the touched-area selection and the run metadata are collapsed sections. A second S3 latency profile
(`--latencies 0,50`) runs the control smoke shapes on the flushed layers and is a collapsed section of the same comment.

**Flaky policy.** A check that fails and then passes on retry is a finding, never retried away. A PR that fails 1 request in 6 passes a 7-run re-measure clean
28 % of the time ((5/6)^7; 48 % at 1 in 10), so a clean re-measure does not clear an `unstable` cell: if the base was clean in both passes the intermittent failure is the PR's
and the cell **fails** (the comment says so); if the base had failing requests in either pass, the cell is flaky on both (host noise): it is listed as a FLAKY finding in
*Known differences and gaps*, needs an issue like an untracked difference, and does not fail by itself. A failure that reproduces on the re-measure always fails.

**Budget.** The smoke is meant to finish within 30 minutes on `ubuntu-latest`: two image builds (cached), the seed (about
2 minutes), the 0 ms profile always, and the 50 ms profile on the control smoke shapes only. Selection keeps it small: a
docs-only or test-only PR runs nothing and the comments say "skipped".

**Noise floor.** `pr_smoke.py --same-build` runs one build as both targets; a cell it calls slower or faster is a false positive of
the noise rule. Run it before trusting the thresholds on a new host, and record the host load next to the result (`meta.json` does).

## Selection

`selection.py` maps changed files to shape sets without a hand-kept table: a shape's `covers` name registry rows and features, a
feature names the tests that prove it (`internal/x/y_test.go#TestZ`) and the rows it owns, and the package directory of those tests
is the code the shape is about. A changed non-test file under that directory selects the shape. `areas` in `shapes.json` is a coarse
fallback for sets whose items name no test package. `go.mod` or `go.sum` of any module (`lakehouse-traces` too), the Makefile and the Dockerfiles select the whole control sets; any
other file of `cmd/`, `internal/` and `lakehouse-traces/` that is not Go source (embedded configuration, UI assets) selects the smoke subset;
the harness itself selects the smoke subset; docs, tests, CI, charts, registry text and `deps/` select nothing.

`selection.py --check` (part of `make conformance-check`) fails on an unknown registry id, a shape that covers nothing, a smoke
shape outside the control sets, an unknown layer, range, kind or pick, a duplicate name, a shape no change can select, and an `areas` prefix that does not exist in the repository.

## The API matrix

`api_rows.py` turns every registry row that declares a request into one smoke request, with the row's method, path, parameters and
tenant headers; `api_matrix.py` sends it three times to hot, base and PR (rotating order) and scores the first round with
`scripts/proof`'s metrics (status, error text, rows, values, series, traces, JSON schema); the other rounds classify (a verdict that
flips is `nondeterministic`) and give the latency p50. Rows without a `hot` target and the `/lakehouse/api/v1/*` endpoints are
Lakehouse-only: the PR is scored against the base. The check fails on `regressed`, `harness-error` and on a
status change from base to PR. A row whose verdict flips between samples is listed (`FLAKY` in the known differences) and fails only when one
sample had the PR worse than the base.

**Scope of the matrix.** The window is the last hour of the cold layer, tenant `0:0`, one trace id picked from the reference; the
`tenant_scope` rows (they need the isolation fixture) and the other layers are not covered by it. The perf shapes cover the compacted,
buffer and after-restart layers and the alias and `1001:0` tenants for the queries they run; widening the matrix itself is future work.

Rows that cannot be exercised read-only are **listed with the reason and counted, never dropped**: writes, deletes, drains and the
cache clear; flags; rows that need a fixture the stack does not seed (edge-case logs, a second tenant for isolation, the
field-metadata generators, the ingest matrix); streaming endpoints (`tail`); binary answers. A row whose query ends in `limit N` keeps an arbitrary subset
of the matching rows, so two correct systems answer different rows: it is scored by membership (every row an answer returns must
be a row of the unlimited reference answer, and an answer is empty exactly when the reference is). Rows that prove nothing about the
PR are listed apart: `arbitrary` (`facets` and `sample N`, which are samples by definition), `vacuous` (empty on every target) and `unusable` (hot itself rejects the request the registry row
declares: the row needs fixing).

## Known differences and gaps

Every difference a run finds is classified, from the registry:

- **pre-existing on base**: the PR answers like the base. It must map to a registry row that records it (`expect: differ`,
  `unsupported` or `absent`) with its GitHub issue and/or its entry in `docs/parity-and-gaps.md`. The comment lists the row, the issue
  and the gap.
- **NEW, introduced by the PR**: fails the check, whether or not a row records an older difference.
- **UNTRACKED**: no registry row records it (the covered rows say `expect: pass`, or there is none). It is printed as `UNTRACKED` and
  fails the check: every difference needs an issue, a registry row and a line in the comment.
- **fixed by the PR**: listed with the rows that can flip to `expect: pass` and get a lock.

`--require-issue` additionally fails a tracked difference whose row cites no GitHub issue (the full URL of an issue of a ReliablyObserve repository, or `ReliablyObserve/victoria-lakehouse#123`; a bare `#123`, which may be an
upstream issue, a pull request link or an issue of another project does not count). A row tracks a difference of its own signal only (`vl` rows logs,
`vt` rows traces) and a perf difference is keyed by shape, layer and signal; registry rows carry no data layer, so a row that covers the shape
tracks it on every layer. The comment also states how many of the
registry's known-difference rows cite an issue and how many do not.

## Quality report

`quality.py collect` snapshots a tree: the number of Go tests and fuzz targets per module (a static count), the registry (rows per
`expect`, features per status, known differences and how many cite an issue), and, from files the CI jobs already produce, coverage
profiles, the parity suite's `go test -json` stream (parsed with `scripts/ci/parity_ratchet.py`) and a golangci-lint JSON report.
`quality.py report` joins a base and a PR snapshot and gates: a module's coverage below 90 % or down by more than 0.2 points, fewer
tests than the base, more rows that record a difference without new rows, a parity failure outside the allowlist, more lint findings.
A figure that was not collected is shown as "not collected", never as zero.

## Limits

- The seed is small (tens of thousands of rows per tenant); timings are dominated by the S3 round trips and the number of requests,
  which is what the injected latency and the S3 columns are for. CPU-bound regressions need more rows than this stack seeds.
- The stack shares a host. A timing is only comparable with timings of the same run; `meta.json` records the load average.
- Logs over Loki push and OTLP, and traces over anything but OTLP, are not seeded: `cmd/datagen` writes jsonline to VictoriaLogs and
  Lakehouse and OTLP traces. The ingest parity matrix (`tests/e2e/ingest_matrix_test.go`) covers the forms.
- Flush latency and manifest refresh time are not measured; the component table says so.
- Hot VictoriaTraces writes its trace-index rows 20 to 40 s after ingest and Lakehouse drops them (issue 458); queries that match
  them (`*`) differ on every flushed layer. Row-by-row scoring of `limit` queries without a sort is not meaningful (above).
