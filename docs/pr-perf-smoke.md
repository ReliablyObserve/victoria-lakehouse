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
Posting the comments, wiring the perf smoke and the API matrix into GitHub Actions, and the visual proof are separate changes. The
quality report is computed in CI as an artifact (see [Quality report](#quality-report)).

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

The `pr-quality` workflow (`.github/workflows/pr-quality.yaml`, name `PR Quality`) computes this report for every pull request and uploads
it as artifacts. It is informational: it is not a required check, it holds a read-only token (`contents: read`, `actions: read`) and cannot
comment. It is not gated by the CI scope (the scope can only skip a job, and a skipped job leaves the previous head's comment on the pull
request): a PR that changes no Go package pays for the checkout and the static counts only, and its comment says "skipped" when nothing the
report counts moved.

What it measures, base (the merge base) vs PR, for both modules:

| Section | Source | Gate |
|---|---|---|
| Go tests, fuzz targets | static count of `func Test*` / `func Fuzz*` in the two trees | fewer tests than the base |
| Coverage | `go test -cover -json` of the Go packages the PR changes (at most 12 per module, the busiest first; the rest is listed), on the merge base and on the PR; the figure is the one `go test -cover` prints, which `scripts/ci/check_coverage.sh` reads in the CI test jobs, with the same 90 % floor and exclusions (a test pins them to `ci.yaml`) | a package under `internal/` below the floor, or down by more than 0.2 points |
| Registry | rows per `expect`, pending rows, features per status, known-difference rows and how many cite an issue | more `differ`/`unsupported`/`absent` rows without more rows overall |
| Parity | pass/fail per signal from the parity run of the same head when it has finished, otherwise the allowlist of the tree only (known failures with their issue links, the pass floor) | a failure outside the allowlist |
| New lint findings | `golangci-lint run --new-from-rev=<merge base>` on the changed packages (the CI lint jobs run whole modules) | any new finding |
| Flaky-test watch | jobs that failed in one attempt and passed in a later attempt of the same workflow run of this head, with the failed test names read from the failed attempt's log | none (a finding, not noise); fills in only after a re-run |
| Test time | package time of the coverage run against the 600 s `-timeout` the CI test jobs hold (without `-race`, so a lower bound) | none |

Only the changed packages are measured: the job does not re-run the CI test jobs, so it costs minutes, not the 35 of `test-logs`.
Measured on `ubuntu-latest`: 1.4 minutes of job time for a PR that changes two small packages, 9.6 minutes for one that changes
`internal/storage/parquets3` in both modules (the coverage runs of the merge base and the PR are 8 of those). A
figure that was not collected is shown as "not collected", never as zero. The report puts the verdict first, lists only the rows that
changed, regressed or fall below the floor, and collapses the rest into a count.

### The artifacts

| Artifact | Files | For |
|---|---|---|
| `lh-proof-comment` | `manifest.json`, `pr-quality.md` | the commenter workflow, which runs the base branch's code, treats the artifact as untrusted data, sanitises the body again and posts the sticky comment |
| `pr-quality-data` | `quality.json` (schema `scripts/bench/ab/schemas/pr-quality.schema.json`, `schema_version` 1) | the data behind the body, for a later trend |

`lh-proof-comment` is the commenter's contract: `manifest.json` has `schema_version`, `pr_number`, `head_sha`, one comment `{kind: pr-quality,
verdict}` (`pass`, `fail`, `skipped` or `incomplete`), no image, and `allowed_issue_refs` (the issue numbers the body writes as `#N`). The body
starts with the marker `<!-- lh-pr-quality -->` and uses only text, tables, code spans, attribute-free `<details>`, `<summary>` and `<sub>`,
links to this repository's runs and issues, and `#N` for the allowed numbers. It is uploaded on every run, also when the job failed (verdict
`incomplete`), so a stale verdict never stays. Every string that comes from the PR (test names, file paths, lint text) goes through `md_safe` /
`md_code` in `quality_bundle.py`, which defuse it the way the commenter's sanitiser does (so the sanitiser leaves it unchanged), and the
body is capped at 60000 characters with a note. `quality.py verify <dir>` checks a directory against these rules, and the tests hold the
body to them.

### Run it by hand

```bash
python3 scripts/bench/ab/quality.py packages --base-ref origin/main --module root          # what would be measured
python3 scripts/bench/ab/quality.py collect --tree . --out pr.json --gotest root=go-test.json --module-path root=github.com/ReliablyObserve/victoria-lakehouse
python3 scripts/bench/ab/quality.py bundle --base base.json --pr pr.json --out-dir lh-proof-comment --data-dir pr-quality-data --pr-number N --head-sha SHA
python3 -m pytest scripts/bench/ab/tests/test_quality*.py -q
```

## Runner fit: does the stack fit a standard GitHub-hosted runner

`.github/workflows/pr-proof.yaml` (informational: it posts nothing, gates nothing and is not a required check) brings up this stack on
`ubuntu-latest`, runs `pr_smoke.py` once and records what it cost. The measurement tool is `scripts/proof/runner_fit.py` (standard library;
unit tests in `scripts/proof/tests/test_runner_fit.py`, run first in the job). It does not change the harness: the seed lever patches two module
constants of `stack.py` in memory.

It runs on a manual dispatch (the file has to be on the default branch first: `gh workflow run` answers HTTP 404 before) and on pull requests
that change `pr-proof.yaml`, `deployment/docker/docker-compose-proof.yml`, `scripts/bench/ab` or `runner_fit.py`; ordinary pull requests do not
run it. A pull request run executes three variants side by side, each on its own runner, plus a disk probe; a manual run executes one variant
from the inputs. Every variant tears down its compose project and images by exact tag (`stack.py down`, never a prune), removes its base tree
without `--force`, uploads its artifact on failure too, and has a 60 minute job limit and a 40 minute limit on the `pr-smoke` step, so the
teardown always has time. The job has `contents: read` and nothing else; every image is pulled anonymously.

| Variant | Base build | Layer cache | UI tier | Runs / API rounds | S3 latency |
|---|---|---|---|---|---|
| `full` (the worst case) | merge-base with main, built separately | none | loki-vl-proxy x3, Jaeger UI x3, Grafana | 6 / 3 | 0 and 50 ms |
| `lean` | the PR image, tagged as base too (A/A) | none | no | 3 / 1 | 0 ms |
| `lean-cache` | as `lean` | BuildKit local cache, keyed by tree SHA | no | 3 / 1 | 0 ms |

The seed is also a lever (`seed_chunks`, `seed_scale`, dispatch inputs only; see the findings: neither works). All variants run the shape sets
`control-logs` and `control-traces` (56 cells, both signals, four layers).

What is recorded (`runner-fit.json` in the artifact `runner-fit-<variant>`, and the table in the job summary):

- wall time per phase (pull, setup-buildx, one build per image, probe images, `pr-smoke`, the UI tier, teardown) and, from the harness's own log
  timestamps, per step inside `pr-smoke` (stack up, seed, forced compaction, buffer layer, seed equality, perf core, further latency profiles,
  API matrix); `restart_approx` and `confirm_remeasure_approx` are derived from log line gaps and are approximate;
- every 10 s: `free -m`, `df` of `/`, `/mnt` and the Docker root, `docker system df`, `docker stats`, the load average and CPU utilisation from `/proc/stat`;
- before teardown: cgroup `memory.peak`, `OOMKilled` and the restart count of every container, and the kernel OOM lines;
- after teardown: the containers, volumes, networks and images of the compose project that remain (none is the pass);
- the fit verdict, on memory and disk only: it fails when `MemAvailable` falls below 1 GiB, a container is `OOMKilled`, the kernel logged an OOM
  kill, the free space of the Docker root falls below 1 GiB, or a phase log says "no space left on device". CPU saturation is reported, never a verdict.

### Measured on `ubuntu-latest` (pull request 815; five runs of the workflow, 18 measured artifacts; "derived" marks what is not a direct measurement)

The machine in every run: 4 vCPU (AMD EPYC 9V45), 15 983 MB RAM, 3 GB swap, image `ubuntu26` 20261023.163.3, Docker 29.4.2 (overlayfs). The root
disk is 147.8 GB with 88 GB free at the start, not the 14 GB the planning assumed; the stack added 5.6 GB (`lean`) to 12.2 GB (`full`) to `/`.
**`/tmp` is the exception: it is a 7.9 GB tmpfs, that is RAM**, and a probe that wrote 1 GiB files stopped after 6 GiB with `Disk quota exceeded`
(twice; `$RUNNER_TEMP`, the workspace and `$HOME` are on the 148 GB ext4 disk and took every GiB tried, 40 in the first run, 12 in the second). A local
BuildKit cache written to `/tmp` next to its restored copy hit that limit, and the cache files counted as memory: `lean-cache` showed 5.3-5.7 GB of RAM
used while its cache lived in `/tmp` and 3.5 GB after the job moved its scratch files to `$RUNNER_TEMP`. Any CI step that writes more than a few GB
to `/tmp` on this image is spending RAM and fails at about 6 GiB.

| Variant | Job wall, per run | Median | Build (all images) | `pr-smoke` | RAM used, max | Containers' RAM, sum max | Fit |
|---|---|--:|--:|--:|--:|--:|---|
| `full` | 22.6, 23.2, 22.9, 23.1 min | 23.0 | 4.4-4.6 min | 15.0-16.5 min | 3.7-4.2 GB | 2.6-3.0 GB | fits |
| `lean` | 11.9, 12.6, 11.4, 13.8, 13.5 min | 12.6 | 2.3-3.0 min | 8.4-10.1 min | 3.5-3.9 GB | 2.4-2.7 GB | fits |
| `lean-cache` | 12.7, 14.8, 15.5, 13.2, 15.1 min | 14.8 | 3.0-4.4 min | 8.5-10.0 min | 3.5-5.7 GB (3.5 with the cache under `$RUNNER_TEMP`) | 2.4-2.8 GB | fits |

One-off variants of earlier runs of the pull request: `lean-main-cache` (a separate base build with the layer cache) 15.6 and 17.2 min, and once
failed with the `/tmp` quota above; `lean-scale05` (half the rows per chunk) 12.2 min; `lean-chunks4` and a 3-chunk seed failed in 5-7 min (below).

Every variant: `MemAvailable` never below 8.6 GB (the minimum over all runs of any variant; 10.3-12.5 GB for the three that remain), no container
`OOMKilled`, none restarted, no kernel OOM line, no container, volume, network or image left after teardown (0 of 18). The cgroup `memory.peak` of
a container, largest over the runs: RustFS 878 MB, Grafana 618 MB (UI tier only), Lakehouse logs 493 MB, VictoriaLogs 390 MB, Lakehouse traces
326 MB, VictoriaTraces 211 MB; the other twelve are under 50 MB. CPU utilisation: mean 27-44 %, at or above 95 % in 4-16 of the 10 s samples (the
builds and the compaction); load average up to 7.7 (up to 11.6 with the UI tier) on 4 vCPU.

Where the time goes in `lean` (seconds, range over the runs): build logs 54-68, traces 61-86, datagen 19-23, stack up 18, seed 102-117, forced
compaction 2-4, buffer layer 16-17, seed equality 75-78, perf core 192-203 (56 cells, 3.4-3.6 s per cell including the restart layer and the confirming
re-measures), API matrix 95-167 (one round; the noisiest step), components 3-6, teardown 1.5. `full` adds, against `lean`: a second pair of image builds
(+113 s), the UI tier (pull 2, build 50, up 10, settle 30 s), the 50 ms profile (+72-104 s), and two more API rounds plus six instead of three runs
(API matrix 379-464 s; perf core 209-219 s).

Noise. Base and PR are the same code in every run, so a cell that moves is a false positive: 24 of 952 cells (2.5 %) moved in the first pass, none after
the confirming re-measure. The wall time of `lean` spread 11.4-13.8 min on identical code, and its API matrix 95-167 s: read a difference under
about 15 % as noise. Latency stays report-only until it is calibrated on a 4-vCPU shared runner, where small shifts cannot be read.

### Findings

- **The stack fits a standard runner.** 16 GB RAM: yes (peak used 3.1-7.4 GB, minimum `MemAvailable` 8.6 GB). 4 vCPU: yes (CPU is busy only
  during builds and compaction). Disk: yes (5.6 GB added by `lean`, 12.2 GB by `full`, on a 148 GB disk; an actual 14 GB disk would fit `lean` and
  be tight for `full`; this image's 7.9 GB RAM-backed `/tmp` is the constraint to design around). No variant ran out of memory or space.
- **The wall time is the cost, not the fit.** `full` takes about 23 min. `lean` takes 11.4-13.8 min (median 12.6), so ordinary levers bring it to
  about 12-13 min but not reliably under 12. Going from `full` to `lean` saves about 10.4 min (measured differences): API rounds 3 to 1 (-280 to -300 s),
  no 50 ms profile (-72 to -104 s), no separate base build (-113 s), no UI tier (-93 s), runs 6 to 3 (-8 to -19 s).
- **Recommended core-tier budget on a standard runner: 15 min wall, job limit 20 min, for the `lean` shape.** The 8 min target of the core tier is not met by
  any lever that exists today. Derived, not measured: running the 32 smoke cells of the control sets instead of all 56 would save about 85 s (24
  cells x 3.5 s), giving about 11 min. What would still stand between that and 8 min is 190 s of seed plus seed-equality wait and 140-170 s of image
  builds: prebuilt images, a shorter chunk schedule or a shorter settle, all harness changes.
- **The image layer cache does not pay.** Written cold or restored, `lean-cache` is slower than `lean` in four of five paired runs (median 14.8 vs 12.6 min):
  +15-30 s of build per image for the export and import, 1.9 GB of the repository's 10 GB cache limit (3.3 GB with a separate base build) and about
  2 GB more resident memory. Only the dependency layers (8-11 per image) were `CACHED`; the compile layer sits after `COPY . .`, so any changed file in
  the tree misses it. An exact hit on the compile layer of the unchanged base tree was not obtained either (it was rebuilt, 46-52 s, in all four base
  builds that had a restored cache); the cause was not isolated (issue 825).
- **Neither seed lever works.** Halving the rows (`lean-scale05`) saved nothing (seed 103 vs 102-117 s, perf core 198 vs 192-203 s): the seed time is
  the chunk schedule (a 7 s pause per chunk) and per-request overhead, not the volume. Fewer chunks (4, and 3 in an earlier run) breaks the compacted
  layer: the forced compaction answers `400 partition not found or empty` for every hour and the run stops with exit 3. Hypothesis, supported by the code
  (`ForceCompactPartition` takes only files the segment guard has released) and by the full seed compacting 4 input files out of 6: the files of a short
  seed are still held back. A smaller seed is not a lever until the seed schedule changes in the harness (issue 824: the harness neither guards against a short seed nor says why it fails).
- Registry images (RustFS, mc, toxiproxy, nginx, Jaeger, the Grafana base image) pulled anonymously from Docker Hub and GHCR without error in every run,
  so the job needs no `packages: read`. Anonymous Docker Hub pulls from shared runner addresses can still be rate limited.

## Limits

- The seed is small (tens of thousands of rows per tenant); timings are dominated by the S3 round trips and the number of requests,
  which is what the injected latency and the S3 columns are for. CPU-bound regressions need more rows than this stack seeds.
- The stack shares a host. A timing is only comparable with timings of the same run; `meta.json` records the load average.
- Logs over Loki push and OTLP, and traces over anything but OTLP, are not seeded: `cmd/datagen` writes jsonline to VictoriaLogs and
  Lakehouse and OTLP traces. The ingest parity matrix (`tests/e2e/ingest_matrix_test.go`) covers the forms.
- Flush latency and manifest refresh time are not measured; the component table says so.
- Hot VictoriaTraces writes its trace-index rows 20 to 40 s after ingest and Lakehouse drops them (issue 458); queries that match
  them (`*`) differ on every flushed layer. Row-by-row scoring of `limit` queries without a sort is not meaningful (above).
