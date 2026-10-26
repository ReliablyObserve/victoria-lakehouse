# Conformance registry

The registry (`registry/rows/**/*.yaml`) is the single source of truth for what the
verification machine checks: one row per endpoint or feature, **native VictoriaLogs /
VictoriaTraces surfaces first, Lakehouse additions second**. The inventory
(`inventory.generated.yaml`) is extracted from the vendored upstream sources; CI fails
when upstream has a route, pipe, filter, stats function or TraceQL function without a row,
and warns for flags (add a row under `rows/flags/matters.yaml` when a flag changes behavior
or Lakehouse compatibility).

Lakehouse mounts the upstream VictoriaLogs / VictoriaTraces HTTP handlers directly and
never re-implements an upstream API from scratch — the registry lists native surfaces
first because that's what's actually being exercised, then documents what Lakehouse adds
on top.

- Regenerate: `make conformance-gen` · Check: `make conformance-check`
- Row schema: `registry/schema.go` (`Row.Validate` documents every rule)
- Statuses: `expect: pass | differ (with differ_note) | absent | unsupported`; `pending: true`
  marks rows declared but not yet executed by the runner.
- Adding an endpoint or changing a handler? Add or update its row in the same PR
  (`scripts/ci/check_registry_touch.sh` enforces it; see "Registry gate on every PR" below).
- `UPSTREAM_COVERAGE.md` is generated from the inventory + registry — never edit it by hand.
- All rows are declared expectations until the runner executes them (future work).
- Request params/paths use a small placeholder vocabulary instead of literal values:
  `{{seed.start}}` / `{{seed.cold_end}}` (RFC3339), `{{seed.start_ms}}` /
  `{{seed.cold_end_ms}}` (millisecond-epoch integers, e.g. VT's dependencies `endTs`),
  `{{seed.start_us}}` / `{{seed.cold_end_us}}` (microsecond-epoch integers, e.g. VT's
  Jaeger traces-search `start`/`end`), `{{seed.trace_id}}` / `{{seed.stream_id}}`,
  `{{seed.task_id}}` (a delete-task id), `{{tenant.account}}` / `{{tenant.project}}`,
  `{{tenant.global_read}}` (the cold stack's configured global-read header value, sent
  only by the `*.tenant_scope.global.*` rows),
  `{{tombstone_id}}` (the id returned by a prior delete request in the same run, not
  part of the seed), and `{{proto.internal_select}}` / `{{proto.internal_delete}}` (not
  part of the seed either: the protocol version constant the runner reads from the
  vendored VL — `internalselect`'s `ProtocolVersion` consts — that
  `/internal/select/*`/`/internal/delete/*` requests must send as `version=...` or be
  rejected before ever reaching the query logic). Full list and rationale: the header
  comment in `registry/rows/vl/select.yaml`.
- **The two `{{proto.*}}` placeholders resolve PER MODULE**, from the vendored
  VictoriaLogs tree of the binary the row targets: `deps/VictoriaLogs`
  (`VL_VERSION_LOGS`) for `surface: vl` rows, `lakehouse-traces/deps/VictoriaLogs`
  (`VL_COMMIT_TRACES`) for `surface: vt` rows — `lakehouse-traces` mounts
  VictoriaLogs' `internalselect` package for these endpoints, so it follows its own
  VictoriaLogs pin, not VictoriaTraces and not the logs pin. The pins may differ (they
  did, VL v1.52.0 against v1.51.0, until VictoriaTraces v0.12.0 moved the traces
  pin to VL v1.52.0); today `select` is `v5` and `delete` is `v2` on both. The
  resolved pairs are extracted from both
  trees and recorded under `protocol:` in `inventory.generated.yaml`, so a future bump
  that moves either version surfaces as drift instead of as a runtime
  "unexpected protocol version" rejection between peers.
- Flag rows (`kind: flag`) declare `compare: { type: status }` with no `request`: they
  document that a flag exists and matters, not a specific HTTP call. They stay
  declarative only until the runner exercises actual flag-variant stacks.

## Flag-warning triage

An upstream flag without a row is a **soft warning**, not a failure — it says
"upstream registers this, Lakehouse inherits whatever it does, and nothing
Lakehouse-specific depends on it". Only flags in the two classes below get a row
in `rows/flags/matters.yaml`:

1. **Changed between the pinned versions** — the flag arrived, was renamed or was
   deprecated in the range the bump crosses.
2. **Touches Lakehouse compatibility** — admission control and queueing,
   retention and backfill windows, ingest limits, cold-tier lookbehind windows,
   or a route Lakehouse serves differently from the hot tier.

Everything else stays a warning on purpose: ingest-format knobs (`syslog.*`,
`splunk.*`, `journald.*`, `datadog.*`, `loki.*`), storage-node and TLS plumbing
(`storageNode.*`), local-disk and auth-key knobs (`storageDataPath`,
`*AuthKey`, `inmemoryDataFlushInterval`, `retention.maxDisk*`), and the internal
peer transport caps (`internalinsert.*`, `internalselect.*`). They are either
pre-storage admission control Lakehouse mounts verbatim, or they act on the hot
local storage Lakehouse replaces wholesale.

The VL v1.52.0 / VT v0.11.0 bump added six flags. Five already had rows carrying
`since:`; the bump only moved them from pending-bump to live, and their notes were
rewritten from "not in the pinned version, re-check after the bump" to what is
actually true now:

| flag | row | class |
| --- | --- | --- |
| `vl:vmalert.proxyURL` | `vl.flag.vmalert_proxy_url` | route Lakehouse does not serve |
| `vt:vmalert.proxyURL` | `vt.flag.vmalert_proxy_url` (**added**) | same, traces side |
| `vt:search.maxTraces` | `vt.flag.search_max_traces` | result cap |
| `vt:search.maxTags` | `vt.flag.search_max_tags` | result cap |
| `vt:search.fieldsLookbehind` | `vt.flag.search_fields_lookbehind` | cold lookbehind default |
| `vt:search.streamFieldsLookbehind` | `vt.flag.search_stream_fields_lookbehind` | cold lookbehind default |
| `vt:nativeinsert.maxRequestSize` | `vt.flag.nativeinsert_max_request_size` (**added**) | ingest limit on a new route |

`vl:nativeinsert.maxRequestSize` did not change with the bump but was rowed
alongside its VictoriaTraces twin, so the native-ingest admission cap is covered
on both surfaces.

The VT v0.12.0 bump moved one flag (`search.allowPartialResponse`, above), reworded
`search.latencyOffset` (which now also caps the LogsQL query APIs) and added no flag.
Both are covered by `vt.flag.search_allow_partial_response` and
`vt.flag.search_latency_offset`.

Two deprecations to keep in view: `search.traceMaxServiceNameList` and
`search.traceMaxSpanNameList` are assigned to `_` in VictoriaTraces 0.11.0
(superseded by `search.maxTags`) — still registered, so still warned about, but
setting them now does nothing.

## LogsQL literals outside the registry

VictoriaLogs 1.51.0 tightened the pipe grammar: a bare word after a pipe is no
longer silently a filter, so `_time:5m | error` must become
`_time:5m | filter error` (or fold into the leading filter as
`_time:5m error`). Quoted tokens, non-word tokens (`!foo`, `{host="x"}`, `>5`)
and `not` are still accepted bare — 1.51.0 rejected those too and 1.52.0
restored them, which is why the rule is pinned by a test rather than by prose.

`TestRows_QueriesParseWithUpstream` already parses the registry's own row
queries. `logsql_literals_test.go` extends the same idea to every other LogsQL
literal in the repository — Go tests and sources, benchmark and operational
shell scripts, docs, dashboards, alerts, workflows, the changelog: it extracts
quoted/backticked spans containing a `|`, keeps the ones that look like LogsQL
(a stage name the parser recognises after a pipe, or a `_time`/`_msg`/`_stream`
field), and fails on any that VictoriaLogs rejects with the missing-`filter`
error. Strings that fail for any other reason (templates with `%s` holes,
partial pipelines) are ignored, and genuine look-alikes go in the `notLogsQL`
map with a reason — where a second test deletes entries that stop matching
anything. `TestLogsQLPipeGrammarContract` pins the accepted and rejected forms
directly against the vendored parser, so the next grammar change fails a unit
test instead of a production query.

## Flag collisions in the traces binary

`lakehouse-traces` links VictoriaLogs' and VictoriaTraces' packages into one
process, and both register flags of the same name (`-retentionPeriod`,
`-storageDataPath`, `-insert.maxFieldsPerLine`, ...) with the same global
`flag.CommandLine`. The `patches/vt-traces/*-flag-dedup*` patches make the
VictoriaTraces side reuse VictoriaLogs' registration instead of panicking.

`TestVTFlagDedupCoversEveryCollision` recomputes that collision set from the two
vendored trees — using `go list -deps` on the traces module, so only packages
actually linked count — and requires the dedup list to match it exactly in both
directions: an unguarded collision means the binary panics at startup, a guard
with nothing behind it means VictoriaTraces silently skips its own registration.
The count is recomputed on every run (`t.Logf` prints it and the names); at VL v1.52.0 /
VT v0.12.0 it is 32 (`-insert.maxFieldsPerLine` and `-defaultMsgValue` are no longer among them:
VictoriaTraces' ingest package aliases VictoriaLogs' flags instead of registering its own).
VT v0.12.0 moved
`-search.allowPartialResponse` into `vtselect/searchutil` (Tempo and Jaeger read it too) and
`lakehouse-traces` now serves LogsQL through VictoriaTraces' own `vtselect/logsql`, so
VictoriaLogs' `vlselect/logsql` is no longer linked into the traces binary at all: that flag,
`search.maxQueryTimeRange` and `search.maxQueryLen` are registered once, by VictoriaTraces, and
need no dedup.

## Feature catalog

The rows above answer "what does upstream have that we might have missed". The feature
catalog (`registry/features/<area>.yaml`) answers the mirror question: **what did we build,
and is any of it unverified or undocumented?** No working Lakehouse feature may be missed by
verification, and the feature highlights users read are generated from what actually ships —
never written separately.

One entry per capability:

```yaml
- id: lh.feature.<area>.<name>      # stable, unique; <area> must match the area field
  title: Human-readable name
  status: shipped | in-progress | planned
  area: storage|query|ingest|tenancy|ui|ops|cache|compaction|traces|deletion|observability|security|deploy
  readme_section: Write Path        # optional; places the highlight in README's Key Features
  surfaces: [api, ui, flag, storage, ingest, cli]
  rows: [lh.bloom.status.schema]    # registry row ids that verify it (must exist)
  tests: ["tests/e2e/delete_test.go#TestDelete_Verify", "internal/delete/tombstone_test.go"]
  bench: [count_total]              # scripts/bench/run.sh scenario ids
  docs: ["docs/deletion-strategy.md#verify-endpoint"]
  highlight: "one line, rendered into README.md and docs/features.md"
  description: >
    a paragraph, rendered into docs/features.md
  changelog_bullets:                # the exact bold lead-ins of its `### Added` bullets
    - 'Per-field storage/metadata size stats — foundation.'
  # Optional release information — only what CHANGELOG.md cannot supply:
  since: "v0.83.0"                  # a released version; see "Releases" below
  changelog: ["0.85.0"]             # only without changelog_bullets: released versions describing it
```

**Releases** are read from `CHANGELOG.md`, never recorded in the catalog: after every release
the release workflow assembles the `changelog.d/` fragments
under the new version heading and does not touch the catalog, so a recorded "unreleased" would go stale the moment a feature ships.
`docs/features.md` shows a feature's first release as the oldest version among its claimed
bullets and lists every version they sit under. An entry still under `[Unreleased]` reads "the
release after v*N*" (*N* being the newest release) — a statement that stays true once the
release workflow moves it under the next heading, so `confgen -check` accepts it there as well
as the exact version, and the release PR stays green without regenerating anything; the next
`make conformance-gen` renders the exact version. `since:` is optional and only ever a released
version: on a feature with bullets it overrides the derived first release with the release of
another of its bullets (a rebuild that keeps the superseded implementation's bullets as
history); on a feature without bullets it declares the release it shipped in.

Decoding is strict (an unknown key fails), ids must be unique, and every `tests:` and `docs:`
reference is checked against the filesystem: the file must exist, a `#TestName` suffix must
resolve to a `func TestName(` in that file (or, for a script, to that word), and a `#anchor`
must match a real heading in the document. A `tests:` entry must name a test — a `*_test.go`
file, a test script (`*_test.sh`, `test_*.sh`, `*_test.py`, `test_*.py`), or a shell or Python
script under `tests/` or `scripts/**/tests/` — never the CI workflow, checker script or
implementation it covers; when a CI job is the verification, say so in `notes:`, and say there
too when a linked test is not run by any CI job. Linking a test that does not exist is worse than
linking none — it claims verification the repo does not have — so `tests: []` and a place in
the verification-gap list is the honest answer for a feature nothing covers yet.

**Gate rules** (`tests/conformance/features.go`, run by `make conformance-check` and by
`confgen -check`):

1. every registry row with `origin: lh-addition` or `lh-shim` belongs to **exactly one** feature;
2. every `shipped` feature cites at least one row or one test;
3. every referenced row, test and doc exists;
4. every `### Added` changelog bullet with a bold lead-in maps to a feature, matched on the
   exact lead-in text via `changelog_bullets` (bullets without a bold lead-in are sub-details
   and are out of scope), and every lead-in a feature claims is such a bullet;
5. an `in-progress` or `planned` feature may only cite rows that are `pending: true`;
6. release information agrees with `CHANGELOG.md`: a `since:` override names the release of
   one of the feature's own bullets (and differs from the derived first release), a `since:`
   or `changelog:` on a feature without bullets names a released version heading.

Rule 2 is satisfied by a declared (pending) row, which is why the generated
`docs/features.md` separates ✅ (a regression test is linked, or a non-pending row) from 🟡
(shipped, but only declared verification) and lists every 🟡 feature under **Coverage gaps** —
that list is the backlog. ✅ asserts the test exists, not that CI runs it.

### Adding or extending a feature

A PR that adds or extends a Lakehouse feature is not mergeable without its catalog entry and
the regenerated documents. `scripts/ci/check_registry_touch.sh` classifies a PR as a feature
PR when, compared with its merge base, it adds a bold lead-in to a `### Added` section of
`CHANGELOG.md` or an `added` fragment in `changelog.d/`, adds or removes a route, handler or `lakehouse.*` flag registration in non-test
Go under `internal/`, `cmd/` or `lakehouse-traces/`, adds a YAML config key to a struct in
`internal/config/`, or adds a new `lh.*` registry row — and fails it unless
`tests/conformance/registry/features/**` also changed and the generated documents are current.
The first three are set comparisons rather than diff greps, so moving text is never mistaken
for a new capability: a bold bullet under `### Fixed` or `### Changed`, the release workflow
moving the fragments under a version heading, and a registration or config field
that only moved within its file are not feature signals.

Checklist:

1. Add or update the entry in `tests/conformance/registry/features/<area>.yaml` — including
   `rows:`, `tests:`, `docs:` and a `highlight:` written for a reader, not for a reviewer.
2. If the change added a `### Added` changelog bullet, put its exact bold lead-in in that
   feature's `changelog_bullets:` (one feature may span several versions). Do not add
   `since:` for it: the release is read from the changelog.
3. If it is a new Lakehouse endpoint, add its registry row too, and cite the row in the feature.
4. Run `make conformance-gen` and commit the regenerated `docs/features.md` and `README.md`.
5. Run `make conformance-check` (drift + generated files + the feature gate) and
   `bash scripts/ci/tests/test_check_registry_touch.sh` if you touched the checker.

`docs/features.md` and the README block between `<!-- features:begin -->` and
`<!-- features:end -->` are generated — never edit them by hand.

## Proof coverage report

[`docs/proof-coverage.md`](../../docs/proof-coverage.md) is the single answer to "what is
proven, how, and what is missing". `make conformance-gen` computes it from the rows, the
feature catalog, the upstream inventory and the repository tree (`report/proof_coverage.go`,
`report/proof_shared.go`); `make conformance-check` fails when it is stale, like the other
generated files, so it is regenerated in the same PR as any registry change. It is
deterministic (sorted, no timestamps). It holds, with a verdict header first:

- per upstream kind and signal: items, with a row, verified, pending, differs, absent or
  unsupported, no row, and how many link a differential test;
- the upstream pass rows split into differential (the row targets `hot` and links a test
  under `tests/parity/` or `tests/e2e/`), other test and no test, with the no-test row ids;
- every shipped feature with its rows, tests, perf scenario and a proof class
  (`upstream-exact`, `lh-extension`, `perf-proven`, `unproven`);
- the shared upstream code (Makefile pins, `patches/` files, imported VictoriaLogs and
  VictoriaTraces packages) and which of them no Go test or `scripts/ci` gate names;
- the pending rows by reason (waiting for the HTTP runner, or executed by another CI job
  or by cited Go tests).

`docs/proof-coverage.json` carries the same numbers for PR comments (`verdict` holds the
headline keys). The report reads only what the registry already declares; a link is a name,
not proof that the named test checks that exact request.

## Registry gate on every PR

Owner rule (2026-10-07): no PR merges unless its behaviour changes and its tests are covered by
registry changes. `scripts/ci/check_registry_touch.sh` enforces it, on top of the route/pin and
feature-catalog rules above.

**The gate decides by itself, from the verified content of the diff.** There is no exemption: no
label, no line in the PR body, no approver list for the coverage rules. The owner's required
approval on every PR (branch protection, `CODEOWNERS`) is the human review; the gate adds no second
human step. What a reviewer should look at but the gate cannot judge (edits of lock code and of the
gate's own files) is listed in the report, not failed. What can be judged from content (Rules 1-3)
is hard and has no bypass.

**Run it before every push:** `make registry-gate` (`scripts/ci/gate_local.sh`) runs the gate script
against `origin/main` for the current `HEAD` and prints the same report. It is a preview of CI, not the
same code path, in three ways it says out loud:
- it runs the gate code of your WORKING TREE, while CI judges a pull request with the gate code of its
  merge base (so a PR cannot weaken the gate that judges it). When your branch changes gate files, the
  command prints a notice: CI's verdict will come from main's copy of the gate;
- it judges `HEAD` only, so uncommitted edits are ignored (a warning lists them): commit first;
- it fetches `origin/main` first and exits non-zero when it cannot, because a verdict against a stale
  base is worse than none. `REGISTRY_GATE_OFFLINE=1` judges against the local `origin/main` anyway (with
  a warning when it was last fetched more than 24h ago).
`GATE_BASE=<rev> make registry-gate` compares against another base (no fetch).

**One report.** The gate writes one Markdown report to the job summary (`$GITHUB_STEP_SUMMARY`; no
new token permission) and prints it when run locally: the kind of PR (an exempt kind, product change,
or not a product change), a PASS / FAIL / n/a row per rule with its detail, for every failure a
one-line "how to fix", the lock-affecting files and gate files the PR touches (informational), and the
registry entries the PR adds, changes and removes. When files need a person's review the verdict says so
instead of a bare PASS ("PASS - 3 lock-affecting files and 1 gate file need review (listed below)"), and
every such file also gets an inline `::warning file=<path>::` annotation (at most 10 per kind, then one
"... and N more"), which GitHub shows in the pull request's Files tab. Every path, row id and test name
that reaches the report or a workflow command is escaped first (Markdown: `& < > |` and the backtick;
workflow commands: `%`, CR, LF, and `:` `,` in a property); a file named ``a`<!--`` or a row id with a
newline and `::error::` cannot forge report markup or a log command. `registry-gate` and `registry-gate-base` run the same
script and print the same report. The exit status is nonzero only for a hard failure.

**Where the gate runs, and what it never runs.**
- `registry-gate.yaml` (job `registry-gate`, on `pull_request`) is a job of its own on a fresh
  runner. It runs no code from the PR: it checks the PR out as data, reads `gate_bootstrap.sh`
  from the merge base, builds `cmd/testlinks` from a worktree of the merge base, and runs the base's
  `check_registry_touch.sh` against the PR's diff with an allow-listed environment and git hooks
  disabled. The heavy `Conformance` workflow (which does run PR code: `make conformance-check`, the
  gate's self-tests) listens to pushes only, never to label or body edits, so a bot's label cannot
  cancel or mask it.
- `registry-gate-base.yaml` (job `registry-gate-base`, on `pull_request_target`) is the same gate
  from the base branch's own workflow file: a PR can edit `registry-gate.yaml` (that file comes from
  the PR) but not this one. It fetches the PR head into a detached worktree and only reads it; the
  gate code comes from the base checkout. It only starts running after it is on `main`.
- The gate no longer runs `confgen`: `make conformance-check` (heavy job) checks the generated
  documents.
- A PR that changes the gate is judged by the gate of its merge base; its own changes take effect
  once merged. Its changes to gate files are listed in the report for the reviewer.

**Owner setup (what code cannot do).** Require these checks on `main`: `conformance-inventory`
(the heavy checks; it no longer holds the gate), `registry-gate`, `registry-gate-base` and the Parity
Tests job `parity` (it always runs and skips quickly when nothing parity-relevant changed). Pin
`registry-gate-base` to the workflow on `main` with a ruleset ("Require workflows to pass before
merging", path `.github/workflows/registry-gate-base.yaml`, branch `main`) and enforce it for
administrators. Until `registry-gate-base` is pinned, a PR that rewrites `registry-gate.yaml` can
neutralise that one workflow (the base-run workflow judges it as soon as it exists).

*Canary check after the setup (a throwaway PR each):* (1) change one line of `internal/` code and no
registry file: `registry-gate` and `registry-gate-base` must both fail; (2) edit
`.github/workflows/registry-gate.yaml` to `exit 0` in a PR: `registry-gate-base` must still fail,
and the PR must show both checks as required; (3) a PR whose only changes are a Go toolchain bump
(`go` directive, `golang:` image tags) must be green on both checks with the kind `toolchain-only`.

All the actions in the two gate workflows are pinned by full commit SHA.

**Rule 1 — product change needs a registry change.** A PR is product-changing when it changes any
file that is not a test under `internal/`, `cmd/` or `lakehouse-traces/` (Go, embedded UI assets,
SQL, YAML; only `README.md` and `RUNBOOK.md` are documentation), any file under `patches/` or
`charts/` (their Markdown and text docs excepted, templates included), or a shipped build file:
`Dockerfile`, `Dockerfile.logs`, `Dockerfile.traces`, and the root `go.mod` and `go.sum`
(`lakehouse-traces/go.mod` is covered by its tree; a root Dockerfile whose only change is the
`@sha256:` digest of its `FROM` images is not product, see "Digest pins" below). That includes the config defaults:
`internal/config/config.go` (`Default()`), `internal/config/profile.go`, the flag defaults in
`cmd/*/main.go` and `charts/victoria-lakehouse/values*.yaml`. A test is `*_test.go`, anything under
`testdata/`, and `test_*` / `*_test` shell and Python scripts. There is no generated-file
exclusion: the product trees hold none, and a marker comment must not switch the gate off. PRs that
touch only tests, `Makefile`, `scripts/`, `.github/`, docs or other Dockerfiles (`Dockerfile.datagen`
among them) are not product-changing. A product-changing PR must add, remove or change a row or feature by more than
prose: edits of `title`, `notes`, `description`, `highlight`, `differ_note` and `refs.doc` do not
count, and neither do comments, blank lines or indentation (entries are compared parsed, at both
revisions). Add or update the row that describes the changed behaviour; for a Lakehouse capability
also the feature. The job summary lists the rows and features the PR adds, changes and removes
(counts, the first eight ids, the rest in a collapsed block).

*Not checked yet:* that the changed entries are *about* the changed product paths (a stray structural
edit to an unrelated row still passes). A `covers:` glob per row or feature would close it; it is
proposed, not built, because the scoring below says the migration is the cost: code about 150 lines
(schema field, loader, matcher in `registry/` and `testlinks`) and 1 package touched; features 1 new
key to author per entry (about 800 rows and 160 features today to fill, by a one-off script from
their refs) and one new failure mode (a path nobody covers); scale none (a glob match per changed
file, microseconds). The alternative of requiring each changed product package to appear in some
changed entry's `refs.tests` paths costs nothing to add but fails for rows that prove behaviour from
`tests/e2e` and `tests/parity`, so it would need a long list of special cases. All numbers are estimates
(assumed), not measurements.

**Rule 2 — tests are linked (Linking tests).** Applies to every PR, including test-only ones. Every
top-level `func Test…` / `func Fuzz…` the PR adds in `internal/**`, `cmd/**`, `lakehouse-traces/**`,
`tests/parity`, `tests/e2e`, `tests/conformance` or `tests/ingestmatrix` must be named by a row
(`refs.tests`) or a feature (`tests:`), as `path/to/file_test.go#TestName`. A bare
`path/to/file_test.go` reference does not link a new test (it was written before the test existed);
`tests/s3compat` is in scope too. Tests are found with `go/parser` (any Unicode
name; a test file that does not parse is an error). "Added" is a per-package set comparison against
the merge base, so a test moved between files of its package is not added, a renamed test is a
removal plus an addition, and a test moved to another package is an addition. A reference into a
test file the PR touched must still resolve: removing or renaming a test while leaving its
reference fails. Existing unlinked tests need no backfill (`go run ./tests/conformance/cmd/testlinks
-report-unlinked` counts them; dot-directories are skipped). `TestMain` and helpers without the
`Test` prefix are not tests. The engine is `registry/testlinks.go`; the command is `cmd/testlinks`.

**Exempt kinds** (decided first, from the content of the diff, never from a label, a title or a
commit message):
- release-metadata PRs. The files are exactly `CHANGELOG.md`, `charts/victoria-lakehouse/Chart.yaml`
  (required), `README.md` (optional badge) and the regenerated `docs/features.md` (optional;
  `UPSTREAM_COVERAGE.md` and the inventory carry no version naming and may not change). Each file
  is held to its release shape:
  - `CHANGELOG.md`: exactly one new `## [x.y.z] - date` section directly below `[Unreleased]`, with
    x.y.z newer than the newest section. Text may move only between `[Unreleased]`, the new section
    and the previously newest section (a merge of main into the metadata branch moves bullets
    there); every older section is byte-identical, and no line may be lost, added, edited or
    duplicated. The `changelog.d/<name>.<kind>.md` fragments the PR deletes are also files of a
    release-metadata PR (plain deletions of valid fragments, nothing else under `changelog.d/`),
    and the lines of the deleted fragments are the lines the new section gains over `[Unreleased]`.
  - `Chart.yaml`: only the top-level `version` and `appVersion` lines change, both to that same
    x.y.z, which is newer than the base chart version; every other byte is identical (no stripping).
  - `README.md`: identical once version numbers are replaced.
  - `docs/features.md`: identical line for line, in order, except `since:` / `Changelog:` values
    that turn "the release after vOLD" (OLD = the previously newest release) into vNEW, `NEW` or
    "the release after vNEW". A `since:` swapped between features, a reordered line, or any other
    version change fails. A feature that still has an Unreleased bullet is named "the release
    after vX", so every release renames it. The auto-release workflow regenerates the file with
    `confgen -write` (and reverts every other generated file); `make conformance-gen` does the
    same by hand.
  - The PR author is the release bot (`github-actions[bot]`) or an approver, and when the
    repository has tags, the release tag `vx.y.z` exists.
  Which gate checks what: `check_changelog_pr.py` (changelog-check) decides the file set,
  the CHANGELOG shape and the `docs/features.md` naming; `pr_classify.py` (the registry gate)
  reuses those and adds the author, Chart.yaml, README and tag checks.
- dependency-only PRs, decided from the content of every changed file and from nothing the author
  writes (no title, no commit subject): the files are the two modules' `go.mod` / `go.sum` and
  `requirements*.txt` outside the product trees; in `go.mod` only `require` entries are added, removed
  or change version (no `replace`, `go`, `toolchain` or other directive); `go.sum` changes only
  well-formed hash lines of non-critical modules; a requirements file changes only package
  requirement lines (no URLs, `-r`, `-e`, index options). The set of required modules is equal at both
  revisions (a newly added `require` is a new dependency, not a bump, and is refused like a removed
  one). **Modules are critical unless they are known to be inert.** The first version of the gate listed
  the critical modules and was one short more than once (`xxhash`, then `valyala/fastjson`,
  `valyala/quicktemplate`, `gopkg.in/yaml.v3`, `google/uuid`, `golang.org/x/text`, ...), so
  `INERT_MODULES` in `pr_classify.py` is an allowlist of the modules a bump of which cannot change a
  stored or answered byte, each with its reason: test-only libraries (`go.uber.org/goleak`), runtime
  the outbound OAuth2 token source (`x/oauth2`), the OpenTelemetry SDK and
  its exporters (`go.opentelemetry.io/otel/...`, which trace the process and ship spans out of band), the
  logging facade and retry timer behind them, and `golang.org/x/net`. `x/net` is inert because nothing in
  this repository or in `deps/` imports it: it is reached only through `google.golang.org/grpc` (the OTLP
  trace exporter client), and `net/http` uses the copy vendored into GOROOT; `test_pr_classify_critical.py`
  re-checks that, by scanning import lines with a regular expression (it is not `go list -deps`, so it sees
  neither build-tag-only nor generated imports; cross-check by hand when deciding). NOT inert:
  `golang.org/x/sync` (the `singleflight` of `internal/s3reader/dedup.go` hands one result to every concurrent
  cold read of the same range) and `golang.org/x/sys` (`x/sys/cpu` is what parquet-go consults to select its SIMD
  encoders): a bump of either can change a stored or answered byte, so it needs registry coverage;
  `go.opentelemetry.io/proto/` (the OTLP wire format),
  `go.opentelemetry.io/contrib/` and `httpsnoop` (they wrap every handler's `ResponseWriter`),
  `golang.org/x/text`, `google/uuid`, `yaml.v3`, every `valyala/*` module, the codecs, the AWS SDK, parquet-go,
  VictoriaMetrics/VictoriaLogs and `google.golang.org/*`. A module in neither list is critical, and the drift
  test fails until a person classifies it. A bump of a critical module is never dependency-only: it needs
  registry coverage. The kind waives **Rule 1 only**; Rules 2 and 3 still run. (It used to read the commit
  subjects and the PR title, which the author writes, and to skip Rules 1-3.) The drift test also fails when
  an inert module is imported by the stream-id, trace-index, schema or Parquet code, or by a package it was
  not reviewed for;
- `toolchain-only` PRs: a Go toolchain bump such as #491 (Go 1.27.1 to 1.27.2). Every changed file
  must be in this set and its diff must be nothing but versions: `go.mod` and
  `lakehouse-traces/go.mod` (the `go` and `toolchain` directive lines, and the version of `require`
  entries, none of them a critical module (see above); same set of modules, every other line equal),
  `go.work` (the `go` / `toolchain` lines), `go.sum` and `lakehouse-traces/go.sum` (well-formed hash
  lines, none for a storage-critical module), `Dockerfile`, `Dockerfile.logs`, `Dockerfile.traces` and
  `Dockerfile.datagen` (`FROM [--platform=...] golang:<version>[-alpine[<ver>]][@sha256:<digest>] [AS name]` lines whose
  version changes (either side may carry a digest, and the digests may differ), the platform, the suffix shape and the stage name equal, every other line
  byte-identical, same line count) and `tests/parity/docker-compose.yml` (`image: golang:<version>[@sha256:<digest>]`
  lines, the same way). **A bump moves forward**: the `go` and `toolchain` directives, the Go version of a
  `golang:` image and its alpine release may not be older at head than at the merge base (compared
  numerically: `alpine3.9` is older than `alpine3.23`), and a `toolchain` line may not be dropped; a
  downgrade is a product change. Plain non-product files may ride along: documentation and Markdown, CI
  workflows and scripts that are not gate files. A different base image, an added flag, a changed env
  var, a changed `replace`, a storage-critical module bump, a test, a registry row, a parity lock or
  a gate file in the same PR make it a normal product change. The kind waives **Rule 1 only**: Rules 2
  and 3 still run. (`pr_classify.py`; its tests are `scripts/ci/tests/test_pr_classify_toolchain.py`.)
  The alpine release in a builder tag (`golang:1.27.2-alpine3.23`) is part of the toolchain: a PR
  that moves only `alpine3.23` to `alpine3.24` is `toolchain-only` by design (the Go version is
  unchanged but the builder image is a toolchain input, exactly like the Go tag).
- **Digest pins of the release Dockerfiles.** Not an exempt kind: a PR whose only change to
  `Dockerfile`, `Dockerfile.logs` or `Dockerfile.traces` is the `@sha256:` digest of its `FROM` lines
  (`Dockerfile.datagen` is not a shipped build file) is not product-changing for Rule 1 (`exempt=none`,
  `product=0`; Rules 2 and 3 still run). For every `FROM [--platform=...] <image>:<tag>[@sha256:<digest>] [AS name]`
  line that differs, the platform, the image name, the tag and the stage name must be equal on both sides and
  only the digest may be added, removed or changed, for any base image (golang, distroless, alpine, ...). The
  digest is exactly 64 lowercase hex characters after `@sha256:`; a short, uppercase or other-algorithm digest,
  an image with a digest but no tag, a `${VAR}` or `registry:port` reference, a lowercase `from`, an indented
  line, extra blanks, a CR, an added or removed line, a heredoc or an `escape=` directive in the file, or a
  line continuation in front of the changed line all keep the file a product change. The same file with a
  version bump in the same PR is judged by the toolchain rule (golang lines only, digests optional), so a bump
  that also changes the digest of another image is a product change (fail closed). Only the root files are
  covered. (`pr_classify.py` `image_pin_only`; the shared fixture
  `scripts/ci/tests/testdata/golang_image_cases.json` has the `dockerfile-pin` cases, Python only: the Go
  twin `registry.GolangImageBumpOnly` reads the compose `golang` line of the lock suites' runtime
  configuration, where a digest is optional on either side and may change on its own.)
- **How the files are read (every kind above).** A classifier that reads a file differently from the tool
  that consumes it can be made to approve a change it never saw. So `pr_classify.py` reads each file as
  BYTES, decodes it as strict UTF-8 and splits lines only on `\n`, as Docker, Go and git do. Python's
  text mode and `str.splitlines()` also split on a lone CR and on U+0085, U+2028 and U+2029, which let a
  build change hide inside a "comment" line (a CR between `# ... per deployment.` and `ARG FIPS=0` in
  `Dockerfile.logs` made the FIPS argument vanish from the Docker build but not from the classifier; a CR
  before a `replace` line in `go.mod` dropped the patched VictoriaLogs tree). A covered file whose base
  or head holds a control byte (CR included, so a CRLF file is never verified), a BOM, one of those
  separators or invalid UTF-8 is not verified: the PR falls through to the path rules and stays an
  ordinary product change (it is rejected, never normalised). Every line a kind interprets is matched
  with ASCII-only character classes (`[0-9]`, `[!-~]`; Python's `\d` matches Arabic-Indic digits, Go's
  and Docker's do not), and every line that CHANGES in a covered file must be printable ASCII. Unchanged
  non-ASCII text is fine: `lakehouse-traces/go.mod` and the parity compose file hold an em dash in a
  comment, and rejecting those files outright would have made the real #491 diff a product change. The Go
  twin (`GolangImageBumpOnly`) rejects the same bytes and shares the fixture
  `scripts/ci/tests/testdata/golang_image_cases.json`.
  Changed paths are read NUL-separated (`git diff -z`) by the classifier, `testlinks` and the shell gate: git
  prints a path with a space unquoted (white-space splitting would lose the first half, and with it the new
  tests in that file) and a non-ASCII path in octal quotes (an anchored pattern would miss it).
- docs-only and CI-only PRs (not product-changing).
Upstream pin and patch changes keep the stricter route rule above.

**There is no exemption.** The `registry-exempt` label and the `Registry: none - <reason>` body line
are gone: a PR with genuinely nothing to cover (a pure refactor with tests already linked) passes
Rule 1 only when it is not a product change; a product change that changes no behaviour still has to
change a row or feature by more than prose, so that the registry says what it covers. A PR that must
weaken a lock on purpose cannot pass Rule 3; the owner merges it with the administrator bypass, once,
knowingly.

**Reported, not failed.** The gate lists these in the report, because the owner's required approval
covers them and a content check cannot judge them:
- **Gate files:** any file under `scripts/ci/` (the gate's scripts, its tests, its fixtures and the replay table),
  `.gitattributes`, `tests/parity/lock_cells_test.go`, `tests/parity/known_failures.txt` and
  `tests/parity/lock_cells.txt` (the data the Go gate and the Python ratchet both read; a parity fix that adds
  floors is therefore listed, not failed), `cmd/testlinks`, `registry/{testlinks,paritygate,lockcode,escape,canonical}.go`
  (`escape.go` and `canonical.go` shape what the gate reads and prints), `.github/CODEOWNERS`,
  `.github/workflows/{conformance,registry-gate,registry-gate-base,gate-selftest,parity}.yaml` and
  `.github/release-metadata-approvers` (its deprecated name `.github/registry-exempt-approvers` is
  still read from a merge base that predates the rename, and is deleted one release later). `.github/CODEOWNERS` lists the owner for `.github/`,
  `scripts/ci/` and `tests/conformance/`. A PR that changes any of them is judged by the merge
  base's copy of the gate, so it cannot loosen its own judge. Lowering or removing a floor in
  `tests/parity/lock_cells.txt` is still a Rule 3 failure.
- **Lock-affecting files** (see "Lock packages" below): every existing file of a lock package the PR edits or
  deletes, **every new `.go` file there** (a test file that declares only funcs still runs inside the lock
  package: a first-sorting `TestAAAWarmup` that repoints the shared base URLs makes every later lock compare
  Lakehouse with itself, and a test that prints a forged transcript and exits does the same; no syntactic rule
  can tell those from a legitimate new test, so the reviewer reads them), any file of the package that gains an
  import of a package the package did not import at the merge base (a blank import runs that package's `init()`
  before any test), every new native source file (`.c`, `.s`, `.syso` ...) in the package directory, and the
  lock suites' runtime configuration.
- **Lock-adjacent packages.** A lock package's test binary also links every package its test layer imports, and
  an `init()` there runs before any test. A lock-adjacent package is an in-repo package whose NON-test files are
  linked into a lock package's test binary through the lock package's test layer (its `_test.go` files, and every `.go`
  file of a package under `tests/`), followed through non-test files to depth 3, computed at the merge base AND at
  head (a package the PR adds and a lock test imports is covered from its first commit). Today (computed by
  `LockAdjacentDirs` over 20 lock directories) that is 16 packages: `tests/ingestmatrix` (imported by `tests/e2e`,
  neither lock nor product), `internal/testutil` and `internal/testutil/storageinvariants`, and 13 product packages
  (`internal/pmeta`, `internal/config`, `internal/buffer`, `internal/s3reader` ...); none of them has ever held an
  `init()` (git history, `git log -S'func init()'`). A new `.go` file in a test-support
  package (under `tests/`), a package-level var whose initializer calls a function, a blank or dot import, an import of
  a module outside this repository (or of a package under `tests/`), and a changed file that stops parsing are listed.
  An added `init()`, `import "C"` and any new or changed native source file are hard failures (Rule 3). A new func-only
  file in an adjacent product package is not listed (it runs nothing at load). **Known limits** (issue #545): the
  product files of the lock packages themselves and what they import (the code under test; a registry content change
  is the only control), dynamic loading (plugins, `go:linkname`), build tags set from the environment, `go.mod`
  `replace` directives and other non-blob inputs.
- **Listing size.** The report lists every file above, so its length is a cost of review. Measured on
  2026-10-12 with the gate binaries of main (#512) and of this change over eight pull requests, files listed
  before and after: #487 49 and 49 (22 before the new-file rule of #512), #438 28 and 28, #430 17 and 17, #310 48 and
  49, #412 9 and 9, #369 6 and 6, #404 37 and 37, #483 77 and 77. The lock-adjacent rules add one listed file in
  eight pull requests (a called package-level var in `internal/metrics`, #310). Not reduced: a func-only new test file
  can be the `TestAAAWarmup` shape, and a new product file of a lock package could not be proven harmless (issue #546).
- **Not enforced, on purpose:** that an edited lock or helper still compares what it compared. The gate cannot
  decide that from content; it makes sure a person looks.

**Gate replay.** `make gate-replay` (`scripts/ci/gate_replay.sh`) replays the gate over recorded pull
requests, pinned by sha, and over synthetic pull requests built in a scratch clone, and compares each
verdict and its set of failing rules with `scripts/ci/tests/gate_replay_expected.txt`. It runs in CI
(`gate-selftest.yaml`) whenever the gate, `scripts/ci/`, `tests/conformance/` or the approvers file
change, so a change that makes the gate accept what it must reject (or reject what it must accept) fails
on real history. The table has exactly nine columns (a row with another count is an error; a literal `|`
in a cell is written `\|`), and for a synthetic case the title column is also the commit subject, so a case
can wear the title an attacker would choose. The synthetic negatives include the review's smuggles
(`cr_fips`, `cr_replace_tc`, `cr_replace_dep`, `u2028_replace_dep`, `tc_unidigit`, `init_exit`) and the shapes of the second review
(issue #502): `warmup_test`, `forged_transcript_exit`, `blank_import_hook`, `cgo_constructor`, `allowlist_nel`, `fastjson_bump`,
`toolchain_downgrade`, `dependency_new_require`, `loadtime_var_move`, `loadtime_file_rename`, `path_with_newline`,
`gate_file_listing`; the third review adds the symlink rows (`s_newlink_e2e_nontest`, `s_newlink_parity`,
`s_newlink_product_lockpkg`, `s_replace_lockfile`, `s_symlinked_test`, `s_symlink_dir`, `s_symlink_docs_only`), the moved-declaration rows
(`m_alias_swap`, `m_alias_noalias`, `m_literal_whitespace`, `m_build_constraint`, `m_move_changed`, and the controls
`m_pure_move_imports`, `m_pure_move_nearby`), the lock-adjacent rows (`x_ingestmatrix_init`, `x_ingestmatrix_newfile`,
`x_ingestmatrix_cgo`, `x_ingestmatrix_native`, `x_product_adjacent_init`; `blank_import_hook` became a failure), `minpass_lowered`, `minpass_removed`, `x_sync_bump`, `x_sys_bump` and
`toolchain_overflow`. Cost
(measured 2026-10-11 by the independent review; it varies with the runner): judging one large recorded pull request
took 68 s (#483) and 79 s (#487) locally, one CI replay shard about 1:40, one gate job about 30 s; the gate jobs have a
10 minute timeout and the replay jobs 40, and a timeout fails the check (closed). `testlinks` reads every blob through one
persistent `git cat-file --batch` process (`cmd/testlinks/blobs.go`, cached per revision and path) instead of one `git show`
per blob; a path with a line break, a non-blob object and any protocol surprise fall back to `git show`, so the answers
are identical. Measured 2026-10-12 on a busy host (load average 20-40), the gate over the recorded pull requests with the
old reader against the new one, the reports byte-identical: #483 338 s to 19 s, #487 276 s to 20 s, #491 215 s to 12 s. To record a new case, add a line to the table (the header of
the script documents the columns); a fixed gate bug gets the pull request that exposed it.

### Parity fixes ship locks

Owner rule (2026-10-07): every parity we fix needs hardening and detailed tests, so a later
performance change cannot break the compatibility pattern. `cmd/testlinks` (engine:
`registry/paritygate.go`) detects a **parity-fix PR** when, against the merge base, the PR

- removes an entry from the parity allowlist, or
- marks a divergence **Resolved** in `docs/parity-and-gaps.md` (a table row whose cell is
  `Resolved` that was not resolved before, including a new row born resolved), or
- flips a registry row from `expect: differ` (a known gap) to `expect: pass`.

(A fourth signal, "closes #N" on an issue labelled `parity`, is not implemented: the gate does not
read issue labels. Reviewers check it.)

Such a PR must ship a **lock**: a registry row with `expect: pass`, not `pending` (a pending row is
declared but not executed), and an exact-equivalent compare
(`exact-json`, `count`, `trace`, `ndjson-multiset`, `values-with-hits` with `hits_tolerance` 0,
`series` with `rel_tolerance` 0) whose `refs.tests` names, as `file#Test`, a function of the
differential suite `tests/parity` that this PR **added or modified** (comments and whitespace do not
count as a modification). A bare file reference is not enough, and an unchanged row is not this PR's
lock. For a `differ`-to-`pass` flip, the flipped row itself must be that lock. For a removed
allowlist entry, a lock must reference the **top-level test of that entry** (entry
`TestParity_X/logs/parquet` needs a lock naming `file#TestParity_X`), so a lock for an unrelated test
does not lock the fix. The owner's rule asks
for more (every layer, both signals, both tenant forms, property or fuzz coverage); CI can only check
that the parity test and the exact lock exist, so reviewers check the breadth.

**Locks have a runtime floor.** Static checks on a test file can never be complete (an early return,
an aliased `Skip`, a helper that skips, a `TestMain` that exits 0, a file renamed so it is not built).
So every lock test also reports how many cells it compared, through the shared helper
`tests/parity/lock_cells_test.go` (`reportLockCells`; `RunParity` calls it per case), and
`tests/parity/lock_cells.txt` holds the minimum per test. The ratchet (`parity_ratchet.py --registry
--lock-cells`) fails the Parity Tests job when a lock test is missing, skipped, failed, or compared
fewer cells than its floor, and refuses a registry lock that names a `file#Test` without a floor. The
floor may only grow: adding a test or raising a number is free (a parity fix that adds cells passes
without review friction), lowering or removing one is a weakening. A lock test renamed or moved keeps its
row only if the new name has at least the old floor. The helper file and the ratchet are gate files, the
helper prints its line without `t.Helper` so go test prefixes it with its own file name and line (the ratchet
accepts only that file:line, for the test test2json attributes it to) and carries a per-run random
nonce (`PARITY_LOCK_NONCE`, generated in the workflow step and passed to the test container). **The nonce is
not a security control**: any code in the test process can read the variable and print a matching line. It
only guards against accidental collisions (a stale or copied line). What stops forged cells is two controls: the
content rules below (hard Rule 3: no new `init()`, called package-level `var` or `TestMain` in a lock package, no new
`t.Skip` or build constraint in a lock's test file, no lowered floor), and the required approval of the pull
request, which covers the rest: the report and inline annotations list the lock packages' files for the
reviewer (below).
**Lock packages are reported to the reviewer.** A lock package is any directory that holds a lock-referenced test at the
merge base. In a pure test package (anything under `tests/`: `tests/parity`, `tests/e2e`, ...) a PR that edits or
deletes ANY existing `.go` file is listed in the report's "Lock-affecting files" section (informational, not a
failure: the required approval covers it); so does an edit
or deletion of an existing file of the lock suites' runtime config (`tests/parity/` except `lock_cells.txt`,
`known_failures.txt` and the README, and except a change that only moves the version of an
`image: golang:<version>` line, which changes nothing a lock compares; `deployment/docker/docker-compose-e2e.yml`;
`deployment/docker/lakehouse-e2e-config.yml`; `cmd/datagen/`). In a product package (`internal/...`,
`lakehouse-traces/...`, `cmd/...`) only the existing `_test.go` files are gated: product code is what the locks
verify, so editing it is free. A NEW file there is listed whether it is a test or not (informational; a new product
file is linked into the package's test binary like any other), and an edit that adds an import the package did not have is listed. **A lock package may not gain load-time code (hard, Rule 3).** An `init()`, a `TestMain` or a package-level
`var` whose initializer calls a function runs before any test and can end the test binary with success
(`func init() { if os.Getenv("PARITY_LOCK_NONCE") != "" { os.Exit(0) } }`), silencing every lock of the
package, so the gate fails a PR that adds one to a file of a lock package: a new file, or an existing file
that now holds an `init()` or a called-var the merge base did not. These declarations are compared as a
**multiset per package**, not per file, and the key of a declaration holds what decides what it runs: its exact
Go token stream (`go/scanner`: string and rune literals keep their exact bytes, so `"a b"` and `"a  b"` differ; only
white space, comments and the spelling of automatic semicolons are normalised), the **resolved import path** of every
package qualifier it uses (`schema.F` is `schema` as the alias or `package` clause of which import path; for a package of
this repository the clause is read from the revision's blobs, because the author of a change chooses it, so an
unaliased `tests/evilpkg` that declares `package schema` is not `internal/schema`) together with the dot-imported
paths of the file, and the file's **build constraints** (`//go:build`, `// +build` and the GOOS/GOARCH file-name
suffix: moving a declaration out of an `e2e`-constrained file into an unconstrained one changes whether it runs). Moving
a declaration to another file of the package, or renaming the file that holds it, with the same imports and
constraints adds nothing (the files are still listed for the reviewer), while a new declaration (another body or
initializer, an alias for another package, a changed literal, a changed constraint, one more copy; a var is keyed by
its name too, `_` counted per occurrence) fails wherever it sits. A move that also changes the declaration is an
addition: `m_move_changed`, `m_alias_swap`, `m_alias_noalias`, `m_literal_whitespace` and `m_build_constraint` are replay rows. **cgo is load-time code
too** (an `__attribute__((constructor))` in the preamble runs before `main`), so a lock package may not start
importing `"C"` in any `.go` file, test or product, new or edited (hard, Rule 3). In a pure test package that applies to every `.go` file, in a product package to
`_test.go` files (non-test files there are product code and free). A var whose initializer runs no code
is free: a literal table (`var fvPropMaps = []fvPropMap{{...}}`, which #438 added), a conversion to a
predeclared type, a pure builtin (`len`, `make` ...), a func literal that is not called, a bare
declaration, and the two standard-library constructors `regexp.MustCompile` and `errors.New` (when the
file imports the standard-library package of that name; they cannot end the run with success, and #412
added `regexp.MustCompile` vars to a lock helper). A call to anything else counts, including a conversion
to a user-defined type, which looks like a call: wrap those in a function the test calls. Funcs, consts and types are free; moving or removing a var is free; editing a literal table
in place is listed as lock-affecting, not failed. A package that declares a top-level name shadowing a predeclared type or builtin (`func len(...)`) is refused,
because it would turn the trusted `len(x)` into a call of that func. The check is syntactic; the required review
covers what it cannot see. A new file that declares only funcs, consts and
types is listed like every other new file of the package (informational: a parity fix adds its tests in a NEW
file and the PR stays PASS, with the file named for the reviewer). A lock a PR adds itself is not protected yet
(the merge base decides).
Residual risk of the call-graph helper search (it names the reason a file is lock code, depth 4 inside the
lock test's own package, product code excluded): it does not follow func values, calls into other packages, or
chains deeper than 4. The whole-package rule covers the package of every lock test regardless; a helper in a
different package that a lock calls is only reached if that package holds a lock too. Product code in a lock
package is deliberately free, so a lock that compares through product code is protected by the locks being
the check on that code, not by this gate. What a content check can decide stays automatic and hard (Rule 3
below, and the runtime floor): the listing is for the human reviewer.
Every lock names `file#TestName`; a bare file reference is refused for new or changed lock rows, and every
`tests/parity` lock has a floor in `lock_cells.txt`. The runtime floor applies only to `tests/parity` locks (the
job that runs them reports cells); locks elsewhere are covered by the review of their packages' files.
Design scored against "any code change to a lock test fails the gate" (assumed, not measured):
code about 60 lines Go + 90 lines Python, one baseline file; ongoing burden near zero for a parity fix
that only adds cells; failure modes: a floor set too high fails the job (visible at once), a
floor too low is weaker but never silent; scale: one log line per compare, negligible. Measured on the
gate's history (2026-10-07..11): the owner-review failure fired on about 20 files for #483 and #487 each,
and made the toolchain bump #491 unmergeable, with no way to turn it green; it is now a report section.

**Locks are never weakened.** Any PR that fails one of these fails, parity fix or not:
- adds an allowlist entry (renaming one, that is removing and adding the same top-level test, is
  still an addition, and the message says so: keep the old entry name);
- changes an `expect: pass` row in any way that could loosen it: the row deleted, no longer `pass`,
  or ANY field changed other than `title`, `notes`, `description`, `highlight`, `differ_note`,
  `refs.doc`, more `refs.tests`, or `pending` going from true to false. That covers `compare`
  (type, options such as a tolerance, project), `request`, `targets`, `seed`, `layers`, `upstream`
  and `pending` set to true. The one exception is the `perf` block, which is decided per field (see
  "Performance rows" below); A test reference may be replaced only when a tests/parity lock was renamed or moved and the new
  name has at least the old name's cell floor (any other reference, e2e or unit, may not be replaced);
- changes a test file that a lock row references so it may stop running: any edit of the build
  constraint header (everything before the `package` clause, comments included), a newly added
  `t.Skip`, `t.SkipNow` or `t.Skipf` call, a lock's test file new under a name the go tool ignores or
  constrains (leading `_` or `.`, a GOOS/GOARCH suffix), or an edited `TestMain` in a lock's package
  (the runtime floor above catches whatever these static checks cannot);
- adds an `init()` function or a package-level `var` whose initializer calls a function to a file of a
  lock package (a new file or an existing one), or changes a `TestMain` there: load-time code that can
  silence the locks;
- adds `import "C"` (cgo) to any `.go` file of a lock package;
- adds or changes a **symbolic link or submodule** where `go test` or a lock would read it: any link whose name is a
  `.go` or native-source file (`.c`, `.h`, `.s`, `.F` ...) anywhere in the repository, and any link at all inside a
  lock package directory (tests/e2e and tests/parity included), inside a lock-adjacent package (below), or at the lock
  suites' runtime configuration. `go test` compiles a linked `.go` file, and every lock rule reads blobs of regular
  files (git mode 100*), so a link was invisible to all of them; a regular lock file replaced by a link is a failure,
  not a deletion. A link elsewhere (`docs/`) is free. The modes come from `git diff --raw`, and a linked path never
  reaches the Go parsers (a symlinked `_test.go` is a Rule 3 failure, not a tool error);
- adds an `init()`, `import "C"` or a native source file to a **lock-adjacent package** (below);
- lowers or removes `# min-pass: N`, `# min-pass-logs: N` or `# min-pass-traces: N` in `tests/parity/known_failures.txt`
  (raising one is free; the directives are read as the Python ratchet reads them, with an ASCII-digit value, and a
  duplicate or unreadable one fails closed), or adds a `# expected-skip: <test>  # <reason>` line there (a skipped
  parity test fails the run unless the allowlist names it, so an added name loosens the ratchet);
- leaves a control byte other than LF and TAB, a NEL, U+2028, U+2029, a BOM or invalid UTF-8 in the parity
  allowlist or in `tests/parity/lock_cells.txt`. The Go gate splits these files on `\n`; Python's
  `splitlines()` also splits on a lone CR, VT, FF, FS/GS/RS, NEL, U+2028 and U+2029, so a separator inside an existing
  line hid a smuggled entry from the gate while the ratchet counted it. Both readers now refuse those bytes (the
  ratchet reads bytes and splits on LF only; two fixtures are read by the Go and the Python tests:
  `scripts/ci/tests/testdata/parity_data_bytes.json` classifies bytes, and `parity_data_parse.json` pins what each
  reader PARSES from Unicode spaces, Unicode digits, tabs, trailing spaces, CRLF and duplicate keys. The rule it encodes:
  where both readers accept an input they return the same entries, and every one-sided rejection fails closed (a
  rejecting ratchet fails the parity job, a rejecting gate fails the pull request);
- lowers or removes a cell floor in `tests/parity/lock_cells.txt`;
- deletes or moves the allowlist file, or changes the `--allowlist` argument of the parity workflow
  (the allowlist path is read from `.github/workflows/parity.yaml` at both revisions).
There is no bypass for these; the owner merges a deliberate weakening with the administrator bypass.

### Performance rows

The perf rows (`rows/perf/field_metadata.yaml`) hold exact, deterministic counters (`s3_gets`, `s3_bytes`,
`row_groups`, `pages`) measured on generated Parquet files, next to a latency budget (`p50_ms`, `p90_ms`,
`valid`). Any change that alters the bytes of the generated files (a nullable column's definition levels, a new
footer key, a different encoding) moves `s3_bytes` by a few bytes, so the `field-metadata-perf` job goes red until
the rows are updated. Treating every change to a pass row's perf block as a weakened lock made "field-metadata-perf
green" and "registry gate PASS" impossible together (#483 moved 744 rows by +17 to +385 bytes on `s3_bytes` only). The
gate therefore decides a perf-block change of an `expect: pass` row by content, per field, against the **merge base**
(owner rule 2026-10-12: performance must not go down):

| change to a pass row's perf block | verdict |
|---|---|
| any counter or budget lower, or unchanged | free |
| `s3_bytes` higher by at most 1.0 % (inclusive, integer arithmetic) | allowed and **listed** |
| `s3_bytes` higher by more than 1.0 % | Rule 3 FAIL: "perf counter raised beyond 1.0 %" |
| `s3_gets`, `row_groups` or `pages` higher by any amount | Rule 3 FAIL: more requests or more work is never file-layout noise |
| `p50_ms` or `p90_ms` higher by any amount | Rule 3 FAIL: latency is noise-bound, not a structural counter |
| `budget.valid` weaker (fewer iterations, or not exact in every iteration) | Rule 3 FAIL |
| `cell`, `counters.path`, any other key of the perf block | Rule 3 FAIL: a different measurement (as every other field of a pass row) |
| `expect`, `request`, `layers`, `compare`, `refs` removals | unchanged: Rule 3 FAIL, as above |
| a new row | free |
| a deleted pass row | Rule 3 FAIL |

The rule runs per row and per field, so one regressed field fails the PR even when other rows only raise `s3_bytes`
within the allowance. A row that fails is reported by its failures only. The allowed raises are never silent: the
Rule 3 verdict reads "PASS - N perf counters raised within 1.0 % (listed below)" (and the report header with it),
the log lists every one (`perf counter raised: <row id> <field> <old> -> <new> (+x.xx %)`), the Files tab gets one
`::warning file=<rows file>::` annotation per row (at most 10, then "... and N more"), and the report has a section with
the totals, the largest relative increase and the rows. **Limit:** the allowance is taken against main's current
value, so a slow drift of +1.0 % per PR is possible; the gate does not track it, the owner's review of the listed
raises (and the history of the rows file) is the control. A real increase the owner accepts is merged with the
administrator bypass.

When the generated files change, update the counters only (`s3_bytes`, `row_groups`, `pages` from a fresh
`perf_rows.py` matrix) and keep the latency budgets as they are: a regenerated budget is a fresh, noisy measurement and
any increase of it fails.

**The ratchet holds the locks too.** `scripts/ci/parity_ratchet.py --registry
tests/conformance/registry/rows` (the Parity Tests job) reads the rows with `expect: pass`, not
pending, exact-equivalent compare, and requires every `tests/parity/...#Test` they reference to
report `pass` in the run: a skipped, missing or failing lock test fails the job whatever the test
file says. Make the `parity` job a required check so a lock cannot go red or skip unnoticed.

A changed path that contains a control character (a line break, a CR ...) is a hard failure of its own row
("Changed paths"): the route, handler and feature-catalog checks read paths one per line and would not see
`internal/stats/zz<LF>route.go`.

Registry YAML is read the same way by the loader and the gate: one document per file (a `---` second
document is rejected) and booleans spelled `true` or `false` only (`pending: yes` is rejected). The
gate reads the PR as git blobs (a symlinked or submodule registry file is rejected) and diffs with
`--text --no-textconv --no-ext-diff`, so a `.gitattributes` cannot hide a change; `.gitattributes` and
every file directly under `scripts/ci/` are gate files, and a change to an upstream pin in the `Makefile`
is a product change. The parity ratchet runs with `python -I`.

Self-tests: `bash scripts/ci/tests/test_check_registry_touch.sh`,
`bash scripts/ci/tests/gate_replay_test.sh` (the replay harness against a stub gate),
`bash scripts/ci/tests/gate_local_test.sh` (`make registry-gate`),
`bash scripts/ci/tests/test_gate_workflow_emulation.sh` (the workflows' steps in a scratch
repository: PR code, a planted git hook, an edited gate or bootstrap, and an exported
`TESTLINKS_BIN` never run) and `python -m unittest discover -s scripts/ci/tests`.

## CI scope: which checks a change needs

`pr_classify.py --ci-scope --event pull_request` lets CI decide, from the changed files, which checks a
change needs. It prints the five registry-gate keys exactly as before, then seven more:

| key | meaning |
|---|---|
| `ci_class` | `product`, `release-image-pin`, `stack`, `ci`, `deps`, `docs`, `release-metadata` or `mixed-infra` (a report label; it decides nothing) |
| `skip_go` | `true` when no Go-affecting path changed: the Go group (Go tests, lint, build, fuzz, vulnerability scans, Go benchmarks) is not needed |
| `skip_image` | `true` when the release-image build and scan is not needed |
| `skip_parity` | `true` when the parity suite is not needed |
| `force` | jobs, as `<workflow>:<job>,...`, that run although their group is skipped (see below) |
| `ci_reason` | the first path that made it a full run (empty for a scoped run) |
| `ci_detail` | what was verified |

`skip_go=false` always comes with `skip_image=false` and `skip_parity=false`: a product change runs everything.
Workflows read these keys through the `ci-scope` action (see "Opting a workflow in" below); the workflows that have
opted in so far are listed there.

**Fail open.** A path that is not on the explicit infra list is a product path, so a new directory runs
everything until someone adds it. Any error, an unreadable diff, a path that is not clean text, an empty
diff, a workflow edit that cannot be read per job, and any event other than `pull_request` (a push to `main`,
a schedule, a manual dispatch, an empty or unknown value) give the full answer. A union of paths asks for the
union of what each asks for; one product path asks for everything.

**Classes** (the first matching rule wins, in `_ci_path`):

- Product, everything runs: any `.go` file, `go.mod`, `go.sum`, `go.work` or `Makefile` anywhere;
  `internal/`, `cmd/`, `lakehouse-traces/` (except their `README.md` and `RUNBOOK.md`), `patches/` (all of it,
  its `.md` and `.txt` files are read by the build and the tests), `charts/` (except prose outside `templates/`), `alerts/`, `dashboards/`, `.golangci*`; the shipped Dockerfiles; every
  `tests/` path that is not listed below (Go tests, the registry rows and features, the parity locks, the e2e and
  ingest-matrix trees); and the classifier itself and its wiring and fixtures (`pr_classify.py`,
  `check_changelog_pr.py`, `ci_scope_replay_check.py`, `testdata/ci_scope_replay.json`,
  `.github/actions/ci-scope/`), so a change to the classifier never scopes itself.
- `release-image-pin`: a release Dockerfile whose only change is the digest of its `FROM` images
  (`image_pin_only`): the Go group is skipped, the image build and scan and the parity suite run.
- Parity tooling (`Dockerfile.datagen`, `tests/parity/docker-compose.yml`,
  `deployment/docker/Dockerfile.upstream-probe`, `scripts/ci/parity_ratchet.py`,
  `scripts/ci/tests/test_parity_ratchet.py`, `scripts/ci/requirements.txt`): `skip_parity=false`.
- `.github/codeql/` (the CodeQL configuration) forces `codeql.yaml:analyze`, the Go analysis that reads it.
- `stack`, `ci`, `deps`, `docs`: `deployment/`, `scripts/`, the non-Go test tooling (`tests/readers/`,
  `tests/s3compat/compose/`, `tests/playwright/`, `tests/verification/`), `.github/` and documentation. The
  stack workflows (e2e, readers, s3-compat, proof, minio images, docs site) keep their own `paths:` filters.
- `release-metadata`: the exact shape the registry gate checks (`release_metadata()`: CHANGELOG move,
  `Chart.yaml` versions, generated docs naming, an approved author, the tag). It is decided from the content
  and the author, never from the title.

**Workflow edits** to `ci.yaml`, `fuzz-stress-memleak.yaml`, `security.yaml`, `codeql.yaml` and `parity.yaml`
are judged per job from the diff: a change that only moves the ref of `uses:` lines (the same actions removed
and added, any ref) forces nothing; a changed line inside `jobs.<job>` forces that job; a changed line outside
every job (`on:`, `env:`, `permissions:`) forces every job of the workflow.

**Scripts that a gated job runs** (`CI_CONSUMERS`) force those jobs when they change.

**Adding a path.** Add it to the rule that fits in `_ci_path` (or `CI_CONSUMERS` when a gated job runs it),
add a case to `PathTable` in `scripts/ci/tests/test_pr_classify_ci_scope.py` and regenerate the recorded
merges if the answer for one of them changes.

Tests: `python -m unittest scripts/ci/tests/test_pr_classify_ci_scope.py` (the path table, workflow and
Dockerfile edits on temporary repositories, the unchanged registry-gate keys, and 70 recorded merges replayed
from their file lists), and `python scripts/ci/tests/ci_scope_replay_check.py` (the same 70 merges on the real
git objects; it needs the full history and runs in `gate-selftest.yaml`). `scripts/ci/ci_scope_replay_gen.py`
regenerates the recorded file; its header explains how.

### Opting a workflow in

`.github/actions/ci-scope` is a local composite action that runs the classifier of the **merge base** (never the
one of the pull request, so a change to the classifier cannot decide its own scope) against the checked-out
change, and exposes `skip_go`, `skip_image`, `skip_parity` and `force`. Any error, an unknown base, a classifier
that predates the flag or prints something unexpected, a push to `main`, a schedule and a manual dispatch give a
full run (all `skip_*` false). The repository variable `CI_SCOPE=full` also gives a full run for every pull request;
it is read only as an environment variable, and there is no label or text override. The action checks nothing out and
needs `contents: read` only.

A workflow opts in with one `scope` job and one `if:` per job it lets the scope skip:

```yaml
jobs:
  scope:
    runs-on: ubuntu-latest
    timeout-minutes: 5
    permissions:
      contents: read
    env:
      CI_SCOPE: ${{ vars.CI_SCOPE }}
    outputs:
      skip_go: ${{ steps.scope.outputs.skip_go }}
      force: ${{ steps.scope.outputs.force }}
    steps:
      - uses: actions/checkout@<sha> # v7.0.1
        with:
          fetch-depth: 0          # the merge base needs the history
          persist-credentials: false
      - id: scope
        uses: ./.github/actions/ci-scope
        with:
          base-sha: ${{ github.event.pull_request.base.sha }}
          author: ${{ github.event.pull_request.user.login }}

  fuzz-logs:
    needs: [scope]
    if: ${{ !cancelled() && (needs.scope.outputs.skip_go != 'true' || contains(format(',{0},', needs.scope.outputs.force), ',fuzz-stress-memleak.yaml:fuzz-logs,')) }}
```

Why this exact form (`test_ci_scope_workflows.py` fails on any other):

- `!= 'true'` (never `== 'false'`): a job is skipped only on an explicit `true`, so an empty, missing or garbled
  output runs it.
- `!cancelled()` (never `always()`, never missing): a job whose `needs` failed or was skipped is skipped unless its
  `if:` lets it continue, and a skipped job reads as passing; so a failed `scope` job must run the job, while a
  cancelled run stays cancelled.
- The comma-delimited `contains(format(',{0},', ...), ',<workflow file>:<job id>,')` matches whole `force` entries
  (a substring such as `test-logs` inside `test-logs-x` does not match). The job id and the file name are the ones
  the classifier prints in `force`.
- The `image` and `parity` groups use the same form with `skip_image` or `skip_parity`.
- A scope output is read only in a job-level `if:`: not in a step `if:`, `env`, `with` or `run`.

Rules that keep the required checks working (also enforced by the test, with the required contexts listed in
`scripts/ci/tests/testdata/required_checks.txt`, which mirrors the branch protection of `main`):

- A job that reports a required check never gets a scope `if:` and never `needs` the `scope` job. A skipped job
  reports success, so gating a required job would let it pass without running.
- No `paths:` or `branches:` filter on a workflow that produces a required check or that is meant to run on every
  pull request: a workflow skipped by a filter leaves its checks pending.
- `force` lists jobs, not their `needs:` ancestors. A gated job that `needs` another gated job must list the same
  guard on the ancestor (or the ancestor must not be gated): the ancestor's skip would otherwise skip the job.
  The lint checks both directions: every `needs` names a job of the workflow, an ungated job never needs a gated
  one, and a gated job needs only ungated jobs or jobs gated by the same group. The shared setup job (`deps` in
  `ci.yaml` and `security.yaml`) is never gated.
- Job names and matrix legs do not change. A skipped matrix job is shown as one unexpanded check, which is one
  more reason no required matrix job is ever gated.
- A job that never ran on a push (the benchmark smoke runs) keeps that: its `if:` is the same form behind
  `github.event_name == 'pull_request' && `, and the lint accepts exactly that prefix and no other.

Opted in so far:

- `fuzz-stress-memleak.yaml`: the memleak, stress and fuzz jobs (`fuzz-matrix-drift` always runs).
- `ci.yaml`: the heavy jobs that are not required checks (`test-parquets3-heavy`, `storage-health-compaction`,
  `parity-unit`, `field-metadata-perf`, `parquet-readback`, `security-logs`, `security-traces`,
  `benchmarks-logs`, `benchmarks-traces`). `deps`, `helm-config-drift`, `bench-selftest` and every required check
  (`helm`, `test-*`, `lint-*`, `build-*`, `docker-*`) always run. Each gated job needs `[scope, deps]`, so after a
  failed `deps` job (and no skip) it still runs and fails on the missing tree instead of being skipped. The test
  checks that each script a gated job runs is in `CI_CONSUMERS` (an edit to it forces the job).

**Known gap (not caused by the scope, not fixed here).** The required jobs that `need: deps` (in `ci.yaml`: `lint-*`,
`test-*`, `build-*`, `docker-*`; in `security.yaml`: `govulncheck`, `gosec`) are skipped when `deps` fails, and a
skipped job reads as passing. `DepsHoleKnownGap` in `test_ci_scope_workflows.py` lists them and fails when the list
is stale; the fix (a `!cancelled()` condition plus a step that fails when `deps` did not succeed) changes required
jobs and is a separate change.

Tests: `python -m unittest scripts/ci/tests/test_ci_scope_workflows.py` (the lint and its fixtures, the repository
workflows, and the action's script run as the runner runs it in temporary git repositories).

## Release decision (non-product merges, `[skip release]`, and merges whose run was cancelled)

A merge to `main` that changes nothing that ships is never released, so the marker below is no longer
needed for it (it is still honoured). The decision is made on the range `last-tag..HEAD`, not on the
pushed commit alone (#756): release runs are serialised by a concurrency group, GitHub keeps one pending
run per group, and the run of a middle merge of a burst is cancelled, so a head-only decision lost the
product merge #736 behind a CI-only and a test-only merge (2026-10-23). `scripts/ci/release_product_check.py`
runs in the workflow's `skip` step, after the version step (which computes `LATEST_TAG`, the highest
`vMAJOR.MINOR.PATCH` tag), and releases when ANY commit in the range is owed a release:

- it has no `[skip release]` marker in its subject or in the title of the pull request that produced it
  (`scripts/ci/release_skip.py`; the title is read from the API only for a commit that would otherwise
  release), and
- it is a product change, asked per commit to the registry gate's own classifier
  (`scripts/ci/pr_classify.py`, taken from the commit's parent) with the rules below.

The marker of the pushed commit therefore never hides an earlier owed commit. Release-metadata commits
(`chore: release metadata for vX [skip release]`) carry the marker, so a release's own metadata merge
never releases and never loops. The first run after a tag, a re-run of a finished run and a run of an
older commit see an empty range (HEAD is the tag's commit or an ancestor of it) and release nothing.
A revert of a product commit is a product commit and is released (an extra release, never a lost one).
Not released: docs, CI workflows, tests, scripts, test-stack
compose files and Dockerfiles, tooling locks (pip, npm) and Dependabot bumps of them, release metadata.
Released although the gate waives them, because they rebuild the binaries or images: `go.mod`, `go.sum`,
`go.work` of the root and `lakehouse-traces` modules (Go toolchain and dependency bumps), a changed
`FROM` digest or tag in `Dockerfile`, `Dockerfile.logs` or `Dockerfile.traces` (a digest *added* to a
line that had none pins what the tag already resolves to and is not released; a digest that changes
or goes away is), and the upstream pins in `auto-release.yaml`. Every doubt releases: a manual run
(`workflow_dispatch`, never skipped, also on a commit that is already tagged), a merge commit, an empty
diff, a classifier error, no usable tag, a tag that is not an ancestor of HEAD, more than 100 commits
since the tag. The one fail-closed case is a pull-request title that cannot be read for a commit that
would otherwise release: no release, the run turns red, start the workflow by hand; another owed commit
in the range still releases. The version bump (patch, minor, ...) is still read from the labels and
subject of the pushed commit's pull request, not from the commits in the range. Known gap: other edits of the build steps in
`auto-release.yaml` (not the pins) are treated as CI. Tests: `python -m unittest
scripts.ci.tests.test_release_product_check` (fixture histories, including the exact #736 sequence).
