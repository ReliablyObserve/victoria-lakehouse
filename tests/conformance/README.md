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
the release workflow moves the `[Unreleased]` bullets under the new version heading and does
not touch the catalog, so a recorded "unreleased" would go stale the moment a feature ships.
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
`CHANGELOG.md`, adds or removes a route, handler or `lakehouse.*` flag registration in non-test
Go under `internal/`, `cmd/` or `lakehouse-traces/`, adds a YAML config key to a struct in
`internal/config/`, or adds a new `lh.*` registry row — and fails it unless
`tests/conformance/registry/features/**` also changed and the generated documents are current.
The first three are set comparisons rather than diff greps, so moving text is never mistaken
for a new capability: a bold bullet under `### Fixed` or `### Changed`, the release workflow
moving the `[Unreleased]` bullets under a version heading, and a registration or config field
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

## Registry gate on every PR

Owner rule (2026-10-07): no PR merges unless its behaviour changes and its tests are covered by
registry changes. `scripts/ci/check_registry_touch.sh` enforces it, on top of the route/pin and
feature-catalog rules above.

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
- A PR that changes the gate takes effect once merged, and until then it is held to the owner
  exemption (Rule 4).

**Owner setup (what code cannot do).** Require these checks on `main`: `conformance-inventory`
(the heavy checks; it no longer holds the gate), `registry-gate`, `registry-gate-base` and the Parity
Tests job `parity` (it always runs and skips quickly when nothing parity-relevant changed). Pin
`registry-gate-base` to the workflow on `main` with a ruleset ("Require workflows to pass before
merging", path `.github/workflows/registry-gate-base.yaml`, branch `main`) and enforce it for
administrators. Only the repository owner applies `registry-exempt`; automation never does. Until
`registry-gate-base` is pinned, a PR that rewrites `registry-gate.yaml` can neutralise that one
workflow (the base-run workflow judges it as soon as it exists).

*Canary check after the setup (a throwaway PR each):* (1) change one line of `internal/` code and no
registry file: `registry-gate` and `registry-gate-base` must both fail; (2) edit
`.github/workflows/registry-gate.yaml` to `exit 0` in a PR: `registry-gate-base` must still fail,
and the PR must show both checks as required; (3) add `registry-exempt` as a non-approver: the gate
must stay red; as the owner, after the last push: green; push again: red.

All the actions in the two gate workflows are pinned by full commit SHA.

**Rule 1 — product change needs a registry change.** A PR is product-changing when it changes any
file that is not a test under `internal/`, `cmd/` or `lakehouse-traces/` (Go, embedded UI assets,
SQL, YAML; only `README.md` and `RUNBOOK.md` are documentation), any file under `patches/` or
`charts/` (their Markdown and text docs excepted, templates included), or a shipped build file:
`Dockerfile`, `Dockerfile.logs`, `Dockerfile.traces`, and the root `go.mod` and `go.sum`
(`lakehouse-traces/go.mod` is covered by its tree). That includes the config defaults:
`internal/config/config.go` (`Default()`), `internal/config/profile.go`, the flag defaults in
`cmd/*/main.go` and `charts/victoria-lakehouse/values*.yaml`. A test is `*_test.go`, anything under
`testdata/`, and `test_*` / `*_test` shell and Python scripts. There is no generated-file
exclusion: the product trees hold none, and a marker comment must not switch the gate off. PRs that
touch only tests, `Makefile`, `scripts/`, `.github/`, docs or other Dockerfiles are not
product-changing. A product-changing PR must add, remove or change a row or feature by more than
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
`tests/e2e` and `tests/parity`, so it needs the same exemption volume. All numbers are estimates
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

**Exempt without ceremony** (decided before any label is read):
- release-metadata PRs. The files are exactly `CHANGELOG.md`, `charts/victoria-lakehouse/Chart.yaml`
  (required), `README.md` (optional badge) and the regenerated `docs/features.md` (optional;
  `UPSTREAM_COVERAGE.md` and the inventory carry no version naming and may not change). Each file
  is held to its release shape:
  - `CHANGELOG.md`: exactly one new `## [x.y.z] - date` section directly below `[Unreleased]`, with
    x.y.z newer than the newest section. Text may move only between `[Unreleased]`, the new section
    and the previously newest section (a merge of main into the metadata branch moves bullets
    there); every older section is byte-identical, and no line may be lost, added, edited or
    duplicated.
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
- dependency-only PRs: only `go.mod`, `go.sum` and `requirements*.txt`, every commit `build(deps…)`
  or `chore(deps…)` (at least one commit), and in `go.mod` only `require` version lines change (no
  `replace`, `go`, `toolchain` or other directive). A bump of a storage-critical module
  (`github.com/VictoriaMetrics/*`, `github.com/parquet-go/*`, `github.com/aws/*`, quoted or not)
  is never dependency-only: it needs registry coverage or the owner's exemption;
- docs-only and CI-only PRs (not product-changing).
Upstream pin and patch changes keep the stricter route rule above.

**Exemption by the owner.** A PR with genuinely nothing to cover (a pure refactor, say) is
exempted by the label `registry-exempt` plus a line starting `Registry: none — <reason>` in the
PR body (the line may sit in a Markdown quote). Both are required: the label alone fails. **Only
the owner applies the label, and the gate enforces it, in one run only:** the gate honours the
label in the run its own `labeled` event triggers, when the label is `registry-exempt` and the
sender (`github.event.sender.login`) is listed in `.github/registry-exempt-approvers` (read from the
merge base, so a PR cannot add itself). Every other run (a push, an edit, another label, a re-run)
denies it, so an approval can never outlive the head it was given for: the owner applies the label
after the last push, and re-applies it (remove, add) after any later push. There is no timeline
lookup and no token needed. It fails closed: a missing approvers file or sender denies the
exemption, and a local run (no labeled event) fails with a message. The workflows re-run on
`labeled`, `unlabeled` and `edited`, one run per PR (a newer run cancels the one in flight). The
exemption skips Rules 1-4, not the route/pin and feature-catalog rules.

**Rule 4 — the gate itself.** A PR that changes any file directly under `scripts/ci/` (so a new
file that could shadow an import counts too), `.gitattributes`, `tests/parity/lock_cells_test.go`,
`cmd/testlinks`, `registry/testlinks.go`, `registry/paritygate.go`,
`.github/workflows/{conformance,registry-gate,registry-gate-base,parity}.yaml` or
`.github/registry-exempt-approvers` fails unless it carries the owner's verified exemption (the
other checks still run and report too). `tests/parity/lock_cells.txt` is not a blanket gate file: a
parity fix adds floors freely, and lowering or removing one is a weakening. `.github/CODEOWNERS`
lists the owner for `.github/`, `scripts/ci/` and `tests/conformance/`.

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
without the owner), lowering or removing one is a weakening. A lock test renamed or moved keeps its
row only if the new name has at least the old floor. The helper file and the ratchet are gate files, the
helper prints its line without `t.Helper` so go test prefixes it with its own file name (the ratchet
accepts only that prefix, for the test that printed it), and a change to the `TestMain` of any package
that holds a lock needs the owner. A bare `tests/parity/<file>_test.go` reference holds every top-level
test of that file to passing (no count).
Design scored against "any code change to a lock test needs the owner" (assumed, not measured):
code about 60 lines Go + 90 lines Python, one baseline file; ongoing burden near zero for a parity fix
that only adds cells, against one owner review per edit of a lock test under the alternative; failure
modes: a floor set too high fails the job (visible at once, fixed by lowering it with the owner), a
floor too low is weaker but never silent; scale: one log line per compare, negligible.

**Locks are never weakened.** Any PR that fails one of these fails, parity fix or not:
- adds an allowlist entry (renaming one, that is removing and adding the same top-level test, is
  still an addition, and the message says so: keep the old entry name or ask the owner);
- changes an `expect: pass` row in any way that could loosen it: the row deleted, no longer `pass`,
  or ANY field changed other than `title`, `notes`, `description`, `highlight`, `differ_note`,
  `refs.doc`, more `refs.tests`, or `pending` going from true to false. That covers `compare`
  (type, options such as a tolerance, project), `request`, `targets`, `seed`, `layers`, `upstream`
  and `pending` set to true. A test reference may be replaced only when a tests/parity lock was renamed or moved and the new
  name has at least the old name's cell floor (any other reference, e2e or unit, needs the owner);
- changes a test file that a lock row references so it may stop running: any edit of the build
  constraint header (everything before the `package` clause, comments included), a newly added
  `t.Skip`, `t.SkipNow` or `t.Skipf` call, a lock's test file new under a name the go tool ignores or
  constrains (leading `_` or `.`, a GOOS/GOARCH suffix), or an edited `TestMain` in a lock's package
  (the runtime floor above catches whatever these static checks cannot);
- lowers or removes a cell floor in `tests/parity/lock_cells.txt`;
- deletes or moves the allowlist file, or changes the `--allowlist` argument of the parity workflow
  (the allowlist path is read from `.github/workflows/parity.yaml` at both revisions).
Only the owner's exemption (above) lets one through.

**The ratchet holds the locks too.** `scripts/ci/parity_ratchet.py --registry
tests/conformance/registry/rows` (the Parity Tests job) reads the rows with `expect: pass`, not
pending, exact-equivalent compare, and requires every `tests/parity/...#Test` they reference to
report `pass` in the run: a skipped, missing or failing lock test fails the job whatever the test
file says. Make the `parity` job a required check so a lock cannot go red or skip unnoticed.

Registry YAML is read the same way by the loader and the gate: one document per file (a `---` second
document is rejected) and booleans spelled `true` or `false` only (`pending: yes` is rejected). The
gate reads the PR as git blobs (a symlinked or submodule registry file is rejected) and diffs with
`--text --no-textconv --no-ext-diff`, so a `.gitattributes` cannot hide a change; `.gitattributes` and
every file directly under `scripts/ci/` are gate files, and a change to an upstream pin in the `Makefile`
is a product change. The parity ratchet runs with `python -I`.

Self-tests: `bash scripts/ci/tests/test_check_registry_touch.sh`,
`bash scripts/ci/tests/test_gate_workflow_emulation.sh` (the workflows' steps in a scratch
repository: PR code, a planted git hook, an edited gate or bootstrap, and an exported
`TESTLINKS_BIN` never run) and `python -m unittest discover -s scripts/ci/tests`.

## Release skip (`[skip release]`)

`auto-release.yaml` does not release a push whose commit subject, or whose merged pull request's
title, contains `[skip release]` (`scripts/ci/release_skip.py`). The PR is looked up from the
pushed commit (`GET /commits/{sha}/pulls`), so squash, merge and rebase merges all work; a job
re-run re-reads the title. If the lookup fails three times the push is not released, the run
summary says why and the run turns red; a manual `workflow_dispatch` run (main only, never
skipped, optional `pr` input for labels and size) releases. The loki-vl-proxy sync probe commit
carries the marker itself; a product follow-up commit pushed on that sync PR would not release
either, so give such a PR its own title without the marker.
