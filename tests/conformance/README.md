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
  (`scripts/ci/check_registry_touch.sh` enforces it).
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
- Flag rows (`kind: flag`) declare `compare: { type: status }` with no `request`: they
  document that a flag exists and matters, not a specific HTTP call. They stay
  declarative only until the runner exercises actual flag-variant stacks.

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
