# Subsystem A — Config Parity Audit Phase 2-3 + Drift CI + Auto-Sync

**Status:** Design

**Date:** 2026-06-02

**Continues:** PR #107 (Phase 1 — extraction & comparison matrix, merged)

**Successor work:** Subsystems B/C/D/E (API parity, Parquet format, direct analytics, community standards)

---

## Goal

Close the configuration drift between `internal/config/config.go`, `docs/configuration.md`, and `charts/victoria-lakehouse/values.yaml`, then prevent recurrence with a code-as-source-of-truth pipeline enforced by CI.

This subsystem must:
1. Resolve every gap inventoried in `docs/superpowers/specs/config-comparison-matrix.md` (113 non-MATCH settings out of 167 total unique settings across all three sources).
2. Establish a mechanical source-of-truth via Go struct tags + a centralized registry.
3. Generate `docs/configuration.md`, `values.yaml` doc-comments, and `config-schema.json` from code.
4. Gate every PR with a CI workflow that fails on configuration drift.

**Counting note:** PR #107's description counts 178 code-side settings (every const + struct field). The matrix dedupes to 141 logical code settings (some PR #107 entries map to the same setting via different code paths). The matrix's 167 total covers all three sources unioned (141 code + 26 Helm-only K8s patterns). This spec uses **167 unique settings** as the canonical count for coverage assertions; the 178 figure is reserved for the per-field registry coverage test.

---

## Verified Gap Inventory (from PR #107)

The Phase 1 comparison matrix produced these counts (executive summary, 167 total settings):

| Status      | Count | % of total | Resolution tier |
|-------------|-------|------------|-----------------|
| MATCH       | 54    | 32%        | None — already aligned |
| MISMATCH    | 8     | 5%         | **Tier 2** — research required |
| MISSING     | 91    | 54%        | **Tier 1** — auto-fill from code |
| UNIT_DIFF   | 2     | 1%         | **Tier 2** — pick canonical format |
| PARTIAL     | 6     | 4%         | **Tier 2** — document profile variants |
| OVERRIDE    | 6     | 4%         | **Tier 3** — intentional divergence, document rationale |
| **Total non-MATCH** | **113** | **68%** | |

### Tier 2 Critical Mismatches (require research with ≥3 sources)

The 8 MISMATCH settings the matrix flags as critical:

| Setting | Code | Docs | Helm | Significance |
|---------|------|------|------|--------------|
| `insert.flush_interval` | `60s` | `10s` | `30s` | Write latency vs S3 PUT cost trade-off |
| `query.file_workers` | `64` | `8` | `8` | 8× query parallelism difference |
| `prefetch.max_concurrent` | `8` | `4` | `4` | Cache prefetch concurrency |
| `prefetch.max_queue` | `128` | `64` | `64` | Prefetch queue depth |
| `startup.max_resync_time` | `10m` | (omit) | `2m` | K8s readiness vs cautious resync |
| `shutdown.flush_timeout` | `30s` | (omit) | `15s` | terminationGracePeriodSeconds alignment |
| `compaction.enabled` | `true` | `false` (default; profiles enable) | `true` | Background load significantly affected |
| `s3.bucket` | (required) | (required) | `""` (minLength: 1) | Validation contract differs |

### Tier 2 UNIT_DIFF (2 settings)

- `logs.bloom_columns` — Code: JSON array `["service.name", "trace_id"]`; Docs: CSV `"service.name,trace_id"`. Canonical: JSON array (functional source).
- `storage_classes` — Code: JSON array `["STANDARD"]`; Docs: shorthand `"STANDARD"`. Canonical: JSON array.

### Tier 2 PARTIAL (6 settings — profile variants needing explicit documentation)

- `cache.memory_limit` — fixed `512MB` in code; varies `64MB-2GB` by profile in docs.
- `cache.disk_limit` — fixed `50GB` in code; varies `1GB-100GB` by profile.
- `insert.compression_level` — fixed `7`; varies `1-11` by profile.
- `bloom.enabled` — fixed `true`; profiles disable for max-cost-savings/dev.
- `compaction.enabled` (also in MISMATCH above) — fixed `true`; profiles disable.
- `tenant.stats_enabled` — fixed `true`; varies by profile.

### Tier 3 OVERRIDE (6 settings — intentional Helm-only K8s patterns)

K8s operational settings that have no code-side counterpart and live only in Helm:
- `image.*` (registry, tag, pullPolicy)
- `resources.*` (requests, limits per container)
- `securityContext.*`, `serviceAccount.*`
- `service.*` (type, port, annotations)
- `ingress.*` (enabled, host, tls)
- `podDisruptionBudget.*`

These remain Helm-only. Registry marks them `Scope: HelmOnly`. They are documented in a dedicated "K8s Operational Settings" appendix in `configuration.md`, not in the main settings tables.

### Tier 1 MISSING (91 settings) — auto-fill from code

Code has a setting documented; docs lag. No research required. Generator emits these from the registry. Major categories with high doc gap:
- 16+ tenant settings missing
- 9+ startup/shutdown settings missing
- 8+ peer/replication settings missing
- 6 telemetry settings entirely undocumented
- K8s-style request/limit/scaling triples (15+ settings)

### Cross-source gap totals (from raw detail rows, not exec summary)

The detail tables show 105 rows marked MISMATCH and 85 rows marked MISSING. The executive summary collapses related groups (e.g., K8s request/limit/scaling triples count as one logical setting). This spec treats the executive summary numbers as canonical for tiering, but the migration must touch all detail rows.

**Coverage discrepancy noted:** code defines 141 settings, Helm defines 167, docs cover 89. The Phase 1 matrix flagged this as the central problem.

---

## Architecture

Hybrid source-of-truth pattern:
- **Trivial settings** (single default, no profile variation, short description) declare metadata via Go struct tags on existing `*Config` types in `internal/config/`.
- **Complex settings** (profile variants, long-form guidance, multiple validation rules) live in a new `internal/config/registry.go` file.

A generator binary (`cmd/config-gen/`) walks both via `go/ast` and reflection. It produces four artifacts:
1. `docs/configuration.md` — full user-facing documentation (replaces hand-edited version).
2. `charts/victoria-lakehouse/values.yaml` doc-comments only — operator-edited *values* preserved between sentinel markers, *comments* regenerated.
3. `charts/victoria-lakehouse/config-schema.json` — JSON Schema Draft 2020-12 for IDE autocomplete and external validation.
4. `internal/config/registry-fingerprint.json` — hash of registry state used by CI to detect uncommitted regeneration.

A CI workflow (`.github/workflows/config-parity.yaml`) runs three jobs on every PR:
- `registry-completeness` — every struct field has a tag or registry entry (no orphans, no uncovered fields).
- `regenerate-and-diff` — runs the generator from a clean checkout, diffs against committed files. Drift = PR fails.
- `drift-summary` — Step Summary listing any mismatch with remediation hint (`run make config-gen and commit`).

### Resolution Policy (Tiered)

| Tier | What it covers | Action | Cost |
|------|----------------|--------|------|
| **Tier 1** | 91 MISSING entries | Auto-fill `docs/configuration.md` from code via generator | Zero — mechanical |
| **Tier 2** | 8 MISMATCH + 2 UNIT_DIFF + 6 PARTIAL = 15 distinct settings (compaction.enabled appears in both MISMATCH and PARTIAL — counted once) | Research with ≥3 sources, decide canonical value, apply to all three surfaces | ~8 hours total |
| **Tier 3** | 6 OVERRIDE (K8s-only) | Document as `Scope: HelmOnly` in registry, emit in "K8s Operational Settings" appendix only | ~30 min |

---

## Components

### `internal/config/registry.go` (new)

Single file declaring rich-metadata settings:

```go
package config

import "time"

type Tier int

const (
    Tier1 Tier = iota + 1 // trivial doc gap (auto-fill)
    Tier2                 // research-driven decision
    Tier3                 // K8s-only / intentional divergence
)

type Scope int

const (
    ScopeAll      Scope = iota // emitted to docs, helm, schema
    ScopeHelmOnly              // emitted to helm appendix only
    ScopeCodeOnly              // internal tuning, omitted from docs/helm
)

type Entry struct {
    Name      string         // "insert.flush_interval"
    Default   any            // 60 * time.Second
    Type      Type           // TypeDuration, TypeInt, TypeBool, ...
    Category  string         // "Ingestion", "Cache", ...
    Tier      Tier
    Scope     Scope
    Doc       string         // markdown body
    Range     string         // "5s - 30m"
    HelmPath  string         // "insert.flushInterval"
    Profiles  map[string]any // {"development": 10*time.Second, "production": 60*time.Second}
    Rationale string         // required for Tier2: why this value was chosen
    Sources   []string       // required for Tier2: at least 3 evidence references
}

var Registry = []Entry{
    {
        Name: "insert.flush_interval",
        Default: 60 * time.Second,
        Type: TypeDuration,
        Category: "Ingestion",
        Tier: Tier2,
        Doc: "Maximum time before buffered data flushes to S3...",
        Range: "5s - 30m",
        HelmPath: "insert.flushInterval",
        Profiles: map[string]any{
            "development": 10 * time.Second,
            "production":  60 * time.Second,
        },
        Rationale: "60s balances S3 PUT cost vs query freshness; see benchmark 2026-05-20-phase1.",
        Sources: []string{
            "benchmark://docs/superpowers/specs/2026-05-20-phase1-instrumentation-baselines.md",
            "prod://lakehouse-prod-eu observed PUT cost reduction 65% at 60s vs 10s",
            "upstream://VictoriaLogs default 30s, but VL stores locally with no S3 cost",
        },
    },
    // ... ~30-40 entries (one per Tier 2 + complex Tier 3 entry)
}
```

### Struct tag annotations on `*Config` types (additive)

For trivial settings only:

```go
type CacheConfig struct {
    MemoryLimit string `cfg:"name=cache.memory_limit,default=512MB,doc=L1 in-memory cache size,helm=cache.memory.limit,category=Cache"`
    DiskLimit   string `cfg:"name=cache.disk_limit,default=50GB,doc=L2 disk cache size,helm=cache.disk.limit,category=Cache"`
}
```

Tag schema:
- `name` (required) — dot-notation setting name matching docs/Helm path semantics.
- `default` (required) — string-encoded default value.
- `doc` (required) — one-line description.
- `helm` (optional) — Helm key path; defaults to camelCase of `name`.
- `category` (required) — for table grouping in `configuration.md`.
- `range` (optional) — valid range string.

If a setting needs anything beyond the above (profile variants, multi-paragraph docs, multiple sources), it migrates from tag to registry.

### `cmd/config-gen/` (new)

Single Go binary, three subcommands:

```
config-gen docs    # writes docs/configuration.md (full overwrite)
config-gen helm    # writes values.yaml doc-comments (sentinel-bounded, preserves values)
config-gen schema  # writes config-schema.json
config-gen all     # runs all three sequentially
```

Each subcommand:
1. Walks `internal/config/*.go` via `go/ast` to enumerate struct fields and read `cfg:` tags.
2. Loads `internal/config/registry.go` via reflection on the `Registry` variable.
3. Validates completeness (every struct field has tag XOR registry entry).
4. Sorts entries by category, then name (deterministic output).
5. Renders templated output.

### `internal/config/registry_test.go` (new)

Enforces invariants at unit-test time:
- `TestRegistryCoversAllStructFields` — walks every exported field of every `*Config` type; asserts each has a tag or registry entry.
- `TestNoOrphanRegistryEntries` — each registry entry's `Name` resolves to a real struct field.
- `TestTier2EntriesHaveEvidence` — every Tier 2 entry has non-empty `Rationale` and `len(Sources) >= 3`.
- `TestProfilesAreSubsetOfValid` — profile keys ⊆ `{development, staging, production, max-performance, max-cost-savings, max-durability}`.
- `TestNoDuplicateNames` — no two entries share a `Name`.
- `TestRangeFormatValid` — `Range` strings parse as `<min> - <max>` with consistent units.
- `TestHelmPathsExistInValuesYaml` — every `HelmPath` resolves to a real key in `values.yaml` (after registry-driven Helm regeneration).

### `.github/workflows/config-parity.yaml` (new)

```yaml
name: Config Parity
on:
  pull_request:
    paths:
      - 'internal/config/**'
      - 'docs/configuration.md'
      - 'charts/victoria-lakehouse/values.yaml'
      - 'charts/victoria-lakehouse/config-schema.json'
      - 'cmd/config-gen/**'
      - '.github/workflows/config-parity.yaml'
  push:
    branches: [main]

jobs:
  registry-completeness:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version-file: go.mod }
      - run: go test ./internal/config/ -run 'TestRegistry|TestNoOrphan|TestTier2|TestProfiles|TestNoDuplicate|TestRange|TestHelmPaths'

  regenerate-and-diff:
    runs-on: ubuntu-latest
    needs: registry-completeness
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version-file: go.mod }
      - run: go run ./cmd/config-gen all
      - run: |
          if ! git diff --quiet; then
            echo "::error::Config drift detected. Run 'make config-gen' locally and commit the changes."
            git diff
            exit 1
          fi
```

---

## Data Flow

### Developer adds or modifies a setting

1. Edit `internal/config/<file>.go` — add/change struct field.
2. If trivial, add/update `cfg:` tag inline. If complex (profile variants, multi-source rationale), add/update entry in `registry.go`.
3. Run `make config-gen` locally → regenerates `docs/configuration.md`, `values.yaml` comments, `config-schema.json`.
4. Commit all changes together (struct + tag/registry + generated files).
5. CI runs `config-parity.yaml`:
   - `registry-completeness` ensures struct fields and registry/tags are in sync.
   - `regenerate-and-diff` re-runs the generator from a clean checkout and diffs.
6. If drift, developer runs `make config-gen` locally and commits the regenerated files.

### Phase 2-3 one-time migration

Tasks 4-9 from PR #107's plan map to PRs A1-A6:

1. **Task 4 — Classify gaps** (PR A1): Walk `config-comparison-matrix.md`. Tag every gap as Tier 1/2/3 in `docs/superpowers/specs/config-gap-classification.md`.
2. **Task 5 — Research best practices** (PR A2): For each Tier 2 setting, collect ≥3 sources. Output `docs/superpowers/specs/config-best-practices.md`.
3. **Task 6 — Recommendations** (PR A3): Convert research into proposed canonical values. Output `docs/superpowers/specs/config-recommendations.md`. **User reviews before A4.**
4. **Task 7 — Registry + generator** (PR A4): Implement `registry.go`, struct tags for all 178 settings, `cmd/config-gen/`. Generator runs but does not change docs yet (output committed verbatim as new baseline).
5. **Task 8 — Apply Tier 2 fixes** (PR A5): For each approved Tier 2 recommendation, update code default + registry rationale. Re-run generator. Update CHANGELOG.
6. **Task 9 — Wire CI + final verification** (PR A6): Add `config-parity.yaml` workflow. Intentionally introduce drift, confirm CI fails. Land.

### Profile variants

Registry entries with non-nil `Profiles` map produce documentation showing:
- Base default (used when no profile is selected).
- Per-profile override table.

In `configuration.md`, a "Profile defaults" subsection per category lists profile-varying settings. In `values.yaml` comments, profile-bound settings cite the active profile assumed. In JSON schema, profile-bound settings use `oneOf` enum or `$comment` to document the variation.

---

## Error Handling

| Condition | Generator behavior | CI behavior |
|-----------|-------------------|-------------|
| Registry entry references non-existent struct field | Exits 1: `orphan registry entry: <name>` | PR fails with link to entry |
| Struct field lacks tag and registry entry | Exits 1: `uncovered field: <pkg>.<field>` | PR fails with file:line |
| Tag malformed (missing required key) | Exits 1: `malformed tag at <file>:<line>` | PR fails |
| Tier 2 entry missing `Rationale` or `<3 Sources` | Exits 1: `incomplete tier-2 entry: <name>` | PR fails |
| `values.yaml` sentinel damaged | Exits 1: refuses to overwrite that section | PR fails with remediation step |
| Profile key not in valid set | Exits 1: `unknown profile: <name>` | PR fails |
| Duplicate registry `Name` | Exits 1: `duplicate name: <name>` | PR fails |

### `values.yaml` sentinel-based merging

The generator only touches lines between `# BEGIN config-gen:<name>` and `# END config-gen:<name>` markers around each setting:

```yaml
# BEGIN config-gen:insert.flushInterval
# Maximum time before buffered data flushes to S3.
# Default: 60s. Range: 5s - 30m. Profile-varying.
# See: docs/configuration.md#insertflushinterval
# END config-gen:insert.flushInterval
insert:
  flushInterval: 60s
```

If an operator deletes the sentinels, the generator refuses to regenerate that section. CI then fails with a message: `damaged sentinel for <name>; restore by re-cloning values.yaml from origin/main and re-applying your value changes`.

### Determinism guarantees

Generator output must be byte-identical across runs. Risks:
- Map iteration order → sort all map keys before emission.
- Time-of-day in headers → header includes only commit SHA, not timestamps.
- Go version differences → CI pins Go version to `go.mod`.

A `TestDocsGenerationDeterministic` unit test runs the generator twice and asserts byte equality.

### Backward compatibility

Existing `docs/configuration.md` is fully replaced in PR A4. The diff is enormous (91 new entries + reformatting). Strategy: land the migration in a single `[generated]` commit with reviewers diffing structure, not content.

---

## Testing Strategy

### Unit tests (`internal/config/registry_test.go`)

Enforces all invariants listed under Components above.

### Generator tests (`cmd/config-gen/gen_test.go`)

- `TestDocsGenerationDeterministic` — output byte-identical across runs.
- `TestHelmCommentMergePreservesValues` — pre-populate `values.yaml` with operator-edited value, run helm generator, assert value preserved but comment refreshed.
- `TestSchemaValidatesExampleValues` — generate schema, validate a known-good config (must pass) and known-bad config (must fail).
- `TestProfileVariantsInDocs` — entry with profile map produces a documented profile table.
- `TestSentinelRecoveryFailsLoudly` — damaged sentinel causes refusal with clear message.

### Integration tests (`tests/config-parity/`)

- `TestCIWorkflowDetectsAddedField` — add a struct field without tag/registry, run the registry-completeness step via `act`, assert it fails with `uncovered field` message.
- `TestCIWorkflowDetectsValueDrift` — change a default in code without regenerating, assert CI catches it.
- `TestCIWorkflowDetectsBrokenSentinel` — damage a `values.yaml` sentinel, assert CI flags it.

### Regression tests for Tier 2 settings (15 distinct settings)

- `TestTier2DefaultsMatchResearch` — asserts each Tier 2 setting's code default matches the value documented in `config-recommendations.md`. Prevents accidental revert. Test table is generated from registry entries with `Tier: Tier2`, so adding a new Tier 2 setting automatically extends coverage.

### Negative-control proofs (per harden-and-lock rule)

Each load-bearing assertion includes a comment naming the negative control:

```go
// negative control: comment out the registry-completeness CI step → this test must fail
// when a new field is added without a tag or registry entry.
func TestRegistryCoversAllStructFields(t *testing.T) { ... }
```

---

## Migration Plan & Deliverables

Six PRs landed sequentially. Each independently mergeable; each leaves the repo green.

| PR | Scope | Files | Risk |
|----|-------|-------|------|
| **A1** | Gap classification | `docs/superpowers/specs/config-gap-classification.md` (new) — every gap from PR #107 tagged Tier 1/2/3 with rationale | Zero — docs only |
| **A2** | Tier 2 research | `docs/superpowers/specs/config-best-practices.md` (new) — 15 distinct Tier 2 settings × ≥3 sources × evidence | Zero — docs only |
| **A3** | Recommendations | `docs/superpowers/specs/config-recommendations.md` (new) — proposed canonical values; **user reviews before A4** | Zero — docs only, gate for code changes |
| **A4** | Registry + generator | `internal/config/registry.go`, `cmd/config-gen/{main,docs,helm,schema}.go`, `internal/config/registry_test.go`, `Makefile` target, regenerated `docs/configuration.md`, `values.yaml`, `config-schema.json` (no value changes yet — just structural sync) | Medium — new infrastructure, no behavior change |
| **A5a** | Apply low-risk Tier 2 fixes | Code default changes for UNIT_DIFF (2) + PARTIAL (6) settings. No behavior change for runtime defaults; only canonical-format and profile-variant documentation. | Medium — format normalization |
| **A5b** | Apply MISMATCH Tier 2 fixes | Code default changes for the 8 MISMATCH settings (only those approved in A3), regenerated artifacts. CHANGELOG entry under `### Changed`. Per-setting rollback flags. | **High** — behavior changes; reviewed individually, gated by A5 Safety Plan below |
| **A6** | CI enforcement | `.github/workflows/config-parity.yaml`, `tests/config-parity/` integration tests | Low — gates future drift |

### A5 Safety Plan — "Nothing missed, nothing broken"

The migration must change defaults for 16 Tier 2 settings without breaking existing deployments or silently dropping settings. A5 ships in two stages (A5a then A5b) and enforces these safeguards:

**1. Per-setting verification matrix (mandatory artifact)**

Before A5a/A5b lands, `docs/superpowers/specs/config-tier2-verification.md` lists every Tier 2 setting in a table:

| Setting | Old default | New default | Rollback flag/env | Regression test | E2E test exercising path | Performance impact (benchmark ID) |
|---------|------------|-------------|-------------------|-----------------|--------------------------|-----------------------------------|
| `insert.flush_interval` | 60s | (decided in A3) | `LH_LEGACY_FLUSH_INTERVAL=60s` env | `TestInsertFlushAtNewDefault` | `e2e/ingestion_flush_test.go` | `BenchmarkInsertThroughput` |
| ... 15 more rows ... | | | | | | |

A row may have `Rollback: N/A` only if both `Regression test` and `E2E test exercising path` rows are populated. Empty cells anywhere in the table block A5 from merging.

**2. Per-setting rollback mechanism**

Every Tier 2 setting change ships with one of:
- A boolean flag (`-lakehouse.<setting>.legacy=true`) that restores the old default for one minor release.
- An env var (`LH_LEGACY_<SETTING>=<old_value>`) for operators who hit unexpected regressions.
- An explicit "no rollback — old value was a bug" entry in CHANGELOG, with linked evidence in `config-best-practices.md`.

Rollback hooks live in `internal/config/legacy.go` (new). They are deleted in the release **two** after A5b lands (giving operators one full release cycle to migrate). A CI step in A6 enforces "every legacy flag has a documented removal milestone."

**3. Coverage assertion test (catches missed settings)**

`TestEveryGapAddressed` in `internal/config/migration_test.go`:
- Parses `docs/superpowers/specs/config-comparison-matrix.md` and extracts every row tagged MISMATCH, UNIT_DIFF, PARTIAL, OVERRIDE, MISSING.
- Asserts every extracted setting has a corresponding registry entry OR struct tag with explicit `Tier` value.
- Fails the build with `unaddressed gap: <name> (tier: ?)` if any setting from PR #107's matrix is not classified and resolved.

This test is the **single mechanical gate** that proves nothing from PR #107's inventory was forgotten.

**4. E2E smoke regression (full stack)**

`make e2e` runs before A5a and after A5b lands. Required checks:
- Insert path: 10k logs ingested successfully with default config.
- Query path: representative LogsQL queries return expected row counts.
- Flush path: data appears in S3 within `2 × flush_interval` (validates new value).
- Compaction path: at least one compaction completes within the e2e window if `compaction.enabled` flips.
- All 4 resource bounds report non-zero `acquired_total` counters (validates wiring still works after default churn).

E2E results recorded in `docs/superpowers/specs/config-tier2-e2e-evidence.md`. A5b cannot merge until this file is committed with run links.

**5. Performance regression gates**

`make bench` runs `BenchmarkInsertThroughput`, `BenchmarkQueryHotPath`, `BenchmarkColdQueryRangeRead` before A5a and after A5b. Per-benchmark thresholds:
- > 10% throughput regression → A5b blocked, requires justification or revert.
- > 20% latency increase at p95 → A5b blocked.
- Bench results committed to `docs/superpowers/specs/config-tier2-perf-evidence.md`.

**6. CHANGELOG explicit-call-out**

A5b's CHANGELOG entry under `### Changed` lists every default change as a bullet with old value, new value, and migration guidance:

```markdown
### Changed
- `insert.flush_interval` default 60s → 30s. Restores parity with Helm chart and aligns with VictoriaLogs upstream default. **Operator action:** if your deployment relies on 60s batching to control S3 PUT cost, set `-lakehouse.insert.flush-interval=60s` explicitly or set env `LH_LEGACY_INSERT_FLUSH_INTERVAL=60s`. See [config-recommendations.md](docs/superpowers/specs/config-recommendations.md#insertflush_interval).
```

**7. Pre-merge checklist (PR template)**

A5a and A5b PR descriptions must check off:
- [ ] Every Tier 2 setting present in `config-tier2-verification.md`.
- [ ] Every cell in the verification matrix populated (no empty cells).
- [ ] `TestEveryGapAddressed` passing.
- [ ] E2E evidence committed (`config-tier2-e2e-evidence.md`).
- [ ] Performance evidence committed (`config-tier2-perf-evidence.md`).
- [ ] CHANGELOG entry lists every change with rollback instructions.
- [ ] `internal/config/legacy.go` rollback hooks present for all behavior changes.

A5a and A5b cannot be force-merged. Requires green check on all of the above.

---

## Definition of Done

### Coverage (no setting missed)

- [ ] All exported struct fields across `internal/config/*.go` (178 PR #107 count, 141 logical-setting count after dedupe) registered via struct tag XOR registry entry.
- [ ] All 167 unique settings from `config-comparison-matrix.md` classified into Tier 1/2/3 (verified by `TestEveryGapAddressed`).
- [ ] 91 Tier 1 MISSING settings filled in `docs/configuration.md`.
- [ ] 8 Tier 2 MISMATCH settings resolved with documented rationale and ≥3 sources each.
- [ ] 2 Tier 2 UNIT_DIFF settings normalized to canonical format.
- [ ] 6 Tier 2 PARTIAL settings have profile variants documented.
- [ ] 6 Tier 3 OVERRIDE settings documented in "K8s Operational Settings" appendix.

### Infrastructure (drift prevention)

- [ ] `make config-gen && git diff --quiet` is clean (drift-free baseline).
- [ ] CI workflow blocks PRs that introduce drift (verified by intentional drift test in A6).
- [ ] `config-schema.json` validates a known-good example and rejects a known-bad one.
- [ ] Negative-control test comments present on every load-bearing assertion.

### A5 safety gates (no behavior broken)

- [ ] `config-tier2-verification.md` complete: every Tier 2 setting has old value, new value, rollback mechanism, regression test, E2E test, perf benchmark ID.
- [ ] `internal/config/legacy.go` ships rollback flags/env vars for every behavior change.
- [ ] `TestEveryGapAddressed` passes (mechanical proof no PR #107 gap was forgotten).
- [ ] `config-tier2-e2e-evidence.md` committed with green run links from A5a and A5b.
- [ ] `config-tier2-perf-evidence.md` committed: no benchmark regresses > 10% throughput or > 20% p95 latency.
- [ ] `CHANGELOG.md` `### Changed` section lists every Tier 2 default change with rollback instructions.
- [ ] Pre-merge checklist (Safety Plan §7) is fully checked on both A5a and A5b PRs.

### Operational (operator preserved)

- [ ] `values.yaml` operator-edited values preserved across regeneration (sentinel merging verified).
- [ ] Helm chart still passes existing `helm lint` and `helm template` smoke tests.
- [ ] Existing e2e stack (`make e2e`) passes end-to-end without changes to deployment manifests.

---

## Out of Scope

Deferred to other subsystems:
- API parity tests (Subsystem B).
- Parquet schema validation (Subsystem C).
- Cross-system query testing on Parquet (Subsystem D).
- OpenTelemetry semantic conventions / community standards alignment (Subsystem E).

Deferred indefinitely:
- Full mechanical generation of `values.yaml` values (not just comments). Considered in brainstorming, rejected to preserve operator editing workflow. Re-evaluate after A5 if drift in values proves problematic.
- Auto-resolution of Tier 2 mismatches by majority vote (e.g., "two of three sources agree"). Each Tier 2 mismatch gets human-reviewed rationale; mechanical voting would ship the wrong answer when production data contradicts upstream defaults.

---

## Open Questions

None — all decisions resolved during brainstorming:
- Source of truth: hybrid struct tags + central registry (Option C).
- Generation scope: docs + Helm comments + JSON schema (Option 2).
- Resolution policy: tiered severity (Option 3).
- Scope ceiling: finish + drift CI + auto-sync (Option 3).
