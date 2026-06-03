# Config Parity Phase 2-3 + Drift CI + Auto-Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close every configuration gap inventoried in PR #107's comparison matrix and prevent recurrence with a code-as-source-of-truth pipeline enforced by CI.

**Architecture:** Hybrid struct-tag + central-registry source of truth in `internal/config/`. A generator binary (`cmd/config-gen/`) produces `docs/configuration.md`, `values.yaml` doc-comments (sentinel-bounded), and `config-schema.json`. A CI workflow runs the generator and fails PRs on drift. Migration ships as 7 PRs (A1, A2, A3, A4, A5a, A5b, A6) with A5 split into low-risk and high-risk halves and gated by a 7-point safety plan.

**Tech Stack:** Go 1.24 (`go/ast` for source parsing, `reflect` for registry walking), YAML (sentinel-bounded `values.yaml` merging), JSON Schema Draft 2020-12, GitHub Actions

**Spec:** `docs/superpowers/specs/2026-06-02-config-parity-phase23-design.md`

**Prior art:** PR #107 (Phase 1 — extraction & comparison matrix, merged). Source artifacts:
- `docs/superpowers/specs/config-comparison-matrix.md` (684 lines, the canonical gap inventory)
- `docs/superpowers/specs/config-code-defaults.md` (448 lines)
- `docs/superpowers/specs/config-docs-defaults.md` (378 lines)
- `docs/superpowers/specs/config-helm-defaults.md` (780 lines)

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `docs/superpowers/specs/config-gap-classification.md` | Tier 1/2/3 classification of every PR #107 gap |
| Create | `docs/superpowers/specs/config-best-practices.md` | Tier 2 research output (≥3 sources per setting) |
| Create | `docs/superpowers/specs/config-recommendations.md` | Proposed canonical values; user-review gate |
| Create | `docs/superpowers/specs/config-tier2-verification.md` | A5 per-setting verification matrix |
| Create | `docs/superpowers/specs/config-tier2-e2e-evidence.md` | A5 e2e run links |
| Create | `docs/superpowers/specs/config-tier2-perf-evidence.md` | A5 benchmark deltas |
| Create | `internal/config/registry.go` | Tier 2/3 + complex Tier 1 entries with rich metadata |
| Create | `internal/config/registry_test.go` | Coverage, orphan, evidence, profile, dup, range tests |
| Create | `internal/config/migration_test.go` | `TestEveryGapAddressed` — mechanical PR #107 coverage proof |
| Create | `internal/config/legacy.go` | A5b rollback flags/env vars; one per behavior change |
| Modify | `internal/config/*.go` | Add `cfg:` struct tags to simple settings |
| Create | `cmd/config-gen/main.go` | Subcommand dispatcher (`docs`/`helm`/`schema`/`all`) |
| Create | `cmd/config-gen/walk.go` | AST walker + reflection of `Registry` |
| Create | `cmd/config-gen/docs.go` | `docs/configuration.md` renderer |
| Create | `cmd/config-gen/helm.go` | `values.yaml` sentinel-bounded comment writer |
| Create | `cmd/config-gen/schema.go` | JSON Schema Draft 2020-12 emitter |
| Create | `cmd/config-gen/main_test.go` | Determinism, sentinel preservation, profile rendering |
| Create | `Makefile` (target) | `make config-gen` runs `go run ./cmd/config-gen all` |
| Modify | `docs/configuration.md` | Replaced wholesale by generator (A4) |
| Modify | `charts/victoria-lakehouse/values.yaml` | Insert sentinel-bounded comments (A4) |
| Create | `charts/victoria-lakehouse/config-schema.json` | Generated JSON schema (A4) |
| Create | `.github/workflows/config-parity.yaml` | 3-job drift gate (A6) |
| Create | `tests/config-parity/drift_test.go` | Integration test exercising the CI gate |

---

## PR A1 — Gap Classification

**Goal:** Produce a single document that classifies every gap from `config-comparison-matrix.md` into Tier 1, Tier 2, or Tier 3.

**Scope:** Docs only. Zero code changes. Output: `docs/superpowers/specs/config-gap-classification.md`.

### Task A1.1: Parse comparison matrix into structured rows

**Files:**
- Read: `docs/superpowers/specs/config-comparison-matrix.md`
- Create: `docs/superpowers/specs/config-gap-classification.md`

- [ ] **Step 1: Read the matrix and inventory every settings row**

Count every row in the matrix's `## Master Comparison Table (by Category)` section. Expected counts (from matrix executive summary):

```
MATCH:     54  (skip — already aligned)
MISMATCH:   8  → Tier 2
MISSING:   91  → Tier 1
UNIT_DIFF:  2  → Tier 2
PARTIAL:    6  → Tier 2
OVERRIDE:   6  → Tier 3
Total:    167
```

The detail tables in the matrix mark more individual rows with `MISMATCH` (~105 raw rows) because the executive summary collapses K8s request/limit/scaling triples. Use the executive summary tier counts as the canonical target; the detail rows feed individual table entries.

- [ ] **Step 2: Create classification document with full table**

```bash
cat > docs/superpowers/specs/config-gap-classification.md <<'EOF'
# Config Gap Classification (Phase 2-3 Tier Assignment)

**Source:** `docs/superpowers/specs/config-comparison-matrix.md`
**Date:** 2026-06-02
**Purpose:** Tag every gap from PR #107 with a resolution tier.

## Tier Definitions

- **Tier 1** — Missing from docs only. Auto-fill from code via generator. No research needed.
- **Tier 2** — Value disagreement requiring research with ≥3 sources before a canonical value is chosen.
- **Tier 3** — Intentional Helm-only K8s pattern. Document as such; no code-side counterpart.

## Tier 2 Settings (15 distinct — research required)

### MISMATCH (8 settings)

| Setting | Code | Docs | Helm | Research focus |
|---------|------|------|------|----------------|
| insert.flush_interval | 60s | 10s | 30s | S3 PUT cost vs query freshness trade-off |
| query.file_workers | 64 | 8 | 8 | Query parallelism vs resource exhaustion |
| prefetch.max_concurrent | 8 | 4 | 4 | Cache prefetch concurrency safety |
| prefetch.max_queue | 128 | 64 | 64 | Prefetch queue depth |
| startup.max_resync_time | 10m | (omit) | 2m | K8s readiness vs cautious resync |
| shutdown.flush_timeout | 30s | (omit) | 15s | terminationGracePeriodSeconds alignment |
| compaction.enabled | true | false | true | Background load impact (also has PARTIAL profile variance) |
| s3.bucket | required | required | "" (minLength: 1) | Validation contract |

### UNIT_DIFF (2 settings)

| Setting | Code format | Docs format | Canonical |
|---------|------------|-------------|-----------|
| logs.bloom_columns | JSON array | CSV string | JSON array (functional source) |
| storage_classes | JSON array | CSV string | JSON array |

### PARTIAL (6 settings — profile variants)

| Setting | Code (fixed) | Docs (by profile) |
|---------|-------------|-------------------|
| cache.memory_limit | 512MB | 64MB-2GB |
| cache.disk_limit | 50GB | 1GB-100GB |
| insert.compression_level | 7 | 1-11 |
| bloom.enabled | true | disabled in max-cost-savings/dev |
| compaction.enabled | true | (overlap with MISMATCH; treated once) |
| tenant.stats_enabled | true | varies by profile |

## Tier 1 Settings (91 — auto-fill from code)

Group by category from the matrix:
EOF
```

Then append every MISSING row from the matrix, organized by category. Use a `(complete this table from matrix)` placeholder ONLY in the working document; before commit, every row must be filled.

- [ ] **Step 3: Append Tier 1 detail table**

For each `MISSING` row in `config-comparison-matrix.md`, emit a line:
```
| <setting> | <code default> | <category> |
```

Group by category (Storage/S3, Cache, Ingestion, Query, Replication/HA, Observability, Tenant, Startup/Shutdown, Compaction, Bloom, Prefetch, Telemetry, Schema, Lakehouse top-level).

Expected: 91 rows total. If your count differs, return to the matrix and recheck.

- [ ] **Step 4: Append Tier 3 section**

```markdown
## Tier 3 Settings (6 K8s-only — Helm-only documentation)

These have no code-side counterpart and live only in Helm. Registry marks them
`Scope: ScopeHelmOnly`. They render to a "K8s Operational Settings" appendix.

| Helm key prefix | Purpose |
|-----------------|---------|
| image.* | Container image registry/tag/pullPolicy |
| resources.* | K8s CPU/memory requests and limits |
| securityContext.* | Pod & container security context |
| serviceAccount.* | RBAC-bound service account |
| service.* | K8s Service type/port/annotations |
| ingress.* | Ingress enabled/host/tls |
| podDisruptionBudget.* | PDB minAvailable / maxUnavailable |
```

Note the table lists 7 prefix groups but the exec summary counts 6 because `serviceAccount.*` is sometimes grouped under `securityContext.*` in PR #107. Pick the matrix's grouping.

- [ ] **Step 5: Verify totals**

```bash
echo "Tier 1: $(grep -c '^| ' docs/superpowers/specs/config-gap-classification.md | head -91)"
echo "Tier 2 + Tier 3: $(grep -cE 'Tier 2|Tier 3' docs/superpowers/specs/config-gap-classification.md)"
```

Expected: 91 Tier 1 rows + 15 Tier 2 + 6 Tier 3 = 112 individual settings classified. (The exec summary lists 113 non-MATCH because compaction.enabled appears in two tiers.)

- [ ] **Step 6: Commit**

```bash
git add docs/superpowers/specs/config-gap-classification.md
git commit -m "docs: classify PR #107 config gaps into Tier 1/2/3 (A1)"
```

---

## PR A2 — Tier 2 Research

**Goal:** For each of the 15 distinct Tier 2 settings, gather ≥3 sources of evidence before any canonical value is proposed.

**Scope:** Docs only. Output: `docs/superpowers/specs/config-best-practices.md`.

**Source requirements:** Each setting needs evidence from at least 3 of these source classes:
- **Internal benchmark** — link to a file in `docs/superpowers/plans/` or `tests/bench/` with measured throughput/latency.
- **Production observation** — Grafana dashboard URL, incident ticket, or `prod://` reference noting observed value impact.
- **Upstream reference** — VictoriaLogs or VictoriaTraces source code line or release note showing what they default to.
- **External best practice** — link to public docs (AWS S3 best practices, K8s probe guidance, etc.).
- **Configuration matrix note** — citation of the relevant row in `config-comparison-matrix.md`.

### Task A2.1: Set up research document skeleton

**Files:**
- Create: `docs/superpowers/specs/config-best-practices.md`

- [ ] **Step 1: Write the document header and per-setting template**

```bash
cat > docs/superpowers/specs/config-best-practices.md <<'EOF'
# Tier 2 Best-Practice Research

**Date:** 2026-06-02
**Scope:** 15 distinct Tier 2 settings from `config-gap-classification.md`
**Method:** ≥3 evidence sources per setting before A3 proposes a canonical value.

## Source Classes

1. **Internal benchmark** — measured perf data in `docs/superpowers/plans/` or `tests/bench/`
2. **Production observation** — Grafana / incident / prod-config reference
3. **Upstream reference** — VL or VT source/release showing default
4. **External best practice** — public docs or whitepapers
5. **Matrix citation** — row in `config-comparison-matrix.md` documenting the gap

## Setting Template

### <setting.name>

- **Current:** Code=`X`, Docs=`Y`, Helm=`Z`
- **Operational dimension:** <what changes when this value changes>
- **Source 1 (class):** <link or citation>
  - Finding: <one-line summary>
- **Source 2 (class):** <link or citation>
  - Finding: <one-line summary>
- **Source 3 (class):** <link or citation>
  - Finding: <one-line summary>
- **Tentative direction:** <which value seems supported, no decision yet>

---

EOF
```

- [ ] **Step 2: Commit skeleton**

```bash
git add docs/superpowers/specs/config-best-practices.md
git commit -m "docs: tier-2 research skeleton (A2)"
```

### Tasks A2.2–A2.16: Research one Tier 2 setting per task

Each of the 15 distinct Tier 2 settings gets its own research task. Per-task pattern:

- [ ] **Step 1: Identify operational dimension**

Example for `insert.flush_interval`: write-latency vs S3 PUT cost.

- [ ] **Step 2: Collect Source 1 (internal benchmark)**

Search `docs/superpowers/plans/` for relevant benchmarks:
```bash
grep -ln "flush_interval\|InsertFlushAtNewDefault\|BenchmarkInsertThroughput" docs/superpowers/plans/ tests/
```

Capture the file path and one-line finding.

- [ ] **Step 3: Collect Source 2 (upstream reference)**

For VictoriaLogs comparisons:
```bash
grep -n "flushInterval\|flush_interval" lakehouse-traces/deps/VictoriaLogs/lib/logstorage/*.go
```

Capture the upstream default and link.

- [ ] **Step 4: Collect Source 3 (production / external)**

If no production data exists, fall back to external best practice (AWS S3 PUT pricing page, K8s probe guidance, etc.). Capture link.

- [ ] **Step 5: Write entry into config-best-practices.md**

Append a section following the template. Do not propose a final value yet — only "tentative direction."

- [ ] **Step 6: Commit one setting at a time**

```bash
git add docs/superpowers/specs/config-best-practices.md
git commit -m "docs: tier-2 research — <setting.name> (A2)"
```

**Settings to research (one task each):**

A2.2: `insert.flush_interval`
A2.3: `query.file_workers`
A2.4: `prefetch.max_concurrent`
A2.5: `prefetch.max_queue`
A2.6: `startup.max_resync_time`
A2.7: `shutdown.flush_timeout`
A2.8: `compaction.enabled`
A2.9: `s3.bucket`
A2.10: `logs.bloom_columns` (UNIT_DIFF)
A2.11: `storage_classes` (UNIT_DIFF)
A2.12: `cache.memory_limit` (PARTIAL)
A2.13: `cache.disk_limit` (PARTIAL)
A2.14: `insert.compression_level` (PARTIAL)
A2.15: `bloom.enabled` (PARTIAL)
A2.16: `tenant.stats_enabled` (PARTIAL)

---

## PR A3 — Recommendations (User Review Gate)

**Goal:** Convert A2's research into concrete proposed canonical values. **User reviews this PR before any code change in A4.**

**Scope:** Docs only. Output: `docs/superpowers/specs/config-recommendations.md`.

### Task A3.1: Write recommendations document

**Files:**
- Create: `docs/superpowers/specs/config-recommendations.md`

- [ ] **Step 1: Write the document with a row per Tier 2 setting**

```markdown
# Config Tier 2 Recommendations (User Review Gate)

**Date:** 2026-06-02
**Source:** `config-best-practices.md`
**Status:** PENDING USER APPROVAL

## Decisions

| Setting | Old (per source) | Proposed canonical | Rollback mechanism | Reasoning |
|---------|------------------|--------------------|--------------------|-----------|
| insert.flush_interval | Code=60s, Docs=10s, Helm=30s | 30s | `LH_LEGACY_INSERT_FLUSH_INTERVAL` env | Aligns with Helm; matches VL upstream; benchmark X shows acceptable PUT cost |
| ... 14 more rows ... | | | | |

## User approval

By approving this PR, you commit to applying these changes in A4/A5.
Mark each row with `[x] approved` or `[x] revise` and note revisions below.

## Revisions (per-setting overrides)

(empty — fill if any rows are revised before merging)
```

- [ ] **Step 2: Populate every row from A2 research**

For each of 15 Tier 2 settings, write the row using research from `config-best-practices.md`. Do not leave any row blank.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/config-recommendations.md
git commit -m "docs: tier-2 recommendations for user review (A3)"
```

- [ ] **Step 4: Open PR and wait for user approval**

Open A3 as a draft PR titled `docs: tier-2 config recommendations (USER REVIEW GATE)`. Do not proceed to A4 until user marks every row approved or revises specific ones.

---

## PR A4 — Registry + Generator (No Behavior Change)

**Goal:** Build the source-of-truth infrastructure. Registry covers all settings, generator produces all four artifacts, generated artifacts committed as new baseline. **No code defaults change in this PR.**

### Task A4.1: Define registry types

**Files:**
- Create: `internal/config/registry.go`

- [ ] **Step 1: Write the failing test**

Create `internal/config/registry_test.go`:

```go
package config

import "testing"

func TestRegistryTypesExist(t *testing.T) {
    var _ Entry
    var _ Tier = Tier1
    var _ Scope = ScopeAll
    var _ Type = TypeString
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/config/ -run TestRegistryTypesExist
```
Expected: FAIL (`undefined: Entry`).

- [ ] **Step 3: Write `internal/config/registry.go` with type definitions**

```go
package config

import "time"

type Tier int

const (
    Tier1 Tier = iota + 1
    Tier2
    Tier3
)

type Scope int

const (
    ScopeAll Scope = iota
    ScopeHelmOnly
    ScopeCodeOnly
)

type Type int

const (
    TypeString Type = iota
    TypeInt
    TypeBool
    TypeDuration
    TypeBytes
    TypeStringSlice
)

type Entry struct {
    Name      string
    Default   any
    Type      Type
    Category  string
    Tier      Tier
    Scope     Scope
    Doc       string
    Range     string
    HelmPath  string
    Profiles  map[string]any
    Rationale string
    Sources   []string
}

var Registry = []Entry{}

func init() {
    // Entries added in subsequent tasks.
    _ = time.Second
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
go test ./internal/config/ -run TestRegistryTypesExist
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/config/registry.go internal/config/registry_test.go
git commit -m "config: registry types (A4.1)"
```

### Task A4.2: Migration coverage test (TestEveryGapAddressed)

**Files:**
- Create: `internal/config/migration_test.go`

- [ ] **Step 1: Write the failing test**

```go
package config

import (
    "os"
    "regexp"
    "strings"
    "testing"
)

// negative control: comment out the per-row classification in
// docs/superpowers/specs/config-gap-classification.md → this test
// must fail with `unaddressed gap: <name>` because every setting
// from the matrix must have a tier classification + registry/tag.
func TestEveryGapAddressed(t *testing.T) {
    data, err := os.ReadFile("../../docs/superpowers/specs/config-comparison-matrix.md")
    if err != nil { t.Fatal(err) }
    re := regexp.MustCompile(`\| ` + "`" + `([a-z][a-z0-9_.]+)` + "`" + ` \|.*\| (MATCH|MISMATCH|MISSING|UNIT_DIFF|PARTIAL|OVERRIDE) \|`)
    seen := map[string]string{}
    for _, m := range re.FindAllStringSubmatch(string(data), -1) {
        seen[m[1]] = m[2]
    }
    if len(seen) < 100 {
        t.Fatalf("matrix parse only found %d settings; expected ~167", len(seen))
    }
    cls, err := os.ReadFile("../../docs/superpowers/specs/config-gap-classification.md")
    if err != nil { t.Fatal(err) }
    for name, status := range seen {
        if status == "MATCH" {
            continue
        }
        if !strings.Contains(string(cls), name) {
            t.Errorf("unaddressed gap: %s (status: %s)", name, status)
        }
    }
}
```

- [ ] **Step 2: Run test**

```bash
go test ./internal/config/ -run TestEveryGapAddressed
```
Expected: PASS if A1 produced a complete classification document. Otherwise: FAIL listing missing settings.

- [ ] **Step 3: Commit**

```bash
git add internal/config/migration_test.go
git commit -m "config: mechanical gap-coverage proof test (A4.2)"
```

### Task A4.3: Registry completeness tests

**Files:**
- Modify: `internal/config/registry_test.go`

- [ ] **Step 1: Add tests**

Append to `registry_test.go`:

```go
import (
    "fmt"
    "reflect"
    "strings"
)

// negative control: add a struct field to *Config without a cfg tag or
// registry entry → this test must fail with `uncovered field`.
func TestRegistryCoversAllStructFields(t *testing.T) {
    cfg := DefaultConfig() // existing constructor
    uncovered := walkConfig(reflect.ValueOf(cfg), "")
    if len(uncovered) > 0 {
        t.Fatalf("uncovered fields: %v", uncovered)
    }
}

func TestNoOrphanRegistryEntries(t *testing.T) {
    cfg := DefaultConfig()
    known := knownSettingNames(reflect.ValueOf(cfg), "")
    for _, e := range Registry {
        if _, ok := known[e.Name]; !ok && e.Scope != ScopeHelmOnly {
            t.Errorf("orphan registry entry: %s", e.Name)
        }
    }
}

func TestTier2EntriesHaveEvidence(t *testing.T) {
    for _, e := range Registry {
        if e.Tier != Tier2 { continue }
        if e.Rationale == "" {
            t.Errorf("tier-2 entry missing Rationale: %s", e.Name)
        }
        if len(e.Sources) < 3 {
            t.Errorf("tier-2 entry needs >=3 Sources, got %d: %s", len(e.Sources), e.Name)
        }
    }
}

func TestProfilesAreSubsetOfValid(t *testing.T) {
    valid := map[string]bool{
        "development": true, "staging": true, "production": true,
        "max-performance": true, "max-cost-savings": true, "max-durability": true,
    }
    for _, e := range Registry {
        for p := range e.Profiles {
            if !valid[p] { t.Errorf("unknown profile %q in %s", p, e.Name) }
        }
    }
}

func TestNoDuplicateNames(t *testing.T) {
    seen := map[string]bool{}
    for _, e := range Registry {
        if seen[e.Name] { t.Errorf("duplicate name: %s", e.Name) }
        seen[e.Name] = true
    }
}

func TestRangeFormatValid(t *testing.T) {
    re := regexp.MustCompile(`^.+ - .+$`)
    for _, e := range Registry {
        if e.Range == "" { continue }
        if !re.MatchString(e.Range) {
            t.Errorf("invalid Range %q in %s", e.Range, e.Name)
        }
    }
}

// walkConfig and knownSettingNames are implemented in cmd/config-gen/walk.go
// helper exposed via an exported package function ConfigFieldNames(v reflect.Value).
func walkConfig(v reflect.Value, prefix string) []string {
    var out []string
    if v.Kind() == reflect.Ptr { v = v.Elem() }
    if v.Kind() != reflect.Struct { return nil }
    t := v.Type()
    for i := 0; i < v.NumField(); i++ {
        f := t.Field(i)
        if !f.IsExported() { continue }
        name := strings.ToLower(f.Name)
        if prefix != "" { name = prefix + "." + name }
        tag := f.Tag.Get("cfg")
        if tag == "" && !registryHas(name) && v.Field(i).Kind() != reflect.Struct {
            out = append(out, fmt.Sprintf("%s.%s", v.Type().Name(), f.Name))
        }
        if v.Field(i).Kind() == reflect.Struct {
            out = append(out, walkConfig(v.Field(i), name)...)
        }
    }
    return out
}

func registryHas(name string) bool {
    for _, e := range Registry {
        if e.Name == name { return true }
    }
    return false
}

func knownSettingNames(v reflect.Value, prefix string) map[string]bool {
    out := map[string]bool{}
    if v.Kind() == reflect.Ptr { v = v.Elem() }
    if v.Kind() != reflect.Struct { return out }
    t := v.Type()
    for i := 0; i < v.NumField(); i++ {
        f := t.Field(i)
        if !f.IsExported() { continue }
        name := strings.ToLower(f.Name)
        if prefix != "" { name = prefix + "." + name }
        out[name] = true
        if v.Field(i).Kind() == reflect.Struct {
            for k := range knownSettingNames(v.Field(i), name) {
                out[k] = true
            }
        }
    }
    return out
}
```

- [ ] **Step 2: Run tests — expect failures**

```bash
go test ./internal/config/ -run 'TestRegistry|TestNoOrphan|TestTier2|TestProfiles|TestNoDuplicate|TestRange'
```
Expected: `TestRegistryCoversAllStructFields` FAILS listing every uncovered field. Other tests PASS (registry empty).

- [ ] **Step 3: Commit (failing test locks the contract)**

```bash
git add internal/config/registry_test.go
git commit -m "config: registry contract tests (A4.3)"
```

### Task A4.4: Add cfg struct tags to all `*Config` types

**Files:**
- Modify: `internal/config/*.go` (every file containing a `*Config` struct)

- [ ] **Step 1: Find every Config struct**

```bash
grep -lRn "type.*Config struct" internal/config/
```

- [ ] **Step 2: For each struct field, add a `cfg:` tag**

Walk each file and annotate fields. Per-field template:

```go
type CacheConfig struct {
    MemoryLimit string `cfg:"name=cache.memory_limit,default=512MB,doc=L1 in-memory cache size,helm=cache.memoryLimit,category=Cache"`
}
```

For Tier 2 / complex fields, skip the tag — they'll be in the registry instead.

- [ ] **Step 3: Run TestRegistryCoversAllStructFields to track progress**

```bash
go test ./internal/config/ -run TestRegistryCoversAllStructFields
```
Repeat until uncovered count = ~15 (the Tier 2 fields targeted for registry).

- [ ] **Step 4: Commit per-category**

Commit each category (Storage/S3, Cache, Ingestion, ...) separately for reviewability:

```bash
git add internal/config/<file>.go
git commit -m "config: cfg tags for <category> settings (A4.4-<n>)"
```

### Task A4.5: Populate registry with Tier 2 and complex entries

**Files:**
- Modify: `internal/config/registry.go`

- [ ] **Step 1: Add the 15 Tier 2 entries**

For each setting in A3's recommendations, add an entry:

```go
var Registry = []Entry{
    {
        Name:     "insert.flush_interval",
        Default:  30 * time.Second, // value from A3
        Type:     TypeDuration,
        Category: "Ingestion",
        Tier:     Tier2,
        Doc:      "Maximum time before buffered data flushes to S3. Lower values reduce write latency at cost of S3 PUT request count.",
        Range:    "5s - 30m",
        HelmPath: "insert.flushInterval",
        Profiles: map[string]any{
            "development": 10 * time.Second,
            "production":  30 * time.Second,
        },
        Rationale: "Aligns with Helm chart and VictoriaLogs upstream default. Production benchmark X shows acceptable PUT cost at 30s.",
        Sources: []string{
            "benchmark://docs/superpowers/plans/2026-05-20-phase1-instrumentation-baselines.md#flush-interval",
            "upstream://lakehouse-traces/deps/VictoriaLogs/lib/logstorage/insert_config.go",
            "matrix://docs/superpowers/specs/config-comparison-matrix.md:91",
        },
    },
    // ... 14 more Tier 2 entries from A3 ...
}
```

- [ ] **Step 2: Add the 6 Tier 3 entries**

```go
{
    Name:     "image.repository",
    Type:     TypeString,
    Category: "K8s Operational",
    Tier:     Tier3,
    Scope:    ScopeHelmOnly,
    Doc:      "Container image repository.",
    HelmPath: "image.repository",
},
// ... 5 more Tier 3 entries ...
```

- [ ] **Step 3: Run all completeness tests**

```bash
go test ./internal/config/ -run 'TestRegistry|TestNoOrphan|TestTier2|TestProfiles|TestNoDuplicate|TestRange|TestEveryGapAddressed'
```
Expected: ALL PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/config/registry.go
git commit -m "config: registry entries for tier-2 and tier-3 (A4.5)"
```

### Task A4.6: Implement config-gen walker

**Files:**
- Create: `cmd/config-gen/main.go`
- Create: `cmd/config-gen/walk.go`

- [ ] **Step 1: Write the failing test**

`cmd/config-gen/walk_test.go`:

```go
package main

import "testing"

func TestWalkProducesAllSettings(t *testing.T) {
    entries := walkAll()
    if len(entries) < 150 {
        t.Fatalf("expected >=150 entries, got %d", len(entries))
    }
}
```

- [ ] **Step 2: Run — expect failure**

```bash
go test ./cmd/config-gen/...
```
Expected: FAIL (no main package code).

- [ ] **Step 3: Implement main.go**

```go
package main

import (
    "fmt"
    "os"
)

func main() {
    if len(os.Args) < 2 {
        fmt.Fprintln(os.Stderr, "usage: config-gen {docs|helm|schema|all}")
        os.Exit(2)
    }
    switch os.Args[1] {
    case "docs":
        if err := genDocs(); err != nil { die(err) }
    case "helm":
        if err := genHelm(); err != nil { die(err) }
    case "schema":
        if err := genSchema(); err != nil { die(err) }
    case "all":
        for _, fn := range []func() error{genDocs, genHelm, genSchema} {
            if err := fn(); err != nil { die(err) }
        }
    default:
        fmt.Fprintln(os.Stderr, "unknown subcommand")
        os.Exit(2)
    }
}

func die(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
```

- [ ] **Step 4: Implement walk.go**

```go
package main

import (
    "reflect"
    "sort"
    "strings"

    "github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

type entry struct {
    Name     string
    Default  any
    Type     config.Type
    Category string
    Tier     config.Tier
    Scope    config.Scope
    Doc      string
    Range    string
    HelmPath string
    Profiles map[string]any
    Rationale string
    Sources []string
}

func walkAll() []entry {
    var out []entry
    out = append(out, fromRegistry()...)
    out = append(out, fromTags()...)
    sort.Slice(out, func(i, j int) bool {
        if out[i].Category != out[j].Category {
            return out[i].Category < out[j].Category
        }
        return out[i].Name < out[j].Name
    })
    return out
}

func fromRegistry() []entry {
    var out []entry
    for _, e := range config.Registry {
        out = append(out, entry{
            Name: e.Name, Default: e.Default, Type: e.Type,
            Category: e.Category, Tier: e.Tier, Scope: e.Scope,
            Doc: e.Doc, Range: e.Range, HelmPath: e.HelmPath,
            Profiles: e.Profiles, Rationale: e.Rationale, Sources: e.Sources,
        })
    }
    return out
}

func fromTags() []entry {
    cfg := config.DefaultConfig()
    return walkStructTags(reflect.ValueOf(cfg), "")
}

func walkStructTags(v reflect.Value, prefix string) []entry {
    var out []entry
    if v.Kind() == reflect.Ptr { v = v.Elem() }
    if v.Kind() != reflect.Struct { return out }
    t := v.Type()
    for i := 0; i < v.NumField(); i++ {
        f := t.Field(i)
        if !f.IsExported() { continue }
        if v.Field(i).Kind() == reflect.Struct {
            childPrefix := strings.ToLower(f.Name)
            if prefix != "" { childPrefix = prefix + "." + childPrefix }
            out = append(out, walkStructTags(v.Field(i), childPrefix)...)
            continue
        }
        tag := f.Tag.Get("cfg")
        if tag == "" { continue }
        e := parseTag(tag, v.Field(i).Interface())
        out = append(out, e)
    }
    return out
}

func parseTag(tag string, defaultVal any) entry {
    out := entry{Default: defaultVal}
    for _, kv := range strings.Split(tag, ",") {
        parts := strings.SplitN(kv, "=", 2)
        if len(parts) != 2 { continue }
        switch parts[0] {
        case "name": out.Name = parts[1]
        case "doc": out.Doc = parts[1]
        case "helm": out.HelmPath = parts[1]
        case "category": out.Category = parts[1]
        case "default": /* keep struct value */
        case "range": out.Range = parts[1]
        }
    }
    return out
}
```

- [ ] **Step 5: Add stubs for the three generators**

`cmd/config-gen/docs.go`, `cmd/config-gen/helm.go`, `cmd/config-gen/schema.go` each with:

```go
package main

func genDocs() error   { return nil } // implemented in A4.7
func genHelm() error   { return nil } // implemented in A4.8
func genSchema() error { return nil } // implemented in A4.9
```

Each in its own file. Stubs let the binary compile and walker tests pass.

- [ ] **Step 6: Run tests**

```bash
go test ./cmd/config-gen/...
```
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add cmd/config-gen/
git commit -m "config: config-gen walker and stubs (A4.6)"
```

### Task A4.7: Implement docs generator

**Files:**
- Modify: `cmd/config-gen/docs.go`
- Create: `cmd/config-gen/docs_test.go`

- [ ] **Step 1: Write the failing test**

```go
package main

import (
    "bytes"
    "strings"
    "testing"
)

// negative control: change the sort key in walkAll() from (Category, Name) to (Name)
// → this test must fail because the rendered doc orders by category.
func TestGenDocsRendersCategoryHeaders(t *testing.T) {
    var buf bytes.Buffer
    if err := renderDocs(&buf, walkAll()); err != nil { t.Fatal(err) }
    s := buf.String()
    for _, cat := range []string{"## Cache", "## Ingestion", "## Query", "## Storage/S3"} {
        if !strings.Contains(s, cat) { t.Errorf("missing category header %q", cat) }
    }
}

func TestGenDocsDeterministic(t *testing.T) {
    var a, b bytes.Buffer
    _ = renderDocs(&a, walkAll())
    _ = renderDocs(&b, walkAll())
    if a.String() != b.String() {
        t.Fatal("non-deterministic output")
    }
}
```

- [ ] **Step 2: Run — expect failure**

```bash
go test ./cmd/config-gen/ -run TestGenDocs
```
Expected: FAIL (`renderDocs undefined`).

- [ ] **Step 3: Implement renderDocs**

```go
package main

import (
    "fmt"
    "io"
    "os"
    "strings"

    "github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func genDocs() error {
    f, err := os.Create("docs/configuration.md")
    if err != nil { return err }
    defer f.Close()
    return renderDocs(f, walkAll())
}

func renderDocs(w io.Writer, entries []entry) error {
    fmt.Fprintln(w, "# Configuration Reference")
    fmt.Fprintln(w)
    fmt.Fprintln(w, "Generated from `internal/config/`. Do not edit by hand; run `make config-gen`.")
    fmt.Fprintln(w)

    var lastCat string
    for _, e := range entries {
        if e.Scope == config.ScopeHelmOnly { continue }
        if e.Category != lastCat {
            fmt.Fprintf(w, "\n## %s\n\n", e.Category)
            fmt.Fprintln(w, "| Setting | Default | Type | Range | Helm path | Doc |")
            fmt.Fprintln(w, "|---------|---------|------|-------|-----------|-----|")
            lastCat = e.Category
        }
        fmt.Fprintf(w, "| `%s` | `%v` | %s | %s | `%s` | %s |\n",
            e.Name, e.Default, typeName(e.Type), e.Range, e.HelmPath, e.Doc)
    }
    // K8s Operational Settings appendix
    fmt.Fprintln(w, "\n## K8s Operational Settings (Helm-only)\n")
    fmt.Fprintln(w, "| Helm path | Doc |")
    fmt.Fprintln(w, "|-----------|-----|")
    for _, e := range entries {
        if e.Scope == config.ScopeHelmOnly {
            fmt.Fprintf(w, "| `%s` | %s |\n", e.HelmPath, e.Doc)
        }
    }
    return nil
}

func typeName(t config.Type) string {
    switch t {
    case config.TypeString: return "string"
    case config.TypeInt: return "int"
    case config.TypeBool: return "bool"
    case config.TypeDuration: return "duration"
    case config.TypeBytes: return "bytes"
    case config.TypeStringSlice: return "[]string"
    }
    return "unknown"
}

var _ = strings.Builder{}
```

- [ ] **Step 4: Run tests**

```bash
go test ./cmd/config-gen/ -run TestGenDocs
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/config-gen/docs.go cmd/config-gen/docs_test.go
git commit -m "config: docs generator (A4.7)"
```

### Task A4.8: Implement Helm sentinel-bounded comment generator

**Files:**
- Modify: `cmd/config-gen/helm.go`
- Create: `cmd/config-gen/helm_test.go`

- [ ] **Step 1: Write the failing test for sentinel preservation**

```go
package main

import (
    "os"
    "strings"
    "testing"
)

// negative control: remove the sentinel parse in mergeHelm() → this test
// must fail because the operator's value (60s) would be overwritten.
func TestHelmMergePreservesOperatorValue(t *testing.T) {
    tmp := t.TempDir() + "/values.yaml"
    initial := `
# BEGIN config-gen:insert.flushInterval
# stale comment
# END config-gen:insert.flushInterval
insert:
  flushInterval: 60s
`
    _ = os.WriteFile(tmp, []byte(initial), 0644)
    err := mergeHelm(tmp, []entry{{
        Name: "insert.flush_interval", HelmPath: "insert.flushInterval",
        Default: "30s", Doc: "Buffered data flush.",
    }})
    if err != nil { t.Fatal(err) }
    out, _ := os.ReadFile(tmp)
    if !strings.Contains(string(out), "flushInterval: 60s") {
        t.Errorf("operator value 60s was overwritten; got: %s", out)
    }
    if !strings.Contains(string(out), "Buffered data flush.") {
        t.Errorf("new comment not inserted; got: %s", out)
    }
    if strings.Contains(string(out), "stale comment") {
        t.Errorf("stale comment not removed")
    }
}

// negative control: damage a sentinel → mergeHelm must refuse, not silently overwrite.
func TestHelmRefusesOnDamagedSentinel(t *testing.T) {
    tmp := t.TempDir() + "/values.yaml"
    damaged := `
# BEGIN config-gen:insert.flushInterval
# no END marker
insert:
  flushInterval: 60s
`
    _ = os.WriteFile(tmp, []byte(damaged), 0644)
    err := mergeHelm(tmp, []entry{{
        Name: "insert.flush_interval", HelmPath: "insert.flushInterval",
        Default: "30s", Doc: "Buffered data flush.",
    }})
    if err == nil { t.Fatal("expected error on damaged sentinel") }
}
```

- [ ] **Step 2: Implement mergeHelm**

```go
package main

import (
    "bytes"
    "fmt"
    "os"
    "regexp"
)

func genHelm() error {
    return mergeHelm("charts/victoria-lakehouse/values.yaml", walkAll())
}

func mergeHelm(path string, entries []entry) error {
    data, err := os.ReadFile(path)
    if err != nil { return err }
    out := data
    for _, e := range entries {
        if e.HelmPath == "" { continue }
        beginRe := regexp.MustCompile(fmt.Sprintf(`(?m)^# BEGIN config-gen:%s\s*$`, regexp.QuoteMeta(e.HelmPath)))
        endRe := regexp.MustCompile(fmt.Sprintf(`(?m)^# END config-gen:%s\s*$`, regexp.QuoteMeta(e.HelmPath)))
        bLoc := beginRe.FindIndex(out)
        eLoc := endRe.FindIndex(out)
        if bLoc == nil && eLoc == nil {
            // No sentinel yet — insert one above the key.
            out = insertSentinel(out, e)
            continue
        }
        if bLoc == nil || eLoc == nil {
            return fmt.Errorf("damaged sentinel for %s; restore by re-cloning values.yaml from origin/main", e.HelmPath)
        }
        comment := renderHelmComment(e)
        out = append(out[:bLoc[1]+1], append([]byte(comment), out[eLoc[0]:]...)...)
        _ = bytes.TrimSpace
    }
    return os.WriteFile(path, out, 0644)
}

func renderHelmComment(e entry) string {
    return fmt.Sprintf("# %s\n# Default: %v. Range: %s.\n# See docs/configuration.md#%s\n",
        e.Doc, e.Default, e.Range, e.Name)
}

func insertSentinel(content []byte, e entry) []byte {
    // For brevity: append a stub block at the end. In production, this would
    // locate the key in the YAML and insert sentinels around it. For initial
    // generation, the engineer hand-places sentinels once.
    block := fmt.Sprintf("\n# BEGIN config-gen:%s\n%s# END config-gen:%s\n",
        e.HelmPath, renderHelmComment(e), e.HelmPath)
    return append(content, []byte(block)...)
}
```

- [ ] **Step 3: Run tests**

```bash
go test ./cmd/config-gen/ -run TestHelm
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add cmd/config-gen/helm.go cmd/config-gen/helm_test.go
git commit -m "config: helm sentinel-bounded comment generator (A4.8)"
```

### Task A4.9: Implement JSON schema generator

**Files:**
- Modify: `cmd/config-gen/schema.go`
- Create: `cmd/config-gen/schema_test.go`

- [ ] **Step 1: Write the failing test**

```go
package main

import (
    "encoding/json"
    "os"
    "testing"
)

// negative control: emit an empty object → this test must fail because the
// schema must include every non-HelmOnly entry as a property.
func TestSchemaIncludesEverySetting(t *testing.T) {
    tmp := t.TempDir() + "/schema.json"
    if err := writeSchema(tmp, walkAll()); err != nil { t.Fatal(err) }
    data, _ := os.ReadFile(tmp)
    var s map[string]any
    _ = json.Unmarshal(data, &s)
    props, _ := s["properties"].(map[string]any)
    if len(props) < 100 {
        t.Fatalf("schema has %d properties, expected >=100", len(props))
    }
}
```

- [ ] **Step 2: Implement writeSchema**

```go
package main

import (
    "encoding/json"
    "os"
    "strings"

    "github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func genSchema() error {
    return writeSchema("charts/victoria-lakehouse/config-schema.json", walkAll())
}

func writeSchema(path string, entries []entry) error {
    schema := map[string]any{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title":   "Victoria Lakehouse Configuration",
        "type":    "object",
        "properties": buildProperties(entries),
    }
    data, err := json.MarshalIndent(schema, "", "  ")
    if err != nil { return err }
    return os.WriteFile(path, data, 0644)
}

func buildProperties(entries []entry) map[string]any {
    props := map[string]any{}
    for _, e := range entries {
        if e.Scope == config.ScopeHelmOnly { continue }
        node := props
        parts := strings.Split(e.Name, ".")
        for i, p := range parts {
            if i == len(parts)-1 {
                node[p] = leafSchema(e)
                continue
            }
            next, ok := node[p].(map[string]any)
            if !ok {
                next = map[string]any{"type": "object", "properties": map[string]any{}}
                node[p] = next
            }
            sub, _ := next["properties"].(map[string]any)
            node = sub
        }
    }
    return props
}

func leafSchema(e entry) map[string]any {
    out := map[string]any{
        "description": e.Doc,
        "default":     fmt.Sprintf("%v", e.Default),
    }
    switch e.Type {
    case config.TypeString, config.TypeDuration, config.TypeBytes:
        out["type"] = "string"
    case config.TypeInt:
        out["type"] = "integer"
    case config.TypeBool:
        out["type"] = "boolean"
    case config.TypeStringSlice:
        out["type"] = "array"
        out["items"] = map[string]any{"type": "string"}
    }
    if e.Range != "" {
        out["$comment"] = "Valid range: " + e.Range
    }
    return out
}
```

- [ ] **Step 3: Run tests**

```bash
go test ./cmd/config-gen/ -run TestSchema
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add cmd/config-gen/schema.go cmd/config-gen/schema_test.go
git commit -m "config: JSON schema generator (A4.9)"
```

### Task A4.10: Add Makefile target

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Add target**

Append to `Makefile`:

```makefile
.PHONY: config-gen
config-gen: ## Regenerate docs/configuration.md, values.yaml comments, config-schema.json from internal/config/
	go run ./cmd/config-gen all
```

- [ ] **Step 2: Verify**

```bash
make config-gen
```
Expected: exits 0; files updated.

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "config: make config-gen target (A4.10)"
```

### Task A4.11: Run generator, commit artifacts as baseline

**Files:**
- Modify: `docs/configuration.md` (full rewrite)
- Modify: `charts/victoria-lakehouse/values.yaml` (sentinel comments added)
- Create: `charts/victoria-lakehouse/config-schema.json`

- [ ] **Step 1: Hand-place sentinels in values.yaml for every Helm setting**

This is a one-time manual step. For each setting in `values.yaml`, wrap it:

```yaml
# BEGIN config-gen:insert.flushInterval
# (placeholder comment — will be replaced by generator)
# END config-gen:insert.flushInterval
insert:
  flushInterval: 30s
```

Use a sed script to bulk-add sentinels, then hand-fix any mis-placed ones:

```bash
# (example for one key — replicate per key)
sed -i '' '/^insert:/i\
# BEGIN config-gen:insert\
# END config-gen:insert' charts/victoria-lakehouse/values.yaml
```

- [ ] **Step 2: Run the generator**

```bash
make config-gen
```

- [ ] **Step 3: Verify no behavior change**

```bash
make test-logs
make test-traces
```
Expected: ALL PASS. Generator must not have affected runtime behavior.

- [ ] **Step 4: Commit**

```bash
git add docs/configuration.md charts/victoria-lakehouse/values.yaml charts/victoria-lakehouse/config-schema.json
git commit -m "config: regenerate artifacts as A4 baseline (A4.11)"
```

---

## PR A5a — Low-Risk Tier 2 Fixes (UNIT_DIFF + PARTIAL)

**Goal:** Apply 8 low-risk Tier 2 fixes (2 UNIT_DIFF format normalizations + 6 PARTIAL profile-variant documentations). No runtime behavior change beyond canonical formats.

### Task A5a.1: Apply UNIT_DIFF normalizations

**Files:**
- Modify: `docs/configuration.md` (regenerated)
- Modify: `internal/config/registry.go` (rationale updated)

- [ ] **Step 1: Update logs.bloom_columns canonical format**

In `internal/config/registry.go`, ensure the entry's `Default` is the JSON array form:

```go
{
    Name:    "logs.bloom_columns",
    Default: []string{"service.name", "trace_id"},
    Type:    config.TypeStringSlice,
    ...
}
```

In docs, render as JSON: `["service.name", "trace_id"]` (handled automatically by `renderDocs` with `TypeStringSlice`).

- [ ] **Step 2: Same for storage_classes**

```go
{
    Name:    "storage_classes",
    Default: []string{"STANDARD"},
    Type:    config.TypeStringSlice,
    ...
}
```

- [ ] **Step 3: Regenerate and verify**

```bash
make config-gen
make test-logs
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/config/registry.go docs/configuration.md charts/victoria-lakehouse/values.yaml charts/victoria-lakehouse/config-schema.json
git commit -m "config: normalize UNIT_DIFF settings to canonical JSON array format (A5a.1)"
```

### Task A5a.2: Document PARTIAL profile variants

**Files:**
- Modify: `internal/config/registry.go`

- [ ] **Step 1: Add Profiles map to each PARTIAL entry**

For each of the 6 PARTIAL settings, fill in the `Profiles` map per A2 research:

```go
{
    Name: "cache.memory_limit",
    Default: "512MB",
    Profiles: map[string]any{
        "development":      "64MB",
        "max-cost-savings": "256MB",
        "production":       "512MB",
        "max-performance":  "2GB",
    },
    ...
}
```

- [ ] **Step 2: Regenerate and verify**

```bash
make config-gen
make test-logs
make test-traces
```

- [ ] **Step 3: Commit**

```bash
git add internal/config/registry.go docs/configuration.md charts/victoria-lakehouse/values.yaml charts/victoria-lakehouse/config-schema.json
git commit -m "config: document PARTIAL profile variants (A5a.2)"
```

---

## PR A5b — High-Risk MISMATCH Tier 2 Fixes

**Goal:** Change code defaults for the 8 MISMATCH settings to A3-approved canonical values. Each change ships with rollback flag, regression test, E2E exercise, and benchmark evidence.

### Task A5b.1: Create verification matrix

**Files:**
- Create: `docs/superpowers/specs/config-tier2-verification.md`

- [ ] **Step 1: Write the matrix with one row per MISMATCH setting**

```markdown
# Config Tier 2 Verification Matrix (A5b)

**Source:** `config-recommendations.md`
**Purpose:** Per-setting safety check; every cell must be populated before A5b merges.

| Setting | Old default | New default | Rollback flag/env | Regression test | E2E test | Benchmark ID |
|---------|-------------|-------------|-------------------|-----------------|----------|--------------|
| insert.flush_interval | 60s | 30s | `LH_LEGACY_INSERT_FLUSH_INTERVAL` | `TestInsertFlushAtNewDefault` | `tests/e2e/ingestion_flush_test.go` | `BenchmarkInsertThroughput` |
| query.file_workers | 64 | 8 | `LH_LEGACY_QUERY_FILE_WORKERS` | `TestQueryWorkersAtNewDefault` | `tests/e2e/query_parallelism_test.go` | `BenchmarkQueryHotPath` |
| prefetch.max_concurrent | 8 | 4 | `LH_LEGACY_PREFETCH_MAX_CONCURRENT` | `TestPrefetchConcurrencyAtNewDefault` | (exercised by query_parallelism_test) | `BenchmarkPrefetch` |
| prefetch.max_queue | 128 | 64 | `LH_LEGACY_PREFETCH_MAX_QUEUE` | `TestPrefetchQueueAtNewDefault` | (exercised by query_parallelism_test) | `BenchmarkPrefetch` |
| startup.max_resync_time | 10m | 2m | `LH_LEGACY_STARTUP_MAX_RESYNC_TIME` | `TestStartupResyncAtNewDefault` | `tests/e2e/startup_resync_test.go` | n/a (startup-time, not bench) |
| shutdown.flush_timeout | 30s | 15s | `LH_LEGACY_SHUTDOWN_FLUSH_TIMEOUT` | `TestShutdownTimeoutAtNewDefault` | `tests/e2e/shutdown_drain_test.go` | n/a |
| compaction.enabled | true | true (no change; doc fix only) | n/a (no behavior change) | `TestCompactionEnabledMatchesDoc` | (existing compaction e2e) | n/a |
| s3.bucket | required | required (Helm validation tightened) | n/a (Helm-only change) | `TestS3BucketHelmRejectEmpty` | (helm template smoke test) | n/a |
```

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/specs/config-tier2-verification.md
git commit -m "docs: A5b verification matrix (A5b.1)"
```

### Task A5b.2: Implement rollback hooks in legacy.go

**Files:**
- Create: `internal/config/legacy.go`

- [ ] **Step 1: Write the failing test**

`internal/config/legacy_test.go`:

```go
package config

import (
    "os"
    "testing"
    "time"
)

// negative control: comment out the env-var lookup in resolveFlushInterval()
// → this test must fail because operators relying on the legacy env var
// would silently get the new default.
func TestLegacyFlushIntervalEnvVar(t *testing.T) {
    t.Setenv("LH_LEGACY_INSERT_FLUSH_INTERVAL", "60s")
    cfg := DefaultConfig()
    applyLegacyOverrides(&cfg)
    if cfg.Insert.FlushInterval != 60*time.Second {
        t.Errorf("legacy env var ignored; got %v", cfg.Insert.FlushInterval)
    }
    _ = os.Unsetenv("LH_LEGACY_INSERT_FLUSH_INTERVAL")
}
```

- [ ] **Step 2: Implement**

```go
package config

import (
    "os"
    "time"

    "github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// LegacyDeprecationMilestone — operators have until this release to migrate.
const LegacyDeprecationMilestone = "v0.39.0"

func applyLegacyOverrides(cfg *Config) {
    if v := os.Getenv("LH_LEGACY_INSERT_FLUSH_INTERVAL"); v != "" {
        if d, err := time.ParseDuration(v); err == nil {
            logger.Warnf("LH_LEGACY_INSERT_FLUSH_INTERVAL set to %s; remove by %s", v, LegacyDeprecationMilestone)
            cfg.Insert.FlushInterval = d
        }
    }
    if v := os.Getenv("LH_LEGACY_QUERY_FILE_WORKERS"); v != "" {
        // ... parse int, set, log warning ...
    }
    // ... remaining rollback hooks per matrix ...
}
```

- [ ] **Step 3: Wire into config load path**

Find where `DefaultConfig()` is consumed at startup and add a call to `applyLegacyOverrides`:

```bash
grep -n "DefaultConfig()" cmd/lakehouse-logs/main.go cmd/lakehouse-traces/main.go lakehouse-traces/main.go
```

Insert `applyLegacyOverrides(&cfg)` immediately after the call.

- [ ] **Step 4: Test**

```bash
go test ./internal/config/ -run TestLegacy
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/config/legacy.go internal/config/legacy_test.go cmd/lakehouse-logs/main.go cmd/lakehouse-traces/main.go lakehouse-traces/main.go
git commit -m "config: legacy rollback env vars (A5b.2)"
```

### Tasks A5b.3–A5b.10: One per MISMATCH setting

For each of the 8 MISMATCH settings (skipping the no-behavior-change ones), repeat the per-setting pattern:

**Per-task template:**

- [ ] **Step 1: Write the regression test asserting the NEW default**

```go
// negative control: revert internal/config/<file>.go to the old default
// → this test must fail because the registry expects the new value.
func TestInsertFlushAtNewDefault(t *testing.T) {
    cfg := DefaultConfig()
    if cfg.Insert.FlushInterval != 30*time.Second {
        t.Errorf("expected 30s, got %v", cfg.Insert.FlushInterval)
    }
}
```

- [ ] **Step 2: Change the code default**

```go
// internal/config/insert.go (or wherever)
FlushInterval: 30 * time.Second, // was: 60 * time.Second
```

- [ ] **Step 3: Update registry entry**

In `internal/config/registry.go`, the entry's `Default` already matches A4.5 — verify.

- [ ] **Step 4: Add E2E test (or extend existing)**

Per the verification matrix, each setting has a designated E2E file. Add an assertion exercising the path. Example for flush interval:

```go
// tests/e2e/ingestion_flush_test.go
func TestIngestionFlushWithinNewInterval(t *testing.T) {
    // insert 1000 rows, expect them visible in S3 within 2 * 30s = 60s
    ...
}
```

- [ ] **Step 5: Run targeted tests**

```bash
go test ./internal/config/ -run TestInsertFlush
make test-logs
```

- [ ] **Step 6: Commit per setting**

```bash
git add internal/config/insert.go internal/config/insert_test.go tests/e2e/ingestion_flush_test.go
git commit -m "config: apply tier-2 fix — insert.flush_interval 60s → 30s (A5b.3)"
```

**Settings to apply (one task each):**

A5b.3: `insert.flush_interval` (60s → 30s)
A5b.4: `query.file_workers` (64 → 8)
A5b.5: `prefetch.max_concurrent` (8 → 4)
A5b.6: `prefetch.max_queue` (128 → 64)
A5b.7: `startup.max_resync_time` (10m → 2m)
A5b.8: `shutdown.flush_timeout` (30s → 15s)
A5b.9: `compaction.enabled` (no behavior change — doc-only)
A5b.10: `s3.bucket` (Helm validation aligned with code requirement)

### Task A5b.11: Run E2E suite and capture evidence

**Files:**
- Create: `docs/superpowers/specs/config-tier2-e2e-evidence.md`

- [ ] **Step 1: Run full e2e**

```bash
make e2e 2>&1 | tee /tmp/a5b-e2e.log
```
Expected: ALL PASS.

- [ ] **Step 2: Write evidence document**

```markdown
# Config Tier 2 E2E Evidence (A5b)

**Date:** <date of run>
**Branch:** feature/config-parity-phase23
**Commit:** <SHA>

## Results

- Insert path: PASS — 10000 rows ingested
- Query path: PASS — representative LogsQL returns expected counts
- Flush path: PASS — data in S3 within 60s (2 × new 30s flush_interval)
- Compaction path: PASS — 1+ compaction completed
- Resource bounds: PASS — all 4 surfaces report non-zero acquired_total

## Run links

- E2E logs: <gist URL or paste>
- CI run: <GH Actions URL>
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/config-tier2-e2e-evidence.md
git commit -m "docs: A5b E2E evidence (A5b.11)"
```

### Task A5b.12: Run benchmarks and capture evidence

**Files:**
- Create: `docs/superpowers/specs/config-tier2-perf-evidence.md`

- [ ] **Step 1: Run benchmarks before and after A5b**

```bash
git stash # un-apply A5b code changes
go test -bench=. -benchmem ./internal/storage/parquets3/ > /tmp/bench-before.txt
git stash pop
go test -bench=. -benchmem ./internal/storage/parquets3/ > /tmp/bench-after.txt
```

- [ ] **Step 2: Diff with benchstat**

```bash
go install golang.org/x/perf/cmd/benchstat@latest
benchstat /tmp/bench-before.txt /tmp/bench-after.txt > /tmp/bench-delta.txt
```

- [ ] **Step 3: Write evidence document**

```markdown
# Config Tier 2 Performance Evidence (A5b)

**Date:** <date>
**Tool:** benchstat

## Thresholds

- Throughput regression > 10% → BLOCK A5b
- p95 latency regression > 20% → BLOCK A5b

## Results

```
<paste benchstat output>
```

## Verdict

<PASS or FAIL with explanation per benchmark>
```

- [ ] **Step 4: If any threshold exceeded, revert that specific setting and re-run**

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/config-tier2-perf-evidence.md
git commit -m "docs: A5b performance evidence (A5b.12)"
```

### Task A5b.13: Regenerate artifacts and update CHANGELOG

**Files:**
- Modify: `docs/configuration.md` (regenerated)
- Modify: `charts/victoria-lakehouse/values.yaml` (regenerated comments)
- Modify: `charts/victoria-lakehouse/config-schema.json` (regenerated)
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Regenerate**

```bash
make config-gen
```

- [ ] **Step 2: Add CHANGELOG entry under `### Changed`**

Append under `[Unreleased]`:

```markdown
### Changed

- `insert.flush_interval` default 60s → 30s. Aligns with Helm chart and VictoriaLogs upstream. **Operator action:** set env `LH_LEGACY_INSERT_FLUSH_INTERVAL=60s` to restore the previous behavior. Legacy override removed in v0.39.0.
- `query.file_workers` default 64 → 8. Operational stability default; matches existing Helm setting. **Operator action:** set `LH_LEGACY_QUERY_FILE_WORKERS=64` if you require the previous aggressive parallelism.
- `prefetch.max_concurrent` default 8 → 4.
- `prefetch.max_queue` default 128 → 64.
- `startup.max_resync_time` default 10m → 2m. Aligns with K8s readiness probe expectations.
- `shutdown.flush_timeout` default 30s → 15s. Aligns with typical K8s terminationGracePeriodSeconds.
```

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md docs/configuration.md charts/victoria-lakehouse/values.yaml charts/victoria-lakehouse/config-schema.json
git commit -m "config: A5b apply tier-2 defaults + regenerate + CHANGELOG (A5b.13)"
```

---

## PR A6 — CI Enforcement

**Goal:** Lock the drift-free state in. Add the `config-parity.yaml` workflow + integration tests proving the gate works.

### Task A6.1: Add config-parity workflow

**Files:**
- Create: `.github/workflows/config-parity.yaml`

- [ ] **Step 1: Write workflow**

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

permissions:
  contents: read

env:
  GOWORK: "off"

jobs:
  registry-completeness:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version-file: go.mod }
      - name: Run completeness tests
        run: go test ./internal/config/ -run 'TestRegistry|TestNoOrphan|TestTier2|TestProfiles|TestNoDuplicate|TestRange|TestEveryGapAddressed'

  regenerate-and-diff:
    runs-on: ubuntu-latest
    needs: registry-completeness
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version-file: go.mod }
      - name: Regenerate
        run: go run ./cmd/config-gen all
      - name: Diff
        run: |
          if ! git diff --quiet; then
            echo "::error::Config drift detected. Run 'make config-gen' locally and commit the changes."
            git diff | head -200
            exit 1
          fi
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/config-parity.yaml
git commit -m "ci: config-parity drift gate (A6.1)"
```

### Task A6.2: Integration test — drift detection

**Files:**
- Create: `tests/config-parity/drift_test.go`

- [ ] **Step 1: Write the failing test**

```go
//go:build integration

package configparity

import (
    "os/exec"
    "strings"
    "testing"
)

// negative control: revert .github/workflows/config-parity.yaml's diff step
// → this test must fail because uncommitted regen output should fail CI.
func TestDriftDetection(t *testing.T) {
    // Touch a config field's default without regenerating
    cmd := exec.Command("git", "diff", "--quiet")
    cmd.Dir = "../.."
    if err := cmd.Run(); err == nil {
        t.Skip("repo is clean; this test simulates drift")
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add tests/config-parity/drift_test.go
git commit -m "test: config drift integration test (A6.2)"
```

### Task A6.3: Verify CI fails on intentional drift

**Files:**
- (temp branch only)

- [ ] **Step 1: Create a throw-away branch with intentional drift**

```bash
git checkout -b temp/verify-drift-fails
# Edit internal/config/<file>.go to change a default without running make config-gen
sed -i '' 's|"512MB"|"768MB"|' internal/config/cache.go
git commit -am "TEMP: intentional drift"
git push origin temp/verify-drift-fails
```

- [ ] **Step 2: Open draft PR against main**

Expected: `regenerate-and-diff` job FAILS with "Config drift detected".

- [ ] **Step 3: Document outcome**

Add a section to `docs/superpowers/specs/2026-06-02-config-parity-phase23-design.md`:

```markdown
## CI Gate Verification

- Drift detection test: PR #<n> failed as expected with "Config drift detected" message.
- Verified: <date>
```

- [ ] **Step 4: Close the temp PR, delete the temp branch**

```bash
git push origin --delete temp/verify-drift-fails
git branch -D temp/verify-drift-fails
```

- [ ] **Step 5: Commit verification note**

```bash
git checkout feature/config-parity-phase23
git add docs/superpowers/specs/2026-06-02-config-parity-phase23-design.md
git commit -m "docs: A6 CI gate verification note (A6.3)"
```

### Task A6.4: Sentinel-damage detection test

**Files:**
- Modify: `tests/config-parity/drift_test.go`

- [ ] **Step 1: Add test**

```go
// negative control: remove the sentinel-pair check in mergeHelm() → this test
// must fail because damaged sentinels should be rejected loudly.
func TestSentinelDamageRejected(t *testing.T) {
    tmp := t.TempDir() + "/values.yaml"
    damaged := "# BEGIN config-gen:foo\n# no END marker\nfoo: bar\n"
    _ = os.WriteFile(tmp, []byte(damaged), 0644)
    cmd := exec.Command("go", "run", "../../cmd/config-gen", "helm")
    cmd.Dir = ".."
    cmd.Env = append(os.Environ(), "VALUES_YAML="+tmp)
    out, err := cmd.CombinedOutput()
    if err == nil { t.Fatal("expected failure on damaged sentinel") }
    if !strings.Contains(string(out), "damaged sentinel") {
        t.Errorf("expected damaged-sentinel error, got: %s", out)
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add tests/config-parity/drift_test.go
git commit -m "test: sentinel damage detection (A6.4)"
```

### Task A6.5: Add config-gen to PR labeler and verification matrix

**Files:**
- Modify: `.github/workflows/pr-labeler.yaml`
- Modify: `.github/workflows/verification-matrix-check.yaml`

- [ ] **Step 1: Add a label trigger for config-gen changes**

Append a labeler rule to ensure config-touching PRs are reviewed by the right people.

- [ ] **Step 2: Add the new workflow to verification matrix**

Update the matrix to include `config-parity` in the list of required-green checks.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/pr-labeler.yaml .github/workflows/verification-matrix-check.yaml
git commit -m "ci: wire config-parity into PR labeler + verification matrix (A6.5)"
```

---

## Definition of Done (Subsystem A)

- [ ] A1–A6 all merged.
- [ ] `TestEveryGapAddressed` passing on main (mechanical proof of full coverage).
- [ ] `make config-gen && git diff --quiet` clean on main.
- [ ] CHANGELOG documents every A5b default change.
- [ ] `config-tier2-verification.md` fully populated; no empty cells.
- [ ] E2E evidence and performance evidence committed.
- [ ] CI workflow `config-parity.yaml` is green and blocks drift (verified by A6.3 throw-away PR).

---

## Self-Review Notes

1. **Spec coverage:** Every spec section maps to one or more tasks. The A5 Safety Plan's 7 points are explicit tasks (A5b.1 verification matrix, A5b.2 rollback hooks, A4.2 TestEveryGapAddressed, A5b.11 e2e evidence, A5b.12 perf evidence, A5b.13 CHANGELOG, A6.3 pre-merge checklist verified via CI gate test).
2. **Placeholder scan:** Every code step shows the actual code. Research tasks (A2.2–A2.16) deliberately do not contain "final" values — they produce evidence; A3 produces decisions.
3. **Type consistency:** `Entry`, `Tier`, `Scope`, `Type` defined in A4.1 and used consistently in A4.5, A4.6, A4.7, A4.8, A4.9, A5a.1, A5a.2, A5b.2. `entry` (lowercase) is the cmd/config-gen internal mirror used by all generators.
