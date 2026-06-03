# Subsystem E — Community Standards Alignment

**Status:** Design

**Date:** 2026-06-03

**Continues:** Final subsystem in the broader parity initiative (A–E).

**Earlier subsystems narrowed E's scope:**
- OpenTelemetry semantic conventions for column names → Subsystem C (validated via `tests/parquet-format/`).
- Wire protocol conformance (Loki, OTLP, Jaeger, etc.) → Subsystem B (validated via `tests/parity/protocol-conformance/`).
- Apache Parquet spec compliance (multi-reader) → Subsystem C.

---

## Goal

Add production-grade supply-chain governance and ecosystem positioning around the existing LH codebase. Five concerns covered:

1. **License audit & policy enforcement** — every Go dependency in both modules classified into `allow / warn / deny` tiers; CI blocks PRs introducing `deny`-tier licenses or unexcepted `warn`-tier licenses.
2. **CycloneDX SBOM generation** — generated per release per binary and per container image; attached to GitHub releases; renewed nightly on `main`.
3. **SLSA Build Level 2 provenance** — signed provenance attestations on every release artifact via `slsa-github-generator`.
4. **Sigstore release signing** — keyless cosign signatures on all binaries and container images.
5. **Apache Arrow IPC compatibility + CNCF positioning** — loss-free Arrow IPC roundtrip test; CNCF Observability category alignment document.

---

## Architecture

Implementation across three locations:

```
tests/community-standards/
├── license-policy.yaml          # allow / warn / deny tier definitions (SPDX ids)
├── license-overrides.yaml       # corrections for misdetected licenses
├── license_audit_test.go        # walks go.mod, classifies, asserts gate
├── arrow_ipc_test.go            # Parquet → Arrow IPC → Parquet roundtrip
├── workflow_lint_test.go        # asserts release.yaml has required permissions/triggers
├── integration_test.go          # CycloneDX validity (build tag community_standards)
├── synthesis_test.go            # synthetic deny/warn fixtures
└── regression_test.go           # stale-exception governance

.github/workflows/
├── license-policy.yaml          # blocking gate on every PR
├── sbom.yaml                    # SBOM generation (PR advisory + release + nightly)
├── arrow-ipc.yaml               # Arrow roundtrip on schema/storage PRs
└── release.yaml                 # extended with slsa-provenance, cosign-binaries, cosign-images jobs

docs/
├── LICENSE_EXCEPTIONS.md        # accepted warn-tier deps with rationale + linking-model
├── CNCF-ALIGNMENT.md            # maps LH features to CNCF Observability whitepaper
└── SUPPLY_CHAIN.md              # operator-facing verification guide (cosign, slsa-verifier)
```

CI surface added:
- `license-policy` — blocking on every PR.
- `arrow-ipc-roundtrip` — blocking on PRs touching schema/storage paths.
- `workflow-lint` — blocking on PRs touching `.github/workflows/`.
- `sbom-build` — advisory on PR; required on release; nightly cron.
- `slsa-provenance` — release-only; required.
- `cosign-binaries` — release-only; required.
- `cosign-images` — release-only; required.

---

## Components

### License Audit & Policy

**`license-policy.yaml`** — SPDX identifier classification:

```yaml
allow:   [MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, 0BSD, Unlicense, CC0-1.0]
warn:    [LGPL-2.1, LGPL-2.1-only, LGPL-3.0, LGPL-3.0-only, MPL-2.0, EPL-2.0, CDDL-1.0, CDDL-1.1]
deny:    [GPL-2.0, GPL-2.0-only, GPL-3.0, GPL-3.0-only, AGPL-3.0, AGPL-3.0-only, SSPL-1.0, BUSL-1.1, Commons-Clause]
```

**`license-overrides.yaml`** — corrections for misdetected licenses:

```yaml
overrides:
  - module: "github.com/example/lib"
    license: "MIT"
    reason: "upstream LICENSE file is unambiguous MIT; pkg.go.dev incorrectly classifies as Apache-2.0"
    verified: "2026-06-03"
```

Each override requires a `verified` date; quarterly review flags overrides older than 12 months.

**`docs/LICENSE_EXCEPTIONS.md`** — single source of truth for accepted `warn`-tier deps:

| Dependency | License | Pinned version | Why we accept it | Linking model |
|-----------|---------|----------------|-------------------|---------------|
| (rows added as needed) | | | | |

**`license_audit_test.go`** — invokes `go list -m -json all` for both `./` and `./lakehouse-traces/`, deduplicates by module path, resolves license via SPDX header / `pkg.go.dev` / overrides, classifies into tiers, cross-references `LICENSE_EXCEPTIONS.md`, asserts:
- No `TierDeny` modules.
- No `TierWarn` modules unless listed in exceptions.
- No `TierUnknown` modules unless override exists.

### CycloneDX SBOM Generation

**`.github/workflows/sbom.yaml`** runs `cyclonedx-gomod app -licenses` per binary. Artifacts uploaded; attached to release on `release` event.

Per-binary outputs:
- `sbom-lakehouse-logs.cyclonedx.json`
- `sbom-lakehouse-traces.cyclonedx.json`

Per-image outputs (separate job using image scanner, e.g., `syft`):
- `sbom-lakehouse-logs-image.cyclonedx.json`
- `sbom-lakehouse-traces-image.cyclonedx.json`

### SLSA Build Level 2

**`release.yaml`** modification consumes `slsa-framework/slsa-github-generator/.github/workflows/generator_generic_slsa3.yml@v2.0.0`. Builds emit base64-encoded `subjects` (sha256:digest name pairs) for the binaries and images. Provenance file `lakehouse-<tag>.intoto.jsonl` attached to release.

Permissions required:
- `id-token: write` (OIDC for SLSA signer).
- `contents: write` (release upload).
- `actions: read`.

Level 3 explicitly deferred (requires hardened-runner image plumbing).

### Sigstore Release Signing

Two jobs in `release.yaml`:

**`cosign-binaries`** — uses keyless cosign via OIDC:
```bash
cosign sign-blob --yes lakehouse-logs \
  --output-signature lakehouse-logs.sig \
  --output-certificate lakehouse-logs.crt
```
Outputs `.sig` and `.crt` files attached to release.

**`cosign-images`** — signs by image digest:
```bash
cosign sign --yes ghcr.io/reliablyobserve/lakehouse-logs@<digest>
```
Signature lives alongside the image in the OCI registry.

Both jobs require `id-token: write`. Operator verification documented in `SUPPLY_CHAIN.md` with copy-paste-ready commands.

### Apache Arrow IPC Roundtrip

**`arrow_ipc_test.go`** uses `github.com/apache/arrow-go/v18`:

```go
func TestArrowIPCRoundtripLogs(t *testing.T) {
    rows := schemaFixtureLogRows()
    pq := writeLogsParquetForTest(rows)
    arrowTable := readParquetIntoArrow(t, pq)
    pqRoundtrip := writeArrowToParquet(t, arrowTable)
    origCols := readParquetSchema(pq)
    rtCols := readParquetSchema(pqRoundtrip)
    if !reflect.DeepEqual(origCols, rtCols) {
        t.Errorf("schema lost in roundtrip:\noriginal=%v\nroundtrip=%v", origCols, rtCols)
    }
    if origRows := countRows(pq); origRows != countRows(pqRoundtrip) {
        t.Errorf("row count lost: original=%d roundtrip=%d", origRows, countRows(pqRoundtrip))
    }
}
```

Coverage:
- `TestArrowIPCRoundtripLogs`
- `TestArrowIPCRoundtripTraces`
- `TestArrowIPCPreservesNanoTimestamps` — INT64 nanoseconds survive without precision loss.
- `TestArrowIPCPreservesUnicodeStrings` — non-ASCII preserved.

Documented limitation: Parquet KV metadata is NOT preserved in Arrow IPC format. Subsystem C's `TestKVMetadataPresent` is the authoritative test for KV metadata persistence.

### CNCF Alignment Document

**`docs/CNCF-ALIGNMENT.md`** maps LH features to CNCF Observability whitepaper sections:

- Signal coverage table (Logs / Traces / Metrics / Profiles / Events).
- Compliance areas referencing back to Subsystem B/C tests as authoritative.
- CNCF maturity model status (Sandbox / Incubating / Graduated).

Not a CI-gated file; reviewed manually.

### Supply Chain Verification Guide

**`docs/SUPPLY_CHAIN.md`** — operator-facing instructions:
- `cosign verify-blob` for binaries.
- `cosign verify` for container images.
- `slsa-verifier verify-artifact` for provenance.
- `jq` snippets for SBOM inspection.

---

## Data Flow

### Every PR

1. `license-policy` job runs: walks both modules' deps, classifies, cross-references exceptions, fails on deny-tier or unexcepted warn-tier.
2. `arrow-ipc-roundtrip` job runs if PR touches schema/storage paths.
3. `workflow-lint` runs if PR touches `.github/workflows/`.
4. If PR modifies `go.mod`/`go.sum`, `sbom-build` runs as advisory (uploads artifact; doesn't block).

### Nightly cron

1. `sbom.yaml` scheduled cron runs at 04:00 UTC daily.
2. Generates fresh SBOM for `main` branch.
3. Uploaded as `sbom-nightly-<date>` artifact.
4. Catches transitive-dep churn not triggered by PR paths.

### Release

1. Maintainer pushes `v0.X.Y` tag → triggers `release.yaml`.
2. `build` produces binaries + pushes container images.
3. `sbom-build` (now required) generates per-binary + per-image SBOM; attaches to release.
4. `slsa-provenance` consumes `build`'s subject digests; produces `lakehouse-v0.X.Y.intoto.jsonl`; attaches to release.
5. `cosign-binaries` signs binaries keyless; attaches `.sig`/`.crt` files.
6. `cosign-images` signs images by digest; signature stored alongside in OCI registry.

### License exception lifecycle

1. Developer adds new dep with warn-tier license.
2. `license-policy` fails with: `WARN_UNEXCEPTED: <module> license=<X> — add entry to docs/LICENSE_EXCEPTIONS.md`.
3. Developer adds row with rationale + linking-model assessment + today's date.
4. CI rerun passes.
5. Quarterly manual review prunes entries for removed deps.

### Operator verification flow

1. Operator downloads `lakehouse-logs`, `.sig`, `.crt`.
2. Runs `cosign verify-blob lakehouse-logs --signature lakehouse-logs.sig --certificate lakehouse-logs.crt --certificate-identity-regexp '^https://github.com/ReliablyObserve/' --certificate-oidc-issuer https://token.actions.githubusercontent.com`.
3. Output confirms binary was built by a workflow in the org with OIDC-issued cert.
4. For SLSA: `slsa-verifier verify-artifact lakehouse-logs --provenance-path lakehouse-v0.X.Y.intoto.jsonl --source-uri github.com/ReliablyObserve/victoria-lakehouse --source-tag v0.X.Y`.

---

## Error Handling

### License auto-detection misidentifies a dep
- Add entry to `license-overrides.yaml` with reason + verified date.
- Audit consults overrides before classification.

### Module has dual-license (`MIT OR Apache-2.0`)
- Audit picks most permissive allow-tier; alphabetical tiebreak.
- Override can pin a specific choice.

### Module has no LICENSE file
- `TierUnknown` → CI failure with explicit remediation message.

### Vendored deps
- Audit scans `vendor/` too if present.

### Transitive dep license changes between releases
- Nightly SBOM regeneration catches transitive churn within 24h.
- Upstream-check workflow extension deferred.

### CycloneDX generator missing license info
- SBOM shows `NOASSERTION` (CycloneDX standard).
- Audit test is authoritative; SBOM is for downstream consumers.

### Provenance generator requires GitHub-hosted runners
- `slsa-github-generator` only supports `ubuntu-latest` / `windows-latest`.
- Self-hosted runners drop to SLSA Level 1.

### Cosign OIDC token unavailable
- Misconfigured `permissions` → signing fails.
- `workflow-lint` test asserts `id-token: write` is declared.

### Signed but unpublished image
- All tags signed at release; job loops over `:vX.Y.Z` and `:latest`.

### SLSA-verifier rejects valid provenance
- Common cause: `--source-tag` doesn't match release tag.
- Remediation in `SUPPLY_CHAIN.md`.

### Arrow IPC roundtrip loses metadata
- Arrow doesn't preserve Parquet KV in IPC format.
- Documented in test comment; Subsystem C owns KV-metadata persistence.

### Arrow IPC version mismatch
- Format version pinned to latest stable; bumped explicitly.

### `LICENSE_EXCEPTIONS.md` becomes stale
- Quarterly review prunes dangling entries; deferred to manual until file grows.

### Sigstore Fulcio CA rotation
- Older signatures remain verifiable via transparency log; no action needed.

### Multi-platform image signing
- Cosign signs manifest index, covering all platforms.
- `SUPPLY_CHAIN.md` notes platform-specific digest verification differences.

### Audit module-by-module mode
- `go list` runs once per module path; results deduplicated by module.

---

## Testing Strategy

### Unit tests (`tests/community-standards/`)

- `TestLicensePolicyLoads` — YAML parses; tier lists non-empty; no SPDX id in multiple tiers.
- `TestLicenseOverridesLoadCleanly` — every override has `verified` date in ISO format; no duplicate paths.
- `TestClassifyAllow` / `TestClassifyWarn` / `TestClassifyDeny` / `TestClassifyUnknown`.
- `TestLicenseExceptionsParse` — `LICENSE_EXCEPTIONS.md` parses; every row has rationale + linking-model.

### Arrow IPC tests

- `TestArrowIPCRoundtripLogs`
- `TestArrowIPCRoundtripTraces`
- `TestArrowIPCPreservesNanoTimestamps`
- `TestArrowIPCPreservesUnicodeStrings`

### Integration tests (build tag `community_standards`)

- `TestFullDepAuditPassesOnMain` — runs full audit against today's `go.mod`.
- `TestSBOMGeneratorProducesValidCycloneDX` — invokes `cyclonedx-gomod` against tiny test module; validates against CycloneDX 1.5 JSON schema.

### Workflow lint

- `TestReleaseWorkflowHasIdTokenPermission` — cosign + SLSA jobs declare `id-token: write`.
- `TestSBOMWorkflowUploadsArtifact`.
- `TestLicensePolicyWorkflowRunsOnPR`.

### Synthesis

- `TestDenyTierAddedFailsAudit` — synthetic GPL-3.0 module; asserts failure.
- `TestWarnTierWithoutExceptionFailsAudit` — synthetic MPL-2.0 without exception; asserts failure.
- `TestWarnTierWithExceptionPasses` — same MPL-2.0 with matching exception; asserts pass.

### Regression / governance

- `TestExceptionsTableNoDuplicatePathRows`.
- `TestExceptionsTableNoStaleVerifiedDates` — entries older than 12 months emit `STALE_VERIFICATION:` log (warn, not block).

### Negative-control proofs

Every load-bearing assertion has a comment naming the production code that must break it.

### CI integration matrix

| Job | Trigger | Required for merge |
|-----|---------|--------------------|
| `license-policy` | every PR | yes |
| `arrow-ipc-roundtrip` | PR touching schema/storage | yes |
| `workflow-lint` | PR touching `.github/workflows/` | yes |
| `sbom-build` | PR touching go.mod/go.sum + release + nightly | release: yes; PR: advisory |
| `slsa-provenance` | release | yes |
| `cosign-binaries` | release | yes |
| `cosign-images` | release | yes |

Path triggers:
- `go.mod`, `go.sum`, `lakehouse-traces/go.mod`, `lakehouse-traces/go.sum`
- `tests/community-standards/**`
- `docs/LICENSE_EXCEPTIONS.md`, `docs/SUPPLY_CHAIN.md`, `docs/CNCF-ALIGNMENT.md`
- `.github/workflows/sbom.yaml`, `license-policy.yaml`, `release.yaml`
- `internal/schema/**`, `internal/storage/parquets3/**`

### Local verification

Makefile targets:
- `make license-audit`
- `make sbom`
- `make verify-release VERSION=v0.X.Y`

---

## Migration Plan & Deliverables

Five PRs landed sequentially. Each independently mergeable; each leaves CI green.

| PR | Scope | New files (approx LoC) | Risk |
|----|-------|------------------------|------|
| **E1** | License policy + audit + first gate | `license-policy.yaml`, `license-overrides.yaml`, `license_audit_test.go` (~300), unit tests (~250), `docs/LICENSE_EXCEPTIONS.md`, `.github/workflows/license-policy.yaml` | Low — data + Go |
| **E2** | CycloneDX SBOM workflow | `.github/workflows/sbom.yaml`, Makefile `make sbom`, `TestSBOMGeneratorProducesValidCycloneDX` | Low — additive |
| **E3** | Arrow IPC roundtrip | `arrow_ipc_test.go` (~250). New CI workflow `.github/workflows/arrow-ipc.yaml` | Low — pure Go |
| **E4** | SLSA L2 + Sigstore signing | Extend `release.yaml` with `slsa-provenance`, `cosign-binaries`, `cosign-images` jobs. `workflow_lint_test.go`. `docs/SUPPLY_CHAIN.md` | Medium — touches release path |
| **E5** | CNCF doc + governance polish | `docs/CNCF-ALIGNMENT.md`, `make verify-release` target, `TestExceptionsTableNoStaleVerifiedDates` | Low — docs + automation |

**Ordering rationale:**
- E1 lowest risk first; establishes policy + first gate.
- E2 adds SBOM independent of policy.
- E3 is loose Arrow item bundled here; pure Go, no infra.
- E4 modifies release path; lands after gates stable.
- E5 closes loose ends.

---

## Definition of Done

### License policy
- [ ] `license-policy.yaml` defines allow/warn/deny SPDX tiers.
- [ ] `license-overrides.yaml` exists for misdetected licenses.
- [ ] `docs/LICENSE_EXCEPTIONS.md` exists; every active warn-tier dep has rationale + linking-model + verified date.
- [ ] CI workflow blocks PRs introducing deny or unexcepted warn licenses.
- [ ] Audit covers both module paths.

### CycloneDX SBOM
- [ ] `sbom.yaml` generates per-binary SBOM on PR (advisory), release (required), nightly cron.
- [ ] SBOM attached to release as `sbom-lakehouse-logs.cyclonedx.json` and `sbom-lakehouse-traces.cyclonedx.json`.
- [ ] Per-image SBOM via syft attached to release.
- [ ] `make sbom` generates SBOMs locally.

### SLSA Build Level 2
- [ ] `slsa-provenance` job uses `slsa-framework/slsa-github-generator` upstream workflow.
- [ ] Provenance file attached to release.
- [ ] `slsa-verifier verify-artifact` documented in SUPPLY_CHAIN.md.

### Sigstore signing
- [ ] `cosign-binaries` signs both binaries keyless; uploads `.sig` and `.crt`.
- [ ] `cosign-images` signs both container images.
- [ ] No long-lived keys checked in or stored as secrets.
- [ ] Verification commands documented in `SUPPLY_CHAIN.md`.

### Arrow IPC compatibility
- [ ] Roundtrip test passes for LogRow and TraceRow.
- [ ] Nano-timestamp precision preserved.
- [ ] Unicode strings preserved.
- [ ] Test file documents Parquet KV metadata is intentionally not preserved.

### CNCF alignment
- [ ] `docs/CNCF-ALIGNMENT.md` exists and maps features to CNCF Observability whitepaper.
- [ ] Compliance areas reference B/C as authoritative.

### Governance polish
- [ ] `TestExceptionsTableNoStaleVerifiedDates` emits warn log for entries older than 12 months.
- [ ] Workflow-lint tests pass.
- [ ] `make verify-release VERSION=v0.X.Y` runs cosign + slsa-verifier; exits 0 on success.

### Negative-control proofs
- [ ] Every load-bearing assertion has a comment naming what must break it.

---

## Out of Scope

Deferred to other subsystems:
- OpenTelemetry semantic conventions for columns — Subsystem C.
- Protocol conformance (Loki, OTLP, Jaeger) — Subsystem B.
- Apache Parquet spec compliance (multi-reader) — Subsystem C.

Deferred indefinitely:
- **SLSA Build Level 3** — requires hardened runner image + two-party review. LH at Level 2 already above most OSS projects.
- **Dual SBOM (CycloneDX + SPDX)** — CycloneDX covers modern needs; SPDX can be added later if procurement asks.
- **Reproducible builds** — would require Bazel-style sandboxing.
- **in-toto v2 attestations beyond SLSA provenance** — over-engineering at current scale.
- **License-change upstream poller** — deferred until needed.
- **Quarterly stale-exception PR bot** — manual review for now.

---

## Open Questions

None — all decisions resolved during brainstorming:
- Scope: production-grade governance (license + Arrow IPC + SBOM + SLSA + Sigstore + CNCF).
- License policy: tiered allow / warn / deny + `LICENSE_EXCEPTIONS.md`.
- SLSA / SBOM: Level 2 + CycloneDX + Sigstore keyless signing.
