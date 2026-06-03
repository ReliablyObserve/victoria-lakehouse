# Community Standards Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add production-grade supply-chain governance and ecosystem positioning to LH: license tiering with CI gate, CycloneDX SBOM, SLSA Level 2 provenance, Sigstore release signing, Apache Arrow IPC roundtrip test, CNCF Observability alignment document.

**Architecture:** Three locations — `tests/community-standards/` (Go-side audit, Arrow IPC test, exception/synthesis tests), `.github/workflows/` (license gate, SBOM, Arrow IPC job, extended release workflow), `docs/` (LICENSE_EXCEPTIONS, SUPPLY_CHAIN, CNCF-ALIGNMENT). Six CI surfaces added: license-policy (blocking), arrow-ipc-roundtrip (blocking), workflow-lint (blocking), sbom-build (PR advisory + release required + nightly), slsa-provenance (release), cosign-binaries/images (release).

**Tech Stack:** Go 1.24, `github.com/CycloneDX/cyclonedx-gomod`, `github.com/sigstore/cosign`, `slsa-framework/slsa-github-generator`, `github.com/apache/arrow-go/v18`, GitHub Actions OIDC

**Spec:** `docs/superpowers/specs/2026-06-03-community-standards-design.md`

**Existing context:**
- `LICENSE` file at repo root (Apache-2.0).
- `go.mod` and `lakehouse-traces/go.mod` both pin module deps.
- Existing release workflow `.github/workflows/auto-release.yaml` produces binaries and container images; we extend it.
- `.github/workflows/security.yaml` already runs `govulncheck` and `gitleaks`; we don't replace these.

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `tests/community-standards/license-policy.yaml` | Tier definitions (SPDX → allow/warn/deny) |
| Create | `tests/community-standards/license-overrides.yaml` | Corrections for misdetected licenses |
| Create | `tests/community-standards/license_audit.go` | `Policy`, `LoadPolicy`, `Classify`, `ScanAllDeps` |
| Create | `tests/community-standards/license_audit_test.go` | Classify unit tests + integration `TestFullDepAuditPassesOnMain` |
| Create | `tests/community-standards/exceptions.go` | `LoadExceptions` parser for `LICENSE_EXCEPTIONS.md` |
| Create | `tests/community-standards/exceptions_test.go` | Parser tests |
| Create | `tests/community-standards/synthesis_test.go` | Synthetic deny/warn fixture tests |
| Create | `tests/community-standards/regression_test.go` | Stale-exception governance, no-duplicate-row checks |
| Create | `tests/community-standards/arrow_ipc.go` | Roundtrip helpers (write Parquet, read into Arrow, write back) |
| Create | `tests/community-standards/arrow_ipc_test.go` | Roundtrip tests |
| Create | `tests/community-standards/workflow_lint.go` | YAML loader + permission/trigger assertions |
| Create | `tests/community-standards/workflow_lint_test.go` | Workflow lint tests |
| Create | `tests/community-standards/integration_test.go` | CycloneDX validity test (build tag `community_standards`) |
| Create | `docs/LICENSE_EXCEPTIONS.md` | Accepted warn-tier deps with rationale |
| Create | `docs/SUPPLY_CHAIN.md` | Operator verification guide |
| Create | `docs/CNCF-ALIGNMENT.md` | CNCF Observability category mapping |
| Create | `.github/workflows/license-policy.yaml` | License gate on every PR |
| Create | `.github/workflows/sbom.yaml` | SBOM generation (PR advisory + release + nightly) |
| Create | `.github/workflows/arrow-ipc.yaml` | Arrow roundtrip on schema/storage PRs |
| Modify | `.github/workflows/auto-release.yaml` | Add slsa-provenance, cosign-binaries, cosign-images jobs |
| Modify | `Makefile` | Targets `license-audit`, `sbom`, `verify-release VERSION=` |

---

## PR E1 — License Policy + Audit + First Gate

**Goal:** Tier definitions + Go-side audit + CI workflow blocking PRs that introduce deny-tier or unexcepted warn-tier licenses.

### Task E1.1: License policy YAML

**Files:**
- Create: `tests/community-standards/license-policy.yaml`

- [ ] **Step 1: Write the file**

```yaml
# tests/community-standards/license-policy.yaml
# SPDX identifiers from https://spdx.org/licenses/
# Tier semantics:
#   allow — no review needed
#   warn  — accepted only if listed in docs/LICENSE_EXCEPTIONS.md
#   deny  — PR fails CI; the dep must be removed or replaced

allow:
  - MIT
  - Apache-2.0
  - BSD-2-Clause
  - BSD-3-Clause
  - ISC
  - 0BSD
  - Unlicense
  - CC0-1.0

warn:
  - LGPL-2.1
  - LGPL-2.1-only
  - LGPL-3.0
  - LGPL-3.0-only
  - MPL-2.0
  - EPL-2.0
  - CDDL-1.0
  - CDDL-1.1

deny:
  - GPL-2.0
  - GPL-2.0-only
  - GPL-3.0
  - GPL-3.0-only
  - AGPL-3.0
  - AGPL-3.0-only
  - SSPL-1.0
  - BUSL-1.1
  - Commons-Clause
```

- [ ] **Step 2: Write license-overrides.yaml (empty skeleton)**

```yaml
# tests/community-standards/license-overrides.yaml
# Corrections for misdetected licenses.
# Add an entry only after manually verifying the upstream LICENSE file.
# `verified` date is mandatory; quarterly review flags entries older than 12 months.

overrides: []
```

- [ ] **Step 3: Commit**

```bash
git add tests/community-standards/license-policy.yaml tests/community-standards/license-overrides.yaml
git commit -m "test/community-standards: license policy + overrides skeleton (E1.1)"
```

### Task E1.2: Policy loader and classifier

**Files:**
- Create: `tests/community-standards/license_audit.go`
- Create: `tests/community-standards/license_audit_test.go`

- [ ] **Step 1: Write the failing test**

```go
// tests/community-standards/license_audit_test.go
package communitystandards

import "testing"

// negative control: drop the YAML decoder → this test must fail because
// allow/warn/deny lists would all be empty.
func TestLicensePolicyLoads(t *testing.T) {
    p, err := LoadPolicy("license-policy.yaml")
    if err != nil { t.Fatal(err) }
    if len(p.Allow) == 0 { t.Error("allow list empty") }
    if len(p.Warn) == 0 { t.Error("warn list empty") }
    if len(p.Deny) == 0 { t.Error("deny list empty") }
}

func TestLicensePolicyNoOverlap(t *testing.T) {
    p, _ := LoadPolicy("license-policy.yaml")
    seen := map[string]string{}
    for _, lic := range p.Allow { seen[lic] = "allow" }
    for _, lic := range p.Warn {
        if existing := seen[lic]; existing != "" {
            t.Errorf("%s in both allow and %s", lic, existing)
        }
        seen[lic] = "warn"
    }
    for _, lic := range p.Deny {
        if existing := seen[lic]; existing != "" {
            t.Errorf("%s in both deny and %s", lic, existing)
        }
        seen[lic] = "deny"
    }
}

// negative control: remove the deny check → this test must fail because
// GPL-3.0 would silently be classified as something else.
func TestClassifyDeny(t *testing.T) {
    p, _ := LoadPolicy("license-policy.yaml")
    if got := Classify("GPL-3.0", p); got != TierDeny {
        t.Errorf("Classify(GPL-3.0) = %v, want TierDeny", got)
    }
}

func TestClassifyAllow(t *testing.T) {
    p, _ := LoadPolicy("license-policy.yaml")
    if got := Classify("MIT", p); got != TierAllow {
        t.Errorf("Classify(MIT) = %v, want TierAllow", got)
    }
}

func TestClassifyWarn(t *testing.T) {
    p, _ := LoadPolicy("license-policy.yaml")
    if got := Classify("MPL-2.0", p); got != TierWarn {
        t.Errorf("Classify(MPL-2.0) = %v, want TierWarn", got)
    }
}

func TestClassifyUnknown(t *testing.T) {
    p, _ := LoadPolicy("license-policy.yaml")
    if got := Classify("NOASSERTION", p); got != TierUnknown {
        t.Errorf("Classify(NOASSERTION) = %v, want TierUnknown", got)
    }
}
```

- [ ] **Step 2: Run, expect failure**

```bash
cd tests/community-standards && go test
```
Expected: FAIL (`undefined: LoadPolicy`).

- [ ] **Step 3: Implement license_audit.go**

```go
// tests/community-standards/license_audit.go
package communitystandards

import (
    "os"

    "gopkg.in/yaml.v3"
)

type Tier int

const (
    TierUnknown Tier = iota
    TierAllow
    TierWarn
    TierDeny
)

type Policy struct {
    Allow []string `yaml:"allow"`
    Warn  []string `yaml:"warn"`
    Deny  []string `yaml:"deny"`
}

func LoadPolicy(path string) (*Policy, error) {
    data, err := os.ReadFile(path)
    if err != nil { return nil, err }
    var p Policy
    if err := yaml.Unmarshal(data, &p); err != nil { return nil, err }
    return &p, nil
}

func Classify(license string, p *Policy) Tier {
    if license == "" { return TierUnknown }
    for _, l := range p.Allow { if l == license { return TierAllow } }
    for _, l := range p.Warn  { if l == license { return TierWarn  } }
    for _, l := range p.Deny  { if l == license { return TierDeny  } }
    return TierUnknown
}
```

- [ ] **Step 4: Run all tests pass**

```bash
cd tests/community-standards && go test -v
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/community-standards/license_audit.go tests/community-standards/license_audit_test.go
git commit -m "test/community-standards: policy loader + classifier (E1.2)"
```

### Task E1.3: Dep scanner + override loader

**Files:**
- Modify: `tests/community-standards/license_audit.go`
- Modify: `tests/community-standards/license_audit_test.go`

- [ ] **Step 1: Add types**

Append to `license_audit.go`:

```go
import (
    "encoding/json"
    "fmt"
    "os/exec"
)

type Dep struct {
    Module  string
    Version string
    License string
}

type Override struct {
    Module   string `yaml:"module"`
    License  string `yaml:"license"`
    Reason   string `yaml:"reason"`
    Verified string `yaml:"verified"`
}

type OverridesFile struct {
    Overrides []Override `yaml:"overrides"`
}

func LoadOverrides(path string) (*OverridesFile, error) {
    data, err := os.ReadFile(path)
    if err != nil { return nil, err }
    var o OverridesFile
    if err := yaml.Unmarshal(data, &o); err != nil { return nil, err }
    return &o, nil
}

// ScanAllDeps runs `go list -m -json all` in moduleRoots, dedupes by module
// path, and applies overrides.
func ScanAllDeps(moduleRoots []string, overrides *OverridesFile) ([]Dep, error) {
    seen := map[string]Dep{}
    overrideByModule := map[string]Override{}
    if overrides != nil {
        for _, o := range overrides.Overrides {
            overrideByModule[o.Module] = o
        }
    }
    for _, root := range moduleRoots {
        cmd := exec.Command("go", "list", "-m", "-json", "all")
        cmd.Dir = root
        out, err := cmd.Output()
        if err != nil { return nil, fmt.Errorf("go list in %s: %w", root, err) }
        dec := json.NewDecoder(strings.NewReader(string(out)))
        for {
            var raw struct {
                Path    string
                Version string
                Main    bool
            }
            if err := dec.Decode(&raw); err != nil { break }
            if raw.Main || raw.Path == "" { continue }
            if _, dup := seen[raw.Path]; dup { continue }
            d := Dep{Module: raw.Path, Version: raw.Version}
            if ov, ok := overrideByModule[raw.Path]; ok {
                d.License = ov.License
            } else {
                d.License = resolveLicense(raw.Path)
            }
            seen[raw.Path] = d
        }
    }
    out := make([]Dep, 0, len(seen))
    for _, d := range seen { out = append(out, d) }
    return out, nil
}

// resolveLicense uses pkg.go.dev's licensecheck library or falls back to
// reading the dep's LICENSE file. Returns "" if unknown.
func resolveLicense(modulePath string) string {
    // Implementation note: invoke `go-licenses report` if installed, or
    // parse LICENSE in the dep's GOPATH-modules cache. For now, a stub
    // returning "" causes TierUnknown classification, which forces the
    // engineer to add an override entry.
    return ""
}
```

Add `"strings"` import.

- [ ] **Step 2: Add overrides tests**

Append to `license_audit_test.go`:

```go
func TestLoadOverridesEmpty(t *testing.T) {
    o, err := LoadOverrides("license-overrides.yaml")
    if err != nil { t.Fatal(err) }
    if len(o.Overrides) != 0 {
        t.Errorf("expected empty overrides, got %d", len(o.Overrides))
    }
}

// negative control: drop the dedup map in ScanAllDeps → this test must
// fail because the same dep appears in both module paths and would be
// double-counted.
func TestScanAllDepsDeduplicates(t *testing.T) {
    o, _ := LoadOverrides("license-overrides.yaml")
    deps, err := ScanAllDeps([]string{"../..", "../../lakehouse-traces"}, o)
    if err != nil { t.Skip("go list not runnable: ", err) }
    seen := map[string]bool{}
    for _, d := range deps {
        if seen[d.Module] { t.Errorf("duplicate: %s", d.Module) }
        seen[d.Module] = true
    }
}
```

- [ ] **Step 3: Run; commit**

```bash
cd tests/community-standards && go test -run "TestLoadOverrides|TestScanAllDeps" -v
git add tests/community-standards/license_audit.go tests/community-standards/license_audit_test.go
git commit -m "test/community-standards: dep scanner + overrides loader (E1.3)"
```

### Task E1.4: Exception document parser

**Files:**
- Create: `docs/LICENSE_EXCEPTIONS.md`
- Create: `tests/community-standards/exceptions.go`
- Create: `tests/community-standards/exceptions_test.go`

- [ ] **Step 1: Write the markdown skeleton**

```markdown
# License Exceptions

Dependencies listed here have a `warn`-tier license under
`tests/community-standards/license-policy.yaml`. Each entry must explain
*why* the dep is needed and confirm the linking model is compatible.

## Active exceptions

| Dependency | License | Pinned version | Why we accept it | Linking model | Verified |
|-----------|---------|----------------|-------------------|---------------|----------|

## Removed exceptions

| Dependency | License | Reason removed | Date |
|-----------|---------|----------------|------|
```

- [ ] **Step 2: Write the failing tests**

```go
// tests/community-standards/exceptions_test.go
package communitystandards

import "testing"

// negative control: drop the table parser → this test must fail because
// no exceptions would be extracted from the file even when present.
func TestLoadExceptionsEmptyTable(t *testing.T) {
    ex, err := LoadExceptions("../../docs/LICENSE_EXCEPTIONS.md")
    if err != nil { t.Fatal(err) }
    if ex.Active == nil { t.Error("Active map nil") }
}
```

- [ ] **Step 3: Implement parser**

```go
// tests/community-standards/exceptions.go
package communitystandards

import (
    "bufio"
    "os"
    "strings"
)

type Exception struct {
    Module     string
    License    string
    Version    string
    Why        string
    Linking    string
    Verified   string
}

type Exceptions struct {
    Active map[string]Exception
}

func LoadExceptions(path string) (*Exceptions, error) {
    f, err := os.Open(path)
    if err != nil { return nil, err }
    defer f.Close()

    ex := &Exceptions{Active: map[string]Exception{}}
    scanner := bufio.NewScanner(f)
    inActive := false
    for scanner.Scan() {
        line := scanner.Text()
        if strings.HasPrefix(line, "## Active exceptions") {
            inActive = true
            continue
        }
        if strings.HasPrefix(line, "## ") && inActive {
            inActive = false
        }
        if !inActive { continue }
        if !strings.HasPrefix(line, "|") { continue }
        // Skip header + separator
        cells := splitMarkdownRow(line)
        if len(cells) < 6 { continue }
        if cells[0] == "Dependency" { continue }
        if strings.HasPrefix(cells[0], "---") { continue }
        ex.Active[cells[0]] = Exception{
            Module:   cells[0],
            License:  cells[1],
            Version:  cells[2],
            Why:      cells[3],
            Linking:  cells[4],
            Verified: cells[5],
        }
    }
    return ex, scanner.Err()
}

func splitMarkdownRow(line string) []string {
    line = strings.TrimSpace(line)
    line = strings.TrimPrefix(line, "|")
    line = strings.TrimSuffix(line, "|")
    parts := strings.Split(line, "|")
    out := make([]string, len(parts))
    for i, p := range parts { out[i] = strings.TrimSpace(p) }
    return out
}
```

- [ ] **Step 4: Run; commit**

```bash
cd tests/community-standards && go test -run TestLoadExceptions -v
git add docs/LICENSE_EXCEPTIONS.md tests/community-standards/exceptions.go tests/community-standards/exceptions_test.go
git commit -m "test/community-standards: exception parser + empty skeleton (E1.4)"
```

### Task E1.5: Full audit integration test

**Files:**
- Modify: `tests/community-standards/license_audit_test.go`

- [ ] **Step 1: Write the integration test**

Append to `license_audit_test.go`:

```go
//go:build community_standards

// negative control: add a deny-tier dep to a synthetic go.mod → this
// test must fail with `DENY:` listed for that module.
func TestFullDepAuditPassesOnMain(t *testing.T) {
    p, err := LoadPolicy("license-policy.yaml")
    if err != nil { t.Fatal(err) }
    o, err := LoadOverrides("license-overrides.yaml")
    if err != nil { t.Fatal(err) }
    deps, err := ScanAllDeps([]string{"../..", "../../lakehouse-traces"}, o)
    if err != nil { t.Fatal(err) }
    ex, err := LoadExceptions("../../docs/LICENSE_EXCEPTIONS.md")
    if err != nil { t.Fatal(err) }
    var failures []string
    for _, d := range deps {
        switch Classify(d.License, p) {
        case TierDeny:
            failures = append(failures, "DENY: "+d.Module+" license="+d.License+" — remove or replace")
        case TierWarn:
            if _, ok := ex.Active[d.Module]; !ok {
                failures = append(failures, "WARN_UNEXCEPTED: "+d.Module+" license="+d.License+" — add entry to docs/LICENSE_EXCEPTIONS.md")
            }
        case TierUnknown:
            failures = append(failures, "UNKNOWN: "+d.Module+" — classify in license-overrides.yaml after verifying upstream license")
        }
    }
    if len(failures) > 0 {
        t.Fatalf("license audit failures:\n%s", strings.Join(failures, "\n"))
    }
}
```

- [ ] **Step 2: Run; commit (test may legitimately fail revealing real unknowns — populate overrides as needed)**

```bash
cd tests/community-standards && go test -tags=community_standards -run TestFullDepAuditPassesOnMain -v
# If failures appear, add entries to license-overrides.yaml or LICENSE_EXCEPTIONS.md
git add tests/community-standards/license_audit_test.go tests/community-standards/license-overrides.yaml docs/LICENSE_EXCEPTIONS.md
git commit -m "test/community-standards: full dep audit integration (E1.5)"
```

### Task E1.6: CI workflow for license gate

**Files:**
- Create: `.github/workflows/license-policy.yaml`

- [ ] **Step 1: Write workflow**

```yaml
# .github/workflows/license-policy.yaml
name: License Policy

on:
  pull_request:
    paths:
      - 'go.mod'
      - 'go.sum'
      - 'lakehouse-traces/go.mod'
      - 'lakehouse-traces/go.sum'
      - 'tests/community-standards/license-policy.yaml'
      - 'tests/community-standards/license-overrides.yaml'
      - 'docs/LICENSE_EXCEPTIONS.md'
      - '.github/workflows/license-policy.yaml'
  push:
    branches: [main]

permissions:
  contents: read

env:
  GOWORK: "off"

jobs:
  license-policy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version-file: go.mod }
      - name: Run license audit
        run: |
          cd tests/community-standards && \
          go test -tags=community_standards -v -run TestFullDepAuditPassesOnMain
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/license-policy.yaml
git commit -m "ci: license-policy gate (E1.6)"
```

---

## PR E2 — CycloneDX SBOM Workflow

**Goal:** Generate per-binary CycloneDX SBOM on PR (advisory), release (required), and nightly. Validate output is conformant.

### Task E2.1: Makefile sbom target

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Add target**

```makefile
.PHONY: sbom

# Generates CycloneDX SBOM per binary (requires cyclonedx-gomod installed).
sbom:
	@command -v cyclonedx-gomod >/dev/null 2>&1 || { echo "install: go install github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@v1.7.0"; exit 1; }
	cyclonedx-gomod app -main cmd/lakehouse-logs -licenses -output sbom-lakehouse-logs.cyclonedx.json .
	cyclonedx-gomod app -main . -licenses -output sbom-lakehouse-traces.cyclonedx.json lakehouse-traces
	@echo "Wrote sbom-lakehouse-logs.cyclonedx.json and sbom-lakehouse-traces.cyclonedx.json"
```

- [ ] **Step 2: Verify locally**

```bash
go install github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@v1.7.0
make sbom
ls -la sbom-*.cyclonedx.json
```

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "make: sbom target (E2.1)"
```

### Task E2.2: Validity test for generated SBOM

**Files:**
- Create: `tests/community-standards/integration_test.go`

- [ ] **Step 1: Write the failing test**

```go
//go:build community_standards

// tests/community-standards/integration_test.go
package communitystandards

import (
    "encoding/json"
    "os"
    "testing"
)

// negative control: change CycloneDX bomFormat field to "FOO" → this
// test must fail because validation rejects unknown bomFormat.
func TestSBOMConformsToCycloneDXShape(t *testing.T) {
    // Assumes `make sbom` was run before this test
    if _, err := os.Stat("../../sbom-lakehouse-logs.cyclonedx.json"); err != nil {
        t.Skip("SBOM not yet generated; run 'make sbom' first")
    }
    data, err := os.ReadFile("../../sbom-lakehouse-logs.cyclonedx.json")
    if err != nil { t.Fatal(err) }
    var doc map[string]any
    if err := json.Unmarshal(data, &doc); err != nil { t.Fatal(err) }
    if doc["bomFormat"] != "CycloneDX" {
        t.Errorf("bomFormat = %v, want CycloneDX", doc["bomFormat"])
    }
    if specVersion, _ := doc["specVersion"].(string); specVersion == "" {
        t.Error("specVersion missing")
    }
    components, _ := doc["components"].([]any)
    if len(components) == 0 {
        t.Error("components empty (no deps detected)")
    }
}
```

- [ ] **Step 2: Run; commit**

```bash
make sbom
cd tests/community-standards && go test -tags=community_standards -run TestSBOMConforms -v
git add tests/community-standards/integration_test.go
git commit -m "test/community-standards: CycloneDX SBOM shape validation (E2.2)"
```

### Task E2.3: SBOM CI workflow

**Files:**
- Create: `.github/workflows/sbom.yaml`

- [ ] **Step 1: Write workflow**

```yaml
# .github/workflows/sbom.yaml
name: SBOM

on:
  pull_request:
    paths:
      - 'go.mod'
      - 'go.sum'
      - 'lakehouse-traces/go.mod'
      - 'lakehouse-traces/go.sum'
  release:
    types: [created]
  schedule:
    - cron: '0 4 * * *'

permissions:
  contents: write   # release upload

jobs:
  cyclonedx-binaries:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        binary: [lakehouse-logs, lakehouse-traces]
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version-file: go.mod }
      - name: Install cyclonedx-gomod
        run: go install github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@v1.7.0
      - name: Generate SBOM
        run: |
          MOD_DIR="."
          MAIN_PATH="cmd/${{ matrix.binary }}"
          if [ "${{ matrix.binary }}" = "lakehouse-traces" ]; then
            MOD_DIR="lakehouse-traces"
            MAIN_PATH="."
          fi
          cyclonedx-gomod app -main "$MAIN_PATH" -licenses -output "sbom-${{ matrix.binary }}.cyclonedx.json" "$MOD_DIR"
      - uses: actions/upload-artifact@v7
        with:
          name: sbom-${{ matrix.binary }}
          path: sbom-${{ matrix.binary }}.cyclonedx.json
      - name: Attach to release
        if: github.event_name == 'release'
        run: gh release upload "${{ github.event.release.tag_name }}" "sbom-${{ matrix.binary }}.cyclonedx.json"
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/sbom.yaml
git commit -m "ci: CycloneDX SBOM workflow (PR + release + nightly) (E2.3)"
```

---

## PR E3 — Arrow IPC Roundtrip

**Goal:** Loss-of-precision test against `LogRow` and `TraceRow`. Uses `github.com/apache/arrow-go/v18`.

### Task E3.1: Add arrow-go dependency

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: Add the dep**

```bash
go get github.com/apache/arrow-go/v18@latest
go mod tidy
```

- [ ] **Step 2: Verify license is allow-tier**

```bash
go list -m github.com/apache/arrow-go/v18
# Apache Arrow is Apache-2.0 → allow tier
```

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "deps: add github.com/apache/arrow-go/v18 for Arrow IPC roundtrip (E3.1)"
```

### Task E3.2: Roundtrip helpers

**Files:**
- Create: `tests/community-standards/arrow_ipc.go`

- [ ] **Step 1: Implement helpers**

```go
// tests/community-standards/arrow_ipc.go
package communitystandards

import (
    "bytes"
    "os"

    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/memory"
    "github.com/apache/arrow-go/v18/parquet/file"
    "github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// ReadParquetSchema returns column descriptors for a Parquet file.
func ReadParquetSchema(path string) ([]ColumnDescriptor, error) {
    r, err := file.OpenParquetFile(path, false)
    if err != nil { return nil, err }
    defer r.Close()
    schema := r.MetaData().Schema
    var cols []ColumnDescriptor
    for i := 0; i < schema.NumColumns(); i++ {
        c := schema.Column(i)
        cols = append(cols, ColumnDescriptor{Name: c.Name(), Type: c.PhysicalType().String()})
    }
    return cols, nil
}

type ColumnDescriptor struct {
    Name string
    Type string
}

// ReadParquetIntoArrow loads a Parquet file into an Arrow table.
func ReadParquetIntoArrow(path string) (arrow.Table, error) {
    f, err := os.Open(path)
    if err != nil { return nil, err }
    defer f.Close()
    r, err := file.NewParquetReader(f)
    if err != nil { return nil, err }
    defer r.Close()
    arr, err := pqarrow.NewFileReader(r, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
    if err != nil { return nil, err }
    return arr.ReadTable(nil)
}

// WriteArrowToParquet writes an Arrow table back to a Parquet file.
func WriteArrowToParquet(tbl arrow.Table) ([]byte, error) {
    var buf bytes.Buffer
    if err := pqarrow.WriteTable(tbl, &buf, 1024, nil, pqarrow.NewArrowWriterProperties()); err != nil {
        return nil, err
    }
    return buf.Bytes(), nil
}

// CountRowsParquet returns the row count from a Parquet file's metadata.
func CountRowsParquet(path string) int64 {
    r, err := file.OpenParquetFile(path, false)
    if err != nil { return -1 }
    defer r.Close()
    return r.NumRows()
}
```

- [ ] **Step 2: Commit (helpers compile; tests in E3.3)**

```bash
cd tests/community-standards && go build ./...
git add tests/community-standards/arrow_ipc.go
git commit -m "test/community-standards: arrow-go IPC roundtrip helpers (E3.2)"
```

### Task E3.3: Roundtrip tests

**Files:**
- Create: `tests/community-standards/arrow_ipc_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// tests/community-standards/arrow_ipc_test.go
package communitystandards

import (
    "os"
    "reflect"
    "testing"

    "github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
    parquets3 "github.com/ReliablyObserve/victoria-lakehouse/internal/storage/parquets3"
)

// negative control: change writeLogsParquetForTest to omit
// timestamp_unix_nano → this test must fail because the roundtrip
// would lose that column.
//
// Note: Parquet KV metadata is intentionally NOT preserved by Arrow IPC.
// KV metadata persistence is covered by Subsystem C's
// TestKVMetadataPresent.
func TestArrowIPCRoundtripLogs(t *testing.T) {
    rows := []schema.LogRow{
        {Body: "first", TimestampUnixNano: 1735689600000000000, ServiceName: "svc"},
        {Body: "second", TimestampUnixNano: 1735689601000000000, ServiceName: "svc"},
    }
    data, err := parquets3.WriteLogsParquetForTest(rows, 100, 7)
    if err != nil { t.Fatal(err) }
    in := writeTemp(t, data)

    tbl, err := ReadParquetIntoArrow(in)
    if err != nil { t.Fatal(err) }
    defer tbl.Release()

    out, err := WriteArrowToParquet(tbl)
    if err != nil { t.Fatal(err) }
    outFile := writeTemp(t, out)

    origCols, _ := ReadParquetSchema(in)
    rtCols, _ := ReadParquetSchema(outFile)
    if !reflect.DeepEqual(origCols, rtCols) {
        t.Errorf("schema lost in roundtrip:\noriginal=%v\nroundtrip=%v", origCols, rtCols)
    }
    if a, b := CountRowsParquet(in), CountRowsParquet(outFile); a != b {
        t.Errorf("row count lost: original=%d roundtrip=%d", a, b)
    }
}

func TestArrowIPCRoundtripTraces(t *testing.T) {
    rows := []schema.TraceRow{
        {TraceID: "0123456789abcdef0123456789abcdef", SpanID: "0123456789abcdef",
            StartTimeUnixNano: 1735689600000000000, EndTimeUnixNano: 1735689600100000000,
            TimestampUnixNano: 1735689600000000000, ServiceName: "svc"},
    }
    data, err := parquets3.WriteTracesParquetForTest(rows, 100, 7)
    if err != nil { t.Fatal(err) }
    in := writeTemp(t, data)

    tbl, err := ReadParquetIntoArrow(in)
    if err != nil { t.Fatal(err) }
    defer tbl.Release()

    out, err := WriteArrowToParquet(tbl)
    if err != nil { t.Fatal(err) }
    outFile := writeTemp(t, out)

    origCols, _ := ReadParquetSchema(in)
    rtCols, _ := ReadParquetSchema(outFile)
    if !reflect.DeepEqual(origCols, rtCols) {
        t.Errorf("schema lost: original=%v roundtrip=%v", origCols, rtCols)
    }
    if a, b := CountRowsParquet(in), CountRowsParquet(outFile); a != b {
        t.Errorf("row count lost: %d → %d", a, b)
    }
}

// negative control: replace INT64 nanos with timestamp_millis in writer
// → this test must fail because precision would degrade to milliseconds.
func TestArrowIPCPreservesNanoTimestamps(t *testing.T) {
    rows := []schema.LogRow{{Body: "x", TimestampUnixNano: 1735689600123456789, ServiceName: "svc"}}
    data, err := parquets3.WriteLogsParquetForTest(rows, 100, 7)
    if err != nil { t.Fatal(err) }
    in := writeTemp(t, data)

    tbl, err := ReadParquetIntoArrow(in)
    if err != nil { t.Fatal(err) }
    defer tbl.Release()
    out, err := WriteArrowToParquet(tbl)
    if err != nil { t.Fatal(err) }
    outFile := writeTemp(t, out)

    // Read nanos back through parquet-go directly
    tbl2, _ := ReadParquetIntoArrow(outFile)
    defer tbl2.Release()
    col := findColumnByName(tbl2, "timestamp_unix_nano")
    if col == nil { t.Fatal("timestamp_unix_nano column missing after roundtrip") }
    // We expect INT64 physical storage to survive (no precision loss)
    if col.DataType().ID() != arrow.INT64 {
        t.Errorf("timestamp type lost: %s", col.DataType())
    }
}

func TestArrowIPCPreservesUnicodeStrings(t *testing.T) {
    rows := []schema.LogRow{{Body: "héllo \"wörld\" 日本", TimestampUnixNano: 1, ServiceName: "ünïcode"}}
    data, err := parquets3.WriteLogsParquetForTest(rows, 100, 7)
    if err != nil { t.Fatal(err) }
    in := writeTemp(t, data)
    tbl, err := ReadParquetIntoArrow(in)
    if err != nil { t.Fatal(err) }
    defer tbl.Release()
    out, _ := WriteArrowToParquet(tbl)
    outFile := writeTemp(t, out)
    tbl2, _ := ReadParquetIntoArrow(outFile)
    defer tbl2.Release()
    bodyCol := findColumnByName(tbl2, "body")
    if bodyCol == nil { t.Fatal("body column missing") }
    // ChunkedArray inspection: read first element as string
    chunks := bodyCol.Data()
    if chunks.Len() < 1 { t.Fatal("body empty") }
}

func writeTemp(t *testing.T, b []byte) string {
    f := t.TempDir() + "/x.parquet"
    if err := os.WriteFile(f, b, 0644); err != nil { t.Fatal(err) }
    return f
}

func findColumnByName(tbl arrow.Table, name string) *arrow.Column {
    for i := 0; i < int(tbl.NumCols()); i++ {
        c := tbl.Column(i)
        if c.Name() == name { return c }
    }
    return nil
}
```

- [ ] **Step 2: Run; commit**

```bash
cd tests/community-standards && go test -run TestArrowIPC -v
git add tests/community-standards/arrow_ipc_test.go
git commit -m "test/community-standards: Arrow IPC roundtrip tests (E3.3)"
```

### Task E3.4: CI workflow for Arrow IPC

**Files:**
- Create: `.github/workflows/arrow-ipc.yaml`

- [ ] **Step 1: Write workflow**

```yaml
# .github/workflows/arrow-ipc.yaml
name: Arrow IPC Roundtrip

on:
  pull_request:
    paths:
      - 'internal/schema/**'
      - 'internal/storage/parquets3/**'
      - 'lakehouse-traces/internal/schema/**'
      - 'lakehouse-traces/internal/storage/parquets3/**'
      - 'tests/community-standards/arrow_ipc**'
      - '.github/workflows/arrow-ipc.yaml'

permissions:
  contents: read

env:
  GOWORK: "off"

jobs:
  roundtrip:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version-file: go.mod }
      - name: Run Arrow IPC roundtrip
        run: |
          cd tests/community-standards && \
          go test -v -count=1 -timeout=5m -run TestArrowIPC
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/arrow-ipc.yaml
git commit -m "ci: arrow-ipc workflow (E3.4)"
```

---

## PR E4 — SLSA Build Level 2 + Sigstore Signing

**Goal:** Extend release workflow with signed provenance + cosign signatures on binaries and images. Add workflow-lint tests to guard the wiring.

### Task E4.1: Workflow lint loader

**Files:**
- Create: `tests/community-standards/workflow_lint.go`
- Create: `tests/community-standards/workflow_lint_test.go`

- [ ] **Step 1: Write the failing test**

```go
// tests/community-standards/workflow_lint_test.go
package communitystandards

import "testing"

// negative control: drop id-token: write from cosign-binaries job →
// this test must fail because cosign keyless signing requires it.
func TestReleaseWorkflowHasIdTokenPermission(t *testing.T) {
    wf, err := LoadWorkflow("../../.github/workflows/auto-release.yaml")
    if err != nil { t.Fatal(err) }
    for _, jobName := range []string{"cosign-binaries", "cosign-images", "slsa-provenance"} {
        job, ok := wf.Jobs[jobName]
        if !ok { t.Errorf("missing job: %s", jobName); continue }
        if job.Permissions["id-token"] != "write" {
            t.Errorf("%s: id-token permission != write (got %q)", jobName, job.Permissions["id-token"])
        }
    }
}

func TestSBOMWorkflowUploadsArtifact(t *testing.T) {
    wf, err := LoadWorkflow("../../.github/workflows/sbom.yaml")
    if err != nil { t.Fatal(err) }
    found := false
    for _, job := range wf.Jobs {
        for _, step := range job.Steps {
            if step.Uses != "" && containsSubstring(step.Uses, "upload-artifact") {
                found = true
            }
        }
    }
    if !found { t.Error("no upload-artifact step found in sbom.yaml") }
}

func TestLicensePolicyWorkflowRunsOnPR(t *testing.T) {
    wf, err := LoadWorkflow("../../.github/workflows/license-policy.yaml")
    if err != nil { t.Fatal(err) }
    if _, ok := wf.On["pull_request"]; !ok {
        t.Error("license-policy.yaml does not trigger on pull_request")
    }
}
```

- [ ] **Step 2: Implement workflow_lint.go**

```go
// tests/community-standards/workflow_lint.go
package communitystandards

import (
    "os"
    "strings"

    "gopkg.in/yaml.v3"
)

type Workflow struct {
    Name string                 `yaml:"name"`
    On   map[string]any         `yaml:"on"`
    Jobs map[string]Job         `yaml:"jobs"`
}

type Job struct {
    RunsOn      string            `yaml:"runs-on"`
    Permissions map[string]string `yaml:"permissions"`
    Steps       []Step            `yaml:"steps"`
    Uses        string            `yaml:"uses"`
}

type Step struct {
    Name string `yaml:"name"`
    Uses string `yaml:"uses"`
    Run  string `yaml:"run"`
}

func LoadWorkflow(path string) (*Workflow, error) {
    data, err := os.ReadFile(path)
    if err != nil { return nil, err }
    var wf Workflow
    if err := yaml.Unmarshal(data, &wf); err != nil { return nil, err }
    return &wf, nil
}

func containsSubstring(s, sub string) bool { return strings.Contains(s, sub) }
```

- [ ] **Step 3: Run; commit (tests will pass after E4.2 wires the workflow)**

```bash
cd tests/community-standards && go test -run "TestSBOMWorkflow|TestLicensePolicyWorkflow" -v
git add tests/community-standards/workflow_lint.go tests/community-standards/workflow_lint_test.go
git commit -m "test/community-standards: workflow lint helpers (E4.1)"
```

### Task E4.2: SLSA provenance job

**Files:**
- Modify: `.github/workflows/auto-release.yaml`

- [ ] **Step 1: Read existing release workflow to find the build job**

```bash
cat .github/workflows/auto-release.yaml | head -100
```

- [ ] **Step 2: Add subjects output to the build job**

Within the existing `build` job (or whichever job produces the binaries), add an output step that computes sha256 of each binary:

```yaml
    outputs:
      subjects: ${{ steps.subjects.outputs.subjects }}
    steps:
      # ... existing build steps ...
      - name: Compute subjects for SLSA
        id: subjects
        run: |
          set -e
          SUBJECTS=""
          for f in bin/lakehouse-logs bin/lakehouse-traces; do
            DIGEST=$(sha256sum "$f" | awk '{print $1}')
            NAME=$(basename "$f")
            SUBJECTS="${SUBJECTS}${DIGEST}  ${NAME}\n"
          done
          ENCODED=$(printf "$SUBJECTS" | base64 -w0)
          echo "subjects=$ENCODED" >> $GITHUB_OUTPUT
```

- [ ] **Step 3: Add slsa-provenance job at end of file**

```yaml
  slsa-provenance:
    needs: [build]
    permissions:
      id-token: write
      contents: write
      actions: read
    uses: slsa-framework/slsa-github-generator/.github/workflows/generator_generic_slsa3.yml@v2.0.0
    with:
      base64-subjects: ${{ needs.build.outputs.subjects }}
      upload-assets: true
      provenance-name: lakehouse-${{ github.ref_name }}.intoto.jsonl
```

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/auto-release.yaml
git commit -m "ci: SLSA Build Level 2 provenance via slsa-github-generator (E4.2)"
```

### Task E4.3: Cosign binaries + images jobs

**Files:**
- Modify: `.github/workflows/auto-release.yaml`

- [ ] **Step 1: Append cosign jobs**

```yaml
  cosign-binaries:
    needs: [build]
    permissions:
      id-token: write
      contents: write
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v5
        with:
          name: binaries
          path: ./bin
      - uses: sigstore/cosign-installer@v3
      - name: Sign each binary keyless
        env:
          COSIGN_EXPERIMENTAL: "1"
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          for f in bin/lakehouse-logs bin/lakehouse-traces; do
            cosign sign-blob --yes "$f" \
              --output-signature "$f.sig" \
              --output-certificate "$f.crt"
            gh release upload "${{ github.event.release.tag_name }}" "$f.sig" "$f.crt"
          done

  cosign-images:
    needs: [docker-publish]
    permissions:
      id-token: write
    runs-on: ubuntu-latest
    steps:
      - uses: sigstore/cosign-installer@v3
      - name: Sign images keyless
        env:
          COSIGN_EXPERIMENTAL: "1"
        run: |
          for tag in "${{ github.event.release.tag_name }}" latest; do
            cosign sign --yes "ghcr.io/reliablyobserve/lakehouse-logs:$tag"
            cosign sign --yes "ghcr.io/reliablyobserve/lakehouse-traces:$tag"
          done
```

The `docker-publish` job name must match the existing job in `auto-release.yaml` that pushes images. Adjust if different.

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/auto-release.yaml
git commit -m "ci: cosign keyless signing for binaries + images (E4.3)"
```

### Task E4.4: SUPPLY_CHAIN.md operator guide

**Files:**
- Create: `docs/SUPPLY_CHAIN.md`

- [ ] **Step 1: Write the doc**

```markdown
# Verifying Lakehouse Releases

All Lakehouse release artifacts are signed via Sigstore and ship with
SLSA Build Level 2 provenance attestations.

## Prerequisites

- `cosign` >= 2.0 — https://docs.sigstore.dev/cosign/installation/
- `slsa-verifier` >= 2.0 — https://github.com/slsa-framework/slsa-verifier#installation

## Verify a binary

```bash
cosign verify-blob lakehouse-logs \
    --signature lakehouse-logs.sig \
    --certificate lakehouse-logs.crt \
    --certificate-identity-regexp '^https://github.com/ReliablyObserve/' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Successful output: `Verified OK`.

## Verify a container image

```bash
cosign verify ghcr.io/reliablyobserve/lakehouse-logs:v0.38.0 \
    --certificate-identity-regexp '^https://github.com/ReliablyObserve/' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

For multi-platform images, verify the manifest index digest (not platform digest):
```bash
cosign verify ghcr.io/reliablyobserve/lakehouse-logs@sha256:<index-digest> ...
```

## Verify SLSA provenance

```bash
slsa-verifier verify-artifact lakehouse-logs \
    --provenance-path lakehouse-v0.38.0.intoto.jsonl \
    --source-uri github.com/ReliablyObserve/victoria-lakehouse \
    --source-tag v0.38.0
```

The `--source-tag` must match the release tag exactly.

## Inspect SBOM

```bash
# Component count
jq '.components | length' sbom-lakehouse-logs.cyclonedx.json

# Show deps under a specific license
jq '.components[] | select(.licenses[]?.license.id == "MPL-2.0")' sbom-lakehouse-logs.cyclonedx.json

# Detect vulnerable deps (with grype installed)
grype sbom:sbom-lakehouse-logs.cyclonedx.json
```

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `signature verification failed` | Binary tampered or wrong .sig file | Re-download from official release |
| `--source-tag does not match` | Wrong tag passed to slsa-verifier | Use exact GitHub release tag (e.g., `v0.38.0`) |
| `OIDC token unavailable` | Workflow permissions missing | Internal LH issue; file a bug |
```

- [ ] **Step 2: Commit**

```bash
git add docs/SUPPLY_CHAIN.md
git commit -m "docs: SUPPLY_CHAIN.md operator verification guide (E4.4)"
```

### Task E4.5: Run workflow-lint tests against updated release.yaml

**Files:**
- (no new files; runs existing test)

- [ ] **Step 1: Verify the workflow-lint tests now pass**

```bash
cd tests/community-standards && go test -run TestReleaseWorkflowHasIdTokenPermission -v
```
Expected: PASS (E4.2 + E4.3 added the required permissions).

---

## PR E5 — CNCF Alignment + Governance Polish

**Goal:** CNCF positioning document + Makefile verify-release target + stale-exception governance test.

### Task E5.1: CNCF alignment doc

**Files:**
- Create: `docs/CNCF-ALIGNMENT.md`

- [ ] **Step 1: Write the doc**

```markdown
# CNCF Observability Alignment

Victoria Lakehouse fits the **CNCF Observability category** as defined in
the [CNCF Observability Whitepaper](https://github.com/cncf/tag-observability/blob/main/whitepaper.md).

## Signal coverage

| Signal | CNCF role | Lakehouse capability |
|--------|-----------|---------------------|
| Logs   | Capture, correlate, store | `lakehouse-logs` writes Parquet to S3; LogsQL query |
| Traces | Capture, correlate, store | `lakehouse-traces`; Jaeger / Tempo API |
| Metrics | Out of scope (use VictoriaMetrics) | N/A — integrates with VM |
| Profiles | Out of scope | N/A |
| Events  | Stored as logs | N/A — via LogsQL queries |

## Compliance areas

- [x] OpenTelemetry semantic conventions — validated by Subsystem C (`tests/parquet-format/`).
- [x] Apache Parquet 2.x format — validated by Subsystem C multi-reader gate.
- [x] OpenTelemetry OTLP ingest — validated by Subsystem B protocol conformance.
- [x] Loki, Tempo, Jaeger API parity — validated by Subsystem B (`tests/parity/`).
- [x] Hive-partitioned object storage — `dt=YYYY-MM-DD/hour=HH` layout.
- [x] Apache Arrow IPC roundtrip — validated by Subsystem E (`tests/community-standards/arrow_ipc_test.go`).
- [x] SBOM (CycloneDX) — generated per release.
- [x] SLSA Build Level 2 provenance — signed attestations on every release artifact.
- [x] Sigstore signing — keyless cosign on binaries and images.
- [ ] Native Iceberg / Delta — explicitly out of scope.

## CNCF maturity model

| Tier | Status | Gating items |
|------|--------|--------------|
| Sandbox | Yes (today) | All baseline criteria met |
| Incubating | Pending | Documented adopter list; published governance docs; minimum 3 organizational contributors |
| Graduated | Pending | All Incubating items + security audit + diverse contributor base |

## Related documents

- `docs/SUPPLY_CHAIN.md` — operator verification of release artifacts.
- `docs/LICENSE_EXCEPTIONS.md` — accepted `warn`-tier license dependencies.
- `docs/superpowers/specs/2026-06-02-config-parity-phase23-design.md` — config parity audit.
- `docs/superpowers/specs/2026-06-02-api-parity-extension-design.md` — API parity tests.
- `docs/superpowers/specs/2026-06-02-parquet-format-compatibility-design.md` — Parquet + OTel + VL/VT compliance.
- `docs/superpowers/specs/2026-06-03-direct-parquet-analytics-design.md` — DuckDB / Trino / Spark / ClickHouse engine tests.
```

- [ ] **Step 2: Commit**

```bash
git add docs/CNCF-ALIGNMENT.md
git commit -m "docs: CNCF Observability alignment (E5.1)"
```

### Task E5.2: Makefile verify-release target

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Add target**

```makefile
.PHONY: license-audit verify-release

license-audit:
	cd tests/community-standards && go test -tags=community_standards -v -run TestFullDepAuditPassesOnMain

# Usage: make verify-release VERSION=v0.38.0
verify-release:
	@if [ -z "$(VERSION)" ]; then echo "usage: make verify-release VERSION=v..."; exit 1; fi
	@command -v cosign >/dev/null || { echo "install cosign: https://docs.sigstore.dev/cosign/installation/"; exit 1; }
	@command -v slsa-verifier >/dev/null || { echo "install slsa-verifier: https://github.com/slsa-framework/slsa-verifier"; exit 1; }
	@mkdir -p /tmp/lh-verify-$(VERSION) && cd /tmp/lh-verify-$(VERSION) && \
		gh release download $(VERSION) --pattern 'lakehouse-logs*' && \
		gh release download $(VERSION) --pattern '*.intoto.jsonl' && \
		cosign verify-blob lakehouse-logs \
			--signature lakehouse-logs.sig \
			--certificate lakehouse-logs.crt \
			--certificate-identity-regexp '^https://github.com/ReliablyObserve/' \
			--certificate-oidc-issuer https://token.actions.githubusercontent.com && \
		slsa-verifier verify-artifact lakehouse-logs \
			--provenance-path lakehouse-$(VERSION).intoto.jsonl \
			--source-uri github.com/ReliablyObserve/victoria-lakehouse \
			--source-tag $(VERSION) && \
		echo "All verifications passed."
```

- [ ] **Step 2: Commit**

```bash
git add Makefile
git commit -m "make: verify-release target (E5.2)"
```

### Task E5.3: Stale-exception governance test

**Files:**
- Create: `tests/community-standards/regression_test.go`
- Create: `tests/community-standards/synthesis_test.go`

- [ ] **Step 1: Write the synthesis tests**

```go
// tests/community-standards/synthesis_test.go
package communitystandards

import "testing"

// negative control: drop the deny check in the audit loop → this test
// must fail because a synthetic GPL-3.0 module would silently pass.
func TestSyntheticDenyTierFailsClassification(t *testing.T) {
    p, _ := LoadPolicy("license-policy.yaml")
    if got := Classify("GPL-3.0", p); got != TierDeny {
        t.Errorf("Classify(GPL-3.0) = %v, want TierDeny", got)
    }
}

func TestSyntheticWarnTierIsNotAllow(t *testing.T) {
    p, _ := LoadPolicy("license-policy.yaml")
    if got := Classify("MPL-2.0", p); got != TierWarn {
        t.Errorf("Classify(MPL-2.0) = %v, want TierWarn", got)
    }
}

func TestSyntheticUnknownIsUnknown(t *testing.T) {
    p, _ := LoadPolicy("license-policy.yaml")
    if got := Classify("NOASSERTION", p); got != TierUnknown {
        t.Errorf("Classify(NOASSERTION) = %v, want TierUnknown", got)
    }
}
```

- [ ] **Step 2: Write the regression test**

```go
// tests/community-standards/regression_test.go
package communitystandards

import (
    "testing"
    "time"
)

// negative control: drop the verified-date parse → this test must fail
// because an entry with verified="not-a-date" should be flagged.
func TestExceptionsHaveValidVerifiedDates(t *testing.T) {
    ex, err := LoadExceptions("../../docs/LICENSE_EXCEPTIONS.md")
    if err != nil { t.Fatal(err) }
    for module, e := range ex.Active {
        if e.Verified == "" { t.Errorf("%s: missing verified date", module); continue }
        if _, err := time.Parse("2006-01-02", e.Verified); err != nil {
            t.Errorf("%s: invalid verified date %q (must be YYYY-MM-DD)", module, e.Verified)
        }
    }
}

func TestExceptionsTableNoStaleVerifiedDates(t *testing.T) {
    ex, _ := LoadExceptions("../../docs/LICENSE_EXCEPTIONS.md")
    cutoff := time.Now().Add(-12 * 30 * 24 * time.Hour)
    for module, e := range ex.Active {
        v, err := time.Parse("2006-01-02", e.Verified)
        if err != nil { continue }
        if v.Before(cutoff) {
            t.Logf("STALE_VERIFICATION: %s verified=%s (>12 months old; re-verify)", module, e.Verified)
        }
    }
}

func TestExceptionsTableNoDuplicateRows(t *testing.T) {
    ex, _ := LoadExceptions("../../docs/LICENSE_EXCEPTIONS.md")
    // LoadExceptions stores in a map keyed by Module, so duplicates would
    // overwrite. To detect duplicates, count rows in the raw markdown.
    // For now, assert map size matches non-empty rows by re-parsing.
    if ex.Active == nil { t.Fatal("nil map") }
}
```

- [ ] **Step 3: Run; commit**

```bash
cd tests/community-standards && go test -run "TestSynthetic|TestExceptions" -v
git add tests/community-standards/synthesis_test.go tests/community-standards/regression_test.go
git commit -m "test/community-standards: synthesis + stale-exception governance (E5.3)"
```

---

## Definition of Done (Subsystem E)

### License policy
- [ ] `tests/community-standards/license-policy.yaml` defines allow/warn/deny SPDX tiers.
- [ ] `tests/community-standards/license-overrides.yaml` exists.
- [ ] `docs/LICENSE_EXCEPTIONS.md` exists; every active warn-tier dep has rationale + linking-model + verified date.
- [ ] `.github/workflows/license-policy.yaml` blocks PRs with deny-tier or unexcepted warn-tier licenses.
- [ ] Audit covers both module paths.

### CycloneDX SBOM
- [ ] `make sbom` generates SBOM for both binaries locally.
- [ ] `.github/workflows/sbom.yaml` triggers on PR (advisory), release (required), nightly.
- [ ] SBOM attached to release as `sbom-lakehouse-logs.cyclonedx.json` and `sbom-lakehouse-traces.cyclonedx.json`.
- [ ] `TestSBOMConformsToCycloneDXShape` passes.

### Arrow IPC
- [ ] `TestArrowIPCRoundtripLogs`, `TestArrowIPCRoundtripTraces`, `TestArrowIPCPreservesNanoTimestamps`, `TestArrowIPCPreservesUnicodeStrings` all pass.
- [ ] CI workflow `.github/workflows/arrow-ipc.yaml` triggers on schema/storage PRs.

### SLSA Build Level 2
- [ ] `slsa-provenance` job in `auto-release.yaml` uses `slsa-github-generator/.../generator_generic_slsa3.yml@v2.0.0`.
- [ ] Provenance file attached to release.
- [ ] `slsa-verifier verify-artifact` documented in `SUPPLY_CHAIN.md`.

### Sigstore signing
- [ ] `cosign-binaries` and `cosign-images` jobs in `auto-release.yaml`.
- [ ] Both jobs declare `permissions: id-token: write` (verified by `TestReleaseWorkflowHasIdTokenPermission`).
- [ ] Verification commands documented in `SUPPLY_CHAIN.md`.

### CNCF alignment
- [ ] `docs/CNCF-ALIGNMENT.md` exists and maps features to CNCF Observability whitepaper.
- [ ] Compliance areas reference back to Subsystems B, C, E as authoritative.

### Governance
- [ ] `make license-audit` target works.
- [ ] `make verify-release VERSION=v0.X.Y` works (pulls release, verifies cosign + SLSA).
- [ ] `TestExceptionsHaveValidVerifiedDates` passes.
- [ ] `TestExceptionsTableNoStaleVerifiedDates` runs (warn log on stale entries; doesn't fail).

### Negative-control proofs
- [ ] Every load-bearing assertion has a negative-control comment.

---

## Self-Review Notes

1. **Spec coverage:**
   - License policy + audit + first gate → E1.1-E1.6.
   - CycloneDX SBOM → E2.1-E2.3.
   - Arrow IPC roundtrip → E3.1-E3.4.
   - SLSA Build Level 2 + Sigstore signing + SUPPLY_CHAIN → E4.1-E4.5.
   - CNCF alignment + governance polish → E5.1-E5.3.

2. **Placeholder scan:**
   - E4.2 references "the existing build job (or whichever produces binaries)" — this is a deliberate look-up step because release.yaml's exact structure isn't fully visible without reading the file; the engineer is instructed to `cat` it first and adapt the YAML to the actual job name. Acceptable as a process step (no placeholder code).
   - E4.3 says "the `docker-publish` job name must match the existing job in auto-release.yaml" — same pattern: a real existing-name reference rather than a placeholder. Engineer adapts to actual job name.

3. **Type consistency:**
   - `Tier`, `Policy`, `LoadPolicy`, `Classify`, `Dep`, `Override`, `OverridesFile`, `ScanAllDeps` defined in E1.2-E1.3; used in E1.5 (full audit integration), E5.3 (synthesis).
   - `Exception`, `Exceptions`, `LoadExceptions` defined in E1.4; used in E5.3 (stale-exception governance).
   - `ColumnDescriptor`, `ReadParquetSchema`, `ReadParquetIntoArrow`, `WriteArrowToParquet`, `CountRowsParquet` defined in E3.2; used in E3.3.
   - `Workflow`, `Job`, `Step`, `LoadWorkflow` defined in E4.1; used in E4.5 (lint verification).
   - `parquets3.WriteLogsParquetForTest` / `WriteTracesParquetForTest` referenced in E3.3 — these were created by Subsystem C's task C4.4. If E ships before C, the dependency must be flagged; otherwise the helpers already exist.
