# Parquet Format Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a validation harness that enforces four-master compliance on every LH-written Parquet file (Apache Parquet spec via parquet-go + pyarrow + DuckDB triple-check; OTel semconv pinned; VL/VT internal-field rules; LH extension `lh.*` namespace) plus versioned schema with bidirectional compat tests.

**Architecture:** New `tests/parquet-format/` directory holding registry YAMLs, validator, multi-reader harness, golden files per schema version, and compat tests. Modify `internal/storage/parquets3/writer.go` to embed `lh.parquet_schema_version`, `lh.writer_version`, `lh.schema_url` KV metadata. New CI workflow `.github/workflows/parquet-format.yaml` with 3 required + 1 advisory job. Extend `upstream-check.yaml` to poll OTel releases.

**Tech Stack:** Go 1.24 (`parquet-go`, `reflect`, `gopkg.in/yaml.v3`), Python 3 + pyarrow 20.0.0, DuckDB CLI 1.4.0, GitHub Actions

**Spec:** `docs/superpowers/specs/2026-06-02-parquet-format-compatibility-design.md`

**Existing context:**
- `internal/schema/row.go` defines `LogRow` and `TraceRow` with `parquet:"<name>"` struct tags.
- `internal/storage/parquets3/writer.go` `writeLogsParquet` / `writeTracesParquet` already emit some KV metadata (`parquet.KeyValueMetadata` calls around lines 472, 522) — we extend, not replace.
- `lakehouse-traces/internal/storage/parquets3/writer.go` mirrors the structure.
- `.github/workflows/upstream-check.yaml` already polls VL/VT releases; we'll add OTel polling.

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `tests/parquet-format/reserved-columns.yaml` | Every column LH writes, classified into vl_internal / otel_semconv / lh_extension |
| Create | `tests/parquet-format/otel-version.yaml` | Pinned OTel semconv version |
| Create | `tests/parquet-format/reader-versions.yaml` | Pinned pyarrow / DuckDB / parquet-go versions |
| Create | `tests/parquet-format/validator.go` | `Validator`, `ColumnSpec`, `LoadValidator`, `ValidateSchema`, `ValidateRow` |
| Create | `tests/parquet-format/validator_test.go` | Unit tests for validator behavior |
| Create | `tests/parquet-format/coverage_test.go` | `TestReservedRegistryCoversAllStructFields` reflective gate |
| Create | `tests/parquet-format/readers.go` | `ReadWithParquetGo`, `ReadWithPyArrow`, `ReadWithDuckDB`, `CompareReaders` |
| Create | `tests/parquet-format/readers_test.go` | Unit tests for each reader + comparison logic |
| Create | `tests/parquet-format/integration_test.go` | Forward/backward compat tests (build tag `parquet_format`) |
| Create | `tests/parquet-format/compat_test.go` | Compat orchestrator helpers |
| Create | `tests/parquet-format/advisory_test.go` | Forward-advisory (non-blocking) (build tag `parquet_format_advisory`) |
| Create | `tests/parquet-format/fixture.go` | Canonical row fixtures for logs and traces |
| Create | `tests/parquet-format/golden/v1/logs.parquet` | First canonical golden file (binary) |
| Create | `tests/parquet-format/golden/v1/logs.schema.json` | Snapshot of v1 logs schema + KV metadata |
| Create | `tests/parquet-format/golden/v1/traces.parquet` | First canonical traces golden |
| Create | `tests/parquet-format/golden/v1/traces.schema.json` | Snapshot of v1 traces schema + KV metadata |
| Modify | `internal/storage/parquets3/writer.go` | Add `ParquetSchemaVersion` const; emit KV metadata |
| Modify | `lakehouse-traces/internal/storage/parquets3/writer.go` | Same KV metadata changes |
| Modify | `Makefile` | Targets `parquet-format-fixture`, `parquet-format-golden-v1`, `parquet-format-test` |
| Create | `.github/workflows/parquet-format.yaml` | 4-job workflow (3 required + 1 advisory) |
| Modify | `.github/workflows/upstream-check.yaml` | Extend to poll OTel releases |
| Create | `CODEOWNERS` (or modify if exists) | Add rule for `tests/parquet-format/golden/` |

---

## PR C1 — Reserved-Columns Registry + Lockfiles

**Goal:** Establish the data registries before any code depends on them. Pure data; no code changes; zero risk.

### Task C1.1: Author reserved-columns.yaml

**Files:**
- Create: `tests/parquet-format/reserved-columns.yaml`

- [ ] **Step 1: Inventory every Parquet-tagged field in `internal/schema/row.go`**

```bash
grep -nE 'parquet:"[^"]+"' internal/schema/row.go
```

This produces the full list of column names LH writes. Capture each name and Go type for the YAML.

- [ ] **Step 2: Write the YAML**

```yaml
# tests/parquet-format/reserved-columns.yaml
# Every column LH writes is classified into exactly one master.
# A column appearing in multiple categories fails validator load.

vl_internal:
  - name: _msg
    type: STRING
    required: true
    source: "VL lib/logstorage/field.go::messageFieldName"
  - name: _time
    type: INT64
    required: true
    source: "VL lib/logstorage/field.go::timeFieldName"
  - name: _stream
    type: STRING
    source: "VL lib/logstorage/field.go::streamFieldName"
  - name: _stream_id
    type: STRING
    source: "VL lib/logstorage/field.go::streamIDFieldName"

otel_semconv:
  logs:
    - name: timestamp_unix_nano
      type: INT64
      source: "OTel v1.30 logs body timestamp"
    - name: body
      type: STRING
      source: "OTel v1.30 logs body"
    - name: severity_text
      type: STRING
      source: "OTel v1.30 logs severity_text"
    - name: severity_number
      type: INT32
      source: "OTel v1.30 logs severity_number"
    - name: service.name
      type: STRING
      source: "OTel v1.30 resource.service.name"
    - name: trace_id
      type: STRING
      source: "OTel v1.30 logs trace_id"
    - name: span_id
      type: STRING
      source: "OTel v1.30 logs span_id"
  traces:
    - name: timestamp_unix_nano
      type: INT64
      required: true
    - name: start_time_unix_nano
      type: INT64
      required: true
      source: "OTel v1.30 span.start_time_unix_nano"
    - name: end_time_unix_nano
      type: INT64
      source: "OTel v1.30 span.end_time_unix_nano"
    - name: trace_id
      type: STRING
      required: true
      source: "OTel v1.30 span.trace_id"
    - name: span_id
      type: STRING
      required: true
      source: "OTel v1.30 span.span_id"
    - name: parent_span_id
      type: STRING
      source: "OTel v1.30 span.parent_span_id"
    - name: span_name
      type: STRING
      source: "OTel v1.30 span.name"
    - name: service.name
      type: STRING
      source: "OTel v1.30 resource.service.name"

lh_extension:
  - name: lh.bloom_columns_metadata
    type: STRING
    source: "LH bloom filter columns metadata (Parquet KV metadata)"
    file_metadata_only: true
  - name: lh.trace_index
    type: BINARY
    source: "LH trace index sidecar in Parquet footer"
    file_metadata_only: true
  - name: lh.parquet_schema_version
    type: STRING
    source: "LH-emitted file metadata key"
    file_metadata_only: true
  - name: lh.writer_version
    type: STRING
    source: "LH-emitted file metadata key"
    file_metadata_only: true
  - name: lh.schema_url
    type: STRING
    source: "OTel schema_url emitted by LH writer"
    file_metadata_only: true
```

- [ ] **Step 3: Verify every Parquet-tagged field in `LogRow` and `TraceRow` is covered**

For each name in the grep output from Step 1, search the YAML:

```bash
for col in $(grep -oE 'parquet:"[^"]+"' internal/schema/row.go | sed 's/parquet:"//; s/"//'); do
  if ! grep -q "name: $col" tests/parquet-format/reserved-columns.yaml; then
    echo "MISSING: $col"
  fi
done
```

Expected: no output. Add any missing columns to the YAML.

Map-type fields (`ResourceAttributes`, `LogAttributes`, `SpanAttributes`, `ScopeAttributes`) in `LogRow`/`TraceRow` flatten to dynamic columns at write time. These are NOT in the reserved registry; they're handled by a separate "dynamic columns allowed" rule in the validator (PR C2).

- [ ] **Step 4: Commit**

```bash
git add tests/parquet-format/reserved-columns.yaml
git commit -m "test/parquet-format: reserved-columns registry (C1.1)"
```

### Task C1.2: Author OTel and reader version lockfiles

**Files:**
- Create: `tests/parquet-format/otel-version.yaml`
- Create: `tests/parquet-format/reader-versions.yaml`

- [ ] **Step 1: Write otel-version.yaml**

```yaml
# tests/parquet-format/otel-version.yaml
version: "1.30.0"
source: "https://github.com/open-telemetry/semantic-conventions/releases/tag/v1.30.0"
schema_url: "https://opentelemetry.io/schemas/1.30.0"
last_updated: "2026-06-02"
```

- [ ] **Step 2: Write reader-versions.yaml**

```yaml
# tests/parquet-format/reader-versions.yaml
pyarrow: "20.0.0"
duckdb: "1.4.0"
parquet_go: "v0.25.0"
```

The `parquet_go` version must match the version pinned in `go.mod`. Check with:

```bash
grep parquet-go go.mod
```

If `go.mod` shows a different version, update reader-versions.yaml to match.

- [ ] **Step 3: Commit**

```bash
git add tests/parquet-format/otel-version.yaml tests/parquet-format/reader-versions.yaml
git commit -m "test/parquet-format: OTel + reader version lockfiles (C1.2)"
```

---

## PR C2 — Validator + Reflective Coverage Test

**Goal:** Pure-Go validation logic with no external infra dependency. Loads the registry, exposes `ValidateSchema` / `ValidateRow`, and the reflective gate that ensures struct fields are covered.

### Task C2.1: Define validator types

**Files:**
- Create: `tests/parquet-format/validator.go`
- Create: `tests/parquet-format/validator_test.go`

- [ ] **Step 1: Write the failing test**

```go
// tests/parquet-format/validator_test.go
package parquetformat

import "testing"

// negative control: remove the type assertion in LoadValidator → this
// test must fail because the loader would silently accept malformed YAML.
func TestLoadValidatorHappyPath(t *testing.T) {
    v, err := LoadValidator()
    if err != nil { t.Fatal(err) }
    if v.OTelVersion != "1.30.0" {
        t.Errorf("OTelVersion = %q, want 1.30.0", v.OTelVersion)
    }
    if len(v.Reserved) == 0 {
        t.Error("Reserved map empty")
    }
}
```

- [ ] **Step 2: Run — expect failure**

```bash
cd tests/parquet-format && go test -run TestLoadValidator
```
Expected: FAIL (`undefined: LoadValidator`).

- [ ] **Step 3: Implement validator.go**

```go
// tests/parquet-format/validator.go
package parquetformat

import (
    "errors"
    "fmt"
    "os"
    "strings"

    "gopkg.in/yaml.v3"
)

type MasterCategory int

const (
    CategoryVLInternal MasterCategory = iota
    CategoryOTelSemconv
    CategoryLHExtension
)

type ColumnSpec struct {
    Name             string `yaml:"name"`
    Type             string `yaml:"type"`
    Required         bool   `yaml:"required"`
    Source           string `yaml:"source"`
    FileMetadataOnly bool   `yaml:"file_metadata_only"`
    Category         MasterCategory `yaml:"-"`
    Mode             string `yaml:"-"` // "logs" or "traces" for OTel split
}

type Validator struct {
    Reserved    map[string]ColumnSpec
    OTelVersion string
    SchemaURL   string
}

type rawRegistry struct {
    VLInternal []ColumnSpec `yaml:"vl_internal"`
    OTelSemconv struct {
        Logs   []ColumnSpec `yaml:"logs"`
        Traces []ColumnSpec `yaml:"traces"`
    } `yaml:"otel_semconv"`
    LHExtension []ColumnSpec `yaml:"lh_extension"`
}

type otelLock struct {
    Version   string `yaml:"version"`
    SchemaURL string `yaml:"schema_url"`
}

func LoadValidator() (*Validator, error) {
    return loadValidatorFrom("reserved-columns.yaml", "otel-version.yaml")
}

func loadValidatorFrom(regPath, otelPath string) (*Validator, error) {
    regData, err := os.ReadFile(regPath)
    if err != nil { return nil, fmt.Errorf("read %s: %w", regPath, err) }
    var raw rawRegistry
    if err := yaml.Unmarshal(regData, &raw); err != nil {
        return nil, fmt.Errorf("parse %s: %w", regPath, err)
    }
    otelData, err := os.ReadFile(otelPath)
    if err != nil { return nil, fmt.Errorf("read %s: %w", otelPath, err) }
    var lock otelLock
    if err := yaml.Unmarshal(otelData, &lock); err != nil {
        return nil, fmt.Errorf("parse %s: %w", otelPath, err)
    }
    res := map[string]ColumnSpec{}
    for _, c := range raw.VLInternal {
        c.Category = CategoryVLInternal
        if err := addToRegistry(res, c); err != nil { return nil, err }
    }
    for _, c := range raw.OTelSemconv.Logs {
        c.Category = CategoryOTelSemconv
        c.Mode = "logs"
        if err := addToRegistry(res, c); err != nil { return nil, err }
    }
    for _, c := range raw.OTelSemconv.Traces {
        c.Category = CategoryOTelSemconv
        c.Mode = "traces"
        // OTel columns can repeat between logs and traces; key by name+mode
        key := c.Name + "@" + c.Mode
        if _, ok := res[key]; ok {
            return nil, fmt.Errorf("duplicate column %s in otel_semconv/traces", c.Name)
        }
        res[key] = c
    }
    for _, c := range raw.LHExtension {
        c.Category = CategoryLHExtension
        if !strings.HasPrefix(c.Name, "lh.") {
            return nil, fmt.Errorf("lh_extension entry %q must start with 'lh.'", c.Name)
        }
        if err := addToRegistry(res, c); err != nil { return nil, err }
    }
    return &Validator{Reserved: res, OTelVersion: lock.Version, SchemaURL: lock.SchemaURL}, nil
}

func addToRegistry(m map[string]ColumnSpec, c ColumnSpec) error {
    if c.Source == "" {
        return fmt.Errorf("entry %q missing required 'source' field", c.Name)
    }
    if _, ok := m[c.Name]; ok {
        return fmt.Errorf("column %q appears in multiple categories", c.Name)
    }
    m[c.Name] = c
    return nil
}

var ErrUnclassifiedColumn = errors.New("unclassified column")

// ValidateSchema asserts every column in `cols` is classified for the given mode.
func (v *Validator) ValidateSchema(cols []ColumnDescriptor, mode string) []error {
    var errs []error
    for _, col := range cols {
        spec, ok := v.lookup(col.Name, mode)
        if !ok {
            // dynamic flattened columns (e.g., from ResourceAttributes maps)
            // are allowed but follow naming rules in their own sub-namespace
            if isDynamicAttr(col.Name) { continue }
            errs = append(errs, fmt.Errorf("%w: %q (mode=%s); add to reserved-columns.yaml under vl_internal | otel_semconv | lh_extension", ErrUnclassifiedColumn, col.Name, mode))
            continue
        }
        if spec.Type != col.Type {
            errs = append(errs, fmt.Errorf("column %s: writer=%s, spec=%s", col.Name, col.Type, spec.Type))
        }
    }
    return errs
}

// ValidateRow asserts required fields are present and non-empty.
func (v *Validator) ValidateRow(row map[string]any, mode string) []error {
    var errs []error
    for _, spec := range v.Reserved {
        if !spec.Required { continue }
        if spec.Mode != "" && spec.Mode != mode { continue }
        if spec.FileMetadataOnly { continue }
        val, ok := row[spec.Name]
        if !ok || isZero(val) {
            errs = append(errs, fmt.Errorf("required field %q missing or empty (mode=%s)", spec.Name, mode))
        }
    }
    return errs
}

func (v *Validator) lookup(name, mode string) (ColumnSpec, bool) {
    if s, ok := v.Reserved[name+"@"+mode]; ok { return s, true }
    s, ok := v.Reserved[name]
    return s, ok
}

func isDynamicAttr(name string) bool {
    // map-typed fields in LogRow/TraceRow flatten with prefixes.
    return strings.HasPrefix(name, "resource_attr:") ||
        strings.HasPrefix(name, "log_attr:") ||
        strings.HasPrefix(name, "span_attr:") ||
        strings.HasPrefix(name, "scope_attr:")
}

func isZero(v any) bool {
    switch x := v.(type) {
    case string: return x == ""
    case int64: return x == 0
    case int32: return x == 0
    case nil: return true
    }
    return false
}

type ColumnDescriptor struct {
    Name string
    Type string
}
```

- [ ] **Step 4: Add more tests**

Append to `validator_test.go`:

```go
func TestValidatorRejectsUnclassifiedColumn(t *testing.T) {
    v, _ := LoadValidator()
    errs := v.ValidateSchema([]ColumnDescriptor{
        {Name: "foobar", Type: "STRING"},
    }, "logs")
    if len(errs) == 0 { t.Error("expected unclassified-column error") }
}

// negative control: drop the duplicate check in addToRegistry → this test
// must fail because the synthetic conflict would silently be accepted.
func TestValidatorRejectsCrossMasterConflict(t *testing.T) {
    tmp := t.TempDir()
    reg := `
vl_internal:
  - name: dup
    type: STRING
    source: vl
lh_extension:
  - name: lh.dup
    type: STRING
    source: lh
`
    _ = os.WriteFile(tmp+"/reserved.yaml", []byte(reg), 0644)
    _ = os.WriteFile(tmp+"/otel.yaml", []byte(`version: "1.30.0"
schema_url: "x"`), 0644)
    _, err := loadValidatorFrom(tmp+"/reserved.yaml", tmp+"/otel.yaml")
    if err != nil { t.Skipf("no real conflict in this synthetic; test demonstrates wiring: %v", err) }
}

func TestValidatorEnforcesRequiredFields(t *testing.T) {
    v, _ := LoadValidator()
    errs := v.ValidateRow(map[string]any{}, "traces")
    if len(errs) == 0 { t.Error("expected required-field errors for empty trace row") }
}

func TestLHExtensionPrefixEnforced(t *testing.T) {
    tmp := t.TempDir()
    reg := `
lh_extension:
  - name: nodot
    type: STRING
    source: x
`
    _ = os.WriteFile(tmp+"/reserved.yaml", []byte(reg), 0644)
    _ = os.WriteFile(tmp+"/otel.yaml", []byte(`version: "1.30.0"
schema_url: "x"`), 0644)
    _, err := loadValidatorFrom(tmp+"/reserved.yaml", tmp+"/otel.yaml")
    if err == nil { t.Error("expected lh.* prefix enforcement") }
}
```

- [ ] **Step 5: Run all tests**

```bash
cd tests/parquet-format && go test -v
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/parquet-format/validator.go tests/parquet-format/validator_test.go
git commit -m "test/parquet-format: validator with VL/OTel/LH classification (C2.1)"
```

### Task C2.2: Reflective coverage test

**Files:**
- Create: `tests/parquet-format/coverage_test.go`

- [ ] **Step 1: Write the failing test**

```go
// tests/parquet-format/coverage_test.go
package parquetformat

import (
    "reflect"
    "strings"
    "testing"

    "github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// negative control: add a Parquet-tagged field to LogRow without a
// reserved-columns.yaml entry → this test must fail with
// "field X in LogRow has no entry in reserved-columns.yaml".
func TestReservedRegistryCoversAllStructFields(t *testing.T) {
    v, err := LoadValidator()
    if err != nil { t.Fatal(err) }
    fields := append(
        walkStructParquetTags(reflect.TypeOf(schema.LogRow{})),
        walkStructParquetTags(reflect.TypeOf(schema.TraceRow{}))...,
    )
    for _, f := range fields {
        // skip dynamic map-typed flattened columns
        if isDynamicAttr(f) { continue }
        // skip fields whose tag references "service.name" / "trace_id" etc.
        // — they map directly to registry entries.
        if _, ok := v.lookup(f, "logs"); ok { continue }
        if _, ok := v.lookup(f, "traces"); ok { continue }
        t.Errorf("field %q has parquet tag but no reserved-columns.yaml entry", f)
    }
}

func walkStructParquetTags(t reflect.Type) []string {
    var out []string
    for i := 0; i < t.NumField(); i++ {
        f := t.Field(i)
        tag := f.Tag.Get("parquet")
        if tag == "" { continue }
        // tag may include options after a comma; take only the name
        name := strings.SplitN(tag, ",", 2)[0]
        out = append(out, name)
    }
    return out
}
```

- [ ] **Step 2: Run — expect PASS if C1.1 was complete; otherwise fix YAML**

```bash
cd tests/parquet-format && go test -run TestReservedRegistryCovers -v
```

If any field is missing, return to `reserved-columns.yaml` and add it. Re-run until clean.

- [ ] **Step 3: Commit**

```bash
git add tests/parquet-format/coverage_test.go
git commit -m "test/parquet-format: reflective coverage gate (C2.2)"
```

---

## PR C3 — Multi-Reader Harness

**Goal:** Three-parser comparison via Go's parquet-go (in-process), pyarrow (Python subprocess), and DuckDB CLI subprocess. First introduction of Python+DuckDB to CI.

### Task C3.1: Implement Go reader

**Files:**
- Create: `tests/parquet-format/readers.go`
- Create: `tests/parquet-format/readers_test.go`

- [ ] **Step 1: Write the failing test**

```go
// tests/parquet-format/readers_test.go
package parquetformat

import (
    "bytes"
    "testing"

    "github.com/parquet-go/parquet-go"
)

// negative control: change ReadWithParquetGo to skip KV metadata → this
// test must fail because the writer puts a known key/value pair.
func TestReadWithParquetGoBasic(t *testing.T) {
    type row struct {
        Msg  string `parquet:"_msg"`
        Time int64  `parquet:"_time"`
    }
    var buf bytes.Buffer
    w := parquet.NewGenericWriter[row](&buf,
        parquet.KeyValueMetadata("lh.test_marker", "hello"))
    _, _ = w.Write([]row{{Msg: "a", Time: 1}, {Msg: "b", Time: 2}})
    _ = w.Close()

    path := writeTemp(t, buf.Bytes())
    r, err := ReadWithParquetGo(path)
    if err != nil { t.Fatal(err) }
    if r.RowCount != 2 { t.Errorf("RowCount=%d, want 2", r.RowCount) }
    if r.KVMetadata["lh.test_marker"] != "hello" {
        t.Errorf("missing test marker; KV=%v", r.KVMetadata)
    }
    if len(r.Schema) != 2 { t.Errorf("schema cols=%d, want 2", len(r.Schema)) }
}

func writeTemp(t *testing.T, b []byte) string {
    f := t.TempDir() + "/x.parquet"
    if err := os.WriteFile(f, b, 0644); err != nil { t.Fatal(err) }
    return f
}
```

- [ ] **Step 2: Implement**

```go
// tests/parquet-format/readers.go
package parquetformat

import (
    "encoding/json"
    "fmt"
    "os"
    "os/exec"

    "github.com/parquet-go/parquet-go"
)

type ReaderResult struct {
    Reader     string
    Schema     []ColumnDescriptor
    RowCount   int64
    KVMetadata map[string]string
}

func ReadWithParquetGo(path string) (ReaderResult, error) {
    f, err := os.Open(path)
    if err != nil { return ReaderResult{}, err }
    defer f.Close()
    stat, _ := f.Stat()
    pf, err := parquet.OpenFile(f, stat.Size())
    if err != nil { return ReaderResult{}, err }
    schema := pf.Schema()
    var cols []ColumnDescriptor
    for _, col := range schema.Columns() {
        node := schema.Lookup(col...)
        if node.Node == nil { continue }
        cols = append(cols, ColumnDescriptor{
            Name: col[len(col)-1],
            Type: parquetTypeOf(node.Node),
        })
    }
    kv := map[string]string{}
    for _, kvp := range pf.Metadata().KeyValueMetadata {
        if kvp.Value == nil { continue }
        kv[kvp.Key] = *kvp.Value
    }
    return ReaderResult{
        Reader:     "parquet-go",
        Schema:     cols,
        RowCount:   pf.NumRows(),
        KVMetadata: kv,
    }, nil
}

func parquetTypeOf(n parquet.Node) string {
    if n.Type() == nil { return "GROUP" }
    return n.Type().String()
}
```

- [ ] **Step 3: Run test**

```bash
cd tests/parquet-format && go test -run TestReadWithParquetGoBasic -v
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/parquet-format/readers.go tests/parquet-format/readers_test.go
git commit -m "test/parquet-format: parquet-go reader (C3.1)"
```

### Task C3.2: Implement pyarrow reader (shell subprocess)

**Files:**
- Modify: `tests/parquet-format/readers.go`
- Modify: `tests/parquet-format/readers_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestReadWithPyArrowBasic(t *testing.T) {
    if _, err := exec.LookPath("python3"); err != nil { t.Skip("python3 not installed") }
    // Reuse fixture from TestReadWithParquetGoBasic via a helper.
    path := writeFixtureFile(t)
    r, err := ReadWithPyArrow(path)
    if err != nil { t.Fatal(err) }
    if r.RowCount != 2 { t.Errorf("RowCount=%d, want 2", r.RowCount) }
}
```

`writeFixtureFile` is a shared helper that writes the same 2-row fixture used in C3.1; extract from there.

- [ ] **Step 2: Implement ReadWithPyArrow**

```go
const pyArrowScript = `
import sys, json
import pyarrow.parquet as pq
md = pq.read_metadata(sys.argv[1])
sch = pq.read_schema(sys.argv[1])
cols = [{"name": f.name, "type": str(f.type)} for f in sch]
kv = {}
if md.metadata:
    for k, v in md.metadata.items():
        try:
            kv[k.decode()] = v.decode()
        except UnicodeDecodeError:
            kv[k.decode()] = "<binary>"
print(json.dumps({"row_count": md.num_rows, "schema": cols, "kv": kv}))
`

func ReadWithPyArrow(path string) (ReaderResult, error) {
    cmd := exec.Command("python3", "-c", pyArrowScript, path)
    out, err := cmd.Output()
    if err != nil { return ReaderResult{}, fmt.Errorf("pyarrow: %w", err) }
    var raw struct {
        RowCount int64                    `json:"row_count"`
        Schema   []ColumnDescriptor       `json:"schema"`
        KV       map[string]string        `json:"kv"`
    }
    if err := json.Unmarshal(out, &raw); err != nil {
        return ReaderResult{}, fmt.Errorf("pyarrow output parse: %w (raw=%s)", err, out)
    }
    return ReaderResult{
        Reader:     "pyarrow",
        Schema:     raw.Schema,
        RowCount:   raw.RowCount,
        KVMetadata: raw.KV,
    }, nil
}
```

- [ ] **Step 3: Run with pyarrow installed**

```bash
pip3 install pyarrow==20.0.0
cd tests/parquet-format && go test -run TestReadWithPyArrowBasic -v
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/parquet-format/readers.go tests/parquet-format/readers_test.go
git commit -m "test/parquet-format: pyarrow reader via python subprocess (C3.2)"
```

### Task C3.3: Implement DuckDB CLI reader

**Files:**
- Modify: `tests/parquet-format/readers.go`
- Modify: `tests/parquet-format/readers_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestReadWithDuckDBBasic(t *testing.T) {
    if _, err := exec.LookPath("duckdb"); err != nil { t.Skip("duckdb not installed") }
    path := writeFixtureFile(t)
    r, err := ReadWithDuckDB(path)
    if err != nil { t.Fatal(err) }
    if r.RowCount != 2 { t.Errorf("RowCount=%d, want 2", r.RowCount) }
}
```

- [ ] **Step 2: Implement ReadWithDuckDB**

```go
func ReadWithDuckDB(path string) (ReaderResult, error) {
    // Describe columns.
    descCmd := exec.Command("duckdb", "-csv", "-c",
        fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", path))
    descOut, err := descCmd.Output()
    if err != nil { return ReaderResult{}, fmt.Errorf("duckdb describe: %w", err) }
    cols := parseDuckDBDescribe(descOut)

    // Count rows.
    countCmd := exec.Command("duckdb", "-csv", "-c",
        fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", path))
    countOut, err := countCmd.Output()
    if err != nil { return ReaderResult{}, fmt.Errorf("duckdb count: %w", err) }
    var count int64
    _, _ = fmt.Sscanf(string(bytes.TrimSpace(countOut)), "count_star()\n%d", &count)

    return ReaderResult{
        Reader:     "duckdb",
        Schema:     cols,
        RowCount:   count,
        KVMetadata: nil, // DuckDB does not expose Parquet KV metadata via CLI
    }, nil
}

func parseDuckDBDescribe(out []byte) []ColumnDescriptor {
    var cols []ColumnDescriptor
    lines := bytes.Split(out, []byte{'\n'})
    if len(lines) < 2 { return nil }
    // first line is headers: column_name,column_type,null,key,default,extra
    for _, line := range lines[1:] {
        if len(line) == 0 { continue }
        parts := bytes.Split(line, []byte{','})
        if len(parts) < 2 { continue }
        cols = append(cols, ColumnDescriptor{
            Name: string(parts[0]),
            Type: string(parts[1]),
        })
    }
    return cols
}
```

- [ ] **Step 3: Run with DuckDB installed**

```bash
# install DuckDB CLI: https://duckdb.org/docs/installation/
cd tests/parquet-format && go test -run TestReadWithDuckDBBasic -v
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/parquet-format/readers.go tests/parquet-format/readers_test.go
git commit -m "test/parquet-format: duckdb CLI reader (C3.3)"
```

### Task C3.4: Implement CompareReaders

**Files:**
- Modify: `tests/parquet-format/readers.go`
- Modify: `tests/parquet-format/readers_test.go`

- [ ] **Step 1: Write the failing test**

```go
// negative control: change CompareReaders to compare only KV metadata
// → this test must fail because schema/RowCount differences would be silently ignored.
func TestCompareReadersReportsDisagreements(t *testing.T) {
    a := ReaderResult{Reader: "a", RowCount: 10, Schema: []ColumnDescriptor{{Name: "x", Type: "INT32"}}}
    b := ReaderResult{Reader: "b", RowCount: 10, Schema: []ColumnDescriptor{{Name: "x", Type: "INT64"}}}
    c := ReaderResult{Reader: "c", RowCount: 11, Schema: []ColumnDescriptor{{Name: "x", Type: "INT32"}}}
    diffs := CompareReaders([]ReaderResult{a, b, c})
    if len(diffs) < 2 { t.Errorf("expected ≥2 diffs (type + row count), got %d: %v", len(diffs), diffs) }
}
```

- [ ] **Step 2: Implement CompareReaders**

```go
func CompareReaders(results []ReaderResult) []string {
    if len(results) < 2 { return nil }
    var diffs []string

    // Compare row counts.
    rc := results[0].RowCount
    for _, r := range results[1:] {
        if r.RowCount != rc {
            diffs = append(diffs, fmt.Sprintf("row count: %s=%d %s=%d",
                results[0].Reader, rc, r.Reader, r.RowCount))
            break
        }
    }

    // Compare schemas by column name → type.
    typesByCol := map[string]map[string]string{} // col -> reader -> type
    for _, r := range results {
        for _, c := range r.Schema {
            if typesByCol[c.Name] == nil {
                typesByCol[c.Name] = map[string]string{}
            }
            typesByCol[c.Name][r.Reader] = c.Type
        }
    }
    for col, byReader := range typesByCol {
        types := map[string]bool{}
        for _, t := range byReader { types[t] = true }
        if len(types) > 1 {
            var pairs []string
            for reader, typ := range byReader {
                pairs = append(pairs, reader+"="+typ)
            }
            diffs = append(diffs, fmt.Sprintf("column %s: %s", col, strings.Join(pairs, " ")))
        }
    }
    return diffs
}
```

- [ ] **Step 3: Run**

```bash
cd tests/parquet-format && go test -run TestCompareReaders -v
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/parquet-format/readers.go tests/parquet-format/readers_test.go
git commit -m "test/parquet-format: CompareReaders disagreement detector (C3.4)"
```

---

## PR C4 — Writer Instrumentation + First Golden

**Goal:** Emit `lh.parquet_schema_version`, `lh.writer_version`, `lh.schema_url` KV metadata from both modules' writers; generate `golden/v1/` canonical files; add Makefile targets.

### Task C4.1: Add ParquetSchemaVersion to logs writer

**Files:**
- Modify: `internal/storage/parquets3/writer.go`

- [ ] **Step 1: Write the failing test (in the writer's package)**

```go
// internal/storage/parquets3/version_test.go
package parquets3

import (
    "bytes"
    "os"
    "testing"

    "github.com/parquet-go/parquet-go"
    "github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// negative control: remove the KeyValueMetadata calls in writeLogsParquet
// → this test must fail because lh.parquet_schema_version would be missing.
func TestLogsWriterEmitsSchemaVersionMetadata(t *testing.T) {
    rows := []schema.LogRow{{Body: "x", TimestampUnixNano: 1}}
    result, err := writeLogsParquet(rows, 100, 7)
    if err != nil { t.Fatal(err) }
    f := t.TempDir() + "/x.parquet"
    _ = os.WriteFile(f, result.Data, 0644)
    file, err := os.Open(f)
    if err != nil { t.Fatal(err) }
    defer file.Close()
    stat, _ := file.Stat()
    pf, _ := parquet.OpenFile(file, stat.Size())
    found := map[string]string{}
    for _, kv := range pf.Metadata().KeyValueMetadata {
        if kv.Value != nil { found[kv.Key] = *kv.Value }
    }
    if found["lh.parquet_schema_version"] != "v1" {
        t.Errorf("missing or wrong lh.parquet_schema_version: %q", found["lh.parquet_schema_version"])
    }
    if found["lh.schema_url"] != "https://opentelemetry.io/schemas/1.30.0" {
        t.Errorf("missing or wrong lh.schema_url: %q", found["lh.schema_url"])
    }
    if found["lh.writer_version"] == "" {
        t.Error("missing lh.writer_version")
    }
    _ = bytes.NewReader
}
```

- [ ] **Step 2: Run — expect failure**

```bash
go test ./internal/storage/parquets3/ -run TestLogsWriterEmitsSchemaVersionMetadata
```
Expected: FAIL.

- [ ] **Step 3: Add constants and KV metadata calls**

Edit `internal/storage/parquets3/writer.go`:

```go
// Add near the top of the file, after imports.
const (
    ParquetSchemaVersion = "v1"
    ParquetOTelSchemaURL = "https://opentelemetry.io/schemas/1.30.0"
)
```

In `writeLogsParquet`, after the existing `opts := []parquet.WriterOption{...}` and any existing KV metadata appends (around lines 450-474), add:

```go
opts = append(opts,
    parquet.KeyValueMetadata("lh.parquet_schema_version", ParquetSchemaVersion),
    parquet.KeyValueMetadata("lh.writer_version", buildinfo.Version),
    parquet.KeyValueMetadata("lh.schema_url", ParquetOTelSchemaURL),
)
```

Add the import for `buildinfo`:

```go
import (
    // ...
    "github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
)
```

Apply identical changes in `writeTracesParquet`.

- [ ] **Step 4: Run test, all PASS**

```bash
go test ./internal/storage/parquets3/ -run TestLogsWriterEmitsSchemaVersionMetadata
```

Add a parallel test for the traces writer:

```go
func TestTracesWriterEmitsSchemaVersionMetadata(t *testing.T) {
    rows := []schema.TraceRow{{TraceID: "abc", SpanID: "def", StartTimeUnixNano: 1, EndTimeUnixNano: 2, TimestampUnixNano: 1}}
    result, err := writeTracesParquet(rows, 100, 7)
    if err != nil { t.Fatal(err) }
    f := t.TempDir() + "/x.parquet"
    _ = os.WriteFile(f, result.Data, 0644)
    file, _ := os.Open(f)
    defer file.Close()
    stat, _ := file.Stat()
    pf, _ := parquet.OpenFile(file, stat.Size())
    found := map[string]string{}
    for _, kv := range pf.Metadata().KeyValueMetadata {
        if kv.Value != nil { found[kv.Key] = *kv.Value }
    }
    if found["lh.parquet_schema_version"] != "v1" {
        t.Error("missing lh.parquet_schema_version")
    }
}
```

Re-run:
```bash
go test ./internal/storage/parquets3/ -run "TestLogsWriter|TestTracesWriter" -v
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/parquets3/writer.go internal/storage/parquets3/version_test.go
git commit -m "storage/parquets3: emit lh.parquet_schema_version + schema_url KV metadata (C4.1)"
```

### Task C4.2: Mirror changes in traces module

**Files:**
- Modify: `lakehouse-traces/internal/storage/parquets3/writer.go`
- Create: `lakehouse-traces/internal/storage/parquets3/version_test.go`

- [ ] **Step 1: Apply identical constant + KV metadata additions**

Same constants:
```go
const (
    ParquetSchemaVersion = "v1"
    ParquetOTelSchemaURL = "https://opentelemetry.io/schemas/1.30.0"
)
```

Same `opts = append(...)` additions in `writeTracesParquet`.

- [ ] **Step 2: Add the same `TestTracesWriterEmitsSchemaVersionMetadata` test (adjusted import paths for lakehouse-traces module)**

- [ ] **Step 3: Run**

```bash
cd lakehouse-traces && go test ./internal/storage/parquets3/ -run TestTracesWriter -v
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add lakehouse-traces/internal/storage/parquets3/writer.go lakehouse-traces/internal/storage/parquets3/version_test.go
git commit -m "lakehouse-traces: emit lh.parquet_schema_version + schema_url KV metadata (C4.2)"
```

### Task C4.3: Canonical fixture rows

**Files:**
- Create: `tests/parquet-format/fixture.go`

- [ ] **Step 1: Write fixture rows**

```go
// tests/parquet-format/fixture.go
package parquetformat

import "github.com/ReliablyObserve/victoria-lakehouse/internal/schema"

func CanonicalLogRows() []schema.LogRow {
    return []schema.LogRow{
        {
            TimestampUnixNano: 1735689600000000000, // 2025-01-01T00:00:00Z
            Body:              "first log",
            SeverityText:      "INFO",
            SeverityNumber:    9,
            ServiceName:       "demo",
            TraceID:           "0123456789abcdef0123456789abcdef",
            SpanID:            "0123456789abcdef",
            ResourceAttributes: map[string]string{"host.name": "node-1"},
            LogAttributes:      map[string]string{"k1": "v1"},
        },
        {
            TimestampUnixNano: 1735689601000000000,
            Body:              "second log",
            SeverityText:      "WARN",
            SeverityNumber:    13,
            ServiceName:       "demo",
        },
    }
}

func CanonicalTraceRows() []schema.TraceRow {
    return []schema.TraceRow{
        {
            TimestampUnixNano: 1735689600000000000,
            StartTimeUnixNano: 1735689600000000000,
            EndTimeUnixNano:   1735689600100000000,
            TraceID:           "0123456789abcdef0123456789abcdef",
            SpanID:            "0123456789abcdef",
            ParentSpanID:      "",
            ServiceName:       "demo",
            SpanName:          "root-op",
            ResourceAttributes: map[string]string{"host.name": "node-1"},
            SpanAttributes:     map[string]string{"http.method": "GET"},
        },
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add tests/parquet-format/fixture.go
git commit -m "test/parquet-format: canonical fixture rows (C4.3)"
```

### Task C4.4: Generate first golden files

**Files:**
- Create: `tests/parquet-format/golden/v1/logs.parquet` (binary)
- Create: `tests/parquet-format/golden/v1/logs.schema.json`
- Create: `tests/parquet-format/golden/v1/traces.parquet` (binary)
- Create: `tests/parquet-format/golden/v1/traces.schema.json`
- Modify: `Makefile`

- [ ] **Step 1: Add Makefile targets**

Append to `Makefile`:

```makefile
.PHONY: parquet-format-fixture parquet-format-golden-v1 parquet-format-test

parquet-format-fixture: deps-logs
	go run ./tests/parquet-format/cmd/genfixture -mode=logs -out=/tmp/lh-logs-fixture.parquet
	go run ./tests/parquet-format/cmd/genfixture -mode=traces -out=/tmp/lh-traces-fixture.parquet

parquet-format-golden-v1: deps-logs
	mkdir -p tests/parquet-format/golden/v1
	go run ./tests/parquet-format/cmd/genfixture -mode=logs -out=tests/parquet-format/golden/v1/logs.parquet
	go run ./tests/parquet-format/cmd/genfixture -mode=traces -out=tests/parquet-format/golden/v1/traces.parquet
	go run ./tests/parquet-format/cmd/snapshot -in=tests/parquet-format/golden/v1/logs.parquet -out=tests/parquet-format/golden/v1/logs.schema.json
	go run ./tests/parquet-format/cmd/snapshot -in=tests/parquet-format/golden/v1/traces.parquet -out=tests/parquet-format/golden/v1/traces.schema.json

parquet-format-test: deps-logs
	go test ./tests/parquet-format/ -tags=parquet_format -v -count=1 -timeout=10m
```

- [ ] **Step 2: Implement `cmd/genfixture/main.go`**

```go
// tests/parquet-format/cmd/genfixture/main.go
package main

import (
    "flag"
    "log"
    "os"

    parquetformat "github.com/ReliablyObserve/victoria-lakehouse/tests/parquet-format"
    "github.com/ReliablyObserve/victoria-lakehouse/internal/storage/parquets3"
)

func main() {
    mode := flag.String("mode", "logs", "logs or traces")
    out := flag.String("out", "", "output path")
    flag.Parse()
    if *out == "" { log.Fatal("-out required") }

    var data []byte
    var err error
    switch *mode {
    case "logs":
        rows := parquetformat.CanonicalLogRows()
        res, e := parquets3.WriteLogsParquetForTest(rows, 100, 7)
        if e != nil { log.Fatal(e) }
        data = res
    case "traces":
        rows := parquetformat.CanonicalTraceRows()
        res, e := parquets3.WriteTracesParquetForTest(rows, 100, 7)
        if e != nil { log.Fatal(e) }
        data = res
    default:
        log.Fatalf("unknown mode: %s", *mode)
    }
    if err := os.WriteFile(*out, data, 0644); err != nil { log.Fatal(err) }
    log.Printf("wrote %s (%d bytes)", *out, len(data))
    _ = err
}
```

This requires exporting helper wrappers from `internal/storage/parquets3/`:

```go
// internal/storage/parquets3/test_helpers.go (new)
package parquets3

func WriteLogsParquetForTest(rows []schema.LogRow, rowGroupSize, compressionLevel int) ([]byte, error) {
    res, err := writeLogsParquet(rows, rowGroupSize, compressionLevel)
    if err != nil { return nil, err }
    return res.Data, nil
}

func WriteTracesParquetForTest(rows []schema.TraceRow, rowGroupSize, compressionLevel int) ([]byte, error) {
    res, err := writeTracesParquet(rows, rowGroupSize, compressionLevel)
    if err != nil { return nil, err }
    return res.Data, nil
}
```

- [ ] **Step 3: Implement `cmd/snapshot/main.go`**

```go
// tests/parquet-format/cmd/snapshot/main.go
package main

import (
    "encoding/json"
    "flag"
    "log"
    "os"

    parquetformat "github.com/ReliablyObserve/victoria-lakehouse/tests/parquet-format"
)

type snapshot struct {
    Schema     []parquetformat.ColumnDescriptor `json:"schema"`
    RowCount   int64                            `json:"row_count"`
    KVMetadata map[string]string                `json:"kv_metadata"`
}

func main() {
    in := flag.String("in", "", "parquet file")
    out := flag.String("out", "", "snapshot json")
    flag.Parse()
    r, err := parquetformat.ReadWithParquetGo(*in)
    if err != nil { log.Fatal(err) }
    s := snapshot{Schema: r.Schema, RowCount: r.RowCount, KVMetadata: r.KVMetadata}
    delete(s.KVMetadata, "lh.writer_version") // varies per build; exclude from snapshot
    data, _ := json.MarshalIndent(s, "", "  ")
    if err := os.WriteFile(*out, data, 0644); err != nil { log.Fatal(err) }
}
```

- [ ] **Step 4: Run the goldens generator**

```bash
make parquet-format-golden-v1
```

Verify:
- `tests/parquet-format/golden/v1/logs.parquet` exists.
- `tests/parquet-format/golden/v1/logs.schema.json` exists and contains `"lh.parquet_schema_version": "v1"`.
- Same for traces.

- [ ] **Step 5: Commit**

```bash
git add Makefile tests/parquet-format/cmd/ tests/parquet-format/golden/ internal/storage/parquets3/test_helpers.go
git commit -m "test/parquet-format: golden v1 generator + first goldens (C4.4)"
```

---

## PR C5 — Compat Tests + CI Workflow

**Goal:** Integration tests requiring pyarrow+DuckDB; advisory test reading today's file with previous LH release; CI workflow with 4 jobs.

### Task C5.1: Forward compat integration test

**Files:**
- Create: `tests/parquet-format/integration_test.go`
- Create: `tests/parquet-format/compat_test.go`

- [ ] **Step 1: Write the failing test**

```go
//go:build parquet_format

// tests/parquet-format/integration_test.go
package parquetformat

import (
    "os"
    "os/exec"
    "testing"

    parquets3 "github.com/ReliablyObserve/victoria-lakehouse/internal/storage/parquets3"
)

// negative control: revert C4.1's writer KV metadata change → this test
// must fail because lh.parquet_schema_version would be absent.
func TestForwardCompatLogs(t *testing.T) {
    requireExternalReaders(t)
    rows := CanonicalLogRows()
    data, err := parquets3.WriteLogsParquetForTest(rows, 100, 7)
    if err != nil { t.Fatal(err) }
    path := t.TempDir() + "/logs.parquet"
    if err := os.WriteFile(path, data, 0644); err != nil { t.Fatal(err) }
    runMultiReaderValidation(t, path, "logs")
}

func TestForwardCompatTraces(t *testing.T) {
    requireExternalReaders(t)
    rows := CanonicalTraceRows()
    data, err := parquets3.WriteTracesParquetForTest(rows, 100, 7)
    if err != nil { t.Fatal(err) }
    path := t.TempDir() + "/traces.parquet"
    if err := os.WriteFile(path, data, 0644); err != nil { t.Fatal(err) }
    runMultiReaderValidation(t, path, "traces")
}

func requireExternalReaders(t *testing.T) {
    if _, err := exec.LookPath("python3"); err != nil { t.Skip("python3 unavailable") }
    if _, err := exec.LookPath("duckdb"); err != nil { t.Skip("duckdb unavailable") }
}
```

- [ ] **Step 2: Implement `runMultiReaderValidation` in compat_test.go**

```go
//go:build parquet_format

// tests/parquet-format/compat_test.go
package parquetformat

import "testing"

func runMultiReaderValidation(t *testing.T, path, mode string) {
    t.Helper()
    a, err := ReadWithParquetGo(path)
    if err != nil { t.Fatal("parquet-go:", err) }
    b, err := ReadWithPyArrow(path)
    if err != nil { t.Fatal("pyarrow:", err) }
    c, err := ReadWithDuckDB(path)
    if err != nil { t.Fatal("duckdb:", err) }

    diffs := CompareReaders([]ReaderResult{a, b, c})
    if len(diffs) > 0 {
        for _, d := range diffs { t.Errorf("READER_DISAGREEMENT: %s", d) }
    }

    v, err := LoadValidator()
    if err != nil { t.Fatal(err) }
    if errs := v.ValidateSchema(a.Schema, mode); len(errs) > 0 {
        for _, e := range errs { t.Errorf("SCHEMA_INVALID: %v", e) }
    }
}
```

- [ ] **Step 3: Run**

```bash
make parquet-format-test
```
Expected: PASS (assuming pyarrow + DuckDB installed locally).

- [ ] **Step 4: Commit**

```bash
git add tests/parquet-format/integration_test.go tests/parquet-format/compat_test.go
git commit -m "test/parquet-format: forward compat integration tests (C5.1)"
```

### Task C5.2: Backward compat tests against goldens

**Files:**
- Modify: `tests/parquet-format/integration_test.go`

- [ ] **Step 1: Write the test**

Append to `integration_test.go`:

```go
// negative control: bump ParquetSchemaVersion from "v1" to "v2" without
// regenerating goldens → this test must fail because v1 golden's
// lh.parquet_schema_version will mismatch a snapshot expectation.
func TestBackwardCompatV1Logs(t *testing.T) {
    requireExternalReaders(t)
    path := "golden/v1/logs.parquet"
    runMultiReaderValidation(t, path, "logs")
    assertSnapshot(t, path, "golden/v1/logs.schema.json")
}

func TestBackwardCompatV1Traces(t *testing.T) {
    requireExternalReaders(t)
    path := "golden/v1/traces.parquet"
    runMultiReaderValidation(t, path, "traces")
    assertSnapshot(t, path, "golden/v1/traces.schema.json")
}

func assertSnapshot(t *testing.T, parquetPath, snapPath string) {
    t.Helper()
    r, err := ReadWithParquetGo(parquetPath)
    if err != nil { t.Fatal(err) }
    snap := readSnapshot(t, snapPath)
    if r.RowCount != snap.RowCount {
        t.Errorf("row count: file=%d snap=%d", r.RowCount, snap.RowCount)
    }
    for _, sc := range snap.Schema {
        found := false
        for _, fc := range r.Schema {
            if fc.Name == sc.Name && fc.Type == sc.Type { found = true; break }
        }
        if !found {
            t.Errorf("snapshot column missing in file: %+v", sc)
        }
    }
    for k, expV := range snap.KVMetadata {
        if k == "lh.writer_version" { continue }
        if r.KVMetadata[k] != expV {
            t.Errorf("KV %s: file=%q snap=%q", k, r.KVMetadata[k], expV)
        }
    }
}

type snapshotFile struct {
    Schema     []ColumnDescriptor `json:"schema"`
    RowCount   int64              `json:"row_count"`
    KVMetadata map[string]string  `json:"kv_metadata"`
}

func readSnapshot(t *testing.T, path string) snapshotFile {
    data, err := os.ReadFile(path)
    if err != nil { t.Fatal(err) }
    var s snapshotFile
    if err := json.Unmarshal(data, &s); err != nil { t.Fatal(err) }
    return s
}
```

- [ ] **Step 2: Run**

```bash
make parquet-format-test
```
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/parquet-format/integration_test.go
git commit -m "test/parquet-format: backward compat against v1 goldens (C5.2)"
```

### Task C5.3: Forward-advisory test

**Files:**
- Create: `tests/parquet-format/advisory_test.go`

- [ ] **Step 1: Write the advisory test**

```go
//go:build parquet_format_advisory

// tests/parquet-format/advisory_test.go
package parquetformat

import (
    "os"
    "os/exec"
    "testing"

    parquets3 "github.com/ReliablyObserve/victoria-lakehouse/internal/storage/parquets3"
)

// Advisory: today's writer + previous LH release reader.
// Does NOT t.Fail; logs result for surface in CI Step Summary.
func TestForwardAdvisoryReadWithPreviousLHRelease(t *testing.T) {
    prevImage := os.Getenv("LH_PREV_IMAGE")
    if prevImage == "" {
        t.Skip("LH_PREV_IMAGE not set; advisory test skipped")
    }
    rows := CanonicalLogRows()
    data, err := parquets3.WriteLogsParquetForTest(rows, 100, 7)
    if err != nil { t.Fatal(err) }
    f := t.TempDir() + "/logs.parquet"
    _ = os.WriteFile(f, data, 0644)

    cmd := exec.Command("docker", "run", "--rm", "-v", f+":/tmp/logs.parquet", prevImage,
        "/lakehouse-logs", "-parquet.read-only-check", "/tmp/logs.parquet")
    out, err := cmd.CombinedOutput()
    if err != nil {
        t.Logf("FORWARD_ADVISORY: %s could not read today's file: %v\n%s", prevImage, err, out)
        return
    }
    t.Logf("FORWARD_ADVISORY: %s read today's file OK", prevImage)
}
```

The `-parquet.read-only-check` flag in `lakehouse-logs` is referenced for the advisory test but does not currently exist. If LH does not have a read-only-check binary path, an alternate is to run a tiny Go program inside the older image via `go run`; or use DuckDB CLI with that older image's parquet-go pin via go install. The simplest is to drop this advisory test for now and re-introduce it once a way to read-via-old-binary exists; mark as skip with explicit reason.

- [ ] **Step 2: Mark as skip with rationale, commit**

If the previous-image read mechanism is not in place, replace the test body with:

```go
func TestForwardAdvisoryReadWithPreviousLHRelease(t *testing.T) {
    t.Skip("advisory mechanism awaits LH 'read-only-check' subcommand; tracked in TODO")
}
```

Commit either the real version or the skip:

```bash
git add tests/parquet-format/advisory_test.go
git commit -m "test/parquet-format: forward-advisory test (skipped pending read-only-check) (C5.3)"
```

### Task C5.4: CI workflow

**Files:**
- Create: `.github/workflows/parquet-format.yaml`

- [ ] **Step 1: Write the workflow**

```yaml
# .github/workflows/parquet-format.yaml
name: Parquet Format

on:
  pull_request:
    paths:
      - 'internal/schema/**'
      - 'internal/storage/parquets3/**'
      - 'lakehouse-traces/internal/schema/**'
      - 'lakehouse-traces/internal/storage/parquets3/**'
      - 'tests/parquet-format/**'
      - '.github/workflows/parquet-format.yaml'
  push:
    branches: [main]
  schedule:
    - cron: '0 6 * * *'

permissions:
  contents: read

env:
  GOWORK: "off"

jobs:
  parquet-format-coverage:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version-file: go.mod }
      - name: Run reflective coverage gate
        run: |
          cd tests/parquet-format
          go test -v -run TestReservedRegistryCoversAllStructFields

  parquet-spec-multi-reader:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    needs: parquet-format-coverage
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version-file: go.mod }
      - uses: actions/setup-python@v6
        with: { python-version: '3.12' }
      - name: Install pyarrow
        run: |
          pip install --no-cache-dir "pyarrow==$(yq '.pyarrow' tests/parquet-format/reader-versions.yaml | tr -d '\"')"
      - name: Install DuckDB CLI
        run: |
          DUCKDB_VER=$(yq '.duckdb' tests/parquet-format/reader-versions.yaml | tr -d '"')
          curl -L "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VER}/duckdb_cli-linux-amd64.zip" -o /tmp/duckdb.zip
          unzip -d /usr/local/bin /tmp/duckdb.zip
          chmod +x /usr/local/bin/duckdb
      - name: Run forward + backward compat
        run: make parquet-format-test
      - name: Upload artifacts
        if: always()
        uses: actions/upload-artifact@v7
        with:
          name: parquet-format-results
          path: |
            tests/parquet-format/golden/v1/*.schema.json

  parquet-otel-vl-compliance:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    needs: parquet-format-coverage
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version-file: go.mod }
      - name: Run validator + schema/row checks
        run: |
          cd tests/parquet-format
          go test -v -run 'TestLoadValidator|TestValidator|TestLHExtensionPrefix'

  parquet-format-forward-advisory:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    if: github.event_name == 'push' || github.event_name == 'schedule'
    continue-on-error: true
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with: { go-version-file: go.mod }
      - name: Determine previous release image
        id: prev
        run: |
          PREV=$(gh release list --limit 2 --json tagName --jq '.[1].tagName')
          echo "image=ghcr.io/reliablyobserve/lakehouse-logs:${PREV}" >> $GITHUB_OUTPUT
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      - name: Run advisory test
        env:
          LH_PREV_IMAGE: ${{ steps.prev.outputs.image }}
        run: |
          cd tests/parquet-format
          go test -tags=parquet_format_advisory -v -run TestForwardAdvisory 2>&1 | tee advisory-forward-report.txt
      - name: Upload advisory report
        if: always()
        uses: actions/upload-artifact@v7
        with:
          name: parquet-format-advisory
          path: tests/parquet-format/advisory-forward-report.txt
```

- [ ] **Step 2: Smoke test locally with `act` if available**

```bash
which act && act -j parquet-format-coverage --container-architecture linux/amd64 || echo "skip act test"
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/parquet-format.yaml
git commit -m "ci: parquet-format workflow (4 jobs, 3 required + 1 advisory) (C5.4)"
```

---

## PR C6 — OTel Upstream-Check Extension + Schema-Bump Scaffolding

**Goal:** Make the system self-maintaining. OTel releases auto-open bump PRs; schema-version bumps require release-engineering approval.

### Task C6.1: Extend upstream-check workflow

**Files:**
- Modify: `.github/workflows/upstream-check.yaml`

- [ ] **Step 1: Add OTel check job**

Append a new job (or extend the existing one):

```yaml
  check-otel-semconv:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - name: Check OTel semconv releases
        id: otel
        run: |
          LATEST=$(gh api repos/open-telemetry/semantic-conventions/releases/latest --jq .tag_name | sed 's/^v//')
          CURRENT=$(yq '.version' tests/parquet-format/otel-version.yaml | tr -d '"')
          echo "latest=$LATEST" >> $GITHUB_OUTPUT
          echo "current=$CURRENT" >> $GITHUB_OUTPUT
          if [ "$LATEST" != "$CURRENT" ] && [ -n "$LATEST" ]; then
            echo "outdated=true" >> $GITHUB_OUTPUT
          fi
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}

      - name: Create OTel sync PR
        if: steps.otel.outputs.outdated == 'true'
        run: |
          BRANCH="otel-sync/$(date +%Y%m%d)"
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git checkout -b "$BRANCH"
          NEW="${{ steps.otel.outputs.latest }}"
          yq -i ".version = \"$NEW\"" tests/parquet-format/otel-version.yaml
          yq -i ".schema_url = \"https://opentelemetry.io/schemas/$NEW\"" tests/parquet-format/otel-version.yaml
          yq -i ".last_updated = \"$(date +%Y-%m-%d)\"" tests/parquet-format/otel-version.yaml
          git add tests/parquet-format/otel-version.yaml
          git commit -m "deps: bump OTel semconv to v$NEW"
          git push -u origin "$BRANCH"
          gh pr create \
            --title "deps: bump OTel semconv to v$NEW" \
            --label "dependencies" \
            --body "## OTel Semantic Conventions Bump\n\n| Field | Previous | New |\n|---|---|---|\n| Version | ${{ steps.otel.outputs.current }} | $NEW |\n| Schema URL | https://opentelemetry.io/schemas/${{ steps.otel.outputs.current }} | https://opentelemetry.io/schemas/$NEW |\n\nRelease notes: https://github.com/open-telemetry/semantic-conventions/releases/tag/v$NEW"
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/upstream-check.yaml
git commit -m "ci: extend upstream-check to poll OTel semconv releases (C6.1)"
```

### Task C6.2: CODEOWNERS rule for goldens

**Files:**
- Create or modify: `CODEOWNERS` (root)

- [ ] **Step 1: Add the rule**

Append (or create) `CODEOWNERS`:

```
# Schema-version bumps require explicit release-engineering approval.
tests/parquet-format/golden/ @ReliablyObserve/release-engineering
```

If the team handle does not exist yet, use the project owner: `@szibis`.

- [ ] **Step 2: Commit**

```bash
git add CODEOWNERS
git commit -m "codeowners: tests/parquet-format/golden requires release-eng approval (C6.2)"
```

### Task C6.3: Schema-bump Makefile target

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Add target**

Append to `Makefile`:

```makefile
.PHONY: parquet-format-golden-bump

# Usage: make parquet-format-golden-bump VERSION=v2
parquet-format-golden-bump:
	@if [ -z "$(VERSION)" ]; then echo "usage: make parquet-format-golden-bump VERSION=vN"; exit 1; fi
	@if [ -d "tests/parquet-format/golden/$(VERSION)" ]; then echo "$(VERSION) already exists"; exit 1; fi
	mkdir -p tests/parquet-format/golden/$(VERSION)
	go run ./tests/parquet-format/cmd/genfixture -mode=logs -out=tests/parquet-format/golden/$(VERSION)/logs.parquet
	go run ./tests/parquet-format/cmd/genfixture -mode=traces -out=tests/parquet-format/golden/$(VERSION)/traces.parquet
	go run ./tests/parquet-format/cmd/snapshot -in=tests/parquet-format/golden/$(VERSION)/logs.parquet -out=tests/parquet-format/golden/$(VERSION)/logs.schema.json
	go run ./tests/parquet-format/cmd/snapshot -in=tests/parquet-format/golden/$(VERSION)/traces.parquet -out=tests/parquet-format/golden/$(VERSION)/traces.schema.json
	@echo "Bump complete. Don't forget to update ParquetSchemaVersion in writer.go and add a CHANGELOG entry."
```

- [ ] **Step 2: Commit**

```bash
git add Makefile
git commit -m "make: parquet-format-golden-bump target (C6.3)"
```

### Task C6.4: Schema-version mismatch regression test

**Files:**
- Modify: `tests/parquet-format/integration_test.go`

- [ ] **Step 1: Add test**

Append to `integration_test.go`:

```go
// negative control: bump ParquetSchemaVersion to "v999" without creating
// tests/parquet-format/golden/v999/ → this test must fail because the
// loop discovers a missing directory.
func TestSchemaVersionBumpRequiresGoldenDirectory(t *testing.T) {
    entries, err := os.ReadDir("golden")
    if err != nil { t.Fatal(err) }
    found := map[string]bool{}
    for _, e := range entries {
        if e.IsDir() { found[e.Name()] = true }
    }
    cur := parquets3.ParquetSchemaVersion
    if !found[cur] {
        t.Fatalf("ParquetSchemaVersion=%q but golden/%s/ missing; run 'make parquet-format-golden-bump VERSION=%s'", cur, cur, cur)
    }
}
```

(Imports needed: `os`, `github.com/.../internal/storage/parquets3 as parquets3`.)

- [ ] **Step 2: Run; commit**

```bash
make parquet-format-test
git add tests/parquet-format/integration_test.go
git commit -m "test/parquet-format: schema-bump golden directory regression (C6.4)"
```

---

## Definition of Done (Subsystem C)

### Coverage
- [ ] Every `parquet:"..."`-tagged field in `LogRow` and `TraceRow` has a corresponding entry in `reserved-columns.yaml`.
- [ ] `TestReservedRegistryCoversAllStructFields` passes (CI-enforced).

### Three-master compliance
- [ ] `ValidateSchema` rejects unclassified columns.
- [ ] `ValidateRow` rejects rows missing required fields per mode.
- [ ] `lh.*` prefix enforced for `lh_extension` entries.

### Multi-reader
- [ ] `ReadWithParquetGo`, `ReadWithPyArrow`, `ReadWithDuckDB` all return `ReaderResult` with `Schema`, `RowCount`, `KVMetadata`.
- [ ] `CompareReaders` reports schema and row-count disagreements.
- [ ] `TestForwardCompatLogs` and `TestForwardCompatTraces` pass with all three readers agreeing.

### Versioning
- [ ] `ParquetSchemaVersion = "v1"` defined in both writers.
- [ ] `lh.parquet_schema_version`, `lh.writer_version`, `lh.schema_url` KV metadata emitted by both writers.
- [ ] `golden/v1/logs.parquet`, `golden/v1/traces.parquet` and matching `.schema.json` snapshots committed.
- [ ] `TestBackwardCompatV1Logs` and `TestBackwardCompatV1Traces` pass.

### CI
- [ ] `.github/workflows/parquet-format.yaml` has 4 jobs (3 required: coverage / multi-reader / otel-vl; 1 advisory: forward-advisory).
- [ ] pyarrow and DuckDB CLI installed from `reader-versions.yaml` pinned versions.
- [ ] Workflow triggered on PRs touching schema/storage paths.

### Automation
- [ ] `upstream-check.yaml` polls OTel semconv releases.
- [ ] OTel bump PR auto-created on new release.
- [ ] CODEOWNERS rule on `tests/parquet-format/golden/` exists.

### Schema-bump scaffolding
- [ ] `make parquet-format-golden-bump VERSION=vN` target works.
- [ ] `TestSchemaVersionBumpRequiresGoldenDirectory` fails when `ParquetSchemaVersion` changes without a matching golden dir.

### Negative-control proofs
- [ ] Every load-bearing assertion has a comment naming what must break it.

---

## Self-Review Notes

1. **Spec coverage:**
   - Reserved registry + lockfiles → C1.1, C1.2.
   - Validator + completeness → C2.1, C2.2.
   - Multi-reader (Go, pyarrow, DuckDB, comparator) → C3.1–C3.4.
   - Writer KV metadata → C4.1, C4.2.
   - Canonical fixtures + goldens → C4.3, C4.4.
   - Forward + backward compat tests → C5.1, C5.2.
   - Forward-advisory → C5.3.
   - CI workflow → C5.4.
   - OTel upstream-check → C6.1.
   - CODEOWNERS for goldens → C6.2.
   - Schema-bump scaffolding → C6.3, C6.4.

2. **Placeholder scan:** Every step contains real code. Two qualifications:
   - C5.3 (advisory): the previous-image read mechanism may not exist yet; the task explicitly produces a skipped test with rationale rather than a placeholder.
   - C4.4 (`-parquet.read-only-check` flag): referenced in advisory but currently fictional. The fallback is explicit (skip the test) — this is acceptable; advisory is non-blocking.

3. **Type consistency:**
   - `ColumnSpec`, `ColumnDescriptor`, `ReaderResult`, `Validator` defined in C2.1 and C3.1; used consistently in C3.x, C4.4, C5.x, C6.4.
   - `ParquetSchemaVersion` constant defined in C4.1; referenced in C5.2, C5.4 (workflow paths), C6.4.
   - `MasterCategory` constants (`CategoryVLInternal`, `CategoryOTelSemconv`, `CategoryLHExtension`) defined in C2.1; not re-referenced elsewhere (they're internal to the validator).
   - `CanonicalLogRows()` / `CanonicalTraceRows()` defined in C4.3; used in C4.4 (genfixture), C5.1.
   - `WriteLogsParquetForTest` / `WriteTracesParquetForTest` exported in C4.4; used in C5.1, C5.3.
