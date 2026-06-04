# Subsystem C — Parquet Format Compatibility

**Status:** Design

**Date:** 2026-06-02

**Continues:** Foundation for Subsystem D (direct Parquet analytics) and Subsystem E (community standards, narrowed scope).

---

## Goal

Build a validation harness that enforces three-master compliance on every LH-written Parquet file:

1. **Apache Parquet spec compliance** — three different parsers (Go's `parquet-go`, Python's `pyarrow`, DuckDB CLI) read each file and agree on schema, row count, and per-column type.
2. **OpenTelemetry semantic conventions** — non-underscore, non-`lh.*` columns conform to OTel logs/traces semconv at a pinned version (v1.30.0 initially). Types match OTel proto.
3. **VL/VT structural conventions** — `_`-prefixed columns conform to VL's internal-field rules (`_msg`, `_time`, `_stream`, `_stream_id`). Required for LH to remain a drop-in replacement for VL/VT query API.

Plus:
4. **LH extension namespace** — LH-only columns live exclusively under the `lh.*` prefix. Reserved in `reserved-columns.yaml`. Any column LH writes that isn't classified into one of the three masters fails CI.

Schema is **versioned**. Each LH release writes a `parquet_schema_version` KV pair into the Parquet file footer. Bidirectional compat tests protect rolling-deploy operations:
- Forward (today's writer + today's reader) — must pass.
- Backward (old writer + new reader) — must pass; protects mixed-version production fleets.
- Forward-advisory (new writer + old reader) — logged, not blocking; surfaces upcoming compat risks.

---

## Architecture

Tests live under `tests/parquet-format/`. New CI workflow `.github/workflows/parquet-format.yaml` runs four jobs on PRs touching `internal/schema/`, `internal/storage/parquets3/`, `lakehouse-traces/internal/storage/parquets3/`, or `tests/parquet-format/**`:

- `parquet-spec-multi-reader` — required.
- `parquet-otel-vl-compliance` — required.
- `parquet-format-coverage` — required.
- `parquet-format-forward-advisory` — advisory (logs only; doesn't block).

The harness uses:
- **parquet-go** (the lib LH already uses) for writing fixtures and reading them.
- **pyarrow** (pinned version) for cross-language read verification.
- **DuckDB CLI** (pinned version) for SQL-engine read verification and as a stepping stone to Subsystem D.

A reserved-columns YAML registry classifies every column LH writes into exactly one master. A reflective coverage test (`TestReservedRegistryCoversAllStructFields`) walks `schema.LogRow` and `schema.TraceRow` to enforce that no Parquet-tagged field escapes classification.

OTel version tracking lives in `tests/parquet-format/otel-version.yaml` (lockfile). An extended `upstream-check.yaml` polls OTel releases and auto-opens bump PRs.

---

## Components

### Reserved-columns registry (`tests/parquet-format/reserved-columns.yaml`)

Single source of truth classifying every column LH writes. Schema:

```yaml
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
    - name: service.name
      type: STRING
      source: "OTel v1.30 resource.service.name"
    - name: severity_text
      type: STRING
    - name: severity_number
      type: INT32
    - name: timestamp_unix_nano
      type: INT64
    - name: trace_id
      type: STRING
    - name: span_id
      type: STRING
  traces:
    - name: trace_id
      type: STRING
      required: true
    - name: span_id
      type: STRING
      required: true
    - name: parent_span_id
      type: STRING
    - name: start_time_unix_nano
      type: INT64
      required: true
    - name: end_time_unix_nano
      type: INT64
      required: true
    - name: service.name
      type: STRING

lh_extension:
  - name: lh.bloom_columns_metadata
    type: STRING
    source: "LH bloom filter columns metadata"
    file_metadata_only: true
  - name: lh.trace_index
    type: BINARY
    source: "LH trace index sidecar"
    file_metadata_only: true
```

Every entry must declare `source` citing an upstream spec line. `lh_extension` entries must have names starting with `lh.`.

### OTel lockfile (`tests/parquet-format/otel-version.yaml`)

```yaml
version: "1.30.0"
source: "https://github.com/open-telemetry/semantic-conventions/releases/tag/v1.30.0"
schema_url: "https://opentelemetry.io/schemas/1.30.0"
last_updated: "2026-06-02"
```

Used by:
- The validator (loads the locked version).
- The writer (emits `lh.schema_url` matching this URL in KV metadata).
- The upstream-check workflow (detects new releases).

### Reader version lockfile (`tests/parquet-format/reader-versions.yaml`)

```yaml
pyarrow: "20.0.0"
duckdb: "1.4.0"
parquet_go: "v0.25.0"
```

CI installs exact versions. Reader bumps are their own PRs.

### Validator (`tests/parquet-format/validator.go`)

```go
type MasterCategory int

const (
    CategoryVLInternal MasterCategory = iota
    CategoryOTelSemconv
    CategoryLHExtension
)

type ColumnSpec struct {
    Name             string
    Type             string
    Required         bool
    Category         MasterCategory
    Source           string
    FileMetadataOnly bool
}

type Validator struct {
    Reserved    map[string]ColumnSpec
    OTelVersion string
}

func LoadValidator() (*Validator, error)
func (v *Validator) ValidateSchema(schema parquet.Schema, mode string) error
func (v *Validator) ValidateRow(row map[string]any, mode string) error
```

Modes: `"logs"` or `"traces"`. Validator picks the right OTel ruleset.

`LoadValidator` enforces invariants:
- No column appears in two registries.
- Every `lh_extension` entry's name starts with `lh.`.
- Every entry has non-empty `source`.

### Multi-reader harness (`tests/parquet-format/readers.go`)

```go
type ReaderResult struct {
    Reader     string                  // "parquet-go" / "pyarrow" / "duckdb"
    Schema     []ColumnDescriptor
    RowCount   int64
    KVMetadata map[string]string
}

type ColumnDescriptor struct {
    Name string
    Type string
}

func ReadWithParquetGo(path string) (ReaderResult, error)
func ReadWithPyArrow(path string) (ReaderResult, error)   // shells to python
func ReadWithDuckDB(path string) (ReaderResult, error)    // shells to duckdb CLI

func CompareReaders(results []ReaderResult) []string      // diagnoses returned as strings
```

Shell invocations:
- pyarrow: `python3 -c "import pyarrow.parquet as pq; ..."`.
- DuckDB: `duckdb -c "DESCRIBE SELECT * FROM '<path>' LIMIT 0"` + `duckdb -c "SELECT COUNT(*) FROM '<path>'"`.

`CompareReaders` returns one string per disagreement, e.g.:
```
column severity_number: parquet-go=INT32 pyarrow=INT32 duckdb=INT64
```

### Writer KV metadata (modifications to `internal/storage/parquets3/writer.go`)

Add a constant:
```go
const ParquetSchemaVersion = "v1"
```

In `writeLogsParquet` and `writeTracesParquet`, append metadata:
```go
opts = append(opts, parquet.KeyValueMetadata("lh.parquet_schema_version", ParquetSchemaVersion))
opts = append(opts, parquet.KeyValueMetadata("lh.writer_version", buildinfo.Version))
opts = append(opts, parquet.KeyValueMetadata("lh.schema_url", "https://opentelemetry.io/schemas/1.30.0"))
```

### Golden file layout

```
tests/parquet-format/golden/
├── v1/
│   ├── logs.parquet                  # canonical logs file from v1 era
│   ├── logs.schema.json              # captured schema + KV metadata snapshot
│   ├── traces.parquet
│   └── traces.schema.json
└── v2/  # (added on next schema bump)
    └── ...
```

Each `<file>.schema.json` snapshot has:
```json
{
  "schema": [
    {"name": "_msg", "type": "STRING"},
    {"name": "_time", "type": "INT64"}
  ],
  "row_count": 100,
  "kv_metadata": {
    "lh.parquet_schema_version": "v1",
    "lh.schema_url": "https://opentelemetry.io/schemas/1.30.0"
  }
}
```

### Compat test orchestrator (`tests/parquet-format/compat_test.go`)

```go
func TestForwardCompat(t *testing.T) {
    // Today's writer produces today's file; today's reader reads it.
    file := writeLogsParquet(canonicalRows)
    runMultiReaderValidation(t, file)
}

func TestBackwardCompat(t *testing.T) {
    // Old goldens must be readable by today's reader.
    for _, ver := range listGoldenVersions("tests/parquet-format/golden/") {
        runMultiReaderValidation(t, "tests/parquet-format/golden/" + ver + "/logs.parquet")
        runMultiReaderValidation(t, "tests/parquet-format/golden/" + ver + "/traces.parquet")
    }
}

func TestForwardAdvisory(t *testing.T) {
    // Today's writer's output, read with previous LH release reader.
    // Advisory only — log but do not t.Fail.
}
```

### CI workflow (`.github/workflows/parquet-format.yaml`)

Four jobs (described above). Triggers on path changes to schema/storage/test directories.

### Upstream-check extension

Modify `.github/workflows/upstream-check.yaml` to also poll `https://api.github.com/repos/open-telemetry/semantic-conventions/releases/latest`. On version mismatch, open PR `deps: bump OTel semconv to <new>` updating `otel-version.yaml`.

### CODEOWNERS rule

```
tests/parquet-format/golden/ @reliablyobserve/release-engineering
```

Schema-version bumps require explicit release-engineering approval.

---

## Data Flow

### Normal CI run for a PR touching storage code

1. Trigger: PR modifies any storage-related path.
2. `parquet-format-check` workflow starts.
3. **Job: `parquet-spec-multi-reader`**:
   a. Set up Go, install `python3 pyarrow` and DuckDB CLI from pinned versions.
   b. Run `make build` to produce LH binaries.
   c. `TestForwardCompat` writes a canonical fixture file using LH writer; reads it with all three parsers; `CompareReaders` must return empty.
   d. `TestBackwardCompat` opens every golden file with today's reader; schema must match snapshot.
   e. Fail job on any reader disagreement or schema mismatch.
4. **Job: `parquet-otel-vl-compliance`**:
   a. Reuse canonical fixture file from job 1.
   b. For each column: match against `reserved-columns.yaml`; unclassified → fail with remediation instructions.
   c. Type compatibility check against the master's declared type.
   d. Required-fields check (mode-dependent).
   e. KV metadata check: `lh.parquet_schema_version`, `lh.writer_version`, `lh.schema_url` present.
5. **Job: `parquet-format-forward-advisory`** (advisory):
   a. Pull older LH docker image.
   b. Mount today's freshly-written file; read inside older container.
   c. Log result. Never fails the PR.
6. Artifact uploads: canonical files, `multi-reader-diff.json`, `advisory-forward-report.txt`.

### Adding a column (no schema-version bump)

1. Developer adds a Go struct field with `parquet:"new_column"` tag in `internal/schema/row.go`.
2. Developer adds entry to `reserved-columns.yaml` under correct master.
3. Developer regenerates fixture: `make parquet-format-fixture`.
4. PR includes both changes.
5. CI passes because:
   - Multi-reader test still shows agreement.
   - Validator finds the new entry.
   - Backward compat against old goldens still passes (additive).

### Bumping schema version

1. Developer increments `ParquetSchemaVersion` from `"v1"` to `"v2"`.
2. Developer marks deprecated columns with `deprecated_after: v1` in `reserved-columns.yaml`.
3. Developer runs `make parquet-format-golden-v2` to generate new goldens.
4. `TestBackwardCompat` loop auto-extends via `listGoldenVersions`.
5. CHANGELOG entry under `### Changed`.
6. CODEOWNERS approval from release engineering required.

### OTel version bump

1. Daily cron runs `upstream-check.yaml`.
2. Detects new OTel release; opens PR updating `otel-version.yaml`.
3. PR runs `parquet-otel-vl-compliance` against new version.
4. Failures (LH column type changed in OTel) surface as test failures. Operator chooses to update LH or hold OTel.

---

## Error Handling

### Reader disagreement
- Log `READER_DISAGREEMENT: <column> parquet-go=X pyarrow=Y duckdb=Z` to `multi-reader-diff.json`.
- Test fails (`t.Fatal`).
- Triage: usually fix LH to use more portable type code.

### Pinned reader version unavailable
- Job exits with sentinel code 3 (infra failure).
- Distinct from code 1 (real test failure).

### Unclassified column
- Fail with `unclassified column "<name>"; add to tests/parquet-format/reserved-columns.yaml under one of vl_internal | otel_semconv | lh_extension`.

### Cross-master conflict
- `LoadValidator` errors with `column "<name>" appears in both <X> and <Y>`.
- CI fails immediately.
- Migration: drop the `lh.*` version; add `deprecated_alias_of: <new_name>` for old goldens.

### Backward compat fails on old golden
- Fail with `BACKWARD_COMPAT_BROKEN: golden/<ver>/<file> no longer readable; revert OR document breaking change in CHANGELOG and bump major version`.
- No automatic mitigation.

### Forward-advisory fails
- Log `FORWARD_ADVISORY: v<prev> reader cannot read today's file`.
- Does not block PR.
- Step Summary surfaces it.

### Required field missing in fixture
- Validator fails. Fix the fixture, not the validator.

### OTel proto type mismatch
- Validator fails: `column <name>: writer <X>, OTel spec <Y>`.
- Fix on LH side; bump schema version; generate new golden.

### File metadata fields treated as columns
- `reserved-columns.yaml` entries with `file_metadata_only: true` skip the column-existence check; validator checks KV metadata key presence instead.

### OTel spec edition changes column type
- Bumping `otel-version.yaml` fails validator on existing column.
- Update LH writer to match new OTel type (then bump LH schema version) OR decline OTel bump.

### Empty Parquet file (zero rows)
- Multi-reader test reads zero rows successfully.
- Validator runs schema-only checks; required-fields skipped for empty files.

### Bloom filter metadata corruption
- Multi-reader test catches it via DuckDB schema-read failure.
- Log `BLOOM_METADATA_INVALID: <details>`; fail.

### Reserved-columns YAML drift (developer edits schema but not registry)
- `TestReservedRegistryCoversAllStructFields` walks structs reflectively and asserts every Parquet-tagged field has an entry.
- Fails: `field <name> in LogRow has no entry in reserved-columns.yaml`.

---

## Testing Strategy

### Unit tests (`tests/parquet-format/validator_test.go`)

- `TestValidatorLoadsRegistryCleanly` — happy path.
- `TestValidatorRejectsUnclassifiedColumn`
- `TestValidatorRejectsCrossMasterConflict`
- `TestValidatorEnforcesRequiredFields`
- `TestValidatorTypeMismatch`
- `TestLHExtensionNamespacePrefixEnforced`

### Multi-reader tests (`tests/parquet-format/readers_test.go`)

- `TestReadWithParquetGoReturnsSchema`
- `TestPyArrowShellInvocation` (mocked subprocess)
- `TestDuckDBShellInvocation` (mocked subprocess)
- `TestCompareReadersReportsDisagreements`

### Reflective coverage (`tests/parquet-format/coverage_test.go`)

```go
// negative control: add a field to LogRow with parquet tag but no
// reserved-columns.yaml entry → this test must fail.
func TestReservedRegistryCoversAllStructFields(t *testing.T) {
    fields := walkStructParquetTags(reflect.TypeOf(schema.LogRow{}))
    fields = append(fields, walkStructParquetTags(reflect.TypeOf(schema.TraceRow{}))...)
    reg := loadReservedColumns(t)
    for _, f := range fields {
        if _, ok := reg[f.Name]; !ok {
            t.Errorf("field %s has parquet tag but no reserved-columns.yaml entry", f.Name)
        }
    }
}
```

### Integration tests (`tests/parquet-format/integration_test.go`, build tag `parquet_format`)

Requires pyarrow + duckdb installed.

- `TestForwardCompatLogs`
- `TestForwardCompatTraces`
- `TestBackwardCompatV1Logs`
- `TestBackwardCompatV1Traces`
- (Future per-version variants auto-discovered.)
- `TestKVMetadataPresent`
- `TestSchemaURLPointsAtPinnedOTelVersion`

### Advisory tests (`tests/parquet-format/advisory_test.go`, build tag `parquet_format_advisory`)

- `TestForwardAdvisoryReadWithPreviousLHRelease`
- `TestForwardAdvisoryReadWithSpark` (optional, behind `spark_advisory` tag)

### Boundary tests

- `TestEmptyParquetFile`
- `TestSingleRowFile`
- `TestMaxColumnCount`
- `TestUnicodeColumnValues`
- `TestNanosTimestampPrecision`
- `TestNegativeTimestamps`

### Mechanism regression tests

- `TestOTelVersionBumpDetectsBreakingChange` — synthetic OTel rule change must trigger validator failure.
- `TestSchemaVersionBumpRequiresNewGolden` — bumping `ParquetSchemaVersion` to a value without a golden directory must fail.

### Negative-control proofs

Every load-bearing assertion has a comment naming the production code that must break it. Example:
```go
// negative control: remove the column-name regexp in validateLHExtensionPrefix() →
// this test must fail because "lh_foo" (underscore not dot) would pass when
// only "lh.foo" should.
func TestLHExtensionNamespacePrefixEnforced(t *testing.T) { ... }
```

### CI integration matrix

| Job | Trigger | Required for merge |
|-----|---------|--------------------|
| `parquet-spec-multi-reader` | every PR touching storage paths | yes |
| `parquet-otel-vl-compliance` | every PR touching storage paths | yes |
| `parquet-format-coverage` | every PR | yes |
| `parquet-format-forward-advisory` | nightly cron + push to main | no (advisory) |

Path triggers:
- `internal/schema/**`
- `internal/storage/parquets3/**`
- `lakehouse-traces/internal/storage/parquets3/**`
- `tests/parquet-format/**`
- `cmd/lakehouse-logs/main.go`, `lakehouse-traces/main.go`

---

## Migration Plan & Deliverables

Six PRs landed sequentially. Each independently mergeable; each leaves CI green.

| PR | Scope | New files (approx LoC) | Risk |
|----|-------|------------------------|------|
| **C1** | Reserved-columns registry + OTel lockfile | `tests/parquet-format/reserved-columns.yaml`, `otel-version.yaml`, `reader-versions.yaml` | Zero — data only |
| **C2** | Validator + reflective coverage test | `tests/parquet-format/validator.go` (~250), `validator_test.go` (~200), `coverage_test.go` (~80) | Low — Go-only, no infra |
| **C3** | Multi-reader harness | `tests/parquet-format/readers.go` (~250), `readers_test.go` (~150), pyarrow + DuckDB CLI installation in CI runner | Medium — first Python+DuckDB introduction |
| **C4** | Writer instrumentation + first golden | Writer KV metadata. Generate `golden/v1/{logs,traces}.parquet` + snapshots. Makefile targets. | Low — additive metadata |
| **C5** | Compat tests + CI workflow | `integration_test.go` (~400), `compat_test.go` (~200), `advisory_test.go` (~150), `parquet-format.yaml` workflow | Medium — new CI job |
| **C6** | Upstream-check extension + schema-bump scaffolding | Extend `upstream-check.yaml` for OTel polling. `make parquet-format-golden-v<N>` helper. CODEOWNERS rule. | Low — additive automation |

**Ordering rationale:**
- C1 ships data + intent before any code depends on it.
- C2 builds validator against registry; testable without infra.
- C3 introduces Python+DuckDB infra as focused PR.
- C4 instruments writer with KV metadata; unlocks compat tests.
- C5 ties everything together.
- C6 makes system self-maintaining.

---

## Definition of Done

### Coverage
- [ ] Every Parquet-tagged field in `internal/schema/row.go` has an entry in `reserved-columns.yaml`.
- [ ] Every registry entry corresponds to a real schema field or KV-metadata-only field.
- [ ] Verified by `TestReservedRegistryCoversAllStructFields`.

### Three-master compliance
- [ ] Apache Parquet spec compliance verified by three readers agreeing.
- [ ] OTel v1.30.0 compliance verified by validator passing.
- [ ] VL/VT internal-field compliance verified.
- [ ] LH extension namespace `lh.*` enforced.

### Versioning + compat
- [ ] `lh.parquet_schema_version` present in every LH file.
- [ ] `lh.writer_version` present.
- [ ] `lh.schema_url` matches `otel-version.yaml` URL.
- [ ] `golden/v1/` contains canonical files + snapshots.
- [ ] Backward compat test passes.
- [ ] Forward-advisory test runs without blocking.

### Infrastructure
- [ ] `.github/workflows/parquet-format.yaml` has 4 jobs.
- [ ] CI installs pinned pyarrow + DuckDB from `reader-versions.yaml`.
- [ ] Three jobs required for merge; advisory job nightly + on main push.
- [ ] `multi-reader-diff.json` uploaded as artifact every run.

### Automation
- [ ] `upstream-check.yaml` polls OTel releases.
- [ ] OTel bump PR auto-created on new release.
- [ ] CODEOWNERS on `tests/parquet-format/golden/` requires release-eng approval.

### Negative-control proofs
- [ ] Every load-bearing assertion has a negative-control comment.

---

## Out of Scope

Deferred to other subsystems:
- **Direct Parquet analytics with DuckDB/Trino/Spark queries** — Subsystem D.
- **Apache Arrow IPC compatibility** — narrowed Subsystem E.
- **License/SBOM audit of parquet-go and pyarrow** — narrowed Subsystem E.

Deferred indefinitely:
- **Java parquet-mr standalone reader** — Subsystem D's Trino tests provide transitive coverage.
- **Streaming Parquet (read-while-write)** — LH writes atomically; not a supported pattern.
- **Encrypted Parquet** — not currently a LH feature.

---

## Open Questions

None — all decisions resolved during brainstorming:
- Compatibility dimensions: all four (Parquet spec + OTel + VL/VT + version stability).
- Reader set: Go (parquet-go) + Python (pyarrow) + DuckDB CLI.
- OTel alignment: strict semconv compliance + reserved `lh.*` extension namespace; auto-bumped by upstream-check.
- Schema evolution: versioned + bidirectional compat (forward, backward, forward-advisory).
