# Subsystem B — API Parity Test Coverage Extension

**Status:** Design

**Date:** 2026-06-02

**Continues:** Existing `tests/parity/` suite (21 files, ~3300 LoC, select-heavy).

**Successor work:** Subsystems C (Parquet format), D (direct Parquet analytics), E (community standards, narrowed scope).

---

## Goal

Extend the existing dual-write parity test suite into comprehensive coverage across three dimensions:

1. **Surface** — every API endpoint LH exposes (insert + select + cross-cutting + internal).
2. **Validation** — dual-mode comparison: byte-equal (logged for drift tracking) + semantic compare (CI-blocking).
3. **Conformance** — every wire protocol gets triple-checked (LH + VL/VT + third-party reference implementation) against spec-derived golden payloads, with explicit capability matrix for cases where one party lacks support.

The biggest gap to close: **insert API parity is currently untested**. Existing suite covers the select side strongly but leaves all `/insert/*`, `/loki/*`, `/api/v2/logs`, `/services/collector/`, `/api/v1/validate`, and trace insert endpoints completely uncovered.

---

## Architecture

Tests live in three new sub-trees under `tests/parity/`:

```
tests/parity/
├── (21 existing select-side files stay where they are)
├── insert/                # NEW — insert API parity
│   ├── helpers.go
│   ├── logs_jsonline_test.go
│   ├── logs_loki_test.go
│   ├── logs_elasticsearch_bulk_test.go
│   ├── logs_otlp_test.go
│   ├── logs_splunk_hec_test.go
│   ├── traces_jaeger_thrift_test.go
│   ├── traces_zipkin_test.go
│   ├── traces_otlp_test.go
│   └── schema_drift_test.go
├── select-extended/       # NEW — fill weak spots in select coverage
│   ├── logs_jaeger_compat_test.go
│   ├── traces_tempo_http_test.go
│   ├── traces_jaeger_extended_test.go
│   ├── internal_select_test.go
│   └── tail_streaming_test.go
├── protocol-conformance/  # NEW — spec-derived goldens + reference-impl triple-check
│   ├── conformance.go
│   ├── capabilities.yaml
│   ├── docker-compose.references.yml
│   ├── KNOWN_DIVERGENCES.md
│   ├── KNOWN_VL_BUGS.md
│   ├── spec-versions.json
│   ├── loki/{golden/*.json, snapshots/*.snap.json, loki_conformance_test.go}
│   ├── elasticsearch/...
│   ├── otlp/{golden-http/*, golden-grpc/*, snapshots/*, otlp_conformance_test.go}
│   ├── jaeger-thrift/...
│   ├── zipkin/...
│   ├── tempo-http/...
│   └── splunk-hec/...
├── comparator.go          # NEW — dual-mode (byte + semantic) shared by all families
├── byte-drift-report.go   # NEW — drift tracking
├── coverage_test.go       # NEW — mechanical "every endpoint covered" proof
└── exempt-endpoints.yaml  # NEW — operational endpoints exempt from parity
```

The existing `tests/parity/docker-compose.yml` gains a `references` profile that brings up reference implementations only when conformance tests run.

---

## Components

### Insert harness (`tests/parity/insert/helpers.go`)

```go
type InsertCase struct {
    Name           string
    Endpoint       string                  // "/insert/jsonline", "/loki/api/v1/push", ...
    Method         string                  // typically "POST"
    Headers        map[string]string
    Body           []byte                  // canonical payload
    ExpectStatus   int                     // expected HTTP status from BOTH LH and VL
    ExpectErrorRE  string                  // regex on response body if 4xx/5xx
    ReadBackQuery  string                  // LogsQL to verify ingest after flush
    ReadBackMode   CompareMode             // SetEqual / CountEqual / RowsMatch
    ExpectRows     int                     // for CountEqual mode
    WaitFlushS     time.Duration           // override default flush wait (5s)
}

func RunInsertParity(t *testing.T, c InsertCase) {
    statusLH := postLH(c)
    statusVL := postVL(c)
    require.Equal(t, c.ExpectStatus, statusLH, "LH status")
    require.Equal(t, c.ExpectStatus, statusVL, "VL status")
    if c.ExpectStatus >= 300 {
        return // error case, no read-back
    }
    waitFlush(c.WaitFlushS)
    RunInsertWriteSideCompare(t, c)
    lhRows := query(lhURL, c.ReadBackQuery)
    vlRows := query(vlURL, c.ReadBackQuery)
    result := Compare(lhRows, vlRows, c.ReadBackMode)
    if !result.SemanticPass { t.Fatal(result.Diff) }
    if !result.ByteEqual    { recordByteDrift(c.Name, result.Diff) }
}

func RunInsertWriteSideCompare(t *testing.T, c InsertCase) {
    // Scrapes /metrics from both systems, asserts insert counters match.
}
```

### Select-extended harness

Reuses existing `tests/parity/parity_test.go::RunParity`. No new harness — just new `ParityCase` entries in `select-extended/*_test.go` for thinly-covered endpoints.

### Dual-mode comparator (`tests/parity/comparator.go`)

```go
type CompareMode int

const (
    ModeByteEqual CompareMode = iota
    ModeSetEqual
    ModeCountEqual
    ModeRowsMatch
    ModeSuperset
    ModeSetEqualWithRetry
)

type CompareResult struct {
    SemanticPass bool
    ByteEqual    bool       // false → log to t.Log as warning, do NOT t.Fail
    Diff         string
}

func Compare(lh, vl []byte, mode CompareMode) CompareResult { ... }
```

**Failure rule:**
- `SemanticPass == false` → `t.Fatal` (CI-blocking).
- `ByteEqual == false && SemanticPass == true` → `t.Logf("BYTE_DRIFT: %s", diff)` + record in `byte-drift-report.json`.

### Byte-drift report (`tests/parity/byte-drift-report.go`)

Per-run JSON artifact uploaded by CI:

```json
{
  "run_id": "...",
  "commit": "abc1234",
  "drifts": [
    {"test": "TestLokiInsertBatch100", "case": "valid-batch.json", "diff_size": 42, "diff_preview": "..."}
  ]
}
```

A trend dashboard (deferred to operations) can chart drift count over time; sudden growth signals serialization regressions.

### Protocol-conformance harness (`tests/parity/protocol-conformance/conformance.go`)

```go
type ConformanceCase struct {
    Protocol      string         // "loki" / "otlp-http" / "otlp-grpc" / "jaeger-thrift" / ...
    GoldenFile    string         // path under golden/ subdir
    ExpectAccept  bool           // valid payload (must 2xx) or invalid (must 4xx)
    ExpectStatus  int
    ReferenceImpl string         // service name in docker-compose.references.yml
    Capabilities  Capabilities   // which legs participate
}

type Capabilities struct {
    LHSupported   bool          // default true; set false if LH lacks this protocol
    VLSupported   bool          // default true; set false if VL/VT lacks this
    RefSupported  bool          // default true; set false if ref impl lacks this
    SkipReason    string        // required when any of the above is false
    UpstreamIssue string        // optional link tracking expected resolution
}

func RunConformance(t *testing.T, c ConformanceCase) {
    payload := loadGolden(c.GoldenFile)
    legs := participatingLegs(c.Capabilities)
    statuses := map[string]int{}
    for _, leg := range legs {
        statuses[leg] = postTo(legURL(leg), payload)
    }
    requireAllParticipating(t, c.ExpectStatus, statuses, c.Capabilities)
    memoToSnapshot(c, statuses)
}
```

### Capabilities registry (`tests/parity/protocol-conformance/capabilities.yaml`)

Single source of truth for which leg supports what protocol/transport:

```yaml
otlp:
  http:
    lh: true
    vl: true
    ref: true
  grpc:
    lh: true
    vl: false
    ref: true
    skip_reason: "VictoriaLogs supports OTLP via HTTP only (as of v1.50)"
    upstream_issue: "https://github.com/VictoriaMetrics/VictoriaLogs/issues/<n>"

loki:
  push_v1:
    lh: true
    vl: true
    ref: true

elasticsearch:
  bulk_v8:
    lh: true
    vl: true
    ref: true

splunk_hec:
  v1:
    lh: true
    vl: true
    ref: true   # otel-collector HEC receiver

jaeger_thrift:
  binary:
    lh: true
    vl: false   # VL doesn't ingest traces (VT does)
    ref: true
    skip_reason: "Traces ingest only — VL leg N/A for this protocol"

zipkin:
  v2:
    lh: true
    vl: false
    ref: true
    skip_reason: "Traces ingest only — VL leg N/A"

tempo_http:
  search:
    lh: true
    vl: false
    ref: true
    skip_reason: "Traces query — VL N/A"
```

### Reference-impl stack (`docker-compose.references.yml`)

Brought up only with `--profile references`. Services:

| Service | Image | Protocols it validates |
|---------|-------|------------------------|
| promtail | `grafana/promtail:latest` | Loki push v1 |
| otel-collector | `otel/opentelemetry-collector-contrib:latest` | OTLP (HTTP+gRPC), Splunk HEC, Zipkin, Jaeger |
| tempo | `grafana/tempo:latest` | Tempo HTTP search/query |
| jaeger-collector | `jaegertracing/jaeger-collector:latest` | Jaeger Thrift binary |
| elasticsearch | `docker.elastic.co/elasticsearch/elasticsearch:8.x` | ES Bulk v8 |

Each writes to tmpfs so storage doesn't accumulate between runs.

### CI workflow extension (`.github/workflows/parity.yaml`)

| Job | Trigger | Required for merge |
|-----|---------|--------------------|
| `parity` (select, existing) | every PR | yes |
| `parity-insert` (new) | every PR | yes |
| `parity-conformance` (new) | push to main, nightly cron, PR with `conformance` label | no (advisory) |

`parity-conformance` brings up `--profile references`, runs `go test -tags=conformance ./tests/parity/protocol-conformance/...`, uploads `byte-drift-report.json` + per-protocol snapshot diffs as artifacts.

### Coverage gate (`tests/parity/coverage_test.go`)

```go
// negative control: register a new mux.HandleFunc in cmd/lakehouse-logs/main.go
// without adding a parity test → this test must fail with `endpoint not covered`.
func TestEveryExposedEndpointHasParityTest(t *testing.T) {
    endpoints := extractEndpointsFromSource(t)
    covered := extractEndpointsFromTests(t)
    for ep := range endpoints {
        if isExempt(ep) { continue }
        if _, ok := covered[ep]; !ok {
            t.Errorf("endpoint not covered by parity test: %s", ep)
        }
    }
}
```

`isExempt` reads `tests/parity/exempt-endpoints.yaml`, which covers operational endpoints (health, metrics, debug pprof, LH-specific stats).

---

## Data Flow

### Insert parity test

1. Test invokes `RunInsertParity(t, c)`.
2. Helper sends `POST /loki/api/v1/push` to both LH and VL with identical bytes.
3. Both must return the same status code. Mismatch → `t.Fatal`.
4. If 2xx, helper waits up to `c.WaitFlushS` (default 5s, exponential backoff) for LH flush to S3.
5. `RunInsertWriteSideCompare` scrapes `/metrics` on both, asserts insert counters match.
6. Helper runs `c.ReadBackQuery` on `/select/logsql/query` of both, applies dual-mode comparator.
7. Semantic failures → `t.Fatal`. Byte drift → logged to `byte-drift-report.json`.

### Protocol-conformance test

1. Test loops over every file in `protocol-conformance/<protocol>/golden/`.
2. Filename prefix derives `ExpectAccept` (`valid-*` → 2xx, `invalid-*` → 4xx).
3. `RunConformance` consults `capabilities.yaml` to determine which legs (LH/VL/ref) participate.
4. POSTs payload to each participating leg.
5. Three-way compare per the capability matrix:

| LH | VL | Ref | Action |
|----|----|----|--------|
| ✓ | ✓ | ✓ | Triple-check; all three must match `ExpectStatus` |
| ✓ | ✓ | ✗ | LH-vs-VL only; log `REFERENCE_GAP` |
| ✓ | ✗ | ✓ | LH-vs-Ref only; log `VL_GAP` with UpstreamIssue |
| ✗ | ✓ | ✓ | VL-vs-Ref only; log `LH_GAP` — **real LH bug to triage** |
| ✓ | ✗ | ✗ | LH-only — match against `ExpectStatus` directly; log `PROTOCOL_LH_ONLY` |
| ✗ | ✓ | ✗ | Skipped — golden irrelevant to LH conformance |
| ✗ | ✗ | ✓ | `BOTH_GAP` logged; test passes (no contradiction) |

6. Snapshot file `<protocol>/snapshots/<golden>.snap.json` memos statuses + body hashes. Future runs diff; mismatch fails unless `-update-snapshots` flag passed.
7. Read-back follow-up (accept cases only): query LH and VL, dual-mode compare results.

### CI flow for parity-conformance

1. Checkout repo.
2. `make build` produces LH images.
3. `docker compose -f tests/parity/docker-compose.yml --profile references up -d`.
4. Wait for all services healthy (existing pattern from `parity.yaml`).
5. Datagen seeds VL and LH with identical streams for read-back queries.
6. `go test -tags=conformance ./tests/parity/protocol-conformance/...`.
7. Collect `byte-drift-report.json` + snapshot diffs as artifacts.
8. Tear down stack.

### Local iteration

```bash
SKIP_REFERENCES=1 go test -tags=conformance ./tests/parity/protocol-conformance/loki/...
```

Skips third-party comparison but keeps LH-vs-VL. CI never sets this flag.

---

## Error Handling

### Reference-impl divergence (LH+VL agree, ref disagrees)

A real divergence signal. Behavior:
- Log `REFERENCE_DIVERGENCE: <protocol>/<golden>: LH=<s>, VL=<s>, <ref>=<s>` to `byte-drift-report.json`.
- Test **fails** (`t.Fatal`).
- Operator's recourse: review the case, either fix LH (and possibly file VL bug), or add an entry to `KNOWN_DIVERGENCES.md` with rationale; tests then `t.Skip("known divergence: <reason>")` for that golden.

### LH+ref agree, VL disagrees

LH is more spec-compliant than VL. Same logging, same `t.Fatal`. Resolution: `KNOWN_VL_BUGS.md` lists cases where VL is known to deviate from spec with required upstream issue link before any skip.

### Insert flush timing flake

Read-back may flake if flush is slow. Mitigation:
- Helper waits up to `WAIT_FLUSH_S` (default 5s) with exponential backoff polling.
- Uses `?force_flush=1` hint where endpoint supports it.
- `ModeSetEqualWithRetry` retries entire read-back up to 3 times before failing.

### Schema inference drift

LH and VL may infer different schemas for same payload (int vs float). `ModeSetEqual` catches row-level data agreement. Type drift goes to `tests/parity/insert/schema_drift_test.go` which diffs `GET /select/logsql/field_names` after each insert.

### Streaming endpoints (tail)

Tests verify *interface parity* only:
- Both LH and VL accept the tail connection (or both reject).
- If both accept, first chunk arrives within X seconds.
- Does NOT compare full streams (timing-dependent).

### Reference-impl downtime

Image unavailable / pull failure:
- Log `REFERENCE_UNAVAILABLE: <impl>: <error>`.
- CI exits with sentinel code 3 (infra failure) — distinguishes from code 1 (real test failure).
- Operator sees a separate alarm class.

### Golden file lifecycle

- New golden file: first run requires `-update-snapshots`; subsequent runs diff.
- Spec edition bump: regenerate goldens, update `spec-versions.json`. PR titled `protocol: bump <protocol> goldens to spec edition <X>`.
- Lint check fails if golden references newer spec edition than recorded.

### Cross-protocol bleed-through

Same trace data can be sent via OTLP, Jaeger Thrift, Zipkin. Test:
```go
RunInsertCrossProtocolEquivalence(t, traceID, []protocol{"otlp", "jaeger-thrift", "zipkin"})
```
Sends identical logical trace through each protocol; verifies all three resolve to the same trace_id with the same span set.

### OTLP gRPC vs HTTP

Two transports per protocol. Both parity-tested. Subdirs `golden-http/` and `golden-grpc/` under `protocol-conformance/otlp/`. Capability matrix declares LH/VL support per transport (currently VL: HTTP only). Test loops over both with appropriate client.

---

## Testing Strategy

### Coverage matrix (mechanical proof)

`tests/parity/coverage_test.go::TestEveryExposedEndpointHasParityTest` walks all `mux.HandleFunc` calls in `cmd/`, `internal/selectapi/`, and `lakehouse-traces/`. Cross-references against all `ParityCase` / `InsertCase` / `ConformanceCase` entries. Unmatched endpoints (non-exempt) fail the test.

### Per-test-family requirements

**Insert family — minimum per protocol:**
- `TestEmptyPayload` — agreed status from both.
- `TestSingleValidRow` — minimal valid payload; 2xx + 1 row queryable.
- `TestBatch100Rows` — typical batch; counts match.
- `TestMalformedJSON` — corrupted bytes; both reject with same status class.
- `TestOversizedPayload` — exceeds size limit; both 413.
- `TestUnicodeAndEscaping` — non-ASCII fields, embedded quotes; round-trip verified.
- `TestTimestampPrecision` — millis/micros/nanos in same batch; storage retention verified.

**Select-extended family:**
- Cases mirror existing patterns from `logs_filters_test.go` etc., applied to Tempo/Jaeger/internal endpoints.

**Conformance family — minimum per protocol:**
- ≥5 `valid-*` cases (canonical happy paths from spec).
- ≥3 `invalid-*` cases (documented rejection cases).
- ≥2 `edge-*` cases (boundary conditions).
Total: ≥10 goldens per protocol.

### Snapshot regression

Each conformance test memos response status + body hash to `<protocol>/snapshots/<golden>.snap.json`. Future runs diff; mismatch fails unless explicit `-update-snapshots` flag passed. Catches silent protocol drift across releases.

### Performance gates

Insert parity tests must not become benchmarks. Per-test bounds:
- Payload ≤ 1MB.
- Row count ≤ 10000.
- Timeout: 30s.
- Total `tests/parity/insert/` suite: < 5 minutes on CI runners.

`BenchmarkParityHarnessOverhead` in `helpers_test.go` measures harness overhead; must stay < 100µs per case beyond actual HTTP cost.

### Negative-control proofs

Every load-bearing assertion has a comment naming what must break it:

```go
// negative control: comment out byte-drift recording in Compare() → byte
// drift will silently pass; this test verifies drift report file contains
// entries when LH and VL serialize differently.
func TestByteDriftRecordedToReportFile(t *testing.T) { ... }
```

### Unit tests

- `comparator_test.go` — comparator correctness for each `CompareMode`.
- `capabilities_test.go` — capability matrix parsing; missing-skip-reason rejection.
- `coverage_test.go` — `TestEveryExposedEndpointHasParityTest` self-test (synthetic mux registration must trigger the failure).
- `byte-drift-report_test.go` — drift report file format stability.

---

## Migration Plan & Deliverables

Seven PRs sequentially. Each independently mergeable; each leaves CI green.

| PR | Scope | New files (approx LoC) | Risk |
|----|-------|------------------------|------|
| **B1** | Insert harness + first protocol (jsonline) | `tests/parity/insert/helpers.go` (~200), `comparator.go` (~150), `byte-drift-report.go` (~80), `logs_jsonline_test.go` (~250) | Low — additive |
| **B2** | Logs insert coverage | `logs_loki_test.go` (~250), `logs_elasticsearch_bulk_test.go` (~280), `logs_otlp_test.go` (~300), `logs_splunk_hec_test.go` (~220) | Medium — touches docker-compose for VL endpoints |
| **B3** | Traces insert coverage | `traces_jaeger_thrift_test.go` (~280), `traces_zipkin_test.go` (~220), `traces_otlp_test.go` (~320) | Low — additive |
| **B4** | Select-extended (Tempo/Jaeger/internal/tail) | `logs_jaeger_compat_test.go`, `traces_tempo_http_test.go`, `traces_jaeger_extended_test.go`, `internal_select_test.go`, `tail_streaming_test.go` (~1000 total) | Low — additive |
| **B5** | Protocol-conformance infrastructure | `protocol-conformance/conformance.go` (~250), `capabilities.yaml`, `docker-compose.references.yml`, `KNOWN_DIVERGENCES.md`, `KNOWN_VL_BUGS.md`, `spec-versions.json`. CI workflow extension. | Medium — new CI infra (reference impls) |
| **B6** | Protocol goldens — first wave | `protocol-conformance/{loki,elasticsearch,otlp}/golden/*.json` + per-protocol `*_conformance_test.go` (~1500 total). Initial snapshots. | Low — additive |
| **B7** | Protocol goldens — second wave + coverage gate | `protocol-conformance/{jaeger-thrift,zipkin,tempo-http,splunk-hec}/golden/*` (~1500). `tests/parity/coverage_test.go`. `exempt-endpoints.yaml`. | Medium — coverage gate may surface existing-but-uncovered endpoints |

**Ordering rationale:**
- B1 builds infrastructure with one simple protocol to validate the pattern before scaling.
- B2–B3 fan out insert coverage using established pattern.
- B4 fills select gaps without touching new infra.
- B5 adds reference-impl stack as its own PR — CI changes reviewed in isolation.
- B6–B7 fan out conformance goldens once infra is proven.

---

## Definition of Done

### Coverage
- [ ] Every endpoint LH exposes is covered by at least one parity test (verified by `TestEveryExposedEndpointHasParityTest`).
- [ ] All 8 insert protocols have insert parity tests (logs: jsonline, loki, ES bulk, OTLP, Splunk HEC; traces: Jaeger Thrift, Zipkin, OTLP).
- [ ] Read-back + write-side counter compare both implemented for every insert test.
- [ ] Protocol conformance: every protocol has ≥10 goldens (5 valid + 3 invalid + 2 edge).

### Infrastructure
- [ ] Dual-mode comparator (byte-equal logged + semantic enforced) active for every test.
- [ ] Reference-impl stack starts cleanly under `--profile references` in CI.
- [ ] `capabilities.yaml` declares LH/VL/Ref support for every protocol with skip reasons.
- [ ] `spec-versions.json` records spec edition tested against per protocol.
- [ ] Snapshot files committed for every conformance golden.

### Governance
- [ ] `KNOWN_DIVERGENCES.md` lists every exempted case with rationale.
- [ ] `KNOWN_VL_BUGS.md` lists VL deviations with upstream issue links.
- [ ] Negative-control comment on every load-bearing assertion.

### CI
- [ ] `parity-insert` job blocks merges on PRs.
- [ ] `parity-conformance` job runs nightly + on labeled PRs (advisory only).
- [ ] Byte-drift report uploaded as CI artifact every run.
- [ ] Distinct exit codes for test failure (1) vs infrastructure failure (3).

---

## Out of Scope

Deferred to other subsystems:
- **Parquet format compatibility** — Subsystem C.
- **Direct Parquet analytics with DuckDB/Trino/Spark** — Subsystem D.
- **OpenTelemetry semantic conventions, Apache Arrow alignment, license/SBOM audit** — narrowed Subsystem E.

Deferred indefinitely:
- **Performance regression detection in parity suite** — separate `BenchmarkParityHarnessOverhead` guards harness cost only. Full perf regression belongs to the existing `benchmarks-logs` / `benchmarks-traces` CI jobs.
- **Streaming endpoints (tail) deep comparison** — current interface-only test is sufficient; full stream parity would require deterministic clock injection across both systems.
- **End-to-end TLS variants** — assumes both systems use the same TLS config from docker-compose. Customer-specific TLS profiles out of scope.

---

## Open Questions

None — all decisions resolved during brainstorming:
- Validation strategy: dual-mode (byte-equal + semantic).
- Insert verification: write-side + read-back.
- Conformance ground truth: spec goldens + reference impl + VL/VT triple-check.
- Capability handling: per-protocol/per-transport `capabilities.yaml` with explicit skip reasons.
- Scope: comprehensive coverage + protocol conformance (overlapping with what was originally Subsystem E).
