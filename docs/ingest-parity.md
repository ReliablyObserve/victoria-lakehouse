# Ingest parity matrix

Lakehouse mounts the upstream VictoriaLogs (`vlinsert`) and VictoriaTraces (`vtinsert`) HTTP
handlers unchanged. The matrix checks the protocol fixtures listed below against the pinned
hot binaries, on both signals. It covers representative payloads, rather than every payload
accepted by a protocol.

The matrix cells are registry rows (`vl.ingest.<protocol>.<form>` and
`vt.ingest.<protocol>.<form>`, in `tests/conformance/registry/rows/{vl,vt}/ingest_matrix.yaml`).
The generated table of all cells, with the upstream route or flag each one exercises, is the
"Ingest protocols (parity matrix)" section of the generated `UPSTREAM_COVERAGE.md` at the repository root.

## What each cell proves

`tests/e2e/ingest_matrix_test.go` runs on the e2e compose stack, which has hot VictoriaLogs and
VictoriaTraces next to `lakehouse-logs` and `lakehouse-traces`. For every protocol and tenant form it
sends the **same payload** to the hot binary and to Lakehouse and checks, in order:

| Step | Check |
|---|---|
| `ingest` | status code and body of the ingest answer are equal (ES `took` aside); for a payload upstream refuses, both refuse it the same way. Right after the cell's own write the ingest counters are checked: `vl_rows_ingested_total{type=...}` / `vt_rows_ingested_total{type=...}` moved on hot and on Lakehouse, and so did `lakehouse_insert_rows_total`. The counters are per-protocol series of the whole process, not per tenant, so this is a **lower bound** (moved by at least the rows the cell wrote). |
| `buffer` | each cell is queried before later protocol writes; Lakehouse equals hot field for field (`_stream_id` and `_time` included), apart from declared gaps, and fresh S3 scans before and after that observation must find zero marker rows. A flush already on S3 fails the preflush observation |
| `parquet` | a standard Parquet reader checks exact marker/span counts, every captured hot field and value, timestamps in nanoseconds, numeric tenant columns (also on trace-index rows), promoted scalars and spilled MAP values. Conflicting duplicate representations, extra fields and malformed MAPs fail. The exact documented #332 message value is the sole physical-value exception below. Lakehouse is compared with hot again |
| `stable` | established cells are sampled throughout later writes and flush polling, then repeatedly over a shared 10 s window. A missing, duplicate or changed result fails on its first sampled observation. Logs report sample counts and the largest observed gap between samples; changes between samples are not ruled out |
| `known_gaps` | every gap a cell declares was observed in that cell (see Known gaps) |
| `storage_health` | `lakehouse_insert_rows_lost_total` and `lakehouse_insert_rejected_total` did not move |
| `other_signal_unaffected` | for every tenant written, a word/`trace_id` query that finds the rows on their own binary (self-check) finds none on the other binary |

`cross_tenant_isolation` queries every marker through the other numeric tenant and configured
alias, and scans all fresh tenant prefixes for misplaced physical rows. A separate native
control sends six rows for two tenants in one multitenant request, requires actual preflush
and persisted raw parity, and checks negative reads. It uses nonhexadecimal trace IDs accepted
by the native parser; it does not send those IDs through OTLP.

Whether a Parquet-only gap may apply to a cell is decided on every read from S3 (the cell has rows in
Parquet), never from the step the test is in, so a flush that lands during `buffer` cannot flip a result.

The non-data ingest routes (readiness, health, and the Elasticsearch, Datadog and Splunk
compatibility stubs) are probed by `TestIngestMatrix_Probes`: same status and body on both sides.
`TestIngestMatrix_RouteGaps` sends the observed route gaps (below) and requires the documented statuses.

## Protocols

| Binary | Protocols |
|---|---|
| `lakehouse-logs` vs VictoriaLogs v1.52 | `/insert/jsonline`, `/insert/native`, `/insert/multitenant/native`, Loki push (JSON and snappy protobuf), Elasticsearch `_bulk`, Splunk HEC events (four route spellings), Datadog v2 logs (two spellings), journald upload, OTLP/HTTP logs (protobuf stored; JSON refused upstream and by Lakehouse the same way), syslog over TCP and over UDP in RFC3164 and RFC5424 |
| `lakehouse-traces` vs VictoriaTraces v0.12 | OTLP/HTTP traces (protobuf and JSON), OTLP/gRPC traces, `/insert/native`, `/insert/multitenant/native` (span rows) |

Splunk HEC *raw* is not a protocol in the pinned VictoriaLogs (only `/event` and `/event/1.0`
are routed), so there is no raw cell. `/internal/insert` is a case for `lakehouse-logs` (native-format
rows, the tenant inside each row); for `lakehouse-traces` it is an observed route gap (see Known gaps).
The one listener the matrix does not reach, `-syslog.listenAddr.unix`, is listed with its reason in
`tests/ingestmatrix` (`Exclusions`); the drift gate checks that list too.

Payloads are built with upstream's own libraries where it exports them (`logstorage.InsertRow` for
the native protocol, the OTLP proto types the upstream parsers decode) and with the documented wire
formats otherwise.

## Tenant forms

Every HTTP cell runs in both forms where upstream has both:

* **numeric**: `AccountID` / `ProjectID` request headers, as VictoriaLogs and VictoriaTraces take them.
* **alias**: `X-Scope-OrgID: acme-corp` (a configured alias, `-lakehouse.tenant.alias=acme-corp:1001:0`)
  sent to Lakehouse; hot VL/VT has no aliases, so it receives `AccountID: 1001, ProjectID: 0`. The rows read
  back from Lakehouse through the alias must equal the hot rows of tenant 1001:0.

Three protocols have a single form, each for an upstream reason: `/insert/multitenant/native`
carries the tenant inside each row (request headers are ignored upstream), syslog pins the tenant
per listener (`-syslog.tenantID.tcp/udp`), and OTLP/gRPC carries `AccountID` / `ProjectID` as call
metadata only.

## Opt-in listeners: syslog and OTLP/gRPC

Both are off by default everywhere; the HTTP port is the only listener until you enable them.

**Helm** (insert pods only; each enabled listener adds the flag, a container port, a Service port,
the same port on the headless Service and a NetworkPolicy ingress port):

```yaml
logs:
  insert:
    syslog:
      tcp: { enabled: true, port: 5140, tenantID: "7:1" }   # tenantID "AccountID:ProjectID", empty = 0:0
      udp: { enabled: true, port: 5141, tenantID: "7:1" }
traces:
  insert:
    otlpGrpc:
      enabled: true
      port: 4317
      tls:
        enabled: true                 # upstream default; needs certFile and keyFile
        certFile: /tls/tls.crt        # mount with insert.extraVolumes / extraVolumeMounts
        keyFile: /tls/tls.key         # tls.enabled: false gives a plaintext listener
```

A null syslog port resolves once for every rendered surface: TCP uses 5140 and UDP uses 5141.
The listen flag, container port, both Services and NetworkPolicy must agree.

With TLS on and no certificate the chart refuses to render, instead of letting the pod fail at startup.

**Docker Compose**: `deployment/docker/docker-compose-e2e.yml` enables the listeners on the hot
and on the Lakehouse containers (flags `-syslog.listenAddr.tcp/udp`, `-syslog.tenantID.*`,
`-otlpGRPCListenAddr`, `-otlpGRPC.tls=false`) and publishes them on localhost; the host ports are
`E2E_*` variables with defaults (see the file). Other compose files do not enable them. To enable
them in your own file add the same flags to the `command` list and publish the ports.

## Known gaps

A divergence from hot is never a skip. Each one is a gap with a tracking issue, declared **per case**
in `Gaps()` and `Case.Gaps` in `tests/ingestmatrix`, with its rewrite in `gapRewrites` in the e2e test:

* the cell still compares every other field exactly; a gap rewrites a Lakehouse row into the form hot
  returns, and only counts when the rewritten row then equals a hot row;
* the set-level gap #279 (a span returned twice) is accepted only when the result is **exactly two
  copies of every hot row**, so a writer that duplicates spans fails;
* the registry rows of an affected cell are `expect: differ` and cite the issues, and a test keeps
  `Case.Gaps` and the rows in step in both directions;
* a cell **fails when a declared gap is no longer observed**, so a fixed gap cannot stay on the list.

| Gap | Where | Issue |
|---|---|---|
| After the flush, Lakehouse returns `severity_number: "0"` on rows VictoriaLogs stores without it | logs, every protocol that does not carry a severity | [#274](https://github.com/ReliablyObserve/victoria-lakehouse/issues/274) |
| After the flush, an OTLP log row comes back with `level` in place of `severity_text` | logs, OTLP/HTTP protobuf | [#331](https://github.com/ReliablyObserve/victoria-lakehouse/issues/331) |
| Spans are stored with `_msg` = VictoriaLogs' default text instead of VictoriaTraces' `-` | traces: OTLP protobuf, OTLP/gRPC and native (OTLP/JSON spans are right), buffer and Parquet | [#332](https://github.com/ReliablyObserve/victoria-lakehouse/issues/332) |
| A `trace_id` query returns each flushed span twice while the buffer still holds it (the trace-ID fast path skips the buffer watermark) | traces, every protocol, after the flush | [#279](https://github.com/ReliablyObserve/victoria-lakehouse/issues/279) |

Fresh writes after native-message preservation no longer declare #333: physical `body` and
served `_msg` must be present and match hot, apart from the exact #332 value. The #332 raw
exception requires a separately recorded preflush observation: hot `_msg` must be `-` and
physical `body` must equal `missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field`.
Absent or arbitrary bodies fail; every other field remains checked.

Route gap, observed by `TestIngestMatrix_RouteGaps` (hot answers `200`, Lakehouse `404`; the test fails
if Lakehouse starts answering `200`): [#334](https://github.com/ReliablyObserve/victoria-lakehouse/issues/334),
`lakehouse-traces` does not mount `/internal/insert`. The existing `vt.internal.insert.count`
retains its pending count obligation; the distinct `vt.ingest.internal_insert.numeric` row
measures the status divergence. A route status check does not satisfy the data-count obligation.

## Drift gate

`tests/conformance/ingest_matrix_test.go` extracts the ingest routes (everything registered under
`app/vlinsert` and `app/vtinsert`) and the listener flags (`*listenAddr*`) from the vendored upstream
trees and fails when:

* upstream has a route or listener flag that no case, probe, route gap or exclusion accounts for (a new
  protocol), or
* the matrix sends to a route or flag upstream no longer has (a removed protocol), or
* a case has no registry row, a row is pending or does not cite the e2e test, or a registry
  ingest row has no case.

`TestIngestMatrixDrift_DetectsAddedAndRemovedRoutes` proves the gate fails in both directions on a
mutated inventory.

## Adding a protocol

1. Add the case to `tests/ingestmatrix` (`logs.go` or `traces.go`): routes or flags, payload builder,
   counter series, tenant forms.
2. Add its rows to `tests/conformance/registry/rows/{vl,vt}/ingest_matrix.yaml` (copy a neighbouring
   row; `ingest_matrix_test.go` names the missing ids).
3. `make conformance-gen conformance-check`.

## Running it locally

The test needs the e2e stack (hot VL/VT, `lakehouse-logs`, `lakehouse-traces`, S3) and the seeded data the
e2e package's `TestMain` waits for (the `datagen-seed` service, which also leaves a manifest with files in
both binaries; allow about two minutes for the first flush). Use your own compose project name and ports so
it never touches another stack. The fixed ports of the e2e file (S3 29000, the two Lakehouse HTTP ports,
hot VT 10428) are remapped with an override file:

```yaml
# my-ports.yml
services:
  s3:               { ports: !override ["127.0.0.1:39300:9000"] }
  victoriatraces:   { ports: !override ["127.0.0.1:39301:10428", "127.0.0.1:39302:4317"] }
  victorialogs:     { ports: !override ["127.0.0.1:39303:9428", "127.0.0.1:39304:5140", "127.0.0.1:39305:5141/udp"] }
  lakehouse-logs:   { ports: !override ["127.0.0.1:39310:9428", "127.0.0.1:39311:5140", "127.0.0.1:39312:5141/udp"] }
  lakehouse-traces: { ports: !override ["127.0.0.1:39313:10428", "127.0.0.1:39314:4317"] }
  datagen-seed:
    command: !override ["--logs=2000", "--traces=500", "--hours-back=3", "--vl-endpoint=http://victorialogs:9428", "--vt-endpoint=http://victoriatraces:10428", "--lh-logs-endpoint=http://lakehouse-logs:9428", "--lh-traces-endpoint=http://lakehouse-traces:10428"]
```

```bash
cd deployment/docker
docker compose -p lhingest -f docker-compose-e2e.yml -f my-ports.yml up -d --build \
  s3 s3-init s3-latency victorialogs victoriatraces lakehouse-logs lakehouse-traces datagen-seed
cd ../..
GOWORK=off \
LOGS_BASE_URL=http://127.0.0.1:39310 TRACES_BASE_URL=http://127.0.0.1:39313 \
HOT_VL_URL=http://127.0.0.1:39303 HOT_VT_URL=http://127.0.0.1:39301 \
LH_SYSLOG_TCP_ADDR=127.0.0.1:39311 LH_SYSLOG_UDP_ADDR=127.0.0.1:39312 \
HOT_VL_SYSLOG_TCP_ADDR=127.0.0.1:39304 HOT_VL_SYSLOG_UDP_ADDR=127.0.0.1:39305 \
LH_TRACES_GRPC_ADDR=127.0.0.1:39314 HOT_VT_GRPC_ADDR=127.0.0.1:39302 \
S3_URL=http://127.0.0.1:39300 \
  go test -tags=e2e -count=1 -timeout=40m ./tests/e2e/ -run 'TestIngestMatrix_'
docker compose -p lhingest -f deployment/docker/docker-compose-e2e.yml down -v --remove-orphans
```

Set the `E2E_*` host-port variables of the compose file (see `docs/docker-compose-setup.md`) to the same
ports when the compose file, not the override, publishes them. The package needs the vendored
VictoriaLogs tree (`make deps-logs`), because the native payloads are built with `logstorage.InsertRow`.
The flush wait is one flush interval (120 s in the e2e stack); the Logs and Traces matrices run in parallel.
