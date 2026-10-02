# Ingest parity matrix

Lakehouse mounts the upstream VictoriaLogs (`vlinsert`) and VictoriaTraces (`vtinsert`) HTTP
handlers unchanged, so it should accept exactly the writes the pinned VictoriaLogs and
VictoriaTraces accept and store exactly what they store. This matrix proves that in CI, for
every write protocol, on both binaries.

The matrix cells are registry rows (`vl.ingest.<protocol>.<form>` and
`vt.ingest.<protocol>.<form>`, in `tests/conformance/registry/rows/{vl,vt}/ingest_matrix.yaml`).
The generated table of all cells, with the upstream route or flag each one exercises, is the
"Ingest protocols (parity matrix)" section of [`UPSTREAM_COVERAGE.md`](../UPSTREAM_COVERAGE.md).

## What each cell proves

`tests/e2e/ingest_matrix_test.go` runs on the e2e compose stack, which has hot VictoriaLogs and
VictoriaTraces next to `lakehouse-logs` and `lakehouse-traces`. For every protocol and tenant form it
sends the **same payload** to the hot binary and to Lakehouse and checks, in order:

| Step | Check |
|---|---|
| `ingest` | status code and body of the ingest answer are equal (ES `took` aside); for a payload upstream refuses, both refuse it the same way |
| `counters` | the upstream `vl_rows_ingested_total{type=...}` / `vt_rows_ingested_total{type=...}` series moves on hot and on Lakehouse, and so does `lakehouse_insert_rows_total` |
| `buffer` | straight after the write, before any flush, Lakehouse returns the same rows as hot, field for field (the `_stream`, `_stream_id` and `_time` included) |
| `parquet` | after the flush, the tenant's Parquet objects hold exactly those rows (read with a plain Parquet reader, as DuckDB or pyarrow would), and Lakehouse still equals hot, with no dip and no duplicate while the buffer hands the rows over |
| `storage_health` | `lakehouse_insert_rows_lost_total` and `lakehouse_insert_rejected_total` did not move |
| `other_signal_unaffected` | the other binary did not receive any of the rows |

The non-data ingest routes (readiness, health, and the Elasticsearch, Datadog and Splunk
compatibility stubs) are probed by `TestIngestMatrix_Probes`: same status and body on both sides.

## Protocols

| Binary | Protocols |
|---|---|
| `lakehouse-logs` vs VictoriaLogs v1.52 | `/insert/jsonline`, `/insert/native`, `/insert/multitenant/native`, Loki push (JSON and snappy protobuf), Elasticsearch `_bulk`, Splunk HEC events (four route spellings), Datadog v2 logs (two spellings), journald upload, OTLP/HTTP logs (protobuf stored; JSON refused upstream and by Lakehouse the same way), syslog over TCP and over UDP in RFC3164 and RFC5424 |
| `lakehouse-traces` vs VictoriaTraces v0.12 | OTLP/HTTP traces (protobuf and JSON), OTLP/gRPC traces |

Splunk HEC *raw* is not a protocol in the pinned VictoriaLogs (only `/event` and `/event/1.0`
are routed), so there is no raw cell. VictoriaTraces' own `/insert/native` and
`/insert/multitenant/native` and both `/internal/insert` routes (peer replication, not a client
protocol) are listed with a reason in `tests/ingestmatrix` (`Exclusions`); the drift gate
checks that list too.

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

With TLS on and no certificate the chart refuses to render, instead of letting the pod fail at startup.

**Docker Compose**: `deployment/docker/docker-compose-e2e.yml` enables the listeners on the hot
and on the Lakehouse containers (flags `-syslog.listenAddr.tcp/udp`, `-syslog.tenantID.*`,
`-otlpGRPCListenAddr`, `-otlpGRPC.tls=false`) and publishes them on localhost; the host ports are
`E2E_*` variables with defaults (see the file). Other compose files do not enable them. To enable
them in your own file add the same flags to the `command` list and publish the ports.

## Known gaps

Each gap is an explicit entry in `knownGaps` in the e2e test with a tracking issue. It applies to the
one named field and value, only after the flush, and the run **fails** when the gap is no longer
observed, so a fixed gap cannot stay on the list.

| Gap | Where | Issue |
|---|---|---|
| After the flush, Lakehouse returns `severity_number: "0"` on rows VictoriaLogs stores without it | logs, every protocol that does not carry a severity | [#274](https://github.com/ReliablyObserve/victoria-lakehouse/issues/274) |
| After the flush, an OTLP log row comes back with `level` in place of `severity_text` | logs, OTLP/HTTP protobuf | [#331](https://github.com/ReliablyObserve/victoria-lakehouse/issues/331) |
| Spans are stored with `_msg` = VictoriaLogs' default text instead of VictoriaTraces' `-` | traces, every protocol, buffer and Parquet | [#332](https://github.com/ReliablyObserve/victoria-lakehouse/issues/332) |
| Spans flushed to Parquet come back without `_msg` (VictoriaTraces returns `-`) | traces, every protocol, after the flush | [#333](https://github.com/ReliablyObserve/victoria-lakehouse/issues/333) |
| A `trace_id` query returns each flushed span twice while the buffer still holds it (the trace-ID fast path skips the buffer watermark) | traces, every protocol, after the flush | [#279](https://github.com/ReliablyObserve/victoria-lakehouse/issues/279) |

## Drift gate

`tests/conformance/ingest_matrix_test.go` extracts the ingest routes (everything registered under
`app/vlinsert` and `app/vtinsert`) and the listener flags (`*listenAddr*`) from the vendored upstream
trees and fails when:

* upstream has a route or listener flag that no case, probe or exclusion accounts for (a new
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

On the e2e compose stack (own project name and ports so it never touches another stack):

```bash
cd deployment/docker
export E2E_HOT_VL_HTTP_PORT=39303 E2E_HOT_VL_SYSLOG_TCP_PORT=39304 E2E_HOT_VL_SYSLOG_UDP_PORT=39305 \
       E2E_HOT_VT_GRPC_PORT=39302 E2E_LH_SYSLOG_TCP_PORT=39311 E2E_LH_SYSLOG_UDP_PORT=39312 E2E_LH_TRACES_GRPC_PORT=39314
docker compose -p lhingest -f docker-compose-e2e.yml -f my-ports-override.yml up -d --build   # override remaps 29428, 20428, 10428, 29000
cd ../.. && go test -tags=e2e -count=1 -timeout=40m ./tests/e2e/ -run 'TestIngestMatrix_'
docker compose -p lhingest -f deployment/docker/docker-compose-e2e.yml down -v --remove-orphans
```

The flush wait is one flush interval (120 s in the e2e stack) per signal.
