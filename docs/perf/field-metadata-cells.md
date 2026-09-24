# Field-metadata performance cells (field_values, field_names, streams)

Validated measurement of the cold field-metadata endpoints on both binaries, and the registry
rows that hold them. Current state: **#239** against **v0.143.1** (`7bbbdd80`), measured
2026-09-24 on a developer host (Apple M-series, 18 cores, darwin/arm64; load average 12–36 from
other work during the runs — see [Noise](#noise)).

- **v0.143.1** (#237): exact value sets, but pmeta-on answers came from the catalog with `hits=1`
  and hour granularity; scans were serial except traces `field_values`; unflushed rows were not
  enumerated at all.
- **#239**: every object wholly inside the window answers from its label aggregate (exact per-value
  row counts, no S3 read); the rest are scanned window-confined on the file-worker pool with
  row-group time pruning; unflushed rows (local buffer, every peer through the buffer bridge) are
  merged like a query merges them; responses use VictoriaLogs' `MergeValuesWithHits`.

**Rule applied throughout: a fast wrong answer is never a win.** Every iteration's answer is
compared with the dataset's known truth; only exact iterations are timed.

## Harness

| piece | where |
|---|---|
| logs matrix, go-bench view, hot VictoriaLogs reference | `internal/storage/parquets3/field_values_bench_test.go` — `TestFieldMetadataMatrix`, `BenchmarkFieldMetadata`, `TestFieldMetadataMatrixVL` |
| traces matrix | `lakehouse-traces/internal/storage/parquets3/field_values_bench_test.go` — `TestFieldMetadataMatrixTraces` |
| exactness in every layout (runs in every `go test`) | `TestFieldMetadata_ExactInBothLayouts`, `TestFieldMetadataTraces_ExactInBothLayouts` |
| interleaved A/B driver | `scripts/bench/field_metadata/run.sh` |
| aggregation | `scripts/bench/field_metadata/aggregate.py <matrix.jsonl>` |
| registry rows + CI gate | `scripts/bench/field_metadata/perf_rows.py gen` / `check`, CI job `field-metadata-perf`, rows in `tests/conformance/registry/rows/perf/field_metadata.yaml` |

**S3 mock**: in-process; every read sleeps a first-byte latency (0 or 100 ms), and GETs, bytes,
row groups and pages are counted per request. **Peer mock**: an insert instance answering
`/internal/buffer/query` with its unflushed rows inside the requested window, read through the
real `BufferBridge`.

**Dataset**: two partition hours, 12 flushes × 2000 rows per hour (logs and traces generators;
~72–93 KB objects, the production median is 71 KiB). Hour 10:00 is quiet (8 services, `TRACE`
level / `warmup` span only in 10:00–10:10); hour 11:00 is busy (150 distinct services per object,
700 in the hour; `FATAL` / `shutdown` only in 11:50–12:00).

**Cells** (per signal): endpoint (logs: `field_values level`, `field_values service.name`,
`streams`, `field_names`; traces: `field_values name`, `field_values resource_attr:service.name`,
`streams`, `field_names`) × pmeta {on, off} × layout × window × filter {none,
`service.name:="svc-a"`} × S3 latency {0, 100 ms} = 384 cells per signal.

| layout | what it is |
|---|---|
| `flushed` | 24 flushed objects |
| `compacted` | the real `compaction.Compactor` per hour, then `PmetaOnCompacted` as the scheduler hook runs it → 2 hour objects |
| `peer` | flushed, except the last ten minutes (11:50–12:00) still unflushed on a peer insert instance |

| window | bounds | exercises |
|---|---|---|
| `whole` | 10:00–11:59:59.999999999 | every object inside the window |
| `cut` | 10:30:00.075–11:30:00.075 | objects straddling both ends |
| `narrow` | 11:20:10.075–11:20:40.075 | a 30-second call on one flushed object / one compacted hour object |
| `edge` | 10:44:59.925–11:07:30.075 | across the hour boundary: flushed 10:45–11:05 wholly inside, 11:05 cut; both hour objects cut |

**Protocol**: 0 ms — 2 rounds alternating builds, 2 measured iterations + 1 warm-up per cell per
round (n = 4); 100 ms — 1 round, 1 iteration (latency-bound: the result is the number of
round-trip waves). Object caches are cold every iteration (memCache and footer cache replaced).

Reproduce:

```sh
make deps-logs deps-traces deps-vt
BEFORE_REF=7bbbdd80 LATENCIES_MS=0 ROUNDS=2 scripts/bench/field_metadata/run.sh /tmp/fm-0ms
BEFORE_REF=7bbbdd80 LATENCIES_MS=100 ROUNDS=1 ITERS=1 WARMUP=0 VL=0 scripts/bench/field_metadata/run.sh /tmp/fm-100ms
cat /tmp/fm-0ms/matrix.jsonl /tmp/fm-100ms/matrix.jsonl > /tmp/fm.jsonl
python3 scripts/bench/field_metadata/perf_rows.py check /tmp/fm.jsonl tests/conformance/registry/rows/perf/field_metadata.yaml
```

## Results — #239

### Exactness

| cells (values + hits vs truth) | v0.143.1 | #239 | hot VictoriaLogs |
|---|---|---|---|
| `field_values` + `streams`, both signals, all layouts/windows/filters/pmeta, per latency | 242 / 288 | **288 / 288** | 24 / 24 (logs) |
| … of which `layout=peer` | 66 / 96 | **96 / 96** | — |
| `field_names`, both signals | 0 / 96 | 0 / 96 (not fixed here) | 8 / 8 |

v0.143.1 fails the peer cells (unflushed rows missing: `whole` gets no `FATAL` / `shutdown` and
short hits) and every pmeta-on unfiltered `field_values` cell (catalog `hits=1`, hour granular).

### Latency and S3 cost at 0 ms (unfiltered, pmeta off)

p50 ms · S3 GETs · S3 bytes per request; `invalid` = not exact, never timed.

| endpoint | layout | window | v0.143.1 p50 ms · GETs · bytes | #239 p50 ms · GETs · bytes | hot VL p50 |
|---|---|---|---|---|---|
| fv_level | flushed | whole | 11.2 · 24 · 1.64M | 0.018 · 0 · 0 | 0.514 |
| fv_level | flushed | cut | 6.6 · 13 · 911K | 0.601 · 2 · 140K | 0.519 |
| fv_level | flushed | narrow | 0.429 · 1 · 72K | 0.468 · 1 · 72K | 0.231 |
| fv_level | flushed | edge | 2.5 · 5 · 347K | 0.538 · 1 · 72K | 0.446 |
| fv_level | compacted | whole | 8.7 · 18 · 555K | 0.008 · 0 · 0 | 0.514 |
| fv_level | compacted | cut | 27.1 · 24 · 579K | 3.9 · 18 · 555K | 0.519 |
| fv_level | compacted | narrow | 13.0 · 12 · 299K | 1.8 · 6 · 275K | 0.231 |
| fv_level | compacted | edge | 11.2 · 24 · 579K | 2.6 · 15 · 543K | 0.446 |
| fv_level | peer | whole | invalid | 19.0 · 0 · 0 | 0.514 |
| fv_level | peer | cut | 7.0 · 13 · 911K | 0.696 · 2 · 140K | 0.519 |
| fv_level | peer | narrow | 0.469 · 1 · 72K | 0.455 · 1 · 72K | 0.231 |
| fv_level | peer | edge | 2.7 · 5 · 347K | 0.519 · 1 · 72K | 0.446 |
| fv_service | flushed | whole | 13.4 · 24 · 1.64M | 1.7 · 12 · 869K | 0.590 |
| fv_service | flushed | cut | 7.7 · 13 · 911K | 1.1 · 8 · 574K | 0.480 |
| fv_service | flushed | narrow | 0.520 · 1 · 72K | 0.558 · 1 · 72K | 0.342 |
| fv_service | flushed | edge | 2.8 · 5 · 347K | 0.745 · 2 · 145K | 0.385 |
| fv_service | compacted | whole | 9.8 · 18 · 555K | 5.9 · 9 · 287K | 0.590 |
| fv_service | compacted | cut | 11.5 · 24 · 579K | 3.8 · 18 · 555K | 0.480 |
| fv_service | compacted | narrow | 5.3 · 12 · 299K | 2.0 · 6 · 275K | 0.342 |
| fv_service | compacted | edge | 11.1 · 24 · 579K | 3.0 · 15 · 543K | 0.385 |
| fv_service | peer | whole | invalid | 19.3 · 10 · 724K | 0.590 |
| fv_service | peer | cut | 13.6 · 13 · 911K | 1.1 · 8 · 574K | 0.480 |
| fv_service | peer | narrow | 0.693 · 1 · 72K | 0.506 · 1 · 72K | 0.342 |
| fv_service | peer | edge | 4.4 · 5 · 347K | 0.921 · 2 · 145K | 0.385 |
| streams | flushed | whole | 13.8 · 24 · 1.64M | 3.1 · 24 · 1.64M | 0.974 |
| streams | flushed | cut | 7.4 · 13 · 911K | 1.7 · 13 · 911K | 0.598 |
| streams | flushed | narrow | 0.466 · 1 · 72K | 0.659 · 1 · 72K | 0.271 |
| streams | flushed | edge | 2.9 · 5 · 347K | 0.881 · 5 · 347K | 0.406 |
| streams | compacted | whole | 8.5 · 18 · 555K | 5.4 · 18 · 555K | 0.974 |
| streams | compacted | cut | 10.7 · 24 · 579K | 6.4 · 18 · 555K | 0.598 |
| streams | compacted | narrow | 3.8 · 12 · 299K | 2.6 · 6 · 275K | 0.271 |
| streams | compacted | edge | 9.3 · 24 · 579K | 3.6 · 15 · 543K | 0.406 |
| streams | peer | whole | invalid | 23.1 · 22 · 1.50M | 0.974 |
| streams | peer | cut | 7.9 · 13 · 911K | 2.2 · 13 · 911K | 0.598 |
| streams | peer | narrow | 0.416 · 1 · 72K | 0.684 · 1 · 72K | 0.271 |
| streams | peer | edge | 2.4 · 5 · 347K | 1.1 · 5 · 347K | 0.406 |
| traces.fv_name | flushed | whole | 1.8 · 24 · 2.17M | 0.042 · 0 · 0 | — |
| traces.fv_name | flushed | cut | 1.1 · 13 · 1.18M | 0.761 · 2 · 185K | — |
| traces.fv_name | flushed | narrow | 0.549 · 1 · 93K | 0.623 · 1 · 93K | — |
| traces.fv_name | flushed | edge | 0.758 · 5 · 462K | 0.660 · 1 · 93K | — |
| traces.fv_name | compacted | whole | 2.9 · 2 · 1.86M | 0.020 · 0 · 0 | — |
| traces.fv_name | compacted | cut | 2.9 · 2 · 1.86M | 2.4 · 2 · 1.86M | — |
| traces.fv_name | compacted | narrow | 1.9 · 1 · 962K | 1.7 · 1 · 962K | — |
| traces.fv_name | compacted | edge | 2.2 · 2 · 1.86M | 2.0 · 2 · 1.86M | — |
| traces.fv_name | peer | whole | invalid | 26.0 · 0 · 0 | — |
| traces.fv_name | peer | cut | 1.2 · 13 · 1.18M | 0.877 · 2 · 185K | — |
| traces.fv_name | peer | narrow | 0.478 · 1 · 93K | 0.594 · 1 · 93K | — |
| traces.fv_name | peer | edge | 0.823 · 5 · 462K | 0.636 · 1 · 93K | — |
| traces.fv_service | flushed | whole | 1.7 · 24 · 2.17M | 2.3 · 12 · 1.09M | — |
| traces.fv_service | flushed | cut | 1.2 · 13 · 1.18M | 2.9 · 8 · 746K | — |
| traces.fv_service | flushed | narrow | 0.563 · 1 · 93K | 0.908 · 1 · 93K | — |
| traces.fv_service | flushed | edge | 0.703 · 5 · 462K | 0.801 · 2 · 187K | — |
| traces.fv_service | compacted | whole | 3.2 · 2 · 1.86M | 3.6 · 1 · 962K | — |
| traces.fv_service | compacted | cut | 2.4 · 2 · 1.86M | 2.5 · 2 · 1.86M | — |
| traces.fv_service | compacted | narrow | 2.0 · 1 · 962K | 1.5 · 1 · 962K | — |
| traces.fv_service | compacted | edge | 2.1 · 2 · 1.86M | 2.0 · 2 · 1.86M | — |
| traces.fv_service | peer | whole | invalid | 23.8 · 10 · 934K | — |
| traces.fv_service | peer | cut | 1.3 · 13 · 1.18M | 2.1 · 8 · 746K | — |
| traces.fv_service | peer | narrow | 0.520 · 1 · 93K | 1.0 · 1 · 93K | — |
| traces.fv_service | peer | edge | 0.727 · 5 · 462K | 1.5 · 2 · 187K | — |
| traces.streams | flushed | whole | 13.5 · 24 · 2.17M | 6.1 · 24 · 2.17M | — |
| traces.streams | flushed | cut | 6.4 · 13 · 1.18M | 3.6 · 13 · 1.18M | — |
| traces.streams | flushed | narrow | 0.415 · 1 · 93K | 0.940 · 1 · 93K | — |
| traces.streams | flushed | edge | 2.2 · 5 · 462K | 1.9 · 5 · 462K | — |
| traces.streams | compacted | whole | 5.5 · 2 · 1.86M | 3.7 · 2 · 1.86M | — |
| traces.streams | compacted | cut | 4.6 · 2 · 1.86M | 3.2 · 2 · 1.86M | — |
| traces.streams | compacted | narrow | 1.8 · 1 · 962K | 1.5 · 1 · 962K | — |
| traces.streams | compacted | edge | 3.6 · 2 · 1.86M | 2.2 · 2 · 1.86M | — |
| traces.streams | peer | whole | invalid | 26.2 · 22 · 1.99M | — |
| traces.streams | peer | cut | 6.3 · 13 · 1.18M | 1.6 · 13 · 1.18M | — |
| traces.streams | peer | narrow | 0.529 · 1 · 93K | 0.762 · 1 · 93K | — |
| traces.streams | peer | edge | 2.6 · 5 · 462K | 0.970 · 5 · 462K | — |

### At 100 ms S3 first-byte latency (unfiltered, pmeta off)

p50 ms · S3 GETs, v0.143.1 → **#239**. One round trip ≈ 100 ms, so the number of waves is visible
directly.

| endpoint | layout | whole | cut | narrow | edge |
|---|---|---|---|---|---|
| fv_level | flushed | 2462 · 24 → **0.027 · 0** | 1323 · 13 → **103 · 2** | 102 · 1 → **103 · 1** | 508 · 5 → **103 · 1** |
| fv_level | compacted | 1847 · 18 → **0.018 · 0** | 2440 · 24 → **916 · 18** | 1222 · 12 → **611 · 6** | 2454 · 24 → **930 · 15** |
| fv_level | peer | invalid → **15.7 · 0** | 1331 · 13 → **102 · 2** | 103 · 1 → **101 · 1** | 517 · 5 → **101 · 1** |
| fv_service | flushed | 2462 · 24 → **305 · 12** | 1330 · 13 → **206 · 8** | 101 · 1 → **101 · 1** | 508 · 5 → **102 · 2** |
| fv_service | compacted | 1841 · 18 → **917 · 9** | 2454 · 24 → **920 · 18** | 1222 · 12 → **609 · 6** | 2460 · 24 → **923 · 15** |
| fv_service | peer | invalid → **327 · 10** | 1323 · 13 → **204 · 8** | 102 · 1 → **102 · 1** | 509 · 5 → **103 · 2** |
| streams | flushed | 2470 · 24 → **609 · 24** | 1335 · 13 → **404 · 13** | 101 · 1 → **101 · 1** | 511 · 5 → **203 · 5** |
| streams | compacted | 1840 · 18 → **916 · 18** | 2447 · 24 → **917 · 18** | 1218 · 12 → **611 · 6** | 2445 · 24 → **913 · 15** |
| streams | peer | invalid → **633 · 22** | 1331 · 13 → **409 · 13** | 102 · 1 → **103 · 1** | 530 · 5 → **202 · 5** |
| traces.fv_name | flushed | 105 · 24 → **0.032 · 0** | 104 · 13 → **102 · 2** | 102 · 1 → **104 · 1** | 102 · 5 → **101 · 1** |
| traces.fv_name | compacted | 105 · 2 → **0.020 · 0** | 105 · 2 → **105 · 2** | 102 · 1 → **104 · 1** | 114 · 2 → **103 · 2** |
| traces.fv_name | peer | invalid → **22.6 · 0** | 102 · 13 → **102 · 2** | 103 · 1 → **101 · 1** | 103 · 5 → **103 · 1** |
| traces.fv_service | flushed | 104 · 24 → **103 · 12** | 102 · 13 → **102 · 8** | 103 · 1 → **103 · 1** | 104 · 5 → **101 · 2** |
| traces.fv_service | compacted | 105 · 2 → **103 · 1** | 103 · 2 → **105 · 2** | 102 · 1 → **104 · 1** | 104 · 2 → **104 · 2** |
| traces.fv_service | peer | invalid → **131 · 10** | 104 · 13 → **104 · 8** | 101 · 1 → **102 · 1** | 103 · 5 → **101 · 2** |
| traces.streams | flushed | 2456 · 24 → **107 · 24** | 1331 · 13 → **106 · 13** | 101 · 1 → **101 · 1** | 514 · 5 → **104 · 5** |
| traces.streams | compacted | 207 · 2 → **106 · 2** | 206 · 2 → **104 · 2** | 103 · 1 → **103 · 1** | 207 · 2 → **105 · 2** |
| traces.streams | peer | invalid → **176 · 22** | 1322 · 13 → **104 · 13** | 101 · 1 → **103 · 1** | 508 · 5 → **103 · 5** |

Serial scans are gone: 24 objects cost one wave on traces (2456 → 107 ms for `streams`) and
objects inside the window cost nothing (logs `field_values level` whole: 2462 → 0.03 ms). Logs
`streams` whole is 609 ms, about 6 waves: objects under 128 KiB are fetched whole through the
download semaphore, which the harness sizes at 4 (production default: the S3 download bound, 16).

### Compaction parity (#239, 0 ms, pmeta off)

Same rows, flushed vs compacted. Every compacted cell is exact wherever its flushed twin is (the
`field-metadata-perf` gate enforces it).

| endpoint | window | filter | flushed p50 · GETs · bytes | compacted p50 · GETs · bytes | exact flushed · compacted |
|---|---|---|---|---|---|
| fv_level | whole | none | 0.018 · 0 · 0 | 0.008 · 0 · 0 | 4/4 · 4/4 |
| fv_level | whole | svc | 4.4 · 24 · 1.64M | 10.8 · 36 · 627K | 4/4 · 4/4 |
| fv_level | cut | none | 0.601 · 2 · 140K | 3.9 · 18 · 555K | 4/4 · 4/4 |
| fv_level | cut | svc | 2.6 · 13 · 911K | 6.9 · 26 · 587K | 4/4 · 4/4 |
| fv_level | narrow | none | 0.468 · 1 · 72K | 1.8 · 6 · 275K | 4/4 · 4/4 |
| fv_level | narrow | svc | 0.638 · 1 · 72K | 2.5 · 8 · 283K | 4/4 · 4/4 |
| fv_level | edge | none | 0.538 · 1 · 72K | 2.6 · 15 · 543K | 4/4 · 4/4 |
| fv_level | edge | svc | 1.9 · 5 · 347K | 5.0 · 20 · 561K | 4/4 · 4/4 |
| fv_service | whole | none | 1.7 · 12 · 869K | 5.9 · 9 · 287K | 4/4 · 4/4 |
| fv_service | whole | svc | 4.1 · 24 · 1.64M | 8.7 · 24 · 579K | 4/4 · 4/4 |
| fv_service | cut | none | 1.1 · 8 · 574K | 3.8 · 18 · 555K | 4/4 · 4/4 |
| fv_service | cut | svc | 2.7 · 13 · 911K | 5.8 · 18 · 555K | 4/4 · 4/4 |
| fv_service | narrow | none | 0.558 · 1 · 72K | 2.0 · 6 · 275K | 4/4 · 4/4 |
| fv_service | narrow | svc | 0.549 · 1 · 72K | 1.8 · 6 · 275K | 4/4 · 4/4 |
| fv_service | edge | none | 0.745 · 2 · 145K | 3.0 · 15 · 543K | 4/4 · 4/4 |
| fv_service | edge | svc | 1.7 · 5 · 347K | 4.8 · 15 · 543K | 4/4 · 4/4 |
| streams | whole | none | 3.1 · 24 · 1.64M | 5.4 · 18 · 555K | 4/4 · 4/4 |
| streams | whole | svc | 4.3 · 24 · 1.64M | 23.3 · 36 · 627K | 4/4 · 4/4 |
| streams | cut | none | 1.7 · 13 · 911K | 6.4 · 18 · 555K | 4/4 · 4/4 |
| streams | cut | svc | 3.1 · 13 · 911K | 9.5 · 26 · 587K | 4/4 · 4/4 |
| streams | narrow | none | 0.659 · 1 · 72K | 2.6 · 6 · 275K | 4/4 · 4/4 |
| streams | narrow | svc | 0.654 · 1 · 72K | 3.1 · 8 · 283K | 4/4 · 4/4 |
| streams | edge | none | 0.881 · 5 · 347K | 3.6 · 15 · 543K | 4/4 · 4/4 |
| streams | edge | svc | 1.4 · 5 · 347K | 7.3 · 21 · 567K | 4/4 · 4/4 |
| traces.fv_name | whole | none | 0.042 · 0 · 0 | 0.020 · 0 · 0 | 4/4 · 4/4 |
| traces.fv_name | whole | svc | 4.3 · 24 · 2.17M | 9.4 · 2 · 1.86M | 4/4 · 4/4 |
| traces.fv_name | cut | none | 0.761 · 2 · 185K | 2.4 · 2 · 1.86M | 4/4 · 4/4 |
| traces.fv_name | cut | svc | 3.2 · 13 · 1.18M | 5.5 · 2 · 1.86M | 4/4 · 4/4 |
| traces.fv_name | narrow | none | 0.623 · 1 · 93K | 1.7 · 1 · 962K | 4/4 · 4/4 |
| traces.fv_name | narrow | svc | 0.720 · 1 · 93K | 1.9 · 1 · 962K | 4/4 · 4/4 |
| traces.fv_name | edge | none | 0.660 · 1 · 93K | 2.0 · 2 · 1.86M | 4/4 · 4/4 |
| traces.fv_name | edge | svc | 1.6 · 5 · 462K | 3.9 · 2 · 1.86M | 4/4 · 4/4 |
| traces.fv_service | whole | none | 2.3 · 12 · 1.09M | 3.6 · 1 · 962K | 4/4 · 4/4 |
| traces.fv_service | whole | svc | 4.9 · 24 · 2.17M | 7.5 · 2 · 1.86M | 4/4 · 4/4 |
| traces.fv_service | cut | none | 2.9 · 8 · 746K | 2.5 · 2 · 1.86M | 4/4 · 4/4 |
| traces.fv_service | cut | svc | 2.4 · 13 · 1.18M | 4.1 · 2 · 1.86M | 4/4 · 4/4 |
| traces.fv_service | narrow | none | 0.908 · 1 · 93K | 1.5 · 1 · 962K | 4/4 · 4/4 |
| traces.fv_service | narrow | svc | 0.648 · 1 · 93K | 1.5 · 1 · 962K | 4/4 · 4/4 |
| traces.fv_service | edge | none | 0.801 · 2 · 187K | 2.0 · 2 · 1.86M | 4/4 · 4/4 |
| traces.fv_service | edge | svc | 2.0 · 5 · 462K | 4.2 · 2 · 1.86M | 4/4 · 4/4 |
| traces.streams | whole | none | 6.1 · 24 · 2.17M | 3.7 · 2 · 1.86M | 4/4 · 4/4 |
| traces.streams | whole | svc | 4.9 · 24 · 2.17M | 9.7 · 2 · 1.86M | 4/4 · 4/4 |
| traces.streams | cut | none | 3.6 · 13 · 1.18M | 3.2 · 2 · 1.86M | 4/4 · 4/4 |
| traces.streams | cut | svc | 4.3 · 13 · 1.18M | 6.1 · 2 · 1.86M | 4/4 · 4/4 |
| traces.streams | narrow | none | 0.940 · 1 · 93K | 1.5 · 1 · 962K | 4/4 · 4/4 |
| traces.streams | narrow | svc | 0.812 · 1 · 93K | 1.6 · 1 · 962K | 4/4 · 4/4 |
| traces.streams | edge | none | 1.9 · 5 · 462K | 2.2 · 2 · 1.86M | 4/4 · 4/4 |
| traces.streams | edge | svc | 1.8 · 5 · 462K | 3.2 · 2 · 1.86M | 4/4 · 4/4 |

Whole windows are at parity or better after compaction (label counts answer both; scans read
fewer bytes: 555–579 KB vs 1.64 MB). Cut, narrow and edge windows cost more on compacted
objects: the hour object is cut by the window, so it is scanned, and its row groups are not
sorted by time, so a 30-second window still reaches 2 of its 6 row groups. Row-group time
pruning (this PR) halved that cost:

<details><summary>benchstat, narrow window, v0.143.1 vs #239 (n = 6, interleaved)</summary>

```
                                                                                       │ bs_before.txt │             bs_after.txt             │
                                                                                       │    sec/op     │    sec/op      vs base               │
FieldMetadata/fv_level/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms       308.3µ ±  48%   351.3µ ±  70%        ~ (p=0.240 n=6)
FieldMetadata/fv_level/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms     2.810m ±  95%   1.561m ±  63%  -44.46% (p=0.015 n=6)
FieldMetadata/fv_service/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms     318.6µ ± 110%   471.0µ ± 126%        ~ (p=0.093 n=6)
FieldMetadata/fv_service/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms   2.961m ±  94%   1.394m ±  86%  -52.91% (p=0.009 n=6)
FieldMetadata/streams/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms        401.9µ ±  65%   432.3µ ±  78%        ~ (p=0.589 n=6)
FieldMetadata/streams/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms      2.627m ± 107%   1.619m ±  40%  -38.37% (p=0.002 n=6)
geomean                                                                                  975.7µ          794.7µ         -18.55%

                                                                                       │ bs_before.txt │             bs_after.txt             │
                                                                                       │  call_ns/op   │  call_ns/op    vs base               │
FieldMetadata/fv_level/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms       303.2k ±  47%   346.2k ±  69%        ~ (p=0.240 n=6)
FieldMetadata/fv_level/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms     2.803M ±  95%   1.553M ±  63%  -44.61% (p=0.015 n=6)
FieldMetadata/fv_service/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms     308.4k ± 110%   457.7k ± 129%        ~ (p=0.093 n=6)
FieldMetadata/fv_service/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms   2.947M ±  94%   1.382M ±  86%  -53.11% (p=0.009 n=6)
FieldMetadata/streams/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms        387.5k ±  67%   421.1k ±  78%        ~ (p=0.589 n=6)
FieldMetadata/streams/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms      2.614M ± 107%   1.604M ±  40%  -38.62% (p=0.002 n=6)
geomean                                                                                  959.9k          782.6k         -18.48%

                                                                                       │ bs_before.txt │            bs_after.txt             │
                                                                                       │    gets/op    │  gets/op    vs base                 │
FieldMetadata/fv_level/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms          1.000 ± 0%   1.000 ± 0%        ~ (p=1.000 n=6) ¹
FieldMetadata/fv_level/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms       12.000 ± 0%   6.000 ± 0%  -50.00% (p=0.002 n=6)
FieldMetadata/fv_service/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms        1.000 ± 0%   1.000 ± 0%        ~ (p=1.000 n=6) ¹
FieldMetadata/fv_service/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms     12.000 ± 0%   6.000 ± 0%  -50.00% (p=0.002 n=6)
FieldMetadata/streams/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms           1.000 ± 0%   1.000 ± 0%        ~ (p=1.000 n=6) ¹
FieldMetadata/streams/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms        12.000 ± 0%   6.000 ± 0%  -50.00% (p=0.002 n=6)
geomean                                                                                     3.464        2.449       -29.29%
¹ all samples are equal

                                                                                       │ bs_before.txt │            bs_after.txt            │
                                                                                       │   pages/op    │  pages/op   vs base                │
FieldMetadata/fv_level/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms          81.00 ± 0%   81.00 ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/fv_level/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms        97.00 ± 0%   89.00 ± 0%  -8.25% (p=0.002 n=6)
FieldMetadata/fv_service/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms        81.00 ± 0%   81.00 ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/fv_service/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms      91.00 ± 0%   86.00 ± 0%  -5.49% (p=0.002 n=6)
FieldMetadata/streams/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms           81.00 ± 0%   81.00 ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/streams/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms         91.00 ± 0%   86.00 ± 0%  -5.49% (p=0.002 n=6)
geomean                                                                                     86.77        83.94       -3.26%
¹ all samples are equal

                                                                                       │ bs_before.txt │            bs_after.txt             │
                                                                                       │    rgs/op     │   rgs/op    vs base                 │
FieldMetadata/fv_level/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms          1.000 ± 0%   1.000 ± 0%        ~ (p=1.000 n=6) ¹
FieldMetadata/fv_level/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms        3.000 ± 0%   2.000 ± 0%  -33.33% (p=0.002 n=6)
FieldMetadata/fv_service/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms        1.000 ± 0%   1.000 ± 0%        ~ (p=1.000 n=6) ¹
FieldMetadata/fv_service/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms      3.000 ± 0%   2.000 ± 0%  -33.33% (p=0.002 n=6)
FieldMetadata/streams/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms           1.000 ± 0%   1.000 ± 0%        ~ (p=1.000 n=6) ¹
FieldMetadata/streams/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms         3.000 ± 0%   2.000 ± 0%  -33.33% (p=0.002 n=6)
geomean                                                                                     1.732        1.414       -18.35%
¹ all samples are equal

                                                                                       │ bs_before.txt │            bs_after.txt             │
                                                                                       │    s3B/op     │   s3B/op     vs base                │
FieldMetadata/fv_level/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms         74.09k ± 0%   74.09k ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/fv_level/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms       305.7k ± 0%   281.1k ± 0%  -8.04% (p=0.002 n=6)
FieldMetadata/fv_service/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms       74.09k ± 0%   74.09k ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/fv_service/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms     305.7k ± 0%   281.1k ± 0%  -8.04% (p=0.002 n=6)
FieldMetadata/streams/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms          74.09k ± 0%   74.09k ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/streams/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms        305.7k ± 0%   281.1k ± 0%  -8.04% (p=0.002 n=6)
geomean                                                                                    150.5k        144.3k       -4.10%
¹ all samples are equal

                                                                                       │ bs_before.txt │            bs_after.txt            │
                                                                                       │   set_valid   │ set_valid   vs base                │
FieldMetadata/fv_level/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms          1.000 ± 0%   1.000 ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/fv_level/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms        1.000 ± 0%   1.000 ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/fv_service/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms        1.000 ± 0%   1.000 ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/fv_service/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms      1.000 ± 0%   1.000 ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/streams/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms           1.000 ± 0%   1.000 ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/streams/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms         1.000 ± 0%   1.000 ± 0%       ~ (p=1.000 n=6) ¹
geomean                                                                                     1.000        1.000       +0.00%
¹ all samples are equal

                                                                                       │ bs_before.txt │            bs_after.txt            │
                                                                                       │     valid     │   valid     vs base                │
FieldMetadata/fv_level/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms          1.000 ± 0%   1.000 ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/fv_level/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms        1.000 ± 0%   1.000 ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/fv_service/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms        1.000 ± 0%   1.000 ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/fv_service/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms      1.000 ± 0%   1.000 ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/streams/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms           1.000 ± 0%   1.000 ± 0%       ~ (p=1.000 n=6) ¹
FieldMetadata/streams/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms         1.000 ± 0%   1.000 ± 0%       ~ (p=1.000 n=6) ¹
geomean                                                                                     1.000        1.000       +0.00%
¹ all samples are equal
```

</details>

Narrow calls on compacted objects: −38 to −53 % time, half the GETs (p ≤ 0.015). On single
flushed objects no difference is significant (p = 0.09–0.59; the noise band at this host load is
±50–120 %). What closes the remaining compacted-vs-flushed gap: time-sorted row groups in
compaction output, the page index for straddling row groups, and warm footers (the harness reads
every footer cold).

### Unflushed rows (`layout=peer`)

The last ten minutes live only on a peer. `whole` needs them (`FATAL`, `shutdown` and 4000 rows
of hits); v0.143.1 answered those cells without them. #239 merges them after each tenant's flush
watermark, so the overlap is never counted twice. Cost: 16–26 ms at 0 ms S3 for 4000 unflushed
rows — the bridge ships full rows as JSON and they are converted and filtered, the same path
`RunQuery` pays. Next step: a peer answers enumeration over its own buffer with the VictoriaLogs
engine and returns (value, hits) pairs, so the transfer is proportional to distinct values, not
rows.

## Three-way results: Lakehouse vs disk VictoriaLogs/VictoriaTraces vs ClickHouse on S3

**Before (v0.143.1 behaviour, valid run).** `scripts/bench/run.sh --queries "fv_level fv_service
streams_list fv_name"` on the benchmark stack (MinIO behind toxiproxy, 7 days of seeded data, the
same Parquet read by Lakehouse and ClickHouse), 10 timed iterations per cell after 2 warm-ups,
every response validated. Raw results: `bench-results/field-metadata-2026-09-24/run.{json,md}`.

| Query | Window | Hot VL/VT p50 (ms) | Lakehouse p50 (ms) | ClickHouse p50 (ms) | Answer |
|---|---|---|---|---|---|
| logs `field_values level` | 1h / 6h / 24h, 0 ms S3 | 3.5 / 4.3 / 9.7 | 3.4 / 1.5 / 3.0 | 208 / 157 / 187 | **Lakehouse wrong**: every value has hits=1 (total 4 vs 553 / 14 160) |
| logs `field_values level` | same, 100 ms S3 | 1.7 / 2.3 / 3.9 | 1.1 / 1.5 / 1.2 | 84 / 83 / 74 | **Lakehouse wrong** (same) |
| logs `field_values service.name` | 1h / 6h / 24h, 0 ms S3 | 3.6 / 3.4 / 9.1 | 1.7 / 1.5 / 1.8 | 760 / 144 / 179 | **Lakehouse wrong** (hits=1) |
| logs `streams` | 1h / 6h / 24h, 0 ms S3 | 12.0 / 19.9 / 82.9 | 14.5 / 63.6 / 187.6 | 121.9 / 211.6 / 112.7 | exact; **ClickHouse faster at 24h** |
| logs `streams` | same, 100 ms S3 | 3.1 / 18.5 / 44.1 | 6.8 / 36.8 / 141.5 | 78.3 / 80.6 / 103.7 | exact; **ClickHouse faster at 24h** |
| traces `field_values name` | 1h / 6h / 24h, 0 ms S3 | 1.6 / 1.8 / 2.1 | 1.8 / 3.2 / 7.1 | 90 / 118 / 89 | exact |
| traces `field_values service` | 1h / 6h / 24h, 100 ms S3 | 1.6 / 1.5 / 1.9 | 1.5 / 2.3 / 7.2 | 89 / 94 / 117 | exact |

The rows this PR changes: logs `field_values` wrong on every cell (hits=1) → exact (in-process:
288/288); logs `streams` 24h 188 ms vs ClickHouse 113 ms, driven by the serial per-object scan →
one wave (in-process at 100 ms S3: 2470 → 609 ms with the harness's 4 concurrent downloads, 2456 →
107 ms on traces).

**After #239 and the flush durability fix (valid run).** Same benchmark, same host under load
16–22 from other work — the conditions that lost rows in two earlier attempts. Both signals
converged exactly (logs 14 360 = 14 360; traces 16 562 = 16 562 counted as spans), both parity
gates passed, and **every Lakehouse cell is valid (36/36)**: the same values and hits as disk
VictoriaLogs/VictoriaTraces and ClickHouse. p95/p90 per cell, 10 iterations after 2 warm-ups (the
report's statistic; the before-state above is p50). Raw results:
`bench-results/field-metadata-2026-09-24/run-durability.{json,md}`.

| Query | Window, S3 | Disk VL/VT | Lakehouse | ClickHouse on S3 |
|---|---|---|---|---|
| logs `field_values level` | 1h / 6h / 24h, 0 ms | 4.8 / 2.3 / 6.7 | 5.0 / 8.8 / **5.6** | 83 / 99 / 79 |
| logs `field_values level` | 24h, 100 ms | 5.9 | **5.7** | 97 |
| logs `field_values service.name` | 1h / 6h / 24h, 0 ms | 2.3 / 7.1 / 12.7 | 8.0 / 10.6 / **6.8** | 84 / 81 / 106 |
| logs `streams` | 1h / 6h / 24h, 0 ms | 5.2 / 15.8 / 70.5 | 7.2 / 18.2 / **94.9** | 93 / 81 / 201 |
| logs `streams` | 1h / 6h / 24h, 100 ms | 5.8 / 23.7 / 53.2 | 28.8 / 33.0 / **63.4** | 98 / 131 / 161 |
| traces `field_values name` | 1h / 6h / 24h, 0 ms | 1.7 / 2.2 / 13.2 | 5.8 / 13.4 / 18.1 | 75 / 77 / 155 |
| traces `field_values service` | 24h, 0 / 100 ms | 3.8 / 2.2 | 10.5 / 11.5 | 84 / 101 |
| traces `streams` | 1h / 6h / 24h, 0 ms | 3.2 / 3.0 / 2.1 | 5.8 / 25.1 / 14.0 | 95 / 113 / 132 |

- Logs `field_values` is exact (was hits=1) and at 24h as fast as or faster than disk VictoriaLogs.
- **Logs `streams` at 24h now beats ClickHouse: 95 vs 201 ms (0 ms S3), 63 vs 161 ms (100 ms S3)**
  — the case that was 188 vs 113 ms before — and is 1.2–1.4× disk VictoriaLogs.
- Traces are 9–80× faster than ClickHouse on every cell and 1.2–8× disk VictoriaTraces; closing
  that gap is the metadata work in the plan below.

## Closing the logs `streams` gap (ClickHouse at 24h, disk VictoriaLogs)

`streams` has no metadata answer: every object in the window is read. Where the time goes, from the
counters above, and what each step removes:

| # | step | status | removes | expected effect |
|---|---|---|---|---|
| 1 | **Parallel scan** on the file-worker pool (was one object after another) | done (#239) | serial GET round-trips and serial decode | one wave instead of N; at 24h (hundreds of objects) CPU-bound decode spread over `query.file_workers` |
| 2 | **Row-group time pruning** on straddling objects | done (#239) | row groups outside the window | narrow/cut windows on compacted objects read the row groups they touch, not the whole object |
| 3 | **Unflushed rows merged** (local buffer, peers) | done (#239) | — (correctness) | streams in the last minutes are listed, as upstream does |
| 4 | **Per-object `_stream` counts** as a label aggregate keyed by `_stream_id` (16 B) with one shared id→stream dictionary per partition in pmeta | proposed | the whole scan for objects inside the window | streams like field_values: RAM-only for contained objects. RAM ≈ objects × streams/object × 24 B + distinct streams × length; bounded by the same per-object cap (100) as other aggregates, so a busy object still scans |
| 5 | **Dictionary-index counting** instead of string decoding: `_stream` is dictionary-encoded; count dictionary indexes per page and map once | proposed | per-row string materialisation and hashing | the decode cost that dominates at 24h drops to integer counting |
| 6 | **Sort compaction output by (`_stream`, time)** and read row-group statistics: a row group whose `_stream` min equals max holds one stream, and its row count is in the footer | proposed (with the storage lifecycle plan) | data-page reads for single-stream row groups | big compacted objects answer `streams` from cached footers — the Parquet counterpart of VictoriaLogs reading stream IDs from block headers |
| 7 | **Range reads of the `_stream` chunk** instead of whole small objects (objects < 128 KiB are fetched whole today) | tier T-dict/T-page | bytes | S3 bytes ≈ the `_stream` column, not the object |

Why ClickHouse wins at 24h today and where the steps put Lakehouse: ClickHouse reads the `Stream`
column of every object in parallel (`max_threads`) and aggregates dictionary-encoded strings;
before #239 Lakehouse read the objects one by one. Steps 1–3 remove the structural disadvantage;
steps 4–6 go past ClickHouse by not reading data at all for most objects, which is how disk
VictoriaLogs answers (block headers carry the stream id).

## Findings (not fixed here)

1. **`field_names` is wrong on both signals.** Logs: lists all-null columns, ignores filter and
   window, depends on footer-cache state. Traces: pmeta on returns the catalog's Parquet keys
   (`account_id`, `project_id`, `service.name`, `span.name`) with hits 1; pmeta off returns all 48
   schema columns with hits 1; the answer is 11 fields × 48 000.
2. **Buffered rows are named differently from flushed rows** on map attributes (`resource_attr:K`
   vs `K` on logs), Tier-2 slots (not emitted by the bridge) and traces promoted attributes. The
   logs bridge now carries `_stream_id`, `severity_number` and `scope.name`
   (`TestBridgeLogRows_CarryEveryFieldTheFilePathEmits`); the rest needs the canonical naming
   decided against hot VL/VT first.
3. **Compacted traces objects are downloaded whole when their footer is not cached**: below
   `s3.whole_file_threshold_bytes` (8 MB for traces, whose trace-index footers are ~500 KB) one
   GET beats a footer fetch plus span reads. A 30-second window on a cold node reads the 962 KB
   hour object; warm-footer cells are not measured yet.
4. **Filtered scans of compacted objects issue a varying number of 4 KiB range reads** (±2 per
   request between iterations, fewer at 100 ms than at 0 ms), on v0.143.1 as well. The registry
   rows hold the worst case.
5. **The row path drops a row exactly at the window end** when it opens a row group (end-exclusive
   `rowGroupMatchesTimeRange`); the enumeration scans use an inclusive check
   (`TestFieldValues_RowGroupPruningKeepsTheWindowEnd`).
6. **A failed flush dropped its rows — fixed.** In two attempts at the three-way run (host load
   ~36), ~40 partitions failed `PutObject` with `context deadline exceeded` within the 60 s flush
   timeout and the logs count stayed at 60 % of the baseline: `FlushAll` cleared the write buffers
   before uploading and did not put a failed partition back. Fixed by the flush durability change
   (see [durability](../durability.md#21-failed-uploads)); the traces seed's "87.5 %" was a
   benchmark count, not loss (VictoriaTraces' `*` includes one internal index row per trace).
7. **A slow peer drops out of the unflushed window.** The buffer bridge ships every unflushed row
   as JSON under `select.buffer_query_timeout` (default 2 s). On a shared CI runner the harness's
   in-process peer broke off after 2112 of 4000 rows at 5 s; before #239 those 2112 rows were
   silently counted as the peer's whole answer, now the answer is dropped whole and counted in
   `lakehouse_buffer_bridge_errors_total{reason="decode"}`. A peer answering enumeration with
   (value, hits) pairs instead of rows keeps large unflushed windows inside the timeout.

## Noise

Host load average was 12–36 throughout (other work on the machine). At 0 ms, identical-code cells
(`field_names`) scatter up to 2.5× between rounds for sub-millisecond answers: read 0 ms deltas
under ~1 ms as noise unless benchstat says otherwise. 100 ms cells are latency-bound and stable.

## History: #237 (v0.143.0 → v0.143.1)

### Results — condensed (#237)

p50 ms over **exact** iterations (n = 10 per build); `invalid` = no exact
iteration (never timed); `[x]†` = set-exact but `hits=1` (RAM answer, not
exact). GETs / bytes are per request and deterministic. Hot VictoriaLogs
(same rows, in-process upstream storage) answers every one of these cells
exactly in **0.12–0.82 ms**.

### Unfiltered requests — where #237 changed the answer path

| cell (logs) | before | after 0 ms | after 100 ms | GETs / S3 bytes after | path after |
|---|---|---|---|---|---|
| fv `level`, pmeta off, flushed, whole | invalid (index: 4–5 of 6 values) | 7.6 | 2460 | 24 / 1.64 MB | scan |
| fv `level`, pmeta off, compacted, whole | invalid (index) | 4.7 | 1842 | 18 / 555 KB | scan |
| fv `level`, pmeta on, flushed/compacted, whole | invalid (index) | [0.009]† | [0.017]† | 0 / 0 | catalog, hits=1 |
| fv `level`, pmeta on, flushed/compacted, **cut** | [µs]† 4–6/10 set | **invalid** 0/10 | **invalid** 0/10 | 0 / 0 | catalog, hour-granular: lists TRACE/FATAL from outside the window |
| fv `level`, pmeta off, flushed, cut | [µs]† 4/10 set | 4.3 | 1329 | 13 / 911 KB | scan |
| fv `service.name` (busy), pmeta on, flushed, whole | invalid (catalog: 9 of 709) | 7.8 | 2469 | 24 / 1.64 MB | scan (high-card) |
| fv `service.name`, pmeta on, compacted, whole | invalid (catalog: 9 of 709) | 5.1 | 1841 | 18 / 555 KB | scan (compaction output high-card) |
| fv `service.name`, pmeta off, compacted, cut | invalid (index: 9–159 of 408) | 5.4 | 2462 | 24 / 579 KB | scan |
| `streams`, flushed, whole | 8.2 / 2451 (already exact) | 8.0 | 2465 | 24 / 1.64 MB | scan (unchanged) |
| `streams`, flushed, cut | invalid (458 of 408: leak) | 4.7 | 1336 | 13 / 911 KB | scan |
| `streams`, compacted, cut | invalid (709 of 408: leak) | 5.8 | 2457 | **24** (before 18) / 579 KB | scan |
| `field_names`, every cell | invalid (19 or 37 names vs 8) | invalid | invalid | 24 / 1.64 MB flushed; 2 / 169 KB compacted | footer column index |
| traces fv `name`, pmeta off, flushed, whole | invalid (index) | 1.4 | **105** | 24 / 2.17 MB | scan, **8-way parallel** |
| traces fv `resource_attr:service.name`, flushed, whole | invalid (index) | 1.5 | 104 | 24 / 2.17 MB | scan, parallel |
| traces `streams`, flushed, whole | 8.5 / 2452 | 8.6 | 2464 | 24 / 2.17 MB | scan, **serial** |

### Filtered requests (`service.name:="svc-a"`)

Unchanged by #237 on every path: the catalog was never used with a filter
and the scan's cost is the same. Δ p50 0.95–1.02× at 100 ms on every
filtered cell (e.g. flushed/whole `level` 2478 → 2491 ms, compacted/whole
3298 → 3144 ms, 30–33 GETs). At 0 ms, compacted cut-window filtered cells
are 0.72–0.77× (faster) for `field_values`: the window check now runs before
the filter, so rows outside the window are no longer filter-evaluated. Other
filtered cells at 0 ms fall within 0.72–1.25×, which is inside this host's
±25 % sub-millisecond noise; do not quote a single all-cell range at 0 ms.

### Analysis

**Nothing that was exact before got slower, except one shape.** Every cell
that was exact on v0.143.0 is within noise on v0.143.1 (0.95–1.02× at
100 ms), with one exception: **compacted objects that straddle a cut window**
now read the timestamp column for the row-level window check — 18 → 24 GETs
(**+33 %**), 1838 → 2457 ms (+34 %) at 100 ms for `streams`, the time ratio
measured against the previously invalid cell's timing; the same +6 GETs apply to
`field_values` on that shape. (Before, these answers were wrong — they leaked
rows from outside the window — so it is not a regression against a valid
baseline, but it is the cost of exactness to optimise.)

**The "regressed" cells are the ones that used to answer fast and wrong.**
Every unfiltered `field_values` cell that v0.143.0 answered from RAM
(label index, or the mis-keyed/capped catalog) was exact in 0/10 — at best
set-exact with `hits=1`, depending on which file seeded the index — e.g. 9 of
709 services, 4–5 of 6 levels. v0.143.1 answers them exactly with the
projected scan: **4–8 ms with 0 ms S3, 1.3–2.5 s with 100 ms S3**, against
0.1–0.8 ms on hot VictoriaLogs. Why they reach the scan:

1. **Catalog miss because the field is busy / high-card.** A file with
   ≥ 100 distinct values of a field (`maxLabelsPerField` = 100, the label
   extractor's cap) marks the field truncated for that partition, and
   `FieldValuesExact` refuses the whole range. The catalog's own threshold is
   50 000 (`defaultCardinalityThreshold`): the 700-value busy hour would fit
   ~70× over, but it is never fed more than 100 values per file.
2. **Compaction outputs marked high-card.** `PmetaOnCompacted` feeds the
   output's `Labels`, the union of the inputs' 100-capped label lists
   (`mergeFileLabels`), so any output whose union reaches 100 is truncated —
   even though the compactor has every merged row in memory (it extracts the
   uncapped combined bloom values from them already).
3. **Full projected scan, serial, on 100 ms S3.** The logs
   `GetFieldValues` / `GetStreams` loops scan files one after another, so
   latency is GETs × first-byte: 24 GETs → 2.46 s, 13 → 1.33 s. The traces
   `GetFieldValues` scans files with a worker pool and does the same 24 GETs
   in **105 ms**; traces `GetStreams` is serial and takes 2.46 s. Objects under
   128 KB (every flushed object here, and the production median) are
   downloaded whole: `field_values level` reads **1.64 MB to decode a
   3.6 KB column** (1.5 KB of it dictionary) — ~450× byte amplification.
   Compacted objects use ranged reads: fewer bytes (555–627 KB) but more
   serial GETs per object (9–18), so compaction alone does not cut latency
   (compacted/cut 2457 ms vs flushed/cut 1336 ms).
4. **The catalog is hour-granular.** When it does answer, a window that cuts
   an hour gets the whole hour's values (0/10 exact: `TRACE`, `FATAL` from
   outside the window) and every value carries `hits=1` (never exact; set-exact
   10/10 on whole hours, ~10–20 µs).

**`field_names` is invalid on both builds in every cell** (#237 changed only
the empty-window case, which these cells do not exercise). It lists every
Parquet column with a non-null count, including `account_id`, `project_id`,
the k8s/cloud columns written as empty strings (19 names vs 8), is not
window-confined (hits = whole-file row counts), and ignores the filter
(`trace_id` listed for `svc-a`). On compacted objects read cold the answer
is **37 names**: the tail-read footer has no column index, and
`accumulateFieldHits` then credits every chunk — including all-null optional
dedicated columns — as fully non-null; the same objects answer 19 names once
a whole-file download has warmed the footer cache, so the answer depends on
cache state.

### What a tiered design would fix

| tier | fixes cells | expected effect (from the counters above) |
|---|---|---|
| **T0 — bounded parallel per-file scan** in logs `GetFieldValues` / `GetStreams` / `GetStreamIDs` and traces `GetStreams` (the traces `GetFieldValues` worker pool already exists) | every scan cell at 100 ms | measured on traces: 24 GETs 2.46 s → 0.105 s; logs flushed whole ≈ 2.46 s → ~0.1–0.3 s |
| **T1 — exact catalog sets, fed uncapped** at flush (rows are in memory) and at compaction (union exact input sets or re-extract from merged rows, as the combined bloom already is), bounded by the 50 000 `CardinalityThreshold`, not by the 100-value label cap | `field_values service.name` whole/cut, pmeta on, both layouts | busy fields become catalog-served: 1.8–2.5 s / 18–24 GETs → ~10–20 µs / 0 GETs |
| **T2 — per-value row counts in the catalog** (per partition-hour) | pmeta-on catalog cells (`hits=1` today) | set-exact → exact at ~10–20 µs for hour-aligned windows |
| **T3 — catalog for interior hours + scan only the edge partitions** of a cut window (instead of hour-granular answers) | pmeta-on cut cells (invalid today) | exact; cost = the two straddling hours only (flushed cut: 13 GETs → ≤ 13, with T0 parallel) |
| **T4 — dictionary pages for row groups wholly inside the window** (set from the dictionary, hits from the RLE index pages of the same chunk) and a column-chunk plan for sub-128 KB objects when the footer is cached (or its chunk offsets are carried in pmeta) | pmeta-off and high-card scan cells | `level`: 3.6 KB of column vs 1.64 MB whole-object downloads (~450× fewer bytes); GETs stay 1 per object |
| **T5 — page-index skipping on straddling objects**: use the timestamp column/offset index to select window pages instead of projecting the timestamp column over every row | compacted/cut cells (the +6 GETs, +33 %) | back to ≤ 18 GETs with exact window confinement |
| **T6 — bloom/statistics/dictionary pruning for filtered requests** | filtered cells | no gain on this dataset (`svc-a` is in every object — a worst case); for a selective filter, objects/row groups whose SBBF or dictionary lacks the value are skipped |
| **T7 — `field_names` from the catalog's per-partition names + row counts**, skipping columns that are empty or all-null; filter-aware only through the scan | all `field_names` cells | makes the answer exact and cache-state independent; removes the 24 whole-object downloads for flushed layouts |

### Noise

Load average was 3–7 for the 0 ms run and 3–14 for most of the 100 ms run
(27 at its very end, a local VM outside this work), 18 cores. The 100 ms cells
are latency-bound: p90/p50 ≤ 1.1 in every exact cell, and identical-code
cells land at 0.95–1.02×. At 0 ms, identical-code cells scatter 0.8–1.25×
(e.g. `streams` pmeta on/off, same path): treat 0 ms deltas inside ±25 % as
noise. Traces records carry no truth digest in the JSONL (the traces harness
validates in-process with the same scheme), so traces cells cannot be
re-checked from the raw results alone. The hot-VictoriaLogs reference ran at load ~23 — its absolute numbers are
an upper bound.

### Not measured

- A compose-level comparison through HTTP with toxiproxy
  (`scripts/bench/run.sh`): the in-process VictoriaLogs reference replaces it
  for this step; adding `field_values` scenarios to the compose bench was not
  done.
- Warm object caches (memCache/footer cache hot): every iteration here is
  cold for objects; repeated dropdown requests on a warm node skip the GETs.
- `stream_ids` (same scan as `streams`), `limit > 0`, tombstone-active
  requests, multi-tenant scopes, `s3.projected_fetch_mode=planned` (default
  `window` measured), traces compacted layouts and traces `field_names`.
- Larger scales (thousands of objects per window, wide row groups); the
  counters scale linearly with objects in the window for the scan paths.

### Findings outside #237 (not fixed here)

1. **Row path drops a row at exactly `endNs`** when that row is the minimum of
   its row group: `rowGroupMatchesTimeRange` prunes with `rgMin < endNs`
   (end-exclusive) while `_time` filters are inclusive. The harness places its
   cut bounds between rows to keep its row-path self-check valid.
2. **`field_names` depends on cache state** on objects ≥ 128 KB (37 vs 19
   names, see above).
3. **Logs `field_values` / `streams` and traces `streams` scan serially**,
   while traces `field_values` fans out — a 23× latency difference at 100 ms
   for the same GETs.


## Full matrix

Every cell, both builds, both latencies (`aggregate.py` over the combined JSONL). p50/p90 over
exact iterations only.

Hot VictoriaLogs (in-process upstream storage, same rows, local disk):

| cell | p50 ms | p90 ms | exact k/N |
|---|---|---|---|
| vl.field_names/window=cut/filter=none | 0.198 | 0.208 | 4/4 |
| vl.field_names/window=cut/filter=svc | 0.162 | 0.180 | 4/4 |
| vl.field_names/window=edge/filter=none | 0.218 | 0.232 | 4/4 |
| vl.field_names/window=edge/filter=svc | 0.158 | 0.175 | 4/4 |
| vl.field_names/window=narrow/filter=none | 0.152 | 0.164 | 4/4 |
| vl.field_names/window=narrow/filter=svc | 0.163 | 0.194 | 4/4 |
| vl.field_names/window=whole/filter=none | 0.204 | 0.220 | 4/4 |
| vl.field_names/window=whole/filter=svc | 0.163 | 0.198 | 4/4 |
| vl.fv_level/window=cut/filter=none | 0.519 | 0.834 | 4/4 |
| vl.fv_level/window=cut/filter=svc | 0.236 | 0.246 | 4/4 |
| vl.fv_level/window=edge/filter=none | 0.446 | 0.614 | 4/4 |
| vl.fv_level/window=edge/filter=svc | 0.162 | 0.176 | 4/4 |
| vl.fv_level/window=narrow/filter=none | 0.231 | 0.286 | 4/4 |
| vl.fv_level/window=narrow/filter=svc | 0.160 | 0.197 | 4/4 |
| vl.fv_level/window=whole/filter=none | 0.514 | 0.779 | 4/4 |
| vl.fv_level/window=whole/filter=svc | 0.283 | 0.426 | 4/4 |
| vl.fv_service/window=cut/filter=none | 0.480 | 0.545 | 4/4 |
| vl.fv_service/window=cut/filter=svc | 0.218 | 0.230 | 4/4 |
| vl.fv_service/window=edge/filter=none | 0.385 | 0.400 | 4/4 |
| vl.fv_service/window=edge/filter=svc | 0.157 | 0.182 | 4/4 |
| vl.fv_service/window=narrow/filter=none | 0.342 | 0.375 | 4/4 |
| vl.fv_service/window=narrow/filter=svc | 0.166 | 0.196 | 4/4 |
| vl.fv_service/window=whole/filter=none | 0.590 | 1.9 | 4/4 |
| vl.fv_service/window=whole/filter=svc | 0.187 | 0.201 | 4/4 |
| vl.streams/window=cut/filter=none | 0.598 | 0.724 | 4/4 |
| vl.streams/window=cut/filter=svc | 0.172 | 0.208 | 4/4 |
| vl.streams/window=edge/filter=none | 0.406 | 0.491 | 4/4 |
| vl.streams/window=edge/filter=svc | 0.144 | 0.148 | 4/4 |
| vl.streams/window=narrow/filter=none | 0.271 | 0.357 | 4/4 |
| vl.streams/window=narrow/filter=svc | 0.134 | 0.136 | 4/4 |
| vl.streams/window=whole/filter=none | 0.974 | 1.1 | 4/4 |
| vl.streams/window=whole/filter=svc | 0.224 | 0.237 | 4/4 |

| cell | before p50 ms | p90 | exact k/N | GETs | S3 bytes | path | after p50 ms | p90 | exact k/N | GETs | S3 bytes | path | RGs/pages after | Δ p50 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fv_level/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms | 27.1 | 116 | 4/4 | 24 | 579K | scan | 3.9 | 4.7 | 4/4 | 18 | 555K | scan | 5/186 | 0.14× |
| fv_level/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms | 2440 | 2440 | 1/1 | 24 | 579K | scan | 916 | 916 | 1/1 | 18 | 555K | scan | 5/186 | 0.38× |
| fv_level/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms | 30.9 | 102 | 4/4 | 36 | 627K | scan | 6.9 | 7.9 | 4/4 | 26 | 587K | scan | 5/186 | 0.22× |
| fv_level/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms | 3378 | 3378 | 1/1 | 33 | 615K | scan | 1340 | 1340 | 1/1 | 24 | 579K | scan | 5/186 | 0.40× |
| fv_level/pmeta=false/layout=compacted/window=edge/filter=none/s3=0ms | 11.2 | 17.9 | 4/4 | 24 | 579K | scan | 2.6 | 2.8 | 4/4 | 15 | 543K | scan | 4/178 | 0.24× |
| fv_level/pmeta=false/layout=compacted/window=edge/filter=none/s3=100ms | 2454 | 2454 | 1/1 | 24 | 579K | scan | 930 | 930 | 1/1 | 15 | 543K | scan | 4/178 | 0.38× |
| fv_level/pmeta=false/layout=compacted/window=edge/filter=svc/s3=0ms | 14.3 | 18.9 | 4/4 | 36 | 627K | scan | 5.0 | 5.4 | 4/4 | 20 | 561K | scan | 4/178 | 0.35× |
| fv_level/pmeta=false/layout=compacted/window=edge/filter=svc/s3=100ms | 3379 | 3379 | 1/1 | 33 | 615K | scan | 1326 | 1326 | 1/1 | 21 | 567K | scan | 4/178 | 0.39× |
| fv_level/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms | 13.0 | 47.3 | 4/4 | 12 | 299K | scan | 1.8 | 2.1 | 4/4 | 6 | 275K | scan | 2/89 | 0.14× |
| fv_level/pmeta=false/layout=compacted/window=narrow/filter=none/s3=100ms | 1222 | 1222 | 1/1 | 12 | 299K | scan | 611 | 611 | 1/1 | 6 | 275K | scan | 2/89 | 0.50× |
| fv_level/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=0ms | 18.6 | 69.3 | 4/4 | 18 | 321K | scan | 2.5 | 2.6 | 4/4 | 8 | 283K | scan | 2/89 | 0.13× |
| fv_level/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=100ms | 1725 | 1725 | 1/1 | 17 | 319K | scan | 840 | 840 | 1/1 | 8 | 283K | scan | 2/89 | 0.49× |
| fv_level/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms | 8.7 | 16.1 | 4/4 | 18 | 555K | scan | 0.008 | 0.008 | 4/4 | 0 | 0 | ram | 0/0 | 0.00× |
| fv_level/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms | 1847 | 1847 | 1/1 | 18 | 555K | scan | 0.018 | 0.018 | 1/1 | 0 | 0 | ram | 0/0 | 0.00× |
| fv_level/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms | 45.3 | 74.9 | 4/4 | 36 | 627K | scan | 10.8 | 12.7 | 4/4 | 36 | 627K | scan | 6/194 | 0.24× |
| fv_level/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms | 3460 | 3460 | 1/1 | 34 | 619K | scan | 1831 | 1831 | 1/1 | 34 | 619K | scan | 6/194 | 0.53× |
| fv_level/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms | 6.6 | 7.5 | 4/4 | 13 | 911K | scan | 0.601 | 0.727 | 4/4 | 2 | 140K | scan | 2/162 | 0.09× |
| fv_level/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms | 1323 | 1323 | 1/1 | 13 | 911K | scan | 103 | 103 | 1/1 | 2 | 140K | scan | 2/162 | 0.08× |
| fv_level/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms | 12.7 | 14.0 | 4/4 | 13 | 911K | scan | 2.6 | 3.4 | 4/4 | 13 | 911K | scan | 13/1053 | 0.20× |
| fv_level/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms | 1338 | 1338 | 1/1 | 13 | 911K | scan | 409 | 409 | 1/1 | 13 | 911K | scan | 13/1053 | 0.31× |
| fv_level/pmeta=false/layout=flushed/window=edge/filter=none/s3=0ms | 2.5 | 2.8 | 4/4 | 5 | 347K | scan | 0.538 | 0.608 | 4/4 | 1 | 72K | scan | 1/81 | 0.22× |
| fv_level/pmeta=false/layout=flushed/window=edge/filter=none/s3=100ms | 508 | 508 | 1/1 | 5 | 347K | scan | 103 | 103 | 1/1 | 1 | 72K | scan | 1/81 | 0.20× |
| fv_level/pmeta=false/layout=flushed/window=edge/filter=svc/s3=0ms | 4.2 | 4.7 | 4/4 | 5 | 347K | scan | 1.9 | 2.1 | 4/4 | 5 | 347K | scan | 5/405 | 0.46× |
| fv_level/pmeta=false/layout=flushed/window=edge/filter=svc/s3=100ms | 512 | 512 | 1/1 | 5 | 347K | scan | 206 | 206 | 1/1 | 5 | 347K | scan | 5/405 | 0.40× |
| fv_level/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms | 0.429 | 0.451 | 4/4 | 1 | 72K | scan | 0.468 | 0.513 | 4/4 | 1 | 72K | scan | 1/81 | 1.09× |
| fv_level/pmeta=false/layout=flushed/window=narrow/filter=none/s3=100ms | 102 | 102 | 1/1 | 1 | 72K | scan | 103 | 103 | 1/1 | 1 | 72K | scan | 1/81 | 1.01× |
| fv_level/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=0ms | 0.576 | 0.799 | 4/4 | 1 | 72K | scan | 0.638 | 0.685 | 4/4 | 1 | 72K | scan | 1/81 | 1.11× |
| fv_level/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=100ms | 103 | 103 | 1/1 | 1 | 72K | scan | 104 | 104 | 1/1 | 1 | 72K | scan | 1/81 | 1.01× |
| fv_level/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms | 11.2 | 13.5 | 4/4 | 24 | 1.64M | scan | 0.018 | 0.019 | 4/4 | 0 | 0 | ram | 0/0 | 0.00× |
| fv_level/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms | 2462 | 2462 | 1/1 | 24 | 1.64M | scan | 0.027 | 0.027 | 1/1 | 0 | 0 | ram | 0/0 | 0.00× |
| fv_level/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms | 24.0 | 28.6 | 4/4 | 24 | 1.64M | scan | 4.4 | 5.3 | 4/4 | 24 | 1.64M | scan | 24/1944 | 0.18× |
| fv_level/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms | 2467 | 2467 | 1/1 | 24 | 1.64M | scan | 615 | 615 | 1/1 | 24 | 1.64M | scan | 24/1944 | 0.25× |
| fv_level/pmeta=false/layout=peer/window=cut/filter=none/s3=0ms | 7.0 | 7.4 | 4/4 | 13 | 911K | scan | 0.696 | 0.786 | 4/4 | 2 | 140K | scan | 2/162 | 0.10× |
| fv_level/pmeta=false/layout=peer/window=cut/filter=none/s3=100ms | 1331 | 1331 | 1/1 | 13 | 911K | scan | 102 | 102 | 1/1 | 2 | 140K | scan | 2/162 | 0.08× |
| fv_level/pmeta=false/layout=peer/window=cut/filter=svc/s3=0ms | 14.0 | 20.1 | 4/4 | 13 | 911K | scan | 2.7 | 3.3 | 4/4 | 13 | 911K | scan | 13/1053 | 0.19× |
| fv_level/pmeta=false/layout=peer/window=cut/filter=svc/s3=100ms | 1337 | 1337 | 1/1 | 13 | 911K | scan | 407 | 407 | 1/1 | 13 | 911K | scan | 13/1053 | 0.30× |
| fv_level/pmeta=false/layout=peer/window=edge/filter=none/s3=0ms | 2.7 | 3.0 | 4/4 | 5 | 347K | scan | 0.519 | 0.582 | 4/4 | 1 | 72K | scan | 1/81 | 0.19× |
| fv_level/pmeta=false/layout=peer/window=edge/filter=none/s3=100ms | 517 | 517 | 1/1 | 5 | 347K | scan | 101 | 101 | 1/1 | 1 | 72K | scan | 1/81 | 0.20× |
| fv_level/pmeta=false/layout=peer/window=edge/filter=svc/s3=0ms | 5.2 | 6.6 | 4/4 | 5 | 347K | scan | 1.1 | 1.8 | 4/4 | 5 | 347K | scan | 5/405 | 0.21× |
| fv_level/pmeta=false/layout=peer/window=edge/filter=svc/s3=100ms | 525 | 525 | 1/1 | 5 | 347K | scan | 204 | 204 | 1/1 | 5 | 347K | scan | 5/405 | 0.39× |
| fv_level/pmeta=false/layout=peer/window=narrow/filter=none/s3=0ms | 0.469 | 0.484 | 4/4 | 1 | 72K | scan | 0.455 | 0.506 | 4/4 | 1 | 72K | scan | 1/81 | 0.97× |
| fv_level/pmeta=false/layout=peer/window=narrow/filter=none/s3=100ms | 103 | 103 | 1/1 | 1 | 72K | scan | 101 | 101 | 1/1 | 1 | 72K | scan | 1/81 | 0.98× |
| fv_level/pmeta=false/layout=peer/window=narrow/filter=svc/s3=0ms | 0.558 | 0.586 | 4/4 | 1 | 72K | scan | 0.541 | 0.638 | 4/4 | 1 | 72K | scan | 1/81 | 0.97× |
| fv_level/pmeta=false/layout=peer/window=narrow/filter=svc/s3=100ms | 106 | 106 | 1/1 | 1 | 72K | scan | 101 | 101 | 1/1 | 1 | 72K | scan | 1/81 | 0.95× |
| fv_level/pmeta=false/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 22 | 1.50M | scan | 19.0 | 24.4 | 4/4 | 0 | 0 | ram | 0/0 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=false/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 22 | 1.50M | scan | 15.7 | 15.7 | 1/1 | 0 | 0 | ram | 0/0 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=false/layout=peer/window=whole/filter=svc/s3=0ms | [26.9]† | [33.1]† | 0/4 (set 4) | 22 | 1.50M | scan | 22.1 | 24.0 | 4/4 | 22 | 1.50M | scan | 22/1782 | [0.82×]† |
| fv_level/pmeta=false/layout=peer/window=whole/filter=svc/s3=100ms | [2281]† | [2281]† | 0/1 (set 1) | 22 | 1.50M | scan | 624 | 624 | 1/1 | 22 | 1.50M | scan | 22/1782 | [0.27×]† |
| fv_level/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 3.6 | 4.2 | 4/4 | 18 | 555K | scan | 5/186 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 929 | 929 | 1/1 | 18 | 555K | scan | 5/186 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms | 21.4 | 25.3 | 4/4 | 36 | 627K | scan | 8.6 | 10.1 | 4/4 | 24 | 581K | scan | 5/186 | 0.40× |
| fv_level/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms | 3475 | 3475 | 1/1 | 34 | 619K | scan | 1343 | 1343 | 1/1 | 25 | 583K | scan | 5/186 | 0.39× |
| fv_level/pmeta=true/layout=compacted/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 2.8 | 3.1 | 4/4 | 15 | 543K | scan | 4/178 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=compacted/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 918 | 918 | 1/1 | 15 | 543K | scan | 4/178 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=compacted/window=edge/filter=svc/s3=0ms | 13.6 | 13.8 | 4/4 | 35 | 623K | scan | 5.4 | 6.2 | 4/4 | 21 | 567K | scan | 4/178 | 0.40× |
| fv_level/pmeta=true/layout=compacted/window=edge/filter=svc/s3=100ms | 3464 | 3464 | 1/1 | 34 | 619K | scan | 1316 | 1316 | 1/1 | 20 | 563K | scan | 4/178 | 0.38× |
| fv_level/pmeta=true/layout=compacted/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 2.0 | 2.7 | 4/4 | 6 | 275K | scan | 2/89 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=compacted/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 616 | 616 | 1/1 | 6 | 275K | scan | 2/89 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=0ms | 6.1 | 6.6 | 4/4 | 18 | 323K | scan | 2.8 | 3.1 | 4/4 | 8 | 283K | scan | 2/89 | 0.46× |
| fv_level/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=100ms | 1625 | 1625 | 1/1 | 16 | 315K | scan | 711 | 711 | 1/1 | 7 | 279K | scan | 2/89 | 0.44× |
| fv_level/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms | [0.003]† | [0.003]† | 0/4 (set 4) | 0 | 0 | ram | 0.007 | 0.009 | 4/4 | 0 | 0 | ram | 0/0 | [2.36×]† |
| fv_level/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms | [0.014]† | [0.014]† | 0/1 (set 1) | 0 | 0 | ram | 0.015 | 0.015 | 1/1 | 0 | 0 | ram | 0/0 | [1.04×]† |
| fv_level/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms | 23.5 | 25.2 | 4/4 | 36 | 625K | scan | 12.9 | 16.2 | 4/4 | 36 | 625K | scan | 6/194 | 0.55× |
| fv_level/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms | 3487 | 3487 | 1/1 | 34 | 619K | scan | 1428 | 1428 | 1/1 | 28 | 595K | scan | 6/194 | 0.41× |
| fv_level/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 0.596 | 0.816 | 4/4 | 2 | 140K | scan | 2/162 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 102 | 102 | 1/1 | 2 | 140K | scan | 2/162 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms | 13.1 | 16.0 | 4/4 | 13 | 911K | scan | 3.1 | 3.5 | 4/4 | 13 | 911K | scan | 13/1053 | 0.24× |
| fv_level/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms | 1322 | 1322 | 1/1 | 13 | 911K | scan | 406 | 406 | 1/1 | 13 | 911K | scan | 13/1053 | 0.31× |
| fv_level/pmeta=true/layout=flushed/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 0.576 | 0.733 | 4/4 | 1 | 72K | scan | 1/81 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=flushed/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 103 | 103 | 1/1 | 1 | 72K | scan | 1/81 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=flushed/window=edge/filter=svc/s3=0ms | 4.5 | 5.0 | 4/4 | 5 | 347K | scan | 1.2 | 1.2 | 4/4 | 5 | 347K | scan | 5/405 | 0.26× |
| fv_level/pmeta=true/layout=flushed/window=edge/filter=svc/s3=100ms | 511 | 511 | 1/1 | 5 | 347K | scan | 205 | 205 | 1/1 | 5 | 347K | scan | 5/405 | 0.40× |
| fv_level/pmeta=true/layout=flushed/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 0.485 | 0.686 | 4/4 | 1 | 72K | scan | 1/81 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=flushed/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 103 | 103 | 1/1 | 1 | 72K | scan | 1/81 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=0ms | 0.475 | 0.580 | 4/4 | 1 | 72K | scan | 1.0 | 1.9 | 4/4 | 1 | 72K | scan | 1/81 | 2.19× |
| fv_level/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=100ms | 103 | 103 | 1/1 | 1 | 72K | scan | 101 | 101 | 1/1 | 1 | 72K | scan | 1/81 | 0.98× |
| fv_level/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms | [0.012]† | [0.027]† | 0/4 (set 4) | 0 | 0 | ram | 0.019 | 0.022 | 4/4 | 0 | 0 | ram | 0/0 | [1.59×]† |
| fv_level/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms | [0.022]† | [0.022]† | 0/1 (set 1) | 0 | 0 | ram | 0.081 | 0.081 | 1/1 | 0 | 0 | ram | 0/0 | [3.76×]† |
| fv_level/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms | 23.3 | 27.1 | 4/4 | 24 | 1.64M | scan | 5.2 | 9.7 | 4/4 | 24 | 1.64M | scan | 24/1944 | 0.22× |
| fv_level/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms | 2450 | 2450 | 1/1 | 24 | 1.64M | scan | 607 | 607 | 1/1 | 24 | 1.64M | scan | 24/1944 | 0.25× |
| fv_level/pmeta=true/layout=peer/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 0.582 | 0.693 | 4/4 | 2 | 140K | scan | 2/162 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=peer/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 103 | 103 | 1/1 | 2 | 140K | scan | 2/162 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=peer/window=cut/filter=svc/s3=0ms | 16.2 | 16.7 | 4/4 | 13 | 911K | scan | 2.7 | 3.8 | 4/4 | 13 | 911K | scan | 13/1053 | 0.17× |
| fv_level/pmeta=true/layout=peer/window=cut/filter=svc/s3=100ms | 1332 | 1332 | 1/1 | 13 | 911K | scan | 405 | 405 | 1/1 | 13 | 911K | scan | 13/1053 | 0.30× |
| fv_level/pmeta=true/layout=peer/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 0.503 | 0.535 | 4/4 | 1 | 72K | scan | 1/81 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=peer/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 102 | 102 | 1/1 | 1 | 72K | scan | 1/81 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=peer/window=edge/filter=svc/s3=0ms | 5.5 | 5.8 | 4/4 | 5 | 347K | scan | 1.1 | 1.2 | 4/4 | 5 | 347K | scan | 5/405 | 0.20× |
| fv_level/pmeta=true/layout=peer/window=edge/filter=svc/s3=100ms | 516 | 516 | 1/1 | 5 | 347K | scan | 204 | 204 | 1/1 | 5 | 347K | scan | 5/405 | 0.39× |
| fv_level/pmeta=true/layout=peer/window=narrow/filter=none/s3=0ms | [0.002]† | [0.003]† | 0/4 (set 4) | 0 | 0 | ram | 0.434 | 0.501 | 4/4 | 1 | 72K | scan | 1/81 | [181.03×]† |
| fv_level/pmeta=true/layout=peer/window=narrow/filter=none/s3=100ms | [0.016]† | [0.016]† | 0/1 (set 1) | 0 | 0 | ram | 103 | 103 | 1/1 | 1 | 72K | scan | 1/81 | [6250.41×]† |
| fv_level/pmeta=true/layout=peer/window=narrow/filter=svc/s3=0ms | 0.682 | 0.737 | 4/4 | 1 | 72K | scan | 0.607 | 0.650 | 4/4 | 1 | 72K | scan | 1/81 | 0.89× |
| fv_level/pmeta=true/layout=peer/window=narrow/filter=svc/s3=100ms | 102 | 102 | 1/1 | 1 | 72K | scan | 102 | 102 | 1/1 | 1 | 72K | scan | 1/81 | 0.99× |
| fv_level/pmeta=true/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 18.0 | 21.2 | 4/4 | 0 | 0 | ram | 0/0 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 20.9 | 20.9 | 1/1 | 0 | 0 | ram | 0/0 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=true/layout=peer/window=whole/filter=svc/s3=0ms | [26.8]† | [31.4]† | 0/4 (set 4) | 22 | 1.50M | scan | 22.6 | 23.5 | 4/4 | 22 | 1.50M | scan | 22/1782 | [0.84×]† |
| fv_level/pmeta=true/layout=peer/window=whole/filter=svc/s3=100ms | [2257]† | [2257]† | 0/1 (set 1) | 22 | 1.50M | scan | 626 | 626 | 1/1 | 22 | 1.50M | scan | 22/1782 | [0.28×]† |
| fv_service/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms | 11.5 | 12.7 | 4/4 | 24 | 579K | scan | 3.8 | 4.8 | 4/4 | 18 | 555K | scan | 5/177 | 0.33× |
| fv_service/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms | 2454 | 2454 | 1/1 | 24 | 579K | scan | 920 | 920 | 1/1 | 18 | 555K | scan | 5/177 | 0.37× |
| fv_service/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms | 14.1 | 14.5 | 4/4 | 24 | 579K | scan | 5.8 | 6.5 | 4/4 | 18 | 555K | scan | 5/177 | 0.41× |
| fv_service/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms | 2447 | 2447 | 1/1 | 24 | 579K | scan | 914 | 914 | 1/1 | 18 | 555K | scan | 5/177 | 0.37× |
| fv_service/pmeta=false/layout=compacted/window=edge/filter=none/s3=0ms | 11.1 | 12.7 | 4/4 | 24 | 579K | scan | 3.0 | 3.2 | 4/4 | 15 | 543K | scan | 4/172 | 0.27× |
| fv_service/pmeta=false/layout=compacted/window=edge/filter=none/s3=100ms | 2460 | 2460 | 1/1 | 24 | 579K | scan | 923 | 923 | 1/1 | 15 | 543K | scan | 4/172 | 0.38× |
| fv_service/pmeta=false/layout=compacted/window=edge/filter=svc/s3=0ms | 11.7 | 12.9 | 4/4 | 24 | 579K | scan | 4.8 | 7.2 | 4/4 | 15 | 543K | scan | 4/172 | 0.41× |
| fv_service/pmeta=false/layout=compacted/window=edge/filter=svc/s3=100ms | 2468 | 2468 | 1/1 | 24 | 579K | scan | 917 | 917 | 1/1 | 15 | 543K | scan | 4/172 | 0.37× |
| fv_service/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms | 5.3 | 5.7 | 4/4 | 12 | 299K | scan | 2.0 | 2.4 | 4/4 | 6 | 275K | scan | 2/86 | 0.37× |
| fv_service/pmeta=false/layout=compacted/window=narrow/filter=none/s3=100ms | 1222 | 1222 | 1/1 | 12 | 299K | scan | 609 | 609 | 1/1 | 6 | 275K | scan | 2/86 | 0.50× |
| fv_service/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=0ms | 5.2 | 18.2 | 4/4 | 12 | 299K | scan | 1.8 | 2.1 | 4/4 | 6 | 275K | scan | 2/86 | 0.35× |
| fv_service/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=100ms | 1266 | 1266 | 1/1 | 12 | 299K | scan | 612 | 612 | 1/1 | 6 | 275K | scan | 2/86 | 0.48× |
| fv_service/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms | 9.8 | 11.9 | 4/4 | 18 | 555K | scan | 5.9 | 6.3 | 4/4 | 9 | 287K | scan | 3/87 | 0.60× |
| fv_service/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms | 1841 | 1841 | 1/1 | 18 | 555K | scan | 917 | 917 | 1/1 | 9 | 287K | scan | 3/87 | 0.50× |
| fv_service/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms | 20.0 | 22.4 | 4/4 | 24 | 579K | scan | 8.7 | 10.7 | 4/4 | 24 | 579K | scan | 6/182 | 0.43× |
| fv_service/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms | 2476 | 2476 | 1/1 | 24 | 579K | scan | 1241 | 1241 | 1/1 | 24 | 579K | scan | 6/182 | 0.50× |
| fv_service/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms | 7.7 | 8.4 | 4/4 | 13 | 911K | scan | 1.1 | 1.4 | 4/4 | 8 | 574K | scan | 8/648 | 0.14× |
| fv_service/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms | 1330 | 1330 | 1/1 | 13 | 911K | scan | 206 | 206 | 1/1 | 8 | 574K | scan | 8/648 | 0.16× |
| fv_service/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms | 12.1 | 23.7 | 4/4 | 13 | 911K | scan | 2.7 | 4.3 | 4/4 | 13 | 911K | scan | 13/1053 | 0.22× |
| fv_service/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms | 1330 | 1330 | 1/1 | 13 | 911K | scan | 411 | 411 | 1/1 | 13 | 911K | scan | 13/1053 | 0.31× |
| fv_service/pmeta=false/layout=flushed/window=edge/filter=none/s3=0ms | 2.8 | 2.9 | 4/4 | 5 | 347K | scan | 0.745 | 0.773 | 4/4 | 2 | 145K | scan | 2/162 | 0.27× |
| fv_service/pmeta=false/layout=flushed/window=edge/filter=none/s3=100ms | 508 | 508 | 1/1 | 5 | 347K | scan | 102 | 102 | 1/1 | 2 | 145K | scan | 2/162 | 0.20× |
| fv_service/pmeta=false/layout=flushed/window=edge/filter=svc/s3=0ms | 4.9 | 5.4 | 4/4 | 5 | 347K | scan | 1.7 | 2.6 | 4/4 | 5 | 347K | scan | 5/405 | 0.34× |
| fv_service/pmeta=false/layout=flushed/window=edge/filter=svc/s3=100ms | 521 | 521 | 1/1 | 5 | 347K | scan | 204 | 204 | 1/1 | 5 | 347K | scan | 5/405 | 0.39× |
| fv_service/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms | 0.520 | 0.570 | 4/4 | 1 | 72K | scan | 0.558 | 0.601 | 4/4 | 1 | 72K | scan | 1/81 | 1.07× |
| fv_service/pmeta=false/layout=flushed/window=narrow/filter=none/s3=100ms | 101 | 101 | 1/1 | 1 | 72K | scan | 101 | 101 | 1/1 | 1 | 72K | scan | 1/81 | 1.00× |
| fv_service/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=0ms | 0.599 | 0.639 | 4/4 | 1 | 72K | scan | 0.549 | 0.624 | 4/4 | 1 | 72K | scan | 1/81 | 0.92× |
| fv_service/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=100ms | 102 | 102 | 1/1 | 1 | 72K | scan | 102 | 102 | 1/1 | 1 | 72K | scan | 1/81 | 1.00× |
| fv_service/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms | 13.4 | 14.6 | 4/4 | 24 | 1.64M | scan | 1.7 | 2.2 | 4/4 | 12 | 869K | scan | 12/972 | 0.12× |
| fv_service/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms | 2462 | 2462 | 1/1 | 24 | 1.64M | scan | 305 | 305 | 1/1 | 12 | 869K | scan | 12/972 | 0.12× |
| fv_service/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms | 22.3 | 23.6 | 4/4 | 24 | 1.64M | scan | 4.1 | 5.3 | 4/4 | 24 | 1.64M | scan | 24/1944 | 0.18× |
| fv_service/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms | 2459 | 2459 | 1/1 | 24 | 1.64M | scan | 610 | 610 | 1/1 | 24 | 1.64M | scan | 24/1944 | 0.25× |
| fv_service/pmeta=false/layout=peer/window=cut/filter=none/s3=0ms | 13.6 | 20.2 | 4/4 | 13 | 911K | scan | 1.1 | 1.9 | 4/4 | 8 | 574K | scan | 8/648 | 0.08× |
| fv_service/pmeta=false/layout=peer/window=cut/filter=none/s3=100ms | 1323 | 1323 | 1/1 | 13 | 911K | scan | 204 | 204 | 1/1 | 8 | 574K | scan | 8/648 | 0.15× |
| fv_service/pmeta=false/layout=peer/window=cut/filter=svc/s3=0ms | 14.7 | 17.5 | 4/4 | 13 | 911K | scan | 2.5 | 4.5 | 4/4 | 13 | 911K | scan | 13/1053 | 0.17× |
| fv_service/pmeta=false/layout=peer/window=cut/filter=svc/s3=100ms | 1326 | 1326 | 1/1 | 13 | 911K | scan | 407 | 407 | 1/1 | 13 | 911K | scan | 13/1053 | 0.31× |
| fv_service/pmeta=false/layout=peer/window=edge/filter=none/s3=0ms | 4.4 | 4.6 | 4/4 | 5 | 347K | scan | 0.921 | 2.0 | 4/4 | 2 | 145K | scan | 2/162 | 0.21× |
| fv_service/pmeta=false/layout=peer/window=edge/filter=none/s3=100ms | 509 | 509 | 1/1 | 5 | 347K | scan | 103 | 103 | 1/1 | 2 | 145K | scan | 2/162 | 0.20× |
| fv_service/pmeta=false/layout=peer/window=edge/filter=svc/s3=0ms | 6.1 | 7.7 | 4/4 | 5 | 347K | scan | 1.1 | 1.2 | 4/4 | 5 | 347K | scan | 5/405 | 0.18× |
| fv_service/pmeta=false/layout=peer/window=edge/filter=svc/s3=100ms | 512 | 512 | 1/1 | 5 | 347K | scan | 202 | 202 | 1/1 | 5 | 347K | scan | 5/405 | 0.39× |
| fv_service/pmeta=false/layout=peer/window=narrow/filter=none/s3=0ms | 0.693 | 1.2 | 4/4 | 1 | 72K | scan | 0.506 | 0.592 | 4/4 | 1 | 72K | scan | 1/81 | 0.73× |
| fv_service/pmeta=false/layout=peer/window=narrow/filter=none/s3=100ms | 102 | 102 | 1/1 | 1 | 72K | scan | 102 | 102 | 1/1 | 1 | 72K | scan | 1/81 | 1.00× |
| fv_service/pmeta=false/layout=peer/window=narrow/filter=svc/s3=0ms | 0.839 | 1.2 | 4/4 | 1 | 72K | scan | 0.494 | 0.941 | 4/4 | 1 | 72K | scan | 1/81 | 0.59× |
| fv_service/pmeta=false/layout=peer/window=narrow/filter=svc/s3=100ms | 102 | 102 | 1/1 | 1 | 72K | scan | 103 | 103 | 1/1 | 1 | 72K | scan | 1/81 | 1.01× |
| fv_service/pmeta=false/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 22 | 1.50M | scan | 19.3 | 22.5 | 4/4 | 10 | 724K | scan | 10/810 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=false/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 22 | 1.50M | scan | 327 | 327 | 1/1 | 10 | 724K | scan | 10/810 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=false/layout=peer/window=whole/filter=svc/s3=0ms | [52.9]† | [88.6]† | 0/4 (set 4) | 22 | 1.50M | scan | 25.3 | 27.1 | 4/4 | 22 | 1.50M | scan | 22/1782 | [0.48×]† |
| fv_service/pmeta=false/layout=peer/window=whole/filter=svc/s3=100ms | [2272]† | [2272]† | 0/1 (set 1) | 22 | 1.50M | scan | 623 | 623 | 1/1 | 22 | 1.50M | scan | 22/1782 | [0.27×]† |
| fv_service/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms | 10.3 | 18.9 | 4/4 | 24 | 579K | scan | 3.6 | 4.1 | 4/4 | 18 | 555K | scan | 5/177 | 0.35× |
| fv_service/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms | 2438 | 2438 | 1/1 | 24 | 579K | scan | 923 | 923 | 1/1 | 18 | 555K | scan | 5/177 | 0.38× |
| fv_service/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms | 14.9 | 29.4 | 4/4 | 24 | 579K | scan | 5.0 | 6.1 | 4/4 | 18 | 555K | scan | 5/177 | 0.33× |
| fv_service/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms | 2454 | 2454 | 1/1 | 24 | 579K | scan | 922 | 922 | 1/1 | 18 | 555K | scan | 5/177 | 0.38× |
| fv_service/pmeta=true/layout=compacted/window=edge/filter=none/s3=0ms | 12.9 | 31.0 | 4/4 | 24 | 579K | scan | 2.9 | 3.8 | 4/4 | 15 | 543K | scan | 4/172 | 0.23× |
| fv_service/pmeta=true/layout=compacted/window=edge/filter=none/s3=100ms | 2449 | 2449 | 1/1 | 24 | 579K | scan | 924 | 924 | 1/1 | 15 | 543K | scan | 4/172 | 0.38× |
| fv_service/pmeta=true/layout=compacted/window=edge/filter=svc/s3=0ms | 11.6 | 47.1 | 4/4 | 24 | 579K | scan | 3.8 | 10.6 | 4/4 | 15 | 543K | scan | 4/172 | 0.33× |
| fv_service/pmeta=true/layout=compacted/window=edge/filter=svc/s3=100ms | 2450 | 2450 | 1/1 | 24 | 579K | scan | 917 | 917 | 1/1 | 15 | 543K | scan | 4/172 | 0.37× |
| fv_service/pmeta=true/layout=compacted/window=narrow/filter=none/s3=0ms | 3.8 | 4.0 | 4/4 | 12 | 299K | scan | 1.9 | 2.0 | 4/4 | 6 | 275K | scan | 2/86 | 0.49× |
| fv_service/pmeta=true/layout=compacted/window=narrow/filter=none/s3=100ms | 1230 | 1230 | 1/1 | 12 | 299K | scan | 614 | 614 | 1/1 | 6 | 275K | scan | 2/86 | 0.50× |
| fv_service/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=0ms | 4.9 | 6.6 | 4/4 | 12 | 299K | scan | 2.1 | 2.5 | 4/4 | 6 | 275K | scan | 2/86 | 0.44× |
| fv_service/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=100ms | 1227 | 1227 | 1/1 | 12 | 299K | scan | 614 | 614 | 1/1 | 6 | 275K | scan | 2/86 | 0.50× |
| fv_service/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms | 9.7 | 11.5 | 4/4 | 18 | 555K | scan | 4.5 | 4.7 | 4/4 | 9 | 287K | scan | 3/87 | 0.47× |
| fv_service/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms | 1830 | 1830 | 1/1 | 18 | 555K | scan | 920 | 920 | 1/1 | 9 | 287K | scan | 3/87 | 0.50× |
| fv_service/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms | 18.4 | 20.2 | 4/4 | 24 | 579K | scan | 8.2 | 8.8 | 4/4 | 24 | 579K | scan | 6/182 | 0.44× |
| fv_service/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms | 2445 | 2445 | 1/1 | 24 | 579K | scan | 1230 | 1230 | 1/1 | 24 | 579K | scan | 6/182 | 0.50× |
| fv_service/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms | 7.2 | 7.4 | 4/4 | 13 | 911K | scan | 1.0 | 1.1 | 4/4 | 8 | 574K | scan | 8/648 | 0.14× |
| fv_service/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms | 1349 | 1349 | 1/1 | 13 | 911K | scan | 206 | 206 | 1/1 | 8 | 574K | scan | 8/648 | 0.15× |
| fv_service/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms | 11.1 | 11.8 | 4/4 | 13 | 911K | scan | 2.0 | 3.1 | 4/4 | 13 | 911K | scan | 13/1053 | 0.18× |
| fv_service/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms | 1350 | 1350 | 1/1 | 13 | 911K | scan | 411 | 411 | 1/1 | 13 | 911K | scan | 13/1053 | 0.30× |
| fv_service/pmeta=true/layout=flushed/window=edge/filter=none/s3=0ms | 2.9 | 3.1 | 4/4 | 5 | 347K | scan | 0.675 | 4.1 | 4/4 | 2 | 145K | scan | 2/162 | 0.23× |
| fv_service/pmeta=true/layout=flushed/window=edge/filter=none/s3=100ms | 517 | 517 | 1/1 | 5 | 347K | scan | 101 | 101 | 1/1 | 2 | 145K | scan | 2/162 | 0.20× |
| fv_service/pmeta=true/layout=flushed/window=edge/filter=svc/s3=0ms | 4.2 | 4.8 | 4/4 | 5 | 347K | scan | 1.7 | 2.1 | 4/4 | 5 | 347K | scan | 5/405 | 0.41× |
| fv_service/pmeta=true/layout=flushed/window=edge/filter=svc/s3=100ms | 517 | 517 | 1/1 | 5 | 347K | scan | 204 | 204 | 1/1 | 5 | 347K | scan | 5/405 | 0.39× |
| fv_service/pmeta=true/layout=flushed/window=narrow/filter=none/s3=0ms | 0.579 | 1.2 | 4/4 | 1 | 72K | scan | 0.705 | 1.3 | 4/4 | 1 | 72K | scan | 1/81 | 1.22× |
| fv_service/pmeta=true/layout=flushed/window=narrow/filter=none/s3=100ms | 102 | 102 | 1/1 | 1 | 72K | scan | 104 | 104 | 1/1 | 1 | 72K | scan | 1/81 | 1.02× |
| fv_service/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=0ms | 0.496 | 0.556 | 4/4 | 1 | 72K | scan | 0.506 | 0.586 | 4/4 | 1 | 72K | scan | 1/81 | 1.02× |
| fv_service/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=100ms | 104 | 104 | 1/1 | 1 | 72K | scan | 103 | 103 | 1/1 | 1 | 72K | scan | 1/81 | 0.99× |
| fv_service/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms | 15.4 | 24.4 | 4/4 | 24 | 1.64M | scan | 1.6 | 2.3 | 4/4 | 12 | 869K | scan | 12/972 | 0.10× |
| fv_service/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms | 2520 | 2520 | 1/1 | 24 | 1.64M | scan | 307 | 307 | 1/1 | 12 | 869K | scan | 12/972 | 0.12× |
| fv_service/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms | 25.9 | 31.3 | 4/4 | 24 | 1.64M | scan | 3.8 | 4.9 | 4/4 | 24 | 1.64M | scan | 24/1944 | 0.15× |
| fv_service/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms | 2502 | 2502 | 1/1 | 24 | 1.64M | scan | 612 | 612 | 1/1 | 24 | 1.64M | scan | 24/1944 | 0.24× |
| fv_service/pmeta=true/layout=peer/window=cut/filter=none/s3=0ms | 13.3 | 38.4 | 4/4 | 13 | 911K | scan | 1.7 | 1.8 | 4/4 | 8 | 574K | scan | 8/648 | 0.13× |
| fv_service/pmeta=true/layout=peer/window=cut/filter=none/s3=100ms | 1321 | 1321 | 1/1 | 13 | 911K | scan | 203 | 203 | 1/1 | 8 | 574K | scan | 8/648 | 0.15× |
| fv_service/pmeta=true/layout=peer/window=cut/filter=svc/s3=0ms | 13.3 | 18.4 | 4/4 | 13 | 911K | scan | 2.8 | 3.9 | 4/4 | 13 | 911K | scan | 13/1053 | 0.21× |
| fv_service/pmeta=true/layout=peer/window=cut/filter=svc/s3=100ms | 1340 | 1340 | 1/1 | 13 | 911K | scan | 408 | 408 | 1/1 | 13 | 911K | scan | 13/1053 | 0.30× |
| fv_service/pmeta=true/layout=peer/window=edge/filter=none/s3=0ms | 3.2 | 3.7 | 4/4 | 5 | 347K | scan | 0.727 | 0.858 | 4/4 | 2 | 145K | scan | 2/162 | 0.23× |
| fv_service/pmeta=true/layout=peer/window=edge/filter=none/s3=100ms | 530 | 530 | 1/1 | 5 | 347K | scan | 103 | 103 | 1/1 | 2 | 145K | scan | 2/162 | 0.19× |
| fv_service/pmeta=true/layout=peer/window=edge/filter=svc/s3=0ms | 5.8 | 7.9 | 4/4 | 5 | 347K | scan | 1.2 | 2.7 | 4/4 | 5 | 347K | scan | 5/405 | 0.21× |
| fv_service/pmeta=true/layout=peer/window=edge/filter=svc/s3=100ms | 533 | 533 | 1/1 | 5 | 347K | scan | 203 | 203 | 1/1 | 5 | 347K | scan | 5/405 | 0.38× |
| fv_service/pmeta=true/layout=peer/window=narrow/filter=none/s3=0ms | 0.647 | 0.766 | 4/4 | 1 | 72K | scan | 0.587 | 0.664 | 4/4 | 1 | 72K | scan | 1/81 | 0.91× |
| fv_service/pmeta=true/layout=peer/window=narrow/filter=none/s3=100ms | 102 | 102 | 1/1 | 1 | 72K | scan | 101 | 101 | 1/1 | 1 | 72K | scan | 1/81 | 0.99× |
| fv_service/pmeta=true/layout=peer/window=narrow/filter=svc/s3=0ms | 0.652 | 0.745 | 4/4 | 1 | 72K | scan | 0.515 | 0.588 | 4/4 | 1 | 72K | scan | 1/81 | 0.79× |
| fv_service/pmeta=true/layout=peer/window=narrow/filter=svc/s3=100ms | 102 | 102 | 1/1 | 1 | 72K | scan | 102 | 102 | 1/1 | 1 | 72K | scan | 1/81 | 1.00× |
| fv_service/pmeta=true/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 22 | 1.50M | scan | 20.0 | 21.3 | 4/4 | 10 | 724K | scan | 10/810 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=true/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 22 | 1.50M | scan | 321 | 321 | 1/1 | 10 | 724K | scan | 10/810 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=true/layout=peer/window=whole/filter=svc/s3=0ms | [27.1]† | [36.3]† | 0/4 (set 4) | 22 | 1.50M | scan | 28.0 | 37.0 | 4/4 | 22 | 1.50M | scan | 22/1782 | [1.03×]† |
| fv_service/pmeta=true/layout=peer/window=whole/filter=svc/s3=100ms | [2245]† | [2245]† | 0/1 (set 1) | 22 | 1.50M | scan | 624 | 624 | 1/1 | 22 | 1.50M | scan | 22/1782 | [0.28×]† |
| field_names/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 2 | 169K | scan | invalid | invalid | 0/4 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 2 | 169K | scan | invalid | invalid | 0/1 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/4 | 2 | 169K | scan | invalid | invalid | 0/4 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/1 | 2 | 169K | scan | invalid | invalid | 0/1 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 2 | 169K | scan | invalid | invalid | 0/4 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 2 | 169K | scan | invalid | invalid | 0/1 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=edge/filter=svc/s3=0ms | invalid | invalid | 0/4 | 2 | 169K | scan | invalid | invalid | 0/4 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=edge/filter=svc/s3=100ms | invalid | invalid | 0/1 | 2 | 169K | scan | invalid | invalid | 0/1 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 1 | 88K | scan | invalid | invalid | 0/4 | 1 | 88K | scan | 1/72 | after invalid |
| field_names/pmeta=false/layout=compacted/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 1 | 88K | scan | invalid | invalid | 0/1 | 1 | 88K | scan | 1/72 | after invalid |
| field_names/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 88K | scan | invalid | invalid | 0/4 | 1 | 88K | scan | 1/72 | after invalid |
| field_names/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 88K | scan | invalid | invalid | 0/1 | 1 | 88K | scan | 1/72 | after invalid |
| field_names/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 2 | 169K | scan | invalid | invalid | 0/4 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 2 | 169K | scan | invalid | invalid | 0/1 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/4 | 2 | 169K | scan | invalid | invalid | 0/4 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/1 | 2 | 169K | scan | invalid | invalid | 0/1 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 13 | 911K | scan | invalid | invalid | 0/4 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 13 | 911K | scan | invalid | invalid | 0/1 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/4 | 13 | 911K | scan | invalid | invalid | 0/4 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/1 | 13 | 911K | scan | invalid | invalid | 0/1 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=false/layout=flushed/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 5 | 347K | scan | invalid | invalid | 0/4 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=false/layout=flushed/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 5 | 347K | scan | invalid | invalid | 0/1 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=false/layout=flushed/window=edge/filter=svc/s3=0ms | invalid | invalid | 0/4 | 5 | 347K | scan | invalid | invalid | 0/4 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=false/layout=flushed/window=edge/filter=svc/s3=100ms | invalid | invalid | 0/1 | 5 | 347K | scan | invalid | invalid | 0/1 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 1 | 72K | scan | invalid | invalid | 0/4 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=false/layout=flushed/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 1 | 72K | scan | invalid | invalid | 0/1 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 72K | scan | invalid | invalid | 0/4 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 72K | scan | invalid | invalid | 0/1 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 24 | 1.64M | scan | invalid | invalid | 0/4 | 24 | 1.64M | scan | 24/1944 | after invalid |
| field_names/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 24 | 1.64M | scan | invalid | invalid | 0/1 | 24 | 1.64M | scan | 24/1944 | after invalid |
| field_names/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/4 | 24 | 1.64M | scan | invalid | invalid | 0/4 | 24 | 1.64M | scan | 24/1944 | after invalid |
| field_names/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/1 | 24 | 1.64M | scan | invalid | invalid | 0/1 | 24 | 1.64M | scan | 24/1944 | after invalid |
| field_names/pmeta=false/layout=peer/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 13 | 911K | scan | invalid | invalid | 0/4 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=false/layout=peer/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 13 | 911K | scan | invalid | invalid | 0/1 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=false/layout=peer/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/4 | 13 | 911K | scan | invalid | invalid | 0/4 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=false/layout=peer/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/1 | 13 | 911K | scan | invalid | invalid | 0/1 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=false/layout=peer/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 5 | 347K | scan | invalid | invalid | 0/4 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=false/layout=peer/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 5 | 347K | scan | invalid | invalid | 0/1 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=false/layout=peer/window=edge/filter=svc/s3=0ms | invalid | invalid | 0/4 | 5 | 347K | scan | invalid | invalid | 0/4 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=false/layout=peer/window=edge/filter=svc/s3=100ms | invalid | invalid | 0/1 | 5 | 347K | scan | invalid | invalid | 0/1 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=false/layout=peer/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 1 | 72K | scan | invalid | invalid | 0/4 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=false/layout=peer/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 1 | 72K | scan | invalid | invalid | 0/1 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=false/layout=peer/window=narrow/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 72K | scan | invalid | invalid | 0/4 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=false/layout=peer/window=narrow/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 72K | scan | invalid | invalid | 0/1 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=false/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 22 | 1.50M | scan | invalid | invalid | 0/4 | 22 | 1.50M | scan | 22/1782 | after invalid |
| field_names/pmeta=false/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 22 | 1.50M | scan | invalid | invalid | 0/1 | 22 | 1.50M | scan | 22/1782 | after invalid |
| field_names/pmeta=false/layout=peer/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/4 | 22 | 1.50M | scan | invalid | invalid | 0/4 | 22 | 1.50M | scan | 22/1782 | after invalid |
| field_names/pmeta=false/layout=peer/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/1 | 22 | 1.50M | scan | invalid | invalid | 0/1 | 22 | 1.50M | scan | 22/1782 | after invalid |
| field_names/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 2 | 169K | scan | invalid | invalid | 0/4 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 2 | 169K | scan | invalid | invalid | 0/1 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/4 | 2 | 169K | scan | invalid | invalid | 0/4 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/1 | 2 | 169K | scan | invalid | invalid | 0/1 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 2 | 169K | scan | invalid | invalid | 0/4 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 2 | 169K | scan | invalid | invalid | 0/1 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=edge/filter=svc/s3=0ms | invalid | invalid | 0/4 | 2 | 169K | scan | invalid | invalid | 0/4 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=edge/filter=svc/s3=100ms | invalid | invalid | 0/1 | 2 | 169K | scan | invalid | invalid | 0/1 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 1 | 88K | scan | invalid | invalid | 0/4 | 1 | 88K | scan | 1/72 | after invalid |
| field_names/pmeta=true/layout=compacted/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 1 | 88K | scan | invalid | invalid | 0/1 | 1 | 88K | scan | 1/72 | after invalid |
| field_names/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 88K | scan | invalid | invalid | 0/4 | 1 | 88K | scan | 1/72 | after invalid |
| field_names/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 88K | scan | invalid | invalid | 0/1 | 1 | 88K | scan | 1/72 | after invalid |
| field_names/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 2 | 169K | scan | invalid | invalid | 0/4 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 2 | 169K | scan | invalid | invalid | 0/1 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/4 | 2 | 169K | scan | invalid | invalid | 0/4 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/1 | 2 | 169K | scan | invalid | invalid | 0/1 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 13 | 911K | scan | invalid | invalid | 0/4 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 13 | 911K | scan | invalid | invalid | 0/1 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/4 | 13 | 911K | scan | invalid | invalid | 0/4 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/1 | 13 | 911K | scan | invalid | invalid | 0/1 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=true/layout=flushed/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 5 | 347K | scan | invalid | invalid | 0/4 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=true/layout=flushed/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 5 | 347K | scan | invalid | invalid | 0/1 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=true/layout=flushed/window=edge/filter=svc/s3=0ms | invalid | invalid | 0/4 | 5 | 347K | scan | invalid | invalid | 0/4 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=true/layout=flushed/window=edge/filter=svc/s3=100ms | invalid | invalid | 0/1 | 5 | 347K | scan | invalid | invalid | 0/1 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=true/layout=flushed/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 1 | 72K | scan | invalid | invalid | 0/4 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=true/layout=flushed/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 1 | 72K | scan | invalid | invalid | 0/1 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 72K | scan | invalid | invalid | 0/4 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 72K | scan | invalid | invalid | 0/1 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 24 | 1.64M | scan | invalid | invalid | 0/4 | 24 | 1.64M | scan | 24/1944 | after invalid |
| field_names/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 24 | 1.64M | scan | invalid | invalid | 0/1 | 24 | 1.64M | scan | 24/1944 | after invalid |
| field_names/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/4 | 24 | 1.64M | scan | invalid | invalid | 0/4 | 24 | 1.64M | scan | 24/1944 | after invalid |
| field_names/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/1 | 24 | 1.64M | scan | invalid | invalid | 0/1 | 24 | 1.64M | scan | 24/1944 | after invalid |
| field_names/pmeta=true/layout=peer/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 13 | 911K | scan | invalid | invalid | 0/4 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=true/layout=peer/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 13 | 911K | scan | invalid | invalid | 0/1 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=true/layout=peer/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/4 | 13 | 911K | scan | invalid | invalid | 0/4 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=true/layout=peer/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/1 | 13 | 911K | scan | invalid | invalid | 0/1 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=true/layout=peer/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 5 | 347K | scan | invalid | invalid | 0/4 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=true/layout=peer/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 5 | 347K | scan | invalid | invalid | 0/1 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=true/layout=peer/window=edge/filter=svc/s3=0ms | invalid | invalid | 0/4 | 5 | 347K | scan | invalid | invalid | 0/4 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=true/layout=peer/window=edge/filter=svc/s3=100ms | invalid | invalid | 0/1 | 5 | 347K | scan | invalid | invalid | 0/1 | 5 | 347K | scan | 5/405 | after invalid |
| field_names/pmeta=true/layout=peer/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 1 | 72K | scan | invalid | invalid | 0/4 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=true/layout=peer/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 1 | 72K | scan | invalid | invalid | 0/1 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=true/layout=peer/window=narrow/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 72K | scan | invalid | invalid | 0/4 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=true/layout=peer/window=narrow/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 72K | scan | invalid | invalid | 0/1 | 1 | 72K | scan | 1/81 | after invalid |
| field_names/pmeta=true/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 22 | 1.50M | scan | invalid | invalid | 0/4 | 22 | 1.50M | scan | 22/1782 | after invalid |
| field_names/pmeta=true/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 22 | 1.50M | scan | invalid | invalid | 0/1 | 22 | 1.50M | scan | 22/1782 | after invalid |
| field_names/pmeta=true/layout=peer/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/4 | 22 | 1.50M | scan | invalid | invalid | 0/4 | 22 | 1.50M | scan | 22/1782 | after invalid |
| field_names/pmeta=true/layout=peer/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/1 | 22 | 1.50M | scan | invalid | invalid | 0/1 | 22 | 1.50M | scan | 22/1782 | after invalid |
| streams/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms | 10.7 | 12.4 | 4/4 | 24 | 579K | scan | 6.4 | 9.5 | 4/4 | 18 | 555K | scan | 5/230 | 0.60× |
| streams/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms | 2447 | 2447 | 1/1 | 24 | 579K | scan | 917 | 917 | 1/1 | 18 | 555K | scan | 5/230 | 0.37× |
| streams/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms | 18.3 | 21.4 | 4/4 | 36 | 627K | scan | 9.5 | 12.8 | 4/4 | 26 | 587K | scan | 5/239 | 0.52× |
| streams/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms | 3662 | 3662 | 1/1 | 36 | 627K | scan | 1320 | 1320 | 1/1 | 26 | 587K | scan | 5/239 | 0.36× |
| streams/pmeta=false/layout=compacted/window=edge/filter=none/s3=0ms | 9.3 | 13.5 | 4/4 | 24 | 579K | scan | 3.6 | 4.1 | 4/4 | 15 | 543K | scan | 4/225 | 0.39× |
| streams/pmeta=false/layout=compacted/window=edge/filter=none/s3=100ms | 2445 | 2445 | 1/1 | 24 | 579K | scan | 913 | 913 | 1/1 | 15 | 543K | scan | 4/225 | 0.37× |
| streams/pmeta=false/layout=compacted/window=edge/filter=svc/s3=0ms | 13.4 | 15.4 | 4/4 | 36 | 627K | scan | 7.3 | 9.7 | 4/4 | 21 | 567K | scan | 4/231 | 0.54× |
| streams/pmeta=false/layout=compacted/window=edge/filter=svc/s3=100ms | 3689 | 3689 | 1/1 | 36 | 627K | scan | 1320 | 1320 | 1/1 | 21 | 567K | scan | 4/231 | 0.36× |
| streams/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms | 3.8 | 4.4 | 4/4 | 12 | 299K | scan | 2.6 | 3.5 | 4/4 | 6 | 275K | scan | 2/86 | 0.70× |
| streams/pmeta=false/layout=compacted/window=narrow/filter=none/s3=100ms | 1218 | 1218 | 1/1 | 12 | 299K | scan | 611 | 611 | 1/1 | 6 | 275K | scan | 2/86 | 0.50× |
| streams/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=0ms | 5.6 | 6.5 | 4/4 | 18 | 323K | scan | 3.1 | 5.6 | 4/4 | 8 | 283K | scan | 2/89 | 0.56× |
| streams/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=100ms | 1832 | 1832 | 1/1 | 18 | 323K | scan | 814 | 814 | 1/1 | 8 | 283K | scan | 2/89 | 0.44× |
| streams/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms | 8.5 | 10.1 | 4/4 | 18 | 555K | scan | 5.4 | 10.0 | 4/4 | 18 | 555K | scan | 6/280 | 0.63× |
| streams/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms | 1840 | 1840 | 1/1 | 18 | 555K | scan | 916 | 916 | 1/1 | 18 | 555K | scan | 6/280 | 0.50× |
| streams/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms | 24.5 | 26.8 | 4/4 | 36 | 627K | scan | 23.3 | 32.7 | 4/4 | 36 | 627K | scan | 6/300 | 0.95× |
| streams/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms | 3695 | 3695 | 1/1 | 36 | 627K | scan | 1843 | 1843 | 1/1 | 36 | 627K | scan | 6/300 | 0.50× |
| streams/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms | 7.4 | 7.9 | 4/4 | 13 | 911K | scan | 1.7 | 4.2 | 4/4 | 13 | 911K | scan | 13/1053 | 0.23× |
| streams/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms | 1335 | 1335 | 1/1 | 13 | 911K | scan | 404 | 404 | 1/1 | 13 | 911K | scan | 13/1053 | 0.30× |
| streams/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms | 12.9 | 13.3 | 4/4 | 13 | 911K | scan | 3.1 | 4.8 | 4/4 | 13 | 911K | scan | 13/1053 | 0.24× |
| streams/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms | 1330 | 1330 | 1/1 | 13 | 911K | scan | 414 | 414 | 1/1 | 13 | 911K | scan | 13/1053 | 0.31× |
| streams/pmeta=false/layout=flushed/window=edge/filter=none/s3=0ms | 2.9 | 3.4 | 4/4 | 5 | 347K | scan | 0.881 | 1.4 | 4/4 | 5 | 347K | scan | 5/405 | 0.30× |
| streams/pmeta=false/layout=flushed/window=edge/filter=none/s3=100ms | 511 | 511 | 1/1 | 5 | 347K | scan | 203 | 203 | 1/1 | 5 | 347K | scan | 5/405 | 0.40× |
| streams/pmeta=false/layout=flushed/window=edge/filter=svc/s3=0ms | 5.2 | 8.9 | 4/4 | 5 | 347K | scan | 1.4 | 2.4 | 4/4 | 5 | 347K | scan | 5/405 | 0.28× |
| streams/pmeta=false/layout=flushed/window=edge/filter=svc/s3=100ms | 511 | 511 | 1/1 | 5 | 347K | scan | 207 | 207 | 1/1 | 5 | 347K | scan | 5/405 | 0.40× |
| streams/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms | 0.466 | 0.724 | 4/4 | 1 | 72K | scan | 0.659 | 0.860 | 4/4 | 1 | 72K | scan | 1/81 | 1.41× |
| streams/pmeta=false/layout=flushed/window=narrow/filter=none/s3=100ms | 101 | 101 | 1/1 | 1 | 72K | scan | 101 | 101 | 1/1 | 1 | 72K | scan | 1/81 | 1.00× |
| streams/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=0ms | 0.603 | 0.685 | 4/4 | 1 | 72K | scan | 0.654 | 0.706 | 4/4 | 1 | 72K | scan | 1/81 | 1.08× |
| streams/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=100ms | 102 | 102 | 1/1 | 1 | 72K | scan | 103 | 103 | 1/1 | 1 | 72K | scan | 1/81 | 1.01× |
| streams/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms | 13.8 | 25.2 | 4/4 | 24 | 1.64M | scan | 3.1 | 4.3 | 4/4 | 24 | 1.64M | scan | 24/1944 | 0.22× |
| streams/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms | 2470 | 2470 | 1/1 | 24 | 1.64M | scan | 609 | 609 | 1/1 | 24 | 1.64M | scan | 24/1944 | 0.25× |
| streams/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms | 25.6 | 34.2 | 4/4 | 24 | 1.64M | scan | 4.3 | 5.2 | 4/4 | 24 | 1.64M | scan | 24/1944 | 0.17× |
| streams/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms | 2500 | 2500 | 1/1 | 24 | 1.64M | scan | 611 | 611 | 1/1 | 24 | 1.64M | scan | 24/1944 | 0.24× |
| streams/pmeta=false/layout=peer/window=cut/filter=none/s3=0ms | 7.9 | 13.5 | 4/4 | 13 | 911K | scan | 2.2 | 3.1 | 4/4 | 13 | 911K | scan | 13/1053 | 0.28× |
| streams/pmeta=false/layout=peer/window=cut/filter=none/s3=100ms | 1331 | 1331 | 1/1 | 13 | 911K | scan | 409 | 409 | 1/1 | 13 | 911K | scan | 13/1053 | 0.31× |
| streams/pmeta=false/layout=peer/window=cut/filter=svc/s3=0ms | 12.4 | 13.8 | 4/4 | 13 | 911K | scan | 4.0 | 7.3 | 4/4 | 13 | 911K | scan | 13/1053 | 0.32× |
| streams/pmeta=false/layout=peer/window=cut/filter=svc/s3=100ms | 1337 | 1337 | 1/1 | 13 | 911K | scan | 412 | 412 | 1/1 | 13 | 911K | scan | 13/1053 | 0.31× |
| streams/pmeta=false/layout=peer/window=edge/filter=none/s3=0ms | 2.4 | 3.1 | 4/4 | 5 | 347K | scan | 1.1 | 11.2 | 4/4 | 5 | 347K | scan | 5/405 | 0.44× |
| streams/pmeta=false/layout=peer/window=edge/filter=none/s3=100ms | 530 | 530 | 1/1 | 5 | 347K | scan | 202 | 202 | 1/1 | 5 | 347K | scan | 5/405 | 0.38× |
| streams/pmeta=false/layout=peer/window=edge/filter=svc/s3=0ms | 4.7 | 5.6 | 4/4 | 5 | 347K | scan | 2.5 | 4.2 | 4/4 | 5 | 347K | scan | 5/405 | 0.53× |
| streams/pmeta=false/layout=peer/window=edge/filter=svc/s3=100ms | 518 | 518 | 1/1 | 5 | 347K | scan | 202 | 202 | 1/1 | 5 | 347K | scan | 5/405 | 0.39× |
| streams/pmeta=false/layout=peer/window=narrow/filter=none/s3=0ms | 0.416 | 0.533 | 4/4 | 1 | 72K | scan | 0.684 | 0.841 | 4/4 | 1 | 72K | scan | 1/81 | 1.65× |
| streams/pmeta=false/layout=peer/window=narrow/filter=none/s3=100ms | 102 | 102 | 1/1 | 1 | 72K | scan | 103 | 103 | 1/1 | 1 | 72K | scan | 1/81 | 1.01× |
| streams/pmeta=false/layout=peer/window=narrow/filter=svc/s3=0ms | 0.486 | 0.650 | 4/4 | 1 | 72K | scan | 0.765 | 1.2 | 4/4 | 1 | 72K | scan | 1/81 | 1.58× |
| streams/pmeta=false/layout=peer/window=narrow/filter=svc/s3=100ms | 102 | 102 | 1/1 | 1 | 72K | scan | 102 | 102 | 1/1 | 1 | 72K | scan | 1/81 | 1.00× |
| streams/pmeta=false/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 22 | 1.50M | scan | 23.1 | 30.0 | 4/4 | 22 | 1.50M | scan | 22/1782 | before invalid (fast-wrong) → now exact |
| streams/pmeta=false/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 22 | 1.50M | scan | 633 | 633 | 1/1 | 22 | 1.50M | scan | 22/1782 | before invalid (fast-wrong) → now exact |
| streams/pmeta=false/layout=peer/window=whole/filter=svc/s3=0ms | [21.5]† | [27.8]† | 0/4 (set 4) | 22 | 1.50M | scan | 32.0 | 40.2 | 4/4 | 22 | 1.50M | scan | 22/1782 | [1.49×]† |
| streams/pmeta=false/layout=peer/window=whole/filter=svc/s3=100ms | [2261]† | [2261]† | 0/1 (set 1) | 22 | 1.50M | scan | 624 | 624 | 1/1 | 22 | 1.50M | scan | 22/1782 | [0.28×]† |
| streams/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms | 10.0 | 10.7 | 4/4 | 24 | 579K | scan | 4.8 | 7.0 | 4/4 | 18 | 555K | scan | 5/230 | 0.48× |
| streams/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms | 2456 | 2456 | 1/1 | 24 | 579K | scan | 910 | 910 | 1/1 | 18 | 555K | scan | 5/230 | 0.37× |
| streams/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms | 18.5 | 22.8 | 4/4 | 36 | 627K | scan | 9.8 | 16.0 | 4/4 | 26 | 587K | scan | 5/239 | 0.53× |
| streams/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms | 3674 | 3674 | 1/1 | 36 | 627K | scan | 1326 | 1326 | 1/1 | 26 | 587K | scan | 5/239 | 0.36× |
| streams/pmeta=true/layout=compacted/window=edge/filter=none/s3=0ms | 9.2 | 9.5 | 4/4 | 24 | 579K | scan | 3.7 | 6.1 | 4/4 | 15 | 543K | scan | 4/225 | 0.40× |
| streams/pmeta=true/layout=compacted/window=edge/filter=none/s3=100ms | 2461 | 2461 | 1/1 | 24 | 579K | scan | 917 | 917 | 1/1 | 15 | 543K | scan | 4/225 | 0.37× |
| streams/pmeta=true/layout=compacted/window=edge/filter=svc/s3=0ms | 13.6 | 16.1 | 4/4 | 36 | 627K | scan | 9.5 | 16.1 | 4/4 | 21 | 567K | scan | 4/231 | 0.70× |
| streams/pmeta=true/layout=compacted/window=edge/filter=svc/s3=100ms | 3692 | 3692 | 1/1 | 36 | 627K | scan | 1316 | 1316 | 1/1 | 21 | 567K | scan | 4/231 | 0.36× |
| streams/pmeta=true/layout=compacted/window=narrow/filter=none/s3=0ms | 3.9 | 4.3 | 4/4 | 12 | 299K | scan | 3.9 | 13.3 | 4/4 | 6 | 275K | scan | 2/86 | 1.01× |
| streams/pmeta=true/layout=compacted/window=narrow/filter=none/s3=100ms | 1221 | 1221 | 1/1 | 12 | 299K | scan | 609 | 609 | 1/1 | 6 | 275K | scan | 2/86 | 0.50× |
| streams/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=0ms | 5.7 | 6.3 | 4/4 | 18 | 323K | scan | 4.0 | 15.5 | 4/4 | 8 | 283K | scan | 2/89 | 0.70× |
| streams/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=100ms | 1832 | 1832 | 1/1 | 18 | 323K | scan | 813 | 813 | 1/1 | 8 | 283K | scan | 2/89 | 0.44× |
| streams/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms | 8.8 | 9.5 | 4/4 | 18 | 555K | scan | 5.4 | 9.2 | 4/4 | 18 | 555K | scan | 6/280 | 0.62× |
| streams/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms | 1830 | 1830 | 1/1 | 18 | 555K | scan | 931 | 931 | 1/1 | 18 | 555K | scan | 6/280 | 0.51× |
| streams/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms | 24.8 | 26.7 | 4/4 | 36 | 627K | scan | 13.8 | 19.1 | 4/4 | 36 | 627K | scan | 6/300 | 0.56× |
| streams/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms | 3665 | 3665 | 1/1 | 36 | 627K | scan | 1833 | 1833 | 1/1 | 36 | 627K | scan | 6/300 | 0.50× |
| streams/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms | 7.9 | 10.8 | 4/4 | 13 | 911K | scan | 1.8 | 2.9 | 4/4 | 13 | 911K | scan | 13/1053 | 0.23× |
| streams/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms | 1340 | 1340 | 1/1 | 13 | 911K | scan | 404 | 404 | 1/1 | 13 | 911K | scan | 13/1053 | 0.30× |
| streams/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms | 14.1 | 14.6 | 4/4 | 13 | 911K | scan | 2.7 | 4.3 | 4/4 | 13 | 911K | scan | 13/1053 | 0.19× |
| streams/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms | 1348 | 1348 | 1/1 | 13 | 911K | scan | 410 | 410 | 1/1 | 13 | 911K | scan | 13/1053 | 0.30× |
| streams/pmeta=true/layout=flushed/window=edge/filter=none/s3=0ms | 2.7 | 3.1 | 4/4 | 5 | 347K | scan | 1.2 | 1.7 | 4/4 | 5 | 347K | scan | 5/405 | 0.45× |
| streams/pmeta=true/layout=flushed/window=edge/filter=none/s3=100ms | 516 | 516 | 1/1 | 5 | 347K | scan | 204 | 204 | 1/1 | 5 | 347K | scan | 5/405 | 0.39× |
| streams/pmeta=true/layout=flushed/window=edge/filter=svc/s3=0ms | 5.4 | 5.6 | 4/4 | 5 | 347K | scan | 1.3 | 2.3 | 4/4 | 5 | 347K | scan | 5/405 | 0.24× |
| streams/pmeta=true/layout=flushed/window=edge/filter=svc/s3=100ms | 516 | 516 | 1/1 | 5 | 347K | scan | 203 | 203 | 1/1 | 5 | 347K | scan | 5/405 | 0.39× |
| streams/pmeta=true/layout=flushed/window=narrow/filter=none/s3=0ms | 0.520 | 0.585 | 4/4 | 1 | 72K | scan | 0.600 | 0.792 | 4/4 | 1 | 72K | scan | 1/81 | 1.15× |
| streams/pmeta=true/layout=flushed/window=narrow/filter=none/s3=100ms | 103 | 103 | 1/1 | 1 | 72K | scan | 102 | 102 | 1/1 | 1 | 72K | scan | 1/81 | 0.98× |
| streams/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=0ms | 0.654 | 0.726 | 4/4 | 1 | 72K | scan | 0.646 | 0.699 | 4/4 | 1 | 72K | scan | 1/81 | 0.99× |
| streams/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=100ms | 104 | 104 | 1/1 | 1 | 72K | scan | 101 | 101 | 1/1 | 1 | 72K | scan | 1/81 | 0.98× |
| streams/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms | 17.2 | 27.9 | 4/4 | 24 | 1.64M | scan | 3.4 | 4.3 | 4/4 | 24 | 1.64M | scan | 24/1944 | 0.20× |
| streams/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms | 2474 | 2474 | 1/1 | 24 | 1.64M | scan | 610 | 610 | 1/1 | 24 | 1.64M | scan | 24/1944 | 0.25× |
| streams/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms | 30.0 | 35.8 | 4/4 | 24 | 1.64M | scan | 5.0 | 5.3 | 4/4 | 24 | 1.64M | scan | 24/1944 | 0.17× |
| streams/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms | 2481 | 2481 | 1/1 | 24 | 1.64M | scan | 613 | 613 | 1/1 | 24 | 1.64M | scan | 24/1944 | 0.25× |
| streams/pmeta=true/layout=peer/window=cut/filter=none/s3=0ms | 7.0 | 7.4 | 4/4 | 13 | 911K | scan | 2.5 | 21.0 | 4/4 | 13 | 911K | scan | 13/1053 | 0.35× |
| streams/pmeta=true/layout=peer/window=cut/filter=none/s3=100ms | 1327 | 1327 | 1/1 | 13 | 911K | scan | 405 | 405 | 1/1 | 13 | 911K | scan | 13/1053 | 0.31× |
| streams/pmeta=true/layout=peer/window=cut/filter=svc/s3=0ms | 12.2 | 13.9 | 4/4 | 13 | 911K | scan | 3.2 | 6.4 | 4/4 | 13 | 911K | scan | 13/1053 | 0.26× |
| streams/pmeta=true/layout=peer/window=cut/filter=svc/s3=100ms | 1344 | 1344 | 1/1 | 13 | 911K | scan | 406 | 406 | 1/1 | 13 | 911K | scan | 13/1053 | 0.30× |
| streams/pmeta=true/layout=peer/window=edge/filter=none/s3=0ms | 2.4 | 2.7 | 4/4 | 5 | 347K | scan | 1.1 | 2.1 | 4/4 | 5 | 347K | scan | 5/405 | 0.46× |
| streams/pmeta=true/layout=peer/window=edge/filter=none/s3=100ms | 515 | 515 | 1/1 | 5 | 347K | scan | 202 | 202 | 1/1 | 5 | 347K | scan | 5/405 | 0.39× |
| streams/pmeta=true/layout=peer/window=edge/filter=svc/s3=0ms | 4.7 | 5.8 | 4/4 | 5 | 347K | scan | 1.4 | 2.1 | 4/4 | 5 | 347K | scan | 5/405 | 0.30× |
| streams/pmeta=true/layout=peer/window=edge/filter=svc/s3=100ms | 512 | 512 | 1/1 | 5 | 347K | scan | 205 | 205 | 1/1 | 5 | 347K | scan | 5/405 | 0.40× |
| streams/pmeta=true/layout=peer/window=narrow/filter=none/s3=0ms | 0.366 | 0.474 | 4/4 | 1 | 72K | scan | 0.608 | 0.845 | 4/4 | 1 | 72K | scan | 1/81 | 1.66× |
| streams/pmeta=true/layout=peer/window=narrow/filter=none/s3=100ms | 102 | 102 | 1/1 | 1 | 72K | scan | 101 | 101 | 1/1 | 1 | 72K | scan | 1/81 | 0.99× |
| streams/pmeta=true/layout=peer/window=narrow/filter=svc/s3=0ms | 0.527 | 0.592 | 4/4 | 1 | 72K | scan | 0.606 | 0.817 | 4/4 | 1 | 72K | scan | 1/81 | 1.15× |
| streams/pmeta=true/layout=peer/window=narrow/filter=svc/s3=100ms | 103 | 103 | 1/1 | 1 | 72K | scan | 103 | 103 | 1/1 | 1 | 72K | scan | 1/81 | 1.00× |
| streams/pmeta=true/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 22 | 1.50M | scan | 23.4 | 29.6 | 4/4 | 22 | 1.50M | scan | 22/1782 | before invalid (fast-wrong) → now exact |
| streams/pmeta=true/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 22 | 1.50M | scan | 628 | 628 | 1/1 | 22 | 1.50M | scan | 22/1782 | before invalid (fast-wrong) → now exact |
| streams/pmeta=true/layout=peer/window=whole/filter=svc/s3=0ms | [22.3]† | [24.6]† | 0/4 (set 4) | 22 | 1.50M | scan | 28.1 | 33.4 | 4/4 | 22 | 1.50M | scan | 22/1782 | [1.26×]† |
| streams/pmeta=true/layout=peer/window=whole/filter=svc/s3=100ms | [2263]† | [2263]† | 0/1 (set 1) | 22 | 1.50M | scan | 634 | 634 | 1/1 | 22 | 1.50M | scan | 22/1782 | [0.28×]† |
| traces.fv_name/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms | 2.9 | 4.3 | 4/4 | 2 | 1.86M | scan | 2.4 | 4.8 | 4/4 | 2 | 1.86M | scan | — | 0.84× |
| traces.fv_name/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms | 105 | 105 | 1/1 | 2 | 1.86M | scan | 105 | 105 | 1/1 | 2 | 1.86M | scan | — | 1.00× |
| traces.fv_name/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms | 5.5 | 5.9 | 4/4 | 2 | 1.86M | scan | 5.5 | 5.9 | 4/4 | 2 | 1.86M | scan | — | 0.99× |
| traces.fv_name/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms | 107 | 107 | 1/1 | 2 | 1.86M | scan | 107 | 107 | 1/1 | 2 | 1.86M | scan | — | 1.00× |
| traces.fv_name/pmeta=false/layout=compacted/window=edge/filter=none/s3=0ms | 2.2 | 2.5 | 4/4 | 2 | 1.86M | scan | 2.0 | 2.1 | 4/4 | 2 | 1.86M | scan | — | 0.91× |
| traces.fv_name/pmeta=false/layout=compacted/window=edge/filter=none/s3=100ms | 114 | 114 | 1/1 | 2 | 1.86M | scan | 103 | 103 | 1/1 | 2 | 1.86M | scan | — | 0.90× |
| traces.fv_name/pmeta=false/layout=compacted/window=edge/filter=svc/s3=0ms | 3.7 | 5.2 | 4/4 | 2 | 1.86M | scan | 3.9 | 5.1 | 4/4 | 2 | 1.86M | scan | — | 1.04× |
| traces.fv_name/pmeta=false/layout=compacted/window=edge/filter=svc/s3=100ms | 105 | 105 | 1/1 | 2 | 1.86M | scan | 105 | 105 | 1/1 | 2 | 1.86M | scan | — | 1.00× |
| traces.fv_name/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms | 1.9 | 1.9 | 4/4 | 1 | 962K | scan | 1.7 | 1.8 | 4/4 | 1 | 962K | scan | — | 0.91× |
| traces.fv_name/pmeta=false/layout=compacted/window=narrow/filter=none/s3=100ms | 102 | 102 | 1/1 | 1 | 962K | scan | 104 | 104 | 1/1 | 1 | 962K | scan | — | 1.02× |
| traces.fv_name/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=0ms | 2.0 | 4.0 | 4/4 | 1 | 962K | scan | 1.9 | 2.0 | 4/4 | 1 | 962K | scan | — | 0.93× |
| traces.fv_name/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=100ms | 103 | 103 | 1/1 | 1 | 962K | scan | 106 | 106 | 1/1 | 1 | 962K | scan | — | 1.03× |
| traces.fv_name/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms | 2.9 | 3.2 | 4/4 | 2 | 1.86M | scan | 0.020 | 0.029 | 4/4 | 0 | 0 | ram | — | 0.01× |
| traces.fv_name/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms | 105 | 105 | 1/1 | 2 | 1.86M | scan | 0.020 | 0.020 | 1/1 | 0 | 0 | ram | — | 0.00× |
| traces.fv_name/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms | 8.8 | 9.4 | 4/4 | 2 | 1.86M | scan | 9.4 | 10.6 | 4/4 | 2 | 1.86M | scan | — | 1.06× |
| traces.fv_name/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms | 112 | 112 | 1/1 | 2 | 1.86M | scan | 110 | 110 | 1/1 | 2 | 1.86M | scan | — | 0.98× |
| traces.fv_name/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms | 1.1 | 1.1 | 4/4 | 13 | 1.18M | scan | 0.761 | 1.2 | 4/4 | 2 | 185K | scan | — | 0.72× |
| traces.fv_name/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms | 104 | 104 | 1/1 | 13 | 1.18M | scan | 102 | 102 | 1/1 | 2 | 185K | scan | — | 0.98× |
| traces.fv_name/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms | 2.1 | 6.1 | 4/4 | 13 | 1.18M | scan | 3.2 | 4.8 | 4/4 | 13 | 1.18M | scan | — | 1.49× |
| traces.fv_name/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms | 107 | 107 | 1/1 | 13 | 1.18M | scan | 112 | 112 | 1/1 | 13 | 1.18M | scan | — | 1.05× |
| traces.fv_name/pmeta=false/layout=flushed/window=edge/filter=none/s3=0ms | 0.758 | 0.841 | 4/4 | 5 | 462K | scan | 0.660 | 0.762 | 4/4 | 1 | 93K | scan | — | 0.87× |
| traces.fv_name/pmeta=false/layout=flushed/window=edge/filter=none/s3=100ms | 102 | 102 | 1/1 | 5 | 462K | scan | 101 | 101 | 1/1 | 1 | 93K | scan | — | 0.99× |
| traces.fv_name/pmeta=false/layout=flushed/window=edge/filter=svc/s3=0ms | 1.3 | 1.4 | 4/4 | 5 | 462K | scan | 1.6 | 5.1 | 4/4 | 5 | 462K | scan | — | 1.24× |
| traces.fv_name/pmeta=false/layout=flushed/window=edge/filter=svc/s3=100ms | 104 | 104 | 1/1 | 5 | 462K | scan | 102 | 102 | 1/1 | 5 | 462K | scan | — | 0.99× |
| traces.fv_name/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms | 0.549 | 0.958 | 4/4 | 1 | 93K | scan | 0.623 | 0.712 | 4/4 | 1 | 93K | scan | — | 1.13× |
| traces.fv_name/pmeta=false/layout=flushed/window=narrow/filter=none/s3=100ms | 102 | 102 | 1/1 | 1 | 93K | scan | 104 | 104 | 1/1 | 1 | 93K | scan | — | 1.02× |
| traces.fv_name/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=0ms | 0.841 | 1.3 | 4/4 | 1 | 93K | scan | 0.720 | 0.856 | 4/4 | 1 | 93K | scan | — | 0.86× |
| traces.fv_name/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=100ms | 103 | 103 | 1/1 | 1 | 93K | scan | 103 | 103 | 1/1 | 1 | 93K | scan | — | 1.00× |
| traces.fv_name/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms | 1.8 | 2.4 | 4/4 | 24 | 2.17M | scan | 0.042 | 0.054 | 4/4 | 0 | 0 | ram | — | 0.02× |
| traces.fv_name/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms | 105 | 105 | 1/1 | 24 | 2.17M | scan | 0.032 | 0.032 | 1/1 | 0 | 0 | ram | — | 0.00× |
| traces.fv_name/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms | 3.6 | 4.8 | 4/4 | 24 | 2.17M | scan | 4.3 | 6.6 | 4/4 | 24 | 2.17M | scan | — | 1.21× |
| traces.fv_name/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms | 110 | 110 | 1/1 | 24 | 2.17M | scan | 106 | 106 | 1/1 | 24 | 2.17M | scan | — | 0.97× |
| traces.fv_name/pmeta=false/layout=peer/window=cut/filter=none/s3=0ms | 1.2 | 1.2 | 4/4 | 13 | 1.18M | scan | 0.877 | 1.4 | 4/4 | 2 | 185K | scan | — | 0.73× |
| traces.fv_name/pmeta=false/layout=peer/window=cut/filter=none/s3=100ms | 102 | 102 | 1/1 | 13 | 1.18M | scan | 102 | 102 | 1/1 | 2 | 185K | scan | — | 0.99× |
| traces.fv_name/pmeta=false/layout=peer/window=cut/filter=svc/s3=0ms | 1.9 | 2.1 | 4/4 | 13 | 1.18M | scan | 2.4 | 4.0 | 4/4 | 13 | 1.18M | scan | — | 1.24× |
| traces.fv_name/pmeta=false/layout=peer/window=cut/filter=svc/s3=100ms | 107 | 107 | 1/1 | 13 | 1.18M | scan | 104 | 104 | 1/1 | 13 | 1.18M | scan | — | 0.97× |
| traces.fv_name/pmeta=false/layout=peer/window=edge/filter=none/s3=0ms | 0.823 | 0.825 | 4/4 | 5 | 462K | scan | 0.636 | 0.751 | 4/4 | 1 | 93K | scan | — | 0.77× |
| traces.fv_name/pmeta=false/layout=peer/window=edge/filter=none/s3=100ms | 103 | 103 | 1/1 | 5 | 462K | scan | 103 | 103 | 1/1 | 1 | 93K | scan | — | 1.00× |
| traces.fv_name/pmeta=false/layout=peer/window=edge/filter=svc/s3=0ms | 1.5 | 2.8 | 4/4 | 5 | 462K | scan | 2.4 | 4.3 | 4/4 | 5 | 462K | scan | — | 1.54× |
| traces.fv_name/pmeta=false/layout=peer/window=edge/filter=svc/s3=100ms | 103 | 103 | 1/1 | 5 | 462K | scan | 103 | 103 | 1/1 | 5 | 462K | scan | — | 1.00× |
| traces.fv_name/pmeta=false/layout=peer/window=narrow/filter=none/s3=0ms | 0.478 | 0.497 | 4/4 | 1 | 93K | scan | 0.594 | 0.721 | 4/4 | 1 | 93K | scan | — | 1.24× |
| traces.fv_name/pmeta=false/layout=peer/window=narrow/filter=none/s3=100ms | 103 | 103 | 1/1 | 1 | 93K | scan | 101 | 101 | 1/1 | 1 | 93K | scan | — | 0.98× |
| traces.fv_name/pmeta=false/layout=peer/window=narrow/filter=svc/s3=0ms | 0.562 | 0.584 | 4/4 | 1 | 93K | scan | 0.676 | 0.826 | 4/4 | 1 | 93K | scan | — | 1.20× |
| traces.fv_name/pmeta=false/layout=peer/window=narrow/filter=svc/s3=100ms | 104 | 104 | 1/1 | 1 | 93K | scan | 101 | 101 | 1/1 | 1 | 93K | scan | — | 0.97× |
| traces.fv_name/pmeta=false/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 22 | 1.99M | scan | 26.0 | 30.7 | 4/4 | 0 | 0 | ram | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=false/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 22 | 1.99M | scan | 22.6 | 22.6 | 1/1 | 0 | 0 | ram | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=false/layout=peer/window=whole/filter=svc/s3=0ms | [3.6]† | [5.4]† | 0/4 (set 4) | 22 | 1.99M | scan | 31.3 | 43.1 | 4/4 | 22 | 1.99M | scan | — | [8.57×]† |
| traces.fv_name/pmeta=false/layout=peer/window=whole/filter=svc/s3=100ms | [110]† | [110]† | 0/1 (set 1) | 22 | 1.99M | scan | 126 | 126 | 1/1 | 22 | 1.99M | scan | — | [1.15×]† |
| traces.fv_name/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 2.7 | 7.5 | 4/4 | 2 | 1.86M | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 104 | 104 | 1/1 | 2 | 1.86M | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms | 5.8 | 6.2 | 4/4 | 2 | 1.86M | scan | 6.0 | 6.4 | 4/4 | 2 | 1.86M | scan | — | 1.04× |
| traces.fv_name/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms | 109 | 109 | 1/1 | 2 | 1.86M | scan | 106 | 106 | 1/1 | 2 | 1.86M | scan | — | 0.97× |
| traces.fv_name/pmeta=true/layout=compacted/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 8.0 | 9.2 | 4/4 | 2 | 1.86M | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=compacted/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 103 | 103 | 1/1 | 2 | 1.86M | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=compacted/window=edge/filter=svc/s3=0ms | 4.3 | 6.0 | 4/4 | 2 | 1.86M | scan | 7.4 | 22.8 | 4/4 | 2 | 1.86M | scan | — | 1.73× |
| traces.fv_name/pmeta=true/layout=compacted/window=edge/filter=svc/s3=100ms | 107 | 107 | 1/1 | 2 | 1.86M | scan | 105 | 105 | 1/1 | 2 | 1.86M | scan | — | 0.98× |
| traces.fv_name/pmeta=true/layout=compacted/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 2.0 | 3.8 | 4/4 | 1 | 962K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=compacted/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 104 | 104 | 1/1 | 1 | 962K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=0ms | 2.5 | 2.7 | 4/4 | 1 | 962K | scan | 1.8 | 5.0 | 4/4 | 1 | 962K | scan | — | 0.71× |
| traces.fv_name/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=100ms | 105 | 105 | 1/1 | 1 | 962K | scan | 102 | 102 | 1/1 | 1 | 962K | scan | — | 0.98× |
| traces.fv_name/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms | [0.011]† | [0.013]† | 0/4 (set 4) | 0 | 0 | ram | 0.019 | 0.021 | 4/4 | 0 | 0 | ram | — | [1.71×]† |
| traces.fv_name/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms | [0.020]† | [0.020]† | 0/1 (set 1) | 0 | 0 | ram | 0.021 | 0.021 | 1/1 | 0 | 0 | ram | — | [1.01×]† |
| traces.fv_name/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms | 8.5 | 8.6 | 4/4 | 2 | 1.86M | scan | 10.1 | 13.1 | 4/4 | 2 | 1.86M | scan | — | 1.19× |
| traces.fv_name/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms | 112 | 112 | 1/1 | 2 | 1.86M | scan | 108 | 108 | 1/1 | 2 | 1.86M | scan | — | 0.96× |
| traces.fv_name/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 0.959 | 2.3 | 4/4 | 2 | 185K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 102 | 102 | 1/1 | 2 | 185K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms | 2.1 | 2.6 | 4/4 | 13 | 1.18M | scan | 2.9 | 4.0 | 4/4 | 13 | 1.18M | scan | — | 1.34× |
| traces.fv_name/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms | 103 | 103 | 1/1 | 13 | 1.18M | scan | 103 | 103 | 1/1 | 13 | 1.18M | scan | — | 0.99× |
| traces.fv_name/pmeta=true/layout=flushed/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 0.724 | 1.3 | 4/4 | 1 | 93K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=flushed/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 101 | 101 | 1/1 | 1 | 93K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=flushed/window=edge/filter=svc/s3=0ms | 1.3 | 3.2 | 4/4 | 5 | 462K | scan | 1.5 | 1.7 | 4/4 | 5 | 462K | scan | — | 1.20× |
| traces.fv_name/pmeta=true/layout=flushed/window=edge/filter=svc/s3=100ms | 103 | 103 | 1/1 | 5 | 462K | scan | 102 | 102 | 1/1 | 5 | 462K | scan | — | 0.99× |
| traces.fv_name/pmeta=true/layout=flushed/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 0.690 | 0.917 | 4/4 | 1 | 93K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=flushed/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 102 | 102 | 1/1 | 1 | 93K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=0ms | 0.631 | 0.702 | 4/4 | 1 | 93K | scan | 0.788 | 0.938 | 4/4 | 1 | 93K | scan | — | 1.25× |
| traces.fv_name/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=100ms | 103 | 103 | 1/1 | 1 | 93K | scan | 101 | 101 | 1/1 | 1 | 93K | scan | — | 0.99× |
| traces.fv_name/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms | [0.026]† | [0.034]† | 0/4 (set 4) | 0 | 0 | ram | 0.045 | 0.056 | 4/4 | 0 | 0 | ram | — | [1.74×]† |
| traces.fv_name/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms | [0.044]† | [0.044]† | 0/1 (set 1) | 0 | 0 | ram | 0.076 | 0.076 | 1/1 | 0 | 0 | ram | — | [1.73×]† |
| traces.fv_name/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms | 3.4 | 5.4 | 4/4 | 24 | 2.17M | scan | 10.3 | 25.1 | 4/4 | 24 | 2.17M | scan | — | 3.03× |
| traces.fv_name/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms | 108 | 108 | 1/1 | 24 | 2.17M | scan | 104 | 104 | 1/1 | 24 | 2.17M | scan | — | 0.96× |
| traces.fv_name/pmeta=true/layout=peer/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 0.885 | 2.0 | 4/4 | 2 | 185K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=peer/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 101 | 101 | 1/1 | 2 | 185K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=peer/window=cut/filter=svc/s3=0ms | 1.8 | 2.4 | 4/4 | 13 | 1.18M | scan | 2.1 | 3.5 | 4/4 | 13 | 1.18M | scan | — | 1.17× |
| traces.fv_name/pmeta=true/layout=peer/window=cut/filter=svc/s3=100ms | 105 | 105 | 1/1 | 13 | 1.18M | scan | 112 | 112 | 1/1 | 13 | 1.18M | scan | — | 1.06× |
| traces.fv_name/pmeta=true/layout=peer/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 0.644 | 0.799 | 4/4 | 1 | 93K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=peer/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 103 | 103 | 1/1 | 1 | 93K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=peer/window=edge/filter=svc/s3=0ms | 1.9 | 3.2 | 4/4 | 5 | 462K | scan | 1.4 | 1.5 | 4/4 | 5 | 462K | scan | — | 0.75× |
| traces.fv_name/pmeta=true/layout=peer/window=edge/filter=svc/s3=100ms | 103 | 103 | 1/1 | 5 | 462K | scan | 103 | 103 | 1/1 | 5 | 462K | scan | — | 1.00× |
| traces.fv_name/pmeta=true/layout=peer/window=narrow/filter=none/s3=0ms | [0.005]† | [0.006]† | 0/4 (set 4) | 0 | 0 | ram | 0.600 | 0.721 | 4/4 | 1 | 93K | scan | — | [132.05×]† |
| traces.fv_name/pmeta=true/layout=peer/window=narrow/filter=none/s3=100ms | [0.016]† | [0.016]† | 0/1 (set 1) | 0 | 0 | ram | 103 | 103 | 1/1 | 1 | 93K | scan | — | [6309.95×]† |
| traces.fv_name/pmeta=true/layout=peer/window=narrow/filter=svc/s3=0ms | 0.614 | 0.690 | 4/4 | 1 | 93K | scan | 0.765 | 0.961 | 4/4 | 1 | 93K | scan | — | 1.25× |
| traces.fv_name/pmeta=true/layout=peer/window=narrow/filter=svc/s3=100ms | 103 | 103 | 1/1 | 1 | 93K | scan | 102 | 102 | 1/1 | 1 | 93K | scan | — | 0.99× |
| traces.fv_name/pmeta=true/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | ram | 24.8 | 40.7 | 4/4 | 0 | 0 | ram | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | ram | 25.2 | 25.2 | 1/1 | 0 | 0 | ram | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=true/layout=peer/window=whole/filter=svc/s3=0ms | [2.8]† | [3.4]† | 0/4 (set 4) | 22 | 1.99M | scan | 40.5 | 54.1 | 4/4 | 22 | 1.99M | scan | — | [14.55×]† |
| traces.fv_name/pmeta=true/layout=peer/window=whole/filter=svc/s3=100ms | [107]† | [107]† | 0/1 (set 1) | 22 | 1.99M | scan | 126 | 126 | 1/1 | 22 | 1.99M | scan | — | [1.18×]† |
| traces.fv_service/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms | 2.4 | 2.8 | 4/4 | 2 | 1.86M | scan | 2.5 | 3.0 | 4/4 | 2 | 1.86M | scan | — | 1.07× |
| traces.fv_service/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms | 103 | 103 | 1/1 | 2 | 1.86M | scan | 105 | 105 | 1/1 | 2 | 1.86M | scan | — | 1.01× |
| traces.fv_service/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms | 4.8 | 6.0 | 4/4 | 2 | 1.86M | scan | 4.1 | 8.5 | 4/4 | 2 | 1.86M | scan | — | 0.84× |
| traces.fv_service/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms | 105 | 105 | 1/1 | 2 | 1.86M | scan | 106 | 106 | 1/1 | 2 | 1.86M | scan | — | 1.02× |
| traces.fv_service/pmeta=false/layout=compacted/window=edge/filter=none/s3=0ms | 2.1 | 2.1 | 4/4 | 2 | 1.86M | scan | 2.0 | 2.1 | 4/4 | 2 | 1.86M | scan | — | 0.94× |
| traces.fv_service/pmeta=false/layout=compacted/window=edge/filter=none/s3=100ms | 104 | 104 | 1/1 | 2 | 1.86M | scan | 104 | 104 | 1/1 | 2 | 1.86M | scan | — | 1.00× |
| traces.fv_service/pmeta=false/layout=compacted/window=edge/filter=svc/s3=0ms | 2.8 | 4.4 | 4/4 | 2 | 1.86M | scan | 4.2 | 8.7 | 4/4 | 2 | 1.86M | scan | — | 1.51× |
| traces.fv_service/pmeta=false/layout=compacted/window=edge/filter=svc/s3=100ms | 103 | 103 | 1/1 | 2 | 1.86M | scan | 113 | 113 | 1/1 | 2 | 1.86M | scan | — | 1.10× |
| traces.fv_service/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms | 2.0 | 2.2 | 4/4 | 1 | 962K | scan | 1.5 | 1.9 | 4/4 | 1 | 962K | scan | — | 0.74× |
| traces.fv_service/pmeta=false/layout=compacted/window=narrow/filter=none/s3=100ms | 102 | 102 | 1/1 | 1 | 962K | scan | 104 | 104 | 1/1 | 1 | 962K | scan | — | 1.01× |
| traces.fv_service/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=0ms | 1.9 | 3.3 | 4/4 | 1 | 962K | scan | 1.5 | 1.6 | 4/4 | 1 | 962K | scan | — | 0.80× |
| traces.fv_service/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=100ms | 103 | 103 | 1/1 | 1 | 962K | scan | 103 | 103 | 1/1 | 1 | 962K | scan | — | 1.00× |
| traces.fv_service/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms | 3.2 | 4.7 | 4/4 | 2 | 1.86M | scan | 3.6 | 4.9 | 4/4 | 1 | 962K | scan | — | 1.14× |
| traces.fv_service/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms | 105 | 105 | 1/1 | 2 | 1.86M | scan | 103 | 103 | 1/1 | 1 | 962K | scan | — | 0.98× |
| traces.fv_service/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms | 6.2 | 6.7 | 4/4 | 2 | 1.86M | scan | 7.5 | 8.4 | 4/4 | 2 | 1.86M | scan | — | 1.22× |
| traces.fv_service/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms | 108 | 108 | 1/1 | 2 | 1.86M | scan | 108 | 108 | 1/1 | 2 | 1.86M | scan | — | 1.00× |
| traces.fv_service/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms | 1.2 | 1.4 | 4/4 | 13 | 1.18M | scan | 2.9 | 7.0 | 4/4 | 8 | 746K | scan | — | 2.48× |
| traces.fv_service/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms | 102 | 102 | 1/1 | 13 | 1.18M | scan | 102 | 102 | 1/1 | 8 | 746K | scan | — | 1.00× |
| traces.fv_service/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms | 2.6 | 6.6 | 4/4 | 13 | 1.18M | scan | 2.4 | 4.3 | 4/4 | 13 | 1.18M | scan | — | 0.89× |
| traces.fv_service/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms | 107 | 107 | 1/1 | 13 | 1.18M | scan | 104 | 104 | 1/1 | 13 | 1.18M | scan | — | 0.97× |
| traces.fv_service/pmeta=false/layout=flushed/window=edge/filter=none/s3=0ms | 0.703 | 0.803 | 4/4 | 5 | 462K | scan | 0.801 | 1.1 | 4/4 | 2 | 187K | scan | — | 1.14× |
| traces.fv_service/pmeta=false/layout=flushed/window=edge/filter=none/s3=100ms | 104 | 104 | 1/1 | 5 | 462K | scan | 101 | 101 | 1/1 | 2 | 187K | scan | — | 0.97× |
| traces.fv_service/pmeta=false/layout=flushed/window=edge/filter=svc/s3=0ms | 1.8 | 2.8 | 4/4 | 5 | 462K | scan | 2.0 | 3.7 | 4/4 | 5 | 462K | scan | — | 1.09× |
| traces.fv_service/pmeta=false/layout=flushed/window=edge/filter=svc/s3=100ms | 105 | 105 | 1/1 | 5 | 462K | scan | 101 | 101 | 1/1 | 5 | 462K | scan | — | 0.97× |
| traces.fv_service/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms | 0.563 | 0.800 | 4/4 | 1 | 93K | scan | 0.908 | 1.3 | 4/4 | 1 | 93K | scan | — | 1.61× |
| traces.fv_service/pmeta=false/layout=flushed/window=narrow/filter=none/s3=100ms | 103 | 103 | 1/1 | 1 | 93K | scan | 103 | 103 | 1/1 | 1 | 93K | scan | — | 1.00× |
| traces.fv_service/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=0ms | 0.681 | 0.747 | 4/4 | 1 | 93K | scan | 0.648 | 1.2 | 4/4 | 1 | 93K | scan | — | 0.95× |
| traces.fv_service/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=100ms | 103 | 103 | 1/1 | 1 | 93K | scan | 103 | 103 | 1/1 | 1 | 93K | scan | — | 0.99× |
| traces.fv_service/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms | 1.7 | 1.7 | 4/4 | 24 | 2.17M | scan | 2.3 | 9.8 | 4/4 | 12 | 1.09M | scan | — | 1.39× |
| traces.fv_service/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms | 104 | 104 | 1/1 | 24 | 2.17M | scan | 103 | 103 | 1/1 | 12 | 1.09M | scan | — | 0.99× |
| traces.fv_service/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms | 2.6 | 3.9 | 4/4 | 24 | 2.17M | scan | 4.9 | 9.4 | 4/4 | 24 | 2.17M | scan | — | 1.90× |
| traces.fv_service/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms | 106 | 106 | 1/1 | 24 | 2.17M | scan | 103 | 103 | 1/1 | 24 | 2.17M | scan | — | 0.97× |
| traces.fv_service/pmeta=false/layout=peer/window=cut/filter=none/s3=0ms | 1.3 | 2.0 | 4/4 | 13 | 1.18M | scan | 2.1 | 5.6 | 4/4 | 8 | 746K | scan | — | 1.66× |
| traces.fv_service/pmeta=false/layout=peer/window=cut/filter=none/s3=100ms | 104 | 104 | 1/1 | 13 | 1.18M | scan | 104 | 104 | 1/1 | 8 | 746K | scan | — | 1.00× |
| traces.fv_service/pmeta=false/layout=peer/window=cut/filter=svc/s3=0ms | 1.6 | 1.8 | 4/4 | 13 | 1.18M | scan | 4.7 | 8.2 | 4/4 | 13 | 1.18M | scan | — | 2.92× |
| traces.fv_service/pmeta=false/layout=peer/window=cut/filter=svc/s3=100ms | 103 | 103 | 1/1 | 13 | 1.18M | scan | 105 | 105 | 1/1 | 13 | 1.18M | scan | — | 1.02× |
| traces.fv_service/pmeta=false/layout=peer/window=edge/filter=none/s3=0ms | 0.727 | 0.906 | 4/4 | 5 | 462K | scan | 1.5 | 2.8 | 4/4 | 2 | 187K | scan | — | 2.02× |
| traces.fv_service/pmeta=false/layout=peer/window=edge/filter=none/s3=100ms | 103 | 103 | 1/1 | 5 | 462K | scan | 101 | 101 | 1/1 | 2 | 187K | scan | — | 0.98× |
| traces.fv_service/pmeta=false/layout=peer/window=edge/filter=svc/s3=0ms | 1.5 | 2.8 | 4/4 | 5 | 462K | scan | 1.5 | 2.8 | 4/4 | 5 | 462K | scan | — | 1.03× |
| traces.fv_service/pmeta=false/layout=peer/window=edge/filter=svc/s3=100ms | 103 | 103 | 1/1 | 5 | 462K | scan | 104 | 104 | 1/1 | 5 | 462K | scan | — | 1.01× |
| traces.fv_service/pmeta=false/layout=peer/window=narrow/filter=none/s3=0ms | 0.520 | 0.606 | 4/4 | 1 | 93K | scan | 1.0 | 3.1 | 4/4 | 1 | 93K | scan | — | 2.00× |
| traces.fv_service/pmeta=false/layout=peer/window=narrow/filter=none/s3=100ms | 101 | 101 | 1/1 | 1 | 93K | scan | 102 | 102 | 1/1 | 1 | 93K | scan | — | 1.01× |
| traces.fv_service/pmeta=false/layout=peer/window=narrow/filter=svc/s3=0ms | 0.534 | 0.619 | 4/4 | 1 | 93K | scan | 1.2 | 3.8 | 4/4 | 1 | 93K | scan | — | 2.25× |
| traces.fv_service/pmeta=false/layout=peer/window=narrow/filter=svc/s3=100ms | 102 | 102 | 1/1 | 1 | 93K | scan | 101 | 101 | 1/1 | 1 | 93K | scan | — | 0.99× |
| traces.fv_service/pmeta=false/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 22 | 1.99M | scan | 23.8 | 26.1 | 4/4 | 10 | 934K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_service/pmeta=false/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 22 | 1.99M | scan | 131 | 131 | 1/1 | 10 | 934K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_service/pmeta=false/layout=peer/window=whole/filter=svc/s3=0ms | [2.3]† | [2.6]† | 0/4 (set 4) | 22 | 1.99M | scan | 27.1 | 33.4 | 4/4 | 22 | 1.99M | scan | — | [11.80×]† |
| traces.fv_service/pmeta=false/layout=peer/window=whole/filter=svc/s3=100ms | [109]† | [109]† | 0/1 (set 1) | 22 | 1.99M | scan | 131 | 131 | 1/1 | 22 | 1.99M | scan | — | [1.21×]† |
| traces.fv_service/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms | 3.5 | 5.7 | 4/4 | 2 | 1.86M | scan | 2.7 | 3.2 | 4/4 | 2 | 1.86M | scan | — | 0.76× |
| traces.fv_service/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms | 103 | 103 | 1/1 | 2 | 1.86M | scan | 103 | 103 | 1/1 | 2 | 1.86M | scan | — | 1.00× |
| traces.fv_service/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms | 4.4 | 5.1 | 4/4 | 2 | 1.86M | scan | 4.4 | 5.4 | 4/4 | 2 | 1.86M | scan | — | 0.99× |
| traces.fv_service/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms | 110 | 110 | 1/1 | 2 | 1.86M | scan | 105 | 105 | 1/1 | 2 | 1.86M | scan | — | 0.95× |
| traces.fv_service/pmeta=true/layout=compacted/window=edge/filter=none/s3=0ms | 2.5 | 4.2 | 4/4 | 2 | 1.86M | scan | 2.4 | 3.1 | 4/4 | 2 | 1.86M | scan | — | 0.94× |
| traces.fv_service/pmeta=true/layout=compacted/window=edge/filter=none/s3=100ms | 102 | 102 | 1/1 | 2 | 1.86M | scan | 102 | 102 | 1/1 | 2 | 1.86M | scan | — | 0.99× |
| traces.fv_service/pmeta=true/layout=compacted/window=edge/filter=svc/s3=0ms | 3.0 | 3.2 | 4/4 | 2 | 1.86M | scan | 3.2 | 14.2 | 4/4 | 2 | 1.86M | scan | — | 1.07× |
| traces.fv_service/pmeta=true/layout=compacted/window=edge/filter=svc/s3=100ms | 107 | 107 | 1/1 | 2 | 1.86M | scan | 104 | 104 | 1/1 | 2 | 1.86M | scan | — | 0.97× |
| traces.fv_service/pmeta=true/layout=compacted/window=narrow/filter=none/s3=0ms | 1.9 | 2.0 | 4/4 | 1 | 962K | scan | 1.6 | 2.1 | 4/4 | 1 | 962K | scan | — | 0.82× |
| traces.fv_service/pmeta=true/layout=compacted/window=narrow/filter=none/s3=100ms | 104 | 104 | 1/1 | 1 | 962K | scan | 102 | 102 | 1/1 | 1 | 962K | scan | — | 0.98× |
| traces.fv_service/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=0ms | 1.9 | 2.0 | 4/4 | 1 | 962K | scan | 2.2 | 3.8 | 4/4 | 1 | 962K | scan | — | 1.16× |
| traces.fv_service/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=100ms | 105 | 105 | 1/1 | 1 | 962K | scan | 102 | 102 | 1/1 | 1 | 962K | scan | — | 0.98× |
| traces.fv_service/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms | 2.9 | 5.6 | 4/4 | 2 | 1.86M | scan | 3.3 | 3.7 | 4/4 | 1 | 962K | scan | — | 1.12× |
| traces.fv_service/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms | 105 | 105 | 1/1 | 2 | 1.86M | scan | 103 | 103 | 1/1 | 1 | 962K | scan | — | 0.98× |
| traces.fv_service/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms | 6.6 | 14.7 | 4/4 | 2 | 1.86M | scan | 8.5 | 9.6 | 4/4 | 2 | 1.86M | scan | — | 1.30× |
| traces.fv_service/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms | 109 | 109 | 1/1 | 2 | 1.86M | scan | 107 | 107 | 1/1 | 2 | 1.86M | scan | — | 0.99× |
| traces.fv_service/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms | 1.1 | 1.3 | 4/4 | 13 | 1.18M | scan | 2.2 | 3.3 | 4/4 | 8 | 746K | scan | — | 2.03× |
| traces.fv_service/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms | 105 | 105 | 1/1 | 13 | 1.18M | scan | 104 | 104 | 1/1 | 8 | 746K | scan | — | 0.98× |
| traces.fv_service/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms | 1.6 | 2.9 | 4/4 | 13 | 1.18M | scan | 2.4 | 3.6 | 4/4 | 13 | 1.18M | scan | — | 1.50× |
| traces.fv_service/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms | 103 | 103 | 1/1 | 13 | 1.18M | scan | 102 | 102 | 1/1 | 13 | 1.18M | scan | — | 0.98× |
| traces.fv_service/pmeta=true/layout=flushed/window=edge/filter=none/s3=0ms | 0.737 | 0.826 | 4/4 | 5 | 462K | scan | 1.5 | 2.9 | 4/4 | 2 | 187K | scan | — | 2.00× |
| traces.fv_service/pmeta=true/layout=flushed/window=edge/filter=none/s3=100ms | 103 | 103 | 1/1 | 5 | 462K | scan | 103 | 103 | 1/1 | 2 | 187K | scan | — | 1.00× |
| traces.fv_service/pmeta=true/layout=flushed/window=edge/filter=svc/s3=0ms | 1.5 | 1.9 | 4/4 | 5 | 462K | scan | 2.7 | 4.5 | 4/4 | 5 | 462K | scan | — | 1.87× |
| traces.fv_service/pmeta=true/layout=flushed/window=edge/filter=svc/s3=100ms | 104 | 104 | 1/1 | 5 | 462K | scan | 103 | 103 | 1/1 | 5 | 462K | scan | — | 0.99× |
| traces.fv_service/pmeta=true/layout=flushed/window=narrow/filter=none/s3=0ms | 0.509 | 0.617 | 4/4 | 1 | 93K | scan | 0.760 | 0.991 | 4/4 | 1 | 93K | scan | — | 1.49× |
| traces.fv_service/pmeta=true/layout=flushed/window=narrow/filter=none/s3=100ms | 103 | 103 | 1/1 | 1 | 93K | scan | 104 | 104 | 1/1 | 1 | 93K | scan | — | 1.01× |
| traces.fv_service/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=0ms | 0.544 | 0.624 | 4/4 | 1 | 93K | scan | 0.678 | 0.858 | 4/4 | 1 | 93K | scan | — | 1.25× |
| traces.fv_service/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=100ms | 103 | 103 | 1/1 | 1 | 93K | scan | 101 | 101 | 1/1 | 1 | 93K | scan | — | 0.98× |
| traces.fv_service/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms | 1.8 | 2.2 | 4/4 | 24 | 2.17M | scan | 1.8 | 4.0 | 4/4 | 12 | 1.09M | scan | — | 1.01× |
| traces.fv_service/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms | 102 | 102 | 1/1 | 24 | 2.17M | scan | 104 | 104 | 1/1 | 12 | 1.09M | scan | — | 1.02× |
| traces.fv_service/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms | 2.4 | 2.6 | 4/4 | 24 | 2.17M | scan | 7.5 | 14.1 | 4/4 | 24 | 2.17M | scan | — | 3.20× |
| traces.fv_service/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms | 108 | 108 | 1/1 | 24 | 2.17M | scan | 103 | 103 | 1/1 | 24 | 2.17M | scan | — | 0.95× |
| traces.fv_service/pmeta=true/layout=peer/window=cut/filter=none/s3=0ms | 1.1 | 1.3 | 4/4 | 13 | 1.18M | scan | 1.5 | 14.4 | 4/4 | 8 | 746K | scan | — | 1.30× |
| traces.fv_service/pmeta=true/layout=peer/window=cut/filter=none/s3=100ms | 102 | 102 | 1/1 | 13 | 1.18M | scan | 102 | 102 | 1/1 | 8 | 746K | scan | — | 1.00× |
| traces.fv_service/pmeta=true/layout=peer/window=cut/filter=svc/s3=0ms | 1.7 | 4.2 | 4/4 | 13 | 1.18M | scan | 2.7 | 3.3 | 4/4 | 13 | 1.18M | scan | — | 1.63× |
| traces.fv_service/pmeta=true/layout=peer/window=cut/filter=svc/s3=100ms | 104 | 104 | 1/1 | 13 | 1.18M | scan | 105 | 105 | 1/1 | 13 | 1.18M | scan | — | 1.01× |
| traces.fv_service/pmeta=true/layout=peer/window=edge/filter=none/s3=0ms | 0.799 | 0.883 | 4/4 | 5 | 462K | scan | 0.833 | 0.937 | 4/4 | 2 | 187K | scan | — | 1.04× |
| traces.fv_service/pmeta=true/layout=peer/window=edge/filter=none/s3=100ms | 103 | 103 | 1/1 | 5 | 462K | scan | 104 | 104 | 1/1 | 2 | 187K | scan | — | 1.01× |
| traces.fv_service/pmeta=true/layout=peer/window=edge/filter=svc/s3=0ms | 1.1 | 1.2 | 4/4 | 5 | 462K | scan | 1.3 | 1.8 | 4/4 | 5 | 462K | scan | — | 1.17× |
| traces.fv_service/pmeta=true/layout=peer/window=edge/filter=svc/s3=100ms | 103 | 103 | 1/1 | 5 | 462K | scan | 107 | 107 | 1/1 | 5 | 462K | scan | — | 1.04× |
| traces.fv_service/pmeta=true/layout=peer/window=narrow/filter=none/s3=0ms | 0.652 | 1.1 | 4/4 | 1 | 93K | scan | 0.726 | 1.2 | 4/4 | 1 | 93K | scan | — | 1.11× |
| traces.fv_service/pmeta=true/layout=peer/window=narrow/filter=none/s3=100ms | 103 | 103 | 1/1 | 1 | 93K | scan | 104 | 104 | 1/1 | 1 | 93K | scan | — | 1.01× |
| traces.fv_service/pmeta=true/layout=peer/window=narrow/filter=svc/s3=0ms | 0.718 | 1.0 | 4/4 | 1 | 93K | scan | 0.647 | 0.812 | 4/4 | 1 | 93K | scan | — | 0.90× |
| traces.fv_service/pmeta=true/layout=peer/window=narrow/filter=svc/s3=100ms | 101 | 101 | 1/1 | 1 | 93K | scan | 103 | 103 | 1/1 | 1 | 93K | scan | — | 1.02× |
| traces.fv_service/pmeta=true/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 22 | 1.99M | scan | 25.3 | 28.1 | 4/4 | 10 | 934K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_service/pmeta=true/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 22 | 1.99M | scan | 128 | 128 | 1/1 | 10 | 934K | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_service/pmeta=true/layout=peer/window=whole/filter=svc/s3=0ms | [2.3]† | [3.0]† | 0/4 (set 4) | 22 | 1.99M | scan | 27.7 | 33.9 | 4/4 | 22 | 1.99M | scan | — | [12.00×]† |
| traces.fv_service/pmeta=true/layout=peer/window=whole/filter=svc/s3=100ms | [107]† | [107]† | 0/1 (set 1) | 22 | 1.99M | scan | 144 | 144 | 1/1 | 22 | 1.99M | scan | — | [1.35×]† |
| traces.streams/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms | 4.6 | 5.2 | 4/4 | 2 | 1.86M | scan | 3.2 | 9.0 | 4/4 | 2 | 1.86M | scan | — | 0.71× |
| traces.streams/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms | 206 | 206 | 1/1 | 2 | 1.86M | scan | 104 | 104 | 1/1 | 2 | 1.86M | scan | — | 0.51× |
| traces.streams/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms | 9.6 | 11.3 | 4/4 | 2 | 1.86M | scan | 6.1 | 6.3 | 4/4 | 2 | 1.86M | scan | — | 0.64× |
| traces.streams/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms | 213 | 213 | 1/1 | 2 | 1.86M | scan | 118 | 118 | 1/1 | 2 | 1.86M | scan | — | 0.55× |
| traces.streams/pmeta=false/layout=compacted/window=edge/filter=none/s3=0ms | 3.6 | 4.3 | 4/4 | 2 | 1.86M | scan | 2.2 | 2.8 | 4/4 | 2 | 1.86M | scan | — | 0.62× |
| traces.streams/pmeta=false/layout=compacted/window=edge/filter=none/s3=100ms | 207 | 207 | 1/1 | 2 | 1.86M | scan | 105 | 105 | 1/1 | 2 | 1.86M | scan | — | 0.51× |
| traces.streams/pmeta=false/layout=compacted/window=edge/filter=svc/s3=0ms | 7.1 | 11.5 | 4/4 | 2 | 1.86M | scan | 3.2 | 3.8 | 4/4 | 2 | 1.86M | scan | — | 0.45× |
| traces.streams/pmeta=false/layout=compacted/window=edge/filter=svc/s3=100ms | 208 | 208 | 1/1 | 2 | 1.86M | scan | 105 | 105 | 1/1 | 2 | 1.86M | scan | — | 0.50× |
| traces.streams/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms | 1.8 | 2.2 | 4/4 | 1 | 962K | scan | 1.5 | 1.8 | 4/4 | 1 | 962K | scan | — | 0.82× |
| traces.streams/pmeta=false/layout=compacted/window=narrow/filter=none/s3=100ms | 103 | 103 | 1/1 | 1 | 962K | scan | 103 | 103 | 1/1 | 1 | 962K | scan | — | 1.00× |
| traces.streams/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=0ms | 2.2 | 2.5 | 4/4 | 1 | 962K | scan | 1.6 | 1.8 | 4/4 | 1 | 962K | scan | — | 0.72× |
| traces.streams/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=100ms | 103 | 103 | 1/1 | 1 | 962K | scan | 106 | 106 | 1/1 | 1 | 962K | scan | — | 1.03× |
| traces.streams/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms | 5.5 | 9.8 | 4/4 | 2 | 1.86M | scan | 3.7 | 4.2 | 4/4 | 2 | 1.86M | scan | — | 0.66× |
| traces.streams/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms | 207 | 207 | 1/1 | 2 | 1.86M | scan | 106 | 106 | 1/1 | 2 | 1.86M | scan | — | 0.51× |
| traces.streams/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms | 16.0 | 17.9 | 4/4 | 2 | 1.86M | scan | 9.7 | 14.7 | 4/4 | 2 | 1.86M | scan | — | 0.61× |
| traces.streams/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms | 218 | 218 | 1/1 | 2 | 1.86M | scan | 111 | 111 | 1/1 | 2 | 1.86M | scan | — | 0.51× |
| traces.streams/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms | 6.4 | 7.1 | 4/4 | 13 | 1.18M | scan | 3.6 | 5.1 | 4/4 | 13 | 1.18M | scan | — | 0.56× |
| traces.streams/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms | 1331 | 1331 | 1/1 | 13 | 1.18M | scan | 106 | 106 | 1/1 | 13 | 1.18M | scan | — | 0.08× |
| traces.streams/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms | 11.4 | 13.3 | 4/4 | 13 | 1.18M | scan | 4.3 | 6.9 | 4/4 | 13 | 1.18M | scan | — | 0.38× |
| traces.streams/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms | 1335 | 1335 | 1/1 | 13 | 1.18M | scan | 105 | 105 | 1/1 | 13 | 1.18M | scan | — | 0.08× |
| traces.streams/pmeta=false/layout=flushed/window=edge/filter=none/s3=0ms | 2.2 | 2.6 | 4/4 | 5 | 462K | scan | 1.9 | 4.9 | 4/4 | 5 | 462K | scan | — | 0.85× |
| traces.streams/pmeta=false/layout=flushed/window=edge/filter=none/s3=100ms | 514 | 514 | 1/1 | 5 | 462K | scan | 104 | 104 | 1/1 | 5 | 462K | scan | — | 0.20× |
| traces.streams/pmeta=false/layout=flushed/window=edge/filter=svc/s3=0ms | 4.1 | 4.6 | 4/4 | 5 | 462K | scan | 1.8 | 2.9 | 4/4 | 5 | 462K | scan | — | 0.43× |
| traces.streams/pmeta=false/layout=flushed/window=edge/filter=svc/s3=100ms | 516 | 516 | 1/1 | 5 | 462K | scan | 104 | 104 | 1/1 | 5 | 462K | scan | — | 0.20× |
| traces.streams/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms | 0.415 | 0.517 | 4/4 | 1 | 93K | scan | 0.940 | 1.3 | 4/4 | 1 | 93K | scan | — | 2.27× |
| traces.streams/pmeta=false/layout=flushed/window=narrow/filter=none/s3=100ms | 101 | 101 | 1/1 | 1 | 93K | scan | 101 | 101 | 1/1 | 1 | 93K | scan | — | 1.00× |
| traces.streams/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=0ms | 0.492 | 0.531 | 4/4 | 1 | 93K | scan | 0.812 | 0.981 | 4/4 | 1 | 93K | scan | — | 1.65× |
| traces.streams/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=100ms | 103 | 103 | 1/1 | 1 | 93K | scan | 102 | 102 | 1/1 | 1 | 93K | scan | — | 0.99× |
| traces.streams/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms | 13.5 | 15.9 | 4/4 | 24 | 2.17M | scan | 6.1 | 11.0 | 4/4 | 24 | 2.17M | scan | — | 0.45× |
| traces.streams/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms | 2456 | 2456 | 1/1 | 24 | 2.17M | scan | 107 | 107 | 1/1 | 24 | 2.17M | scan | — | 0.04× |
| traces.streams/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms | 23.4 | 25.0 | 4/4 | 24 | 2.17M | scan | 4.9 | 7.2 | 4/4 | 24 | 2.17M | scan | — | 0.21× |
| traces.streams/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms | 2474 | 2474 | 1/1 | 24 | 2.17M | scan | 108 | 108 | 1/1 | 24 | 2.17M | scan | — | 0.04× |
| traces.streams/pmeta=false/layout=peer/window=cut/filter=none/s3=0ms | 6.3 | 7.0 | 4/4 | 13 | 1.18M | scan | 1.6 | 1.9 | 4/4 | 13 | 1.18M | scan | — | 0.26× |
| traces.streams/pmeta=false/layout=peer/window=cut/filter=none/s3=100ms | 1322 | 1322 | 1/1 | 13 | 1.18M | scan | 104 | 104 | 1/1 | 13 | 1.18M | scan | — | 0.08× |
| traces.streams/pmeta=false/layout=peer/window=cut/filter=svc/s3=0ms | 11.6 | 22.9 | 4/4 | 13 | 1.18M | scan | 2.3 | 2.6 | 4/4 | 13 | 1.18M | scan | — | 0.20× |
| traces.streams/pmeta=false/layout=peer/window=cut/filter=svc/s3=100ms | 1328 | 1328 | 1/1 | 13 | 1.18M | scan | 107 | 107 | 1/1 | 13 | 1.18M | scan | — | 0.08× |
| traces.streams/pmeta=false/layout=peer/window=edge/filter=none/s3=0ms | 2.6 | 3.4 | 4/4 | 5 | 462K | scan | 0.970 | 1.0 | 4/4 | 5 | 462K | scan | — | 0.37× |
| traces.streams/pmeta=false/layout=peer/window=edge/filter=none/s3=100ms | 508 | 508 | 1/1 | 5 | 462K | scan | 103 | 103 | 1/1 | 5 | 462K | scan | — | 0.20× |
| traces.streams/pmeta=false/layout=peer/window=edge/filter=svc/s3=0ms | 4.5 | 6.9 | 4/4 | 5 | 462K | scan | 1.6 | 1.6 | 4/4 | 5 | 462K | scan | — | 0.36× |
| traces.streams/pmeta=false/layout=peer/window=edge/filter=svc/s3=100ms | 510 | 510 | 1/1 | 5 | 462K | scan | 103 | 103 | 1/1 | 5 | 462K | scan | — | 0.20× |
| traces.streams/pmeta=false/layout=peer/window=narrow/filter=none/s3=0ms | 0.529 | 0.681 | 4/4 | 1 | 93K | scan | 0.762 | 0.801 | 4/4 | 1 | 93K | scan | — | 1.44× |
| traces.streams/pmeta=false/layout=peer/window=narrow/filter=none/s3=100ms | 101 | 101 | 1/1 | 1 | 93K | scan | 103 | 103 | 1/1 | 1 | 93K | scan | — | 1.02× |
| traces.streams/pmeta=false/layout=peer/window=narrow/filter=svc/s3=0ms | 1.2 | 2.3 | 4/4 | 1 | 93K | scan | 0.732 | 0.836 | 4/4 | 1 | 93K | scan | — | 0.62× |
| traces.streams/pmeta=false/layout=peer/window=narrow/filter=svc/s3=100ms | 101 | 101 | 1/1 | 1 | 93K | scan | 101 | 101 | 1/1 | 1 | 93K | scan | — | 1.00× |
| traces.streams/pmeta=false/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 22 | 1.99M | scan | 26.2 | 34.8 | 4/4 | 22 | 1.99M | scan | — | before invalid (fast-wrong) → now exact |
| traces.streams/pmeta=false/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 22 | 1.99M | scan | 176 | 176 | 1/1 | 22 | 1.99M | scan | — | before invalid (fast-wrong) → now exact |
| traces.streams/pmeta=false/layout=peer/window=whole/filter=svc/s3=0ms | [28.6]† | [33.2]† | 0/4 (set 4) | 22 | 1.99M | scan | 34.9 | 39.4 | 4/4 | 22 | 1.99M | scan | — | [1.22×]† |
| traces.streams/pmeta=false/layout=peer/window=whole/filter=svc/s3=100ms | [2244]† | [2244]† | 0/1 (set 1) | 22 | 1.99M | scan | 142 | 142 | 1/1 | 22 | 1.99M | scan | — | [0.06×]† |
| traces.streams/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms | 4.2 | 7.8 | 4/4 | 2 | 1.86M | scan | 3.2 | 6.4 | 4/4 | 2 | 1.86M | scan | — | 0.75× |
| traces.streams/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms | 206 | 206 | 1/1 | 2 | 1.86M | scan | 104 | 104 | 1/1 | 2 | 1.86M | scan | — | 0.50× |
| traces.streams/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms | 9.0 | 12.1 | 4/4 | 2 | 1.86M | scan | 6.5 | 17.8 | 4/4 | 2 | 1.86M | scan | — | 0.73× |
| traces.streams/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms | 210 | 210 | 1/1 | 2 | 1.86M | scan | 107 | 107 | 1/1 | 2 | 1.86M | scan | — | 0.51× |
| traces.streams/pmeta=true/layout=compacted/window=edge/filter=none/s3=0ms | 6.7 | 10.3 | 4/4 | 2 | 1.86M | scan | 3.1 | 5.1 | 4/4 | 2 | 1.86M | scan | — | 0.46× |
| traces.streams/pmeta=true/layout=compacted/window=edge/filter=none/s3=100ms | 206 | 206 | 1/1 | 2 | 1.86M | scan | 105 | 105 | 1/1 | 2 | 1.86M | scan | — | 0.51× |
| traces.streams/pmeta=true/layout=compacted/window=edge/filter=svc/s3=0ms | 6.9 | 7.8 | 4/4 | 2 | 1.86M | scan | 3.6 | 6.8 | 4/4 | 2 | 1.86M | scan | — | 0.52× |
| traces.streams/pmeta=true/layout=compacted/window=edge/filter=svc/s3=100ms | 206 | 206 | 1/1 | 2 | 1.86M | scan | 104 | 104 | 1/1 | 2 | 1.86M | scan | — | 0.51× |
| traces.streams/pmeta=true/layout=compacted/window=narrow/filter=none/s3=0ms | 1.8 | 3.1 | 4/4 | 1 | 962K | scan | 1.6 | 1.9 | 4/4 | 1 | 962K | scan | — | 0.88× |
| traces.streams/pmeta=true/layout=compacted/window=narrow/filter=none/s3=100ms | 102 | 102 | 1/1 | 1 | 962K | scan | 103 | 103 | 1/1 | 1 | 962K | scan | — | 1.01× |
| traces.streams/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=0ms | 1.9 | 2.7 | 4/4 | 1 | 962K | scan | 2.9 | 4.5 | 4/4 | 1 | 962K | scan | — | 1.52× |
| traces.streams/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=100ms | 102 | 102 | 1/1 | 1 | 962K | scan | 103 | 103 | 1/1 | 1 | 962K | scan | — | 1.01× |
| traces.streams/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms | 5.5 | 6.5 | 4/4 | 2 | 1.86M | scan | 3.7 | 4.6 | 4/4 | 2 | 1.86M | scan | — | 0.68× |
| traces.streams/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms | 208 | 208 | 1/1 | 2 | 1.86M | scan | 108 | 108 | 1/1 | 2 | 1.86M | scan | — | 0.52× |
| traces.streams/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms | 15.0 | 15.4 | 4/4 | 2 | 1.86M | scan | 12.4 | 30.4 | 4/4 | 2 | 1.86M | scan | — | 0.82× |
| traces.streams/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms | 219 | 219 | 1/1 | 2 | 1.86M | scan | 113 | 113 | 1/1 | 2 | 1.86M | scan | — | 0.51× |
| traces.streams/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms | 6.6 | 7.6 | 4/4 | 13 | 1.18M | scan | 2.5 | 3.1 | 4/4 | 13 | 1.18M | scan | — | 0.38× |
| traces.streams/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms | 1317 | 1317 | 1/1 | 13 | 1.18M | scan | 104 | 104 | 1/1 | 13 | 1.18M | scan | — | 0.08× |
| traces.streams/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms | 12.0 | 13.8 | 4/4 | 13 | 1.18M | scan | 2.6 | 3.2 | 4/4 | 13 | 1.18M | scan | — | 0.22× |
| traces.streams/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms | 1326 | 1326 | 1/1 | 13 | 1.18M | scan | 104 | 104 | 1/1 | 13 | 1.18M | scan | — | 0.08× |
| traces.streams/pmeta=true/layout=flushed/window=edge/filter=none/s3=0ms | 2.7 | 2.8 | 4/4 | 5 | 462K | scan | 1.2 | 1.9 | 4/4 | 5 | 462K | scan | — | 0.45× |
| traces.streams/pmeta=true/layout=flushed/window=edge/filter=none/s3=100ms | 507 | 507 | 1/1 | 5 | 462K | scan | 103 | 103 | 1/1 | 5 | 462K | scan | — | 0.20× |
| traces.streams/pmeta=true/layout=flushed/window=edge/filter=svc/s3=0ms | 4.4 | 11.8 | 4/4 | 5 | 462K | scan | 2.5 | 3.4 | 4/4 | 5 | 462K | scan | — | 0.57× |
| traces.streams/pmeta=true/layout=flushed/window=edge/filter=svc/s3=100ms | 512 | 512 | 1/1 | 5 | 462K | scan | 102 | 102 | 1/1 | 5 | 462K | scan | — | 0.20× |
| traces.streams/pmeta=true/layout=flushed/window=narrow/filter=none/s3=0ms | 0.519 | 0.566 | 4/4 | 1 | 93K | scan | 0.689 | 0.942 | 4/4 | 1 | 93K | scan | — | 1.33× |
| traces.streams/pmeta=true/layout=flushed/window=narrow/filter=none/s3=100ms | 101 | 101 | 1/1 | 1 | 93K | scan | 103 | 103 | 1/1 | 1 | 93K | scan | — | 1.02× |
| traces.streams/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=0ms | 0.526 | 0.642 | 4/4 | 1 | 93K | scan | 0.734 | 1.0 | 4/4 | 1 | 93K | scan | — | 1.39× |
| traces.streams/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=100ms | 101 | 101 | 1/1 | 1 | 93K | scan | 103 | 103 | 1/1 | 1 | 93K | scan | — | 1.02× |
| traces.streams/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms | 13.0 | 14.2 | 4/4 | 24 | 2.17M | scan | 2.9 | 4.7 | 4/4 | 24 | 2.17M | scan | — | 0.22× |
| traces.streams/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms | 2475 | 2475 | 1/1 | 24 | 2.17M | scan | 106 | 106 | 1/1 | 24 | 2.17M | scan | — | 0.04× |
| traces.streams/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms | 25.1 | 31.7 | 4/4 | 24 | 2.17M | scan | 5.5 | 9.5 | 4/4 | 24 | 2.17M | scan | — | 0.22× |
| traces.streams/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms | 2491 | 2491 | 1/1 | 24 | 2.17M | scan | 117 | 117 | 1/1 | 24 | 2.17M | scan | — | 0.05× |
| traces.streams/pmeta=true/layout=peer/window=cut/filter=none/s3=0ms | 8.4 | 10.0 | 4/4 | 13 | 1.18M | scan | 3.0 | 5.0 | 4/4 | 13 | 1.18M | scan | — | 0.36× |
| traces.streams/pmeta=true/layout=peer/window=cut/filter=none/s3=100ms | 1322 | 1322 | 1/1 | 13 | 1.18M | scan | 104 | 104 | 1/1 | 13 | 1.18M | scan | — | 0.08× |
| traces.streams/pmeta=true/layout=peer/window=cut/filter=svc/s3=0ms | 13.8 | 18.8 | 4/4 | 13 | 1.18M | scan | 3.9 | 5.8 | 4/4 | 13 | 1.18M | scan | — | 0.28× |
| traces.streams/pmeta=true/layout=peer/window=cut/filter=svc/s3=100ms | 1324 | 1324 | 1/1 | 13 | 1.18M | scan | 107 | 107 | 1/1 | 13 | 1.18M | scan | — | 0.08× |
| traces.streams/pmeta=true/layout=peer/window=edge/filter=none/s3=0ms | 2.8 | 4.0 | 4/4 | 5 | 462K | scan | 1.2 | 1.5 | 4/4 | 5 | 462K | scan | — | 0.41× |
| traces.streams/pmeta=true/layout=peer/window=edge/filter=none/s3=100ms | 509 | 509 | 1/1 | 5 | 462K | scan | 104 | 104 | 1/1 | 5 | 462K | scan | — | 0.21× |
| traces.streams/pmeta=true/layout=peer/window=edge/filter=svc/s3=0ms | 5.1 | 5.7 | 4/4 | 5 | 462K | scan | 1.6 | 2.1 | 4/4 | 5 | 462K | scan | — | 0.31× |
| traces.streams/pmeta=true/layout=peer/window=edge/filter=svc/s3=100ms | 512 | 512 | 1/1 | 5 | 462K | scan | 116 | 116 | 1/1 | 5 | 462K | scan | — | 0.23× |
| traces.streams/pmeta=true/layout=peer/window=narrow/filter=none/s3=0ms | 0.544 | 0.769 | 4/4 | 1 | 93K | scan | 0.819 | 0.898 | 4/4 | 1 | 93K | scan | — | 1.51× |
| traces.streams/pmeta=true/layout=peer/window=narrow/filter=none/s3=100ms | 102 | 102 | 1/1 | 1 | 93K | scan | 104 | 104 | 1/1 | 1 | 93K | scan | — | 1.02× |
| traces.streams/pmeta=true/layout=peer/window=narrow/filter=svc/s3=0ms | 0.551 | 0.649 | 4/4 | 1 | 93K | scan | 0.736 | 1.1 | 4/4 | 1 | 93K | scan | — | 1.33× |
| traces.streams/pmeta=true/layout=peer/window=narrow/filter=svc/s3=100ms | 101 | 101 | 1/1 | 1 | 93K | scan | 101 | 101 | 1/1 | 1 | 93K | scan | — | 1.00× |
| traces.streams/pmeta=true/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 22 | 1.99M | scan | 29.4 | 43.2 | 4/4 | 22 | 1.99M | scan | — | before invalid (fast-wrong) → now exact |
| traces.streams/pmeta=true/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 22 | 1.99M | scan | 127 | 127 | 1/1 | 22 | 1.99M | scan | — | before invalid (fast-wrong) → now exact |
| traces.streams/pmeta=true/layout=peer/window=whole/filter=svc/s3=0ms | [22.6]† | [25.5]† | 0/4 (set 4) | 22 | 1.99M | scan | 30.4 | 36.2 | 4/4 | 22 | 1.99M | scan | — | [1.35×]† |
| traces.streams/pmeta=true/layout=peer/window=whole/filter=svc/s3=100ms | [2245]† | [2245]† | 0/1 (set 1) | 22 | 1.99M | scan | 129 | 129 | 1/1 | 22 | 1.99M | scan | — | [0.06×]† |
| traces.field_names/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 2 | 816K | scan | invalid | invalid | 0/4 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 2 | 816K | scan | invalid | invalid | 0/1 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/4 | 2 | 816K | scan | invalid | invalid | 0/4 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/1 | 2 | 816K | scan | invalid | invalid | 0/1 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=compacted/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 2 | 816K | scan | invalid | invalid | 0/4 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=compacted/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 2 | 816K | scan | invalid | invalid | 0/1 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=compacted/window=edge/filter=svc/s3=0ms | invalid | invalid | 0/4 | 2 | 816K | scan | invalid | invalid | 0/4 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=compacted/window=edge/filter=svc/s3=100ms | invalid | invalid | 0/1 | 2 | 816K | scan | invalid | invalid | 0/1 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=compacted/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 2 | 818K | scan | invalid | invalid | 0/4 | 2 | 818K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=compacted/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 2 | 818K | scan | invalid | invalid | 0/1 | 2 | 818K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=0ms | invalid | invalid | 0/4 | 2 | 818K | scan | invalid | invalid | 0/4 | 2 | 818K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=compacted/window=narrow/filter=svc/s3=100ms | invalid | invalid | 0/1 | 2 | 818K | scan | invalid | invalid | 0/1 | 2 | 818K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 2 | 816K | scan | invalid | invalid | 0/4 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 2 | 816K | scan | invalid | invalid | 0/1 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/4 | 2 | 816K | scan | invalid | invalid | 0/4 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/1 | 2 | 816K | scan | invalid | invalid | 0/1 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=edge/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=edge/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 1 | 93K | scan | invalid | invalid | 0/4 | 1 | 93K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 1 | 93K | scan | invalid | invalid | 0/1 | 1 | 93K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 93K | scan | invalid | invalid | 0/4 | 1 | 93K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=narrow/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 93K | scan | invalid | invalid | 0/1 | 1 | 93K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=edge/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=edge/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 1 | 93K | scan | invalid | invalid | 0/4 | 1 | 93K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 1 | 93K | scan | invalid | invalid | 0/1 | 1 | 93K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=narrow/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 93K | scan | invalid | invalid | 0/4 | 1 | 93K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=narrow/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 93K | scan | invalid | invalid | 0/1 | 1 | 93K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=false/layout=peer/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | index | invalid | invalid | 0/4 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | index | invalid | invalid | 0/1 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/4 | 2 | 816K | scan | invalid | invalid | 0/4 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/1 | 2 | 816K | scan | invalid | invalid | 0/1 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | index | invalid | invalid | 0/4 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | index | invalid | invalid | 0/1 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=edge/filter=svc/s3=0ms | invalid | invalid | 0/4 | 2 | 816K | scan | invalid | invalid | 0/4 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=edge/filter=svc/s3=100ms | invalid | invalid | 0/1 | 2 | 816K | scan | invalid | invalid | 0/1 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | index | invalid | invalid | 0/4 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | index | invalid | invalid | 0/1 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=0ms | invalid | invalid | 0/4 | 2 | 818K | scan | invalid | invalid | 0/4 | 2 | 818K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=narrow/filter=svc/s3=100ms | invalid | invalid | 0/1 | 2 | 818K | scan | invalid | invalid | 0/1 | 2 | 818K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | index | invalid | invalid | 0/4 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | index | invalid | invalid | 0/1 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/4 | 2 | 816K | scan | invalid | invalid | 0/4 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/1 | 2 | 816K | scan | invalid | invalid | 0/1 | 2 | 816K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | index | invalid | invalid | 0/4 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | index | invalid | invalid | 0/1 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | index | invalid | invalid | 0/4 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | index | invalid | invalid | 0/1 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=edge/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=edge/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | index | invalid | invalid | 0/4 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | index | invalid | invalid | 0/1 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 93K | scan | invalid | invalid | 0/4 | 1 | 93K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=narrow/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 93K | scan | invalid | invalid | 0/1 | 1 | 93K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | index | invalid | invalid | 0/4 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | index | invalid | invalid | 0/1 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=cut/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | index | invalid | invalid | 0/4 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=cut/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | index | invalid | invalid | 0/1 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=edge/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | index | invalid | invalid | 0/4 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=edge/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | index | invalid | invalid | 0/1 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=edge/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=edge/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=narrow/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | index | invalid | invalid | 0/4 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=narrow/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | index | invalid | invalid | 0/1 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=narrow/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 93K | scan | invalid | invalid | 0/4 | 1 | 93K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=narrow/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 93K | scan | invalid | invalid | 0/1 | 1 | 93K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=whole/filter=none/s3=0ms | invalid | invalid | 0/4 | 0 | 0 | index | invalid | invalid | 0/4 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=whole/filter=none/s3=100ms | invalid | invalid | 0/1 | 0 | 0 | index | invalid | invalid | 0/1 | 0 | 0 | index | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/4 | 1 | 92K | scan | invalid | invalid | 0/4 | 1 | 92K | scan | — | after invalid |
| traces.field_names/pmeta=true/layout=peer/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/1 | 1 | 92K | scan | invalid | invalid | 0/1 | 1 | 92K | scan | — | after invalid |

† no iteration had exact hits (catalog and label-index answers carried hits=1); bracketed timing is over set-exact iterations only.

