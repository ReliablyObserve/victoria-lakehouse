# Buffer as a logstorage-native queryable store (Option B)

Status: **accepted, implementation in progress** (PR #123). Owner: cold-tier.

## Problem

LH holds the insert buffer as `[]schema.LogRow` / `[]schema.TraceRow` staging
slices (`BatchWriter.logBufs` / `traceBufs`) and **reconstructs** a
`logstorage.DataBlock` on the query side (`logRowsToDataBlock` /
`traceRowsToDataBlock`) so the select path can merge buffered-but-unflushed
rows with the S3 Parquet scan.

Every column that converter forgets is a silent query gap. This session alone
that bug class produced: buffer rows invisible to `_stream:{…}` filters
(missing `_stream`), Jaeger/Tempo search returning 0 for fresh traces, the
log→trace drilldown crashing Grafana (`Cannot read properties of undefined
(reading 'spanID')`) because GetTrace 404'd on buffered traces (missing
`start_time_unix_nano` / `end_time_unix_nano`), and missing map attributes.
Each was the struct→DataBlock converter drifting from the file-scan emission.

Upstream VT/VL have none of this: ingest goes into `logstorage.LogRows` →
`Storage.MustAddRows`, and queries run over the same in-memory parts via the
same engine. **The buffer *is* the queryable store. Zero conversion.** That is
why VT "just works after restart."

## Decision

Adopt the VT/VL model (**Option B**) using **only exported upstream APIs — no
VL/VT modification.** The LH insert buffer becomes a real per-pod
`logstorage.Storage` (in-memory parts) opened with `MustOpenStorage`, fed via
the exported `MustAddRows`, and queried via the exported `RunQuery`. When the
buffer eventually produces Parquet (final phase), it does so by **reading
itself back through the exported `Storage.RunQuery`** over the just-flushed
window and handing the resulting `DataBlock`s to LH's existing Parquet writer —
again, zero VL changes. All LH indexes/helpers, the BufferBridge peer fan-out,
WAL, and compaction are preserved.

This supersedes an earlier "variant b2" sketch that intercepted VL's
in-memory-part→disk merge via a `patches/` hook (`FlushSink`). That added new
logic *inside* VL; per the project's "reuse upstream, don't modify it" rule it
was removed. The buffer query path (`RunQuery`) and the future Parquet-export
path are both pure exported-API reuse, so no `patches/` entry beyond the
existing `vl-export-*` exports is needed.

Applies to **both** logs (`internal/`) and traces (`lakehouse-traces/internal/`),
which already carry separate patched `deps/VictoriaLogs` trees.

## Reuse-only seam (no VL modification)

Everything runs through exported `logstorage` symbols:

- **Ingest** → `MustOpenStorage(path, *StorageConfig)` once per pod; per batch
  `Storage.MustAddRows(lr)` (the same `*LogRows` VT/VL's insert built).
- **Query** → `Storage.RunQuery(qctx, writeBlock)` with the same `*QueryContext`
  LH's `ExternalStorage.RunQuery` already receives — so the buffer merges into
  the existing query flow with no new types.
- **Parquet export** (final phase) → `Storage.RunQuery` over `[lastFlush, now]`
  yields `DataBlock`s → LH's existing `writeTracesParquet`/`writeLogsParquet` +
  index builders → S3. The buffer's own `FlushInterval`/`Retention` bound how
  much it holds; VL owns block splitting internally.

No interception of VL's merge, no new file in `package logstorage`, no
`deps/` edits.

## Target architecture

```
INSERT pod (or role=all)
  vtinsert/vlinsert ── lr *logstorage.LogRows
        ├─ legacy: logRowsTo{Trace,Schema}Rows → BatchWriter → Parquet  [authoritative]
        └─ Option B: store.MustAddRows(lr)                             [exported REUSE]
        ▼  logstorage.Storage  (the in-mem queryable buffer; REUSED verbatim,
        │   no VL edits) — day-partition → indexdb → rowsBuffer → inmemoryPart
        ▼  Parquet export (P5): store.RunQuery([lastFlush,now]) → DataBlocks →
             writeLogs/TracesParquet → S3 → manifest.AddFile + indexes   [REUSE]

SELECT pod (or role=all): parquets3.Storage.RunQuery
  (A) S3 Parquet scan (manifest→prefilter→bloom→trace_idx→workers)            [REUSE]
  (B) buffer rows (P3):
        co-located store → store.RunQuery(qctx, writeBlock)            [exported REUSE]
        cross-pod        → BufferBridge HTTP fan-out                          [PRESERVED]
  merge via filteredWriteBlock                                                [REUSE]
```

Until P5, the legacy `[]schema.*Row` path remains the authoritative Parquet
producer, so the buffer never double-writes. The buffer's own
`Retention`/`FlushInterval` bound how much it holds; older data is served from
S3 Parquet.

## Reuse surface (exported, no change)

`logstorage.GetLogRows`, `(*LogRows).MustAdd` / `MustAddInsertRow` / `ForEachRow`,
`MustOpenStorage`, `StorageConfig`, `(*Storage).MustAddRows`,
`(*Storage).RunQuery`, `(*Storage).DebugFlush`, `WriteDataBlockFunc`,
`*DataBlock`, `*Query`, `RunQueryExternalWithSubqueries`. LH's existing artifact
builders and `BufferBridge`/`buffer.Handler` are reused unchanged — their inputs
are simply re-sourced from the part rows.

## Patches — none new

This design adds **no** `patches/` entry. It composes the existing exported
`logstorage` surface (`MustOpenStorage`, `MustAddRows`, `RunQuery`,
`DebugFlush`, `QueryContext`, `DataBlock`) plus the `vl-export-*` exports
already in the tree. The earlier `vl-flush-sink.*` patch was reverted.

## Phases (each shippable into PR #123, build+tests green at every commit)

- ~~**P0 — flush-sink export hook.**~~ **Removed** — it modified VL. Superseded
  by the exported-`RunQuery` export path.
- **P1 — per-pod store behind `InsertConfig.BufferEngine` flag (`buffer`|`logstore`).**
  On `logstore`, dual-write: `AddLog/TraceRows` also `store.MustAddRows`. Parquet
  still produced by the legacy path. Parity test: `store.RunQuery` count/fields ==
  legacy buffer. **Done.**
- **P3 — read merge.** Co-located `queryBufferBridge` calls `store.RunQuery`
  instead of the struct→DataBlock conversion. This is the step that fixes the
  cold-tier recently-flushed residual (the buffer serves fresh queries natively).
  e2e parity `buffer` vs `logstore`. **Next.**
- **P4 — cross-pod handler streams from `store.RunQuery`; graceful shutdown
  `DebugFlush`+`Close`; retire the row shadow.**
- **P5 — Parquet from the buffer via exported `RunQuery` export; flip default to
  `logstore`; delete `logBufs/traceBufs` AND the LH WAL.** Keep the flag one
  release for rollback. The legacy `[]schema.*Row` Parquet path stays
  authoritative until this step, so there is never a double-write.

## Durability — reuse VL/VT persistence, no LH WAL

`logstorage.Storage` is already a durable store: in-memory parts are written to
its data dir every `FlushInterval` and read back on `MustOpenStorage`
(`mustReadPartNames`/`mustOpenFilePart`). Its crash-loss window is the last
`FlushInterval` — **identical to VT/VL hot**, which also have no WAL. So the
buffer's durability is 100% upstream:

- The buffer dir lives on a **persistent volume**; restore is automatic.
- **No LH WAL for the buffer** — that would make it *more* durable than VT/VL
  (not parity) and is pure duplication.
- Long-term durability is the S3 Parquet flush.

LH's existing WAL only protects the *legacy* `[]schema.*Row` path (which has no
persistence of its own) and is **removed in P5** when that path is retired. End
state: one ingest+persistence path (`logstorage.Storage`) + S3 Parquet — the
VL/VT model, zero duplication.

## Open decisions (need human)

- **D1**: unify the traces `FlushHook` and logs `BloomObserver` at the sink
  (recommended) vs keep separate wrappers.
- **D2**: P3 co-located detection — explicit `role=all` check vs a
  `localBuf != nil` capability flag.

## Non-goals

No change to the Parquet on-disk format, the manifest, compaction, the
BufferBridge wire protocol (stays ndjson), or AZ-aware peer routing.
