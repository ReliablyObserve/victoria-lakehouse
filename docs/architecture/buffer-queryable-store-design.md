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

Adopt the VT/VL model (**Option B**): the LH insert buffer becomes a real
per-pod `logstorage.Storage` (in-memory parts), queried via its native
`RunQuery`. The Parquet→S3 flush becomes a **conversion from that buffer**
(**variant b2**): intercept the single seam where VL persists an in-memory part
to disk, iterate that part's rows, and run LH's existing Parquet writer +
index builders. All LH indexes/helpers, the BufferBridge peer fan-out, WAL, and
compaction are preserved. Nothing under `deps/` is forked; the one required
unexported-symbol access is added through the existing `patches/` mechanism
(precedent: `vl-export-streamtags-get.patch`).

Applies to **both** logs (`internal/`) and traces (`lakehouse-traces/internal/`),
which already carry separate patched `deps/VictoriaLogs` trees.

## The seam (verified)

`deps/VictoriaLogs/lib/logstorage/datadb.go :: mustMergePartsInternal`
(line ~489) is where data leaves memory for "disk". Two write paths:

- **fast path** (line 538): `isFinal && len(pws)==1 && pws[0].mp != nil` →
  `mp.MustStoreToDisk(dstPartPath)`.
- **merge path** (line 551+): N parts → `bsw.MustInitForFilePart(dstPartPath)`
  via `mustMergeBlockStreams`.

Both fire only when `dstPartType != partInmemory` (i.e. data is being
persisted). That single predicate is the interception point.

Row materialization for the conversion reuses, inside `package logstorage`:
`mustOpenBlockStreamReaders(pws)` → `blockStreamReader.NextBlock()` →
`blockData.unmarshalRows(&rows, …)` → `rows{timestamps []int64, rows [][]Field}`,
plus the block's `streamID` → canonical stream tags via `ddb.pt.idb`.

`maxUncompressedBlockSize = 2 MiB` (`consts.go`); LH never sets it — VL owns
block splitting in `inmemoryPart.mustInitFromRows`.

## Target architecture

```
INSERT pod (or role=all)
  vtinsert/vlinsert ── lr *logstorage.LogRows ── store.MustAddRows(lr)   [REUSE]
        │
        ▼  logstorage.Storage  (the in-mem queryable buffer; REUSED verbatim)
        │   day-partition → indexdb(streams) → datadb.rowsBuffer → inmemoryPart
        │   inmemoryPartsFlusher → mustMergePartsInternal
        │        ╔═════ SEAM (one patch): dstPartType != partInmemory ═════╗
        │        ║ if lhFlushSink != nil { lhFlushSink(pws); dropSrc; ret } ║
        │        ╚═══════════════════════════════════════════════════════════╝
        ▼  parquets3 sink (NEW, thin): iterate part rows →
             extractLog/TraceBloomValues, computeTraceIndex, buildTokenBloom  [REUSE]
             → writeLogs/TracesParquet → S3 → manifest.AddFile + sidecar      [REUSE]
             → bloomObserver.OnFileFlush/PersistDirty, cacheOnFlush, WAL.Truncate [REUSE]

SELECT pod (or role=all): parquets3.Storage.RunQuery
  (A) S3 Parquet scan (manifest→prefilter→bloom→trace_idx→workers)            [REUSE]
  (B) buffer rows:
        co-located store → localBuf.RunQuery(q, writeBlock)                   [Option B]
        cross-pod        → BufferBridge HTTP fan-out                          [PRESERVED]
  merge via filteredWriteBlock                                                [REUSE]
```

Once a part is flushed to Parquet it is **dropped** from the VL store, so the
store holds only the unflushed window; flushed data lives in Parquet and is
queried by the existing Parquet scan. No double-counting.

## Reuse surface (exported, no change)

`logstorage.GetLogRows`, `(*LogRows).MustAdd` / `MustAddInsertRow` / `ForEachRow`,
`MustOpenStorage`, `StorageConfig`, `(*Storage).MustAddRows`,
`(*Storage).RunQuery`, `(*Storage).DebugFlush`, `WriteDataBlockFunc`,
`*DataBlock`, `*Query`, `RunQueryExternalWithSubqueries`. LH's existing artifact
builders and `BufferBridge`/`buffer.Handler` are reused unchanged — their inputs
are simply re-sourced from the part rows.

## Patches (sanctioned `patches/` channel, both vl-logs and vl-traces)

1. `vl-flush-sink.go.src` — NEW file in `package logstorage`: exported
   `var FlushSink func(emit FlushEmitter) (handled bool)` registration + the
   internal `convertPartsForFlush(pws, emit)` that reuses block readers +
   `unmarshalRows` and resolves stream tags. Emits plain
   `(streamTagsCanonical string, timestamp int64, fields []logstorage.Field)` —
   LH never touches unexported types.
2. `vl-flush-sink-hook.patch` — git-apply: insert the `dstPartType !=
   partInmemory && FlushSink != nil` branch at `mustMergePartsInternal` that
   calls `convertPartsForFlush`, drops the source parts, and returns.

Makefile: add the copy + `git apply` lines to `deps-logs` / `deps-traces`,
mirror under `patches/vl-logs/` and `patches/vl-traces/`. Cache key already
hashes `patches/**` + `Makefile`.

## Phases (each shippable into PR #123, build+tests green at every commit)

- **P0 — export patch + dormant hook.** `FlushSink` nil → behavior unchanged.
  Guard test asserts the exported symbols exist; `logstorage` + LH build green.
- **P1 — per-pod store behind `InsertConfig.BufferEngine` flag (`buffer`|`logstore`).**
  On `logstore`, dual-write: `AddLog/TraceRows` also `store.MustAddRows`. Parquet
  still produced by the legacy path. Parity test: `store.RunQuery` count/fields ==
  `BufferedLog/TraceRows`.
- **P2 — divert flush.** Register `FlushSink`; sink builds Parquet + indexes from
  part rows. Test: ingest→`DebugFlush`→assert Parquet object + `_trace_idx`/bloom
  KVs + `manifest.AddFile` + `cacheOnFlush`.
- **P3 — read merge.** Co-located `queryBufferBridge` calls `localBuf.RunQuery`
  instead of struct→DataBlock. e2e parity `buffer` vs `logstore`.
- **P4 — cross-pod handler streams from store; WAL replay feeds `MustAddRows`;
  retire the row shadow.**
- **P5 — flip default to `logstore`, delete `logBufs/traceBufs`.** Keep the flag
  one release for rollback.

## Open decisions (need human)

- **D1**: unify the traces `FlushHook` and logs `BloomObserver` at the sink
  (recommended) vs keep separate wrappers.
- **D2**: P3 co-located detection — explicit `role=all` check vs a
  `localBuf != nil` capability flag.

## Non-goals

No change to the Parquet on-disk format, the manifest, compaction, the
BufferBridge wire protocol (stays ndjson), or AZ-aware peer routing.
