# Metadata + S3 optimization architecture

How Lakehouse scales to PB while staying fast on restart, serving
latest data without flush waits, and minimizing S3 traffic. Use
this doc as the reference when reasoning about query latency,
restart behaviour, or cost — every optimization listed here has
a config knob, a metric, and a documented edge case.

Companion docs:
- `docs/architecture/restart-and-warmup-design.md` — lifecycle spec
- `docs/architecture/scaling-restart-scenarios.md` — worst-case audit
- `docs/operations/lifecycle.md` — operator-facing semantics
- `docs/operations/sizing.md` — capacity matrix

## High-level layout

```
                         ┌─────────────────────────┐
                         │       Grafana / API     │
                         └────────────┬────────────┘
                                      │  Loki / Tempo / Jaeger / VL HTTP
                                      ▼
        ┌─────────────────────────────────────────────────────┐
        │           Lakehouse pod (topology=all)              │
        │                                                     │
        │  ┌─────────────────────────────────────────────┐    │
        │  │            Select path                      │    │
        │  │  ┌─────────────┐  ┌──────────────────────┐  │    │
        │  │  │  Manifest   │→ │  Per-file projection │  │    │
        │  │  │  (in-mem +  │  │   + filter pushdown  │  │    │
        │  │  │   sortedPart│  └────────┬─────────────┘  │    │
        │  │  │   index)    │           ▼                │    │
        │  │  └──────┬──────┘  ┌──────────────────────┐  │    │
        │  │         │         │  Footer cache (LRU)  │  │    │
        │  │         │         └────────┬─────────────┘  │    │
        │  │         │                  ▼                │    │
        │  │         │         ┌──────────────────────┐  │    │
        │  │         │         │   Bloom + zone-map   │  │    │
        │  │         │         │    skip-decisions    │  │    │
        │  │         │         └────────┬─────────────┘  │    │
        │  │         │                  ▼                │    │
        │  │         │         ┌──────────────────────┐  │    │
        │  │         │         │  Smart cache (mem +  │  │    │
        │  │         │         │  disk L1+L2)         │  │    │
        │  │         │         └────────┬─────────────┘  │    │
        │  │         │                  ▼                │    │
        │  │         │         ┌──────────────────────┐  │    │
        │  │         │         │   s3reader pool +    │  │◀═══╪═══ S3
        │  │         │         │   coalescing reader  │  │    │
        │  │         │         └──────────────────────┘  │    │
        │  │         ▼                                   │    │
        │  │  ┌─────────────────────────────────────┐    │    │
        │  │  │  BufferBridge (self + peer fanout)  │◀───┼────┼── peer insert pods
        │  │  └────────────────┬────────────────────┘    │    │
        │  └───────────────────│─────────────────────────┘    │
        │                      ▼                              │
        │  ┌─────────────────────────────────────────────┐    │
        │  │            Insert path                      │    │
        │  │  ┌──────────┐  ┌──────────┐  ┌───────────┐  │    │
        │  │  │  Buffer  │→ │  WAL +   │→ │  Parquet  │──┼────┼─▶ S3 (.parquet)
        │  │  │ (5m TTL) │  │  flush   │  │  writer   │  │    │
        │  │  └──────────┘  └──────────┘  └─────┬─────┘  │    │
        │  │                                     │       │    │
        │  │       ┌─────────────────────────────┘       │    │
        │  │       ▼                                     │    │
        │  │  ┌──────────────┐ ┌──────────────────────┐  │    │
        │  │  │  Footer KV   │ │  Metadata sidecar    │──┼────┼─▶ S3 (.json)
        │  │  │ (_trace_idx) │ │  (_file_metadata)    │  │    │
        │  │  └──────────────┘ └──────────────────────┘  │    │
        │  └─────────────────────────────────────────────┘    │
        │                                                     │
        └─────────────────────────────────────────────────────┘

                ┌─────────────────────────────────────┐
                │  Local disk (PVC)                   │
                │  ├── manifest-snapshot.bin (gob)    │
                │  ├── footer-cache-snapshot.bin (LRU)│
                │  ├── wal/                           │
                │  └── cache/ (L2 disk)               │
                └─────────────────────────────────────┘
```

## Where each S3 byte comes from

Every S3 read in steady state falls into one of five buckets,
listed in order of cost-efficiency. Optimizations push reads up
the list, into cheaper buckets.

| Bucket | Bytes/query | Optimization |
| --- | --- | --- |
| 1. Manifest LIST during refresh | ~50 KB × partitions, every 30 s | Tenant-scoped LIST (only `{account}/{project}/` prefixes), parallel per-partition LISTs |
| 2. Footer prefetch (range-read) | 64 KiB per cold file | LRU footer cache + snapshot persistence (see §1) |
| 3. Footer 2-phase upgrade | Footer-size bytes for files > 64 KiB footer | Single targeted range-read after trailer reveals footer length (see §2) |
| 4. Bloom + zone-map skip | 0 bytes when file is provably empty for the query | Manifest-level labels, footer-stat zone-maps, bloom filter on `service.name` |
| 5. Column data | Only for files surviving §4, only the projected columns | Smart cache (L1 mem + L2 disk), coalescing reader for adjacent ranges |

## §1 Restart parity with hot — instant data, instant cache

### Buffer-bridge self-endpoint (single-node fast-start)

```
        ╭──────────────╮       ╭──────────────╮
        │  Insert path │       │  Select path │
        │  (this pod)  │       │  (this pod)  │
        │              │       │              │
        │  In-memory   │──────▶│  Cold tier   │
        │  buffer      │  HTTP │  parquet     │
        │ (5m TTL)     │       │  + buffer    │
        ╰──────┬───────╯       ╰──────────────╯
               │ /internal/buffer/query?start=...&end=...
               └────────self-loop on localhost ──────────┘

         ▲                                              │
         │                                              ▼
                   ╭──────────────────────────╮
                   │  BufferBridge fanout     │
                   │  endpoints = [self] when │
                   │  no peers discovered     │
                   ╰──────────────────────────╯
```

Single-node deployments self-loop via the BufferBridge so cold
queries against the last `flush-interval` window are served
from the writer's own memory. In a cluster, `DiscoverPeers`
returns the headless-service member set and the self-fallback
silently steps aside.

### Footer-cache snapshot (persistent warm cache)

```
   shutdown:                                  startup:
                                                                  
   manifest.SaveTo("manifest-snap.bin")        manifest.LoadFrom(...)
                                                 │
                                                 ▼
   SaveFooterCacheKeys(fc, "footer-cache.bin")  LoadFooterCacheKeys(...)
   ▲                                              │
   │                                              ▼
   ╭───────────╮                          ╭──────────────────╮
   │  LRU keys │                          │  Manifest lookup │
   │  newest   │                          │  (O(1) by key)   │
   │   first   │                          ╰────────┬─────────╯
   ╰───────────╯                                   ▼
                                          ╭──────────────────╮
                                          │  prefetchFooters │
                                          │  concurrency=32  │
                                          │  async, post-    │
                                          │  /ready=200      │
                                          ╰──────────────────╯
```

The snapshot persists just the key list (no footer bytes — those
are reconstructed via S3 range-reads). Cost on shutdown is a few
KiB even at PB scale. On the next start the prefetch runs
concurrently with the rest of the warmup chain, so first user
queries arrive with the warmest files already cached.

## §2 Two-phase footer fetch

```
   ┌─────────────────┐
   │  S3 file (N MB) │
   │ ┌─────────────┐ │
   │ │ Column data │ │
   │ │  (lazy)     │ │
   │ │             │ │
   │ │             │ │
   │ │             │ │
   │ ├─────────────┤ │
   │ │ Footer +    │ │
   │ │ Trailer     │ │
   │ └─────────────┘ │
   └─────────────────┘
                                 1st range: tail 64 KiB
                                                ↓ contains last 8 bytes
                                                ↓ = (footerLen + "PAR1")
                                                
                                 If footer fits in 64 KiB → done
                                 
                                 Else: 2nd range = exact footerLen+8
                                       from offset (fileSize - footerLen - 8)
                                       
                                 → guaranteed single-RTT in all cases
```

Trace parquet footers can grow to MB-scale when the embedded
`_trace_idx` KV metadata accumulates many distinct trace IDs.
The 2-phase fetch caps this at one extra RTT — never falls
through to full-file scan.

## §3 Metadata written at flush time

Every parquet flush emits THREE persistent artifacts in one S3
PutObject batch, so all metadata is queryable as soon as the
manifest sees the file:

```
   flush()
     ├─ <file>.parquet
     │   └─ Trace IDs encoded in footer's key_value_metadata.
     │      Read-tools (duckdb, pyarrow, parquet-tools) see them
     │      because they're standard Parquet metadata — no LH
     │      framing.
     │
     ├─ <partition>/_file_metadata.json — sidecar with stream IDs,
     │   row counts, raw_bytes per tenant, time bounds, schema
     │   fingerprint. Avoids re-reading footers for /api/v1/tenants
     │   and other meta queries.
     │
     └─ Bloom filters (column-level, embedded in parquet)
         for `service.name`, `trace_id`, `level`. Built at write
         time by the parquet writer — no post-hoc backfill.
```

The trace-shape filter at ingest, the severity_text derivation
chain (severity_number → text → stream-tag lift), and the
SeverityText backfill at compaction all act on the SAME write
path so query-time results stay consistent with what was written.

## §4 Serve-while-warming three-state /ready

```
                  ┌───────┐
                  │  init │
                  └───┬───┘
                      │ Read manifest from disk (gob, streaming)
                      ▼
              ┌───────────────┐
              │ disk_recovery │   →  ServingReady false  →  /ready=503
              └───────┬───────┘
                      │ MinManifestFiles met?
                      ▼
              ┌───────────────┐
              │   s3_refresh  │   →  ServingReady true   →  /ready=204
              └───────┬───────┘     (queries answered, warmup ongoing)
                      │ Manifest refreshed from S3, buffer restored
                      ▼
              ┌───────────────┐
              │ cache_warmup  │   →  Footer prefetch + sample cache prep
              └───────┬───────┘
                      │ Background loop ends
                      ▼
              ┌───────────────┐
              │     ready     │   →  WarmupComplete       →  /ready=200
              └───────────────┘     (lifecycle k8s ALB removes 503 mark)
```

Queries see real data at `/ready=204` and load-balancers route
to fully-warm pods at `/ready=200`. Without this split, kube-
probe semantics force every new pod to block all traffic until
the slowest warmup partition finishes — at PB scale that's
minutes of cluster-wide 503.

## §5 Coalescing reader + s3reader pool

```
   request stream:   [GET range 0–4MB]
                     [GET range 5–8MB]    ← adjacent or near-adjacent ranges
                     [GET range 9–11MB]
                                            ↓
   coalescing:       [GET range 0–11MB]   ← single round-trip
                                            ↓
   smart cache:      L1 mem   ──hits──▶   serve
                       │
                       └─miss─▶ L2 disk ──hits──▶ serve + promote to L1
                                  │
                                  └─miss─▶ S3
                                            │
                                            └─▶ full-jitter retry on
                                                SlowDown / 5xx
                                                (see §6)
```

The coalescing layer turns scattered column reads into a few
big sequential ones — the access pattern S3 is designed for. At
PB scale the difference is a 6-10× throughput multiplier vs the
naive per-column GET.

## §6 S3 throttling resilience

```
   request fails (SlowDown / ServiceUnavailable / 5xx)
        │
        ▼
   attempt N:  base = 100 ms × 2^N   capped at 5 s
        │
        ▼
   jitter = rand(0, base)   ← full Marc-Brooker jitter,
        │                      not "base + rand"
        ▼
   sleep(jitter); retry
```

Full jitter avoids the phase-locked retry storms that bring
multiple peers down at once. Concrete observation: 6 peers
restarting simultaneously generate 6× the load on the first
retry window; with full jitter their retries spread uniformly
across [0, base), and S3 sees the same load it would on a
rolling restart.

## End-to-end scenarios

### Scenario A — single-pod rolling restart, instant data

1. Pod stops (SIGTERM). Manifest + footer-cache snapshots persist
   under `cfg.Shutdown.PersistTimeout` (default 30 s).
2. New pod starts. Manifest loads from disk in milliseconds via
   streaming gob decode. `/ready=503`.
3. buffer restores buffered rows that didn't make it to S3 before
   shutdown. `/ready=503` still.
4. `MinManifestFiles` gate met → `ServingReady` flips. `/ready=204`.
5. **BufferBridge self-endpoint** is registered. Queries against
   the last 5 min hit the freshly-replayed in-memory buffer and
   return instantly.
6. **Footer-cache snapshot** loads. Async prefetch starts hydrating
   the LRU.
7. Periodic warmup completes (MaxTimeNs DESC priority). `/ready=200`.

User-visible: cold-start to first-real-query is ≤1 s in the e2e
stack and ≤10 s at PB scale. No "data hole" window — the buffer
covers the last flush interval.

### Scenario B — cluster simultaneous restart

1. All N peers restart at once (chaos engineer / orchestrator
   accident).
2. Each peer hits the local snapshot path of (A). No peer can
   serve another peer's buffer because nobody has data yet.
3. The data-cold flag (planned: task #75) will return `/ready=503`
   for the buffer-bridge window so callers retry rather than seeing
   incomplete results.
4. Once peers cross flush-interval, BufferBridge resumes serving
   from peers — same shape as steady state.

Edge case: queries during the 5-min cold window will see only
post-restart-flushed parquet rows. Document as "expected outage"
in `docs/operations/lifecycle.md`.

### Scenario C — trace-by-ID drilldown on a brand-new trace

1. User clicks a `trace_id=` derived field in a VL log line.
2. Grafana POSTs `/api/traces/<id>` to lakehouse-traces.
3. Trace-ID fast path looks up `<id>` in the in-memory trace
   index (built from manifest's per-file `_trace_idx` slice on
   manifest load).
4. **If found in 1 file**: range-read the footer (64 KiB +
   2-phase fallback if needed), parse the embedded
   `_trace_idx` KV, locate the row group, project just the
   span-relevant columns. Latency: ~40 ms cached, ~200-1400 ms
   cold first hit.
5. **If found in N files**: parallel range-reads, merge spans
   client-side.
6. **If not found in any flushed file**: BufferBridge fanout
   queries every insert pod's `/internal/buffer/query` for
   un-flushed spans. The user sees real-time traces with no
   "wait for flush" delay.

### Scenario D — PB-scale wide-time-range query

1. User runs `_time:30d | service.name=foo | count()`.
2. Manifest returns ~50k matching files (30 days × ~1700/day at
   PB ingest).
3. Bloom + zone-map skip eliminates ~90% of files (the ones
   without the requested service).
4. Smart cache L1 serves ~30% of remaining footers; L2 disk
   serves ~50%; S3 footers fetched for ~20%.
5. Column projection means we only read the `service.name`
   column for each surviving row group, not the rest of the
   schema. That's ~1% of total parquet bytes.
6. Final answer comes back in seconds for a query that on a
   naive implementation would have read terabytes.

The compounding factor matters: each optimization is 2-10×, and
they multiply. A query that touches 1 PB of logical data may
actually read 1-10 GB of S3 bytes.

## Operator-visible metrics

Reference table mapping each optimization to the metric that
proves it's working:

| Optimization | Metric | Expected steady-state |
| --- | --- | --- |
| Buffer-bridge self-endpoint | `lakehouse_buffer_bridge_az_requests_total{az_type="self"}` | > 0 in single-node mode |
| Footer-cache hits | `lakehouse_footer_cache_hits_total` | > 90% of `lakehouse_footer_cache_lookups_total` |
| Footer prefetch snapshot load | `lakehouse_footer_cache_entries` (right after restart) | ≈ pre-shutdown count |
| Two-phase footer fetch | absence of `"footer larger than prefetch tail"` warnings | 0 warnings/hour |
| Trace-index fast path | `lakehouse_trace_index_lookups_total{result="hit"}` | > 99% of total lookups |
| S3 throttle resilience | `lakehouse_s3_throttle_total` flat over time | not climbing without bounded retries |
| Bloom skip | `lakehouse_parquet_files_skipped_bloom_total` / `lakehouse_parquet_files_opened_total` | > 80% for tenant-scoped queries |
| Cache snapshot age | `lakehouse_manifest_snapshot_age_seconds` | ≤ 2 × `cfg.Manifest.PersistInterval` |
| Lifecycle phase visibility | `lakehouse_serving_ready`, `lakehouse_warmup_complete` | both 1 at steady state |

If any of these drift, the corresponding optimization is silently
degraded.

## Anti-patterns we deliberately avoid

- **In-memory column store at S3 scale** — would force every pod
  to hold the full working set. Smart cache disk L2 fills the
  same role without the memory ceiling.
- **Server-side row-level deletes** — we use compaction + tombstones
  instead so S3 stays append-only and we don't pay for in-place
  rewrites.
- **Custom Parquet framing** — every file is readable by
  duckdb / pyarrow / parquet-tools out of the box. The trace
  index lives in the standard `key_value_metadata` slot, not a
  custom magic header. Operators can spot-check files without
  the lakehouse binary.
- **Eager full warmup** — at PB scale that would mean downloading
  TBs at every restart. The MaxTimeNs DESC priority + max-files
  cap keeps the warmup bounded; the snapshot-driven prefetch
  picks up the slack on the post-warmup async path.
