# Restart, warmup, and cluster-coldstart design

> Spec / design doc. Read this BEFORE editing lifecycle, BufferBridge,
> or footer-cache code. The "Decisions" section below records what
> we chose and why; the "Open questions" section is the only place
> to argue for alternatives.

## Problem

At multi-PB scale with multiple peer pods, naive restart behaviour
produces user-visible empty-data windows ranging from "invisible
during normal rolling deploys" to "1-5 min cluster-wide blackout
on simultaneous restart". The original `/ready=200` flipped the
moment the HTTP listener bound, regardless of whether the pod had
data or had finished buffer restore. This lies in five worst cases
documented in `docs/architecture/scaling-restart-scenarios.md`.

This design closes the lies AND makes the warmup window shorter
without paying for it with strictness in dev/CI.

## Design principles

1. **Never lie about /ready.** A pod should not return 200 while
   it cannot honestly answer queries. The trade-off is between
   "503 keeps me out of rotation" (acceptable, recoverable) and
   "200 routes empty results to dashboards" (unacceptable, hides
   the problem).

2. **Operator hint at every cliff.** Every config knob that
   matters at scale has a runtime hint that fires when the
   observed state suggests tuning. Healthy clusters log no
   hints; suboptimal ones log exactly one line per cliff with
   the knob name + recommended value + cost.

3. **Background as much as possible.** The foreground critical
   path is `disk recovery → buffer restore → ServingReady`. Everything
   else (S3 refresh, cache warmup, bloom backfill, snapshot save
   to S3) is background, with the `/ready=204` state communicating
   "ready, still optimising".

4. **No silent data loss.** Snapshot writes are atomic via
   rename, bounded by a configurable timeout that doesn't extend
   past k8s grace. Failed writes don't advance the SavedAt
   timestamp (the age metric stays honest).

5. **Cluster coordination is best-effort.** BufferBridge fan-out,
   peer cache pull, data-cold cluster flag — all are
   nice-to-have optimisations that improve cold-start latency.
   If they fail (peer unreachable, response slow), the local
   path still works correctly, just slower.

## Lifecycle state machine

```text
                ┌─────────┐
                │  init   │
                └────┬────┘
                     ▼
            ┌───────────────┐
            │ disk_recovery │  ← load manifest + footer snapshots
            └───────┬───────┘
                    ▼
            ┌───────────────┐
            │  wal_replay   │  ← insert role only; gates ServingReady
            └───────┬───────┘
                    ▼
       ╔═══════════════════════╗
       ║  min_manifest_files   ║  ← gate ServingReady on file count
       ║       gate            ║
       ╚═══════════╤═══════════╝
                   ▼
             ┌──────────┐
             │  ready   │  ← /ready=204 (warming) OR 200 if no warmup
             └────┬─────┘
                  │ go background
                  ▼
       ┌──────────────────────┐
       │     s3_refresh       │  ← background; manifest delta
       │   cache_warmup       │  ← priority: recent partitions first
       │   bloom_backfill     │  ← per-file bloom population
       └────────┬─────────────┘
                ▼
          ┌──────────────┐
          │ warmup_done  │  ← /ready=200
          └──────────────┘
```

Phases stay in their enum (`PhaseInit..PhaseReady`) for telemetry
continuity. The state additions are the **ServingReady** and
**WarmupComplete** booleans that drive the 503/204/200 split in
`/ready`.

## /ready contract

| `ServingReady` | `WarmupComplete` | `serve_while_warming` | HTTP |
| :-: | :-: | :-: | :-: |
| false | * | * | 503 |
| true  | true | * | 200 |
| true  | false | true | 204 |
| true  | false | false | 503 |

- 503 always means "do not route traffic here."
- 200 always means "fully ready, complete answers."
- 204 is the new escape valve: "serving partial data, opt in to
  receive routing here."

k8s readinessProbe with `successThreshold: 1` accepts any 2xx →
routes 204. helm chart with strict prod profile sets
`successThreshold: 2` → waits for 200. Operator picks.

## `ServingReady` preconditions

```text
ServingReady ≡ servingReadyFlag
              ∧ (¬walReplayNeeded ∨ walReplayDone)
              ∧ (minManifestFiles = 0 ∨ manifestFiles ≥ minManifestFiles)
```

Every precondition is monotonic except `manifestFiles`. The
manifest can shrink (orphan-sweep, retention expiry) which
correctly flips ServingReady BACK to false — the pod is honest
about transient partial outages, not just startup.

## `WarmupComplete` preconditions

Set by exactly one writer: the background goroutine in
`runStartup`, immediately after every step in its sequence:

1. `RefreshManifest(ctx)` (5 min timeout)
2. `WarmLabelIndex(ctx)` + `WarmMetadata(ctx)` (logs-only)
3. `WarmupCache(ctx)` (2 min timeout, gated by config)
4. `SaveTo(snapshotPath)` (persist refreshed manifest)
5. `PruneNotIn(...)` (drop footer cache entries no longer in manifest)
6. `BackfillBloomIndex(ctx)` (10 min timeout, traces-only)

Failure of any step logs an error but does not block transition —
the warmup completes "best-effort" and the next periodic refresh
fixes whatever broke.

## BufferBridge: fresh data on cold pod

| Local state | Action |
| --- | --- |
| Buffer warm (insert role, post-WAL) | Local rows + peer rows merged in BufferBridge fan-out |
| Buffer cold (just restarted) | Peer rows only |
| All buffers cold (simultaneous restart) | **EMPTY for ~2 min until buffer restore + first flush.** Returns 200 with partial data. Future: "data-cold" cluster flag returns 503 retry-after instead. |

Recent data freshness depends on how many peers have warm
buffers. Cluster topology should ensure ≥1 peer is always warm
during deploys (rolling, `maxUnavailable: 1`).

## Manifest snapshots: lifecycle

| Event | Action |
| --- | --- |
| Startup, snapshot exists | LoadFrom, set `savedAt` from gob, populate `lakehouse_manifest_snapshot_age_seconds` |
| Startup, no snapshot | LoadFrom no-op; `savedAt` zero → age gauge reports +Inf sentinel; `MinManifestFiles` gate holds /ready=503 |
| Every `manifest.persist_interval` (default 5 min) | `SaveTo` writes atomic snapshot, updates `savedAt`, refreshes age gauge |
| SIGTERM | **Manifest snapshot runs FIRST**, before `Stop()` calls. Bounded by `cfg.shutdown.persist_timeout` (default 30 s). Failure logs but doesn't block shutdown. |

`savedAt` is updated only after the atomic rename succeeds — a
failed write doesn't reset the age gauge to "fresh".

## Footer cache snapshots (P3 — separate PR)

> Filed as future work. Spec drafted here so the implementation
> stays aligned with the lifecycle design above.

| Event | Action |
| --- | --- |
| Startup | `LoadFromDisk` runs ASYNC in a separate goroutine. ServingReady doesn't wait — first queries pay per-file S3 footer fetch until the load completes (~30-60 s at PB scale). |
| Every persist tick | `SaveToDisk` atomic via rename, like manifest. |
| SIGTERM | Save BEFORE the long `Stop()` calls, same as manifest. Same timeout bound. |

Async load means a fresh pod with 0 footer cache entries answers
queries (slowly) immediately — versus blocking /ready for 30-60 s
while the snapshot loads.

## Cluster-coldstart protection (P2 — planned, RFC stage)

When ALL peers' buffers are cold AND local manifest is at
`MinManifestFiles` minimum, queries today return empty 200. Two
mitigations on the table:

1. **Data-cold cluster flag** — pod broadcasts buffer-state to
   `/internal/buffer/health`. When the aggregate says "no peer
   warm", queries return 503 with `Retry-After: 30s` instead of
   empty 200. Caller (vtselect, Grafana plugin) retries; by then
   buffers have warmed.

2. **BufferBridge proxy mode** — if local manifest is sparse,
   forward cold-tier queries to peers WITH data. Costs an extra
   network hop on the rare path; saves the new pod from "lying
   empty" while it warms.

The two are complementary: #1 handles "ALL pods cold" (worst
case); #2 handles "I'm cold but peers are warm" (rolling restart).

## Configuration knobs

```yaml
startup:
  # Honesty gate. Refuse /ready=200 until manifest holds this
  # many files. 0 = gate off (legacy, fine for dev/CI). At PB
  # scale set this above smallest healthy partition count.
  min_manifest_files: 10000

  # Allow /ready=204 during background warmup. Off by default
  # (strict 200-or-503). Soft routers (vtselect peer fan-out,
  # k8s with successThreshold=1) can opt in for faster fan-in.
  serve_while_warming: true

  max_warmup_time: 5m              # background goroutine budget
  serve_stale: false               # legacy; superseded by gate above
  wal_reconciliation: true         # gates buffer restore on /ready

manifest:
  refresh_interval: 30s            # periodic S3 LIST cadence
  persist_interval: 2m             # snapshot save cadence

shutdown:
  persist_timeout: 30s             # bound the SIGTERM snapshot save

cache:
  footer_max_items: 100000         # auto-tuned to manifest size by default
  warmup_partitions: 12            # most-recent N partitions to pre-load
  warmup_max_files: 2000
```

### Sizing matrix (current defaults vs PB-scale recommendations)

| Scale | `min_manifest_files` | `footer_max_items` | `persist_interval` |
| --- | ---: | ---: | ---: |
| dev / CI (≤1k files) | 0 | 10k (default) | 5m (default) |
| single-node prod (~10k files) | 1k | 10k | 5m |
| small cluster (~100k files) | 10k | 50k | 5m |
| PB-scale (1M+ files) | 100k | 200k+ (~10 GB RAM) | 2m |

## Metrics surface

Every state change writes to a metric. Operators see when the gate flips, when warmup completes, and when the snapshot ages.

| Metric | Type | What it tells you |
| --- | --- | --- |
| `lakehouse_serving_ready` | gauge | 1 = ServingReady, 0 = not |
| `lakehouse_warmup_complete` | gauge | 1 = WarmupComplete |
| `lakehouse_manifest_snapshot_age_seconds` | gauge | wall-clock age of local snapshot |
| `lakehouse_min_manifest_files_gate` | gauge | configured threshold (debug visibility) |
| `lakehouse_manifest_files` | gauge | current file count |
| `lakehouse_startup_phase` | gauge | phase enum 0..6 |
| `lakehouse_startup_total_seconds` | gauge | last cold start total |
| `lakehouse_ready` | gauge | legacy IsReady (true once both flips set) |

## Operator-facing hints (P3 — landed)

VL/VM-style warn-level hints fire after warmup. Healthy clusters
log zero hints; suboptimal ones log exactly one line per cliff:

- `hint:footer-cache` — cache cap << recent file count
- `hint:snapshot-staleness` — loaded snapshot >6× persist interval
- `hint:buffer-peers` — only 1 insert peer visible
- `hint:warmup-time` — warmup >30 s (slow disk or huge delta)
- `hint:ready-gate` — `min_manifest_files=0` with non-trivial manifest

Each hint names the config knob to tune AND quantifies the cost
(memory, latency, time).

## Test contract

| Layer | Tests |
| --- | --- |
| `internal/startup/honesty_test.go` | 16 sub-tests pinning ServingReady preconditions: (manifest_files × WAL needed/done × gate threshold) combinations |
| `internal/startup/hints_test.go` | 6 cases pinning each hint category fires when expected AND silence when healthy |
| `internal/manifest/snapshot_age_test.go` | SaveTo updates SavedAt; LoadFrom restores it; failed SaveTo doesn't advance it |
| e2e compose | rolling-restart probe, simultaneous-restart probe (manual), MinManifestFiles=high triggers /ready=503 |

## Decisions (and what was rejected)

| Question | Answer | Why |
| --- | --- | --- |
| Single `Ready` bool or split? | Split into `ServingReady` + `WarmupComplete` | Lets soft routers route partial-data pods (204) while strict routers wait for 200 |
| MinManifestFiles default? | 0 (gate off) | Don't break dev/CI; PB-scale operators explicitly opt in |
| Snapshot format on disk? | Binary gob with magic prefix + size cap | 3-5× smaller than JSON, decoder bounded against DoS |
| Save before or after stops on SIGTERM? | Save FIRST | If SIGKILL fires after grace, at least the snapshot lands |
| Async footer cache load? | Yes (planned) | Blocking /ready on 30-60 s footer parse at PB scale is a regression |
| Cluster data-cold flag? | Planned, not in this PR | Requires peer endpoint coordination + RFC on backpressure semantics |
| BufferBridge cold-tier proxy mode? | Planned, not in this PR | Deep change to query path; design needs review |

## Open questions

1. **Cluster-wide buffer restore state.** During the all-peers-restart
   scenario, every peer is replaying its WAL independently.
   Should they share a "WAL-replay coordination" so the first
   one to finish takes over while others catch up? Probably not
   (independent WALs by design) — but worth noting.

2. **Snapshot to S3 as well as local disk.** Today the snapshot
   is local-disk only. A fresh-PVC pod gets no benefit. Could
   we periodically also push the snapshot to S3 (one peer
   owns it via HRW) so fresh pods skip the full S3 LIST?
   Filed as RFC for P5.

3. **Manifest delta log.** Append-only entry per flush to S3:
   `{partition, key, size, time_bounds}`. Replay deltas since
   last snapshot — much faster than LIST at PB scale. Affects
   ingest path. Filed as RFC for P5.

## Implementation roadmap

| PR | Scope | Status |
| --- | --- | --- |
| #122 (this) | P1 lifecycle honesty + P3 hints + docs | in progress |
| next | P3 async footer cache + streaming decode + backoff/jitter | drafted |
| next+1 | P2 cluster data-cold flag + BufferBridge proxy mode | RFC |
| later | P5 S3 snapshot + manifest delta log | RFC |
