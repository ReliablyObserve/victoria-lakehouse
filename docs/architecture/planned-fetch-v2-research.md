# Planned-fetch v2 research — fewer GETs, for review

**Status: awaiting review.** Three research angles on why planned mode lost the live bench
and how v2 wins it: (A) failure anatomy + an **offline planner simulator over real L2
footers** (5 live files, 3 workload shapes, 5 planner variants), (B) source-level span
policies from ClickHouse Prefetcher / arrow-rs / DuckDB (constants cited), (C) an RTT-aware
cost-model design. Simulator preserved at `scripts/bench/plansim-scratch/` (untracked).

## The corrected root cause (better than the first verdict)

Plans were **already per-file** — the live verdict's "per-RG plans" was wrong about arming.
The real fragmentation, from code + footer geometry:
1. **Cross-RG spans never merge**: same-column chunks in adjacent row groups are separated
   by all other columns' chunks (~1.9 MB stride on real L2 files) — above the 1 MB gap, so
   every (RG × column-run) is its own span: 13–15 spans/file.
2. **The 16 MB cap punishes coalescing**: merged gap bytes count into the *plan total*, so
   exactly the merging that would cut GETs trips `fallback{reason=cap}`.
3. **k=4 span concurrency + all-or-nothing Fetch**: 13–15 spans become ~4 serial RTT waves
   per file; decode waits for the last span.
4. **2 serial RTTs per cold open** (magic+footer) — 65% of per-file wall on metadata-light
   scans once fetch is fixed.

## The simulator's verdict (real footers, RTT=100 ms, 50 MB/s/conn, 40 files / 8 workers)

| workload | window (live) | planned-v1 (live) | **v2' = per-file + k=16 + footer prefetch** |
|---|--:|--:|--:|
| count_24h | 2.45 s / 175 GETs | 17.7 s / 1185 GETs | **~0.8–1.6 s / ~600 GETs / 32 MB** |
| fulltext | 2.25 s | 5.6 s | **~1.5–1.9 s** |
| filtered_count (scan part) | 1.67 s | 7.1 s | **~1.6 s** |

Crucial negative result from the sim: a **flat 5 MB "RTT-aware" gap is a trap** on this
geometry — anything ≥ the 1.9 MB RG stride merges to whole-file, wasting 74–98% of bytes
(and at 50 MB/s/conn, transferring 24 MB costs more than one extra RTT wave saves). The
operative levers are **span concurrency and the cap scope**, not the gap.

## What the references actually do (constants)

- **ClickHouse** (remote): gap `min_bytes_for_seek` = **4 MiB**, span cap
  `bytes_per_read_task = 4×gap` = **16 MiB PER SPAN** (not per plan); whole-file range
  registration, sort once, coalesce across RGs; small ranges always coalescible,
  big ones only when `likely_to_be_used`; tasks dispatched immediately to a shared IO pool
  (no fixed per-file k), decode overlaps via stage scheduler with memory watermarks;
  waste tracked as `memory_amplification`.
- **arrow-rs/object_store**: coalesce parallelism **10**; per-RG fetch batching.
- **DuckDB**: BDP-based cost model for the gap (EWMA-refined per query) + the
  **dense fast path**: ≥95% of a row group's span needed → fetch the contiguous span whole.

## Proposed v2 (the composite the evidence picks)

| # | change | source | risk |
|---|---|---|---|
| 1 | **Span in-flight k: 4 → min(16, spans)** (`s3.planned_fetch_max_inflight`, per-signal) | sim lever #1 — biggest, smallest diff | S |
| 2 | **Cap re-scope: per-plan → per-SPAN 16 MiB** + plan admission via the existing memory ledger (retires `fallback{reason=cap}`) | CH `bytes_per_read_task`; unblocks coalescing | S/M |
| 3 | **Footer-open prefetch across files** (batch-open each worker wave's footers; warm queries already covered by the cache) | sim lever #2: count_24h ~1.56 s → ~0.8 s cold | M |
| 4 | **Streaming Fetch**: spans dispatched all-at-once, decode starts when the first needed span lands (per-span ready latches) | CH dispatch-immediately; fulltext −13%, UX latency better | M |
| 5 | **EWMA RTT/BW estimator** feeding gap* = clamp(RTT×BW, **1 MiB, 4 MiB**) — NOT the naive 5 MB (the sim's geometry trap); TTFB source = existing `S3RequestDuration` point | DuckDB cost model + CH 4 MiB; clamped by A's empirics | M |
| 6 | **Dense degeneration ≥95%** of file/RG span → 1–2 contiguous big spans (DuckDB rule) — the path to eventually retiring window mode entirely (Phase 3, own bench gate) | DuckDB `WHOLE_GROUP_PREFETCH` | M |
| 7 | **Density gate recast**: planned only when spans ≤ 2k AND per-span cap holds (a bytes-% gate would wrongly demote fulltext, which planned wins) | sim | S |

**Not doing**: flat 5 MB gap (proven trap), GET-parity-at-all-costs (the V5 split variant
trades 30× bytes for it — only if a real S3 bill demands it).

## Rollout gate (the #154 re-entry condition, made precise)
Opt-in `planned` until the live bench shows, on BOTH conditions (0 ms and 100/30 ms) and
both modules: **p50 ≤ window on ALL scenarios AND GETs/q within 4× of window** with
out-of-plan ≈ 0, fallbacks ≈ 0, waste ≈ 0. Plus the instrumentation the sim asked for:
per-scenario spans-per-plan + GETs-by-phase (the 1185 GETs/q didn't fully reconcile with
footer geometry — resolve before trusting the next verdict).

## Decision points for review
1. Approve the composite (1–7) and the order (1+2 first — they alone may pass the gate)?
2. The gap estimator clamp [1, 4] MiB — or skip the estimator in v2 and keep static 1 MiB
   (the sim says gap barely matters once k=16; estimator is the smallest win, most code)?
3. Phase-3 ambition (retire window entirely once dense degeneration proves itself) — in
   scope as a goal, or keep window indefinitely as the rollback?
