# Victoria Lakehouse — Priority Status Overview

**Date:** 2026-05-24
**Branch:** feat/columnar-hot-path-optimization
**Reference Architecture:** 3 combined nodes + 10 select nodes with consistent hash ring

---

## Priority 1: Fix Manifest Fast-Path Race Condition — CRITICAL, BLOCKING

**Status:** Root cause identified, NOT fixed.

**Root Cause:** After container restart, all files have `RowCount=0` because manifest enrichment (RowCount, MinTimeNs, MaxTimeNs) is populated lazily via `enrichManifestFromFooter()` during first query and is NOT persisted to disk. The fast-path (`* | stats count()`) checks `fi.RowCount > 0 && fi.MinTimeNs > 0 && fi.MaxTimeNs > 0` — files with RowCount=0 bypass fast-path and go to S3 workers. Concurrent compaction deletes source files mid-query, producing 404 errors that are silently skipped, resulting in **40% undercount**.

**Fix Required:**
1. Persist manifest enrichment (RowCount, MinTimeNs, MaxTimeNs) to disk snapshot + S3 backup
2. Add 404 error handling in query workers — retry from manifest, not silent skip
3. Reload enriched manifest from disk on startup before marking ready

**Helm Impact:** `manifest.persist_path` and `manifest.persist_interval` already exist in values.yaml — code needs to USE them.

**Files:**
- `internal/manifest/manifest.go` — Add `SaveTo()`/`LoadFrom()` for enriched fields
- `internal/storage/parquets3/storage_query.go` — Handle 404 in `queryFile()`
- `cmd/lakehouse-logs/main.go` — Load persisted manifest on startup
- `cmd/lakehouse-traces/main.go` — Same

---

## Priority 2: Complete Comparative Benchmark — BLOCKED by #1

**Status:** Infrastructure ready, results invalid.

**Ready:**
- 100K logs + 20K traces seeded via datagen
- Compaction enabled (30s interval) in docker-compose-benchmark.yml
- All 7 systems deployed: LH-logs, VL, Loki, LH-traces, VT, Tempo, ClickHouse

**Blocked:** Wildcard count undercounts by 40% due to manifest fast-path race. `stats by(service.name)` works correctly (bypasses fast-path) but headline numbers are unreliable.

**When Ready:** After #1 is fixed + full compaction settles (~85 min for 169 partitions).

---

## Priority 3: Plan 3 Quality Gate — PENDING

**Status:** Tasks 1-3 completed, need full review.

**Completed:**
- Task 1: PartitionSharding — hash-based ownership
- Task 2: Wire sharding into scheduler
- Task 3: Sharding config + CLI flags (ported to traces)

**Remaining:** Code quality review, test coverage check, integration verification, benchmark regression check. Tasks 4-15 are separate.

**Dependencies:** None — can run review now.

---

## Priority 4: Phase 0 Correctness Gate — PLAN EXISTS, NOT STARTED

**Plan:** `docs/superpowers/plans/2026-05-20-phase0-correctness-gate.md`

**What:** End-to-end correctness verification — exact row counts, field preservation, timestamp ordering, filter accuracy, aggregation parity. Validates that LH returns identical results to VL/VT for all query types.

**Dependencies:** Blocked by #1 (manifest fix). Running correctness tests with 40% undercount would produce false failures.

---

## Priority 5: Phase 5 Long-Range Optimizations — PLAN EXISTS, NOT STARTED

**Plan:** `docs/superpowers/plans/2026-05-22-query-perf-phase5-long-range-optimizations.md`

**What:** Partition pruning, time-range skip, multi-partition parallel scan, partition-level bloom, streaming aggregation for 7d/30d queries. Biggest user-visible improvement for cold-tier queries.

**Dependencies:** Should follow Phase 0 (correctness gate) and Phase 2 (latency optimizations, mostly done).

---

## Priority 6: Plan 3 Tasks 4-15 — PENDING

**What:** Select-tier fan-out, hybrid hot+cold routing, peer health tracking, ring rebalancing, failover logic, distributed query coordination. This is the core distributed scaling layer.

**Dependencies:** Tasks 1-3 complete. Aligned with K8s scaling safety layer design.

---

## Priority 7: Phase 3 Concurrency Stress Testing — PLAN EXISTS, PARTIALLY STARTED

**Plan:** `docs/superpowers/plans/2026-05-20-phase3-concurrency-stress-testing.md`

**What:** Concurrent read/write stress, mixed R/W under compaction, cache contention under parallel queries, manifest refresh under load.

**Dependencies:** Should follow Phase 0 correctness gate.

---

## Priority 8: Phase 1 Instrumentation — PLAN EXISTS, NOT STARTED

**Plan:** `docs/superpowers/plans/2026-05-20-phase1-instrumentation-baselines.md`

**What:** Structured query metrics (latency histograms, file scan counts, cache hit rates, S3 latency), Prometheus/VictoriaMetrics integration, baseline dashboards.

**Dependencies:** Can start independently but most valuable after #1 is fixed.

---

## Execution Order

```
#1 Manifest fix ──────> #4 Phase 0 ──────> #2 Benchmark ──────> #5 Phase 5
                              |                                       |
                              v                                       v
                         #7 Phase 3 stress                     #8 Phase 1 instrumentation
                              |
                              v
#3 Plan 3 QG (now) ───> #6 Plan 3 Tasks 4-15 <── K8s safety layer design (now)
```

**#1 is the blocker for everything.** #3 and the K8s safety layer design proceed in parallel since they don't depend on the manifest fix.
