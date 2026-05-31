# Election-Free Compaction — Implementation Spec

**Date:** 2026-05-31
**Status:** Draft — awaiting maintainer review before implementation PR
**Driving research:** agent `a6f04ad21db622dc9` compaction research report (Option 1
HRW + manifest CAS + distributed orphan sweep).
**Driving feedback:** `feedback_vl_vt_upstream` (no upstream changes),
`feedback_no_storage_nodes` (no VL/VT cluster protocol), `feedback_k8s_style_resource_bounds`
(request/limit semantics), `feedback_layered_test_strategy`,
`feedback_harden_and_lock`, `feedback_close_feedback_loops`,
`feedback_no_silent_regressions`, `feedback_logs_traces_module_parity`,
`feedback_scope_simplicity`, `feedback_per_component_verification`.

**Scope:** PR A only — the new ownership + orphan-sweep code, scheduler
rewiring, manifest watermark, regression tests. PR B = chart + flags
removal. PR C = `internal/election/` deletion. This spec covers PR A
end-to-end; PR B and PR C are listed in the **Deletion checklist** so
no agent loses the breadcrumb.

**Maintainer decision recorded here:** remove `internal/election/`
entirely, drop `internal/compaction/sentinel.go`, drop
`internal/bloomindex/{Set,Is}Leader` (no caller), drop the
`coordination.k8s.io/leases` RBAC from the chart, drop the kind e2e
workflow + `tests/e2e-k8s/`, drop all election-related CLI flags +
helm values, default to HRW ownership. Single-pod case is the degenerate
HRW case (pod owns 100 %, trivially).

---

## Table of contents

1. [Architecture overview](#1-architecture-overview)
2. [Component design](#2-component-design)
   - 2.1 [`internal/compaction/ownership.go`](#21-internalcompactionownershipgo)
   - 2.2 [Manifest changes](#22-manifest-changes)
   - 2.3 [`internal/compaction/scheduler.go` (modified)](#23-internalcompactionschedulergo-modified)
   - 2.4 [`internal/compaction/orphan_sweep.go` (new)](#24-internalcompactionorphan_sweepgo-new)
   - 2.5 [Discovery integration](#25-discovery-integration)
3. [Edge-case mitigation — 33 cases](#3-edge-case-mitigation--33-cases)
4. [Migration path](#4-migration-path)
5. [Test plan](#5-test-plan)
6. [Observability](#6-observability)
7. [Deletion checklist (PR B + PR C)](#7-deletion-checklist-pr-b--pr-c)
8. [Risk register](#8-risk-register)
9. [Rollback plan](#9-rollback-plan)
10. [Open questions for the maintainer](#10-open-questions-for-the-maintainer)

---

## 0. TL;DR

Today, distributed compaction safety hinges on **two layers** of
coordination:

- A **leader-election** layer (`internal/election/`, ~3 600 LoC
  including tests) that elects exactly one pod via K8s Lease or S3
  lock and lets only the leader run `scheduler.Scan`. Sharded mode
  (`Compaction.ShardCount > 1`) skips this gate. See
  `internal/compaction/scheduler.go:120-127`.
- A per-partition **sentinel** (`internal/compaction/sentinel.go`,
  72 LoC) — an S3 object that other pods check before compacting.
  It is a **TOCTOU-racy fake lock**: the gap between `IsLocked` and
  `Upload` is wide open. The leader gate is what keeps it safe in
  practice; remove the leader gate and the sentinel collapses. See
  `internal/compaction/sentinel.go:34-48`.

We replace both layers with a single in-process primitive:

- **Hash-based partition ownership** (HRW / "rendezvous hashing")
  over the existing `internal/peercache.Ring`, which is already kept
  in sync with the headless-service discovery (`internal/discovery/discovery.go`).
  Each compaction tick: enumerate partitions, ask `OwnsPartition(self,
  partition, peers)`; if true, run the existing compactor path. The
  manifest's existing `AddFile`/`RemoveFile` semantics (idempotent on
  the partition map keyed by `S3 key`) provide the atomicity guarantee
  for the merge-publish step. Sentinel disappears. Leader gate
  disappears.

Safety in the rare bad cases (ring flap, dual ownership during DNS
lag) is provided by **three independent backstops**:
- (a) S3 output keys carry a `uuid.New().String()[:8]` suffix
  (`internal/compaction/compactor.go:165-166`) — two parallel
  compactions never collide on the output object, they just publish
  two redundant compacted files.
- (b) A **partition-staleness orphan sweep** (Tier A) hands off to a
  secondary owner when the primary fails to advance
  `LastCompactionAttempt` for `3 × Interval`.
- (c) A **prefix-staleness orphan sweep** (Tier B) garbage-collects
  files in S3 that are not in the manifest after `OrphanTTL = 2h`
  with three-step deletion safety.

The result is **simpler** (no Lease / no lock object / no heartbeat),
**cheaper** (no continuous K8s API calls, ~7 MB smaller binary per
`tests/verification/matrix.md:240-249`), and **easier to reason
about** (no split-brain semantics — duplicates are observable and
self-healing rather than a race that corrupts data).

---

## 1. Architecture overview

### 1.1 Flow diagram — AFTER (election-free)

```
+-------------------------------------------------------------------+
|  Pod boot (cmd/lakehouse-logs/main.go / lakehouse-traces/main.go) |
+-------------------------------------------------------------------+
                  |
                  v
+-------------------------------------------------------------------+
| storage.NewStorage  (parquets3/storage.go:124)                    |
|   - discovery.New(...)                                            |
|   - peercache.New(selfAddr, ...)                                  |
|   - Refresh loop already wired (RefreshDiscovery:946)             |
+-------------------------------------------------------------------+
                  |
                  v
+-------------------------------------------------------------------+
| compaction.NewScheduler (NEW signature, no Leader field)          |
|   - ownership   = &compaction.OwnershipResolver{                  |
|       Self:    cfg.SelfAddr,                                      |
|       Peers:   storage.PeerCache().Members,  // live              |
|       Stabilizing: storage.PeerCache().IsStabilizing,             |
|     }                                                             |
|   - sweep      = compaction.NewOrphanSweep(...)                   |
|                                                                   |
| Subscribes to peercache.OnRingChange to record stabilization      |
| events into a metric (no behaviour change inside scheduler — the  |
| .Stabilizing func reads from the same source of truth).           |
+-------------------------------------------------------------------+
                  |
                  v       (ticker every cfg.Compaction.Interval)
+-------------------------------------------------------------------+
| Scheduler.Scan(ctx):                                              |
|   if storage.PeerCache().IsStabilizing() {                        |
|       metrics.CompactionDeferredStabilizing.Inc()                 |
|       return 0, nil                                               |
|   }                                                               |
|   peers := storage.PeerCache().Members()                          |
|   for partition, files := range m.AllFiles() {                    |
|     if !ownership.OwnsPartition(partition, peers) { continue }    |
|     if !policy.Eligible(files, t) { continue }                    |
|     manifest.MarkAttempt(partition, time.Now())                   |
|     // existing compactor.Compact() path — UNCHANGED              |
|     // (download / merge / upload-with-uuid-suffix / manifest CAS)|
|   }                                                               |
+-------------------------------------------------------------------+
                  |
                  v       (every Interval * 3, configurable)
+-------------------------------------------------------------------+
| OrphanSweep.Tier_A_PartitionStaleness(ctx):                       |
|   if storage.PeerCache().IsStabilizing() { return nil }           |
|   for partition, attempt := range manifest.AttemptsView() {       |
|     if since(attempt) < 3 * Interval { continue }                 |
|     if !policy.Eligible(...) { continue }                         |
|     if ownership.SecondaryOwner(partition, peers) != self {       |
|         continue                                                  |
|     }                                                             |
|     // take it: run the same Compact path                         |
|     metrics.CompactionStolenTotal.Inc()                           |
|   }                                                               |
+-------------------------------------------------------------------+
                  |
                  v       (hourly)
+-------------------------------------------------------------------+
| OrphanSweep.Tier_B_PrefixSweep(ctx):                              |
|   for prefix := range walk_top_level_prefixes_we_own() {          |
|     keys := s3.List(prefix)                                       |
|     manifest_keys := manifest.AllFileKeys(prefix)                 |
|     for k in keys {                                               |
|       if k in manifest_keys: continue                             |
|       if isMetaKey(k): continue                  // 3-tier safety |
|       if hash(prefix) % len(peers) != selfIdx: continue           |
|       if age(k) < OrphanTTL: continue                             |
|       manifest_keys2 := manifest.AllFileKeys(prefix)  // re-read  |
|       if k in manifest_keys2: continue           // 3-tier safety |
|       s3.Delete(k)                                                |
|       metrics.CompactionOrphansDeleted.Inc()                      |
|     }                                                             |
|   }                                                               |
+-------------------------------------------------------------------+
```

### 1.2 Flow diagram — BEFORE (election path)

```
+-------------------------------------------------------------------+
|  Pod boot                                                         |
+-------------------------------------------------------------------+
                  |
                  v
+-------------------------------------------------------------------+
| election.NewAutoElector(cfg.Compaction.LeaderElection)            |
|   - "k8s": K8sElector  -> coordination.k8s.io/v1 Lease            |
|       - GET / PUT  every cfg.Compaction.LeaseDuration             |
|       - retry on 409 conflict                                     |
|       - OnStoppedLeading callback                                 |
|   - "s3":  S3Elector   -> S3 object _compaction_lock.json         |
|       - heartbeat every cfg.Compaction.S3Heartbeat                |
|       - lock TTL cfg.Compaction.S3LockTTL                         |
|   - "auto": K8s if KUBERNETES_SERVICE_HOST, else S3, else noop    |
|   - "none": NoopElector (always leader)                           |
+-------------------------------------------------------------------+
                  |
                  v
+-------------------------------------------------------------------+
| Scheduler.Scan(ctx):                                              |
|   if !leader.IsLeader() && shardCount<=1 { return 0, nil }        |
|   for partition := range m.AllFiles() {                           |
|     if sharding && !sharding.OwnsPartition(partition) { continue }|
|     locked, _ := sentinel.IsLocked(...)        // 1. CHECK        |
|     if locked { continue }                                        |
|     ok, _ := sentinel.Acquire(...)             // 2. WRITE — gap! |
|     if !ok { continue }                                           |
|     // run compactor (download / merge / upload / manifest)       |
|     sentinel.Release(...)                                         |
|   }                                                               |
+-------------------------------------------------------------------+

Bugs / smells visible in the BEFORE diagram:
  * Steps 1 + 2 = TOCTOU; two pods racing IsLocked can both win.
  * In sharded mode, sentinel is still used but leader gate is
    skipped — so the sentinel race is exposed even on the happy path.
  * Acquire() never sends If-None-Match — it just overwrites.
  * Stale lock cleanup is timestamp-based, vulnerable to clock skew.
  * "auto" silently degrades to "s3" when k8s_election build tag
    absent (election/auto.go:75); operators can be misled.
```

### 1.3 Mapping — primitives we keep vs. drop

| Primitive | Today | Tomorrow |
|---|---|---|
| `peercache.Ring` (CRC32 vnodes) | Used for select-side cache routing | **Reused** for compaction ownership |
| `peercache.OnRingChange` + `IsStabilizing` | Used for select-side shadow ring | **Reused** for compaction deferral |
| `discovery.DiscoverPeers` | Already running every refresh | **Reused** as ownership input |
| `compaction.PartitionSharding` (CRC32 % shardCount) | Sharded mode toggle | **Replaced** by `OwnershipResolver` (HRW over live peer list) |
| `compaction.Sentinel` | TOCTOU per-partition lock | **Deleted** |
| `election.{Auto,K8s,S3,Noop}Elector` | Leader gate | **Deleted** |
| Manifest `AddFile` / `RemoveFile` | Idempotent on key | **Reused** + new `MarkAttempt` |
| Compactor uuid-suffixed output keys (`compactor.go:165-166`) | Already collision-free | **Reused** as our "duplicate is harmless" backstop |
| K8s RBAC for `coordination.k8s.io/leases` | Required by K8sElector | **Deleted** from chart |

---

## 2. Component design

### 2.1 `internal/compaction/ownership.go`

**Purpose:** the single source of truth for *"does this pod own this
partition?"*. Used by the scheduler on every tick and by the orphan
sweep when checking secondary ownership.

#### 2.1.1 Hash algorithm choice — recommendation: **xxhash**

| Algorithm | Throughput | Library | Already in tree? | Notes |
|---|---|---|---|---|
| CRC32 (IEEE) | ~1 GB/s | `hash/crc32` (stdlib) | Yes — already used in `peercache/ring.go` and `compaction/sharding.go` | Acceptable; mediocre distribution for short strings; we'd be consistent with rest of code |
| SipHash-2-4 | ~2 GB/s | `golang.org/x/crypto/siphash` | No | Keyed; better distribution. Overkill for non-adversarial input |
| xxhash (xxh64) | ~12 GB/s | `github.com/cespare/xxhash/v2` | **Yes** — transitive dep via `github.com/parquet-go/parquet-go` | Excellent distribution, fast, already linked, no new dep |

**Recommendation: xxhash (xxh64)**. It's already in the binary
(parquet-go uses it for dictionary hashing), distribution is better
than CRC32 for short ASCII strings like partition keys
(`dt=2026-05-31/hour=14`), and it's ~12× faster than CRC32. The
ownership decision runs on every partition on every tick, so the
speedup matters at high partition counts.

**HRW (rendezvous hashing) is preferred over consistent hashing /
ring lookups** for this use case because:

- It is *stateless* — no ring rebuild on membership change. We compute
  on the fly from `(peer, partition)` pairs.
- It naturally produces a **deterministic ordering** of all peers for
  a given partition, which gives us secondary / tertiary owners "for
  free" — needed for the orphan sweep.
- HRW guarantees that when one peer joins or leaves, only `1/N` of
  partitions change ownership (the same minimal-disruption property
  as consistent hashing).

We do **not** reuse `peercache.Ring`'s consistent-hashing lookup for
the primary-owner computation because the ring lookup doesn't expose
a clean secondary/tertiary owner API. We *do* reuse `peercache.Ring`
for membership and stabilization events (those are already correct).

#### 2.1.2 Public API

```go
// Package compaction owns partition assignment, scheduling, and
// orphan cleanup. After PR A this package no longer depends on
// internal/election.

// OwnershipResolver decides which pod owns which compaction partition
// using Highest-Random-Weight (HRW / rendezvous) hashing over the
// live peer set. It is safe for concurrent use.
type OwnershipResolver struct {
    // Self is the address of this pod, as advertised in the peer set
    // (e.g. "10.0.1.42:9428"). Must match exactly what discovery
    // returns in GetPeers().
    Self string

    // Peers returns the current peer list. We re-read on every call
    // rather than caching, so ring updates are picked up within one
    // discovery refresh interval (~30 s by default). Returning nil
    // or an empty list is equivalent to "I am alone" (this pod owns
    // every partition).
    Peers func() []string

    // Stabilizing returns true while the ring is in its post-change
    // stabilization window (see peercache.OnRingChange). Callers
    // should treat ownership as undefined during this window and
    // defer mutating work. The default is to call
    // PeerCache.IsStabilizing(); tests substitute a func returning
    // false.
    Stabilizing func() bool

    // hashFn is used to compute the HRW weight; default is xxhash.
    // Tests inject a deterministic hash to verify tie-breaking.
    hashFn func(string) uint64
}

// OwnsPartition returns true if this pod is the primary owner of the
// given partition under the current peer set. Always true when there
// are zero or one peers (single-pod degenerate case). Returns false
// during ring stabilization; callers should defer work until the
// window closes.
func (r *OwnershipResolver) OwnsPartition(partition string) bool

// OwnerOf returns the primary owner peer for the given partition, or
// the empty string if the peer set is empty. Useful for logging
// "another pod owns this" decisions.
func (r *OwnershipResolver) OwnerOf(partition string) string

// SecondaryOwner returns the second-ranked HRW owner — the pod that
// should take over compaction work when the primary owner falls
// behind (Tier A orphan sweep). Returns the primary owner when the
// peer set has fewer than two members.
func (r *OwnershipResolver) SecondaryOwner(partition string) string

// TertiaryOwner returns the third-ranked HRW owner. Used only by the
// Tier A sweep when both primary and secondary appear unhealthy
// (consecutive failed compaction attempts). Returns SecondaryOwner
// when the peer set has fewer than three members.
func (r *OwnershipResolver) TertiaryOwner(partition string) string

// RankedOwners returns all peers ranked by HRW weight for this
// partition, primary first. Useful for the sweep to take the
// highest-ranked healthy peer rather than only checking secondary.
// Returns a fresh slice; safe to mutate.
func (r *OwnershipResolver) RankedOwners(partition string) []string

// HRW weight: deterministic, suitable for inspection in tests.
// Exposed only inside the package; tests live in this package.
func hrwWeight(peer, partition string, h func(string) uint64) uint64
```

**HRW implementation sketch:**

```go
func (r *OwnershipResolver) RankedOwners(partition string) []string {
    peers := r.Peers()
    if len(peers) == 0 {
        if r.Self != "" {
            return []string{r.Self}
        }
        return nil
    }

    type weighted struct {
        peer string
        w    uint64
    }
    ranked := make([]weighted, 0, len(peers))
    for _, p := range peers {
        ranked = append(ranked, weighted{peer: p, w: hrwWeight(p, partition, r.hashFn)})
    }
    // Highest weight first; stable lex tiebreak so behaviour is
    // deterministic when the hash collides (vanishingly rare with
    // 64-bit hash but the tiebreak makes property tests sane).
    sort.Slice(ranked, func(i, j int) bool {
        if ranked[i].w != ranked[j].w {
            return ranked[i].w > ranked[j].w
        }
        return ranked[i].peer < ranked[j].peer
    })

    out := make([]string, len(ranked))
    for i, r := range ranked {
        out[i] = r.peer
    }
    return out
}

func (r *OwnershipResolver) OwnsPartition(partition string) bool {
    if r.Stabilizing != nil && r.Stabilizing() {
        return false  // edge case 3 / 22: defer during instability
    }
    owners := r.RankedOwners(partition)
    if len(owners) == 0 {
        // Edge case 10: empty peer list — refuse rather than
        // double-take. We log + skip; the next refresh will recover.
        return false
    }
    return owners[0] == r.Self
}

// hrwWeight combines peer and partition with a single xxhash call.
// We concatenate with a separator that cannot appear in either
// (partitions use only [a-z0-9=/-]) so collisions cannot be
// engineered.
func hrwWeight(peer, partition string, h func(string) uint64) uint64 {
    var b [128]byte
    n := copy(b[:], peer)
    b[n] = 0x1F  // unit separator, will never appear in input
    m := copy(b[n+1:], partition)
    return h(string(b[:n+1+m]))
}
```

**Self-identity wiring:**

The pod's `Self` string must match what peer pods advertise it as,
otherwise HRW computes a "self" weight that no other peer recognizes
and we silently lose ownership. The existing wiring is:

- `cmd/lakehouse-logs/main.go:244` — `addr` is built from
  `*listenAddr` (`flag.String("httpListenAddr", ":9428", ...)`).
- Discovery resolves headless-service SRV records and returns
  `host:port` for every pod
  (`internal/discovery/discovery.go:140-147` + `:160-163`).
- The peercache passes `cfg.ListenAddr()` as `selfAddr` to
  `peercache.New` (`parquets3/storage.go:137`), so `Self ==
  cfg.ListenAddr()`.

We must thread that same string into `OwnershipResolver.Self`. The
spec is **"use `cfg.ListenAddr()` verbatim, no hostname/IP
manipulation"**. To safeguard against drift, the resolver records
`compaction_ownership_self_in_peers` (gauge 0/1) on every tick; if
this pod doesn't appear in its own peer set, the gauge stays at 0
and alerting fires. See §6 for metric definitions.

#### 2.1.3 Stabilization integration

`peercache.PeerCache` already exposes:
- `IsStabilizing() bool` — true during the post-change window
  (default 60 s, `DefaultStabilizeDuration`).
- `OnRingChange(fn)` — async callback when membership changes.

Wiring:
- `OwnershipResolver.Stabilizing` is set to `storage.PeerCache().IsStabilizing`
  in `cmd/lakehouse-{logs,traces}/main.go`.
- The scheduler also calls `IsStabilizing()` at the top of `Scan` and
  bumps `lakehouse_compaction_deferred_stabilizing_total` when true
  — this is an explicit shortcut so we don't enumerate partitions
  during instability.
- We subscribe `OnRingChange` to log "ring change: defer compaction
  for <stabilizeDuration>" at INFO level. No behaviour change beyond
  the log line (the actual deferral is gated by `IsStabilizing()`).

---

### 2.2 Manifest changes

The new `OrphanSweep` needs to know *when* each partition was last
attempted, so it can detect a stale primary owner. We add a single
new field plus three small methods.

#### 2.2.1 New field

```go
// File: internal/manifest/manifest.go
type Manifest struct {
    // ... existing fields ...
    partitionAttempts map[string]time.Time  // NEW: partition -> last compaction attempt
}
```

Initialize in `New()`:
```go
func New(bucket, prefix string) *Manifest {
    return &Manifest{
        // ... existing fields ...
        partitionAttempts: make(map[string]time.Time),
    }
}
```

#### 2.2.2 New methods

```go
// MarkAttempt records that the caller began (or is about to begin) a
// compaction attempt on the given partition. Called by the scheduler
// *before* selecting files, so a crash mid-compaction still leaves a
// fresh attempt timestamp — Tier A sweep then has to wait the full
// staleness window (3*Interval) before stealing, matching how
// LevelDB/RocksDB record "compaction-in-progress" markers.
//
// Safe for concurrent use.
func (m *Manifest) MarkAttempt(partition string, t time.Time) {
    m.mu.Lock()
    m.partitionAttempts[partition] = t
    m.mu.Unlock()
}

// LastAttempt returns the most recent MarkAttempt timestamp for the
// given partition. Returns the zero value when no attempt was ever
// recorded. Safe for concurrent use.
func (m *Manifest) LastAttempt(partition string) time.Time {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return m.partitionAttempts[partition]
}

// AttemptsView returns a snapshot of (partition, lastAttempt) for
// every partition the manifest is aware of, including those with no
// attempt recorded (zero-value time). Used by the orphan sweep.
// The returned map is safe to mutate.
func (m *Manifest) AttemptsView() map[string]time.Time {
    m.mu.RLock()
    defer m.mu.RUnlock()
    out := make(map[string]time.Time, len(m.files))
    for p := range m.files {
        out[p] = m.partitionAttempts[p]  // zero time if absent
    }
    return out
}
```

#### 2.2.3 Persistence

The attempt map is **in-memory only**. Reasoning:

- After a pod restart, the worst case is that Tier A wakes up
  immediately, sees zero attempts, and tries to take over partitions
  that the *previous* owner of this pod had been working. Since HRW
  is deterministic, after restart the same pod owns the same
  partitions and Tier A's secondary-owner check will not pick this
  pod for stealing.
- Persisting attempts to S3 would add S3 traffic and a new failure
  mode (snapshot write race) without buying anything: HRW recovers
  ownership instantly on restart.
- Persisting attempts to disk via the existing `Manifest.SaveTo` /
  `Manifest.LoadFrom` machinery is **opt-in** but the spec leaves
  this out of PR A. If a maintainer later wants restart-survival,
  it's a 5-line addition to `persistedManifest` (see open question
  Q7).

#### 2.2.4 Idempotency confirmation

`AddFile` and `RemoveFile` are already idempotent on the key:

- `AddFile` appends to `m.files[partition]` then re-indexes; calling
  it twice with the same `FileInfo.Key` produces a duplicate entry in
  the slice. **This is not currently idempotent.** It must become
  idempotent before PR A ships, because two pods running a
  duplicate-but-harmless compaction would otherwise leave two manifest
  entries pointing at two distinct UUID-suffixed S3 outputs, both
  valid.
- `RemoveFile` walks the slice and removes the first match
  (`manifest.go:459-476`) — already idempotent (a second call is a no-op).

**Required change in `manifest.go` (verify-on-add):**

```go
// File: internal/manifest/manifest.go:517
func (m *Manifest) AddFile(partition string, fi FileInfo) {
    m.mu.Lock()
    defer m.mu.Unlock()

    // NEW: idempotency guard. Two compaction loops racing can produce
    // duplicate AddFile calls with distinct keys (uuid-suffixed), but
    // also with identical keys when the same compaction is retried.
    // Skip if key already present.
    for _, existing := range m.files[partition] {
        if existing.Key == fi.Key {
            return
        }
    }

    // ... existing body unchanged ...
}
```

The integration test `TestManifest_AddFile_IdempotentOnKey` (§5)
locks this in.

**Concurrent-writer mutex strategy:** unchanged. The manifest already
holds a single `sync.RWMutex` covering both the file map and the
sorted partition index (`manifest.go:77`). The new
`partitionAttempts` map lives under the same mutex; no new lock
order to reason about.

---

### 2.3 `internal/compaction/scheduler.go` (modified)

#### 2.3.1 Diff sketch — `SchedulerConfig`

```go
// BEFORE — internal/compaction/scheduler.go:17
type SchedulerConfig struct {
    Leader           election.Leader     // REMOVE
    Manifest         *manifest.Manifest
    Pool             CompactorPool
    Sentinel         *Sentinel           // REMOVE
    Policy           *LevelPolicy
    Sharding         *PartitionSharding  // REMOVE
    Prefix           string
    Mode             config.Mode
    Interval         time.Duration
    MaxConcurrent    int
    RowGroupSize     int
    CompressionLevel int
    OnCompacted      func(added []manifest.FileInfo, removed []string)
}

// AFTER
type SchedulerConfig struct {
    Manifest         *manifest.Manifest
    Pool             CompactorPool
    Ownership        *OwnershipResolver  // NEW: replaces Leader + Sharding
    Policy           *LevelPolicy
    Prefix           string
    Mode             config.Mode
    Interval         time.Duration
    MaxConcurrent    int
    RowGroupSize     int
    CompressionLevel int
    OnCompacted      func(added []manifest.FileInfo, removed []string)
}
```

#### 2.3.2 Diff sketch — `Scan` body

```go
// BEFORE — internal/compaction/scheduler.go:120-235 (simplified)
func (s *Scheduler) Scan(ctx context.Context) (int, error) {
    if s.sharding == nil || s.sharding.shardCount <= 1 {
        if !s.leader.IsLeader() {
            return 0, nil
        }
    }
    allFiles := s.manifest.AllFiles()
    for partition, files := range allFiles {
        if s.sharding != nil && s.sharding.shardCount > 1 {
            if !s.sharding.OwnsPartition(partition) {
                continue
            }
        }
        // ... eligibility ...
        // sentinel.IsLocked + sentinel.Acquire + compact + sentinel.Release
    }
}

// AFTER
func (s *Scheduler) Scan(ctx context.Context) (int, error) {
    // (A) Defer entire scan during ring stabilization. Cheap shortcut;
    //     OwnsPartition would also return false but enumerating
    //     thousands of partitions only to skip is wasteful.
    if s.ownership.IsStabilizing() {
        metrics.CompactionDeferredStabilizing.Inc()
        return 0, nil
    }

    allFiles := s.manifest.AllFiles()
    var candidates []partitionCandidate
    for partition, files := range allFiles {
        // (B) HRW-based ownership. Single source of truth.
        if !s.ownership.OwnsPartition(partition) {
            continue
        }
        pt, err := manifest.ParsePartitionTime(partition)
        if err != nil {
            continue
        }
        level, eligible := s.policy.Eligible(files, pt)
        if !eligible {
            continue
        }
        candidates = append(candidates, partitionCandidate{
            partition: partition, level: level, time: pt,
        })
    }

    sort.Slice(candidates, func(i, j int) bool {
        return candidates[i].time.Before(candidates[j].time)
    })

    compacted := 0
    for _, c := range candidates {
        if compacted >= s.maxConcurrent {
            break
        }
        // (C) Record attempt BEFORE compaction so a crash leaves a fresh
        //     timestamp. Tier A waits 3*Interval before stealing.
        s.manifest.MarkAttempt(c.partition, time.Now())

        partFiles := s.manifest.FilesForPartition(c.partition)
        fp := MajoritySchemaFingerprint(partFiles, c.level)
        selected := s.policy.SelectFiles(partFiles, c.level, fp)
        if len(selected) < 2 {
            continue
        }

        compactor := NewCompactor(CompactorConfig{
            Pool:             s.pool,
            Manifest:         s.manifest,
            Prefix:           s.prefix,
            Mode:             s.mode,
            RowGroupSize:     s.rowGroupSize,
            CompressionLevel: s.compressionLevel,
        })
        result, err := compactor.Compact(ctx, c.partition, selected, c.level)
        if err != nil {
            logger.Errorf("compaction failed: %s; partition=%s", err, c.partition)
            metrics.CompactionErrorsTotal.Inc()
            continue
        }

        if s.onCompacted != nil {
            addedFiles := s.manifest.FilesForPartition(c.partition)
            var removedKeys []string
            for _, sel := range selected {
                removedKeys = append(removedKeys, sel.Key)
            }
            s.onCompacted(addedFiles, removedKeys)
        }
        compacted++
    }
    return compacted, nil
}
```

**Note on the sentinel removal:** the existing compactor path (`compactor.go:82-205`)
already does the "duplicate output is harmless" trick — `outputKey =
prefix + partition + "/compacted-L%d-%s.parquet"` with `%s` being a
random UUID (`compactor.go:165-166`). Two pods compacting the same
partition will:
1. Both `Download` the same N L0 files (idempotent reads).
2. Both produce a merged Parquet from the same N rows.
3. Both `Upload` with **different output keys** (different UUIDs).
4. Both call `manifest.AddFile` with **different output keys** — the
   manifest ends up with two equivalent compacted files.
5. Both call `manifest.RemoveFile + pool.Delete` on the same N L0
   files — `RemoveFile` is idempotent (no-op on second call),
   `pool.Delete` returns `s3.NoSuchKey` which we log + ignore (see
   compactor.go:188-191: "failed to delete source file").
6. Result: one extra compacted file on disk, costing storage but
   never corrupting data. Tier B sweep eventually catches the
   redundant file (the policy decides which one is "in manifest" —
   both are, but the second `AddFile` is now a no-op thanks to §2.2.4,
   so only the **first to publish** wins the manifest slot. The
   second pod's compacted output is orphan-eligible after `OrphanTTL`).

This is the **load-bearing observation** of the whole design: duplicate
work is safe; we just need to make it observable and self-cleaning.

#### 2.3.3 `OnRingChange` subscription (optional, no behaviour change)

```go
// In NewScheduler:
func NewScheduler(cfg SchedulerConfig) *Scheduler {
    s := &Scheduler{ /* ... */ }
    if cfg.OnRingChange != nil {  // wire-through from main.go
        cfg.OnRingChange(func(ev peercache.RingChangeEvent) {
            logger.Infof("compaction: ring change observed; type=%s peer=%s old=%d new=%d — deferring for %v",
                ev.Type, ev.Peer, ev.OldMemberCount, ev.NewMemberCount,
                peercache.DefaultStabilizeDuration)
            metrics.CompactionRingChangesTotal.Inc(string(ev.Type))
        })
    }
    return s
}
```

The actual deferral is already done by `IsStabilizing()`; this
callback just logs and ticks a counter so operators can correlate
ring churn with compaction throughput.

---

### 2.4 `internal/compaction/orphan_sweep.go` (new)

**Two tiers** because they catch different failures:

- **Tier A: partition-staleness.** Catches a primary owner that
  *thinks* it owns the partition but is hung / crashed / slow.
  Cadence: every `N × Interval` (default N = 3). Cheap — reads the
  in-memory `AttemptsView`.
- **Tier B: prefix-sweep.** Catches files that exist in S3 but not in
  any pod's manifest (e.g. partial multipart upload, manifest snapshot
  drift, the "redundant compacted file" from §2.3.2's duplicate work
  observation). Cadence: hourly. Lists S3, requires three-step
  deletion safety.

#### 2.4.1 Tier A — partition-staleness sweep

```go
// File: internal/compaction/orphan_sweep.go

type OrphanSweepConfig struct {
    Manifest      *manifest.Manifest
    Pool          CompactorPool
    Ownership     *OwnershipResolver
    Policy        *LevelPolicy
    Prefix        string
    Mode          config.Mode
    Interval      time.Duration

    // TierA cadence multiplier: TierA runs every TierAInterval ticks
    // of the main scheduler. Default 3 — i.e. partitions are
    // considered stale if no attempt has been recorded for 3 *
    // Interval (a primary owner that ticks normally will mark every
    // Interval, so 3× is "missed three opportunities").
    TierAStalenessMultiplier int

    // TierB cadence — wall clock, not multiplier. Default 1h.
    TierBInterval time.Duration

    // TierB: an S3 key is orphan-eligible only if its LastModified
    // is older than OrphanTTL. Default 2h.
    OrphanTTL time.Duration

    // TierB: extra paranoia — never delete keys matching this prefix
    // list. Default ["_meta/", "_tombstones/", "_compaction_lock"].
    NeverDeletePrefixes []string

    // Hooks (nil-safe). Used by tests + by Tier A's "we just took
    // over from a slow peer" log line.
    OnSteal func(partition string, primaryOwner string)
}

type OrphanSweep struct {
    cfg     OrphanSweepConfig
    s3List  S3Lister            // Pool wrapped with List(prefix) → []string
    stopCh  chan struct{}
    wg      sync.WaitGroup
}

func NewOrphanSweep(cfg OrphanSweepConfig, lister S3Lister) *OrphanSweep
func (o *OrphanSweep) Start()
func (o *OrphanSweep) Stop()

// Internal — exposed for testing.
func (o *OrphanSweep) RunTierA(ctx context.Context) (stolen int, err error)
func (o *OrphanSweep) RunTierB(ctx context.Context) (deleted int, err error)
```

**Tier A body sketch:**

```go
func (o *OrphanSweep) RunTierA(ctx context.Context) (int, error) {
    if o.cfg.Ownership.IsStabilizing() {
        metrics.CompactionSweepDeferredStabilizing.Inc("tier_a")
        return 0, nil
    }

    threshold := time.Duration(o.cfg.TierAStalenessMultiplier) * o.cfg.Interval
    if threshold <= 0 {
        threshold = 3 * o.cfg.Interval
    }

    attempts := o.cfg.Manifest.AttemptsView()
    stolen := 0
    for partition, lastAttempt := range attempts {
        // Stale check: zero time => never attempted (cold) => apply
        // the same threshold against partition mtime, not Now().
        // Otherwise: time.Since(lastAttempt) > threshold.
        if !lastAttempt.IsZero() && time.Since(lastAttempt) < threshold {
            continue
        }

        files := o.cfg.Manifest.FilesForPartition(partition)
        pt, err := manifest.ParsePartitionTime(partition)
        if err != nil {
            continue
        }
        _, eligible := o.cfg.Policy.Eligible(files, pt)
        if !eligible {
            continue  // Nothing to do anyway
        }

        // Secondary owner check — only the next-ranked owner takes
        // over, so we don't get a thundering herd.
        ranked := o.cfg.Ownership.RankedOwners(partition)
        if len(ranked) < 2 {
            continue  // No fallback available
        }
        if ranked[1] != o.cfg.Ownership.Self {
            continue
        }
        primary := ranked[0]

        // Take it: record a fresh attempt then run the same compactor.
        o.cfg.Manifest.MarkAttempt(partition, time.Now())
        if o.cfg.OnSteal != nil {
            o.cfg.OnSteal(partition, primary)
        }
        metrics.CompactionStolenTotal.Inc()

        fp := MajoritySchemaFingerprint(files, /*level*/ 0)  // Same level
        selected := o.cfg.Policy.SelectFiles(files, 0, fp)
        if len(selected) < 2 {
            continue
        }

        compactor := NewCompactor(CompactorConfig{ /* ... */ })
        if _, err := compactor.Compact(ctx, partition, selected, 0); err != nil {
            logger.Warnf("tier_a steal failed; partition=%s primary=%s: %s",
                partition, primary, err)
            continue
        }
        stolen++
    }
    return stolen, nil
}
```

**Tier B body sketch:**

```go
type S3Lister interface {
    List(ctx context.Context, prefix string) ([]string, error)
    HeadObject(ctx context.Context, key string) (size int64, mtime time.Time, err error)
}

func (o *OrphanSweep) RunTierB(ctx context.Context) (int, error) {
    if o.cfg.Ownership.IsStabilizing() {
        metrics.CompactionSweepDeferredStabilizing.Inc("tier_b")
        return 0, nil
    }

    // Enumerate "top-level prefixes we own". A prefix is
    // {tenant}/{signal}/dt=YYYY-MM-DD/ — partition key absent the
    // hour. Two-level walk: first list the date dirs, then for each
    // date, list keys.
    datePrefixes, err := o.s3List.List(ctx, o.cfg.Prefix)
    if err != nil {
        return 0, fmt.Errorf("list date prefixes: %w", err)
    }
    peers := o.cfg.Ownership.Peers()
    selfIdx := indexOf(o.cfg.Ownership.Self, peers)
    if selfIdx < 0 || len(peers) == 0 {
        return 0, nil  // Defensive — see edge case 10
    }

    deleted := 0
    for _, dp := range datePrefixes {
        // Hash-bucket the prefix to a single peer so two pods don't
        // race the same LIST. xxhash > CRC32 for distribution.
        if int(o.cfg.Ownership.hashFn(dp)%uint64(len(peers))) != selfIdx {
            continue
        }

        keys, err := o.s3List.List(ctx, dp)
        if err != nil {
            continue
        }

        // Snapshot the manifest's view of keys under this prefix.
        manifestKeys := o.cfg.Manifest.KeysUnderPrefix(dp)
        manifestSet := make(map[string]struct{}, len(manifestKeys))
        for _, k := range manifestKeys {
            manifestSet[k] = struct{}{}
        }

        for _, key := range keys {
            // 3-tier safety: (a) NOT in manifest.
            if _, in := manifestSet[key]; in {
                continue
            }
            // (b) Sentinel + meta-key blacklist — never touch.
            if o.isProtected(key) {
                continue
            }
            // Age check — HEAD for LastModified.
            _, mtime, err := o.s3List.HeadObject(ctx, key)
            if err != nil {
                continue
            }
            if time.Since(mtime) < o.cfg.OrphanTTL {
                continue
            }
            // (c) Re-read manifest (lock-free snapshot) — guards
            // against the race where a different pod just published
            // this key between our LIST and HEAD.
            manifestKeys2 := o.cfg.Manifest.KeysUnderPrefix(dp)
            present := false
            for _, k := range manifestKeys2 {
                if k == key {
                    present = true
                    break
                }
            }
            if present {
                continue
            }

            if err := o.cfg.Pool.Delete(ctx, key); err != nil {
                logger.Warnf("tier_b orphan delete failed; key=%s: %s", key, err)
                continue
            }
            metrics.CompactionOrphansDeleted.Inc()
            deleted++
        }
    }
    return deleted, nil
}

func (o *OrphanSweep) isProtected(key string) bool {
    if !strings.HasSuffix(key, ".parquet") {
        return true  // Only ever delete .parquet
    }
    for _, p := range o.cfg.NeverDeletePrefixes {
        if strings.Contains(key, p) {
            return true
        }
    }
    return false
}
```

#### 2.4.2 New manifest method needed by Tier B

```go
// File: internal/manifest/manifest.go — NEW
// KeysUnderPrefix returns all manifest-tracked file keys whose key
// has the given prefix. Used by orphan sweep to compare manifest vs.
// S3 LIST output. Safe for concurrent use.
func (m *Manifest) KeysUnderPrefix(prefix string) []string {
    m.mu.RLock()
    defer m.mu.RUnlock()
    var out []string
    for _, files := range m.files {
        for _, fi := range files {
            if strings.HasPrefix(fi.Key, prefix) {
                out = append(out, fi.Key)
            }
        }
    }
    return out
}
```

---

### 2.5 Discovery integration

The discovery primitive is already wired
(`internal/storage/parquets3/storage.go:946-984` — `RefreshDiscovery`).
The compaction subsystem **does not** add a new refresh loop; it
simply reads the latest peer list on every tick via the closure
threaded into `OwnershipResolver.Peers`.

**Refresh interval:** `cfg.ManifestRefresh` (default 30 s, per
`internal/config/config.go`).

**Debouncing:** owned by `peercache` — `OnRingChange` calls
`processChanges` which sets `stabilizeActive = true` for
`stabilizeDuration` (default 60 s,
`internal/peercache/peercache.go:30`). The scheduler's
`OwnsPartition` check defers during this window, so a flapping ring
naturally backs off without per-component tuning.

**Failure handling:**

| Discovery state | Peers() returns | OwnsPartition behaviour |
|---|---|---|
| Healthy K8s headless service | All pod IPs | Normal HRW |
| Empty list (DNS down at startup) | `[]` | Returns false → no work taken. Logged + metric `compaction_ownership_empty_peers_total`. **Tier A also returns 0 in this state** (no secondary owner exists). |
| Just-me at startup (cold boot before SRV records propagate) | `[self]` | HRW trivially picks self for every partition → I own it all. This is correct and expected — until a second pod arrives, single-pod ownership is the right answer. |
| DNS lag (stale peer list, one peer is actually dead) | Includes dead peer | Dead peer is ranked as owner for some partitions → those partitions are skipped this tick. Next refresh corrects. Tier A picks them up after staleness threshold. |
| DNS lag (missing peer that is actually alive) | Excludes live peer | Both pods may think they own → duplicate work, see §2.3.2 safety. Tier B cleans up. |

---

## 3. Edge-case mitigation — 33 cases

### 3.1 Liveness / correctness

| # | Edge case | Mitigation | Regression test | Severity |
|---|---|---|---|---|
| 1 | Pod dies after output uploaded, before manifest update | Output key is `compacted-L*-{uuid}.parquet`, so the dead pod's S3 file becomes a Tier-B orphan. Live pod's next compaction re-merges the same L0 sources (they were never removed by the dead pod). Result: one orphan file, manifest stays consistent, Tier B garbage-collects after `OrphanTTL`. | `TestOwnership_DiePostUploadPreManifest_OrphanedSafely` | HIGH |
| 2 | Pod dies between manifest-add and input-delete | Already safe today and unchanged: `compactor.go:185-191` calls `m.RemoveFile(...)` then `pool.Delete(...)`; a crash leaves manifest pointing at fresh L1 file PLUS the original L0 files. Next compactor pass sees the L0 files as still-present, treats them as new L0 work (they share schema, will be re-compacted into L2 next cycle). The leftover L0 files are visible to queries (double counting!) — **see §3.6 for the deletion-ordering note**. | `TestOwnership_DiePostManifestPreDelete_DoubleCountedTransient` | CRITICAL — surfaces existing bug, not a new one |
| 3 | Ring flap: two pods both compact same partition | (a) `IsStabilizing()` defers both pods during the 60 s window. (b) If both pods still slip through, output keys are UUID-suffixed, manifest `AddFile` is now idempotent on key (§2.2.4) — first to publish wins, second pod's compacted file becomes Tier-B orphan. (c) `lakehouse_compaction_dual_ownership_total{partition}` alerts the operator. | `TestOwnership_RingFlap_NoDuplicate`, `TestCompactor_ConcurrentSamePartition_OneWinsRestOrphan` | HIGH |
| 4 | Stale peer list (DNS / K8s endpoints lag) | HRW gracefully handles both directions (see §2.5 table). The 30 s discovery interval bounds the staleness window. | `TestOwnership_StalePeerList_EventuallyConverges` | MEDIUM |
| 5 | Network partition: subsets see different peer sets | Same as #4: each partition side computes HRW over its own view. Worst case is dual ownership for partition's lifetime — caught by Tier B (which uses the same peer list per pod, so each pod cleans its own orphans). No data loss. | `TestOwnership_NetworkPartition_BothSidesProgress` | MEDIUM |
| 6 | Manifest snapshot drift between pods | Manifest is in-memory per pod; the source of truth is S3 itself. After compaction, the pusher (`internal/manifest/push.go`) notifies peers, but a peer that hasn't received the notification will re-discover the new files on its next `RefreshFromS3` (default 30 s, `manifest.go:127-262`). HRW ownership is on partition keys (not file keys), so the drift window doesn't affect ownership decisions. | `TestManifest_DriftBetweenPods_OwnershipStable` | LOW |
| 7 | Pod GC pause longer than tick interval | A 5-minute GC pause is unrealistic for Go (typical: <100 ms), but the design tolerates it: the paused pod skips its tick, Tier A picks up the slack after 3×Interval. | `TestOwnership_PausedPod_SecondarySteals` | LOW |
| 8 | All pods crash simultaneously | On restart, every pod sees no `lastAttempt` (in-memory only), HRW deterministically re-assigns ownership, normal scheduling resumes. No orphans to clean up beyond what was already in flight at crash time (covered by #1). | `TestOwnership_FullClusterRestart_ResumesCleanly` | LOW |
| 9 | HRW collision: two pods both think they own | xxh64 has a 2^-64 collision rate; with N=10 peers and 1000 partitions, P(any collision) ≈ 5×10^-15. Even if it happens, the tie-breaker (lex sort) is deterministic so both pods agree on the same primary. **Verify by property test that every (peer, partition) tuple yields exactly one primary.** | `TestOwnership_HRW_ExactlyOnePrimaryProperty` (fuzz) | LOW |
| 10 | HRW gap: nobody owns (transient empty peer list) | `OwnsPartition` returns false when `peers == []`. This is **deliberately conservative**: we'd rather skip a tick than have an unowned partition silently miss compaction. The next discovery refresh fills the list. Tier A treats the absent attempt as "stale" once the staleness window passes, but with `len(peers)==0` it also returns 0 work. | `TestOwnership_EmptyPeerList_RefusesWork` | MEDIUM |
| 11 | Manifest write race (concurrent AddFile) | Single `sync.RWMutex` covers `m.files`, `m.sortedPartitions`, `m.partitionAttempts`, and `m.labelIndex` (`manifest.go:77`). New idempotency check (§2.2.4) prevents duplicate entries on key. Race detector covers it (`internal/manifest/manifest_race_test.go` already exists). | `TestManifest_AddFile_IdempotentOnKey`, `TestManifest_AddFile_ConcurrentSameKeyOnlyOneEntry` | HIGH |
| 12 | Pod 2 deletes input that pod 1 is downloading | (a) Pod 1's `pool.Download(key)` returns the cached bytes if in cache; otherwise S3 returns `NoSuchKey` and the compaction errors out, caught at `compactor.go:108-112`. (b) Pod 2 only deletes after its compaction commits to manifest. (c) Even if pod 1's compaction errors, the manifest is unchanged for pod 1's view, so no corruption — pod 1 just logs and skips. Pod 2's compacted file is the correct output. | `TestCompactor_RacingDeletes_OneSurvives` | MEDIUM |

### 3.2 S3-level

| # | Edge case | Mitigation | Regression test | Severity |
|---|---|---|---|---|
| 13 | S3 LIST eventual-consistency after delete | LIST may briefly return a just-deleted key. Tier B's three-step deletion guards: (a) NOT in manifest, (b) older than `OrphanTTL = 2h` (much longer than S3 EC window), (c) re-read manifest. With this, a stale LIST that *includes* a delete-pending key would still skip it because the second manifest re-read shows it absent and the actual `Delete` is idempotent (NoSuchKey is OK). | `TestOrphanSweep_S3ListEventualConsistency_Safe` | MEDIUM |
| 14 | S3 upload partial failure (multipart fragment leftover) | aws-sdk-go-v2 cleans incomplete multipart uploads via `AbortMultipartUpload` on error — but if a pod dies mid-upload, the fragment remains. We rely on bucket-level lifecycle policy `AbortIncompleteMultipartUpload: 1 day` (chart should configure this; see open question Q5). | `TestOrphanSweep_MultipartFragment_NotMistakenForOrphan` (we never LIST partial uploads, they live under `_multipart/`) | LOW |
| 15 | S3 throttling (429) during heavy compaction | aws-sdk-go-v2 has built-in adaptive retry. We add no compaction-level retry — the next tick will pick up the partition again. Tier B also respects 429 via the same SDK retry. | `TestOrphanSweep_S3Throttled_RetriesViaSDK` | LOW |
| 16 | `_meta/`, `_tombstones/`, `_compaction_lock*` lookalikes — never delete | Tier B's `isProtected(key)` check (§2.4.1): default `NeverDeletePrefixes = ["_meta/", "_tombstones/", "_compaction_lock"]`. Additionally, **only `.parquet` files are eligible**. The `_compaction_lock` prefix is left in the protected list for backward compatibility, even though the sentinel goes away in PR A — the prefix is operator-visible until at least one full retention cycle has passed. | `TestOrphanSweep_NeverDeletesMetaFiles`, `TestOrphanSweep_OnlyDeletesParquet` | CRITICAL |

### 3.3 Membership / topology

| # | Edge case | Mitigation | Regression test | Severity |
|---|---|---|---|---|
| 17 | Pod IP reuse — restart gets new IP, discovery sees new member | HRW on the (new) IP recomputes ownership; old IP drops out, new IP is a "join" event. `IsStabilizing()` defers both pods for 60 s. | `TestOwnership_IPReuse_TreatedAsRejoin` | LOW |
| 18 | Discovery returns empty list (DNS down, K8s API down) | See edge case 10. | (covered by 10) | MEDIUM |
| 19 | Discovery returns just-me at startup (cold boot) | HRW with a single peer gives me 100% of partitions. Correct. | `TestOwnership_SinglePodOwnsEverything` | LOW |
| 20 | Wave-of-pods scaling (HPA: 1→5 in 30 s) | Each new pod triggers a ring change → `IsStabilizing()` → 60 s deferral. After stabilization, HRW redistributes 80 % of partitions to new owners. Tier A picks up any partition that the previous owner failed to advance during the stabilization window. | `TestOwnership_WaveOfPods_NoDuplicateAfterStabilization` | MEDIUM |
| 21 | Multi-AZ peer set → cross-AZ S3 traffic cost | **Deferred.** PR A uses the basic ring (`UpdatePeers`), not the AZ-aware ring (`UpdatePeersWithZones`). HRW over the cross-AZ peer set means a partition in AZ-A may be compacted from AZ-B, pulling from S3 over the AZ boundary. Mitigation: a follow-up PR adds an `AZAware` flag to `OwnershipResolver` that filters peer set to same-AZ first, falling back cross-AZ if no same-AZ peer is available — mirrors the existing pattern at `peercache/ring.go:121-146`. Track as open question Q3. | (none — follow-up PR) | MEDIUM (cost, not correctness) |
| 22 | Partial network failure: pod reaches S3 but not peers | Discovery succeeds (DNS), but actual peer-cache RPC may fail. Ownership is computed from the *discovered* peer list, not RPC health. Worst case: a pod is in the peer set but unhealthy, and partitions HRW-assigns to it are dropped this tick. Tier A picks them up after `3×Interval`. | `TestOwnership_PeerUnreachable_TierAReclaims` | MEDIUM |
| 23 | Pod with stale on-disk manifest snapshot on restart | `manifest.LoadFrom` (`manifest.go:626-655`) reads the snapshot, then a `RefreshFromS3` reconciles with S3 (`manifest.go:127-262`). After reconciliation the manifest is correct. HRW ownership only depends on partition keys, not file lists, so a stale snapshot doesn't affect ownership decisions during the reconciliation gap. | `TestManifest_StaleDiskSnapshot_ReconciledByRefresh` (already exists in some form, verify) | LOW |

### 3.4 Compaction-policy

| # | Edge case | Mitigation | Regression test | Severity |
|---|---|---|---|---|
| 24 | Partition with single huge L0 file | `Policy.SelectFiles` returns 1 file, scheduler checks `len(selected) < 2` and skips (no merging possible with a single source). The huge file stays at L0 indefinitely (correct: nothing to merge with). Unchanged from today. | (existing) `TestPolicy_SingleHugeFile_Skipped` | LOW |
| 25 | Late-arriving inserts | New L0 file appears mid-compaction → not selected for *this* run (we use the snapshot from start of run), picked up next tick. Idempotency of `AddFile` (§2.2.4) means the late file's manifest entry is preserved. | `TestScheduler_LateArrivingInsert_NextTick` | LOW |
| 26 | Schema evolution mid-compaction | `compactor.go:88-93` already verifies all selected files share a fingerprint. The scheduler's `MajoritySchemaFingerprint` selects only the dominant FP. A new schema appears → new fingerprint group → next tick compacts the new group separately. | (existing) | LOW |
| 27 | Compaction backpressure (ingest >> compaction) | Unchanged today. `MaxConcurrent` caps per-pod parallelism. Multi-pod scaling distributes load via HRW. Operator alerts on `lakehouse_storage_files_total / lakehouse_compaction_runs_total` ratio. | `TestScheduler_BackpressureMetricVisible` | MEDIUM |
| 28 | Tenant isolation | Tenant prefix is part of the partition key (`{accountID}/{projectID}/{signal}/dt=.../hour=...`). HRW takes the full key, so each tenant's partitions are independently distributed. No cross-tenant interaction. | `TestOwnership_TenantIsolation_NoCrossTalk` | LOW |
| 29 | Per-tenant compaction quotas | Out of scope for PR A. Currently no quotas exist; if a future PR adds them, the quota check goes inside the per-partition loop in `Scan` (after ownership, before MarkAttempt). Track as open question Q4. | (none — follow-up) | LOW |

### 3.5 Clock / timing

| # | Edge case | Mitigation | Regression test | Severity |
|---|---|---|---|---|
| 30 | Clock skew between pods → watermark comparisons fail | `MarkAttempt` writes `time.Now()` from the pod doing the compaction. Tier A reads `time.Since(lastAttempt)` from the **same** pod's clock (it's an in-memory map, single-pod read). Per-pod clocks are not compared. Skew is irrelevant. | `TestOrphanSweep_ClockSkewBetweenPods_Irrelevant` (negative control: stub `time.Now` returning wildly different values on two `Manifest` instances — both should still self-clean) | LOW |
| 31 | SIGTERM mid-compaction — graceful drain | Current shutdown sequence: `sched.Stop()` closes `stopCh`, ticker exits at the next iteration. A compaction in flight runs to completion. PR A keeps this behaviour — no change. **Negative control:** verify `sched.Stop()` does not wait for an in-flight compaction longer than `Interval + ComputeBudget`. | `TestScheduler_SIGTERMDuringCompaction_CompletesOrAborts` | MEDIUM |

### 3.6 Observability

| # | Edge case | Mitigation | Regression test | Severity |
|---|---|---|---|---|
| 32 | `lakehouse_compaction_partitions_owned{pod=X}` sum across cluster must equal `manifest.PartitionCount()` | New metric `lakehouse_compaction_partitions_owned` (gauge) set on every tick to `len(filter(allFiles, OwnsPartition))`. CI integration test starts 3 in-process schedulers with a shared `Manifest`, asserts sum equals `m.PartitionCount()`. | `TestObservability_OwnedPartitionsSumEqualsTotal` | HIGH (correctness invariant via metrics) |
| 33 | `lakehouse_compaction_orphan_files_deleted_total` counter + alerting target | New counter on Tier B success. Alert rule (Grafana): `increase(lakehouse_compaction_orphan_files_deleted_total[1h]) > 100` ⇒ paging "orphan rate suggests dual ownership or upload failures". Default behaviour with no failures: counter ticks 0/h. | `TestObservability_OrphanDeletedCounterIncrements` | MEDIUM |

### 3.7 Deletion safety rule (Tier B)

The Tier B sweep performs **three independent checks** before any
DELETE call (§2.4.1 implementation):

| Check | Predicate | Where in code |
|---|---|---|
| (a) NOT in manifest | `key ∉ manifestSet` (taken from `manifest.KeysUnderPrefix(prefix)` at the top of the prefix loop) | `RunTierB`, inner loop, first guard |
| (b) Age > OrphanTTL AND .parquet AND not in protected prefixes | `time.Since(HeadObject(key).LastModified) > OrphanTTL` and `isProtected(key) == false` | `RunTierB`, inner loop, second guard |
| (c) Re-read manifest before DELETE | `key ∉ manifestKeys2` where `manifestKeys2 = manifest.KeysUnderPrefix(prefix)` at delete time | `RunTierB`, inner loop, third guard |

**Note on #2 (the "leftover L0 after partial manifest add" bug):** the
current compactor calls `manifest.AddFile` *then* `manifest.RemoveFile`
*then* `pool.Delete`. The fix has two parts:

- **Manifest add/remove ordering:** keep `AddFile` *before* `RemoveFile`
  (current order) — a crash between them leaves L1 + L0 visible to
  queries (double-count window). This is acceptable for *correctness*
  (no data loss) but bad for *cost* (queries scan duplicates) and
  for *test sanity* (`TestIntegration_FullCompactionCycle` would
  fail in this window).
- **`pool.Delete` failure handling:** today (`compactor.go:188-191`)
  the delete failure is **logged then swallowed**, so the manifest
  goes ahead with `RemoveFile`. A failed S3 delete leaves the L0 file
  as an orphan from the manifest's perspective; Tier B eventually
  reclaims it. This is correct *now* but only because we've added
  Tier B; the spec calls out that this is the explicit relied-upon
  contract.

To make the double-count window observable, we add
`lakehouse_compaction_double_count_window_seconds` (gauge): set to
the manifest's longest `(L1.CreatedAt - L0.CreatedAt)` for files
sharing a partition and overlapping time bounds. Steady-state value
is 0; a positive value indicates an incomplete `RemoveFile` cleanup.

---

## 4. Migration path

The PR A diff is large but mechanically simple. We order the changes
so the build is green at every intermediate commit.

### 4.1 PR A — order of commits

1. **Commit 1:** Add new files; no behaviour change.
   - `internal/compaction/ownership.go` (new)
   - `internal/compaction/ownership_test.go` (new — table + fuzz tests)
   - `internal/compaction/orphan_sweep.go` (new)
   - `internal/compaction/orphan_sweep_test.go` (new)
   - `internal/manifest/manifest.go` — add `partitionAttempts`,
     `MarkAttempt`, `LastAttempt`, `AttemptsView`, `KeysUnderPrefix`,
     idempotency guard on `AddFile`
   - `internal/manifest/manifest_attempts_test.go` (new)
   - `internal/metrics/lakehouse.go` — add the 6 new metric families
   - Build green; no callers yet.

2. **Commit 2:** Rewire scheduler to use `OwnershipResolver` instead
   of `Leader` + `Sharding` + `Sentinel`.
   - `internal/compaction/scheduler.go` — rewrite per §2.3
   - Existing scheduler tests updated to construct an
     `OwnershipResolver` instead of a `Leader`.
   - **Existing `internal/election` package still compiles** (no
     deletes yet) so reverse-CI is straightforward.

3. **Commit 3:** Wire main.go for both modules.
   - `cmd/lakehouse-logs/main.go` — replace election construction
     with ownership resolver, start orphan sweep, drop sentinel.
   - `lakehouse-traces/main.go` — same.
   - This commit *also* deletes the now-unused calls to
     `election.NewAutoElector` but keeps the import path (since the
     package isn't deleted yet).

4. **Commit 4:** Drop the `Leader` / `Sentinel` / `Sharding` types
   from the scheduler API entirely. Update any remaining test that
   constructed them.
   - `internal/compaction/scheduler.go` — final cleanup
   - `internal/compaction/sentinel.go` — DELETE
   - `internal/compaction/sentinel_test.go` — DELETE
   - `internal/compaction/sharding.go` — DELETE
   - `internal/compaction/sharding_test.go` — DELETE
   - `internal/compaction/integration_test.go` — update (drop
     `election.NewNoopElector`, drop sentinel construction)

5. **Commit 5:** Drop election from the bloomindex controller. This
   is dead code today (no caller) but the symbol leaks the concept.
   - `internal/bloomindex/controller.go` — remove `isLeader` field,
     `SetLeader`, `IsLeader`
   - `internal/bloomindex/controller_test.go` — drop the 11
     `bc.SetLeader(true)` lines, gate any test that needs "leader-only"
     behaviour on a different field (none, today).

6. **Commit 6:** Documentation
   - `docs/architecture.md` — replace election section with HRW
     ownership
   - `docs/operations.md` — replace election runbook entries
   - `internal/compaction/README.md` (new, short) — describes
     HRW ownership + orphan sweep, mirrors what was in
     `internal/election/RUNBOOK.md`.
   - `tests/verification/matrix.md` — update L11 (binary bloat) note
     to reflect the *new* baseline post-election-removal.

PR B (chart + flags) and PR C (delete election package) are split out
in §7.

### 4.2 Files to delete (full list, applied in PR C)

Listed for the implementation agent so nothing is missed.

```
internal/election/auto.go                                   (100 LoC)
internal/election/auto_test.go                              (149)
internal/election/coverage_hardening_test.go                (127)
internal/election/election_coverage_test.go                 (649)
internal/election/k8s.go                                    (642)
internal/election/k8s_fuzz_test.go                          (112)
internal/election/k8s_integration_test.go                   (131)
internal/election/k8s_leak_test.go                          (63)
internal/election/k8s_regression_test.go                    (291)
internal/election/k8s_soak_test.go                          (83)
internal/election/k8s_test.go                               (1040)
internal/election/leader.go                                 (10)
internal/election/leader_test.go                            (70)
internal/election/leak_test.go                              (268)
internal/election/memleak_test.go                           (222)
internal/election/noop.go                                   (14)
internal/election/s3.go                                     (258)
internal/election/s3_test.go                                (572)
internal/election/README.md                                 (109)
internal/election/RUNBOOK.md                                (182)
internal/compaction/sentinel.go                             (72)
internal/compaction/sentinel_test.go                        (~120)
internal/compaction/sharding.go                             (55)
internal/compaction/sharding_test.go                        (~80)
.github/workflows/e2e-k8s.yaml                              (118)
tests/e2e-k8s/test_leader_election.sh                       (~480)
tests/e2e-k8s/kind-config.yaml                              (~20)
tests/e2e-k8s/                                              (directory)
tests/verification/probe_k8s_election_failover.sh           (51)
charts/victoria-lakehouse/templates/compaction-rbac.yaml    (57)
charts/victoria-lakehouse/templates/tenant-rbac.yaml        (37)
```

Total LoC deleted (rough): **~6 200 lines** including tests.

### 4.3 Files to add (full list)

```
internal/compaction/ownership.go                            (~150 LoC)
internal/compaction/ownership_test.go                       (~400)
internal/compaction/ownership_fuzz_test.go                  (~80)
internal/compaction/orphan_sweep.go                         (~280)
internal/compaction/orphan_sweep_test.go                    (~500)
internal/compaction/orphan_sweep_integration_test.go        (~250)
internal/compaction/README.md                               (~120 LoC docs)
docs/superpowers/specs/2026-05-31-election-free-compaction.md (this file)
tests/verification/probe_compaction_ownership.sh            (~80)
tests/verification/probe_orphan_sweep.sh                    (~100)
```

Add ~2 000 LoC (mostly tests), delete ~6 200 LoC → **net −4 200 LoC**.

### 4.4 Files to modify (line-level sketch)

| File | Change |
|---|---|
| `internal/compaction/scheduler.go` | Remove `Leader`, `Sentinel`, `Sharding` fields; add `Ownership`. Rewrite `Scan` per §2.3. ~30 LoC delta. |
| `internal/manifest/manifest.go` | +`partitionAttempts` field; +`MarkAttempt`, `LastAttempt`, `AttemptsView`, `KeysUnderPrefix`; idempotency guard in `AddFile`. ~50 LoC delta. |
| `internal/metrics/lakehouse.go` | Remove 3 election metrics (lines 213-218). Add 6 new compaction metrics (§6). |
| `internal/metrics/verify_test.go` | Drop 3 election metric entries (lines 44, 128, 201). |
| `cmd/lakehouse-logs/main.go` | Lines 18 (election import), 22, 250-267, 269, 277-289, 292, 295, 297, 318, 1047-1058 (applyCompactionFlags). ~90 LoC delta. Replace with construction of `OwnershipResolver`, `OrphanSweep`. |
| `lakehouse-traces/main.go` | Mirror logs/main.go. **Line-level parity per `feedback_logs_traces_module_parity`.** |
| `cmd/lakehouse-logs/main.go` (flag block) | Remove `compactionElection`, `compactionShardID`, `compactionShardCount`. Lines 82, 84, 85. |
| `lakehouse-traces/main.go` (flag block) | Same. |
| `internal/config/config.go` | `CompactionConfig`: remove `LeaderElection`, `LeaseDuration`, `S3LockTTL`, `S3Heartbeat`, `ShardID`, `ShardCount` (lines 414-419). Remove their defaults (lines 637-642), validators (lines 939-943, 1017-1019), and overlay logic (lines 1619-1631). |
| `internal/config/config_coverage_test.go` | Drop `TestValidate_LeaderElectionModes`, `TestValidate_InvalidLeaderElection`, and the 4 lines exercising the removed overlay fields. |
| `internal/compaction/integration_test.go` | Replace `election.NewNoopElector()` constructions with `&OwnershipResolver{Self: "x", Peers: func() []string { return []string{"x"} }}` (single-peer → owns everything). |
| `internal/bloomindex/controller.go` | Remove `isLeader` field, `SetLeader`, `IsLeader` methods (lines 52, 81-93). Remove the `if !bc.isLeader { return }` guard in `Observe` (line 114). |
| `internal/bloomindex/controller_test.go` | Drop all 11 `bc.SetLeader(true)` lines. Drop `TestBloomController_IsLeader`. |
| `charts/victoria-lakehouse/values.yaml` | Remove `leader_election`, `lease_duration`, `s3_lock_ttl`, `s3_heartbeat`, `shard_id`, `shard_count` keys under `compaction` (lines 463-474). Remove `leader_election`, `lease_duration` under `tenant.aliases` (lines 441, 443). |
| `charts/victoria-lakehouse/templates/*.yaml` | Drop `compaction-rbac.yaml` and `tenant-rbac.yaml` (full file deletes). |
| `tests/verification/matrix.md` | Update the "L11 — Binary bloat" narrative — new baseline post-election-removal is ~30 MB (drop the 7 MB elector overhead). |
| `go.mod` | Remove `k8s.io/apimachinery v0.36.0` and `k8s.io/client-go v0.36.0` from `require`. |
| `go.sum` | `go mod tidy`. |
| `.github/workflows/ci.yaml` | If any path filters reference `internal/election/**`, drop them. |
| `.github/workflows/e2e-k8s.yaml` | Whole-file delete. |
| `Dockerfile.logs`, `Dockerfile.traces` | No change (no K8s SDK linker flags to remove; the dep was load-bearing in the binary, not in the Dockerfile). Verify by running the existing `TestBinarySizeBound` after deletion — expect ~3 MB shrink. |
| `CHANGELOG.md` | New release notes: "compaction: replace leader election with hash-based ownership". |

### 4.5 Flag removals (CLI)

`cmd/lakehouse-logs/main.go` (mirror in `lakehouse-traces/main.go`):

- `lakehouse.compaction.leader-election` (line 82) — REMOVE
- `lakehouse.compaction.shard-id` (line 84) — REMOVE
- `lakehouse.compaction.shard-count` (line 85) — REMOVE

The remaining compaction flags (`enabled`, `interval`,
`daily-rollup-age`) stay.

### 4.6 Helm values removals

`charts/victoria-lakehouse/values.yaml`:

- `lakehouseConfig.compaction.leader_election` (line 464)
- `lakehouseConfig.compaction.lease_duration` (line 466)
- `lakehouseConfig.compaction.s3_lock_ttl` (line 468)
- `lakehouseConfig.compaction.s3_heartbeat` (line 470)
- `lakehouseConfig.compaction.shard_id` (line 472)
- `lakehouseConfig.compaction.shard_count` (line 474)
- `lakehouseConfig.tenant.aliases.leader_election` (line 441) — see Q6
- `lakehouseConfig.tenant.aliases.lease_duration` (line 443) — see Q6

### 4.7 Config struct field removals

`internal/config/config.go`, `CompactionConfig` (lines 406-420):

- `LeaderElection string`
- `LeaseDuration  time.Duration`
- `S3LockTTL      time.Duration`
- `S3Heartbeat    time.Duration`
- `ShardID        int`
- `ShardCount     int`

After removal: `CompactionConfig` has 7 fields (`Enabled`, `Interval`,
`MaxConcurrent`, `MinFilesL0`, `MinFilesL1`, `MinAge`,
`DailyRollupAge`).

Defaults updated (line 629), overlay merge updated (lines 1619-1631).

---

## 5. Test plan

### 5.1 Coverage targets

| Package | New files | Coverage target | Why |
|---|---|---|---|
| `internal/compaction/ownership.go` | `ownership_test.go` + `ownership_fuzz_test.go` | **≥ 95 %** | Pure logic, no I/O — should be 100 %. We allow 5 % slack for defensive nil-checks that fuzz might not hit. |
| `internal/compaction/orphan_sweep.go` | `orphan_sweep_test.go` (table) + `orphan_sweep_integration_test.go` (httptest) | **≥ 90 %** | Has I/O paths; integration test covers the happy-path Tier B prefix walk. |
| `internal/manifest/manifest.go` (new methods) | `manifest_attempts_test.go` | **≥ 95 %** | Trivial — exercise every accessor + the idempotency guard. |
| `internal/compaction/scheduler.go` (rewritten) | Existing `scheduler_test.go` updated | **≥ 90 %** (current is ~88 %) | Existing tests already cover most code paths; we add: ownership-deferred-on-stabilization, no-leader-needed (single pod), wave-of-pods. |

### 5.2 Unit tests — `ownership_test.go`

Each test name doubles as the spec contract.

| Test | Asserts | Negative control (revert to make FAIL) |
|---|---|---|
| `TestOwnership_NilOwnership_PanicsClearly` | `OwnsPartition` on a zero-value resolver panics with a clear message | Remove nil check → panic without message |
| `TestOwnership_EmptyPeerList_RefusesWork` | `Peers() == nil` → `OwnsPartition` returns false; metric `ownership_empty_peers` increments | Comment out the empty-peer guard → returns true (incorrect) |
| `TestOwnership_SinglePeerSelf_AlwaysOwns` | `Peers() == [self]` → `OwnsPartition("any")` returns true | Comment out the trivial case → could still pass via HRW but assertion verifies the *fast path* |
| `TestOwnership_SinglePeerNotSelf_NeverOwns` | `Peers() == [other]` → `OwnsPartition` returns false | Bug in `Self` propagation → returns true |
| `TestOwnership_Stabilizing_ReturnsFalse` | `Stabilizing() == true` → `OwnsPartition` returns false even when HRW agrees | Remove stabilizing guard → returns true (correct HRW result), but spec says defer |
| `TestOwnership_RankedOwners_DeterministicTieBreak` | Two peers with engineered hash collision (via injected hash) → lex sort decides primary | Remove tiebreak → flaky between runs |
| `TestOwnership_OwnsPartition_TableDriven` | Table: 5 peers × 100 partitions; exactly one peer owns each partition; aggregate ownership is within ±5 % of `1/N` | Disable HRW (use constant hash) → distribution skews to one peer |
| `TestOwnership_SecondaryOwner_NeverEqualsPrimary` | For every partition, `SecondaryOwner != OwnerOf` when `len(peers) ≥ 2` | Bug in ranking → primary leaks into secondary slot |
| `TestOwnership_RankedOwners_LengthMatchesPeerCount` | `len(RankedOwners(p)) == len(Peers())` | Off-by-one in slice allocation |
| `TestOwnership_AddRemovePeer_OnlyMinorRedistribution` | Add 1 peer to a 4-peer ring → ≤ 25 % of partitions change ownership (HRW guarantee) | Switch from HRW to CRC32 hashing → distribution can churn 50–100 % |
| `TestOwnership_RingFlap_NoDuplicateAcrossPods` | Simulate 2 in-process `OwnershipResolver` instances; rapidly mutate their peer lists; assert no partition has two owners *outside* the stabilization window | Remove the stabilization gate → duplicates appear during flap |
| `TestOwnership_PausedPod_SecondarySteals` | Pod A is "primary" but `Stabilizing()` returns true forever (paused); pod B's `OwnsPartition` should still return false (B is secondary) until Tier A invokes it explicitly | Bug: secondary auto-promotes → duplicate work |
| `TestOwnership_IPReuse_TreatedAsRejoin` | Peer list change `[A, B]` → `[A]` → `[A, B']` where B' has new IP → HRW recomputes; B' gets some partitions, B's old ones reassigned | (smoke test only) |

### 5.3 Fuzz / property tests — `ownership_fuzz_test.go`

| Test | Asserts |
|---|---|
| `FuzzOwnership_ExactlyOnePrimary` | For random `(peers, partition)`, exactly one peer in `RankedOwners[0]` matches the "winning" definition (highest weight). |
| `FuzzOwnership_RankedOwnersIsPermutationOfPeers` | `sort(RankedOwners(p)) == sort(peers)` |
| `FuzzOwnership_HRWDistribution` | For 1 000 random partitions across 10 peers, each peer's ownership share is in `[0.8/N, 1.2/N]`. |

### 5.4 Manifest tests — `manifest_attempts_test.go`

| Test | Asserts | Negative control |
|---|---|---|
| `TestManifest_MarkAttempt_Records` | `MarkAttempt(p, t)` then `LastAttempt(p) == t` | Remove the assignment → zero time always |
| `TestManifest_LastAttempt_UnseenPartition_ZeroTime` | `LastAttempt("never-marked")` returns zero `time.Time` | Bug: returns `time.Now()` |
| `TestManifest_AttemptsView_IncludesAllPartitions` | `AttemptsView()` contains every partition in `m.files`, even those with no recorded attempt | Bug: only returns marked partitions → orphan sweep would miss cold partitions |
| `TestManifest_AddFile_IdempotentOnKey` | Two `AddFile(p, fi)` with same key → `len(FilesForPartition(p)) == 1` | Remove the idempotency guard → length is 2 |
| `TestManifest_AddFile_ConcurrentSameKeyOnlyOneEntry` | 100 goroutines all calling `AddFile(p, fi)` with the same key — final count is 1 | Run without `sync.Mutex` → race detector fires |
| `TestManifest_KeysUnderPrefix_ReturnsMatching` | Add files with keys `a/x.parquet`, `a/y.parquet`, `b/z.parquet`; `KeysUnderPrefix("a/")` returns `[a/x.parquet, a/y.parquet]` | Bug: returns all keys → Tier B would delete too much |
| `TestManifest_KeysUnderPrefix_EmptyPrefix_ReturnsAll` | `KeysUnderPrefix("")` returns every key | Bug: returns nothing for empty prefix |
| `TestManifest_PartitionAttempts_RaceFree` | Concurrent `MarkAttempt` + `LastAttempt` + `AttemptsView` — race detector clean | Remove lock → data race |

### 5.5 Orphan-sweep tests — `orphan_sweep_test.go`

| Test | Asserts | Negative control |
|---|---|---|
| `TestOrphanSweep_TierA_StalePartitionTaken` | Pod A doesn't `MarkAttempt` for 3 × Interval; pod B (secondary) Tier A picks it up | Reduce staleness threshold to 0 → B always grabs (thundering herd) |
| `TestOrphanSweep_TierA_FreshAttempt_NotTaken` | Pod A `MarkAttempt`ed 10s ago; pod B's Tier A skips | Bug: ignore the timestamp → always steal |
| `TestOrphanSweep_TierA_PrimaryOwnerAlsoSecondary_NoSteal` | Single-pod cluster — primary == secondary → no steal | Bug: steal-from-self loop |
| `TestOrphanSweep_TierA_DeferredOnStabilization` | `Stabilizing() == true` → Tier A returns 0 work | Remove guard → steals during ring change (races primary) |
| `TestOrphanSweep_TierA_NotEligible_NoSteal` | Stale partition but `Policy.Eligible == false` → no steal | Bug: ignore eligibility → wasted IO |
| `TestOrphanSweep_TierB_NeverDeletesMetaFiles` | S3 contains `_meta/foo.json`, `_tombstones/abc.json`; Tier B never deletes these | Remove `isProtected` → CRITICAL data loss |
| `TestOrphanSweep_TierB_OnlyDeletesParquet` | S3 contains `foo.txt`, `bar.bin`; Tier B never deletes | Remove `.parquet` filter → arbitrary deletes |
| `TestOrphanSweep_TierB_RespectsOrphanTTL` | Orphan file with mtime 1h ago; default `OrphanTTL = 2h` → not deleted | Remove age check → race-condition deletes |
| `TestOrphanSweep_TierB_ThreeStepSafety` | Orphan passes check (a) and (b); between (b) and (c), a different pod adds the key to manifest; Tier B does NOT delete | Skip (c) → deletes a just-published file |
| `TestOrphanSweep_TierB_PrefixHashOwnership` | 3 pods, 10 prefixes; assert each prefix is processed by exactly one pod (hash(prefix) % 3 == self_idx) | Remove hash gate → all 3 pods scan all 10 prefixes (3× the LIST calls) |
| `TestOrphanSweep_TierB_DeferredOnStabilization` | `Stabilizing() == true` → Tier B returns 0 work | Remove guard → deletes during ring change (incorrect ownership) |
| `TestOrphanSweep_TierB_S3ListEventualConsistency` | Mock S3 returns a key in LIST that was just deleted (consistency lag); Tier B's third check (re-read manifest) sees it absent; `Delete` returns NoSuchKey which is ignored | Skip the second `KeysUnderPrefix` call → harmless but bumps the orphan counter incorrectly |
| `TestOrphanSweep_TierB_S3Throttled_NoRetry` | Mock S3 returns 429; Tier B logs + skips, no infinite retry | Add a retry loop → flow-control bug |
| `TestOrphanSweep_TierB_EmptyPeerList_NoWork` | `Peers() == []` → Tier B returns 0 work (defensive) | Remove guard → divide-by-zero |
| `TestOrphanSweep_Hooks_OnStealFires` | `OnSteal` callback invoked with correct partition + primary | (smoke) |
| `TestOrphanSweep_ClockSkewBetweenPods_Irrelevant` | Two `Manifest` instances with `time.Now` stubs differing by 1 hour; Tier A behaves identically (uses *local* clock for `time.Since(lastAttempt)`) | (negative control: if Tier A compared timestamps *across* manifests, this test would fail) |

### 5.6 Scheduler tests — `scheduler_test.go` (updated)

Mostly delta from existing tests:

| Test | Asserts | Negative control |
|---|---|---|
| `TestScheduler_NoOwnership_NoWork` | `OwnsPartition` returns false for all partitions → `Scan` returns 0 | Bug: ignore ownership → scans everything |
| `TestScheduler_RecordsAttemptBeforeCompact` | After `Scan`, `manifest.LastAttempt(p)` is set even if compaction fails | Move `MarkAttempt` after compaction → Tier A would steal mid-flight work |
| `TestScheduler_StabilizationDefersScan` | `Stabilizing() == true` → `Scan` returns 0 without enumerating partitions; `CompactionDeferredStabilizing` metric increments | Remove guard → spurious dual-compaction during flap |
| `TestScheduler_NoSentinel_NoSharding_NoLeader_StillWorks` | Construct `SchedulerConfig` with only the new fields; `Scan` works on a 1-peer cluster | (smoke: confirms the type signature change) |

### 5.7 Integration tests — `orphan_sweep_integration_test.go`

Multi-pod simulator using `httptest.Server` for S3 (pattern already
in tree at `internal/election/k8s_test.go` and
`internal/storage/parquets3/storage_coverage_test.go`):

| Test | Setup | Asserts |
|---|---|---|
| `TestIntegration_ThreePodCluster_NoSentinel_NoDuplicates` | 3 in-process schedulers sharing one `httptest`-backed S3 + 3 separate manifests; pre-seed 30 partitions × 10 L0 files | After 10 ticks (10 × Interval), each partition has been compacted exactly once; final manifest sum across pods is consistent |
| `TestIntegration_ThreePodCluster_PodCrash_TierAReclaims` | Same setup; kill pod A mid-tick (close its `stopCh`); assert pod B's Tier A reclaims A's partitions within `3 × Interval + TierAInterval` | Tier A actually runs |
| `TestIntegration_ThreePodCluster_RingFlap_Stabilizes` | Same setup; add/remove pod C every 30 s for 5 minutes; assert no partition gets two distinct compacted-L1 files (modulo Tier-B cleanup) | Stabilization + idempotency guard work together |
| `TestIntegration_ThreePodCluster_OrphanSweepCleansDuplicates` | Bypass stabilization (inject `Stabilizing` returning false); deliberately race two pods on the same partition; assert Tier B reclaims the loser's output | Confirms duplicate-as-orphan story |
| `TestIntegration_SinglePodCluster_HappyPath` | 1 pod, no peers; assert compaction runs normally (degenerate HRW) | Single-pod path works without ANY ownership wiring |

### 5.8 e2e tests — docker-compose 3-pod cluster

The existing `deployment/docker/docker-compose-e2e.yml` already runs
a multi-pod LH stack. We add:

- `tests/verification/probe_compaction_ownership.sh` — starts the 3-pod
  compose, ingests 1 GB of logs over 10 minutes, asserts:
  - Sum of `lakehouse_compaction_partitions_owned` across pods equals
    `lakehouse_storage_partitions_total`.
  - `lakehouse_compaction_runs_total` is roughly distributed 1/3 / 1/3 / 1/3.
  - `lakehouse_compaction_dual_ownership_total == 0` over the run.

- `tests/verification/probe_orphan_sweep.sh` — same compose, but
  manually upload a stray `dt=.../hour=.../leftover-XYZ.parquet` file
  (not in any manifest); assert it disappears within 1h + OrphanTTL.

### 5.9 Negative controls — verification matrix update

For every test in §5.2-5.7 the "Negative control" column states what
to revert to make the test FAIL. This is the **harden-and-lock**
contract (per `feedback_harden_and_lock`): the implementation agent
must run each test, then revert the corresponding fix, re-run, confirm
the test fails, then re-apply the fix and confirm it passes. PR A
must include a script `tests/verification/run_negative_controls.sh`
that automates this for the critical-severity tests (rows marked
CRITICAL or HIGH in §3).

### 5.10 Test commands the implementation agent must run before claiming "done"

```bash
# Per feedback_close_feedback_loops, no "done" without full local pass.
GOWORK=off go test -race -count=1 -timeout=300s ./internal/compaction/...
GOWORK=off go test -race -count=1 -timeout=300s ./internal/manifest/...
GOWORK=off go test -race -count=1 -timeout=300s ./internal/metrics/...
GOWORK=off go test -race -count=1 -timeout=300s ./internal/bloomindex/...
GOWORK=off go test -race -count=1 -timeout=300s ./internal/config/...
GOWORK=off go test -race -count=1 -timeout=600s ./...

# Coverage check
GOWORK=off go test -coverprofile=/tmp/ownership.cov -coverpkg=./internal/compaction ./internal/compaction/...
go tool cover -func=/tmp/ownership.cov | grep -E "ownership.go|orphan_sweep.go"

# Build both binaries; size sanity check
make build-logs build-traces
ls -lh bin/lakehouse-logs bin/lakehouse-traces   # expect ~30-32 MB (down from 37)

# E2E
docker compose -f deployment/docker/docker-compose-e2e.yml up -d --build
bash tests/verification/probe_compaction_ownership.sh
bash tests/verification/probe_orphan_sweep.sh
docker compose -f deployment/docker/docker-compose-e2e.yml down

# Negative controls (per harden-and-lock)
bash tests/verification/run_negative_controls.sh

# Lint
GOWORK=off make lint
```

---

## 6. Observability

### 6.1 New metric families (6)

```go
// File: internal/metrics/lakehouse.go — ADD AFTER compaction metric block

// Ownership / HRW metrics
var (
    // Gauge: number of partitions this pod believes it owns. Sum
    // across the cluster MUST equal lakehouse_storage_partitions_total.
    CompactionPartitionsOwned = NewGauge("lakehouse_compaction_partitions_owned")

    // Gauge 0/1: is this pod's Self address present in its own peer
    // list? 0 = self-discovery broken — ownership decisions are
    // suspect; alert page-able.
    CompactionOwnershipSelfInPeers = NewGauge("lakehouse_compaction_ownership_self_in_peers")

    // Counter: ticks where ring stabilization caused us to defer
    // either the scheduler scan or an orphan sweep tier.
    CompactionDeferredStabilizing = NewCounter("lakehouse_compaction_deferred_stabilizing_total")
    CompactionSweepDeferredStabilizing = NewCounterVec(
        "lakehouse_compaction_sweep_deferred_stabilizing_total", "tier",
    )

    // Counter: ticks where Peers() returned an empty list (DNS down).
    CompactionOwnershipEmptyPeers = NewCounter("lakehouse_compaction_ownership_empty_peers_total")

    // Counter, by ring-change type ("join"/"leave"): observed ring
    // membership events as seen by the compaction subsystem.
    CompactionRingChangesTotal = NewCounterVec("lakehouse_compaction_ring_changes_total", "type")
)

// Orphan-sweep metrics
var (
    // Counter: partitions stolen by Tier A from a stale primary owner.
    CompactionStolenTotal = NewCounter("lakehouse_compaction_stolen_total")

    // Counter: orphan .parquet files deleted by Tier B. Default rate
    // is ~0/hour; >100/hour suggests dual ownership or upload
    // failures — alert page-able.
    CompactionOrphansDeleted = NewCounter("lakehouse_compaction_orphan_files_deleted_total")

    // Counter, by reason: orphan candidates rejected by safety checks.
    // Reasons: "in_manifest", "too_young", "protected_prefix",
    // "not_parquet", "manifest_drift_race".
    CompactionOrphansSkipped = NewCounterVec("lakehouse_compaction_orphans_skipped_total", "reason")

    // Counter: cases where two pods both compacted the same partition.
    // Should be 0 in steady state; >0 indicates ring-flap or
    // DNS-lag-induced dual ownership. Page-able if increasing.
    CompactionDualOwnershipTotal = NewCounterVec("lakehouse_compaction_dual_ownership_total", "partition")

    // Gauge: longest window during which a partition has both L0 and
    // L1 files for the same logical time range — indicates a partial
    // RemoveFile cleanup. Steady-state value 0.
    CompactionDoubleCountWindow = NewGauge("lakehouse_compaction_double_count_window_seconds")
)
```

### 6.2 Metrics removed

```
lakehouse_election_leader                  (gauge)
lakehouse_election_transitions_total       (counter)
lakehouse_election_health_checks_total     (counterVec)
```

All three are currently defined but **never set** (grepped:
`metrics.Election*` has zero call sites under `internal/`). Removing
them is safe.

### 6.3 Log lines at key transitions

| Event | Log line | Level | Where |
|---|---|---|---|
| Pod startup, ownership configured | `compaction: ownership resolver active; self=%s peers_init=%d` | INFO | `cmd/lakehouse-*/main.go` |
| Ring change | `compaction: ring change observed; type=%s peer=%s old=%d new=%d — deferring for %v` | INFO | `OnRingChange` callback |
| Stabilization defer (scheduler scan) | (none — too noisy; tick the metric instead) | — | — |
| Compaction start | (existing) `compaction complete; partition=...` | INFO | `compactor.go:202` |
| Compaction skipped (not owner) | (none — too noisy) | — | — |
| Tier A steal | `tier_a: stealing partition; partition=%s primary_owner=%s last_attempt=%s` | INFO | `orphan_sweep.go` |
| Tier B orphan delete | `tier_b: deleting orphan; key=%s age=%v` | INFO | `orphan_sweep.go` |
| Tier B orphan skip (rare reason) | `tier_b: skipping candidate; key=%s reason=%s` | DEBUG | `orphan_sweep.go` |
| Self not in peers (alert!) | `compaction: WARNING self=%s not in peer set %v — ownership decisions suspect` | WARN | `OwnershipResolver` first-tick check |

### 6.4 Runbooks

#### 6.4.1 "Compaction is lagging" diagnostic flow

1. Check `lakehouse_storage_files_total{compaction_level="0"}` — is it
   growing unbounded?
2. Check `rate(lakehouse_compaction_runs_total[5m])` per pod — are
   any pods at zero?
3. If one pod is at zero compactions but others are not:
   - Check `lakehouse_compaction_partitions_owned` for that pod — is
     it owning anything?
   - If 0: check `lakehouse_compaction_ownership_self_in_peers` — if
     0, the pod isn't seeing itself in discovery (DNS / hostNetwork
     misconfig). Check `kubectl get endpoints <headless-svc>` and
     compare against the pod's `httpListenAddr`.
   - If > 0: check `lakehouse_compaction_deferred_stabilizing_total`
     — if rising, the ring is constantly flapping. Investigate K8s
     network instability or HPA churn.
4. If all pods are slow but ingest is high:
   - Check `lakehouse_compaction_max_concurrent` setting — bump to
     allow more parallel work.
   - Check S3 throttling (`lakehouse_s3_throttled_total`).
5. If `lakehouse_compaction_double_count_window_seconds` is nonzero
   for many partitions: a previous compaction left L0+L1 mixed; queries
   are over-counting. Manually trigger a re-compaction by lowering
   `min_files_l0` temporarily, or wait for Tier B to clean.

#### 6.4.2 "Two pods both compacting same partition" diagnostic flow

This should not happen in steady state. If
`lakehouse_compaction_dual_ownership_total` increments:

1. Check `lakehouse_compaction_ring_changes_total` — recent
   join/leave? Then it's stabilization-window lag; should self-correct
   within `stabilizeDuration` (60 s).
2. If no ring changes but dual ownership keeps incrementing:
   - Mismatch in peer list between pods. Run:
     ```bash
     for pod in $(kubectl get pods -l app=lakehouse-logs -o name); do
       kubectl exec "$pod" -- curl -s localhost:9428/internal/cache/stats
     done
     ```
     Each pod's `members` count must match.
   - Mismatch in `Self` identity. Run:
     ```bash
     kubectl describe pod | grep -E "IP:|httpListenAddr"
     ```
     The pod's IP must match what discovery returns.
3. Worst case: Tier B is cleaning up the duplicate. Check
   `lakehouse_compaction_orphan_files_deleted_total` — non-zero means
   the duplicate is being garbage-collected; check S3 storage cost
   for the partition.

---

## 7. Deletion checklist (PR B + PR C)

PR B (chart + flags + RBAC):

- [ ] `charts/victoria-lakehouse/templates/compaction-rbac.yaml` removed
- [ ] `charts/victoria-lakehouse/templates/tenant-rbac.yaml` removed
- [ ] `charts/victoria-lakehouse/values.yaml`: keys removed:
  - [ ] `lakehouseConfig.compaction.leader_election`
  - [ ] `lakehouseConfig.compaction.lease_duration`
  - [ ] `lakehouseConfig.compaction.s3_lock_ttl`
  - [ ] `lakehouseConfig.compaction.s3_heartbeat`
  - [ ] `lakehouseConfig.compaction.shard_id`
  - [ ] `lakehouseConfig.compaction.shard_count`
  - [ ] `lakehouseConfig.tenant.aliases.leader_election` *(if no other reader; see Q6)*
  - [ ] `lakehouseConfig.tenant.aliases.lease_duration` *(if no other reader; see Q6)*
- [ ] CLI flags removed:
  - [ ] `-lakehouse.compaction.leader-election`
  - [ ] `-lakehouse.compaction.shard-id`
  - [ ] `-lakehouse.compaction.shard-count`
- [ ] `config.CompactionConfig` fields removed:
  - [ ] `LeaderElection`, `LeaseDuration`, `S3LockTTL`, `S3Heartbeat`,
    `ShardID`, `ShardCount`
- [ ] Config defaults updated (`config.go:629` block)
- [ ] Config validator updated (`config.go:939-943`, `:1017-1019`)
- [ ] Config overlay merge updated (`config.go:1619-1631`)

PR C (delete election package + tests):

- [ ] `internal/election/` directory removed
- [ ] `internal/compaction/sentinel.go` removed
- [ ] `internal/compaction/sentinel_test.go` removed
- [ ] `internal/compaction/sharding.go` removed *(replaced by ownership.go in PR A)*
- [ ] `internal/compaction/sharding_test.go` removed
- [ ] `internal/bloomindex/controller.go`: `SetLeader`, `IsLeader`,
      `isLeader` field removed *(deleted in PR A commit 5 — confirm
      no straggler tests remain)*
- [ ] `cmd/lakehouse-logs/main.go`: `election` import removed
- [ ] `lakehouse-traces/main.go`: `election` import removed
- [ ] `internal/metrics/lakehouse.go`: `ElectionLeader`,
      `ElectionTransitionsTotal`, `ElectionHealthChecksTotal` removed
- [ ] `internal/metrics/verify_test.go`: 3 election-metric entries removed
- [ ] `.github/workflows/e2e-k8s.yaml` removed
- [ ] `tests/e2e-k8s/` directory removed
- [ ] `tests/verification/probe_k8s_election_failover.sh` removed
- [ ] `tests/verification/matrix.md` L11 narrative updated (binary
      bloat re-baselined; "kind e2e" block removed)
- [ ] `go.mod`: `k8s.io/apimachinery v0.36.0` removed from `require`
- [ ] `go.mod`: `k8s.io/client-go v0.36.0` removed from `require`
- [ ] `go.sum`: stale entries cleaned via `GOWORK=off go mod tidy`
- [ ] CI workflows that reference `internal/election/**` in path
      filters updated (likely only `e2e-k8s.yaml` which is also deleted)

Verification at end of PR C:

- [ ] `GOWORK=off go test -race -count=1 ./...` passes
- [ ] `GOWORK=off go vet ./...` passes
- [ ] `make build-logs build-traces` succeeds
- [ ] Binary size: `ls -lh bin/lakehouse-logs bin/lakehouse-traces` ≤ 32 MB
      (down from 37 MB pre-deletion; the 5 MB drop is the K8s client-go REST
      closure + tools/leaderelection)
- [ ] `helm lint charts/victoria-lakehouse` passes
- [ ] No grep hits for `coordination.k8s.io`, `LeaseDuration`,
      `S3LockTTL`, `leader_election`, `IsLeader`, `NewAutoElector`,
      `Sentinel`, `PartitionSharding`

---

## 8. Risk register

### 8.1 Top 3 risks of the new design

| Risk | Severity | Mitigation |
|---|---|---|
| **R1 — Discovery returns wrong `Self` identity** | HIGH | The pod's HRW weight depends on its `Self` string matching what peers see in discovery. If `httpListenAddr` differs from the IP K8s broadcasts (e.g. `0.0.0.0:9428` instead of pod IP), every partition will rank the "wrong" self peer first and the pod silently never compacts. **Mitigation:** the `compaction_ownership_self_in_peers` gauge alerts at 0; CI integration test `TestIntegration_SelfIdentity_PresentInPeerList` covers this. Documented in runbook (§6.4.1 step 3). |
| **R2 — Tier B prefix-hash skew makes one pod do all the LIST work** | MEDIUM | We hash *prefixes* (date-level) for Tier B ownership, not partitions (hour-level). With ~365 prefixes per signal across N pods, distribution skew is bounded but could be ±20 % at small N. **Mitigation:** if N pods have visible Tier B imbalance, switch to per-partition (hour) hashing — same code path, just change the key passed to the hash. Tracked as open question Q1. |
| **R3 — Idempotency guard on `AddFile` masks a real bug elsewhere** | MEDIUM | Currently `AddFile` silently appends; we make it skip on duplicate key. If a *different* compaction produces two files with the same UUID-suffixed key (vanishingly unlikely — 2^32 collision space), the second `AddFile` silently no-ops and the file becomes a Tier-B orphan. **Mitigation:** `AddFile` increments `manifest_addfile_duplicate_key_total` on the skip path; alert if > 0. Counter doubles as a canary for hidden upload bugs. |

### 8.2 Top 3 risks NOT addressed (deferred to follow-up PRs)

| Risk | Why deferred | Follow-up PR name |
|---|---|---|
| **D1 — Cross-AZ S3 read amplification when compaction owner is in different AZ from the originating insert pod** | The AZ-aware peercache (`peercache.SetWithZones`) already exists for select-side caching but not for compaction ownership. Adding it doubles the complexity of HRW (preferred-AZ filter, fallback path) and is orthogonal to election removal. | `feat(compaction): AZ-aware HRW ownership` |
| **D2 — Per-tenant compaction quotas** | No quotas exist today; this is net-new functionality, not a regression from PR A. Best added as a follow-up so PR A stays scoped. | `feat(compaction): per-tenant quotas` |
| **D3 — Restart survival of `partitionAttempts` map** | We deliberately keep this in-memory (§2.2.3). On a full cluster restart, Tier A wakes up "blind" for `3 × Interval`. Edge case 8 confirms this is correct (HRW recovers, no orphans). If a future maintainer wants stronger guarantees, persistence is a 5-line addition. | `feat(compaction): persist partition-attempts to disk` |

---

## 9. Rollback plan

If PR A turns out to be wrong in production, rollback is **simple**
because:

- No data format change. Manifest layout, S3 key shape, Parquet
  schema all unchanged.
- No external state. K8s Lease object can be safely left over from
  the old design (it's just an unused CRD) until the next chart
  rollback.

### 9.1 Rollback via Helm

```bash
helm rollback lakehouse <previous-revision>
```

This re-installs the previous chart (election RBAC + values), but
the **binary** rolled out by the previous revision still has the
election code in it. After rollback:

1. Pods come up with the previous binary.
2. Previous binary tries to acquire the K8s Lease — succeeds (Lease
   was never used in PR A).
3. Single leader resumes compaction the old way.
4. The HRW-era `partitionAttempts` map is in-memory only and is lost
   on restart — no leftover state in S3.

### 9.2 What data is at risk

- **None.** The only durable state PR A writes is the manifest
  (unchanged shape) and compacted Parquet files (unchanged shape).
  Rollback discards the in-memory `partitionAttempts` map but loses
  nothing important.

### 9.3 "Bad signal to roll back" looks like

- `lakehouse_compaction_dual_ownership_total > 0` *persistently* over
  hours, despite no ring churn.
- `lakehouse_compaction_orphan_files_deleted_total` ramps to > 1000/hour
  (suggests sustained duplicate work).
- Storage costs (per `lakehouse_storage_bytes_total`) grow 2-3 × the
  expected rate (duplicate compacted files not being cleaned fast
  enough).
- Query result correctness errors (compared against VL/VT parity
  baseline) — would be the most serious signal.

If any two of the above for > 24 h: rollback.

---

## 10. Open questions for the maintainer

Each question should be answerable with a concrete decision before the
implementation agent starts coding PR A.

**Q1. Tier B prefix granularity.**
The spec hashes *date* prefixes (`{tenant}/{signal}/dt=YYYY-MM-DD/`)
to assign Tier B ownership across pods. This is cheap (~365 LIST
calls per pod per signal per day) but can skew if a single date has
much more data than others. Alternative: hash *hour* prefixes
(`dt=.../hour=00/` … `hour=23/`) — better distribution, 24× the LIST
calls. **Recommend date-level for PR A; revisit if Tier B becomes a
hot path.** Confirm?

**Q2. Tier A cadence.**
The spec defaults Tier A to run every `Interval` (so on every
scheduler tick, after the normal scan). The staleness threshold is
`3 × Interval`. Alternative: run Tier A every `N × Interval` with
N = 3 (so once every 3 normal scans). The "every tick" cadence is
more responsive at the cost of 3× the AttemptsView reads (cheap —
in-memory). **Recommend "every tick" with `3 × Interval` staleness
threshold.** Confirm?

**Q3. Cross-AZ ownership.**
Deferred (R1 in §8.2). For PR A, all peers are equal in HRW. In a
multi-AZ deployment, this could double or triple S3 cross-AZ traffic.
Acceptable to ship PR A without and add AZ-awareness in a follow-up?

**Q4. Per-tenant compaction quotas.**
Not part of PR A (D2 in §8.2). Ship without?

**Q5. S3 bucket lifecycle for incomplete multipart uploads.**
The chart should set `AbortIncompleteMultipartUpload: 1 day` on the
bucket lifecycle policy to catch edge case 14. This is a chart
addition that fits PR B. Confirm we should add it?

**Q6. Tenant-alias leader election (separate from compaction).**
The chart at `values.yaml:441` has `tenant.aliases.leader_election:
auto` — a *separate* leader for the tenant-alias sync writer
(separate from the compaction leader). The maintainer's brief says
"drop chart RBAC for `coordination.k8s.io/leases`" which would
strand the alias-sync RBAC binding. Should the tenant-alias sync also
move to HRW ownership in PR A, or is it out of scope and tracked as
PR D? **Recommend: in scope for PR B's chart cleanup, but the
alias-sync code change is PR D.** Confirm?

**Q7. Persist `partitionAttempts` to disk?**
Currently in-memory only (§2.2.3). Persisting via the existing
`manifest.SaveTo`/`LoadFrom` machinery would survive pod restarts
and avoid the 3×Interval blind window after a full cluster restart.
Trade-off: extra ~16 bytes per partition in the snapshot, no S3 cost.
Worth it for PR A or defer to follow-up?

**Q8. HRW hash function commitment.**
Spec recommends xxhash (xxh64) over CRC32 for better distribution.
This means linking `github.com/cespare/xxhash/v2` into ownership.go,
even though it's already in the binary transitively. Acceptable? Or
should we use the existing CRC32 path for "no new dep" purity?

**Q9. Stabilization duration.**
Default `peercache.DefaultStabilizeDuration = 60 s`. During
stabilization, *all compaction defers*. In a constantly-flapping
cluster (sub-60 s ring changes), compaction would never run. This is
correct (we don't want to make data-mutating decisions on a flapping
ring), but the operator should be alerted. Recommended alert:
`rate(lakehouse_compaction_deferred_stabilizing_total[15m]) > 0.1`.
Confirm the threshold?

**Q10. Sentinel prefix protected forever?**
Tier B's `NeverDeletePrefixes` includes `_compaction_lock` so the
*old* sentinel files (left behind by the previous design) are never
deleted. They will eventually be cleaned by S3 lifecycle (or by a
one-off migration script). Should PR A include a one-time cleanup
job that deletes all `_compaction_lock` files at startup, or leave
them to bucket lifecycle?

**Q11. Scheduler tick interval.**
The spec keeps the existing default `Interval = 5m`
(`config.go:629`). With HRW, a partition that misses its tick has to
wait `3 × 5 = 15 m` before Tier A steals. Acceptable, or should
the Interval be lowered (1 m) so staleness recovery is faster?
Lower interval = more S3 work even when there's nothing to do.

**Q12. Orphan TTL.**
Default `OrphanTTL = 2h`. Long enough to cover S3 EC + most pod
restart scenarios, short enough to avoid storage cost balloons from
duplicate work. Confirm 2h is right, or should it be 1h / 4h?

---

## Appendix A — Verification matrix entries

Add to `tests/verification/matrix.md` (after L11):

```
L13 — Election-free compaction (PR #__)

Component:    internal/compaction/ownership.go, orphan_sweep.go
Spec ref:     docs/superpowers/specs/2026-05-31-election-free-compaction.md
VL/VT ref:    spec (LH-only surface)
Last state:   PASS (target post-merge)
Verified:     YYYY-MM-DD
Probes:       tests/verification/probe_compaction_ownership.sh
              tests/verification/probe_orphan_sweep.sh
              tests/verification/run_negative_controls.sh

Acceptance:
- Sum of lakehouse_compaction_partitions_owned across pods
  equals lakehouse_storage_partitions_total within ±1 partition
  (settling delay)
- lakehouse_compaction_dual_ownership_total stays at 0 across
  a 30-minute soak run
- lakehouse_compaction_orphan_files_deleted_total stays under
  10/hour in steady state
- Coverage of internal/compaction/ownership.go ≥ 95 %
- Coverage of internal/compaction/orphan_sweep.go ≥ 90 %
- All negative controls FAIL when respective fix is reverted

Failure mode:
- If self-discovery breaks: lakehouse_compaction_ownership_self_in_peers
  goes to 0. Page; check runbook §6.4.1 step 3.
- If dual ownership detected: page; check runbook §6.4.2.
```

---

## Appendix B — Per-component verification matrix delta

Per `feedback_per_component_verification`, every new API/UI/datasource
surface needs its own row. PR A adds **no new HTTP surfaces** — the
ownership resolver is internal-only, the orphan sweep is internal-only.
The only new "surface" is the **metrics endpoint** which exposes the
6 new metric families; this is covered by the existing
`tests/verification/probe_metrics_exposed.sh` (verify it doesn't
need an update — the probe should auto-discover new metric names).

---

## Appendix C — Why not `internal/peercache.Ring.Lookup` directly?

The peercache's `Ring.Lookup(key)` already returns "which peer owns
this key" via consistent hashing
(`internal/peercache/ring.go:56-72`). Why introduce a new HRW
implementation?

**Reasons:**

1. **Different lookup needs.** `Ring.Lookup` returns the *single*
   owning peer (for cache routing). HRW gives us *ordered* peers
   (primary, secondary, tertiary…), which the orphan sweep needs.
2. **Algorithmic difference.** Consistent hashing has worse
   distribution than HRW at low peer counts (N=3 typical for our
   clusters) — vnodes mitigate but don't eliminate. HRW is exact.
3. **Test surface.** HRW is a pure function of `(peer, partition)` —
   easy to unit-test, fuzz, and reason about. `Ring.Lookup` has
   internal state (vnode positions in a sorted slice) that's harder
   to verify in isolation.
4. **Single-pod degenerate case.** HRW with one peer is trivially
   "I own everything" — no special-casing needed. `Ring.Lookup`
   already handles this via the `len(r.keys) == 0` check, so this is
   a minor advantage.

We **do** share the underlying `Members()` source of truth via the
peercache — no second discovery path, no second stabilization window.

---

## Appendix D — Code citations (verified-on-disk)

Every code reference in this spec was verified against `origin/main`
as of commit `58de6dd`. Key references:

| Citation | File | Lines | Verified |
|---|---|---|---|
| Existing scheduler with leader gate | `internal/compaction/scheduler.go` | 120-127 | ✓ |
| Sentinel TOCTOU race | `internal/compaction/sentinel.go` | 34-48 | ✓ |
| Compactor UUID-suffixed output | `internal/compaction/compactor.go` | 165-166 | ✓ |
| Compactor delete-failure swallow | `internal/compaction/compactor.go` | 188-191 | ✓ |
| `peercache.OnRingChange` / `IsStabilizing` | `internal/peercache/peercache.go` | 174-181 | ✓ |
| `peercache.DefaultStabilizeDuration` | `internal/peercache/peercache.go` | 30 | ✓ |
| `Ring.Lookup` consistent-hash impl | `internal/peercache/ring.go` | 56-72 | ✓ |
| `discovery.DiscoverPeers` headless-service flow | `internal/discovery/discovery.go` | 115-132 | ✓ |
| `discovery.resolveHeadlessService` SRV+host fallback | `internal/discovery/discovery.go` | 134-165 | ✓ |
| Existing CRC32 sharding (to be replaced) | `internal/compaction/sharding.go` | 46-54 | ✓ |
| Election auto-elector decision logic | `internal/election/auto.go` | 37-91 | ✓ |
| K8s elector hand-rolled REST client | `internal/election/k8s.go` | 1-46 | ✓ |
| Bloomindex dead `SetLeader`/`IsLeader` | `internal/bloomindex/controller.go` | 81-93 | ✓ |
| Manifest `AddFile` (no idempotency guard today) | `internal/manifest/manifest.go` | 517-553 | ✓ |
| Manifest `RemoveFile` (already idempotent) | `internal/manifest/manifest.go` | 459-476 | ✓ |
| Manifest mutex pattern | `internal/manifest/manifest.go` | 77 | ✓ |
| Compaction-RBAC chart template | `charts/victoria-lakehouse/templates/compaction-rbac.yaml` | 1-57 | ✓ |
| Tenant-RBAC chart template | `charts/victoria-lakehouse/templates/tenant-rbac.yaml` | 1-37 | ✓ |
| Helm `values.yaml` compaction block | `charts/victoria-lakehouse/values.yaml` | 446-474 | ✓ |
| `CompactionConfig` struct | `internal/config/config.go` | 406-420 | ✓ |
| `CompactionConfig` defaults | `internal/config/config.go` | 629-642 | ✓ |
| `CompactionConfig` validators | `internal/config/config.go` | 939-943, 1017-1019 | ✓ |
| `CompactionConfig` overlay merge | `internal/config/config.go` | 1619-1631 | ✓ |
| `cmd/lakehouse-logs/main.go` election wiring | `cmd/lakehouse-logs/main.go` | 250-267 | ✓ |
| `lakehouse-traces/main.go` election wiring | `lakehouse-traces/main.go` | 245-262 | ✓ |
| `s3PoolAdapter.List` (already exists, reusable for Tier B) | `cmd/lakehouse-logs/main.go` | 1168-1188 | ✓ |
| Storage's `RefreshDiscovery` peer-update path | `internal/storage/parquets3/storage.go` | 946-984 | ✓ |
| Storage wiring of `peercache.New(selfAddr)` | `internal/storage/parquets3/storage.go` | 137 | ✓ |
| `tests/verification/probe_k8s_election_failover.sh` (to be deleted) | `tests/verification/probe_k8s_election_failover.sh` | 1-51 | ✓ |
| `tests/e2e-k8s/test_leader_election.sh` (to be deleted) | `tests/e2e-k8s/test_leader_election.sh` | (full file) | ✓ |
| `tests/verification/matrix.md` L11 narrative | `tests/verification/matrix.md` | 217-292 | ✓ |
| Existing election-metrics declared but never set | `internal/metrics/lakehouse.go` | 213-218 | ✓ |
| `go.mod` k8s dependencies to remove | `go.mod` | 24-25 | ✓ |
| `internal/compaction/integration_test.go` (single-pod path) | `internal/compaction/integration_test.go` | 1-153 | ✓ |

---

## Appendix E — Estimated implementation effort

For the follow-up implementation agent:

| Phase | Effort | Notes |
|---|---|---|
| Read this spec + research report end-to-end | 30 min | Required before coding |
| Commit 1 (add new files, no behaviour change) | 3 h | Most of the LoC; mostly tests |
| Commit 2 (rewire scheduler) | 1 h | Mechanical |
| Commit 3 (wire main.go × 2) | 1 h | Mirror lines exactly between logs+traces |
| Commit 4 (drop sentinel/sharding) | 30 min | Mechanical |
| Commit 5 (drop bloomindex leader) | 20 min | 15 lines of deletes |
| Commit 6 (docs) | 1 h | README + matrix + architecture.md |
| Run full test suite locally + fix anything | 2 h | Expect 1-2 misses on existing tests |
| Run docker-compose e2e probes | 1 h | First-run flakiness |
| Run negative controls + verify all fail-then-pass | 1 h | Per harden-and-lock |
| Verification matrix + open the PR | 30 min | |
| **Total PR A** | **~12 h** | Single focused day |
| PR B (chart + flags) | ~4 h | Mostly review & test pass |
| PR C (delete election package) | ~3 h | `go mod tidy` + CI green |
| **Grand total** | **~19 h** | Spread over 3-5 days |

---

*End of spec.*
