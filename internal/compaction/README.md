# internal/compaction — election-free background Parquet merging

This package merges small Parquet files into larger ones, drives per-tenant
fair-share scheduling, and reclaims orphan files left behind by crashed pods.
It is the second-largest hot-path package in Lakehouse after `parquets3` and
runs continuously on every pod (no leader election — see §1).

## 1. Architecture overview

```
                       ┌──────────────────────────────┐
                       │  every pod runs this stack   │
                       │  (no K8s Lease, no S3 lock)  │
                       └──────────────────────────────┘
                                     │
   ┌──────────────────────┬──────────┴──────────┬──────────────────────┐
   ▼                      ▼                     ▼                      ▼
┌──────────────────┐ ┌───────────────┐ ┌───────────────────┐ ┌────────────────┐
│ OwnershipResolver│ │   Scheduler   │ │   OrphanSweep     │ │  DrainHandler  │
│  (ownership.go)  │ │ (scheduler.go)│ │ (orphan_sweep.go) │ │(drain_handler) │
│                  │ │               │ │                   │ │                │
│ HRW ranking over │ │ Tick-driven   │ │  Tier A: stale    │ │ POST /lakehouse│
│ peer cache; AZ-  │ │ Scan() loop;  │ │  partitions       │ │ /drain →       │
│ stratified,      │ │ fair-share +  │ │  Tier B: S3 prefix│ │ Scheduler.Drain│
│ draining filter, │ │ ownership +   │ │  hash + 3-step    │ │  (waits for in-│
│ stabilize gate.  │ │ ring-thrash   │ │  safety gate.     │ │  flight, emits │
│                  │ │ rate limit.   │ │                   │ │  X-Lakehouse-  │
│                  │ │               │ │                   │ │  Draining hdr).│
└────────┬─────────┘ └───────┬───────┘ └─────────┬─────────┘ └───────┬────────┘
         │                   │                   │                   │
         ▼                   ▼                   ▼                   ▼
  peercache.Members()  manifest.AllFiles    S3 (Pool + Lister)   K8s preStop
                       + AddFile (idempo)                         hook
```

Every pod independently decides which partitions it owns via
`OwnershipResolver.OwnsPartition(partition)`. The owner runs the merge.
Non-owners idle. There is no shared coordination state.

## 2. State machine

Per-pod, per-partition state lives in `manifest.Manifest`:

```
                     ┌─────────────┐
                     │ NOT OWNED   │   (HRW picks another peer)
                     └──────┬──────┘
                            │
              ring change   │   HRW returns true
                ▼           │
              ┌─────────────┘
              │
              ▼
       ┌───────────────┐ MarkAttempt(now)   ┌──────────────────────┐
       │  OWNED + idle │ ────────────────▶ │ OWNED + in-flight    │
       └───────┬───────┘                    │ (Compactor.Compact)  │
               │                            └──────────┬───────────┘
               │ Drain()                               │
               ▼                                       ▼
       ┌───────────────┐                      ┌────────────────────┐
       │   DRAINING    │   compactor done →   │  OWNED + idle      │
       │ (no new work) │   bump LastCompact   │  AddFile(merged)   │
       └───────┬───────┘                      └────────────────────┘
               │ inFlight==0 OR
               │ DrainTimeout elapsed
               ▼
       ┌───────────────┐
       │   DRAINED     │   → process exits gracefully
       └───────────────┘
```

Tier A intercedes when a primary owner is too slow:

```
   pod-X is primary owner; last_attempt > 3 * Interval ago
                            │
                            ▼
   pod-Y is secondary HRW (ranked[1]); Tier A runs locally on pod-Y
   ─────────────────────────────────────────────────────────────────
   pod-Y verifies (a) NOT stabilizing, (b) eligible, (c) ranked[1] == self
   → pod-Y marks attempt + runs compactor + emits OnSteal callback
```

Tier B operates on the storage layer:

```
   Lister.List(prefix) → group by date prefix → hash dates to one pod
                            │
                            ▼
   each pod walks its assigned dates:
     for each .parquet key NOT in manifest:
       if isProtected(key)         → skip (never delete _meta/, _tombstones/)
       if age < OrphanTTL          → skip
       if keyInManifestAt(prefix)  → skip (race: peer just published it)
       else                        → Pool.Delete(key)
```

## 3. Public API

The package exports four primary types. All other types are unexported.

### 3.1 `OwnershipResolver`

```go
func NewOwnershipResolver(self string, peers func() []string) *OwnershipResolver
func (r *OwnershipResolver) WithHashFunc(h func(string) uint64) *OwnershipResolver

// Configurable callbacks (assign after construction):
r.Stabilizing = func() bool { ... } // optional
r.IsDraining = func(peer string) bool { ... } // optional
r.SameAZPeers = func() []string { ... } // optional; nil = all peers

// Core decisions:
func (r *OwnershipResolver) OwnsPartition(partition string) bool
func (r *OwnershipResolver) OwnerOf(partition string) string
func (r *OwnershipResolver) RankedOwners(partition string) []string
func (r *OwnershipResolver) SecondaryOwner(partition string) string
func (r *OwnershipResolver) TertiaryOwner(partition string) string

// Observability:
func (r *OwnershipResolver) IsStabilizing() bool
func (r *OwnershipResolver) SelfInPeersGauge() int64
```

`OwnsPartition` returns `false` while the ring is stabilizing (peer set
churn within the configurable cooldown window). HRW weight is computed
with xxh64 across a `peer + 0x1F + partition` digest (unit-separator
byte cannot appear in either input → no engineerable collision). The
optional `WithHashFunc` swaps in an alternate hasher for testing.

### 3.2 `OrphanSweep`

```go
func NewOrphanSweep(cfg OrphanSweepConfig) *OrphanSweep
func (o *OrphanSweep) Start()
func (o *OrphanSweep) Stop()
func (o *OrphanSweep) RunTierA(ctx context.Context) (stolen int, err error)
func (o *OrphanSweep) RunTierB(ctx context.Context) (deleted int, err error)
```

`Start` launches two tickers (Tier A on `Interval`, Tier B on
`TierBInterval`). `Stop` signals both loops to exit and blocks until
the goroutines terminate. Both `RunTierA` and `RunTierB` are
short-circuited while `Ownership.IsStabilizing()` returns true.

### 3.3 `FairShareScheduler`

```go
func NewFairShareScheduler(compactionsPerTenant int) *FairShareScheduler
func (f *FairShareScheduler) CompactionsPerTenant() int
func (f *FairShareScheduler) PickCandidates(
    candidates []partitionCandidate,
    maxConcurrent int,
) []partitionCandidate
```

Groups candidates by tenant (prefix `<acct>/<proj>/...`), rotates a
persistent cursor across tenants, and emits up to `compactionsPerTenant`
per tenant per call. Single-tenant deployments degenerate to FIFO. The
cursor advances on every call so over N ticks every tenant gets equal
slot opportunities.

### 3.4 `Scheduler` + `DrainHandler`

```go
func NewScheduler(cfg SchedulerConfig) *Scheduler
func (s *Scheduler) Start()
func (s *Scheduler) Stop()
func (s *Scheduler) Drain()
func (s *Scheduler) IsDraining() bool
func (s *Scheduler) Scan(ctx context.Context) (int, error)

func DrainHandler(s *Scheduler) http.HandlerFunc
```

`SchedulerConfig.Ownership` is required (panics if nil). `FairShare`,
`OnRingChange`, `BloomRebuilder`, and `OnCompacted` are optional.
`Drain()` is idempotent and safe to call from a signal handler — it
flips `draining=true`, blocks until `inFlight==0` (or `DrainTimeout`
elapses), and emits the
`lakehouse_compaction_aborted_during_drain_total` counter on timeout.

## 4. Failure modes

### 4.1 Empty peer list

`Peers()` returns `[]` (peer-cache initial state or transient discovery
failure). `OwnsPartition` returns `false` unconditionally — no pod
claims anything. The next peer-cache refresh restores the list.

**Diagnostic:** `lakehouse_compaction_ownership_self_in_peers` gauge
goes to 0 cluster-wide; `lakehouse_compaction_partitions_owned` sums to
0 across all pods.

### 4.2 Self not in peer list

A common operator misconfiguration: pod-A's `Self` string (the value it
hashes against in HRW) does not match the address pod-B sees in
discovery. pod-A computes HRW against itself but the result is never
the "highest weight" peer because pod-A's actual identity (as seen by
others) doesn't exist in its own peer list.

**Diagnostic:** `lakehouse_compaction_ownership_self_in_peers` gauge
reads 0 *for this specific pod* while other pods read 1. The
`compaction_self_in_peers` startup check logs a `WARN` at boot if
detected. See spec §8.1 R1.

### 4.3 Ring flap / stabilization

Peer-cache observes a peer add or remove. The OwnershipResolver enters
"stabilizing" state for the cooldown window. All ownership decisions
return false; scheduler defers; sweepers defer. After the window
closes, HRW redistributes — ownership of ~1/N partitions changes (HRW's
minimal-disruption property).

**Diagnostic:** `lakehouse_compaction_deferred_stabilizing` and
`lakehouse_compaction_sweep_deferred_stabilizing` counters tick.

### 4.4 Dual ownership window

Two pods can briefly think they each own a partition during a ring
change (one pod's peer-cache hasn't refreshed yet). The `Manifest.AddFile`
idempotency guard catches the duplicate upload — the second upload's
`AddFile` no-ops and increments
`lakehouse_manifest_addfile_duplicate_key_total`. Both compacted output
files exist on S3; Tier B reclaims the duplicate after `OrphanTTL`.

**Diagnostic:** `lakehouse_compaction_dual_ownership_total` is the
gold-standard alert (target 0 sustained). The duplicate-key counter is a
fall-through canary.

### 4.5 Graceful pod termination (HPA scale-down)

Operator-driven termination triggers the `preStop` hook → `POST /lakehouse/drain`.
The pod sets `draining=true`, advertises `X-Lakehouse-Draining: true` so peers
exclude it from HRW. The scheduler completes its current partition (if any),
refuses new ones, and exits cleanly within `terminationGracePeriodSeconds`.

**Diagnostic:** `lakehouse_compaction_draining` gauge transitions
0→1→0; `lakehouse_compaction_aborted_during_drain_total` stays at 0 if
the drain completed within `DrainTimeout`.

### 4.6 Hard pod death (SIGKILL)

No preStop, no drain. The pod dies mid-compaction. The partial parquet
upload is on S3 with no manifest entry. Tier B reclaims it after
`OrphanTTL` (default 1 h) — see edge case 14 in the spec.

**Diagnostic:** `lakehouse_compaction_orphan_files_deleted_total` ticks
on the next Tier B sweep that owns the date prefix.

## 5. Spec cross-reference

| Code surface | Spec section |
|---|---|
| `ownership.go` HRW + AZ stratification | §2.1, §12.1 |
| `ownership.go` stabilization gate | §3.1 cases 3 + 22 |
| `orphan_sweep.go` Tier A (staleness) | §2.4.1 |
| `orphan_sweep.go` Tier B (S3 prefix hash) | §2.4.2, §3.7 |
| `fair_share.go` | §12.2 |
| `scheduler.go` ring-thrash rate gate | §11.4 |
| `scheduler.go` Drain() | §11.1 |
| `manifest.AddFile` idempotency | §3.6 edge case 6 |
| `manifest.MarkAttempt` + AttemptsView | §2.4.1 |

See `docs/superpowers/specs/2026-05-31-election-free-compaction.md` for
the full design rationale, decision log, and edge-case enumeration.
