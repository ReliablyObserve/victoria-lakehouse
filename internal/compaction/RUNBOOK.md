# internal/compaction — operator runbook

Diagnostic flows for election-free compaction. Symptom → check → fix.

If you're new to the package, read `README.md` first.

---

## 1. "Compaction stopped — files are accumulating"

**Symptom:** S3 file count under a tenant prefix is growing without
bound; query latency degrades; alerting fires on
`lakehouse_storage_files_total` rate-of-change.

### Checks (run in order)

1. **Is the pod that owns these partitions actually compacting?**
   ```promql
   sum(lakehouse_compaction_partitions_owned) by (pod)
   ```
   Pods returning 0 are not doing any compaction. If only ONE pod is
   doing work in a multi-pod cluster, jump to §1.1.

2. **Are the owner's runs erroring?**
   ```promql
   rate(lakehouse_compaction_errors_total[5m])
   ```
   If > 0, check the pod logs for `compaction failed` lines.

3. **Is the ring stuck in stabilization?**
   ```promql
   rate(lakehouse_compaction_deferred_stabilizing[5m])
   ```
   If > 0 sustained for > 5 minutes, the peer-cache is flapping —
   investigate K8s endpoint controller or DNS. See §3.

4. **Is the ring-thrash rate limit firing?**
   ```promql
   rate(lakehouse_compaction_deferred_ring_thrash[5m])
   ```
   If > 0, HPA is scaling too aggressively (>6 ring changes in 5 min).
   Tune `behavior.scaleUp.stabilizationWindowSeconds` upward.

### Fixes

- **Owner stuck:** `kubectl rollout restart statefulset/<lakehouse>`.
  HRW redistributes within one tick after the pod comes back.
- **All pods say "not owner" for the affected partition:** see §2 —
  the partition is unowned because Self is not in any peer list. Check
  `lakehouse_compaction_ownership_self_in_peers` per pod.

### 1.1 "Only one pod is doing all the work"

In a multi-pod cluster, this is symptomatic of one of:

- **Mis-set `Self` strings.** Other pods may be present in the peer
  list but with addresses that don't match what they themselves
  publish. Confirm: on each pod, `curl localhost:9428/-/peers` and
  compare with what the suspect pod sees. The Self values must match
  what peers see.
- **`IsDraining` callback misconfigured.** If every peer is reported
  as `draining=true` to the resolver, only `self` survives the filter.
  Check `lakehouse_compaction_draining` per pod — if it's stuck at 1
  on a non-terminating pod, the drain handler was misfired.
- **AZ stratification artifact.** In a 1-pod-per-AZ deployment with AZ
  stratification on, each pod's same-AZ peer list is `{self}` — all
  partitions land on `self`. This is correct behaviour (no other
  same-AZ peers to load-balance with), not a bug.

---

## 2. "Pod claims `self_in_peers` is 0"

**Symptom:** `lakehouse_compaction_ownership_self_in_peers` reads 0
for a specific pod (and that pod has no work — see §1).

### Root cause

The HRW computation requires the pod's own identity string (`Self`)
to be present in the peer list returned by `Peers()`. If discovery
publishes a different address (e.g. pod IP vs hostname), HRW always
picks a "wrong-self" peer and this pod compacts nothing.

### Checks

1. **What is `Self`?** Look in pod startup logs:
   ```
   compaction: HRW ownership initialized; self=10.0.5.3:9428 peers=...
   ```
2. **What address do peers publish for this pod?** From a different
   pod:
   ```
   curl localhost:9428/-/peers
   ```
   The address shown for THIS pod must match `Self` exactly (including
   port).

### Fixes

- **`Self` shows `0.0.0.0:9428`:** the embedder is sending the listen
  address instead of the pod IP. Fix by injecting `POD_IP` via downward
  API and concatenating with the port at startup. See spec §8.1 R1
  mitigation.
- **`Self` shows hostname but peers publish IP:** standardise on one
  form across discovery and main.go. The convention in Lakehouse is
  IP:port (peer-cache stores IPs).
- **Self correct, but peers don't list it:** peer-cache hasn't refreshed
  yet (give it 10 s) or the K8s endpoints controller is broken (check
  `kubectl get endpoints`).

---

## 3. "Ring is flapping — stabilization gauge stays high"

**Symptom:** `rate(lakehouse_compaction_deferred_stabilizing[5m]) > 0`
for hours; `lakehouse_compaction_deferred_ring_thrash` is non-zero.

### Root causes (most common first)

1. **HPA scaling too aggressively.** Each scale up/down adds a peer
   change → enters stabilization window. Default rate limit triggers
   at >6 changes / 5 min.
2. **Pod readiness flapping.** A pod fails its readiness probe
   repeatedly → leaves and re-joins the endpoints list. Check
   `kubectl get pods -w` for crashloops.
3. **DNS / endpoint controller lag.** Newly created pods take >30 s to
   appear in the endpoints list. peer-cache misses them. Symptom: the
   ring "settles" with N-1 pods then suddenly jumps to N.

### Fixes

- **HPA tuning:** raise `behavior.scaleUp.stabilizationWindowSeconds`
  to >= 300 s. Raise `scaleDown.stabilizationWindowSeconds` to >= 600 s.
- **Pod readiness:** investigate the failing readiness probe (often the
  bloom-controller warmup or manifest snapshot replay).
- **DNS lag:** confirm CoreDNS is healthy; check
  `coredns_dns_request_duration_seconds`.

### Mitigation

While the root cause is being fixed: increase the
`RingChangeRateLimit` threshold to e.g. 12 to tolerate higher churn.
This is a config-only knob — no code change. Note: raising the limit
trades stabilization-window safety for higher dual-ownership risk.

---

## 4. "Dual ownership counter is ticking"

**Symptom:** `lakehouse_compaction_dual_ownership_total > 0`. This
indicates two pods believed they owned the same partition during a
brief window.

### Severity

- **Transient burst** (a few ticks during a ring change): EXPECTED
  during the stabilization window cooldown. `AddFile` idempotency
  ensures the manifest is correct; Tier B will reclaim the duplicate
  output within `OrphanTTL`.
- **Sustained > 24 h:** SERIOUS. Indicates either the stabilization
  gate isn't firing (resolver `Stabilizing` callback is nil or
  broken), or peer-cache is returning inconsistent results across
  pods (one pod thinks N peers, another thinks N-1 → different HRW
  winner per pod).

### Checks

1. **Is `Stabilizing` configured?** From a pod's debug endpoint:
   `curl localhost:9428/debug/compaction/ownership` — should show
   `stabilizing_callback: bound`.
2. **Cross-pod peer-cache consistency.** Pick two pods, compare
   `curl localhost:9428/-/peers`. Mismatched lists explain split-brain
   HRW.

### Fixes

- **Sustained > 24 h with Tier B keeping up:** roll back to previous
  release. See `docs/superpowers/specs/2026-05-31-election-free-compaction.md`
  §9 rollback plan.

---

## 5. "Orphan deletions are flooding"

**Symptom:** `rate(lakehouse_compaction_orphan_files_deleted_total[1h]) > 1000`.

### Likely causes

1. **Many SIGKILLed pods** (recent OOM kills, node drains without
   graceful shutdown). One partial upload per crash; Tier B reclaims
   each.
2. **Duplicate compaction outputs from a dual-ownership window** (see
   §4) — Tier B catching up.
3. **Bug in `AddFile` idempotency:** if duplicates are getting written
   but `addfile_duplicate_key_total` is 0, a different code path is
   writing the duplicates. Audit recent commits to `internal/compaction`
   and `internal/storage/parquets3/storage_compact.go`.

### Checks

```promql
# Are SIGKILLs the cause?
rate(kube_pod_container_status_terminated_reason_total{reason="OOMKilled"}[1h])

# Is the duplicate-key canary firing?
rate(lakehouse_manifest_addfile_duplicate_key_total[1h])

# Is Tier B able to keep up?
lakehouse_compaction_orphans_skipped{reason!="too_young"}
```

### Fixes

- **OOM:** raise memory limits or tune query/compaction
  `MaxConcurrent`.
- **Duplicate writes without AddFile catching them:** investigate the
  storage layer; this should not happen.

---

## 6. "Pod won't terminate — drain is hanging"

**Symptom:** `kubectl delete pod` blocks past
`terminationGracePeriodSeconds`; pod log shows
`compaction: drain timed out after Ns with in-flight still running`.

### Root cause

A single in-flight compaction is taking > `DrainTimeout` (default
90 s). Big partitions or slow S3 can exceed this.

### Fixes

- **One-off:** wait for `terminationGracePeriodSeconds` to elapse;
  K8s sends SIGKILL; the partial upload becomes a Tier B orphan
  (recoverable, see §5).
- **Recurring:** raise `compaction.drain_timeout` in values.yaml AND
  `terminationGracePeriodSeconds` to match. Both must be large enough
  for the slowest realistic merge. Test by triggering a deliberately
  slow compaction (e.g. a 10 GB partition).
- **If `DrainTimeout` already large:** investigate why a single merge
  takes minutes — usually S3 throttling (`lakehouse_s3_throttled_total`)
  or excessive row-group fan-out.

### Metric

`lakehouse_compaction_aborted_during_drain_total` increments each
time the drain timeout fires before in-flight completes. A sustained
non-zero rate is a sign you need to raise the timeout.

---

## 7. Useful one-liners

```bash
# Per-pod ownership distribution
kubectl exec -it lakehouse-logs-0 -- curl -s localhost:9428/metrics \
  | grep -E '^lakehouse_compaction_partitions_owned '

# All compaction-relevant gauges/counters
kubectl exec -it lakehouse-logs-0 -- curl -s localhost:9428/metrics \
  | grep -E '^lakehouse_compaction_'

# Force a drain on a specific pod
kubectl exec -it lakehouse-logs-0 -- \
  curl -X POST -s localhost:9428/lakehouse/drain

# Inspect the manifest snapshot for a partition
kubectl exec -it lakehouse-logs-0 -- curl -s \
  'localhost:9428/debug/manifest?partition=dt=2026-05-31/hour=12'
```

---

## 8. When to roll back

Roll back to the previous release if ANY two of these are true for
> 24 h continuously:

- `lakehouse_compaction_dual_ownership_total` is increasing (not just
  ticking briefly during a ring change).
- `lakehouse_compaction_orphan_files_deleted_total` is increasing at
  > 1000/hour with no obvious cause (no recent OOM, no scale events).
- `lakehouse_storage_bytes_total` is growing at 2-3× the expected
  rate (duplicate compacted files not being reclaimed fast enough).
- Query result-correctness errors observed in production parity
  comparison against VL/VT baseline.

Rollback procedure (no data loss expected):

```bash
helm rollback lakehouse <previous-revision>
```

The previous binary still has the election code. Pods come up, the
K8s Lease gets re-acquired by one of them, single-leader compaction
resumes. The HRW-era `partitionAttempts` in-memory map is lost on
restart — no leftover state in S3. Manifest layout, S3 key shape, and
Parquet schema are unchanged by the rollback. See spec §9.
