# K8s-Style Resource Bounds — Design Spec

**Date:** 2026-05-29
**Status:** Draft — needs review before plan + implementation
**Driving feedback:** `feedback_k8s_style_resource_bounds`
**Author note:** This is a design spec, not an implementation plan.
The implementation will be broken into separate plans per resource
surface, each PR-sized.

---

## Problem

Victoria Lakehouse currently uses **flat constants** for every
resource ceiling:

| Resource | Current limit | Source |
|---|---|---|
| L1 in-memory cache | 256 MB | `cache.memory-mb` flag |
| Concurrent S3 downloads | 16 | `s3.MaxConcurrentDownloads` |
| File workers per query | 64 | `query.file_workers` |
| Max rows per query | configurable | `query.max_rows` |
| Smart cache disk | configurable | `smart_cache.disk_limit_max` |

These flat values **under-utilize headroom** when the node is idle
and **under-protect** when load spikes. Production OOM-kills (exit
137) under Grafana drilldown load are the visible symptom: the node
has plenty of memory most of the time, but a burst of concurrent
slow queries pushes RSS past the container limit because every
ceiling fires independently rather than coordinating.

K8s solved this for pods with `requests` (always-reserved baseline)
and `limits` (hard ceiling). Each pod requests what it needs, can
burst up to its limit, and the kube-scheduler/cgroup enforces the
upper bound. We need the same semantics for LH resource controls.

## Goals

1. **Bounded growth** — peak resource usage never exceeds a declared
   limit, regardless of query mix or concurrency.
2. **Burst-friendly** — idle replicas don't waste capacity; busy
   replicas can scale up automatically within their limit.
3. **Operator-visible contract** — Helm chart values + Prometheus
   metrics expose `request` and `limit` per resource so an operator
   can size the K8s pod request/limit to match.
4. **Graceful overflow** — when load truly exceeds the limit, the
   system sheds load (HTTP 429 or graceful degradation), it does not
   OOM-kill itself.

## Non-goals

- Cluster-level autoscaling decisions (out of scope; K8s HPA
  handles pod count).
- Per-tenant resource quotas (separate spec).
- Replacing the existing `cache.memory-mb` flag overnight — the
  config schema extension must be backwards-compatible.

## Architecture

### 1. Three-value config schema per resource

Every resource control surfaces three values in YAML config:

```yaml
query:
  file_workers:
    request: 8        # always reserved minimum (was effective default)
    limit: 64         # hard maximum we'll ever run with
    scale_on: queue_depth  # signal that drives request → limit ramp

cache:
  memory:
    request_mb: 64    # smallest cache we keep warm even at idle
    limit_mb: 512     # hard upper bound; eviction kicks in past this
    scale_on: query_rate

s3:
  concurrent_downloads:
    request: 4
    limit: 16
    scale_on: in_flight
```

Backwards compat: legacy `cache.memory-mb: 256` continues to work,
interpreted as `request_mb=256` and `limit_mb=256` (equivalent to
flat behaviour). New three-value form takes precedence when set.

### 2. Scaling policy

Each `scale_on` signal is a monotonic counter or gauge already
emitted by the system:

| Signal | Source | What it drives |
|---|---|---|
| `queue_depth` | length of `taskCh` in RunQuery | `query.file_workers` from `request` toward `limit` |
| `query_rate` | `ConcurrentSelects` gauge | `cache.memory` from `request_mb` toward `limit_mb` |
| `in_flight` | `dlSem` claim count | `s3.concurrent_downloads` from `request` toward `limit` |

The ramp is linear with the signal: at 0% signal load, current value
= `request`. At 100% (signal at its own pressure threshold), current
value = `limit`. Smoothed over a short window (e.g. 5s EWMA) to
avoid thrashing.

### 3. Overflow behaviour

When a request would push a resource past its `limit`:

- **In-process bound (file_workers, dlSem)**: caller blocks on the
  semaphore. If wait exceeds a configurable timeout (default 30s),
  return `429 Too Many Requests`.
- **Cache (memory)**: LRU evicts to make room; if the new entry is
  itself larger than the limit headroom, refuse to cache (request
  still served from S3, no OOM).
- **Disk (smart_cache)**: existing `disk_limit_max` enforces this
  already — adopt the same model for memory.

The system **never** OOM-kills itself. If a query genuinely demands
more than the limit allows, it fails with a clear HTTP error.

### 4. Metrics

Three new families per resource, matching the K8s container
resource metric shape so dashboards can be reused:

```
lakehouse_resource_request{resource="cache_memory"} = 67108864
lakehouse_resource_limit{resource="cache_memory"} = 536870912
lakehouse_resource_usage{resource="cache_memory"} = 234881024
```

`lakehouse_resource_overflow_total{resource="..."}` counter for
shed-load events.

### 5. Helm chart contract

`values.yaml` exposes the three-value form for every resource and
the chart's `resources:` block (K8s container resources) derives
its `limits.memory` from the sum of memory-class resource limits
(L1 cache + buffer headroom + Go runtime budget). Operators see one
authoritative source of truth.

## Affected surfaces (incremental implementation order)

1. **`s3.concurrent_downloads`** — smallest scope, easiest to
   instrument and test. Land first as a reference implementation.
2. **`query.file_workers`** — the OR-fan-out OOM trigger. Direct
   user-visible win.
3. **`cache.memory`** — L1 LRU cap. Coordinated with smart cache
   memory budget.
4. **`smart_cache.memory`** — currently has `disk_limit_max` only;
   add an equivalent memory cap.
5. **`query.max_rows`** — already a hard limit; expose request as
   the soft signal (start streaming early, error on hard limit).
6. **Tombstone / WAL caches** — same pattern, lower priority.

## Test strategy

For each resource:

1. **Unit:** scaling policy returns the right current value for a
   given signal (table-driven).
2. **Integration:** spin up a Storage with `request=2, limit=8`,
   drive load past the limit, assert the overflow path fires and
   no panic occurs.
3. **Stress:** existing `tests/parity/` + a new
   `tests/load/` that ramps concurrent queries past `limit` and
   asserts the container stays under its memory ceiling AND
   returns 429s instead of OOM-killing.

## Open questions for reviewer

1. Linear ramp vs. step function for the request→limit scaling? Linear
   is more responsive; step is more predictable.
2. Per-query memory accounting — currently `query.max_memory_bytes`
   is a separate flat limit. Should it fold into this framework, or
   stay separate?
3. Backwards compat: keep legacy flat flags forever, or define a
   deprecation window (e.g. remove in v1.0)?
4. Default `request`/`limit` ratios — propose `request = limit/4`
   for all resources unless we have a better signal.

## Out-of-scope follow-ups

- Per-tenant resource quotas (request/limit per tenant ID).
- Cluster-aware scaling (LH replica sees its sibling load and
  scales its own request accordingly).
- Adaptive limit tuning (move `limit` itself based on observed
  pod headroom from cgroup memory stats).

## Acceptance criteria for the first implementation PR

The `s3.concurrent_downloads` reference implementation must:

- Accept the three-value form in config + preserve the legacy
  one-value form.
- Emit the three new metrics for that one resource.
- Pass an integration test that ramps concurrent downloads past
  the request, asserts the system uses up to but not past the
  limit, and returns 429 (or queues with timeout) when truly
  saturated.
- Documented in `docs/configuration.md` with a worked Helm chart
  example showing how to size container memory against the sum
  of memory-class resource limits.

## Next step

Convert this spec into an implementation plan under
`docs/superpowers/plans/2026-05-29-k8s-style-resource-bounds.md`
using the writing-plans skill (one plan per surface from the
incremental order above, plus a foundation plan that lands the
shared three-value config struct + scaling policy interface).
