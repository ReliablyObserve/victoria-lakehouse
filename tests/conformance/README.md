# Conformance registry

The registry (`registry/rows/**/*.yaml`) is the single source of truth for what the
verification machine checks: one row per endpoint or feature, **native VictoriaLogs /
VictoriaTraces surfaces first, Lakehouse additions second**. The inventory
(`inventory.generated.yaml`) is extracted from the vendored upstream sources; CI fails
when upstream has a route, pipe, filter, stats function or TraceQL function without a row,
and warns for flags (add a row under `rows/flags/matters.yaml` when a flag changes behavior
or Lakehouse compatibility).

Lakehouse mounts the upstream VictoriaLogs / VictoriaTraces HTTP handlers directly and
never re-implements an upstream API from scratch — the registry lists native surfaces
first because that's what's actually being exercised, then documents what Lakehouse adds
on top.

- Regenerate: `make conformance-gen` · Check: `make conformance-check`
- Row schema: `registry/schema.go` (`Row.Validate` documents every rule)
- Statuses: `expect: pass | differ (with differ_note) | absent | unsupported`; `pending: true`
  marks rows declared but not yet executed by the runner.
- Adding an endpoint or changing a handler? Add or update its row in the same PR
  (`scripts/ci/check_registry_touch.sh` enforces it).
- `UPSTREAM_COVERAGE.md` is generated from the inventory + registry — never edit it by hand.
- All rows are declared expectations until the runner executes them (future work).
- Request params/paths use a small placeholder vocabulary instead of literal values:
  `{{seed.start}}` / `{{seed.cold_end}}` (RFC3339), `{{seed.start_ms}}` /
  `{{seed.cold_end_ms}}` (millisecond-epoch integers, e.g. VT's dependencies `endTs`),
  `{{seed.start_us}}` / `{{seed.cold_end_us}}` (microsecond-epoch integers, e.g. VT's
  Jaeger traces-search `start`/`end`), `{{seed.trace_id}}` / `{{seed.stream_id}}`,
  `{{seed.task_id}}` (a delete-task id), `{{tenant.account}}` / `{{tenant.project}}`,
  `{{tombstone_id}}` (the id returned by a prior delete request in the same run, not
  part of the seed), and `{{proto.internal_select}}` / `{{proto.internal_delete}}` (not
  part of the seed either: the protocol version constant the runner reads from the
  vendored VL — `internalselect`'s `ProtocolVersion` consts — that
  `/internal/select/*`/`/internal/delete/*` requests must send as `version=...` or be
  rejected before ever reaching the query logic). Full list and rationale: the header
  comment in `registry/rows/vl/select.yaml`.
- **The two `{{proto.*}}` placeholders resolve PER MODULE**, from the vendored
  VictoriaLogs tree of the binary the row targets: `deps/VictoriaLogs`
  (`VL_VERSION_LOGS`) for `surface: vl` rows, `lakehouse-traces/deps/VictoriaLogs`
  (`VL_COMMIT_TRACES`) for `surface: vt` rows — `lakehouse-traces` mounts
  VictoriaLogs' `internalselect` package for these endpoints, so it follows its own
  VictoriaLogs pin, not VictoriaTraces and not the logs pin. The pins may differ and
  today do: `select` is `v5` on both, `delete` is `v2` on the logs pin (VL v1.52.0)
  and `v1` on the traces pin (VL v1.51.0). The resolved pairs are extracted from both
  trees and recorded under `protocol:` in `inventory.generated.yaml`, so a future bump
  that moves either version surfaces as drift instead of as a runtime
  "unexpected protocol version" rejection between peers.
- Flag rows (`kind: flag`) declare `compare: { type: status }` with no `request`: they
  document that a flag exists and matters, not a specific HTTP call. They stay
  declarative only until the runner exercises actual flag-variant stacks.

## Flag-warning triage

An upstream flag without a row is a **soft warning**, not a failure — it says
"upstream registers this, Lakehouse inherits whatever it does, and nothing
Lakehouse-specific depends on it". Only flags in the two classes below get a row
in `rows/flags/matters.yaml`:

1. **Changed between the pinned versions** — the flag arrived, was renamed or was
   deprecated in the range the bump crosses.
2. **Touches Lakehouse compatibility** — admission control and queueing,
   retention and backfill windows, ingest limits, cold-tier lookbehind windows,
   or a route Lakehouse serves differently from the hot tier.

Everything else stays a warning on purpose: ingest-format knobs (`syslog.*`,
`splunk.*`, `journald.*`, `datadog.*`, `loki.*`), storage-node and TLS plumbing
(`storageNode.*`), local-disk and auth-key knobs (`storageDataPath`,
`*AuthKey`, `inmemoryDataFlushInterval`, `retention.maxDisk*`), and the internal
peer transport caps (`internalinsert.*`, `internalselect.*`). They are either
pre-storage admission control Lakehouse mounts verbatim, or they act on the hot
local storage Lakehouse replaces wholesale.

The VL v1.52.0 / VT v0.11.0 bump added six flags. Five already had rows carrying
`since:`; the bump only moved them from pending-bump to live, and their notes were
rewritten from "not in the pinned version, re-check after the bump" to what is
actually true now:

| flag | row | class |
| --- | --- | --- |
| `vl:vmalert.proxyURL` | `vl.flag.vmalert_proxy_url` | route Lakehouse does not serve |
| `vt:vmalert.proxyURL` | `vt.flag.vmalert_proxy_url` (**added**) | same, traces side |
| `vt:search.maxTraces` | `vt.flag.search_max_traces` | result cap |
| `vt:search.maxTags` | `vt.flag.search_max_tags` | result cap |
| `vt:search.fieldsLookbehind` | `vt.flag.search_fields_lookbehind` | cold lookbehind default |
| `vt:search.streamFieldsLookbehind` | `vt.flag.search_stream_fields_lookbehind` | cold lookbehind default |
| `vt:nativeinsert.maxRequestSize` | `vt.flag.nativeinsert_max_request_size` (**added**) | ingest limit on a new route |

`vl:nativeinsert.maxRequestSize` did not change with the bump but was rowed
alongside its VictoriaTraces twin, so the native-ingest admission cap is covered
on both surfaces.

Two deprecations to keep in view: `search.traceMaxServiceNameList` and
`search.traceMaxSpanNameList` are assigned to `_` in VictoriaTraces 0.11.0
(superseded by `search.maxTags`) — still registered, so still warned about, but
setting them now does nothing.

## Flag collisions in the traces binary

`lakehouse-traces` links VictoriaLogs' and VictoriaTraces' packages into one
process, and both register flags of the same name (`-retentionPeriod`,
`-storageDataPath`, `-insert.maxFieldsPerLine`, ...) with the same global
`flag.CommandLine`. The `patches/vt-traces/*-flag-dedup*` patches make the
VictoriaTraces side reuse VictoriaLogs' registration instead of panicking.

`TestVTFlagDedupCoversEveryCollision` recomputes that collision set from the two
vendored trees — using `go list -deps` on the traces module, so only packages
actually linked count — and requires the dedup list to match it exactly in both
directions: an unguarded collision means the binary panics at startup, a guard
with nothing behind it means VictoriaTraces silently skips its own registration.
At VL v1.51.0 / VT v0.11.0 there are 34 collisions and all 34 are guarded.
