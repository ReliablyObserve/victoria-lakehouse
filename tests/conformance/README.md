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
  `{{tenant.account}}` / `{{tenant.project}}`, and `{{tombstone_id}}` (the id returned
  by a prior delete request in the same run, not part of the seed). Full list and
  rationale: the header comment in `registry/rows/vl/select.yaml`.
- Flag rows (`kind: flag`) declare `compare: { type: status }` with no `request`: they
  document that a flag exists and matters, not a specific HTTP call. They stay
  declarative only until the runner exercises actual flag-variant stacks.
