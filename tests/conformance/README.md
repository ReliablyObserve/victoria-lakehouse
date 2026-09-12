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
- All rows are declared expectations until the runner executes them (a later milestone).
