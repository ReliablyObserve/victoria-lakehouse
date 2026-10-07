# API data-proof metrics

Pure functions that score a captured answer against a reference, as percentages per quality facet
(`scripts/proof/metrics`). The library uses only the Python standard library.

## Metrics

| id | facet(s) |
|---|---|
| M1 | `status`: 100 if the HTTP status equals the reference, else 0 |
| M2 | `error`: error text equal after normalising whitespace and quoting (case is part of the message) |
| M3 | `row_set`: multiset Jaccard; recall (missing rows), precision (extra rows), exact flag |
| M4 | `field_coverage`: Jaccard of the field-name sets; missing and extra fields listed |
| M5 | `value_equality`: rows paired by identity, share of equal values per field; the minimum field is named |
| M6 | `count`: absolute and % delta (a scalar, or one per group of a `stats by` vector) |
| M7 | `series_set`, `ts_alignment`, `points_within_tol`, `totals`, `nan_agreement`; point error p50/p95/max in details |
| M8 | `value_set` (Jaccard, recall, precision) and `hits_equality` (a lost `hits` scores 0) |
| M9 | `trace_set`, `span_set`, `span_fields` (name, service, start, duration, status, status message, kind, attributes, scope, scope version, scope attributes, resource, events, links), `parent_links`, `span_count` |
| M10 | `order`: Kendall agreement; pairs inside a group tying on the full sort key are not comparable |
| M11 | `json_leaves`: equal leaf paths over the union |
| M12 | `schema_keys`, `schema_types`, `schema_contract` |
| M13 | `truth`: a Lakehouse number against the same calculation over hot VL/VT (shapes `scalar`, `scalar_sum`, `per_key`, `tenant_set`); unflushed rows are reported next to the score, never subtracted |
| M14 | latency ratio PR/base and PR/ref over validated answers only; the invalid count is shown |

An answer that is an error scores 0 on every body facet (never 100). A body no decoder recognises
raises `UnknownShape` and the case becomes a `harness-error`.

## Verdicts

Per request, from the whole facet vector (the table shows the worst facet, base -> PR):
`exact`, `same`, `fixed`, `improved`, `regressed` (worse on any facet), `not-reproduced-on-base`,
`vacuous`, `blocked`, `nondeterministic` (re-sampling only classifies; blocked and harness-error
samples are left out of the check and listed separately), `harness-error`. An empty answer on a
core row (seeded data) is a `harness-error`, not `vacuous`, unless the case says `may_be_empty`.
100% is displayed only for an exact answer. Roll-ups are per row, surface and signal; logs and traces
are never averaged together.

## Surfaces

`vl-native`, `vt-native`, `jaeger` (native upstream APIs), `lh-logs`, `lh-traces` (Lakehouse-only APIs,
compared with base and a derived truth), `loki`, `tempo`. The core tier is the first five; `loki` and
`tempo` belong to the focused and daily tiers. The signal comes from the surface. Tables list native
surfaces first; `--core` limits a run to the core group.

```
python3 -m scripts.proof.metrics scripts/proof/fixtures/cases            # compact table
python3 -m scripts.proof.metrics scripts/proof/fixtures/cases --check    # fixtures vs recorded expectations
python3 -m scripts.proof.metrics <dir> --core --fail-on-regression
```

## Cases and fixtures

A case directory holds `meta.json` (surface, kind, options, expectation), `ref.json`, `base.json`,
`pr.json` (`{"status", "latency_ms", "content_type", "body"}`, the body being the raw text),
`truth_base.json` / `truth_pr.json` for Lakehouse-only truth checks, and optional `resample-N/`
directories. `meta.json` has a `provenance`:

- `recorded`: the answers are recordings of hot VictoriaLogs v1.53.0 / VictoriaTraces v0.12.0
  (`fixtures/recorded/`, taken by `fixtures/record.py` from a stack seeded with `cmd/datagen` plus one
  hand-written OTLP trace carrying events, links, scope attributes and a status message).
- `derived-from-recorded`: the base and/or PR answer is a recorded answer after the documented
  transformation in `derivation`.
- `synthetic`: a hand-written body (Lakehouse-only APIs, Loki, TraceQL metrics). Recording real
  Lakehouse base and PR answers is the job of the runner, not of this library.

`fixtures/_gen.py` regenerates `fixtures/cases/`; CI fails if its output is not committed.
Fixture percentages are synthetic fixture output, not measurements of Lakehouse.

Tests: `pip install -r scripts/proof/requirements.txt && python -m pytest scripts/proof/tests`.

## Tie cut: one rule, two implementations

When a row limit cuts through a group of rows that tie on the full sort key, which members survive
is layout dependent. `metrics/ties.py` ports the rule of `tests/parity/rows_ties.go`. Both are checked
against `tests/parity/testdata/tiecut_golden.json` (`rows_ties_golden_test.go` in Go,
`tests/test_tiecut_golden.py` here), so a change to one that the other does not follow fails CI.
The Go runner will record its own tie-cut decision in the answer envelope (`"tie_cut": {"explained",
"why"}`); the Python side then only consumes it (`evaluate_rows(tie_decision=...)`) and does not
re-derive it. The offline fixtures use the Python port with a re-read group in `meta.json`.

## Runner contract

What the runner (a later change) must provide so these functions can score its answers:

- **Envelope per answer:** the raw body text; HTTP status; content type; `latency_ms`; the transport
  error kind kept separate from the status (a timeout is not a 5xx); `warnings`. Per request: method,
  path, params, tenant form (numeric or alias), target (`ref`, `base`, `pr`, `truth_*`), round, sample
  index, data layer (buffer, cold, compacted, after-restart) and `buffer_unflushed_rows`. Per run: the
  base and PR SHAs and the VL/VT pins. The tie-cut decision is recorded (above).
- **Row identity:** logs pair rows by (`_time`, `_stream_id`, `_msg`), falling back to the full canonical
  row; spans by (`trace_id`, `span_id`). Trace-index and service-graph rows have no `span_id`: they fall
  back to `_stream_id`, then to the row key.
- **VT span rows** use VictoriaTraces' field names: `scope_name`, `scope_version`, `scope_attr:<k>`;
  `event:event_name:<i>`, `event:event_time_unix_nano:<i>`, `event:event_attr:<k>:<i>`,
  `event:event_dropped_attributes_count:<i>`; `link:link_trace_id:<i>`, `link:link_span_id:<i>`,
  `link:link_trace_state:<i>`, `link:link_flags:<i>`, `link:link_dropped_attributes_count:<i>`,
  `link:link_attr:<k>:<i>`; on every row `_msg` is `-`, plus `start_time_unix_nano`, `end_time_unix_nano`,
  `flags`, `status_message` and the `dropped_*_count` fields; `_time` is the end time and an empty value
  is `-`. VT's Jaeger API carries the same data as tags (`scope_attr:<k>`, `otel.scope.name`,
  `otel.scope.version`, `otel.status_description`, `error`), process tags (resource attributes), logs
  (events) and non-`CHILD_OF` references (links).
- **Lakehouse-only truth shapes** (from `internal/stats/api.go`):
  - `/stats/overview`: `total_rows`, `total_files`, `total_bytes`, `total_raw_bytes`, `oldest_data`,
    `newest_data`, `tenant_count`, `partition_count`, `storage_by_class[]`. All tenants, no window.
    `total_rows` is the larger of the manifest's live rows and the cumulative registry count. Truth is the
    sum over tenants of `count()` up to the cold end (shape `scalar_sum`), with the unflushed rows reported.
  - `/tenants`: `{"tenants":[{"account_id":"0","project_id":"0"}],"total_tenants"}`, string ids. The
    reference `/select/tenant_ids` is a bare list, answers 403 when an AccountID header is sent, and its
    end is exclusive.
  - `/cardinality/fields`: `{"fields":[{name,cardinality,type,has_bloom,indexed,storage_bytes}]}`, a list,
    so `per_key` takes `key_field` and `value_field`; the counts are HLL, so a `rel_tol` applies; skip
    `indexed: false` (`where`); `limit=100` by default; the tenant is `?tenant=`. Truth is a stats
    `count_uniq` with `key_label` `__name__`.
