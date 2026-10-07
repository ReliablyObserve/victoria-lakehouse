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
| M10 | `order`: Kendall agreement; pairs inside a group tying on the full sort key are not comparable. `key_order` (opt in with `meta.key_order`): share of paired rows whose JSON members come in the reference's relative order |
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

## The proof stack, the API runner and the visual capture

The metrics library scores answers; the pieces below produce them on a real stack. Nothing here runs in CI
yet; they are run by hand (and the sticky comment and gate come later).

### Stack (`stack.py`, `deployment/docker/docker-compose-proof.yml`)

An isolated compose project (`PROOF_PROJECT`, default `lhproof`), loopback ports only: hot VictoriaLogs and
VictoriaTraces (the reference), Lakehouse built from main (`base`) and from the PR (`pr`) for both signals, one RustFS,
Grafana with a datasource per side (VictoriaLogs, Jaeger, Loki through loki-vl-proxy), the Jaeger UI per side and
loki-vl-proxy per side. Nothing is shared between two stacks: every image carries the project name
(`<project>-logs:base`, `<project>-grafana:1`, `<project>-vlproxy:<pinned release>`, ...; read from the compose file) and `down` removes exactly those tags and no others; every published
port is a `PORT_*` variable computed from the project (`lhproof` uses 48000..48452, any other project a block derived
from its name that stays clear of the ports other stacks of this host use and below 49152, or `PROOF_PORT_BASE`; `up` refuses ports that
are already taken). Hot VictoriaLogs/VictoriaTraces, loki-vl-proxy (the release pinned in
`Dockerfile.loki-vl-proxy`, bumped by the daily loki-vl-proxy workflow) and Grafana carry a build recipe in the compose
file, so a machine with none of the images builds them: `stack.py build` is all it takes.

```
python3 scripts/proof/stack.py build --main <main checkout> --pr <PR checkout>   # <project>-<signal>:base / :pr, datagen, hot, proxy, Grafana
python3 scripts/proof/stack.py up
python3 scripts/proof/stack.py seed --out OUT     # cold layer: the same datagen seed and --now to hot + base, then to the PR
python3 scripts/proof/stack.py hold --out OUT     # buffer layer: restart with a 1 h flush and ingest the buffer batch
python3 scripts/proof/stack.py down               # compose down -v and this project's images, by exact tag
```

The windows are absolute and hour aligned (`OUT/state.json`): cold `[H-4h, H)`, buffer `[H, H+1h)`. Tenants: `0:0` and
`1001:0`, the latter also reached as the alias `acme-corp` (sent to Lakehouse as `X-Scope-OrgID` and to hot as
`AccountID: 1001`, because upstream has no aliases), and tenant `7:0`, a logs-only fixture for ties and for several streams in one window
(#429 / #432): three streams, rows of different streams sharing a second, one column that is the same in a whole stream,
one that varies, one that is sparse, and input names that are not in alphabetical order. Over HTTP both hot and Lakehouse
write the JSON members of a row alphabetically, so the order of a block's columns (#452) is not visible in `/select/logsql/query`
and the `key_order` facet is exact on main; the fixture exercises the row sets and the tie cuts, and the `ko.pack_json`
rows read the one surface that exposes column order (`pack_json` packs a row in block order). `hold` records the unflushed rows of the buffer window per Lakehouse (`buffered`, once two reads in a
row agree) and a trace of tenant 0:0 whose spans carry events and links (`trace_id`).

### API runner (`runner/`)

```
python3 -m scripts.proof.runner.run --state OUT/state.json --out OUT/api --tier core [--tier field-values] [--only REGEXP]
```

Rows are JSON (`runner/rows/*.json`): surface, kind (the metrics kinds), path, params, window, layers
(`cold`, `buffer`, `all`) and tenant forms (`numeric`, `numeric1001`, `alias`). For every concrete request
the three targets are called in the order ref, base, PR, and the answers are written as case directories
(`cases/<surface>/<row>.<form>.<layer>/`: `meta.json`, `ref.json`, `base.json`, `pr.json`, `resample-N/`)
with the envelope fields of the contract above: status, raw body, latency, transport error kind, tenant
form, layer and, for the buffer layer, `buffer_unflushed_rows`. The metrics library scores them. Before any
comparison the row count of every tenant form and layer must be equal on ref, base and PR; otherwise the run
exits 2 (it did not complete). "Equal" means: counts that stay put (rows reach the stores late, hot VictoriaTraces' trace-index rows 20-40 s after ingest: the run first waits until every count of `*` has not moved for 45 s), the same count of span rows (logs: of
all rows), the same hash of the row identities, for every tenant form (numeric, numeric1001, alias, and the key-order
tenant) and every layer (cold, buffer, all); zero rows on all three is an error, because every window is seeded. A request
that is neither `exact` nor `same` is sampled twice more (requests already `fixed` or `improved` are re-sampled for free);
a verdict that flips is `nondeterministic`; a request that could not be re-sampled because the allowance ran out is marked
`resample_skipped` and shown as unconfirmed. The same 4xx on every target is a `harness-error` unless the row says
`expect_error`. A row with `limit_arbitrary` (a list cut at `limit`: upstream keeps arbitrary entries past it) is scored
on the size of the answer and on its membership in the reference answer without the limit, not on which values survived. `report.md` (request, base %, PR %, worst facet, verdict), `report.json`
(facets and notes) and `bodies.jsonl.gz` (every answer) are written next to the cases.

Exit codes: 0 done, 1 a failing verdict (`regressed`, `nondeterministic`, `harness-error`), 2 incomplete.

### Visual capture (`tests/playwright/proof`, `visual/`)

```
cd tests/playwright/proof && npm ci
VP_STATE=OUT/state.json VP_OUT=OUT/visual GRAFANA_URL=http://127.0.0.1:48300 npx playwright test
python3 -m scripts.proof.visual.compare OUT/visual     # compare.md / compare.json
python3 -m scripts.proof.visual.montage OUT/visual     # montage/<page>-<range>.png: base | PR | reference
```

`spec.json` lists the pages: Explore with the VictoriaLogs datasource (logs and volume; the query builder's
value list of a map attribute; the stream-filter value list of a stream field), VMUI and VTUI, Explore with
the Jaeger datasource and the Jaeger UI (service and operation lists, the trace view with span events,
links and scope), and, in the focused tier, Explore with Loki and Logs Drilldown through loki-vl-proxy.
Each page is captured on base, PR and the reference: a screenshot, every backend request and response of
the page load (Grafana `/api/ds/query` frames, datasource resource calls, the own API calls of the UIs),
console errors and settle time.

`compare.py` matches the questions of the three sides by request key and scores each matched answer with
the metrics above (values, rows, series, traces), so a page reads base % and PR % against the reference. The
panel state of every side (data, empty, error, unsettled) is decided from the responses and from the DOM
(`states.py`), and a state change decides the verdict by itself where the table says so: a base that showed
a query error and a PR that answers nothing, as the reference does, is `fixed`, never a new empty panel.

Not covered: the Lakehouse UI (no upstream reference), Tempo pages, tenant forms other than the default
tenant 0:0 in Grafana (the datasources carry no tenant header), and any answer that depends on wall-clock time.

### PR comment (`comment.py`)

```
python3 -m scripts.proof.comment --api OUT/api --visual OUT/visual --label "PR 438" --image-base <url> --explain why.json --out comment.md
```

The verdict line first; the table lists the requests that changed or still differ (matching ones are
counted), with base %, PR % and the worst facet of each; then every remaining difference with the reason
from `why.json` (a map from a request-id prefix to the explanation). A difference without a reason is
printed as `unexplained` and the command exits 1. Row sets: `runner/rows/core.json` (native-first core),
`field-values.json` (field values over map attributes, the empty-value bucket, stream fields) and
`audit.json` (span events, links and scope; column and key order of sort rows; field names).

### Known, tracked differences the page checks do not count

A failed request of a known issue (today `/select/buildinfo`, #463) does not decide the panel state of a page; it is listed
as a warning. A page whose sides are all empty, or where the panel the page is about says "No data" on every side, is
`vacuous`, never `match`. Logs Drilldown's series-limit triangle ("Show all 500") is a warning about the number of values,
not a failed panel.

### Image hosting

`visual/publish.py` is the loki-vl-proxy script unchanged: montages (PNG files of at most 300 KB, at most 80)
go to the orphan branch `pr-visuals` under `pr-<number>/`, one parentless commit per change, and a PR comment
embeds `https://raw.githubusercontent.com/<owner>/<repo>/pr-visuals/pr-<number>/<file>.png`. The branch is
created by the first publish with a token that may push to it (`PUBLISH_TOKEN`, `contents: write` for the
workflow token).

### Ported files

`visual/vio.py` and `tests/test_visual_publish.py` are copied from loki-vl-proxy `bench/visual` (`429f15b9`);
`visual/publish.py` too, with two changes (the token reaches git through `GIT_CONFIG_*` environment variables, and only PNG files are accepted); `visual/montage.py` and `visual/compare.py` and `tests/playwright/proof/capture.spec.ts`
are adapted from it. Each file names its source in its header.

### Dotted custom fields through loki-vl-proxy

The proxy maps an underscore name back to a dotted VictoriaLogs field only for its built-in OTel names and for `-field-mapping`
entries; `-extra-label-fields` alone only lists the field on the label APIs. The proof proxies therefore carry a `-field-mapping`
entry for each dotted attribute the fixtures write (`http.method`, `http.status_code`, `http.target`, `exception.type`,
`exception.message`, `instrumentation.lib`, `otel.span_id`, `otel.trace_id`, `scope.name`). A new fixture field with a dot needs one too, or a Loki
page (Logs Drilldown) answers nothing for it on every side, the reference included.
