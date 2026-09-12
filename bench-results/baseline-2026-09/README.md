# Baseline 2026-09 (pre-upgrade: VL v1.50.0 / VT v0.9.2 / parquet-go v0.30.1)

Files:
- `run-baseline.json` / `run-baseline.md` — `scripts/bench/run.sh --signals both --s3-latency "0 100"` (benchmark compose, parity gate first)
- `full-scope-lat0.csv|md`, `full-scope-lat100.csv|md`, `metrics-lat*/` — `scripts/bench/full-scope-s3-bench.sh` (e2e compose) with the per-scenario S3-ops table
- `env.txt` — image tags, git sha, host, docker version

Judge every later PR against these numbers (perf gate: any LH/CH >= 1.0 cell or an LH/VL regression > 10 % on any scenario blocks).

## Caveats (2026-09-12 harness recheck)

- **`run-baseline.md` logs table is valid** except the `trace_lookup` rows, which
  measure a miss on every system (0 results): `fetch_sample_tid` picked a
  `trace_id` via an unordered `limit 1` over a 7-day window, so the id it grabbed
  usually wasn't the one on screen by the time the query ran. Fixed for the
  traces rerun below (`fetch_sample_tid` now queries a 1h window ordered by
  time descending); the logs table was not rerun because its own
  `trace_lookup` cells are the only ones affected and the fix doesn't change
  any other logs query.
- **`run-baseline.md` traces table is superseded by `run-baseline-traces-v2.md`.**
  The original traces sweep queried VictoriaTraces with Lakehouse-only field
  names (`service.name`, `span.name`, `duration_ns`); VT v0.9.2 stores spans as
  `resource_attr:service.name`, `name`, `duration` (ns), so 12 of 28 VT/LH
  traces cells came back `0` or degenerate. The parity gate also only printed
  (`parity_gate "$signal" || true`) instead of enforcing, so the divergence
  never blocked the run.
- **Harness fixes applied** (`scripts/bench/run.sh`, `scripts/bench/report.py`):
  1. Traces `prep()` queries switched to VT-native field names (Lakehouse serves
     both dialects, so LH/CH results are unaffected). Fields containing a colon
     (`resource_attr:service.name`) must be backtick-quoted in LogsQL — both in
     `stats by (...)` and in an equality filter — or the parser misreads the
     colon as a bucket-size / second-operator clause and either errors or
     silently matches the wrong thing (`resource_attr:service.name:="x"`
     without backticks parsed but returned 0 on both tiers).
  2. `slow_spans` uses `trace_id:* duration:>50000000` (50 ms). 100 ms was the
     original threshold; the seed's spans are 5-54 ms (`cmd/datagen`), so
     >50 ms selects the slowest ~8% and is the largest round number giving a
     non-zero, exactly-equal count on VT/LH/CH (verified at
     10/20/30/40/45/50/53 ms — all matched exactly). `trace_id:*` is required,
     not optional — VictoriaTraces' internal `trace_id_idx_stream` index rows
     (fields `trace_id_idx, start_time, end_time, duration`) carry a
     trace-level `duration` and no `trace_id`; some of those exceed 100 ms, so
     a bare `duration:>N` filter over-counted on VT only (1075 vs LH's real 0)
     until `trace_id:*` was restored. `Duration` threshold made consistent in
     the ClickHouse SQL too.
  3. `fetch_sample_tid` now queries the last 1h window ordered `sort by (_time)
     desc | limit 1` and fails the run (non-zero exit) if no id comes back,
     instead of silently keeping a dummy id over an unordered 7-day `limit 1`.
  4. `parity_gate()` is now enforcing: it compares every system's `count_total`
     against the baseline (VL/VT) within ±5% and the call site aborts the run
     (`exit 1`) on any mismatch, instead of `parity_gate "$signal" || true`.
     A baseline count of `0` (or a non-numeric baseline result) is itself now
     treated as a mismatch — the gate refuses to sweep on empty data instead
     of only comparing other systems against a zero baseline.
  5. `run.sh` logs the exact prepared request per (signal, query, system) via
     `prep()` → `log "req <signal>/<query> <system> <method> <url> body=..."`
     (visible throughout `run-baseline-traces-v2.log`).
  6. `report.py` now invalidates a whole row (`baseline-empty`) when the
     baseline cell itself is 0/empty/zero-bytes, instead of only checking each
     engine's own result against a possibly-bogus baseline; and flags a
     `⚠ shape` cell when an engine's `avg_bytes` differs from the baseline's by
     more than 10× either way (payload-shape outlier, not a validity failure).
  7. Found during the traces rerun (not in the original recheck): ClickHouse's
     `scan` scalar was extracted by summing the last TSV column when it looked
     numeric — correct for count queries, wrong for a raw column scan whose
     last column (`Duration`) is itself numeric. It silently summed all
     returned durations (e.g. `18622000000`) instead of counting rows, so the
     traces `scan` CH cell was `✗` (self-inconsistent, not a real divergence)
     in every run before this fix. `measure_query`/`fetch_scalar` now thread
     the query name through and always use ROW COUNT for `scan`, regardless of
     column numeric-ness.
  8. MinIO images pinned to `quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z` /
     `quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z` in
     `deployment/docker/docker-compose-benchmark.yml`,
     `deployment/docker/docker-compose-e2e.yml`, `tests/parity/docker-compose.yml`
     (`minio/*:latest` is no longer anonymously pullable from Docker Hub).
  9. `report.py`'s header no longer hardcodes "gp3-simulated": it now reads
     `disk_profile` from the JSON rows (written by `run.sh` going forward)
     and prints that, falling back to "unspecified" for JSON produced before
     this fix (both `run-baseline.json` and `run-baseline-traces-v2.json`
     predate it, so their re-rendered headers read "unspecified" — the actual
     profile for both runs was `local-ssd`, the default, recorded below).

**Exact rerun command for `run-baseline-traces-v2.{json,md,log}`** (Step-6
stack already up, disk profile `local-ssd` — the default, not overridden):

```
scripts/bench/run.sh --signals traces --s3-latency "0 100" --ranges "1h 24h" \
  --iterations 20 --warmup 3 --no-up --no-ingest \
  --output bench-results/baseline-2026-09/run-baseline-traces-v2.json
```
- **Pre-rerun verification (Step 6, 24h window, one moment in time — see
  `run-baseline-traces-v2.log` for the actual per-run counts, which drift
  slightly between runs because the seed is a one-time 7-day backfill and the
  window keeps sliding forward):** `count_by_service` VT=17132/LH=17132,
  `service_filter` VT=3499/LH=3499, `span_name` VT=1664/LH=1664, `slow_spans`
  (50 ms) VT=1377/LH=1377, `scan` (1000-row limit) VT=1000/LH=1000 rows — all
  five pairs equal and non-zero on both tiers before the sweep ran.
- **Enforced parity gate (from `run-baseline-traces-v2.log`):** count_total at
  S3 latency 0ms — victoriatraces=16682, lakehouse=16682, clickhouse=16682; at
  100ms — victoriatraces=16668, lakehouse=16668, clickhouse=16668. Both gates
  passed (all three systems exactly equal); no `parity gate FAILED` line
  appears in the log.
- **`run-baseline-traces-v2.md` result:** 28/28 traces cells valid, 0 invalid,
  no `✗` cells, non-zero `[res]` on every scenario. LH is a median 2.7× VT/CH
  baseline (p90 4.7×) and 21× faster than ClickHouse on the same S3 Parquet.
