## Method and limits

- Sources: each system's own documentation, source code, release notes and public price pages, read on the review date; for Lakehouse, this repository's code, tests, CI and issues. Every cell names its source, and the verification notes below record the cells that a second read corrected.
- Vendor roundups were used only to discover systems, never as a source of facts.
- Nothing was run against any competitor. Every "readable by X" cell is from docs or code, apart from LH's CI-proven readers.
- SaaS durability and largest-deployment cells are mostly ❓: vendors do not publish them.
- Prices are public list prices seen on 2026-10-02, in the US region. Where the unit was unclear we say so.
- Counts in the Interfaces matrix follow the rules in the Interfaces section, applied to each vendor's own docs. A vendor whose docs list fewer protocols than its product supports will be under-counted. Cells marked [K] or ❓ were not verified.

---

## Verification notes

Each correction was re-verified on 2026-10-02.

| # | Topic | First reading | After (verified) | Evidence |
|---|---|---|---|---|
| C1 | OpenObserve ack durability | "fsync off by default" | **Off in the binary default, on in the official Helm charts.** `ZO_WAL_FSYNC_DISABLED` defaults to `true` (`config.rs` L2041). `write_batch` calls `wal.sync()` only when `!wal_fsync_disabled` (ingester `writer.rs` L520-523; logs `mod.rs` L615). Both `charts/openobserve` and `charts/openobserve-standalone` set `ZO_WAL_FSYNC_DISABLED: "false"`. Still no replication before ack. | github.com/openobserve/openobserve `src/config/src/config.rs`, `src/ingester/src/writer.rs`, `src/wal/src/writer.rs`; github.com/openobserve/openobserve-helm-chart `values.yaml` |
| C2 | Parseable ack | "no fsync" | **Confirmed.** `DiskWriter::write` flushes the BufWriter and nothing more. The only `sync_all` hits in the repo are the upload function `sync_all_streams`. | `src/parseable/staging/writer.rs` L176-183 |
| C3 | Tempo indexes | "Parquet column bloom filters" | **Wrong.** The Tempo docs say "Parquet has native support for bloom filters. However, Tempo doesn't use them". Tempo uses its own sharded trace-ID bloom files per block. | grafana.com/docs/tempo/latest/operations/schema/ |
| C4 | Tempo block layout | "many small files per block dir" | **Wrong.** A block is one `data.parquet` plus `meta.json`, bloom shards, an index and a temporary `nocompact.flg`. A block is invisible until `meta.json` exists. | grafana.com/docs/tempo/latest/reference-tempo-architecture/block-format/ |
| C5 | Tempo external readability | g1: "not documented for external use"; g3: "yes (Tempo schema)" | **🟡 partial.** The schema *is* documented column by column, and the file is standard Parquet. But each row is one whole trace with deeply nested repeated groups. Dedicated attribute columns are assigned per block, and the mapping is stored in `meta.json` (`BlockMeta` dedicated columns). The only tool the docs name is `tempo-cli`. No DuckDB/Spark/Trino recipe exists, and a GitHub issue search for "duckdb" in grafana/tempo returns 0. Readable in principle, not shown in practice; we did not run DuckDB on a block. | schema and block-format docs; `tempodb/backend/block_meta.go`; GitHub search API |
| C6 | Loki / Tempo S3 class | "no tiering documented" | **Refined.** Both have a single write-time `storage_class` (Loki defaults to `STANDARD`). Neither tiers by age. Loki says lifecycle rules are "an additional safeguard" only; Tempo recommends lifecycle rules only for incomplete multipart uploads. | Loki `configuration.md`; Tempo configuration reference; Tempo S3 doc |
| C7 | Elastic snapshot S3 class | [K] | **Sourced.** `storage_class` / `data_storage_class` exist. Transitioning objects to Glacier classes or Intelligent-Tiering archive tiers "may permanently lose access". | elastic.co s3-repository doc |
| C8 | VictoriaLogs object storage | open draft PR | **Confirmed and detailed.** Issue #48 is open (last comment 2026-09-28, "is there any progress?"). PR #1155 is a draft (created 2026-03-05, updated 2026-08-05) and implements a *read-only* offload of retained partitions; its author notes it "works very slow". The v1.53.0 and v1.52.1 release notes (2026-10-01) mention no object storage. | GitHub API, issue 48 and pull 1155 |
| C9 | LH #311 scope | repository audit: "only select pods PUT shared state" | **Repository audit incomplete.** See the verification notes.1. | issue #311 comment, 2026-10-01T21:37Z |
| C10 | LH "~7% slower than ClickHouse" | "not found in repo; owner-stated" | **Found in draft PR #310** (open, not merged). The body reads: "LH loses to ClickHouse by about 7% at 4 CPUs on the word filters (366 vs 341 ms)", ClickHouse 26.5, RustFS + probe-verified toxiproxy at 50 ms, "measured earlier on the previous head … not re-run". Labelled **measured, unmerged PR**. | `gh pr view 310` |
| C11 | LH ingest protocols | long tail [U] | **Refined.** jsonline and OTLP/HTTP traces run in e2e tests. Loki push (JSON and protobuf), ES `_bulk`, Datadog v2, journald and Splunk HEC passed a **manual** probe sweep (`tests/verification/matrix.md`, May 2026), not CI. **Zipkin** is not exposed, because VT does not register it (matrix row TI2). **Syslog**: the LH binaries start no syslog listener (no syslog code in `cmd/`), so it is ❓. **OTLP gRPC**: not in the coverage inventory, so ❓. | LH repo |
| C12 | LH headline latency | README p95 8.5 / 7.7 ms | **Not used.** That run had no injected S3 latency (wrong injector target), a ~14k-row dataset and warm caches. | memory `reference_bench_latency_injection_broken`; repository audit §4 |
| C13 | Observe engines | "Spark, DuckDB, Trino, PyIceberg" | **Confirmed verbatim.** Data is read straight from the customer bucket, and the mode is in private preview (blog dated 2026-07-30). | snowflake.com blog |
| C14 | CloudWatch S3 Tables | Iceberg, no extra charge | **Confirmed.** "Available at no additional cost". No backfill. Retention follows the log group. | AWS doc |
| C15 | Coralogix archive | 3 JSON-string columns | **Confirmed.** The documented recipes are pandas `read_parquet` and an Athena external table. | coralogix.com archive doc |
| C16 | Datadog archives | storage class, Glacier IR | **Confirmed.** Storage class or bucket lifecycle. Archive Search supports a subset of classes, including Glacier IR. Format: `.json.zst` (default) or `.json.gz` under `dt=/hour=` paths. | docs.datadoghq.com archives |

Also noted, not a research-note error: **LH's `docs/open-parquet-format.md` diagram labels the write buffer "logstore, durable"**, while the default engine is the legacy in-memory buffer. That doc overstates the default durability and is listed in the fix list.

### The #311 discrepancy: verdict

**Both statements are true; the repository audit read only the issue body.**
- The **issue body** (filed 2026-10-01T20:20Z) is narrow. Select-role pods PUT `stats-aggregate.json`, `snapshot.json` and `_label_index.json` on restart.
- The **follow-up comment** (2026-10-01T21:37Z, measured on main d72e0204 with a counting proxy per reader) widens it: "a `-lakehouse.role=select` replica is not read-only".
  - **It compacts.** Two select readers each PUT their own `compacted-L1-*.parquet` from the same 11 inputs (821,864 B and 821,858 B), and both DELETE the inputs. On the next round each reader merged both outputs again. "The partition then holds the same rows 2×, then up to 4×."
  - **Root cause.** `compaction.enabled` defaults to true for every role. Ownership falls back to self when no peer ring is configured. The conflict check sees only the local manifest. Compaction is a pure row union.
  - It also lists the sidecar PUT counts: 42 per logs reader and 25 per traces reader for `stats-aggregate.json`.

So the earlier statement that "select replicas also compact and duplicate rows 2-4×" is **correct and measured**. The issue is still OPEN. Every matrix cell here reflects the wider scope.

---

## Sources (accessed 2026-10-02)

### LH (repository audit)
- `ReliablyObserve/victoria-lakehouse` at `origin/main` c848bf2e, release v0.143.11: `README.md`, `docs/{durability,write-path,configuration,petabyte-scale-audit,cost-estimates,parity-and-gaps,open-parquet-format,deletion-strategy,scaling}.md`, `UPSTREAM_COVERAGE.md`, `tests/verification/matrix.md`, `tests/parity/known_failures.txt`, `tests/conformance/README.md`.
- Issues: #37, #245, #254, #265, #268, #272, #273, #279, #281, #285, #287, #289, #290, #294, #298, #299, #300, #301, #303, #306, #307, #308, #311 (body and the 2026-10-01T21:37Z comment), #316, #317, #318, #319, #320, #324. Draft PR #310.

### Re-verification fetches (the verification notes)
- https://github.com/openobserve/openobserve/blob/main/src/config/src/config.rs
- https://github.com/openobserve/openobserve/blob/main/src/ingester/src/writer.rs
- https://github.com/openobserve/openobserve/blob/main/src/wal/src/writer.rs
- https://github.com/openobserve/openobserve-helm-chart/blob/main/charts/openobserve/values.yaml and `charts/openobserve-standalone/values.yaml`
- https://github.com/parseablehq/parseable/blob/main/src/parseable/staging/writer.rs
- https://github.com/VictoriaMetrics/VictoriaLogs/issues/48 ; https://github.com/VictoriaMetrics/VictoriaLogs/pull/1155 ; VictoriaLogs releases v1.53.0, v1.52.1
- https://docs.victoriametrics.com/victorialogs/roadmap/
- https://grafana.com/docs/tempo/latest/operations/schema/
- https://grafana.com/docs/tempo/latest/reference-tempo-architecture/block-format/
- https://grafana.com/docs/tempo/latest/configuration/parquet/
- https://grafana.com/docs/tempo/latest/operations/dedicated_columns/
- https://github.com/grafana/tempo/blob/main/tempodb/backend/block_meta.go
- https://github.com/grafana/tempo/blob/main/docs/sources/tempo/configuration/_index.md
- https://grafana.com/docs/tempo/latest/configuration/hosted-storage/s3/
- https://raw.githubusercontent.com/grafana/loki/main/docs/sources/shared/configuration.md
- https://grafana.com/docs/loki/latest/configure/storage/
- https://www.elastic.co/docs/deploy-manage/tools/snapshot-and-restore/s3-repository
- https://www.snowflake.com/en/blog/observe-apache-iceberg-open-observability/
- https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/s3-tables-integration.html
- https://coralogix.com/docs/user-guides/data-query/archive-query/access-cx-data-directly/
- https://coralogix.com/docs/user-guides/data-flow/s3-archive/archive-retention-policy/
- https://docs.datadoghq.com/logs/log_configuration/archives/
- https://www.datadoghq.com/product/byoc-logs/
- https://docs.cribl.io/lake/datasets/
- https://www.grepr.ai/blog/store-logs-s3
- https://learn.microsoft.com/en-us/azure/sentinel/datalake/sentinel-lake-overview
- https://github.com/icegatetech/icegate (README and API)
- GitHub API for stars, licences and releases: openobserve/openobserve, parseablehq/parseable, grafana/tempo, grafana/loki, quickwit-oss/quickwit, ReliablyObserve/victoria-lakehouse

### Per-system primary pages (row default source)
