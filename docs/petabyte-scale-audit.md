# Petabyte-Scale Survival Audit

Target deployment: ~5 TB/day ingest, 5 PB total at rest across one bucket, ~50M parquet files over a 30d-hot / 36mo-cold retention split. ~150K new files per day. The cold-tier code paths must operate without per-file linear scans or per-file S3 round trips that wouldn't survive at that file count.

This audit identifies what does and doesn't scale. The recommendations are scoped to "would this break production at PB-scale", not general code-quality opinions.

## Must-fix — operation fails or unacceptable cost at PB-scale

1. **Manifest per-key lookups are O(n)** (`internal/manifest/manifest.go:570,585,601`).
   `SetFileBucket`, `UpdateFileColumnStats`, `EnrichFileMetadata` all iterate every partition × every file to find one key. At 50M files, a single enrichment call is 50M string comparisons. These are hot-path: enrichment during queries, bucket migration, column-stats population. Needs a per-key reverse index. ~2-3 days work + lock-contention analysis.

2. **`TenantSummaries` / `TenantSummariesInWindow` full-scan every call** (`internal/manifest/manifest.go:976`).
   Iterates the entire `m.files` map on every `/tenants` API request and every servicegraph tick. No cache; no incremental update. At 50M files this is unusably slow. Needs an incremental tenant-summary map invalidated by AddFile / RemoveFile. ~1-2 days.

3. **`KeysUnderPrefix` is O(n)** (`internal/manifest/manifest.go:717`).
   Used by orphan-sweep Tier B for every sweep interval. Full manifest scan. Needs a prefix trie / B-tree for O(log n + k) lookup. ~2 days.

4. **`updateLabelIndex` reads full parquet bodies at startup** (`internal/storage/parquets3/storage.go:481`).
   For promoted columns (service.name, severity_text, etc.) it calls `extractDistinctFromStats` / `sampleValueFrequency` which do data-page reads, not footer-only. At 50M files this is ~50M S3 GETs at startup. `WarmLabelIndex` samples only 10 files — the rest happens on-demand which is fine, but full-cluster restarts will be catastrophic. Need persistent label-index on disk + lazy load. ~3 days.

5. **`RefreshFromS3` lists the entire bucket when `{AccountID}` is in the prefix template** (`internal/manifest/manifest.go:164-165`).
   The current code sets `listPrefix = ""` and filters client-side. At 150 TB hot + multi-tenant, this is hundreds of `ListObjectsV2` pages every 30s — burns S3 API budget. Needs per-tenant prefix splits driven by a tenants-of-interest set. ~2 days.

## Should-fix — degraded but functional

6. **Footer cache fixed at 10,000 entries** (`internal/storage/parquets3/footer_cache.go:37`).
   At 5 KB/footer that's a 50 MB working set vs a 50M-file corpus. Typical 7-day query touches ~168 files so per-query hit rate is OK, but background scans (label-index rebuild, compaction) blow the cache out. Auto-size to ~0.1% of manifest file count, expose as config knob. ~half-day.

7. **Label index `LabelIndex.Add` grows unbounded per field** (`cache/persist.go:46`, value cap 10,000 per field).
   No eviction policy beyond the per-field cap. On a high-cardinality fleet (k8s.pod.name × host.name × pod restarts), distinct value count per field is far above 10K, so the cap just truncates randomly. Needs an LRU keyed by frequency. ~1 day.

8. **Manifest snapshot serialization is JSON** (`internal/manifest/manifest.go:769 SaveTo`).
   50M files × ~200 bytes = ~10 GB JSON snapshot. Bootstrap and rolling-update cost is substantial. Switch to a binary format with delta-encoded keys; or partition snapshots so each chunk is a few MB. ~3 days.

9. **Query-time file pruning has no pre-built tenant index** (file selection in `GetFilesForRange`).
   Footer prefetch + bloom filters catch the cross-tenant noise but only after opening the file. A pre-built `tenant → partitions` map would let the planner skip whole partitions without touching the files. ~1 day.

## Acceptable — verified to scale or operator-controllable

- **Partition-keyed file map** (`map[string][]FileInfo` keyed by `dt=YYYY-MM-DD/hour=HH`). With 30 days × 24 hours = 720 partitions, binary search via `sortedPartitions` is O(log 720) and per-partition scans average ~70K files. Range queries land in the right bucket cheaply.

- **Footer prefetch + bloom filter pruning** (`internal/storage/parquets3/footer_prefetch.go:35, storage_query.go:344`). A 7-day query reads ~168 × 16 KB = ~2.7 MB of footers vs. 168 × ~50 MB of full file content. This is the load-bearing optimization; without it nothing would scale.

- **`FileWorkers` semaphore on query concurrency** (`storage_query.go:309-318`). Per-query work is bounded by the K8s-style `s.bounds.FileWorkers` semaphore. Default 8, operator-tunable. No runaway fan-out.

- **Compaction is partition-scoped + HRW-sharded** (`internal/compaction/scheduler.go:259-260`). Scans `for partition, files := range allFiles` (720 partitions) then applies HRW ownership filter. Linear in partition count, not file count.

- **S3 concurrency is bounded** (`storage.go:63,318`, `footer_prefetch.go:143`). `s3DownloadsBound` and prefetch concurrency are operator-tunable. No hardcoded fan-out that would saturate the S3 API.

## What this means for a real PB deployment

A migration to 50M files on today's code hits three classes of failure simultaneously:

- **Latency spikes on every `/tenants` or admin call** because `TenantSummaries` does a full manifest scan.
- **Manifest mutation operations block queries** because key lookups during `SetFileBucket` / `EnrichFileMetadata` hold the manifest lock for tens of seconds while scanning 50M files.
- **Startup label-index rebuild becomes a multi-hour event** unless the index is persisted to disk and lazy-loaded.

Cost side: multi-tenant `RefreshFromS3` doing full-bucket lists every 30s would run $300-500/day in S3 LIST charges alone, on top of the actual workload. That's an immediate operator complaint.

Order of work I'd recommend:

1. Manifest per-key reverse index (must-fix #1)
2. RefreshFromS3 tenant scoping (must-fix #5)
3. Label index disk persistence (must-fix #4)
4. TenantSummaries incremental update (must-fix #2)

Items 1, 2, 4 are mostly independent and ~1 sprint of work. Item 3 is its own sprint. Total: 2 sprints to "would-survive-a-PB-customer" readiness.

What's NOT on the list: the actual storage / query path scales fine on the file-count axis. The blockers are all in metadata management.
