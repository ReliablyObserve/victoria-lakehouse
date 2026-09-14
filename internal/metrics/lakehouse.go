package metrics

// HTTP / RED metrics
var (
	HTTPRequestsTotal    = NewCounterVec("lakehouse_http_requests_total", "path")
	HTTPRequestDuration  = NewHistogram("lakehouse_http_request_duration_seconds", DefBuckets)
	HTTPErrorsTotal      = NewCounterVec("lakehouse_http_errors_total", "path")
	ConcurrentSelects    = NewGauge("lakehouse_concurrent_select_current")
	ConcurrentSelectsCap = NewGauge("lakehouse_concurrent_select_capacity")
	SlowQueriesTotal     = NewCounter("lakehouse_slow_queries_total")
)

// S3 metrics
var (
	S3RequestsTotal   = NewCounterVec("lakehouse_s3_requests_total", "op")
	S3RequestDuration = NewHistogram("lakehouse_s3_request_duration_seconds",
		[]float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5})
	S3ErrorsTotal     = NewCounterVec("lakehouse_s3_errors_total", "op")
	S3BytesReadTotal  = NewCounter("lakehouse_s3_bytes_read_total")
	S3ThrottleTotal   = NewCounter("lakehouse_s3_throttle_total")
	S3RangeReadsTotal = NewCounter("lakehouse_s3_range_reads_total")
	S3RangeBytesRead  = NewCounter("lakehouse_s3_range_bytes_read_total")
	S3BufferHits      = NewCounter("lakehouse_s3_buffer_hits_total")
	S3BufferMisses    = NewCounter("lakehouse_s3_buffer_misses_total")
	S3CoalescedRanges = NewCounter("lakehouse_s3_coalesced_ranges_total")
)

// S3 read-path observability (PR 2a, CH-pattern: prove/tune every read-path
// change before and after). These counters make the GET economics of the
// parquet scan path measurable per scenario — the full-scope bench snapshots
// them before/after each query class and reports the deltas.
var (
	// S3BufferWastedBytes counts bytes fetched into a BufferedS3ReaderAt
	// read-ahead window but never served to a caller before the window was
	// evicted (replaced by the next fetch). Waste is computed from the
	// high-water mark of bytes served out of the window, which matches the
	// forward-sequential access pattern the window exists for. A high
	// waste/fetch ratio means the read-ahead window is oversized for the
	// access pattern (e.g. needle queries on a scan-tuned window).
	S3BufferWastedBytes = NewCounter("lakehouse_s3_buffer_wasted_bytes_total")

	// S3CoalesceOverfetchBytes counts gap bytes fetched ONLY because two
	// requested ranges were merged by the coalescing reader (the bytes
	// between the ranges that nobody asked for). Together with
	// S3CoalescedRanges (merges that saved a round trip) this prices the
	// coalescing gap: overfetch-bytes paid per round-trip saved.
	S3CoalesceOverfetchBytes = NewCounter("lakehouse_s3_coalesce_overfetch_bytes_total")

	// S3ReadAheadGrows / S3ReadAheadResets / S3ReadAheadShrinks observe the
	// adaptive read-ahead window: grows ticks when 2+ consecutive
	// forward-sequential misses double the window (scan detected), resets
	// ticks when a random seek drops it back to the base window (needle
	// detected), shrinks ticks when waste feedback halves the window
	// because the evicted window's never-read ratio exceeded
	// s3.read_ahead_waste_threshold (sparse forward hops detected — the
	// pattern behind the measured 46 MB/query never-read fetch on
	// filtered scans).
	S3ReadAheadGrows   = NewCounter("lakehouse_s3_readahead_grow_total")
	S3ReadAheadResets  = NewCounter("lakehouse_s3_readahead_reset_total")
	S3ReadAheadShrinks = NewCounter("lakehouse_s3_readahead_shrink_total")

	// S3HeadBypassReads counts tiny reads at offset 0 (parquet's 4-byte
	// magic-header check) served via an exact-size ranged GET instead of
	// pulling a full read-ahead window. Each tick is ~one window of
	// head-waste avoided (previously a ~2 MB GET per cold open).
	S3HeadBypassReads = NewCounter("lakehouse_s3_head_bypass_reads_total")

	// S3GetsByPhase counts S3 GETs by read-path phase:
	//   open   — GETs issued while parquet.OpenFile parses magic+footer
	//            on a ranged open (the "serial open catastrophe" being
	//            eliminated; target is 0 once footers ride the cache)
	//   page   — GETs issued by page/column-chunk reads after the open
	//            (includes lazy parquet-internal page-index/bloom reads,
	//            which go through the same reader)
	//   footer — footer-cache fill GETs (prefetchFooters, fetchFooterFile,
	//            shouldSkipByFooter, inline footer fetch on open)
	//   bloom  — per-file `.bloom` sidecar GETs (checkFileBloom fallback)
	S3GetsByPhase = NewCounterVec("lakehouse_s3_gets_by_phase_total", "phase")

	// S3GetsPerOpen is the number of S3 GETs parquet.OpenFile needed for a
	// single ranged open (magic + footer-length + footer + any metadata
	// sections). The research-doc baseline is 4-6 serial GETs per open;
	// Skip*/FileSchema/head-bypass should drive the median toward 1-2 and
	// the zero-GET-open batch toward 0.
	S3GetsPerOpen = NewHistogram("lakehouse_s3_gets_per_open", nil)

	// S3MetaSingleflightDedup counts metadata GETs that were deduplicated
	// by singleflight (concurrent queries asking for the same object key
	// shared one in-flight GET). kind: footer | bloom | pmeta_bundle.
	S3MetaSingleflightDedup = NewCounterVec("lakehouse_s3_singleflight_dedup_total", "kind")

	// Plan-then-fetch observability (S3 Tier-2 items 8/9). The planned path
	// replaces the speculative read-ahead window on column-projected reads:
	// exact coalesced column-chunk ranges are fetched up-front (CH
	// Prefetcher / arrow-rs vectored per-RG pattern), eliminating the
	// measured ~46 MB/query of never-read window bytes on filtered counts.
	//
	// S3PlannedFetchesTotal counts armed plans (one per projected file
	// read); SpansTotal / BytesTotal divided by it give the per-fetch span
	// count and bytes on the wire — the numbers that replace the window
	// path's wasted-bytes accounting for projected reads.
	S3PlannedFetchesTotal    = NewCounter("lakehouse_s3_planned_fetches_total")
	S3PlannedFetchSpansTotal = NewCounter("lakehouse_s3_planned_fetch_spans_total")
	S3PlannedFetchBytesTotal = NewCounter("lakehouse_s3_planned_fetch_bytes_total")

	// S3PlannedOutOfPlanReads counts reads an ARMED planned view could not
	// serve from its fetched spans and passed through to the underlying
	// reader (exact-range GET). Steady-state this should be ~0; a non-zero
	// rate means the plan misses ranges parquet-go actually reads (e.g. a
	// lazily-loaded metadata section) — a tuning signal, never an error.
	S3PlannedOutOfPlanReads = NewCounter("lakehouse_s3_planned_out_of_plan_reads_total")

	// S3ProjectedFetchFallback counts projected reads that fell back from
	// the planned path to the adaptive-window path, by reason:
	//   cap       — the coalesced plan exceeded the absolute plan ceiling
	//               (fi.Size; defensive — the v2 slice-1 cap re-scope
	//               retired the old per-plan 16MB cap, so steady-state
	//               this is 0: spans are bounded per-SPAN by
	//               s3.planned_fetch_span_cap_bytes via splitting, and
	//               plan admission rides the memory ledger)
	//   no-footer — footer metadata unavailable (cache miss + footer
	//               fetch failed), so no plan could be derived
	//   error     — the ranged open or the span download failed
	S3ProjectedFetchFallback = NewCounterVec("lakehouse_s3_projected_fetch_fallback_total", "reason")

	// S3PlannedGapChoice counts the gap-discipline outcome per armed plan:
	// armProjectedPlan prices the plan at candidate gaps (64k | 256k | 1m)
	// with cost = RTT-waves + bytes/BW and fetches with the cheapest. The
	// distribution is the live check on the offline simulator's claim that
	// the gap barely matters once span concurrency is fixed.
	S3PlannedGapChoice = NewCounterVec("lakehouse_s3_planned_gap_choice_total", "gap")

	// S3PlannedStrategy counts the S* strategy decision per PLANNED-mode
	// projected open (slice 1d — the footer-cache-gated ladder):
	//   plan-warm-footer  — footer cached ⇒ plan immediately (zero
	//                       metadata RTTs)
	//   whole-file-warmup — cold footer + file under
	//                       s3.whole_file_threshold_bytes ⇒ ONE whole-file
	//                       GET through the smart-cache path; the download
	//                       IS the footer-cache warmup
	//   plan-cold-footer  — cold footer + large file ⇒ footer range-fetch
	//                       then plan exact spans
	S3PlannedStrategy = NewCounterVec("lakehouse_s3_planned_strategy_total", "strategy")
)

// Cache metrics
var (
	CacheHitsTotal         = NewCounterVec("lakehouse_cache_hits_total", "tier")
	CacheMissesTotal       = NewCounterVec("lakehouse_cache_misses_total", "tier")
	CacheMemoryBytes       = NewGauge("lakehouse_cache_memory_bytes")
	CacheDiskBytes         = NewGauge("lakehouse_cache_disk_bytes")
	CacheSingleflightDedup = NewCounter("lakehouse_cache_singleflight_dedup_total")
)

// Peer cache metrics
var (
	PeerRequestsTotal    = NewCounterVec("lakehouse_peer_requests_total", "op")
	PeerHitsTotal        = NewCounter("lakehouse_peer_hits_total")
	PeerRingMembers      = NewGauge("lakehouse_peer_ring_members")
	PeerBytesTransferred = NewCounterVec("lakehouse_peer_bytes_transferred_total", "direction")
	PeerErrorsTotal      = NewCounter("lakehouse_peer_errors_total")
)

// Manifest & discovery metrics
var (
	ManifestFiles           = NewGauge("lakehouse_manifest_files")
	ManifestBytes           = NewGauge("lakehouse_manifest_bytes")
	ManifestFastPathTotal   = NewCounter("lakehouse_manifest_fast_path_total")
	ManifestRefreshDuration = NewHistogram("lakehouse_manifest_refresh_duration_seconds",
		[]float64{0.1, 0.5, 1, 5, 10, 30, 60})
	// ManifestAddFileDuplicateKeyTotal ticks when AddFile is called twice
	// with the same (partition, key) — the idempotency guard skips the
	// second insert. Steady-state value should be 0; non-zero indicates
	// duplicate compaction work (HRW ring flap, DNS lag dual ownership)
	// or an upstream upload retry bug. See spec §8.1 R3 and §3.1 case 11.
	ManifestAddFileDuplicateKeyTotal = NewCounter("lakehouse_manifest_addfile_duplicate_key_total")

	// ManifestRefreshCliffGuardRejections counts times the
	// cliff-guard in RefreshFromS3 rejected a refresh because the
	// new file count was <50% of the previous. Operationally this
	// is the user-visible "Jaeger Cold suddenly returning 0 results"
	// alarm bell — a single tick means a transient S3 LIST hiccup
	// was successfully masked; persistent ticks mean the bucket
	// genuinely shrank and the guard is now lying to readers, so
	// an operator should restart the pod to force a clean rebuild.
	ManifestRefreshCliffGuardRejections = NewCounter("lakehouse_manifest_refresh_cliff_guard_rejections_total")
	// ManifestRetiredKeys is the number of keys the manifest deliberately
	// stopped listing whose objects may still exist (a publish replaced them,
	// an output was abandoned, or they were removed on another component's
	// behalf). The refresh does not adopt them. It drains as their deletes land;
	// a value that only grows means deletes are failing.
	ManifestRetiredKeys = NewGauge("lakehouse_manifest_retired_keys")
	// ManifestRefreshSkipped counts listed objects the refresh kept out of the
	// manifest (reason=retired|pending) and tracked files it kept although the
	// listing lacked them because they were published while it ran
	// (reason=published_during_listing).
	ManifestRefreshSkipped = NewCounterVec("lakehouse_manifest_refresh_skipped_total", "reason")
	// ManifestRetiredEvicted counts retired keys forgotten by the age or size
	// bound (reason=ttl|cap|cap_delete_owed|cap_delete_landed) rather than by a
	// listing proving the object gone. Should stay 0: an evicted key whose
	// object still exists is adopted again, and so is one whose object a
	// still-running listing read before the delete.
	ManifestRetiredEvicted = NewCounterVec("lakehouse_manifest_retired_evicted_total", "reason")
	// ManifestRetiredReclaimed / ManifestRetiredReclaimErrors count the retry
	// deletes of superseded and abandoned objects whose first delete failed.
	ManifestRetiredReclaimed     = NewCounter("lakehouse_manifest_retired_reclaimed_total")
	ManifestRetiredReclaimErrors = NewCounter("lakehouse_manifest_retired_reclaim_errors_total")
	// ManifestRetiredReclaimOwed is the part of the retired set whose objects
	// this process still owes a delete for — the keys whose eviction would let
	// a refresh serve them again. Drains as the deletes land.
	ManifestRetiredReclaimOwed = NewGauge("lakehouse_manifest_retired_delete_owed")
	// ManifestRetiredDeleteLanded is the part of the retired set whose objects
	// are already deleted and which is held only so a listing that began before
	// the delete cannot adopt them back. Drains at the next accepted refresh, so
	// it tracks one refresh interval of deletes; a value that keeps growing
	// means refreshes are not being accepted.
	ManifestRetiredDeleteLanded = NewGauge("lakehouse_manifest_retired_delete_landed")
	// ManifestHeldKeys is the number of registered files another publish may
	// not supersede yet because the rewrite that swapped them in has not
	// recorded that swap durably. Steady state 0.
	ManifestHeldKeys = NewGauge("lakehouse_manifest_held_keys")
	// ManifestKeyClaimRejected counts refused key claims and publishes by
	// reason (registered, retired, pending, publish_key_taken) — a non-zero
	// value means two writers generated the same object key.
	ManifestKeyClaimRejected    = NewCounterVec("lakehouse_manifest_key_claim_rejected_total", "reason")
	DiscoveryHotBoundaryDays    = NewFloatGauge("lakehouse_discovery_hot_boundary_days")
	DiscoveryGapDays            = NewFloatGauge("lakehouse_discovery_hot_boundary_gap_days")
	ManifestPushTotal           = NewCounter("lakehouse_manifest_push_total")
	ManifestPushPeers           = NewGauge("lakehouse_manifest_push_peers")
	ManifestPushErrorsTotal     = NewCounter("lakehouse_manifest_push_errors_total")
	ManifestUpdateReceivedTotal = NewCounter("lakehouse_manifest_update_received_total")

	// ManifestTenantBucketListErrors counts failed LISTs of a tenant's
	// dedicated bucket during a manifest refresh, by bucket. One failing
	// bucket fails the whole refresh — the manifest then keeps serving its
	// previous state rather than dropping that tenant's objects — so a
	// non-zero rate means the fleet's view of S3 is frozen until the bucket
	// is reachable again. The series of every registered dedicated bucket is
	// created at zero by Manifest.SetTenantBuckets.
	ManifestTenantBucketListErrors = NewCounterVec("lakehouse_manifest_tenant_bucket_list_errors_total", "bucket")
)

// RowGroupSkipReasons is every reason ParquetRowGroupsSkipped is incremented
// with on either binary: the manifest-level file pre-filters (label_index,
// column_stats), the footer-only file skip (footer_prefetch) and the
// row-group checks (stats = time range, bloom, pushdown, token_bloom).
// TestRowGroupSkipReasons_MatchCallSites keeps the list and the call sites in
// step.
var RowGroupSkipReasons = []string{"label_index", "column_stats", "footer_prefetch", "stats", "bloom", "pushdown", "token_bloom"}

func init() {
	// Export every reason from process start. Which stage prunes a query
	// depends on the objects it selects, so a series that only appeared on
	// its first skip could be missing for a long time on a tenant-scoped
	// read that never reaches that stage.
	ParquetRowGroupsSkipped.Init(RowGroupSkipReasons...)
}

// Parquet engine metrics
var (
	ParquetRowGroupsScanned = NewCounter("lakehouse_parquet_row_groups_scanned_total")
	ParquetRowGroupsSkipped = NewCounterVec("lakehouse_parquet_row_groups_skipped_total", "reason")

	// pmeta field/value catalog (--pmeta). CatalogValueLookups{source} is the
	// catalog-vs-scan hit rate that proves the dropdown speedup; ResidentBytes is
	// the RAM guardrail.
	CatalogValueLookups  = NewCounterVec("lakehouse_catalog_value_lookups_total", "source") // catalog|scan
	CatalogResidentBytes = NewGauge("lakehouse_catalog_resident_bytes")
	// CatalogFieldCardinality is the HLL-estimated distinct-count per high-card
	// field — the cardinality-bomb early-warning (alert when an id-like field's
	// count spikes) and a query-planning input.
	CatalogFieldCardinality = NewGaugeVec("lakehouse_catalog_field_cardinality", "field")
	ParquetBloomChecks      = NewCounterVec("lakehouse_parquet_bloom_checks_total", "result")
	ParquetColumnBytesRead  = NewCounter("lakehouse_parquet_column_bytes_read_total")
	ParquetFilesOpened      = NewCounter("lakehouse_parquet_files_opened_total")
	ParquetFilesSkipped     = NewCounter("lakehouse_parquet_files_skipped_bloom_total")
	FooterCacheHits         = NewCounter("lakehouse_footer_cache_hits_total")
	FooterCacheEvictions    = NewCounter("lakehouse_footer_cache_evictions_total")
	FooterCacheEntries      = NewGauge("lakehouse_footer_cache_entries")
	// FooterParseRejected counts ParseFooterFromBytes inputs rejected by
	// its pre-parse validation (invalid/oversized footer length, bad
	// magic, decoder panic recovered), broken out by reason so a
	// malformed/corrupted file — or a legitimate file that outgrew the
	// footer-length policy cap — is visible instead of silently falling
	// back to a full download.
	FooterParseRejected    = NewCounterVec("lakehouse_footer_parse_rejected_total", "reason")
	TraceIDCacheHits       = NewCounter("lakehouse_trace_id_cache_hits_total")
	MetadataOnlyFiles      = NewCounter("lakehouse_metadata_only_files_total")
	QueryFileNotFoundTotal = NewCounter("lakehouse_query_file_not_found_total")
	QueryFileErrorsTotal   = NewCounter("lakehouse_query_file_errors_total")

	// LogsTraceShapedRowsDropped counts rows dropped from
	// LogsProfile query results because their stream tags identify
	// them as trace spans (VT-style `resource_attr:` prefix or
	// `name="..."` partition key) rather than logs. The drop matches
	// what VL upstream's stream-fields enforcement does at write
	// time, so query results stay consistent across tiers. A
	// non-zero rate indicates pre-existing data quality issue in
	// the cold tier (task #70 territory) — operators can correlate
	// the rate with compaction/cleanup runs.
	LogsTraceShapedRowsDropped = NewCounter("lakehouse_logs_trace_shaped_rows_dropped_total")

	// LogsTraceShapedRowsDroppedAtIngest counts rows refused by the
	// insert path because their stream tags marked them as VT data
	// (spans or service-graph rows) reaching the logs ingest. The
	// drop is irreversible — the row never lands in parquet, and the
	// manifest RowCount (which manifestFastPath uses to answer
	// `* | stats count()`) stops including them. The query-time
	// counterpart `LogsTraceShapedRowsDropped` is still incremented
	// for historical files written before this gate landed; once
	// retention rolls those off, both counters should track in
	// lockstep at zero.
	LogsTraceShapedRowsDroppedAtIngest = NewCounter("lakehouse_logs_trace_shaped_rows_dropped_at_ingest_total")

	// LogsTraceShapedRowsDroppedAtCompaction counts trace-shape
	// rows the compactor stripped from a merge output. Together
	// with the ingest-side counter this finishes the cleanup loop:
	// the ingest gate stops new writes, compaction migrates
	// historical bad rows out of the manifest's RowCount as files
	// roll forward. Eventually all three trace-shape counters
	// (read-side, ingest, compaction) sit at zero with no inflated
	// counts left.
	LogsTraceShapedRowsDroppedAtCompaction = NewCounter("lakehouse_logs_trace_shaped_rows_dropped_at_compaction_total")

	// LogsSeverityTextBackfilledAtCompaction counts rows whose
	// SeverityText was empty in the source parquet but recoverable
	// from severity_number (via VL upstream FormatSeverity) or the
	// stream-tag `level` value (via VL upstream StreamTags.Get).
	// Each compaction pass heals historical files that pre-date the
	// insert-time fallback. The counter falls to zero once all
	// affected files have been re-emitted by compaction, at which
	// point query-time `level=""` buckets on cold mirror VL hot's
	// own zero-empty-bucket behavior.
	LogsSeverityTextBackfilledAtCompaction = NewCounter("lakehouse_logs_severity_text_backfilled_at_compaction_total")
)

// Insert / writer metrics
var (
	InsertRowsTotal        = NewCounter("lakehouse_insert_rows_total")
	InsertRowsBuffered     = NewGauge("lakehouse_insert_rows_buffered")
	InsertBytesBuffered    = NewGauge("lakehouse_insert_bytes_buffered")
	InsertFlushTotal       = NewCounter("lakehouse_insert_flush_total")
	InsertFlushErrorsTotal = NewCounter("lakehouse_insert_flush_errors_total")
	// InsertFlushWatermarkNs is the BufferFlusher's last committed flush
	// watermark (ns). Everything at or below it is durably on S3; the buffer
	// covers (watermark, now]. Crash-survival tests assert against this boundary.
	InsertFlushWatermarkNs = NewGauge("lakehouse_insert_flush_watermark_timestamp")
	InsertFlushDuration    = NewHistogram("lakehouse_insert_flush_duration_seconds",
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10})
	InsertBytesUploaded    = NewCounter("lakehouse_insert_bytes_uploaded_total")
	InsertPartitionsActive = NewGauge("lakehouse_insert_partitions_active")

	// VT emits internal "index" log rows alongside span data (trace-ID index
	// stream and service-graph stream). Lakehouse drops them at insert time
	// since they aren't OTLP span data; this counter, keyed by kind, exposes
	// how many we discard so a missing-trace-index regression is visible.
	VTInternalRowsDropped = NewCounterVec("lakehouse_vt_internal_rows_dropped_total", "kind")

	// BufferStoreDualWriteFailures counts batches the Option B logstorage-native
	// buffer (BufferEngine=logstore) failed to accept. The dual-write is
	// isolated with recover() so a buffer failure can NEVER break ingestion —
	// the legacy staging path remains authoritative. A non-zero value means the
	// buffer is missing recent rows and any buffer-served query may under-return
	// until the next healthy flush; alert on rate > 0.
	BufferStoreDualWriteFailures = NewCounter("lakehouse_buffer_store_dualwrite_failures_total")

	// Option B P5 shadow export: the buffer→Parquet path runs in parallel with
	// the authoritative legacy flush, writing to a SHADOW S3 prefix (not the
	// manifest), so an operator can confirm row/byte parity vs the legacy
	// Parquet before the cutover. Compare BufferShadowExportRows against the
	// legacy insert row rate; BufferShadowExportErrors must stay flat at 0.
	BufferShadowExportRows   = NewCounter("lakehouse_buffer_shadow_export_rows_total")
	BufferShadowExportFiles  = NewCounter("lakehouse_buffer_shadow_export_files_total")
	BufferShadowExportBytes  = NewCounter("lakehouse_buffer_shadow_export_bytes_total")
	BufferShadowExportErrors = NewCounter("lakehouse_buffer_shadow_export_errors_total")

	// TraceIndexLookups counts VT-format trace-by-ID lookups served from the
	// embedded `_trace_idx` Parquet footer index. `result` is one of:
	//   hit   — index served the (start_time, end_time) bounds; no span scan
	//   miss  — index does not know this trace; caller falls back to scan
	//   error — footer fetch or unmarshal failed
	// Compare hit/miss to know how cold the lookup path runs without the
	// index — every miss is a full span-scan trace-by-ID.
	TraceIndexLookups = NewCounterVec("lakehouse_trace_index_lookups_total", "result")

	// TraceIdxPreFilterFiles records the file-narrowing efficiency of
	// filterFilesByTraceIdx (the pre-filter that runs after the bloom
	// narrowing on every trace_id:in(...) query). `result` is:
	//   dropped     — footer's _trace_idx KV parsed and the trace IDs
	//                 listed did NOT include any queried ID; file
	//                 skipped without a row scan
	//   kept_match  — footer's _trace_idx KV listed at least one
	//                 queried ID; file scanned
	//   kept_unindexed — file lacks _trace_idx KV entirely; conservatively
	//                 scanned (older parquets pre-date the index)
	//   kept_error  — footer fetch failed; conservatively scanned
	// dropped / (dropped + kept_match) is the pre-filter's selectivity
	// — at PB scale this should be > 0.9 for stale trace IDs.
	TraceIdxPreFilterFiles = NewCounterVec("lakehouse_trace_idx_prefilter_files_total", "result")
)

// Prefetch metrics
var (
	PrefetchTasksTotal = NewCounterVec("lakehouse_prefetch_tasks_total", "type")
	PrefetchHitsTotal  = NewCounter("lakehouse_prefetch_hits_total")
	PrefetchBytesTotal = NewCounter("lakehouse_prefetch_bytes_total")
)

// Smart cache metrics
var (
	SmartCacheHitRatio         = NewFloatGauge("lakehouse_cache_hit_ratio")
	SmartCacheEntriesTotal     = NewGauge("lakehouse_cache_entries_total")
	SmartCacheBytesUsed        = NewGauge("lakehouse_cache_bytes_used")
	SmartCacheBytesLimit       = NewGauge("lakehouse_cache_bytes_limit")
	SmartCacheEvictionsTotal   = NewCounterVec("lakehouse_cache_evictions_total", "reason")
	SmartCacheHotEntries       = NewGauge("lakehouse_cache_hot_entries")
	SmartCachePinnedEntries    = NewGauge("lakehouse_cache_pinned_entries")
	SmartCacheRecommendedBytes = NewGaugeVec("lakehouse_cache_recommended_bytes", "method")
	SmartCacheCoverageHours    = NewFloatGauge("lakehouse_cache_coverage_hours")
	SmartCachePrefetchHitRatio = NewFloatGauge("lakehouse_cache_prefetch_hit_ratio")
	SmartCacheOwnedEntries     = NewGauge("lakehouse_cache_owned_entries")
	SmartCacheOwnedBytes       = NewGauge("lakehouse_cache_owned_bytes")
	SmartCachePeerServedTotal  = NewCounter("lakehouse_cache_peer_served_total")
	SmartCacheEffectiveBytes   = NewGauge("lakehouse_cache_effective_bytes")
)

// Cross-signal eviction metrics
var (
	CrossEvictionSent     = NewCounter("lakehouse_cache_cross_eviction_sent_total")
	CrossEvictionReceived = NewCounter("lakehouse_cache_cross_eviction_received_total")
	CrossEvictionPending  = NewGauge("lakehouse_cache_cross_eviction_pending")
	CrossEvictionApplied  = NewCounter("lakehouse_cache_cross_eviction_applied_total")
	CrossPrefetchSent     = NewCounter("lakehouse_cache_cross_prefetch_sent_total")
	CrossPrefetchReceived = NewCounter("lakehouse_cache_cross_prefetch_received_total")
)

// AZ-aware routing metrics
var (
	PeerSameAZMembers           = NewGauge("lakehouse_peer_same_az_members")
	PeerCrossAZMembers          = NewGauge("lakehouse_peer_cross_az_members")
	PeerAZRequestsTotal         = NewCounterVec("lakehouse_peer_az_requests_total", "az_type")
	BufferBridgeAZRequestsTotal = NewCounterVec("lakehouse_buffer_bridge_az_requests_total", "az_type")
)

// Shutdown lifecycle metrics
var (
	ShutdownPhaseDuration = NewHistogram("lakehouse_shutdown_phase_duration_seconds",
		[]float64{0.1, 0.5, 1, 2, 5, 10, 15, 30, 45, 60})
	ShutdownPhaseActive = NewGaugeVec("lakehouse_shutdown_phase_active", "phase")
	ShutdownFlushRows   = NewCounter("lakehouse_shutdown_flush_rows_total")
	ShutdownSuccess     = NewGauge("lakehouse_shutdown_success")
)

// Startup lifecycle metrics
var (
	StartupPhaseDuration = NewHistogram("lakehouse_startup_phase_duration_seconds",
		[]float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300})
	StartupStalePVDetected   = NewGauge("lakehouse_startup_stale_pv_detected")
	StartupStalenessHours    = NewFloatGauge("lakehouse_startup_staleness_hours")
	StartupWALReconciledRows = NewCounter("lakehouse_startup_wal_reconciled_rows")
	StartupCacheInvalidated  = NewCounter("lakehouse_startup_cache_invalidated_entries")
)

// Ring change metrics
var (
	RingChangeEventsTotal   = NewCounterVec("lakehouse_ring_change_events_total", "type")
	RingStabilizeInProgress = NewGauge("lakehouse_ring_stabilize_in_progress")
	RingPeersTotal          = NewGauge("lakehouse_ring_peers_total")
)

// Query continuity during scaling
var (
	QueryPeerErrorsTotal      = NewCounterVec("lakehouse_query_peer_errors_total", "type")
	BufferBridgeFallbackTotal = NewCounter("lakehouse_buffer_bridge_fallback_total")
)

// Bloom index metrics
var (
	BloomBuildTotal      = NewCounterVec("lakehouse_bloom_build_total", "trigger")
	BloomBuildErrors     = NewCounter("lakehouse_bloom_build_errors_total")
	BloomEntriesTotal    = NewCounter("lakehouse_bloom_entries_total")
	BloomBytesMemory     = NewGauge("lakehouse_bloom_bytes_memory")
	BloomQueriesTotal    = NewCounterVec("lakehouse_bloom_queries_total", "result")
	BloomFilesSkipped    = NewCounter("lakehouse_bloom_files_skipped_total")
	BloomBytesAvoided    = NewCounter("lakehouse_bloom_bytes_avoided_total")
	BloomTierPartitions  = NewGaugeVec("lakehouse_bloom_tier_partitions", "tier")
	BloomTierTransitions = NewCounterVec("lakehouse_bloom_tier_transitions_total", "transition")
	BloomConfigSyncTotal = NewCounter("lakehouse_bloom_config_sync_total")
	BloomConfigSyncError = NewCounter("lakehouse_bloom_config_sync_errors_total")
	BloomControllerAdj   = NewCounterVec("lakehouse_bloom_controller_adjustments_total", "parameter")
)

// Startup & health metrics
var (
	StartupPhase        = NewGauge("lakehouse_startup_phase")
	StartupTotalSeconds = NewFloatGauge("lakehouse_startup_total_seconds")
	Ready               = NewGauge("lakehouse_ready")

	// ServingReady is 1 once the pod accepts queries — disk
	// recovery complete + WAL replay done (if insert) + manifest
	// holds at least cfg.Startup.MinManifestFiles. Distinct from
	// `Ready` because Ready also requires background warmup.
	// k8s readiness probes / vtselect fan-out can read this
	// directly when scraping vs hitting /ready.
	ServingReady = NewGauge("lakehouse_serving_ready")

	// WarmupComplete is 1 once the background goroutine that
	// runs S3 refresh + cache warmup + bloom backfill finishes.
	// Strict load balancers can wait for this.
	WarmupComplete = NewGauge("lakehouse_warmup_complete")

	// ManifestSnapshotAgeSeconds is the wall-clock age of the
	// most recent successful local snapshot write. Operators see
	// when their pod is running on a stale snapshot — relevant
	// during long downtime + first-after-resume restart, where
	// the disk recovery loads a 1-hour-old snapshot and queries
	// during background warmup miss whatever was written in that
	// hour. Updated on every successful SaveTo.
	ManifestSnapshotAgeSeconds = NewFloatGauge("lakehouse_manifest_snapshot_age_seconds")

	// MinManifestFilesGate exposes the configured readiness
	// threshold so operators can see what their pod is gating on.
	// Reads cfg.Startup.MinManifestFiles at startup.
	MinManifestFilesGate = NewGauge("lakehouse_min_manifest_files_gate")
)

// Query metrics
var (
	QueryDuration             = NewHistogram("lakehouse_query_duration_seconds", DefBuckets)
	QueryRowsTotal            = NewCounter("lakehouse_query_rows_returned_total")
	QueryRejectedTotal        = NewCounter("lakehouse_query_rejected_total")
	QueryFileLimitExceeded    = NewCounter("lakehouse_query_file_limit_exceeded_total")
	QueryMemoryBudgetExceeded = NewCounter("lakehouse_query_memory_budget_exceeded_total")

	// TenantScopeViolations counts objects/rows the read path selected that do
	// NOT belong to the requesting tenant. The guard drops them before they can
	// reach a response, so a non-zero value is a defect signal (manifest key
	// shape drift, a new call site that bypassed the scoped file lookup, or a
	// peer answering the buffer bridge without tenant scoping), never routine.
	// {site} names the query path that tripped it.
	TenantScopeViolations = NewCounterVec("lakehouse_tenant_scope_violations_total", "site")

	// GlobalReadQueriesTotal counts select requests that presented a valid
	// global-read credential and were therefore answered across every tenant.
	GlobalReadQueriesTotal = NewCounter("lakehouse_global_read_queries_total")
)

// Compaction metrics
var (
	CompactionRunsTotal         = NewCounter("lakehouse_compaction_runs_total")
	CompactionFilesInputTotal   = NewCounter("lakehouse_compaction_files_input_total")
	CompactionFilesOutputTotal  = NewCounter("lakehouse_compaction_files_output_total")
	CompactionBytesReadTotal    = NewCounter("lakehouse_compaction_bytes_read_total")
	CompactionBytesWrittenTotal = NewCounter("lakehouse_compaction_bytes_written_total")
	CompactionRowsMergedTotal   = NewCounter("lakehouse_compaction_rows_merged_total")
	CompactionDuration          = NewHistogram("lakehouse_compaction_duration_seconds",
		[]float64{0.1, 0.5, 1, 5, 10, 30, 60, 120})
	CompactionErrorsTotal  = NewCounter("lakehouse_compaction_errors_total")
	CompactionSkippedTotal = NewCounterVec("lakehouse_compaction_skipped_total", "reason")

	// CompactionCompressionRatio is bytes_read / bytes_written for a
	// single compactGroup call, observed per `output_level`. The
	// progressive zstd schedule (default [3, 7, 11] in
	// CompactionConfig.CompressionLevelByOutputLevel) means L1 outputs
	// are cheap-and-bulky while L3 outputs are dense. Operators tuning
	// the schedule should see the median at L0 around 1.0-1.2x
	// (re-flushed input ≈ output), L1 around 1.3-1.6x, and L3 around
	// 2.5x+ on real workloads. A regression here is the canary that
	// progressive compression has stopped engaging (typical cause: a
	// per-tenant override pinning level=0 everywhere). Buckets are
	// chosen to span the L0→L3 envelope and the 5x+ ceiling for the
	// rare hot-spot file with very repetitive data (e.g. health probe
	// floods).
	CompactionCompressionRatio = NewHistogramVec(
		"lakehouse_compaction_compression_ratio",
		[]float64{1.0, 1.2, 1.5, 2, 2.5, 3, 4, 5, 7, 10},
		"output_level")

	// CompactionCompressionLevelUsed records which zstd level the
	// compactor actually picked per output_level. The chosen level
	// can come from any of: per-tenant override > global schedule >
	// pre-progressive default. Letting both labels float lets a
	// dashboard answer "what level is tenant X getting at L2?" via
	// a single line — useful when a tenant complains its compaction
	// is too slow or too sparse and the operator suspects the
	// override is misconfigured.
	CompactionCompressionLevelUsed = NewGaugeVec(
		"lakehouse_compaction_compression_level_used",
		"output_level")
)

// Writer-artifact build metrics — what the flusher pays so the reader
// can stay cheap. Every counter here pairs with a query-side speedup;
// e.g. a missing
// .bloom sidecar would surface as ParquetFilesSkipped staying at zero
// while WriterBloomBuildsTotal also stays at zero, instantly pointing
// at the broken side.
var (
	// WriterBloomBuildsTotal counts calls to extractLogBloomValues +
	// extractTraceBloomValues per flush partition+tenant. The
	// rate should track InsertRowsTotal — a divergence means
	// bloomObserver.OnFileFlush isn't running for some path.
	WriterBloomBuildsTotal = NewCounterVec(
		"lakehouse_writer_bloom_builds_total", "mode")

	// WriterBloomValuesTotal sums the (field, distinct-value) count
	// emitted per file. A spike means a tenant's cardinality blew up
	// (think a bug spraying unique values into k8s.pod.name); it
	// shows here long before the bloom sidecar exceeds
	// bloomindex.maxBloomCardinality and silently turns the bloom
	// into a coarser file-level pass-through.
	WriterBloomValuesTotal = NewCounterVec(
		"lakehouse_writer_bloom_values_total", "mode")

	// WriterBloomBuildDuration is the wall-clock cost of building
	// the bloom for one flush. Buckets cover sub-millisecond up to
	// 5s; the median should be a few ms for typical batch sizes.
	WriterBloomBuildDuration = NewHistogram(
		"lakehouse_writer_bloom_build_duration_seconds",
		[]float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5})

	// WriterLabelExtractionsTotal counts calls to
	// extractLogLabels / extractTraceLabels per flush. Pairs with
	// the inverted label index in §4A — every flush MUST run this
	// or the new file lands with Labels=nil and silently relies
	// on the conservative "include unindexed" branch added in
	// be8c126.
	WriterLabelExtractionsTotal = NewCounterVec(
		"lakehouse_writer_label_extractions_total", "mode")

	// WriterLabelValuesTotal sums distinct (field, value) tuples
	// per file. Same blow-up canary as WriterBloomValuesTotal but
	// for the inverted label index instead of the bloom.
	WriterLabelValuesTotal = NewCounterVec(
		"lakehouse_writer_label_values_total", "mode")

	// WriterLabelExtractionDuration is the wall-clock cost of
	// extractLogLabels / extractTraceLabels per flush.
	WriterLabelExtractionDuration = NewHistogram(
		"lakehouse_writer_label_extraction_duration_seconds",
		[]float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1})

	// WriterTraceIdxBuildsTotal counts calls to computeTraceIndex.
	// Traces-only. Should track the trace-side InsertRowsTotal.
	WriterTraceIdxBuildsTotal = NewCounter(
		"lakehouse_writer_trace_idx_builds_total")

	// WriterTraceIdxEntriesTotal sums the per-flush trace-ID count
	// going into the `_trace_idx` footer KV. Lets operators see
	// the cardinality going into the KV without parsing it back
	// out of S3.
	WriterTraceIdxEntriesTotal = NewCounter(
		"lakehouse_writer_trace_idx_entries_total")

	// WriterTraceIdxBuildDuration is the wall-clock cost of one
	// computeTraceIndex + marshalTraceIndex pass.
	WriterTraceIdxBuildDuration = NewHistogram(
		"lakehouse_writer_trace_idx_build_duration_seconds",
		[]float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1})
)

// Election-free compaction metrics (spec §6 + §11.5).
//
// Spec §6.1: Ownership / HRW metrics. The PartitionsOwned gauge is the
// load-bearing observability invariant — sum across cluster must equal
// lakehouse_storage_partitions_total. OwnershipSelfInPeers pages
// operators when 0 for 5m. DeferredStabilizing counts how often the
// scheduler skipped a tick because of ring instability; the
// SweepDeferredStabilizing counterpart is per-tier (label="tier_a"|"tier_b").
// OwnershipEmptyPeers ticks when discovery returned []; RingChangesTotal
// is labeled by RingChangeType ("join"/"leave").
//
// Spec §6.1: Orphan-sweep metrics. StolenTotal counts Tier A successes;
// OrphansDeleted is the Tier B counter (>100/h pages); OrphansSkipped is
// labeled by reason ("in_manifest"/"too_young"/"protected_prefix"/
// "not_parquet"/"manifest_drift_race"/"empty_peers"/"self_not_in_peers");
// DualOwnershipTotal is labeled by partition (cardinality bounded to a
// handful of bad partitions in steady state; 0 expected); DoubleCountWindow
// is the L0+L1 overlap gauge.
//
// Spec §11.5: HPA-visibility metrics. PartitionsInFlight + Draining
// gauges are per-pod; AbortedDuringDrain + OwnershipChanges +
// OrphanFilesFromDrain are counters; InFlightDuration is a histogram
// for terminationGracePeriodSeconds tuning. DeferredRingThrash counts
// rate-gate trips (spec §11.4).
var (
	CompactionPartitionsOwned          = NewGauge("lakehouse_compaction_partitions_owned")
	CompactionOwnershipSelfInPeers     = NewGauge("lakehouse_compaction_ownership_self_in_peers")
	CompactionDeferredStabilizing      = NewCounter("lakehouse_compaction_deferred_stabilizing_total")
	CompactionSweepDeferredStabilizing = NewCounterVec("lakehouse_compaction_sweep_deferred_stabilizing_total", "tier")
	CompactionOwnershipEmptyPeers      = NewCounter("lakehouse_compaction_ownership_empty_peers_total")
	CompactionRingChangesTotal         = NewCounterVec("lakehouse_compaction_ring_changes_total", "type")

	CompactionStolenTotal        = NewCounter("lakehouse_compaction_stolen_total")
	CompactionOrphansDeleted     = NewCounter("lakehouse_compaction_orphan_files_deleted_total")
	CompactionOrphansSkipped     = NewCounterVec("lakehouse_compaction_orphans_skipped_total", "reason")
	CompactionDualOwnershipTotal = NewCounterVec("lakehouse_compaction_dual_ownership_total", "partition")
	CompactionDoubleCountWindow  = NewGauge("lakehouse_compaction_double_count_window_seconds")

	// §11.5 HPA-visibility (PR-B scope per spec, wired in Stage 5
	// here because the scheduler/sweep need to set them as they run).
	CompactionPartitionsInFlight   = NewGauge("lakehouse_compaction_partitions_in_flight")
	CompactionDraining             = NewGauge("lakehouse_compaction_draining")
	CompactionAbortedDuringDrain   = NewCounter("lakehouse_compaction_aborted_during_drain_total")
	CompactionOwnershipChanges     = NewCounter("lakehouse_compaction_ownership_changes_total")
	CompactionInFlightDuration     = NewHistogram("lakehouse_compaction_in_flight_duration_seconds", DefBuckets)
	CompactionOrphanFilesFromDrain = NewCounter("lakehouse_compaction_orphan_files_from_drain_total")
	CompactionDeferredRingThrash   = NewCounter("lakehouse_compaction_deferred_ring_thrash_total")
)

// Tenant metrics (per-tenant, subject to cardinality cap)
var (
	TenantFiles               = NewGaugeVec("lakehouse_tenant_files", "tenant")
	TenantBytes               = NewGaugeVec("lakehouse_tenant_bytes", "tenant")
	TenantRawBytes            = NewGaugeVec("lakehouse_tenant_raw_bytes", "tenant")
	TenantRowsTotal           = NewCounterVec("lakehouse_tenant_rows_total", "tenant")
	TenantIngestionBytesTotal = NewCounterVec("lakehouse_tenant_ingestion_bytes_total", "tenant")
	TenantQueriesTotal        = NewCounterVec("lakehouse_tenant_queries_total", "tenant")
	TenantLastWriteTimestamp  = NewGaugeVec("lakehouse_tenant_last_write_timestamp", "tenant")
	TenantLastQueryTimestamp  = NewGaugeVec("lakehouse_tenant_last_query_timestamp", "tenant")
)

// Global storage metrics
var (
	StorageFilesTotal       = NewGauge("lakehouse_storage_files_total")
	StorageBytesTotal       = NewGauge("lakehouse_storage_bytes_total")
	StorageRawBytesTotal    = NewGauge("lakehouse_storage_raw_bytes_total")
	StorageCompressionRatio = NewFloatGauge("lakehouse_storage_compression_ratio")
	StorageRowsTotal        = NewGauge("lakehouse_storage_rows_total")
	StorageAvgRowBytes      = NewGauge("lakehouse_storage_avg_row_bytes")
	StoragePartitionsTotal  = NewGauge("lakehouse_storage_partitions_total")
	StorageOldestData       = NewGauge("lakehouse_storage_oldest_data_seconds")
	StorageNewestData       = NewGauge("lakehouse_storage_newest_data_seconds")
	StorageTenantsTotal     = NewGauge("lakehouse_storage_tenants_total")
	StorageBytesByClass     = NewGaugeVec("lakehouse_storage_bytes_by_class", "class")
	StorageFilesByClass     = NewGaugeVec("lakehouse_storage_files_by_class", "class")
	StorageCostMonthlyUSD   = NewFloatGauge("lakehouse_storage_cost_monthly_usd")
	StorageCostByClassUSD   = NewFloatGaugeVec("lakehouse_storage_cost_by_class_usd", "class")
	StorageIngestionRate    = NewGauge("lakehouse_storage_ingestion_rate_bytes")
)

// Cardinality limiter meta-metrics
var (
	MetricsCardinalityLimit    = NewGauge("lakehouse_metrics_cardinality_limit")
	MetricsCardinalityTracked  = NewGauge("lakehouse_metrics_cardinality_tracked")
	MetricsCardinalityOverflow = NewCounter("lakehouse_metrics_cardinality_overflow_total")
)

// Stats sync metrics
var (
	StatsPushTotal       = NewCounter("lakehouse_stats_push_total")
	StatsPushErrors      = NewCounter("lakehouse_stats_push_errors_total")
	StatsPushBytesTotal  = NewCounter("lakehouse_stats_push_bytes_total")
	StatsSnapshotTotal   = NewCounter("lakehouse_stats_snapshot_total")
	StatsSnapshotErrors  = NewCounter("lakehouse_stats_snapshot_errors_total")
	StatsMergesTotal     = NewCounter("lakehouse_stats_merges_total")
	StatsHeadObjectTotal = NewCounter("lakehouse_stats_headobject_total")
)

// Retention metrics
var (
	RetentionFilesDeleted = NewCounter("lakehouse_retention_files_deleted_total")
)

// Delete metrics
var (
	DeleteTombstonesActive      = NewGauge("lakehouse_delete_tombstones_active")
	DeleteTombstonesTotal       = NewCounter("lakehouse_delete_tombstones_total")
	DeleteRewriteTotal          = NewCounter("lakehouse_delete_rewrite_total")
	DeleteRewriteErrors         = NewCounter("lakehouse_delete_rewrite_errors_total")
	DeleteRewriteBytesSaved     = NewCounter("lakehouse_delete_rewrite_bytes_saved_total")
	DeleteRewriteSkippedGlacier = NewCounter("lakehouse_delete_rewrite_skipped_glacier_total")
	DeleteRowsSuppressed        = NewCounter("lakehouse_delete_rows_suppressed_total")
	DeleteCompactionRowsRemoved = NewCounter("lakehouse_delete_compaction_rows_removed_total")
	DeleteVerifyTotal           = NewCounter("lakehouse_delete_verify_total")
	DeleteVerifyLeakDetected    = NewCounter("lakehouse_delete_verify_leak_detected_total")
)

// Delete rewrite bookkeeping — the manifest hand-off and the tombstone
// durability path. Every one of these counts a state transition that used to
// happen silently (or not at all), so a non-zero error counter is the operator's
// first signal that a rewritten object and the manifest have diverged.
var (
	// DeleteRewriteManifestUpdated counts rewritten objects successfully
	// re-registered in the manifest (new key in, old key out).
	DeleteRewriteManifestUpdated = NewCounter("lakehouse_delete_rewrite_manifest_updated_total")
	// DeleteRewriteManifestErrors counts rewrites whose manifest hand-off
	// failed. The rewritten object is left unpublished and the tombstone is
	// NOT marked reaped, so the next scheduler tick retries.
	DeleteRewriteManifestErrors = NewCounter("lakehouse_delete_rewrite_manifest_errors_total")
	// DeleteRewriteSkippedNoManifest counts rewrites refused because no
	// manifest was wired into the scheduler. Rewriting without a manifest
	// would orphan the rewritten object, so the safeguard is to not rewrite.
	DeleteRewriteSkippedNoManifest = NewCounter("lakehouse_delete_rewrite_skipped_no_manifest_total")
	// DeleteRewriteOldObjectErrors counts failures to delete the superseded
	// object AFTER the manifest already points at its replacement. Harmless
	// for correctness (the stale object is unmanifested and the orphan sweep
	// reclaims it) but it costs storage until then.
	DeleteRewriteOldObjectErrors = NewCounter("lakehouse_delete_rewrite_old_object_errors_total")
	// DeleteRewriteSuperseded counts rewrites discarded because their source
	// left the manifest between the read and the publish — a concurrent
	// compaction merged it. The tombstone follows the rows to the compacted
	// output instead; publishing would have duplicated them.
	DeleteRewriteSuperseded = NewCounter("lakehouse_delete_rewrite_superseded_total")
	// DeleteRewriteAlreadyReaped counts keys the rewriter found already gone
	// from storage — the self-healing path after a crash between the manifest
	// swap and the tombstone bookkeeping.
	DeleteRewriteAlreadyReaped = NewCounter("lakehouse_delete_rewrite_already_reaped_total")
	// DeleteTombstonesCompleted counts tombstones retired because every key
	// they covered has been rewritten; a completed tombstone leaves Active().
	DeleteTombstonesCompleted = NewCounter("lakehouse_delete_tombstones_completed_total")
	// DeleteTombstonePersistTotal / Errors count durability writes by target
	// ("disk" or "s3").
	DeleteTombstonePersistTotal  = NewCounterVec("lakehouse_delete_tombstone_persist_total", "target")
	DeleteTombstonePersistErrors = NewCounterVec("lakehouse_delete_tombstone_persist_errors_total", "target")
	// DeleteTombstonePersistPending is the number of tombstone records whose
	// S3 copy is behind the in-memory state. Steady-state 0; a sustained
	// non-zero value means S3 writes are failing and only the local disk copy
	// would survive a pod move.
	DeleteTombstonePersistPending = NewGauge("lakehouse_delete_tombstone_persist_pending")
	// DeleteStartupInconsistencies counts manifest/tombstone disagreements
	// found by the boot-time self-check, by kind.
	DeleteStartupInconsistencies = NewCounterVec("lakehouse_delete_startup_inconsistencies_total", "kind")
	// DeleteRewriteInterrupted counts rewrites found unfinished — at startup or
	// by a later pass — by how they were resolved: undone (never published),
	// published (finished: the superseded object is deleted) or discarded (the
	// abandoned replacement is deleted).
	DeleteRewriteInterrupted = NewCounterVec("lakehouse_delete_rewrite_interrupted_total", "outcome")
	// DeleteRewriteAbandonedObjectErrors counts failed deletes of replacements
	// whose publish was refused or failed. Retried on every scheduler pass.
	DeleteRewriteAbandonedObjectErrors = NewCounter("lakehouse_delete_rewrite_abandoned_object_errors_total")
	// DeleteTombstoneNotDurable counts the times a rewrite step could not
	// proceed because the tombstone change authorising it had not reached
	// durable storage. Sustained values mean tombstone writes are failing and
	// deletes have stopped making progress (they are not losing data).
	DeleteTombstoneNotDurable = NewCounter("lakehouse_delete_tombstone_not_durable_total")
	// DeleteRewritesUnfinished is the number of rewrite records whose objects
	// are not settled yet. Must be 0 before rolling back to a release that
	// cannot read them.
	DeleteRewritesUnfinished = NewGauge("lakehouse_delete_rewrites_unfinished")
	// DeleteTombstoneRestoreAttempts counts S3 restore attempts by result
	// (failed, recovered) — a failed startup restore is retried on every
	// rewrite pass until it succeeds.
	DeleteTombstoneRestoreAttempts = NewCounterVec("lakehouse_delete_tombstone_restore_attempts_total", "result")
	// DeleteTombstoneRestorePending is 1 while the S3 copy of the tombstone
	// store has not been read in this process. While it is 1 the node enforces
	// only the deletes it found locally and resolves no interrupted rewrite —
	// the state the restore-failed alert fires on.
	DeleteTombstoneRestorePending = NewGauge("lakehouse_delete_tombstone_restore_pending")
	// DeleteRewriteDeferred counts rewrite work postponed rather than done, by
	// reason: the manifest has not listed the bucket yet (unlisted), the
	// tombstone store is incomplete (restore_pending), or a record is not
	// durable (not_durable).
	DeleteRewriteDeferred = NewCounterVec("lakehouse_delete_rewrite_deferred_total", "reason")
	// DeleteRewriteKeyCollisions counts replacement keys that could not be
	// claimed because the key was already in use.
	DeleteRewriteKeyCollisions = NewCounter("lakehouse_delete_rewrite_key_collisions_total")
	// DeleteTombstoneRemovedMarkersEvicted counts removed-tombstone markers
	// dropped by their TTL or cap. A marker is never evicted while its S3
	// delete is still owed; eviction only bounds the set's size.
	DeleteTombstoneRemovedMarkersEvicted = NewCounter("lakehouse_delete_tombstone_removed_markers_evicted_total")
	// DeleteFieldsScanFallback counts requests that gave up a fast path whose
	// answer a tombstone cannot be applied to (metadata-only field
	// enumeration: field_names/field_values/streams/stream_ids; the
	// pure-buffer aggregate path: pure_buffer) because an active tombstone
	// overlapped what that path answers from, so rows had to be verified.
	DeleteFieldsScanFallback = NewCounterVec("lakehouse_delete_fields_scan_fallback_total", "endpoint")
	// DeleteCompactionKeysReaped counts source keys marked reaped because a
	// compaction merged them (the tombstone follows the rows to the output).
	DeleteCompactionKeysReaped = NewCounter("lakehouse_delete_compaction_keys_reaped_total")
	// CompactionPublishConflicts counts compactions abandoned at publish
	// because one of their sources had already left the manifest (a
	// concurrent delete rewrite, or a racing compaction). The merged output is
	// discarded rather than registered next to whatever replaced the source.
	CompactionPublishConflicts = NewCounter("lakehouse_compaction_publish_conflicts_total")
	// DeleteCatalogRebuilds counts pmeta field-catalog value rebuilds after rows
	// were removed, by result ("rebuilt", "skipped_unlabeled_file"). A skipped
	// rebuild leaves the previous (over-inclusive) value set in place.
	DeleteCatalogRebuilds = NewCounterVec("lakehouse_delete_catalog_rebuilds_total", "result")
	// DeleteTombstoneKeysDiscovered counts files added to a tombstone's work
	// list because they overlap its range but were not in AffectedKeys — the
	// outputs of compactions that carried its rows forward, and late flushes.
	DeleteTombstoneKeysDiscovered = NewCounter("lakehouse_delete_tombstone_keys_discovered_total")
)

// Resource bound metrics — K8s-style request/limit/usage per resource surface.
// One block of four (acquired_total counter, rejected_total counter,
// outstanding_bytes gauge, outstanding_count gauge) per surface, matching
// the lakehouse_resourcebound_<surface>_* shape declared in the
// internal/resourcebounds package. Operator dashboards key on this prefix
// to render the request/limit/usage triple per resource.
//
// Surfaces (5):
//   - s3_concurrent_downloads (count-only; the byte gauge is 0 by design)
//   - query_file_workers (count-only)
//   - cache_memory (size + count)
//   - smart_cache_disk (size + count)
//   - query_max_rows (count-only; counts rows admitted vs ceiling)
var (
	ResourceBoundS3ConcurrentDownloadsAcquired         = NewCounter("lakehouse_resourcebound_s3_concurrent_downloads_acquired_total")
	ResourceBoundS3ConcurrentDownloadsRejected         = NewCounter("lakehouse_resourcebound_s3_concurrent_downloads_rejected_total")
	ResourceBoundS3ConcurrentDownloadsOutstandingBytes = NewGauge("lakehouse_resourcebound_s3_concurrent_downloads_outstanding_bytes")
	ResourceBoundS3ConcurrentDownloadsOutstandingCount = NewGauge("lakehouse_resourcebound_s3_concurrent_downloads_outstanding_count")

	ResourceBoundQueryFileWorkersAcquired         = NewCounter("lakehouse_resourcebound_query_file_workers_acquired_total")
	ResourceBoundQueryFileWorkersRejected         = NewCounter("lakehouse_resourcebound_query_file_workers_rejected_total")
	ResourceBoundQueryFileWorkersOutstandingBytes = NewGauge("lakehouse_resourcebound_query_file_workers_outstanding_bytes")
	ResourceBoundQueryFileWorkersOutstandingCount = NewGauge("lakehouse_resourcebound_query_file_workers_outstanding_count")

	ResourceBoundCacheMemoryAcquired         = NewCounter("lakehouse_resourcebound_cache_memory_acquired_total")
	ResourceBoundCacheMemoryRejected         = NewCounter("lakehouse_resourcebound_cache_memory_rejected_total")
	ResourceBoundCacheMemoryOutstandingBytes = NewGauge("lakehouse_resourcebound_cache_memory_outstanding_bytes")
	ResourceBoundCacheMemoryOutstandingCount = NewGauge("lakehouse_resourcebound_cache_memory_outstanding_count")

	ResourceBoundSmartCacheDiskAcquired         = NewCounter("lakehouse_resourcebound_smart_cache_disk_acquired_total")
	ResourceBoundSmartCacheDiskRejected         = NewCounter("lakehouse_resourcebound_smart_cache_disk_rejected_total")
	ResourceBoundSmartCacheDiskOutstandingBytes = NewGauge("lakehouse_resourcebound_smart_cache_disk_outstanding_bytes")
	ResourceBoundSmartCacheDiskOutstandingCount = NewGauge("lakehouse_resourcebound_smart_cache_disk_outstanding_count")

	ResourceBoundQueryMaxRowsAcquired         = NewCounter("lakehouse_resourcebound_query_max_rows_acquired_total")
	ResourceBoundQueryMaxRowsRejected         = NewCounter("lakehouse_resourcebound_query_max_rows_rejected_total")
	ResourceBoundQueryMaxRowsOutstandingBytes = NewGauge("lakehouse_resourcebound_query_max_rows_outstanding_bytes")
	ResourceBoundQueryMaxRowsOutstandingCount = NewGauge("lakehouse_resourcebound_query_max_rows_outstanding_count")

	// Per-resource request/limit info gauges (set once at startup and
	// after any runtime reconfiguration). Operators read these to size
	// container resources against the sum of memory-class limits.
	ResourceBoundS3ConcurrentDownloadsRequest = NewGauge("lakehouse_resourcebound_s3_concurrent_downloads_request")
	ResourceBoundS3ConcurrentDownloadsLimit   = NewGauge("lakehouse_resourcebound_s3_concurrent_downloads_limit")
	ResourceBoundQueryFileWorkersRequest      = NewGauge("lakehouse_resourcebound_query_file_workers_request")
	ResourceBoundQueryFileWorkersLimit        = NewGauge("lakehouse_resourcebound_query_file_workers_limit")
	ResourceBoundCacheMemoryRequest           = NewGauge("lakehouse_resourcebound_cache_memory_request")
	ResourceBoundCacheMemoryLimit             = NewGauge("lakehouse_resourcebound_cache_memory_limit")
	ResourceBoundSmartCacheDiskRequest        = NewGauge("lakehouse_resourcebound_smart_cache_disk_request")
	ResourceBoundSmartCacheDiskLimit          = NewGauge("lakehouse_resourcebound_smart_cache_disk_limit")
	ResourceBoundQueryMaxRowsRequest          = NewGauge("lakehouse_resourcebound_query_max_rows_request")
	ResourceBoundQueryMaxRowsLimit            = NewGauge("lakehouse_resourcebound_query_max_rows_limit")
)
