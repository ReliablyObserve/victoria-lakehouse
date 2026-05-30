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
	DiscoveryHotBoundaryDays    = NewFloatGauge("lakehouse_discovery_hot_boundary_days")
	DiscoveryGapDays            = NewFloatGauge("lakehouse_discovery_hot_boundary_gap_days")
	ManifestPushTotal           = NewCounter("lakehouse_manifest_push_total")
	ManifestPushPeers           = NewGauge("lakehouse_manifest_push_peers")
	ManifestPushErrorsTotal     = NewCounter("lakehouse_manifest_push_errors_total")
	ManifestUpdateReceivedTotal = NewCounter("lakehouse_manifest_update_received_total")
)

// Parquet engine metrics
var (
	ParquetRowGroupsScanned = NewCounter("lakehouse_parquet_row_groups_scanned_total")
	ParquetRowGroupsSkipped = NewCounterVec("lakehouse_parquet_row_groups_skipped_total", "reason")
	ParquetBloomChecks      = NewCounterVec("lakehouse_parquet_bloom_checks_total", "result")
	ParquetColumnBytesRead  = NewCounter("lakehouse_parquet_column_bytes_read_total")
	ParquetFilesOpened      = NewCounter("lakehouse_parquet_files_opened_total")
	ParquetFilesSkipped     = NewCounter("lakehouse_parquet_files_skipped_bloom_total")
	FooterCacheHits         = NewCounter("lakehouse_footer_cache_hits_total")
	FooterCacheEvictions    = NewCounter("lakehouse_footer_cache_evictions_total")
	FooterCacheEntries      = NewGauge("lakehouse_footer_cache_entries")
	TraceIDCacheHits        = NewCounter("lakehouse_trace_id_cache_hits_total")
	MetadataOnlyFiles       = NewCounter("lakehouse_metadata_only_files_total")
	QueryFileNotFoundTotal  = NewCounter("lakehouse_query_file_not_found_total")
	QueryFileErrorsTotal    = NewCounter("lakehouse_query_file_errors_total")
)

// Insert / writer metrics
var (
	InsertRowsTotal        = NewCounter("lakehouse_insert_rows_total")
	InsertRowsBuffered     = NewGauge("lakehouse_insert_rows_buffered")
	InsertBytesBuffered    = NewGauge("lakehouse_insert_bytes_buffered")
	InsertFlushTotal       = NewCounter("lakehouse_insert_flush_total")
	InsertFlushErrorsTotal = NewCounter("lakehouse_insert_flush_errors_total")
	InsertFlushDuration    = NewHistogram("lakehouse_insert_flush_duration_seconds",
		[]float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10})
	InsertBytesUploaded    = NewCounter("lakehouse_insert_bytes_uploaded_total")
	InsertPartitionsActive = NewGauge("lakehouse_insert_partitions_active")
	InsertWALBytes         = NewGauge("lakehouse_insert_wal_bytes")
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
)

// Query metrics
var (
	QueryDuration             = NewHistogram("lakehouse_query_duration_seconds", DefBuckets)
	QueryRowsTotal            = NewCounter("lakehouse_query_rows_returned_total")
	QueryRejectedTotal        = NewCounter("lakehouse_query_rejected_total")
	QueryFileLimitExceeded    = NewCounter("lakehouse_query_file_limit_exceeded_total")
	QueryMemoryBudgetExceeded = NewCounter("lakehouse_query_memory_budget_exceeded_total")
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
)

// Election metrics
var (
	ElectionLeader            = NewGauge("lakehouse_election_leader")
	ElectionTransitionsTotal  = NewCounter("lakehouse_election_transitions_total")
	ElectionHealthChecksTotal = NewCounterVec("lakehouse_election_health_checks_total", "result")
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
