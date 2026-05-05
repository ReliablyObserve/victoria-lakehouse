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
	S3ErrorsTotal         = NewCounterVec("lakehouse_s3_errors_total", "op")
	S3BytesReadTotal      = NewCounter("lakehouse_s3_bytes_read_total")
	S3ThrottleTotal       = NewCounter("lakehouse_s3_throttle_total")
	S3CircuitBreakerState = NewGauge("lakehouse_s3_circuit_breaker_state")
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

// Startup & health metrics
var (
	StartupPhase        = NewGauge("lakehouse_startup_phase")
	StartupTotalSeconds = NewFloatGauge("lakehouse_startup_total_seconds")
	Ready               = NewGauge("lakehouse_ready")
)

// Query metrics
var (
	QueryDuration  = NewHistogram("lakehouse_query_duration_seconds", DefBuckets)
	QueryRowsTotal = NewCounter("lakehouse_query_rows_returned_total")
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
