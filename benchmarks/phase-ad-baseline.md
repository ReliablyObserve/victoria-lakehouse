# Phase A-D Benchmark Baseline - 2026-05-23T20:40:10Z

## Unit Test Results
- s3reader + compaction: 104 passed
- parquets3: 285 passed (1 pre-existing timeout: TestGetFileData_DiskCacheCorruptedFile)
- Total: 389 passed, 0 Phase A-D failures

## E2E Stack
- All services healthy
- Data seeded: 50,000 logs + 11,687 trace spans (48h window)

## Lakehouse Metrics
lakehouse_bloom_build_errors_total 0
lakehouse_bloom_build_total{trigger="file_bloom"} 140
lakehouse_bloom_build_total{trigger="flush"} 140
lakehouse_bloom_bytes_avoided_total 0
lakehouse_bloom_bytes_memory 0
lakehouse_bloom_config_sync_errors_total 0
lakehouse_bloom_config_sync_total 0
lakehouse_bloom_entries_total 38342
lakehouse_bloom_files_skipped_total 0
lakehouse_cache_bytes_limit 536870912
lakehouse_cache_bytes_used 0
lakehouse_cache_coverage_hours 0
lakehouse_cache_cross_eviction_applied_total 0
lakehouse_cache_cross_eviction_pending 0
lakehouse_cache_cross_eviction_received_total 0
lakehouse_cache_cross_eviction_sent_total 0
lakehouse_cache_cross_prefetch_received_total 0
lakehouse_cache_cross_prefetch_sent_total 0
lakehouse_cache_disk_bytes 0
lakehouse_cache_effective_bytes 0
lakehouse_cache_entries_total 0
lakehouse_cache_hit_ratio 0
lakehouse_cache_hot_entries 0
lakehouse_cache_memory_bytes 0
lakehouse_cache_owned_bytes 0
lakehouse_cache_owned_entries 0
lakehouse_cache_peer_served_total 0
lakehouse_cache_pinned_entries 0
lakehouse_cache_prefetch_hit_ratio 0
lakehouse_cache_singleflight_dedup_total 0
lakehouse_compaction_bytes_read_total 0
lakehouse_compaction_bytes_written_total 0
lakehouse_compaction_errors_total 0
lakehouse_compaction_files_input_total 0
lakehouse_compaction_files_output_total 0
lakehouse_compaction_rows_merged_total 0
lakehouse_compaction_runs_total 0
lakehouse_concurrent_select_capacity 32
lakehouse_concurrent_select_current 0
lakehouse_delete_compaction_rows_removed_total 0
lakehouse_delete_rewrite_bytes_saved_total 0
lakehouse_delete_rewrite_errors_total 0
lakehouse_delete_rewrite_skipped_glacier_total 0
lakehouse_delete_rewrite_total 0
lakehouse_delete_rows_suppressed_total 0
lakehouse_delete_tombstones_active 0
lakehouse_delete_tombstones_total 0
lakehouse_delete_verify_leak_detected_total 0
lakehouse_delete_verify_total 0
lakehouse_discovery_hot_boundary_days 0
lakehouse_discovery_hot_boundary_gap_days 0
lakehouse_election_leader 0
lakehouse_election_transitions_total 0
lakehouse_footer_cache_entries 0
lakehouse_footer_cache_evictions_total 0
lakehouse_footer_cache_hits_total 55
lakehouse_info{mode="logs",role="all",topology="auto",version="dev"} 1
lakehouse_insert_bytes_buffered 0
lakehouse_insert_bytes_uploaded_total 18446040
lakehouse_insert_flush_duration_seconds_bucket{vmrange="5.995e-02...6.813e-02"} 3
lakehouse_insert_flush_duration_seconds_bucket{vmrange="6.813e-02...7.743e-02"} 3
lakehouse_insert_flush_duration_seconds_bucket{vmrange="7.743e-02...8.799e-02"} 2
lakehouse_insert_flush_duration_seconds_bucket{vmrange="1.136e-01...1.292e-01"} 1
lakehouse_insert_flush_duration_seconds_bucket{vmrange="4.084e-01...4.642e-01"} 1
lakehouse_insert_flush_duration_seconds_bucket{vmrange="1.000e+00...1.136e+00"} 1
lakehouse_insert_flush_duration_seconds_sum 2.174065334
lakehouse_insert_flush_duration_seconds_count 11
lakehouse_insert_flush_errors_total 0
lakehouse_insert_flush_total 11
lakehouse_insert_partitions_active 2
lakehouse_insert_rows_buffered 2000
lakehouse_insert_rows_total 97000
lakehouse_insert_wal_bytes 0
lakehouse_manifest_bytes 611515405
lakehouse_manifest_fast_path_total 0
lakehouse_manifest_files 369
lakehouse_manifest_push_errors_total 0
lakehouse_manifest_push_peers 0
lakehouse_manifest_push_total 0
lakehouse_manifest_update_received_total 0
lakehouse_metadata_only_files_total 0
lakehouse_metrics_cardinality_limit 100
lakehouse_metrics_cardinality_overflow_total 0
lakehouse_metrics_cardinality_tracked 0
lakehouse_parquet_column_bytes_read_total 18293663
lakehouse_parquet_files_opened_total 23
lakehouse_parquet_files_skipped_bloom_total 0
lakehouse_parquet_row_groups_scanned_total 25
lakehouse_parquet_row_groups_skipped_total{reason="stats"} 3
lakehouse_peer_cross_az_members 0
lakehouse_peer_errors_total 0
lakehouse_peer_hits_total 0
lakehouse_peer_ring_members 0
lakehouse_peer_same_az_members 0
lakehouse_prefetch_bytes_total 110763268
lakehouse_prefetch_hits_total 0
lakehouse_prefetch_tasks_total{type="footer_prefetch"} 8
lakehouse_prefetch_tasks_total{type="warmup"} 64
lakehouse_query_duration_seconds_bucket{vmrange="1.136e-01...1.292e-01"} 2
lakehouse_query_duration_seconds_sum 0.253752666
lakehouse_query_duration_seconds_count 2
lakehouse_query_rejected_total 0
lakehouse_query_rows_returned_total 58971
lakehouse_ready 1
lakehouse_retention_files_deleted_total 0
lakehouse_s3_buffer_hits_total 0
lakehouse_s3_buffer_misses_total 0
lakehouse_s3_bytes_read_total 223444580
lakehouse_s3_coalesced_ranges_total 0
lakehouse_s3_errors_total{op="GetObject"} 10
lakehouse_s3_range_bytes_read_total 163840
lakehouse_s3_range_reads_total 10
lakehouse_s3_request_duration_seconds_bucket{vmrange="4.084e-04...4.642e-04"} 5
lakehouse_s3_request_duration_seconds_bucket{vmrange="4.642e-04...5.275e-04"} 3
lakehouse_s3_request_duration_seconds_bucket{vmrange="5.275e-04...5.995e-04"} 3
lakehouse_s3_request_duration_seconds_bucket{vmrange="5.995e-04...6.813e-04"} 9
lakehouse_s3_request_duration_seconds_bucket{vmrange="6.813e-04...7.743e-04"} 18
lakehouse_s3_request_duration_seconds_bucket{vmrange="7.743e-04...8.799e-04"} 23
lakehouse_s3_request_duration_seconds_bucket{vmrange="8.799e-04...1.000e-03"} 43
lakehouse_s3_request_duration_seconds_bucket{vmrange="1.000e-03...1.136e-03"} 44
lakehouse_s3_request_duration_seconds_bucket{vmrange="1.136e-03...1.292e-03"} 45
lakehouse_s3_request_duration_seconds_bucket{vmrange="1.292e-03...1.468e-03"} 24
lakehouse_s3_request_duration_seconds_bucket{vmrange="1.468e-03...1.668e-03"} 30
lakehouse_s3_request_duration_seconds_bucket{vmrange="1.668e-03...1.896e-03"} 33
lakehouse_s3_request_duration_seconds_bucket{vmrange="1.896e-03...2.154e-03"} 53
lakehouse_s3_request_duration_seconds_bucket{vmrange="2.154e-03...2.448e-03"} 66
lakehouse_s3_request_duration_seconds_bucket{vmrange="2.448e-03...2.783e-03"} 57
lakehouse_s3_request_duration_seconds_bucket{vmrange="2.783e-03...3.162e-03"} 66
lakehouse_s3_request_duration_seconds_bucket{vmrange="3.162e-03...3.594e-03"} 72
lakehouse_s3_request_duration_seconds_bucket{vmrange="3.594e-03...4.084e-03"} 54
lakehouse_s3_request_duration_seconds_bucket{vmrange="4.084e-03...4.642e-03"} 53
lakehouse_s3_request_duration_seconds_bucket{vmrange="4.642e-03...5.275e-03"} 32
lakehouse_s3_request_duration_seconds_bucket{vmrange="5.275e-03...5.995e-03"} 19
lakehouse_s3_request_duration_seconds_bucket{vmrange="5.995e-03...6.813e-03"} 19
lakehouse_s3_request_duration_seconds_bucket{vmrange="6.813e-03...7.743e-03"} 12
lakehouse_s3_request_duration_seconds_bucket{vmrange="7.743e-03...8.799e-03"} 1
lakehouse_s3_request_duration_seconds_bucket{vmrange="8.799e-03...1.000e-02"} 3
lakehouse_s3_request_duration_seconds_bucket{vmrange="1.000e-02...1.136e-02"} 2
lakehouse_s3_request_duration_seconds_bucket{vmrange="1.136e-02...1.292e-02"} 5
lakehouse_s3_request_duration_seconds_bucket{vmrange="1.292e-02...1.468e-02"} 4
lakehouse_s3_request_duration_seconds_bucket{vmrange="1.468e-02...1.668e-02"} 3
lakehouse_s3_request_duration_seconds_bucket{vmrange="1.668e-02...1.896e-02"} 1
lakehouse_s3_request_duration_seconds_bucket{vmrange="1.896e-02...2.154e-02"} 1
lakehouse_s3_request_duration_seconds_bucket{vmrange="2.154e-02...2.448e-02"} 2
lakehouse_s3_request_duration_seconds_bucket{vmrange="3.162e-02...3.594e-02"} 1
lakehouse_s3_request_duration_seconds_bucket{vmrange="3.594e-02...4.084e-02"} 1
lakehouse_s3_request_duration_seconds_bucket{vmrange="4.084e-02...4.642e-02"} 1
lakehouse_s3_request_duration_seconds_bucket{vmrange="6.813e-02...7.743e-02"} 1
lakehouse_s3_request_duration_seconds_bucket{vmrange="7.743e-02...8.799e-02"} 1
lakehouse_s3_request_duration_seconds_bucket{vmrange="8.799e-02...1.000e-01"} 1
lakehouse_s3_request_duration_seconds_sum 2.7530851569999997
lakehouse_s3_request_duration_seconds_count 811
lakehouse_s3_requests_total{op="DeleteObject"} 52
lakehouse_s3_requests_total{op="GetObject"} 219
lakehouse_s3_requests_total{op="PUT"} 140
lakehouse_s3_requests_total{op="PutObject"} 540
lakehouse_s3_throttle_total 0
lakehouse_slow_queries_total 0
lakehouse_startup_phase 3
lakehouse_startup_total_seconds 0.362543917
lakehouse_stats_headobject_total 0
lakehouse_stats_merges_total 0
lakehouse_stats_push_bytes_total 0
lakehouse_stats_push_errors_total 0
lakehouse_stats_push_total 0
lakehouse_stats_snapshot_errors_total 0
lakehouse_stats_snapshot_total 4
lakehouse_storage_avg_row_bytes 55
lakehouse_storage_bytes_total 611515405
lakehouse_storage_compression_ratio 0.2862988447527336
lakehouse_storage_cost_monthly_usd 0
lakehouse_storage_files_total 369
lakehouse_storage_ingestion_rate_bytes 0
lakehouse_storage_newest_data_seconds 0
lakehouse_storage_oldest_data_seconds 0
lakehouse_storage_partitions_total 100
lakehouse_storage_raw_bytes_total 175076154
lakehouse_storage_rows_total 3177027
lakehouse_storage_tenants_total 1
lakehouse_tenant_bytes{tenant="0:0"} 611515405
lakehouse_tenant_files{tenant="0:0"} 369
lakehouse_tenant_raw_bytes{tenant="0:0"} 175076154
lakehouse_trace_id_cache_hits_total 0

## Query Benchmark
Query: * | start=1h | limit=10
Time: 0.155681s
HTTP: 200

Additional runs:
Run 1: 0.103094s (HTTP 200)
Run 2: 0.091033s (HTTP 200)
Run 3: 0.087413s (HTTP 200)

## Query Benchmark (wider window)
Query: * | start=24h | limit=100
Time: 4.079178s
HTTP: 200
