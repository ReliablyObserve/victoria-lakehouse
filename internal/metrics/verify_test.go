package metrics

import (
	"testing"
)

func TestVerifyMetrics_AllCountersExist(t *testing.T) {
	counters := []struct {
		name string
		c    *Counter
	}{
		{"SlowQueriesTotal", SlowQueriesTotal},
		{"S3BytesReadTotal", S3BytesReadTotal},
		{"S3ThrottleTotal", S3ThrottleTotal},
		{"CacheSingleflightDedup", CacheSingleflightDedup},
		{"PeerHitsTotal", PeerHitsTotal},
		{"PeerErrorsTotal", PeerErrorsTotal},
		{"ManifestFastPathTotal", ManifestFastPathTotal},
		{"ManifestPushTotal", ManifestPushTotal},
		{"ManifestPushErrorsTotal", ManifestPushErrorsTotal},
		{"ManifestUpdateReceivedTotal", ManifestUpdateReceivedTotal},
		{"ParquetRowGroupsScanned", ParquetRowGroupsScanned},
		{"ParquetColumnBytesRead", ParquetColumnBytesRead},
		{"ParquetFilesOpened", ParquetFilesOpened},
		{"ParquetFilesSkipped", ParquetFilesSkipped},
		{"InsertRowsTotal", InsertRowsTotal},
		{"InsertFlushTotal", InsertFlushTotal},
		{"InsertFlushErrorsTotal", InsertFlushErrorsTotal},
		{"InsertBytesUploaded", InsertBytesUploaded},
		{"PrefetchHitsTotal", PrefetchHitsTotal},
		{"PrefetchBytesTotal", PrefetchBytesTotal},
		{"SmartCachePeerServedTotal", SmartCachePeerServedTotal},
		{"CrossEvictionSent", CrossEvictionSent},
		{"CrossEvictionReceived", CrossEvictionReceived},
		{"CrossEvictionApplied", CrossEvictionApplied},
		{"CrossPrefetchSent", CrossPrefetchSent},
		{"CrossPrefetchReceived", CrossPrefetchReceived},
		{"BloomBuildErrors", BloomBuildErrors},
		{"BloomEntriesTotal", BloomEntriesTotal},
		{"BloomFilesSkipped", BloomFilesSkipped},
		{"BloomBytesAvoided", BloomBytesAvoided},
		{"BloomConfigSyncTotal", BloomConfigSyncTotal},
		{"BloomConfigSyncError", BloomConfigSyncError},
		{"ElectionTransitionsTotal", ElectionTransitionsTotal},
		{"QueryRowsTotal", QueryRowsTotal},
		{"QueryRejectedTotal", QueryRejectedTotal},
		{"CompactionRunsTotal", CompactionRunsTotal},
		{"CompactionFilesInputTotal", CompactionFilesInputTotal},
		{"CompactionFilesOutputTotal", CompactionFilesOutputTotal},
		{"CompactionBytesReadTotal", CompactionBytesReadTotal},
		{"CompactionBytesWrittenTotal", CompactionBytesWrittenTotal},
		{"CompactionRowsMergedTotal", CompactionRowsMergedTotal},
		{"CompactionErrorsTotal", CompactionErrorsTotal},
		{"MetricsCardinalityOverflow", MetricsCardinalityOverflow},
		{"StatsPushTotal", StatsPushTotal},
		{"StatsPushErrors", StatsPushErrors},
		{"StatsPushBytesTotal", StatsPushBytesTotal},
		{"StatsSnapshotTotal", StatsSnapshotTotal},
		{"StatsSnapshotErrors", StatsSnapshotErrors},
		{"StatsMergesTotal", StatsMergesTotal},
		{"StatsHeadObjectTotal", StatsHeadObjectTotal},
		{"RetentionFilesDeleted", RetentionFilesDeleted},
		{"DeleteTombstonesTotal", DeleteTombstonesTotal},
		{"DeleteRewriteTotal", DeleteRewriteTotal},
		{"DeleteRewriteErrors", DeleteRewriteErrors},
		{"DeleteRewriteBytesSaved", DeleteRewriteBytesSaved},
		{"DeleteRewriteSkippedGlacier", DeleteRewriteSkippedGlacier},
		{"DeleteRowsSuppressed", DeleteRowsSuppressed},
		{"DeleteCompactionRowsRemoved", DeleteCompactionRowsRemoved},
		{"DeleteVerifyTotal", DeleteVerifyTotal},
		{"DeleteVerifyLeakDetected", DeleteVerifyLeakDetected},
	}

	for _, tc := range counters {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.c == nil {
				t.Fatalf("counter %s is nil", tc.name)
			}
		})
	}
}

func TestVerifyMetrics_AllGaugesExist(t *testing.T) {
	gauges := []struct {
		name string
		g    *Gauge
	}{
		{"ConcurrentSelects", ConcurrentSelects},
		{"ConcurrentSelectsCap", ConcurrentSelectsCap},
		{"CacheMemoryBytes", CacheMemoryBytes},
		{"CacheDiskBytes", CacheDiskBytes},
		{"PeerRingMembers", PeerRingMembers},
		{"ManifestFiles", ManifestFiles},
		{"ManifestBytes", ManifestBytes},
		{"ManifestPushPeers", ManifestPushPeers},
		{"InsertRowsBuffered", InsertRowsBuffered},
		{"InsertBytesBuffered", InsertBytesBuffered},
		{"InsertPartitionsActive", InsertPartitionsActive},
		{"InsertWALBytes", InsertWALBytes},
		{"SmartCacheEntriesTotal", SmartCacheEntriesTotal},
		{"SmartCacheBytesUsed", SmartCacheBytesUsed},
		{"SmartCacheBytesLimit", SmartCacheBytesLimit},
		{"SmartCacheHotEntries", SmartCacheHotEntries},
		{"SmartCachePinnedEntries", SmartCachePinnedEntries},
		{"SmartCacheOwnedEntries", SmartCacheOwnedEntries},
		{"SmartCacheOwnedBytes", SmartCacheOwnedBytes},
		{"SmartCacheEffectiveBytes", SmartCacheEffectiveBytes},
		{"CrossEvictionPending", CrossEvictionPending},
		{"PeerSameAZMembers", PeerSameAZMembers},
		{"PeerCrossAZMembers", PeerCrossAZMembers},
		{"BloomBytesMemory", BloomBytesMemory},
		{"StartupPhase", StartupPhase},
		{"Ready", Ready},
		{"ElectionLeader", ElectionLeader},
		{"StorageFilesTotal", StorageFilesTotal},
		{"StorageBytesTotal", StorageBytesTotal},
		{"StorageRawBytesTotal", StorageRawBytesTotal},
		{"StorageRowsTotal", StorageRowsTotal},
		{"StorageAvgRowBytes", StorageAvgRowBytes},
		{"StoragePartitionsTotal", StoragePartitionsTotal},
		{"StorageOldestData", StorageOldestData},
		{"StorageNewestData", StorageNewestData},
		{"StorageTenantsTotal", StorageTenantsTotal},
		{"StorageIngestionRate", StorageIngestionRate},
		{"MetricsCardinalityLimit", MetricsCardinalityLimit},
		{"MetricsCardinalityTracked", MetricsCardinalityTracked},
		{"DeleteTombstonesActive", DeleteTombstonesActive},
	}

	for _, tc := range gauges {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.g == nil {
				t.Fatalf("gauge %s is nil", tc.name)
			}
		})
	}
}

func TestVerifyMetrics_HistogramsExist(t *testing.T) {
	histograms := []struct {
		name string
		h    *Histogram
	}{
		{"HTTPRequestDuration", HTTPRequestDuration},
		{"S3RequestDuration", S3RequestDuration},
		{"ManifestRefreshDuration", ManifestRefreshDuration},
		{"InsertFlushDuration", InsertFlushDuration},
		{"QueryDuration", QueryDuration},
		{"CompactionDuration", CompactionDuration},
	}

	for _, tc := range histograms {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.h == nil {
				t.Fatalf("histogram %s is nil", tc.name)
			}
		})
	}
}

func TestVerifyMetrics_CounterVecsExist(t *testing.T) {
	vecs := []struct {
		name string
		cv   *CounterVec
	}{
		{"HTTPRequestsTotal", HTTPRequestsTotal},
		{"HTTPErrorsTotal", HTTPErrorsTotal},
		{"S3RequestsTotal", S3RequestsTotal},
		{"S3ErrorsTotal", S3ErrorsTotal},
		{"CacheHitsTotal", CacheHitsTotal},
		{"CacheMissesTotal", CacheMissesTotal},
		{"PeerRequestsTotal", PeerRequestsTotal},
		{"PeerBytesTransferred", PeerBytesTransferred},
		{"ParquetRowGroupsSkipped", ParquetRowGroupsSkipped},
		{"ParquetBloomChecks", ParquetBloomChecks},
		{"PrefetchTasksTotal", PrefetchTasksTotal},
		{"SmartCacheEvictionsTotal", SmartCacheEvictionsTotal},
		{"SmartCacheRecommendedBytes", SmartCacheRecommendedBytes},
		{"PeerAZRequestsTotal", PeerAZRequestsTotal},
		{"BufferBridgeAZRequestsTotal", BufferBridgeAZRequestsTotal},
		{"BloomBuildTotal", BloomBuildTotal},
		{"BloomQueriesTotal", BloomQueriesTotal},
		{"BloomTierTransitions", BloomTierTransitions},
		{"BloomControllerAdj", BloomControllerAdj},
		{"ElectionHealthChecksTotal", ElectionHealthChecksTotal},
		{"TenantRowsTotal", TenantRowsTotal},
		{"TenantIngestionBytesTotal", TenantIngestionBytesTotal},
		{"TenantQueriesTotal", TenantQueriesTotal},
		{"CompactionSkippedTotal", CompactionSkippedTotal},
	}

	for _, tc := range vecs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.cv == nil {
				t.Fatalf("counter vec %s is nil", tc.name)
			}
		})
	}
}

func TestVerifyMetrics_CounterIncrements(t *testing.T) {
	// Each counter must start at some value and increment by exactly 1.
	before := SlowQueriesTotal.Get()
	SlowQueriesTotal.Inc()
	after := SlowQueriesTotal.Get()
	if after != before+1 {
		t.Fatalf("SlowQueriesTotal.Inc(): expected %d, got %d", before+1, after)
	}

	before2 := InsertRowsTotal.Get()
	InsertRowsTotal.Add(5)
	after2 := InsertRowsTotal.Get()
	if after2 != before2+5 {
		t.Fatalf("InsertRowsTotal.Add(5): expected %d, got %d", before2+5, after2)
	}

	before3 := S3BytesReadTotal.Get()
	S3BytesReadTotal.Inc()
	after3 := S3BytesReadTotal.Get()
	if after3 != before3+1 {
		t.Fatalf("S3BytesReadTotal.Inc(): expected %d, got %d", before3+1, after3)
	}

	before4 := CompactionRunsTotal.Get()
	CompactionRunsTotal.Inc()
	after4 := CompactionRunsTotal.Get()
	if after4 != before4+1 {
		t.Fatalf("CompactionRunsTotal.Inc(): expected %d, got %d", before4+1, after4)
	}
}

func TestVerifyMetrics_GaugeSetGet(t *testing.T) {
	ConcurrentSelects.Set(7)
	if got := ConcurrentSelects.Get(); got != 7 {
		t.Fatalf("ConcurrentSelects.Set(7): expected 7, got %d", got)
	}

	ManifestFiles.Set(42)
	if got := ManifestFiles.Get(); got != 42 {
		t.Fatalf("ManifestFiles.Set(42): expected 42, got %d", got)
	}

	InsertPartitionsActive.Set(3)
	if got := InsertPartitionsActive.Get(); got != 3 {
		t.Fatalf("InsertPartitionsActive.Set(3): expected 3, got %d", got)
	}

	Ready.Set(1)
	if got := Ready.Get(); got != 1 {
		t.Fatalf("Ready.Set(1): expected 1, got %d", got)
	}
}

func TestVerifyMetrics_HistogramObserves(t *testing.T) {
	// Observe must not panic and must accept any non-negative value.
	observations := []float64{0.0, 0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 100.0}

	for _, v := range observations {
		HTTPRequestDuration.Observe(v)
		S3RequestDuration.Observe(v)
		ManifestRefreshDuration.Observe(v)
		InsertFlushDuration.Observe(v)
		QueryDuration.Observe(v)
		CompactionDuration.Observe(v)
	}
}

func TestVerifyMetrics_CounterVecIncrements(t *testing.T) {
	before := HTTPRequestsTotal.Get("/insert")
	HTTPRequestsTotal.Inc("/insert")
	after := HTTPRequestsTotal.Get("/insert")
	if after != before+1 {
		t.Fatalf("HTTPRequestsTotal.Inc(\"/insert\"): expected %d, got %d", before+1, after)
	}

	before2 := S3RequestsTotal.Get("GetObject")
	S3RequestsTotal.Inc("GetObject")
	after2 := S3RequestsTotal.Get("GetObject")
	if after2 != before2+1 {
		t.Fatalf("S3RequestsTotal.Inc(\"GetObject\"): expected %d, got %d", before2+1, after2)
	}

	before3 := CacheHitsTotal.Get("hot")
	CacheHitsTotal.Inc("hot")
	after3 := CacheHitsTotal.Get("hot")
	if after3 != before3+1 {
		t.Fatalf("CacheHitsTotal.Inc(\"hot\"): expected %d, got %d", before3+1, after3)
	}

	before4 := CompactionSkippedTotal.Get("no_files")
	CompactionSkippedTotal.Inc("no_files")
	after4 := CompactionSkippedTotal.Get("no_files")
	if after4 != before4+1 {
		t.Fatalf("CompactionSkippedTotal.Inc(\"no_files\"): expected %d, got %d", before4+1, after4)
	}
}

func TestVerifyMetrics_FloatGaugesExist(t *testing.T) {
	floatGauges := []struct {
		name string
		g    *FloatGauge
	}{
		{"DiscoveryHotBoundaryDays", DiscoveryHotBoundaryDays},
		{"DiscoveryGapDays", DiscoveryGapDays},
		{"SmartCacheHitRatio", SmartCacheHitRatio},
		{"SmartCacheCoverageHours", SmartCacheCoverageHours},
		{"SmartCachePrefetchHitRatio", SmartCachePrefetchHitRatio},
		{"StartupTotalSeconds", StartupTotalSeconds},
		{"StorageCompressionRatio", StorageCompressionRatio},
		{"StorageCostMonthlyUSD", StorageCostMonthlyUSD},
	}

	for _, tc := range floatGauges {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.g == nil {
				t.Fatalf("float gauge %s is nil", tc.name)
			}
			tc.g.Set(1.5)
			if got := tc.g.Get(); got != 1.5 {
				t.Fatalf("%s.Set(1.5): expected 1.5, got %f", tc.name, got)
			}
		})
	}
}
