package metrics

import "testing"

func TestSmartCacheMetrics_Initialized(t *testing.T) {
	// Verify all smart cache metrics are non-nil (package-level vars initialized)
	metrics := []struct {
		name string
		ptr  interface{}
	}{
		{"SmartCacheHitRatio", SmartCacheHitRatio},
		{"SmartCacheEntriesTotal", SmartCacheEntriesTotal},
		{"SmartCacheBytesUsed", SmartCacheBytesUsed},
		{"SmartCacheBytesLimit", SmartCacheBytesLimit},
		{"SmartCacheEvictionsTotal", SmartCacheEvictionsTotal},
		{"SmartCacheHotEntries", SmartCacheHotEntries},
		{"SmartCachePinnedEntries", SmartCachePinnedEntries},
		{"SmartCacheRecommendedBytes", SmartCacheRecommendedBytes},
		{"SmartCacheCoverageHours", SmartCacheCoverageHours},
		{"SmartCachePrefetchHitRatio", SmartCachePrefetchHitRatio},
		{"SmartCacheOwnedEntries", SmartCacheOwnedEntries},
		{"SmartCacheOwnedBytes", SmartCacheOwnedBytes},
		{"SmartCachePeerServedTotal", SmartCachePeerServedTotal},
		{"SmartCacheEffectiveBytes", SmartCacheEffectiveBytes},
	}

	for _, m := range metrics {
		if m.ptr == nil {
			t.Errorf("metric %s is nil", m.name)
		}
	}
}

func TestCrossSignalMetrics_Initialized(t *testing.T) {
	metrics := []struct {
		name string
		ptr  interface{}
	}{
		{"CrossEvictionSent", CrossEvictionSent},
		{"CrossEvictionReceived", CrossEvictionReceived},
		{"CrossEvictionPending", CrossEvictionPending},
		{"CrossEvictionApplied", CrossEvictionApplied},
		{"CrossPrefetchSent", CrossPrefetchSent},
		{"CrossPrefetchReceived", CrossPrefetchReceived},
	}

	for _, m := range metrics {
		if m.ptr == nil {
			t.Errorf("metric %s is nil", m.name)
		}
	}
}
