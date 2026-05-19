package bloomindex

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// BloomStatusResponse is the JSON shape of /api/v1/bloom/status.
type BloomStatusResponse struct {
	Enabled        bool                 `json:"enabled"`
	Mode           string               `json:"mode"`
	IndexedColumns []string             `json:"indexed_columns,omitempty"`
	AutoTuning     *AutoTuningStatus    `json:"auto_tuning,omitempty"`
	Tiers          map[string]TierStats `json:"tiers"`
	Cache          CacheStats           `json:"cache"`
}

// AutoTuningStatus contains current auto-tuning state.
type AutoTuningStatus struct {
	Tier1MaxAge          string       `json:"tier1_max_age"`
	Tier2MaxAge          string       `json:"tier2_max_age"`
	Tier3MaxAge          string       `json:"tier3_max_age"`
	TargetFileSize       int64        `json:"target_file_size"`
	PartitionGranularity string       `json:"partition_granularity"`
	CacheMaxBytes        int64        `json:"cache_max_bytes"`
	RecentAdjustments    []Adjustment `json:"recent_adjustments,omitempty"`
}

// TierStats for a single bloom tier.
type TierStats struct {
	Partitions int    `json:"partitions"`
	Entries    int    `json:"entries"`
	Bytes      int64  `json:"bytes"`
	AgeRange   string `json:"age_range"`
}

// CacheStats for bloom cache layers.
type CacheStats struct {
	MemoryBytesUsed  int `json:"memory_bytes_used"`
	MemoryBytesLimit int `json:"memory_bytes_limit"`
	Partitions       int `json:"partitions"`
}

// StatusProvider supplies data for the bloom status endpoint.
type StatusProvider struct {
	Controller     *BloomController
	Cache          *BloomCache
	Mode           string
	IndexedColumns []string
}

// HandleBloomStatus returns an HTTP handler for GET /api/v1/bloom/status.
func HandleBloomStatus(sp *StatusProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		resp := BloomStatusResponse{
			Mode:           sp.Mode,
			IndexedColumns: sp.IndexedColumns,
			Tiers: map[string]TierStats{
				"hot":     {AgeRange: "0-7d"},
				"warm":    {AgeRange: "7-30d"},
				"cold":    {AgeRange: "30-90d"},
				"archive": {AgeRange: "90d+"},
			},
		}

		if sp.Controller != nil {
			cfg := sp.Controller.Config()
			resp.Enabled = cfg.Enabled

			granStr := "hour"
			if cfg.PartitionGranularity == GranularityDay {
				granStr = "day"
			}

			adjs := sp.Controller.Adjustments()
			recent := adjs
			if len(recent) > 10 {
				recent = recent[len(recent)-10:]
			}

			resp.AutoTuning = &AutoTuningStatus{
				Tier1MaxAge:          cfg.Tier1MaxAge.String(),
				Tier2MaxAge:          cfg.Tier2MaxAge.String(),
				Tier3MaxAge:          cfg.Tier3MaxAge.String(),
				TargetFileSize:       cfg.TargetFileSize,
				PartitionGranularity: granStr,
				CacheMaxBytes:        cfg.CacheMaxBytes,
				RecentAdjustments:    recent,
			}
		}

		if sp.Cache != nil {
			resp.Cache = CacheStats{
				MemoryBytesUsed:  sp.Cache.Size(),
				MemoryBytesLimit: sp.Cache.maxSize,
				Partitions:       sp.Cache.Len(),
			}

			var tc TierConfig
			if sp.Controller != nil {
				tc = sp.Controller.TierConfig()
			} else {
				tc = DefaultTierConfig()
			}
			tierCounts := map[string]int{"hot": 0, "warm": 0, "cold": 0, "archive": 0}
			now := time.Now()
			for _, name := range sp.Cache.PartitionNames() {
				age := partitionAge(name, now)
				tier := TierForAge(age, tc)
				tierCounts[tier.String()]++
			}
			for tierName, count := range tierCounts {
				if ts, ok := resp.Tiers[tierName]; ok {
					ts.Partitions = count
					resp.Tiers[tierName] = ts
				}
				metrics.BloomTierPartitions.Set(tierName, int64(count))
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func partitionAge(partition string, now time.Time) time.Duration {
	dtIdx := strings.Index(partition, "dt=")
	if dtIdx < 0 {
		return 0
	}
	dateStr := partition[dtIdx+3:]
	if len(dateStr) < 10 {
		return 0
	}
	dateStr = dateStr[:10]
	t, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return 0
	}
	return now.Sub(t)
}
