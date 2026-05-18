package bloomindex

import (
	"context"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// BloomControllerConfig holds runtime-tunable bloom parameters.
type BloomControllerConfig struct {
	Enabled              bool          `json:"enabled" yaml:"enabled"`
	Tier1MaxAge          time.Duration `json:"tier1_max_age" yaml:"tier1_max_age"`
	Tier2MaxAge          time.Duration `json:"tier2_max_age" yaml:"tier2_max_age"`
	Tier3MaxAge          time.Duration `json:"tier3_max_age" yaml:"tier3_max_age"`
	CacheMaxBytes        int64         `json:"cache_max_bytes" yaml:"cache_max_bytes"`
	TargetFileSize       int64         `json:"target_file_size" yaml:"target_file_size"`
	PartitionGranularity Granularity   `json:"partition_granularity" yaml:"partition_granularity"`
}

// DefaultBloomControllerConfig returns sensible defaults.
func DefaultBloomControllerConfig() BloomControllerConfig {
	return BloomControllerConfig{
		Enabled:              true,
		Tier1MaxAge:          7 * 24 * time.Hour,
		Tier2MaxAge:          30 * 24 * time.Hour,
		Tier3MaxAge:          90 * 24 * time.Hour,
		CacheMaxBytes:        8 * 1024 * 1024 * 1024,
		TargetFileSize:       128 * 1024 * 1024,
		PartitionGranularity: GranularityHour,
	}
}

// Adjustment records a controller parameter change.
type Adjustment struct {
	Time      time.Time `json:"time"`
	Parameter string    `json:"parameter"`
	OldValue  string    `json:"old_value"`
	NewValue  string    `json:"new_value"`
	Reason    string    `json:"reason"`
}

// BloomController observes system metrics and auto-tunes bloom parameters.
type BloomController struct {
	mu          sync.RWMutex
	cfg         BloomControllerConfig
	overrides   map[string]bool
	adjustments []Adjustment
	isLeader    bool
}

// NewBloomController creates a controller with the given initial config.
func NewBloomController(cfg BloomControllerConfig) *BloomController {
	return &BloomController{
		cfg:       cfg,
		overrides: make(map[string]bool),
	}
}

// Config returns the current configuration.
func (bc *BloomController) Config() BloomControllerConfig {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.cfg
}

// TierConfig returns the current tier boundaries.
func (bc *BloomController) TierConfig() TierConfig {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return TierConfig{
		Tier1MaxAge: bc.cfg.Tier1MaxAge,
		Tier2MaxAge: bc.cfg.Tier2MaxAge,
		Tier3MaxAge: bc.cfg.Tier3MaxAge,
	}
}

// SetLeader sets whether this node is the auto-tuning leader.
func (bc *BloomController) SetLeader(leader bool) {
	bc.mu.Lock()
	bc.isLeader = leader
	bc.mu.Unlock()
}

// IsLeader returns whether this node is the auto-tuning leader.
func (bc *BloomController) IsLeader() bool {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.isLeader
}

// PinOverride marks a parameter as operator-pinned, preventing auto-tuning.
func (bc *BloomController) PinOverride(param string) {
	bc.mu.Lock()
	bc.overrides[param] = true
	bc.mu.Unlock()
}

// IsPinned returns whether a parameter is operator-pinned.
func (bc *BloomController) IsPinned(param string) bool {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.overrides[param]
}

// Observe takes current system metrics and adjusts parameters if this node is leader.
func (bc *BloomController) Observe(_ context.Context, obs Observation) {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if !bc.isLeader {
		return
	}

	if obs.FilesPerHour > 3000 && !bc.overrides["target_file_size"] {
		oldSize := bc.cfg.TargetFileSize
		newSize := int64(512 * 1024 * 1024)
		if oldSize != newSize {
			bc.cfg.TargetFileSize = newSize
			bc.recordAdjustment("target_file_size", formatBytes(oldSize), formatBytes(newSize),
				"high volume (>3000 files/h)")
		}
	}

	if obs.SSDUsageRatio > 0.9 && !bc.overrides["tier1_max_age"] {
		old := bc.cfg.Tier1MaxAge
		reduced := old - 24*time.Hour
		if reduced < 24*time.Hour {
			reduced = 24 * time.Hour
		}
		if reduced != old {
			bc.cfg.Tier1MaxAge = reduced
			bc.recordAdjustment("tier1_max_age", old.String(), reduced.String(),
				"SSD usage >90%")
		}
	}

	if obs.SSDUsageRatio < 0.5 && obs.SSDUsageRatio > 0 && !bc.overrides["tier1_max_age"] {
		old := bc.cfg.Tier1MaxAge
		expanded := old + 24*time.Hour
		if expanded > 14*24*time.Hour {
			expanded = 14 * 24 * time.Hour
		}
		if expanded != old {
			bc.cfg.Tier1MaxAge = expanded
			bc.recordAdjustment("tier1_max_age", old.String(), expanded.String(),
				"SSD usage <50%")
		}
	}

	if obs.FilesPerHour > 0 && obs.FilesPerHour < 50 && !bc.overrides["partition_granularity"] {
		if bc.cfg.PartitionGranularity != GranularityDay {
			bc.cfg.PartitionGranularity = GranularityDay
			bc.recordAdjustment("partition_granularity", "hour", "day",
				"low file rate (<50/h)")
		}
	}
}

func (bc *BloomController) recordAdjustment(param, oldVal, newVal, reason string) {
	adj := Adjustment{
		Time:      time.Now(),
		Parameter: param,
		OldValue:  oldVal,
		NewValue:  newVal,
		Reason:    reason,
	}
	bc.adjustments = append(bc.adjustments, adj)
	metrics.BloomControllerAdj.Inc(param)
	logger.Infof("bloom_controller: adjusted %s from %s to %s — %s", param, oldVal, newVal, reason)
}

// Adjustments returns a copy of all recorded adjustments.
func (bc *BloomController) Adjustments() []Adjustment {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	out := make([]Adjustment, len(bc.adjustments))
	copy(out, bc.adjustments)
	return out
}

// ApplyConfig replaces the current config (e.g., from S3 sync).
func (bc *BloomController) ApplyConfig(cfg BloomControllerConfig) {
	bc.mu.Lock()
	bc.cfg = cfg
	bc.mu.Unlock()
}

// Observation contains metrics observed by the controller.
type Observation struct {
	FilesPerHour  int
	SSDUsageRatio float64
	CacheHitRate  float64
	BloomHitRate  float64
}

func formatBytes(b int64) string {
	const mb = 1024 * 1024
	if b >= 1024*mb {
		return formatInt(b/(1024*mb)) + "GB"
	}
	return formatInt(b/mb) + "MB"
}

func formatInt(n int64) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}
