package smartcache

import (
	"sync"
	"time"
)

// SizingConfig controls the cache sizing calculator behavior.
type SizingConfig struct {
	TargetHours int
}

// SizingCalculator estimates required cache size based on ingestion rate
// and query working set, blending both signals over uptime.
type SizingCalculator struct {
	mu                sync.RWMutex
	targetHours       int
	ingestionBytes    int64
	ingestionInterval time.Duration
	queryReads        map[int64]int64
}

// NewSizingCalculator creates a calculator with the given config.
// TargetHours defaults to 24 if zero or negative.
func NewSizingCalculator(cfg SizingConfig) *SizingCalculator {
	if cfg.TargetHours <= 0 {
		cfg.TargetHours = 24
	}
	return &SizingCalculator{
		targetHours: cfg.TargetHours,
		queryReads:  make(map[int64]int64),
	}
}

// RecordIngestion adds bytes to the cumulative ingestion counter.
func (s *SizingCalculator) RecordIngestion(bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ingestionBytes += bytes
}

// SetIngestionInterval sets the time period over which ingestion bytes
// were recorded, used to compute per-hour ingestion rate.
func (s *SizingCalculator) SetIngestionInterval(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ingestionInterval = d
}

// RecordQueryRead tracks a file read by query. Duplicate fileIDs are
// deduplicated (last write wins for size).
func (s *SizingCalculator) RecordQueryRead(fileID int64, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queryReads[fileID] = bytes
}

// IngestionEstimate returns the projected cache size based on ingestion
// rate extrapolated to TargetHours.
func (s *SizingCalculator) IngestionEstimate() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.ingestionInterval <= 0 || s.ingestionBytes <= 0 {
		return 0
	}

	bytesPerHour := float64(s.ingestionBytes) / s.ingestionInterval.Hours()
	return int64(bytesPerHour * float64(s.targetHours))
}

// QueryEstimate returns the total unique bytes read by queries,
// representing the observed working set.
func (s *SizingCalculator) QueryEstimate() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var total int64
	for _, bytes := range s.queryReads {
		total += bytes
	}
	return total
}

// BlendedEstimate combines ingestion and query estimates. At uptime=0
// it returns the ingestion estimate; at uptime>=12h it returns the
// query estimate. In between it linearly interpolates. If only one
// signal is available, that signal is returned regardless of uptime.
func (s *SizingCalculator) BlendedEstimate(uptime time.Duration) int64 {
	ingEst := s.IngestionEstimate()
	qEst := s.QueryEstimate()

	if ingEst == 0 && qEst == 0 {
		return 0
	}
	if ingEst == 0 {
		return qEst
	}
	if qEst == 0 {
		return ingEst
	}

	hours := uptime.Hours()
	weight := hours / 12.0
	if weight > 1.0 {
		weight = 1.0
	}
	if weight < 0 {
		weight = 0
	}

	return int64(float64(1-weight)*float64(ingEst) + weight*float64(qEst))
}

// RecommendedPerNode divides the blended estimate across fleetSize nodes.
func (s *SizingCalculator) RecommendedPerNode(uptime time.Duration, fleetSize int) int64 {
	total := s.BlendedEstimate(uptime)
	if fleetSize <= 1 {
		return total
	}
	return total / int64(fleetSize)
}
