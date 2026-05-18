package compaction

import "time"

// CompressionTier defines age-based compression levels.
type CompressionTier struct {
	MaxAge time.Duration
	Level  int
}

// DefaultCompressionTiers returns the standard tiered compression schedule.
func DefaultCompressionTiers() []CompressionTier {
	return []CompressionTier{
		{MaxAge: 7 * 24 * time.Hour, Level: 3},
		{MaxAge: 30 * 24 * time.Hour, Level: 7},
		{MaxAge: 0, Level: 17},
	}
}

// CompressionLevelForAge returns the compression level for data of the given age.
func CompressionLevelForAge(age time.Duration, tiers []CompressionTier) int {
	for _, t := range tiers {
		if t.MaxAge > 0 && age < t.MaxAge {
			return t.Level
		}
	}
	if len(tiers) > 0 {
		return tiers[len(tiers)-1].Level
	}
	return 3
}
