package bloomindex

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Tier int

const (
	TierHot     Tier = iota // 0-7d: per-row-group bloom
	TierWarm                // 7-30d: per-file bloom
	TierCold                // 30-90d: per-partition summary bloom
	TierArchive             // 90d+: no bloom
)

func (t Tier) String() string {
	switch t {
	case TierHot:
		return "hot"
	case TierWarm:
		return "warm"
	case TierCold:
		return "cold"
	case TierArchive:
		return "archive"
	default:
		return fmt.Sprintf("tier(%d)", int(t))
	}
}

type TierConfig struct {
	Tier1MaxAge time.Duration // hot→warm boundary (default: 7d)
	Tier2MaxAge time.Duration // warm→cold boundary (default: 30d)
	Tier3MaxAge time.Duration // cold→archive boundary (default: 90d)
}

func DefaultTierConfig() TierConfig {
	return TierConfig{
		Tier1MaxAge: 7 * 24 * time.Hour,
		Tier2MaxAge: 30 * 24 * time.Hour,
		Tier3MaxAge: 90 * 24 * time.Hour,
	}
}

func (c TierConfig) Validate() error {
	if c.Tier1MaxAge >= c.Tier2MaxAge {
		return fmt.Errorf("tier1_max_age (%v) must be < tier2_max_age (%v)", c.Tier1MaxAge, c.Tier2MaxAge)
	}
	if c.Tier2MaxAge >= c.Tier3MaxAge {
		return fmt.Errorf("tier2_max_age (%v) must be < tier3_max_age (%v)", c.Tier2MaxAge, c.Tier3MaxAge)
	}
	return nil
}

type TierConfigOverride struct {
	Tier1MaxAge *time.Duration
	Tier2MaxAge *time.Duration
	Tier3MaxAge *time.Duration
}

func (c TierConfig) ApplyOverride(o TierConfigOverride) TierConfig {
	result := c
	if o.Tier1MaxAge != nil {
		result.Tier1MaxAge = *o.Tier1MaxAge
	}
	if o.Tier2MaxAge != nil {
		result.Tier2MaxAge = *o.Tier2MaxAge
	}
	if o.Tier3MaxAge != nil {
		result.Tier3MaxAge = *o.Tier3MaxAge
	}
	return result
}

func TierForAge(age time.Duration, cfg TierConfig) Tier {
	switch {
	case age < cfg.Tier1MaxAge:
		return TierHot
	case age < cfg.Tier2MaxAge:
		return TierWarm
	case age < cfg.Tier3MaxAge:
		return TierCold
	default:
		return TierArchive
	}
}

const SummaryKey = "summary"

const maxBloomCardinality = 50000

func ShouldSkipBloom(distinctCount int) bool {
	return distinctCount > maxBloomCardinality
}

func PerRGKey(fileKey string, rgIndex int) string {
	return fileKey + "#" + strconv.Itoa(rgIndex)
}

func ParseRGKey(key string) (fileKey string, rgIndex int, ok bool) {
	idx := strings.LastIndex(key, "#")
	if idx < 0 {
		return "", 0, false
	}
	rgStr := key[idx+1:]
	rg, err := strconv.Atoi(rgStr)
	if err != nil {
		return "", 0, false
	}
	return key[:idx], rg, true
}

func (idx *Index) Entries() map[string]map[string]*Filter {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	out := make(map[string]map[string]*Filter, len(idx.entries))
	for k, cols := range idx.entries {
		out[k] = cols
	}
	return out
}

func DowngradeToPerFile(idx *Index) *Index {
	entries := idx.Entries()

	perFile := make(map[string]map[string][]*Filter)
	for key, cols := range entries {
		fileKey, _, isRG := ParseRGKey(key)
		if !isRG {
			fileKey = key
		}
		if perFile[fileKey] == nil {
			perFile[fileKey] = make(map[string][]*Filter)
		}
		for col, f := range cols {
			perFile[fileKey][col] = append(perFile[fileKey][col], f)
		}
	}

	merged := New()
	for fileKey, columns := range perFile {
		for col, filters := range columns {
			union := filters[0]
			for _, f := range filters[1:] {
				union.MergeFrom(f)
			}
			merged.Add(fileKey, col, union)
		}
	}
	return merged
}

func DowngradeToSummary(idx *Index) *Index {
	entries := idx.Entries()

	colFilters := make(map[string][]*Filter)
	for _, cols := range entries {
		for col, f := range cols {
			colFilters[col] = append(colFilters[col], f)
		}
	}

	summary := New()
	for col, filters := range colFilters {
		union := filters[0]
		for _, f := range filters[1:] {
			union.MergeFrom(f)
		}
		summary.Add(SummaryKey, col, union)
	}
	return summary
}

func UnionLabels(hourly []map[string][]string) map[string][]string {
	result := make(map[string][]string)
	seen := make(map[string]map[string]bool)

	for _, h := range hourly {
		for col, vals := range h {
			if seen[col] == nil {
				seen[col] = make(map[string]bool)
			}
			for _, v := range vals {
				if !seen[col][v] {
					seen[col][v] = true
					result[col] = append(result[col], v)
				}
			}
		}
	}
	return result
}

const checksumSize = sha256.Size

func MarshalWithChecksum(idx *Index) []byte {
	data := idx.Marshal()
	h := sha256.Sum256(data)
	return append(data, h[:]...)
}

func UnmarshalWithChecksum(data []byte) (*Index, error) {
	if len(data) < checksumSize+5 {
		return nil, errors.New("data too short for checksum verification")
	}
	payload := data[:len(data)-checksumSize]
	stored := data[len(data)-checksumSize:]
	computed := sha256.Sum256(payload)
	for i := 0; i < checksumSize; i++ {
		if stored[i] != computed[i] {
			return nil, errors.New("bloom index checksum mismatch: data corrupted")
		}
	}
	return Unmarshal(payload)
}
