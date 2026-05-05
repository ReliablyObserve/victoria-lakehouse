package delete

import (
	"strings"
	"sync"
)

// StorageClass represents an S3 storage class.
type StorageClass string

const (
	ClassStandard           StorageClass = "STANDARD"
	ClassStandardIA         StorageClass = "STANDARD_IA"
	ClassOnezoneIA          StorageClass = "ONEZONE_IA"
	ClassGlacierIR          StorageClass = "GLACIER_IR"
	ClassGlacier            StorageClass = "GLACIER"
	ClassDeepArchive        StorageClass = "DEEP_ARCHIVE"
	ClassIntelligentTiering StorageClass = "INTELLIGENT_TIERING"
)

// ParseStorageClass parses a string into a StorageClass.
// Empty string returns ClassStandard (S3 default).
func ParseStorageClass(s string) StorageClass {
	if s == "" {
		return ClassStandard
	}
	return StorageClass(strings.ToUpper(s))
}

// CanRewrite reports whether objects in this storage class can be
// rewritten without incurring retrieval costs.
func (sc StorageClass) CanRewrite() bool {
	switch sc {
	case ClassStandard, ClassIntelligentTiering:
		return true
	default:
		return false
	}
}

// IsArchive reports whether this storage class is an archive tier
// that would incur significant retrieval costs.
func (sc StorageClass) IsArchive() bool {
	switch sc {
	case ClassGlacier, ClassGlacierIR, ClassDeepArchive:
		return true
	default:
		return false
	}
}

// RewriteCost holds the estimated costs for rewriting a file.
type RewriteCost struct {
	RetrievalCostUSD float64
	PutCostUSD       float64
	GetCostUSD       float64
	TotalCostUSD     float64
}

// EstimateRewriteCost calculates the estimated cost of rewriting
// an object from the given storage class.
func EstimateRewriteCost(class StorageClass, sizeBytes int64) RewriteCost {
	const (
		gbBytes = 1024 * 1024 * 1024

		getCostPer1000  = 0.0004
		putCostPer1000  = 0.005
		retrievalStdIA  = 0.01 // per GB
		retrievalOneZ   = 0.01 // per GB
		retrievalGlacIR = 0.03 // per GB
		retrievalGlac   = 0.03 // per GB
		retrievalDeep   = 0.09 // per GB
	)

	sizeGB := float64(sizeBytes) / float64(gbBytes)

	var retrievalPerGB float64
	switch class {
	case ClassStandard, ClassIntelligentTiering:
		retrievalPerGB = 0
	case ClassStandardIA:
		retrievalPerGB = retrievalStdIA
	case ClassOnezoneIA:
		retrievalPerGB = retrievalOneZ
	case ClassGlacierIR:
		retrievalPerGB = retrievalGlacIR
	case ClassGlacier:
		retrievalPerGB = retrievalGlac
	case ClassDeepArchive:
		retrievalPerGB = retrievalDeep
	}

	rc := RewriteCost{
		RetrievalCostUSD: sizeGB * retrievalPerGB,
		GetCostUSD:       getCostPer1000 / 1000, // 1 GET request
		PutCostUSD:       putCostPer1000 / 1000, // 1 PUT request
	}
	rc.TotalCostUSD = rc.RetrievalCostUSD + rc.GetCostUSD + rc.PutCostUSD
	return rc
}

// LifecycleRule describes a single S3 lifecycle transition rule.
type LifecycleRule struct {
	TransitionDays int
	Class          StorageClass
}

// PredictClassFromAge predicts the storage class of a file based on
// its age and the lifecycle rules. Rules should be sorted by TransitionDays
// ascending. Returns ClassStandard if no rule threshold is exceeded.
func PredictClassFromAge(rules []LifecycleRule, fileAgeHours float64) StorageClass {
	fileAgeDays := fileAgeHours / 24.0
	result := ClassStandard
	for _, r := range rules {
		if fileAgeDays >= float64(r.TransitionDays) {
			result = r.Class
		} else {
			break
		}
	}
	return result
}

// StorageClassDetector detects storage class using lifecycle rules
// and an optional cache of known classes.
type StorageClassDetector struct {
	rules []LifecycleRule
	mu    sync.RWMutex
	cache map[string]StorageClass
}

// NewStorageClassDetector creates a detector with the given lifecycle rules.
func NewStorageClassDetector(rules []LifecycleRule) *StorageClassDetector {
	return &StorageClassDetector{
		rules: rules,
		cache: make(map[string]StorageClass),
	}
}

// Detect predicts the storage class from the file's age in hours.
func (d *StorageClassDetector) Detect(fileAgeHours float64) StorageClass {
	return PredictClassFromAge(d.rules, fileAgeHours)
}

// SetCache manually sets a cached storage class for a key.
func (d *StorageClassDetector) SetCache(key string, class StorageClass) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cache[key] = class
}

// GetCached retrieves a cached storage class for a key.
func (d *StorageClassDetector) GetCached(key string) (StorageClass, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	sc, ok := d.cache[key]
	return sc, ok
}
