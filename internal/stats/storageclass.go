package stats

import (
	"sort"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// StorageClassTracker predicts the S3 storage class for objects based on
// lifecycle transition rules and object age. It supports per-tenant overrides.
type StorageClassTracker struct {
	defaultRules []config.LifecycleRuleConfig            // sorted by TransitionDays desc
	tenantRules  map[string][]config.LifecycleRuleConfig // tenant key -> rules (each sorted desc)
}

// NewStorageClassTracker creates a tracker with the given default lifecycle rules
// and optional per-tenant rule overrides. All rule slices are copied and sorted
// by TransitionDays descending so that PredictClass walks longest-first.
func NewStorageClassTracker(defaultRules []config.LifecycleRuleConfig, tenantRules map[string][]config.LifecycleRuleConfig) *StorageClassTracker {
	sct := &StorageClassTracker{
		defaultRules: sortedCopy(defaultRules),
		tenantRules:  make(map[string][]config.LifecycleRuleConfig, len(tenantRules)),
	}
	for k, v := range tenantRules {
		sct.tenantRules[k] = sortedCopy(v)
	}
	return sct
}

// sortedCopy returns a copy of rules sorted by TransitionDays descending.
func sortedCopy(rules []config.LifecycleRuleConfig) []config.LifecycleRuleConfig {
	cp := make([]config.LifecycleRuleConfig, len(rules))
	copy(cp, rules)
	sort.Slice(cp, func(i, j int) bool {
		return cp[i].TransitionDays > cp[j].TransitionDays
	})
	return cp
}

// PredictClass returns the predicted storage class for an object created at
// createdAt as of time now, using the default lifecycle rules.
// Returns "STANDARD" if no rule matches.
func (sct *StorageClassTracker) PredictClass(createdAt, now time.Time) string {
	return predictClass(sct.defaultRules, createdAt, now)
}

// PredictClassForTenant returns the predicted storage class using tenant-specific
// rules if available, falling back to default rules otherwise.
func (sct *StorageClassTracker) PredictClassForTenant(createdAt, now time.Time, tenant string) string {
	if rules, ok := sct.tenantRules[tenant]; ok {
		return predictClass(rules, createdAt, now)
	}
	return predictClass(sct.defaultRules, createdAt, now)
}

// NearBoundary reports whether an object created at createdAt is within 2 days
// before any default transition boundary as of time now.
func (sct *StorageClassTracker) NearBoundary(createdAt, now time.Time) bool {
	return nearBoundary(sct.defaultRules, createdAt, now)
}

// NearBoundaryForTenant checks proximity to transition boundaries using
// tenant-specific rules if available, falling back to defaults.
func (sct *StorageClassTracker) NearBoundaryForTenant(createdAt, now time.Time, tenant string) bool {
	if rules, ok := sct.tenantRules[tenant]; ok {
		return nearBoundary(rules, createdAt, now)
	}
	return nearBoundary(sct.defaultRules, createdAt, now)
}

// DefaultRules returns a copy of the default lifecycle rules (sorted desc).
func (sct *StorageClassTracker) DefaultRules() []config.LifecycleRuleConfig {
	cp := make([]config.LifecycleRuleConfig, len(sct.defaultRules))
	copy(cp, sct.defaultRules)
	return cp
}

// predictClass walks rules (already sorted TransitionDays desc) and returns the
// StorageClass of the first rule whose TransitionDays <= ageDays.
func predictClass(rules []config.LifecycleRuleConfig, createdAt, now time.Time) string {
	ageDays := int(now.Sub(createdAt).Hours() / 24)
	for _, r := range rules {
		if ageDays >= r.TransitionDays {
			return r.StorageClass
		}
	}
	return "STANDARD"
}

// nearBoundary returns true if the object's age is within (0, 2] days before
// any rule's TransitionDays boundary: diff > 0 && diff <= 2.
func nearBoundary(rules []config.LifecycleRuleConfig, createdAt, now time.Time) bool {
	ageDays := int(now.Sub(createdAt).Hours() / 24)
	for _, r := range rules {
		diff := r.TransitionDays - ageDays
		if diff > 0 && diff <= 2 {
			return true
		}
	}
	return false
}
