package delete

import (
	"strconv"
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

// TenantLifecycleOverride is the input shape main.go uses to install
// per-tenant rule sets on a StorageClassDetector. Kept here (not
// imported from internal/tenant) so this package stays leaf-level.
type TenantLifecycleOverride struct {
	AccountID      uint32
	ProjectID      uint32
	TransitionDays []int
	Classes        []StorageClass
}

// BuildTenantRules folds overrides into the nested map shape
// SetTenantRules expects. Skips overrides whose TransitionDays /
// Classes slices have mismatched length or are empty — caller is
// responsible for upstream validation.
func BuildTenantRules(overrides []TenantLifecycleOverride) map[uint32]map[uint32][]LifecycleRule {
	out := make(map[uint32]map[uint32][]LifecycleRule)
	for _, ov := range overrides {
		if len(ov.TransitionDays) != len(ov.Classes) {
			continue
		}
		rules := make([]LifecycleRule, 0, len(ov.TransitionDays))
		for i := range ov.TransitionDays {
			rules = append(rules, LifecycleRule{
				TransitionDays: ov.TransitionDays[i],
				Class:          ov.Classes[i],
			})
		}
		byProject := out[ov.AccountID]
		if byProject == nil {
			byProject = make(map[uint32][]LifecycleRule)
			out[ov.AccountID] = byProject
		}
		byProject[ov.ProjectID] = rules
	}
	return out
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

// tenantKey identifies a (account, project) pair for per-tenant
// lifecycle overrides. Lowercase so callers in this package can pass
// it directly without exposing the type publicly.
type tenantKey struct{ Account, Project uint32 }

// StorageClassDetector detects storage class using lifecycle rules
// and an optional cache of known classes. Tenants with an override
// configured via tenant.PolicyRegistry get their own rules slice;
// everyone else falls back to the global rules.
type StorageClassDetector struct {
	rules     []LifecycleRule
	perTenant map[tenantKey][]LifecycleRule
	mu        sync.RWMutex
	cache     map[string]StorageClass
}

// NewStorageClassDetector creates a detector with the given lifecycle rules.
func NewStorageClassDetector(rules []LifecycleRule) *StorageClassDetector {
	return &StorageClassDetector{
		rules:     rules,
		perTenant: make(map[tenantKey][]LifecycleRule),
		cache:     make(map[string]StorageClass),
	}
}

// SetTenantRules installs per-tenant lifecycle overrides. Each map
// entry replaces the global ruleset for its (account, project) pair.
// Safe to call after construction — subsequent Detect / DetectForKey
// calls pick up the new rules immediately.
func (d *StorageClassDetector) SetTenantRules(perTenant map[uint32]map[uint32][]LifecycleRule) {
	if perTenant == nil {
		return
	}
	merged := make(map[tenantKey][]LifecycleRule)
	for acc, byProject := range perTenant {
		for proj, rules := range byProject {
			merged[tenantKey{acc, proj}] = rules
		}
	}
	d.mu.Lock()
	d.perTenant = merged
	d.mu.Unlock()
}

// Detect predicts the storage class from the file's age in hours
// using the global ruleset. Tenant-unaware callers continue to use
// this; tenant-aware callers should use DetectForKey instead.
func (d *StorageClassDetector) Detect(fileAgeHours float64) StorageClass {
	return PredictClassFromAge(d.rules, fileAgeHours)
}

// DetectForKey predicts the storage class for a file, using
// per-tenant lifecycle rules when the file's S3 key parses to a
// known tenant prefix ("{account}/{project}/..."). Falls back to the
// global rules otherwise so files written before Phase 1's per-tenant
// prefix layout still resolve correctly.
func (d *StorageClassDetector) DetectForKey(fileAgeHours float64, key string) StorageClass {
	if acc, proj, ok := parseTenantFromKey(key); ok {
		d.mu.RLock()
		rules, has := d.perTenant[tenantKey{acc, proj}]
		d.mu.RUnlock()
		if has {
			return PredictClassFromAge(rules, fileAgeHours)
		}
	}
	return PredictClassFromAge(d.rules, fileAgeHours)
}

// parseTenantFromKey extracts (account, project) from a tenant-isolated
// S3 key like "1002/0/logs/dt=.../foo.parquet". Returns ok=false for
// any key that doesn't match the expected layout (legacy single-prefix
// deployments, sidecars under _meta/, etc.).
func parseTenantFromKey(key string) (uint32, uint32, bool) {
	parts := strings.SplitN(key, "/", 4)
	if len(parts) < 3 {
		return 0, 0, false
	}
	acc, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, 0, false
	}
	proj, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, 0, false
	}
	return uint32(acc), uint32(proj), true
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
