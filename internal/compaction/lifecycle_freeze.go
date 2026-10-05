package compaction

import (
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// Freeze reasons, also the label values of the frozen-files gauge.
const (
	frozenStorageClass = "storage_class"
	frozenAge          = "age"
	// frozenSizeAge: a group past SizeMergeMaxAge with no lifecycle rule; only
	// stale-schema heal may still merge it.
	frozenSizeAge = "size_age"
)

// defaultSizeMergeMaxAge bounds size merges where no lifecycle rule says when
// objects leave STANDARD: backfill into old data cannot keep rewriting it.
const defaultSizeMergeMaxAge = 7 * 24 * time.Hour

// defaultFreezeMargin is how long before the first lifecycle transition out of
// a rewritable class compaction stops touching a partition (first transition
// − 2 d). S3 runs lifecycle asynchronously, so an
// object can move up to about a day after it becomes eligible; the margin keeps
// compaction clear of that window.
const defaultFreezeMargin = 48 * time.Hour

// LifecycleFreeze keeps compaction away from objects S3 lifecycle has moved,
// or is about to move, out of STANDARD / INTELLIGENT_TIERING. Rewriting those
// costs a retrieval fee plus the early-deletion charge for the rest of the
// class's minimum duration, and fails outright in Glacier Flexible Retrieval
// and Deep Archive.
//
// Lakehouse does not HEAD objects to learn their class. It uses what it
// already has: the class S3 reports for each object in the manifest's LIST
// refresh (FileInfo.StorageClass, updated on every refresh), and the
// lifecycle rules mirrored in config (per tenant, then global). A partition
// whose data is older than the first transition out of a rewritable class,
// minus Margin, is frozen. Partition time is never later than the creation
// time of any object in it, so this is conservative. With no such rule,
// SizeMergeMaxAge bounds size merges instead.
type LifecycleFreeze struct {
	// Detector carries delete.lifecycle_rules and the per-tenant lifecycle
	// overrides. Optional.
	Detector *delete.StorageClassDetector
	// ExtraRules are further global rules that describe the same bucket
	// (stats.s3_lifecycle_rules). The earliest transition of either set wins.
	ExtraRules []delete.LifecycleRule
	// Margin is subtracted from the first transition; 0 means 48h. It is
	// capped at half the transition, so a 1-day rule freezes at 12h rather
	// than freezing everything.
	Margin time.Duration
	// SizeMergeMaxAge: for a tenant with no rule that moves objects out of a
	// rewritable class, partitions older than this get no size merges (the
	// level thresholds, the closed-hour rollup, the fragmentation hint);
	// stale-schema heal still runs. 0 means 7 days; negative disables it.
	SizeMergeMaxAge time.Duration
}

// classFrozen reports whether a file's recorded class (from the LIST refresh,
// or set by a writer) is one whose objects must not be rewritten.
func classFrozen(fi *manifest.FileInfo) bool {
	switch fi.StorageClass {
	case "", string(delete.ClassStandard), string(delete.ClassIntelligentTiering):
		return false
	}
	return !delete.ParseStorageClass(fi.StorageClass).CanRewrite()
}

// freezeLimits is what the lifecycle says about one tenant: from which
// partition age its objects are frozen, or, with no rule, from which age size
// merges stop.
type freezeLimits struct {
	hasRule    bool
	freezeAge  time.Duration // valid when hasRule
	sizeMaxAge time.Duration // valid when !hasRule; <= 0 = unbounded
}

func (l freezeLimits) frozenAt(age time.Duration) bool { return l.hasRule && age >= l.freezeAge }

func (l freezeLimits) sizeMergesAt(age time.Duration) bool {
	return l.hasRule || l.sizeMaxAge <= 0 || age < l.sizeMaxAge
}

// limitsFromTransition turns a first transition into limits.
func (f *LifecycleFreeze) limitsFromTransition(days int, ok bool) freezeLimits {
	if !ok {
		max := f.SizeMergeMaxAge
		if max == 0 {
			max = defaultSizeMergeMaxAge
		}
		return freezeLimits{sizeMaxAge: max}
	}
	transition := time.Duration(days) * 24 * time.Hour
	margin := f.Margin
	if margin <= 0 {
		margin = defaultFreezeMargin
	}
	if margin > transition/2 {
		margin = transition / 2
	}
	return freezeLimits{hasRule: true, freezeAge: transition - margin}
}

// limits returns the limits for the tenant of key (any key or prefix that
// starts with "<account>/<project>/").
func (f *LifecycleFreeze) limits(key string) freezeLimits {
	days, ok := delete.FirstNonRewritableTransition(f.ExtraRules)
	if f.Detector != nil {
		if d, has := f.Detector.FirstNonRewritableTransition(key); has && (!ok || d < days) {
			days, ok = d, true
		}
	}
	return f.limitsFromTransition(days, ok)
}

// FreezeAge returns the partition age from which tenant key's data is frozen,
// and false when no lifecycle rule applies to it. For startup checks.
func (f *LifecycleFreeze) FreezeAge(key string) (time.Duration, bool) {
	if f == nil {
		return 0, false
	}
	l := f.limits(key)
	return l.freezeAge, l.hasRule
}

// freezeView is one scan's read of the freeze: the global limits once, and
// per-tenant limits only when per-tenant overrides exist, cached by group
// prefix. A nil *freezeView freezes nothing but recorded classes.
type freezeView struct {
	f         *LifecycleFreeze
	global    freezeLimits
	perTenant bool
	cache     map[string]freezeLimits
	classes   map[string]struct{}
}

func (f *LifecycleFreeze) view() *freezeView {
	if f == nil {
		return nil
	}
	v := &freezeView{f: f}
	gd, gok := delete.FirstNonRewritableTransition(f.ExtraRules)
	if f.Detector != nil {
		v.classes = f.Detector.CachedNonRewritableKeys()
		if d, has := f.Detector.FirstGlobalNonRewritableTransition(); has && (!gok || d < gd) {
			gd, gok = d, true
		}
		v.perTenant = f.Detector.HasTenantRules()
	}
	v.global = f.limitsFromTransition(gd, gok)
	if v.perTenant {
		v.cache = make(map[string]freezeLimits)
	}
	return v
}

// limitsFor returns the limits of a tenant group by its prefix.
func (v *freezeView) limitsFor(prefix string) freezeLimits {
	if v == nil {
		return freezeLimits{}
	}
	if !v.perTenant || prefix == "" {
		return v.global
	}
	if l, ok := v.cache[prefix]; ok {
		return l
	}
	l := v.f.limits(prefix)
	v.cache[prefix] = l
	return l
}

func (v *freezeView) classFrozen(fi *manifest.FileInfo) bool {
	if classFrozen(fi) {
		return true
	}
	if v != nil && len(v.classes) > 0 {
		_, ok := v.classes[fi.Key]
		return ok
	}
	return false
}

// frozen reports whether fi must stay out of compaction, and why: its recorded
// class (checked even when f is nil), or its partition's age against the
// tenant's first lifecycle transition.
func (f *LifecycleFreeze) frozen(fi manifest.FileInfo, partitionTime, now time.Time) (bool, string) {
	if classFrozen(&fi) {
		return true, frozenStorageClass
	}
	if f == nil {
		return false, ""
	}
	if f.Detector != nil {
		if class, ok := f.Detector.GetCached(fi.Key); ok && !class.CanRewrite() {
			return true, frozenStorageClass
		}
	}
	if f.limits(fi.Key).frozenAt(now.Sub(partitionTime)) {
		return true, frozenAge
	}
	return false, ""
}
