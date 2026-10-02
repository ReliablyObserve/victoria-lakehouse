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
)

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
// already has: a class recorded on the manifest entry, a class the detector
// has cached, and the lifecycle rules mirrored in config (per tenant, then
// global). A partition whose data is older than the first transition out of a
// rewritable class, minus Margin, is frozen. Partition time is never later
// than the creation time of any object in it, so this is conservative.
type LifecycleFreeze struct {
	// Detector carries delete.lifecycle_rules and the per-tenant lifecycle
	// overrides, plus any classes it has cached. Optional.
	Detector *delete.StorageClassDetector
	// ExtraRules are further global rules that describe the same bucket
	// (stats.s3_lifecycle_rules). The earliest transition of either set wins.
	ExtraRules []delete.LifecycleRule
	// Margin is subtracted from the first transition; 0 means 48h. It is
	// capped at half the transition, so a 1-day rule freezes at 12h rather
	// than freezing everything.
	Margin time.Duration
}

// frozen reports whether fi must stay out of compaction. The recorded storage
// class is checked even when f is nil: a non-rewritable class is never
// rewritten, whatever the config.
func (f *LifecycleFreeze) frozen(fi manifest.FileInfo, partitionTime, now time.Time) (bool, string) {
	if fi.StorageClass != "" && !delete.ParseStorageClass(fi.StorageClass).CanRewrite() {
		return true, frozenStorageClass
	}
	if f == nil {
		return false, ""
	}
	if f.Detector != nil {
		if c, ok := f.Detector.GetCached(fi.Key); ok && !c.CanRewrite() {
			return true, frozenStorageClass
		}
	}
	days, ok := f.firstTransition(fi.Key)
	if !ok {
		return false, ""
	}
	transition := time.Duration(days) * 24 * time.Hour
	margin := f.Margin
	if margin <= 0 {
		margin = defaultFreezeMargin
	}
	if margin > transition/2 {
		margin = transition / 2
	}
	if now.Sub(partitionTime) >= transition-margin {
		return true, frozenAge
	}
	return false, ""
}

func (f *LifecycleFreeze) firstTransition(key string) (int, bool) {
	days, ok := delete.FirstNonRewritableTransition(f.ExtraRules)
	if f.Detector != nil {
		if d, has := f.Detector.FirstNonRewritableTransition(key); has && (!ok || d < days) {
			days, ok = d, true
		}
	}
	return days, ok
}
