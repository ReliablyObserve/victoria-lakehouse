package compaction

import (
	"fmt"
	"time"
)

// RollupWindowConflict reports whether a lifecycle freeze age leaves no room
// for the closed-hour rollup: the rollup runs on partitions at least
// dailyRollupAge old (floored at one hour, 0 disables it), and a partition is
// frozen from freezeAge on, so with freezeAge <= the rollup age the rollup can
// never run for that data. The few small files a quiet tenant leaves in an
// hour then stay unmerged for good.
func RollupWindowConflict(freezeAge, dailyRollupAge time.Duration) bool {
	rollup := (&LevelPolicy{DailyRollupAge: dailyRollupAge}).rollupAge()
	return rollup > 0 && freezeAge <= rollup
}

// RollupConflictWarnings returns one startup warning line per scope whose
// freeze age conflicts with the rollup: the global rules, and each tenant
// override. tenantPrefixes are "<account>/<project>/" prefixes of tenants with
// their own lifecycle rules; a nil f or no conflict gives no lines.
func (f *LifecycleFreeze) RollupConflictWarnings(dailyRollupAge time.Duration, tenantPrefixes []string) []string {
	if f == nil {
		return nil
	}
	var out []string
	add := func(scope string, age time.Duration) {
		out = append(out, fmt.Sprintf(
			"compaction: the lifecycle freeze for %s starts at partition age %s, not later than compaction.daily_rollup_age %s; "+
				"the closed-hour rollup can never run for that data, so a tenant's small files in an hour stay unmerged. "+
				"Move the first lifecycle transition out later, or lower compaction.daily_rollup_age below the freeze age",
			scope, age, dailyRollupAge))
	}
	if age, ok := f.FreezeAge(""); ok && RollupWindowConflict(age, dailyRollupAge) {
		add("the global lifecycle rules", age)
	}
	for _, prefix := range tenantPrefixes {
		if age, ok := f.FreezeAge(prefix); ok && RollupWindowConflict(age, dailyRollupAge) {
			add("tenant "+prefix, age)
		}
	}
	return out
}
