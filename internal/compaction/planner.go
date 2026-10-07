package compaction

import (
	"sort"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// planClock is the planner's clock: partition age, rollup age and the
// lifecycle freeze are all measured against it. A variable so tests and the
// write-amplification simulation can drive days of scans deterministically.
var planClock = time.Now

// matureBytes: a file at least this large is never selected to reduce the
// file count (closed-hour rollup, fragmentation hint). Merging it would rewrite
// a large object to absorb a few small ones, so write amplification under
// backfill would grow with the hour's total bytes instead of with the late
// data. Stale-schema heal and the open-hour thresholds still take it.
const matureBytes = manifest.MatureObjectBytes

// Why a merge was planned. The first two are open-hour merges driven by the
// level thresholds; the rest act on closed hours or on compaction hints.
const (
	reasonL0Count    = "l0_count"
	reasonL1Count    = "l1_count"
	reasonRollup     = "rollup"
	reasonStale      = "stale_schema"
	reasonFragmented = "fragmented"
)

// mergePlan is one merge: two or more files of ONE tenant group (tenant prefix
// + bucket, the unit the compactor writes one output for) in one partition.
// A plan never holds a single file, so nothing is ever rewritten 1 → 1.
type mergePlan struct {
	partition string
	// group identifies the tenant group (prefix + bucket) for failure backoff.
	group string
	// tenant is the fair-share key: "<account>/<project>" from the object
	// keys, or "default" for keys without a tenant prefix.
	tenant string
	// level is the source level handed to Compact; the output is level+1.
	level  int
	files  []manifest.FileInfo
	reason string
	time   time.Time
	bytes  int64
}

// open reports whether the plan merges an hour still filling up (level
// thresholds). Those go first: they bound the fan-out of the hours queries hit
// most, and a closed-hour rollup backlog must not starve them.
func (p mergePlan) open() bool { return p.reason == reasonL0Count || p.reason == reasonL1Count }

// debt is the files a plan removes per byte it rewrites: the heaviest
// small-file debt first.
func (p mergePlan) debt() float64 {
	b := p.bytes
	if b < 1 {
		b = 1
	}
	return float64(len(p.files)-1) / float64(b)
}

// groupKey is a tenant group: the tenant prefix of its keys and its bucket,
// the unit the compactor writes one output for.
type groupKey struct{ prefix, bucket string }

// planner plans every partition of one scan. It reuses its scratch map across
// partitions and copies a FileInfo only into a group that has a merge to plan,
// so a settled manifest costs a key parse and a map update per file and no
// allocation (RangePartitions hands it the manifest's own slices).
type planner struct {
	policy    *LevelPolicy
	currentFP string
	now       time.Time
	held      map[string]bool
	// fenced: some object was refused by the forward fence in this process, so
	// the planner checks each file against it (see fenceLog).
	fenced bool
	freeze *freezeView
	// seen, when set, is told how many files were kept out and why.
	seen    func(reason string, n int)
	counts  map[groupKey]int
	members map[groupKey][]int
}

func newPlanner(policy *LevelPolicy, currentFP string, now time.Time, held map[string]bool, freeze *LifecycleFreeze, seen func(string, int)) *planner {
	return &planner{
		policy: policy, currentFP: currentFP, now: now, held: held, fenced: fenceLog.Len() > 0, freeze: freeze.view(), seen: seen,
		counts: make(map[groupKey]int), members: make(map[groupKey][]int),
	}
}

func (pl *planner) saw(reason string, n int) {
	if pl.seen != nil && n > 0 {
		pl.seen(reason, n)
	}
}

// skip reports whether a file stays out before grouping: held (a delete
// rewrite swapped it in but has not recorded it), refused by the forward fence,
// or in a non-rewritable class.
func (pl *planner) skip(f *manifest.FileInfo) bool {
	if len(pl.held) > 0 && pl.held[f.Key] {
		return true
	}
	if pl.fenced && fenceLog.Has(fenceKey(*f)) {
		return true
	}
	return pl.freeze.classFrozen(f)
}

// partition plans the merges of one partition: files are split into tenant
// groups and each group is planned on its own, so one tenant's files never
// make another tenant's lone file look mergeable. files may be the manifest's
// own slice: it is only read, and plans hold copies.
func (pl *planner) partition(partition string, files []manifest.FileInfo, pt time.Time) []mergePlan {
	age := pl.now.Sub(pt)
	clear(pl.counts)
	classFrozenN := 0
	for i := range files {
		f := &files[i]
		if len(pl.held) > 0 && pl.held[f.Key] {
			continue
		}
		if pl.fenced && fenceLog.Has(fenceKey(*f)) {
			continue // refused by the forward fence: never planned again
		}
		if pl.freeze.classFrozen(f) {
			classFrozenN++
			continue
		}
		pl.counts[groupKey{manifest.CompactionGroupPrefix(f.Key), f.Bucket}]++
	}
	pl.saw(frozenStorageClass, classFrozenN)

	// Decide per group before touching files again; collect only groups that
	// may plan a merge.
	clear(pl.members)
	sizeOK := map[groupKey]bool(nil)
	for gk, n := range pl.counts {
		lim := pl.freeze.limitsFor(gk.prefix)
		if lim.frozenAt(age) {
			pl.saw(frozenAge, n)
			continue
		}
		if n < 2 {
			continue
		}
		ok := lim.sizeMergesAt(age)
		if !ok {
			pl.saw(frozenSizeAge, n)
		}
		if sizeOK == nil {
			sizeOK = make(map[groupKey]bool)
		}
		sizeOK[gk] = ok
		pl.members[gk] = nil
	}
	if len(pl.members) == 0 {
		return nil
	}
	for i := range files {
		f := &files[i]
		if pl.skip(f) {
			continue
		}
		gk := groupKey{manifest.CompactionGroupPrefix(f.Key), f.Bucket}
		if idx, ok := pl.members[gk]; ok {
			pl.members[gk] = append(idx, i)
		}
	}
	var plans []mergePlan
	for gk, idx := range pl.members {
		group := make([]manifest.FileInfo, len(idx))
		for j, i := range idx {
			group[j] = files[i]
		}
		level, selected, reason, ok := pl.policy.planGroup(group, pt, pl.now, pl.currentFP, sizeOK[gk])
		if !ok {
			continue
		}
		var bytes int64
		for _, f := range selected {
			bytes += f.Size
		}
		plans = append(plans, mergePlan{
			partition: partition,
			group:     gk.prefix + "|" + gk.bucket,
			tenant:    fairShareTenant(gk.prefix),
			level:     level,
			files:     selected,
			reason:    reason,
			time:      pt,
			bytes:     bytes,
		})
	}
	return plans
}

// planPartition plans one partition on its own (the Tier A steal and tests).
// frozenSeen, when set, is called once per file kept out, with the reason.
func (p *LevelPolicy) planPartition(partition string, files []manifest.FileInfo, pt, now time.Time, currentFP string, freeze *LifecycleFreeze, frozenSeen func(reason string)) []mergePlan {
	var seen func(string, int)
	if frozenSeen != nil {
		seen = func(r string, n int) {
			for i := 0; i < n; i++ {
				frozenSeen(r)
			}
		}
	}
	return newPlanner(p, currentFP, now, nil, freeze, seen).partition(partition, files, pt)
}

// planGroup decides the one merge, if any, for one tenant group's files in one
// partition. Every branch needs two or more selected files.
//
//  1. Open-hour thresholds: ≥ MinFilesL0 L0 files merge to L1; else
//     ≥ MinFilesL1 L1 files merge to L2.
//  2. Closed-hour rollup: once the partition is DailyRollupAge old, every
//     non-mature file of the group's majority schema merges into one, whatever
//     its level. This replaces "≥ 2 L1 files", which (counted per partition)
//     fired forever on one file per tenant and (counted per tenant) would
//     leave a quiet tenant's few L0 files unmerged for good.
//  3. Hints: stale schema, or ≥ 2 non-mature files at a top level ≥ L2
//     (recompactionLevel).
//
// sizeOK false (a tenant with no lifecycle rule past SizeMergeMaxAge) leaves
// only the stale-schema heal.
func (p *LevelPolicy) planGroup(files []manifest.FileInfo, pt, now time.Time, currentFP string, sizeOK bool) (int, []manifest.FileInfo, string, bool) {
	if len(files) < 2 {
		return 0, nil, "", false
	}
	age := now.Sub(pt)
	if age < p.MinAge {
		return 0, nil, "", false
	}
	if !sizeOK {
		return p.planHint(files, currentFP, false)
	}
	if countAtLevel(files, 0) >= p.MinFilesL0 {
		if sel := p.SelectFiles(files, 0, MajoritySchemaFingerprint(files, 0)); len(sel) >= 2 {
			return 0, sel, reasonL0Count, true
		}
	}
	if countAtLevel(files, 1) >= p.MinFilesL1 {
		if sel := p.SelectFiles(files, 1, MajoritySchemaFingerprint(files, 1)); len(sel) >= 2 {
			return 1, sel, reasonL1Count, true
		}
	}
	if rollupAge := p.rollupAge(); rollupAge > 0 && age >= rollupAge {
		var small []manifest.FileInfo
		for _, f := range files {
			if f.Size < matureBytes {
				small = append(small, f)
			}
		}
		fp := majorityFingerprint(small, currentFP, func(manifest.FileInfo) bool { return true })
		var sel []manifest.FileInfo
		top := 0
		for _, f := range small {
			if f.SchemaFingerprint == fp {
				sel = append(sel, f)
				if f.CompactionLevel > top {
					top = f.CompactionLevel
				}
			}
		}
		if len(sel) >= 2 {
			return top, sel, reasonRollup, true
		}
	}
	return p.planHint(files, currentFP, true)
}

// planHint plans the compaction-hint merge (recompactionLevel): stale-schema
// files heal whatever their size; a fragmented top level merges only its
// non-mature files, and only when fragmentation merges are allowed.
func (p *LevelPolicy) planHint(files []manifest.FileInfo, currentFP string, fragmentOK bool) (int, []manifest.FileInfo, string, bool) {
	lvl, needs := recompactionLevel(files, currentFP)
	if !needs {
		return 0, nil, "", false
	}
	sel := p.SelectFiles(files, lvl, MajoritySchemaFingerprint(files, lvl))
	stale := false
	for _, f := range sel {
		if currentFP != "" && f.SchemaFingerprint != currentFP {
			stale = true
			break
		}
	}
	if stale {
		if len(sel) >= 2 {
			return lvl, sel, reasonStale, true
		}
		return 0, nil, "", false
	}
	if !fragmentOK {
		return 0, nil, "", false
	}
	small := sel[:0:0]
	for _, f := range sel {
		if f.Size < matureBytes {
			small = append(small, f)
		}
	}
	if len(small) >= 2 {
		return lvl, small, reasonFragmented, true
	}
	return 0, nil, "", false
}

// rollupAge is DailyRollupAge with its one-hour floor (a rollup of an hour
// that is still being written would only be rolled up again).
func (p *LevelPolicy) rollupAge() time.Duration {
	a := p.DailyRollupAge
	if a > 0 && a < time.Hour {
		a = time.Hour
	}
	return a
}

// sortPlans orders plans: open-hour merges first, then by debt (files removed
// per byte rewritten), then oldest partition, then partition and tenant so the
// order is deterministic.
func sortPlans(plans []mergePlan) {
	sort.SliceStable(plans, func(i, j int) bool {
		a, b := plans[i], plans[j]
		if a.open() != b.open() {
			return a.open()
		}
		if da, db := a.debt(), b.debt(); da != db {
			return da > db
		}
		if !a.time.Equal(b.time) {
			return a.time.Before(b.time)
		}
		if a.partition != b.partition {
			return a.partition < b.partition
		}
		if a.group != b.group {
			return a.group < b.group
		}
		return a.tenant < b.tenant
	})
}

// fairShareTenant turns a tenant group prefix "<acct>/<proj>/<mode>/" into the
// fair-share key "<acct>/<proj>"; legacy keys without a tenant prefix share
// "default".
func fairShareTenant(prefix string) string {
	if prefix == "" {
		return "default"
	}
	n := 0
	for i := 0; i < len(prefix); i++ {
		if prefix[i] == '/' {
			n++
			if n == 2 {
				return prefix[:i]
			}
		}
	}
	return prefix
}

// majorityFingerprint returns the most common schema fingerprint among the
// files keep accepts. Ties go to currentFP, then to the smallest fingerprint,
// so every pod plans the same merge from the same manifest.
func majorityFingerprint(files []manifest.FileInfo, currentFP string, keep func(manifest.FileInfo) bool) string {
	counts := make(map[string]int)
	for _, f := range files {
		if keep(f) {
			counts[f.SchemaFingerprint]++
		}
	}
	var best string
	bestCount := 0
	for fp, c := range counts {
		switch {
		case c > bestCount:
			best, bestCount = fp, c
		case c == bestCount && best != currentFP && (fp == currentFP || fp < best):
			best = fp
		}
	}
	return best
}
