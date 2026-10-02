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

// matureBytes: a file at least this large is never selected by the closed-hour
// rollup. Merging it would rewrite a large object to absorb a few late small
// ones, so write amplification under backfill would grow with the hour's total
// bytes instead of with the late data. Half of the 64 MiB effective target
// object size of the compaction v2 design (mature_fraction 0.5).
const matureBytes = 32 << 20

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

// debt is the files a plan removes per byte it rewrites (compaction v2 §2.3):
// the heaviest small-file debt first.
func (p mergePlan) debt() float64 {
	b := p.bytes
	if b < 1 {
		b = 1
	}
	return float64(len(p.files)-1) / float64(b)
}

// frozenFunc reports whether a file must stay out of compaction, and why.
type frozenFunc func(fi manifest.FileInfo, partitionTime, now time.Time) (bool, string)

// planPartition plans the merges of one partition: it splits the files into
// tenant groups and plans each group on its own, so one tenant's files never
// make another tenant's lone file look mergeable. Held files (a delete rewrite
// swapped them in but has not recorded it) and frozen files (lifecycle) are
// excluded before anything is counted. frozenSeen, when set, is told about
// every frozen file.
func (p *LevelPolicy) planPartition(partition string, files []manifest.FileInfo, pt, now time.Time, currentFP string, frozen frozenFunc, frozenSeen func(reason string)) []mergePlan {
	var eligible []manifest.FileInfo
	for _, f := range files {
		if frozen != nil {
			if ok, reason := frozen(f, pt, now); ok {
				if frozenSeen != nil {
					frozenSeen(reason)
				}
				continue
			}
		}
		eligible = append(eligible, f)
	}
	var plans []mergePlan
	for _, g := range groupFilesByTenant(eligible) {
		level, selected, reason, ok := p.planGroup(g.Files, pt, now, currentFP)
		if !ok {
			continue
		}
		var bytes int64
		for _, f := range selected {
			bytes += f.Size
		}
		plans = append(plans, mergePlan{
			partition: partition,
			tenant:    fairShareTenant(g.TenantPrefix),
			level:     level,
			files:     selected,
			reason:    reason,
			time:      pt,
			bytes:     bytes,
		})
	}
	return plans
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
//  3. Hints: stale schema or ≥ 2 files at a top level ≥ L2 (recompactionLevel).
func (p *LevelPolicy) planGroup(files []manifest.FileInfo, pt, now time.Time, currentFP string) (int, []manifest.FileInfo, string, bool) {
	if len(files) < 2 {
		return 0, nil, "", false
	}
	age := now.Sub(pt)
	if age < p.MinAge {
		return 0, nil, "", false
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
	if lvl, needs := recompactionLevel(files, currentFP); needs {
		if sel := p.SelectFiles(files, lvl, MajoritySchemaFingerprint(files, lvl)); len(sel) >= 2 {
			reason := reasonFragmented
			for _, f := range sel {
				if currentFP != "" && f.SchemaFingerprint != currentFP {
					reason = reasonStale
					break
				}
			}
			return lvl, sel, reason, true
		}
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
