package compaction

import (
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// The planner is pure (files + clock in, plans out) and has no signal
// dependency; the scheduler-level tests in per_tenant_planning_test.go and the
// property/fault/concurrency tests run both modes.

func pf(key string, level int, size int64, fp string) manifest.FileInfo {
	return manifest.FileInfo{Key: key, CompactionLevel: level, Size: size, SchemaFingerprint: fp, RowCount: 1}
}

// pfs builds n small files at level with the given fingerprint.
func pfs(prefix string, n, level int, fp string) []manifest.FileInfo {
	out := make([]manifest.FileInfo, n)
	for i := range out {
		out[i] = pf(fmt.Sprintf("%s-L%d-%02d", prefix, level, i), level, 1000, fp)
	}
	return out
}

func cat(parts ...[]manifest.FileInfo) []manifest.FileInfo {
	var out []manifest.FileInfo
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func unitPolicy() *LevelPolicy {
	p := NewLevelPolicy(10, 10, time.Hour)
	p.DailyRollupAge = 24 * time.Hour
	return p
}

var unitNow = time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

func ago(d time.Duration) time.Time { return unitNow.Add(-d) }

func TestPlanGroup_NeverASingleFile(t *testing.T) {
	// Guards the len(files) < 2 gate: without it a lone file in a closed hour
	// is rewritten 1 -> 1 on every scan (issue #343).
	p := unitPolicy()
	for _, age := range []time.Duration{2 * time.Hour, 48 * time.Hour} {
		for _, lvl := range []int{0, 1, 2, 5} {
			if _, sel, _, ok := p.planGroup([]manifest.FileInfo{pf("a", lvl, 10, "fp")}, ago(age), unitNow, "fp", true); ok {
				t.Fatalf("age %s level %d: planned %v for a single file", age, lvl, sel)
			}
		}
	}
	if _, _, _, ok := p.planGroup(nil, ago(48*time.Hour), unitNow, "fp", true); ok {
		t.Fatal("planned a merge of nothing")
	}
}

func TestPlanGroup_MinAge(t *testing.T) {
	// Guards the age < MinAge gate: without it an hour still filling up is
	// merged while writers add to it.
	p := unitPolicy()
	files := pfs("a", 12, 0, "fp")
	if _, _, _, ok := p.planGroup(files, ago(30*time.Minute), unitNow, "fp", true); ok {
		t.Fatal("merged a partition younger than MinAge")
	}
	if _, _, _, ok := p.planGroup(files, ago(61*time.Minute), unitNow, "fp", true); !ok {
		t.Fatal("did not merge a partition older than MinAge")
	}
}

func TestPlanGroup_L0Threshold(t *testing.T) {
	// Guards countAtLevel(0) >= MinFilesL0: nine L0 files are left to grow, ten merge.
	p := unitPolicy()
	if _, _, _, ok := p.planGroup(pfs("a", 9, 0, "fp"), ago(2*time.Hour), unitNow, "fp", true); ok {
		t.Fatal("nine L0 files must wait")
	}
	level, sel, reason, ok := p.planGroup(pfs("a", 10, 0, "fp"), ago(2*time.Hour), unitNow, "fp", true)
	if !ok || level != 0 || len(sel) != 10 || reason != reasonL0Count {
		t.Fatalf("got level=%d n=%d reason=%q ok=%v", level, len(sel), reason, ok)
	}
}

func TestPlanGroup_L1Threshold(t *testing.T) {
	// Guards the L1 -> L2 branch: ten L1 files merge to L2; nine do not (open hour).
	p := unitPolicy()
	if _, _, _, ok := p.planGroup(pfs("a", 9, 1, "fp"), ago(2*time.Hour), unitNow, "fp", true); ok {
		t.Fatal("nine L1 files must wait")
	}
	level, sel, reason, ok := p.planGroup(pfs("a", 10, 1, "fp"), ago(2*time.Hour), unitNow, "fp", true)
	if !ok || level != 1 || len(sel) != 10 || reason != reasonL1Count {
		t.Fatalf("got level=%d n=%d reason=%q ok=%v", level, len(sel), reason, ok)
	}
}

func TestPlanGroup_L0BeforeL1(t *testing.T) {
	// Guards branch order: L0 -> L1 wins over L1 -> L2 when both are eligible.
	p := unitPolicy()
	level, _, reason, ok := p.planGroup(cat(pfs("a", 10, 0, "fp"), pfs("b", 10, 1, "fp")), ago(2*time.Hour), unitNow, "fp", true)
	if !ok || level != 0 || reason != reasonL0Count {
		t.Fatalf("level=%d reason=%q", level, reason)
	}
}

func TestPlanGroup_OnlyMajorityFingerprintAtLevel(t *testing.T) {
	// Guards SelectFiles' fingerprint filter: a merge never mixes schemas.
	p := unitPolicy()
	files := cat(pfs("a", 6, 0, "fpA"), pfs("b", 5, 0, "fpB"))
	_, sel, _, ok := p.planGroup(files, ago(2*time.Hour), unitNow, "fpA", true)
	if !ok || len(sel) != 6 {
		t.Fatalf("selected %d, want the 6 majority files", len(sel))
	}
	for _, f := range sel {
		if f.SchemaFingerprint != "fpA" {
			t.Fatalf("minority fingerprint selected: %+v", f)
		}
	}
}

func TestPlanGroup_Rollup(t *testing.T) {
	// Guards the closed-hour rollup: every non-mature file of the majority
	// fingerprint, whatever its level, merges into one output at top level + 1.
	// Without it a quiet tenant's few small files are never merged.
	p := unitPolicy()
	files := cat(pfs("a", 2, 0, "fp"), pfs("b", 1, 1, "fp"), pfs("c", 1, 2, "fp"))
	level, sel, reason, ok := p.planGroup(files, ago(30*time.Hour), unitNow, "fp", true)
	if !ok || reason != reasonRollup || len(sel) != 4 {
		t.Fatalf("got n=%d reason=%q ok=%v", len(sel), reason, ok)
	}
	if level != 2 {
		t.Fatalf("source level = %d, want the top level 2 (output L3)", level)
	}
}

func TestPlanGroup_RollupNeedsClosedHour(t *testing.T) {
	// Guards age >= rollupAge: a 12 h old hour with two files is not rolled up.
	p := unitPolicy()
	if _, _, _, ok := p.planGroup(cat(pfs("a", 1, 0, "fp"), pfs("b", 1, 1, "fp")), ago(12*time.Hour), unitNow, "fp", true); ok {
		t.Fatal("rolled up before DailyRollupAge")
	}
}

func TestPlanGroup_RollupDisabledAndHourFloor(t *testing.T) {
	// Guards rollupAge(): 0 disables the rollup; a sub-hour setting is floored
	// at one hour so an hour still being written is not rolled up.
	files := cat(pfs("a", 1, 0, "fp"), pfs("b", 1, 1, "fp"))
	off := unitPolicy()
	off.DailyRollupAge = 0
	if _, _, _, ok := off.planGroup(files, ago(72*time.Hour), unitNow, "fp", true); ok {
		t.Fatal("rollup ran with DailyRollupAge 0")
	}
	short := NewLevelPolicy(10, 10, 0)
	short.DailyRollupAge = 10 * time.Minute
	if _, _, _, ok := short.planGroup(files, ago(30*time.Minute), unitNow, "fp", true); ok {
		t.Fatal("rollup ran inside the one-hour floor")
	}
	if _, _, _, ok := short.planGroup(files, ago(61*time.Minute), unitNow, "fp", true); !ok {
		t.Fatal("rollup did not run after the one-hour floor")
	}
}

func TestPlanGroup_RollupExcludesMatureFiles(t *testing.T) {
	// Guards f.Size < matureBytes: a mature file is never rewritten to absorb a
	// few late small files (write amplification would track the hour's bytes).
	p := unitPolicy()
	mature := pf("big", 2, matureBytes, "fp")
	files := cat([]manifest.FileInfo{mature}, pfs("a", 2, 0, "fp"))
	_, sel, reason, ok := p.planGroup(files, ago(30*time.Hour), unitNow, "fp", true)
	if !ok || reason != reasonRollup || len(sel) != 2 {
		t.Fatalf("n=%d reason=%q ok=%v", len(sel), reason, ok)
	}
	for _, f := range sel {
		if f.Key == "big" {
			t.Fatal("mature file selected")
		}
	}
	// One small file next to a mature one: nothing to merge.
	if _, _, _, ok := p.planGroup(cat([]manifest.FileInfo{mature}, pfs("a", 1, 0, "fp")), ago(30*time.Hour), unitNow, "fp", true); ok {
		t.Fatal("a lone small file was merged with a mature one")
	}
	// One byte under the threshold is not mature.
	almost := pf("almost", 1, matureBytes-1, "fp")
	if _, sel, _, ok := p.planGroup(cat([]manifest.FileInfo{almost}, pfs("a", 1, 0, "fp")), ago(30*time.Hour), unitNow, "fp", true); !ok || len(sel) != 2 {
		t.Fatalf("matureBytes-1 must still be rolled up: n=%d ok=%v", len(sel), ok)
	}
}

func TestPlanGroup_RollupMajorityFingerprint(t *testing.T) {
	// Guards the majority pick: the rollup merges one schema; the minority
	// file stays for the stale-schema hint.
	p := unitPolicy()
	files := cat(pfs("a", 3, 0, "fpA"), pfs("b", 1, 0, "fpB"))
	_, sel, _, ok := p.planGroup(files, ago(30*time.Hour), unitNow, "fpA", true)
	if !ok || len(sel) != 3 {
		t.Fatalf("n=%d ok=%v", len(sel), ok)
	}
	for _, f := range sel {
		if f.SchemaFingerprint != "fpA" {
			t.Fatalf("minority selected: %+v", f)
		}
	}
	// A 1-1 tie goes to the current fingerprint, so a rollup moves toward the
	// schema new files are written with.
	tie := cat(pfs("a", 2, 0, "fpA"), pfs("b", 2, 0, "fpB"))
	_, sel, _, _ = p.planGroup(tie, ago(30*time.Hour), unitNow, "fpB", true)
	if len(sel) != 2 || sel[0].SchemaFingerprint != "fpB" {
		t.Fatalf("tie must go to the current fingerprint: %+v", sel)
	}
}

func TestPlanGroup_HintStaleVsFragmented(t *testing.T) {
	// Guards the hint branch and its reason: a pair at a stale fingerprint is
	// "stale_schema"; a pair at a top level >= 2 with the current fingerprint
	// is "fragmented". Without the branch a stale or fragmented open hour is
	// never healed.
	p := unitPolicy()
	level, sel, reason, ok := p.planGroup(pfs("a", 2, 1, "old"), ago(2*time.Hour), unitNow, "new", true)
	if !ok || reason != reasonStale || level != 1 || len(sel) != 2 {
		t.Fatalf("stale: level=%d n=%d reason=%q ok=%v", level, len(sel), reason, ok)
	}
	level, sel, reason, ok = p.planGroup(pfs("a", 2, 2, "new"), ago(2*time.Hour), unitNow, "new", true)
	if !ok || reason != reasonFragmented || level != 2 || len(sel) != 2 {
		t.Fatalf("fragmented: level=%d n=%d reason=%q ok=%v", level, len(sel), reason, ok)
	}
	// A lone stale file has no peer: left alone.
	if _, _, _, ok := p.planGroup(cat(pfs("a", 1, 1, "old"), pfs("b", 1, 0, "new")), ago(2*time.Hour), unitNow, "new", true); ok {
		t.Fatal("a lone stale file was rewritten")
	}
	// No current fingerprint disables the stale hint.
	if _, _, _, ok := p.planGroup(pfs("a", 2, 1, "old"), ago(2*time.Hour), unitNow, "", true); ok {
		t.Fatal("stale hint fired without a current fingerprint")
	}
}

func TestMajorityFingerprint_DeterministicTies(t *testing.T) {
	// Guards the tie-break: every pod must plan the same merge from the same
	// manifest, so map iteration order must not decide.
	files := cat(pfs("a", 2, 0, "fpC"), pfs("b", 2, 0, "fpA"), pfs("c", 2, 0, "fpB"))
	all := func(manifest.FileInfo) bool { return true }
	for i := 0; i < 200; i++ {
		if got := majorityFingerprint(files, "", all); got != "fpA" {
			t.Fatalf("no current fingerprint: tie went to %q, want the smallest fpA", got)
		}
		if got := majorityFingerprint(files, "fpC", all); got != "fpC" {
			t.Fatalf("tie must go to the current fingerprint fpC, got %q", got)
		}
		if got := majorityFingerprint(files, "zzz", all); got != "fpA" {
			t.Fatalf("a current fingerprint absent from the tie must not win: %q", got)
		}
	}
	if got := majorityFingerprint(cat(pfs("a", 3, 0, "fpB"), pfs("b", 2, 0, "fpA")), "fpA", all); got != "fpB" {
		t.Fatalf("a strict majority beats the current fingerprint: %q", got)
	}
	if got := majorityFingerprint(nil, "x", all); got != "" {
		t.Fatalf("empty input = %q", got)
	}
	// keep filters before counting.
	l1 := func(f manifest.FileInfo) bool { return f.CompactionLevel == 1 }
	if got := majorityFingerprint(cat(pfs("a", 5, 0, "fpA"), pfs("b", 1, 1, "fpZ")), "", l1); got != "fpZ" {
		t.Fatalf("keep not applied: %q", got)
	}
}

func TestMajoritySchemaFingerprint_LevelAndTies(t *testing.T) {
	// Guards the exported helper: per level, ties to the smallest fingerprint.
	files := cat(pfs("a", 2, 1, "fpB"), pfs("b", 2, 1, "fpA"), pfs("c", 9, 0, "fpZ"))
	for i := 0; i < 100; i++ {
		if got := MajoritySchemaFingerprint(files, 1); got != "fpA" {
			t.Fatalf("level 1 tie = %q, want fpA", got)
		}
	}
	if got := MajoritySchemaFingerprint(files, 0); got != "fpZ" {
		t.Fatalf("level 0 = %q", got)
	}
}

func TestFairShareTenant(t *testing.T) {
	// Guards the fair-share key: "<account>/<project>" from the object keys;
	// legacy keys share "default". Returning the partition string collapsed
	// every production candidate into one bucket (issue #343).
	for prefix, want := range map[string]string{
		"":              "default",
		"1001/0/logs/":  "1001/0",
		"7/9/traces/":   "7/9",
		"1001/0":        "1001/0",
		"5":             "5",
		"1/2/logs/more": "1/2",
	} {
		if got := fairShareTenant(prefix); got != want {
			t.Errorf("fairShareTenant(%q) = %q, want %q", prefix, got, want)
		}
	}
}

func TestSortPlans_Ordering(t *testing.T) {
	// Guards each sort key in turn: open-hour merges first, then small-file
	// debt, then oldest, then partition, then tenant.
	t0 := ago(10 * time.Hour)
	mk := func(part, tenant, reason string, n int, bytes int64, ts time.Time) mergePlan {
		return mergePlan{partition: part, tenant: tenant, reason: reason, files: make([]manifest.FileInfo, n), bytes: bytes, time: ts}
	}
	rollupBig := mk("p1", "a", reasonRollup, 50, 1, t0) // huge debt but closed-hour
	openSmall := mk("p2", "a", reasonL0Count, 2, 1000, t0)
	openDebt := mk("p3", "a", reasonL1Count, 10, 1000, t0)
	closedOld := mk("p4", "a", reasonRollup, 3, 100, ago(40*time.Hour))
	closedNew := mk("p5", "a", reasonRollup, 3, 100, ago(30*time.Hour))
	plans := []mergePlan{closedNew, rollupBig, openSmall, closedOld, openDebt}
	sortPlans(plans)
	var got []string
	for _, p := range plans {
		got = append(got, p.partition)
	}
	want := []string{"p3", "p2", "p1", "p4", "p5"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("order %v, want %v", got, want)
	}
	// Ties on everything but partition then tenant are deterministic.
	tie := []mergePlan{mk("pB", "z", reasonRollup, 2, 10, t0), mk("pA", "z", reasonRollup, 2, 10, t0), mk("pA", "y", reasonRollup, 2, 10, t0)}
	sortPlans(tie)
	if tie[0].partition != "pA" || tie[0].tenant != "y" || tie[1].tenant != "z" || tie[2].partition != "pB" {
		t.Fatalf("tie order: %+v", tie)
	}
	// Zero bytes does not divide by zero.
	if d := mk("p", "t", reasonRollup, 2, 0, t0).debt(); d <= 0 {
		t.Fatalf("debt with zero bytes = %v", d)
	}
}

func TestTenantsWithWork(t *testing.T) {
	// Guards the per-scan budget: it grows with distinct tenants, not plans.
	plans := []mergePlan{{tenant: "1/0"}, {tenant: "1/0"}, {tenant: "2/0"}, {tenant: "default"}}
	if n := tenantsWithWork(plans); n != 3 {
		t.Fatalf("tenantsWithWork = %d, want 3", n)
	}
	if n := tenantsWithWork(nil); n != 0 {
		t.Fatalf("empty = %d", n)
	}
}

func TestPlanPartition_SplitsTenantsAndFreezesFirst(t *testing.T) {
	// Guards the order in planPartition: frozen files are removed BEFORE
	// grouping and counting, and tenants are planned independently. A tenant
	// with a single file next to another tenant's pair plans nothing.
	p := unitPolicy()
	t1 := []manifest.FileInfo{pf("1001/0/logs/dt=x/a", 0, 10, "fp"), pf("1001/0/logs/dt=x/b", 0, 10, "fp")}
	t2 := []manifest.FileInfo{pf("1002/0/logs/dt=x/c", 1, 10, "fp")}
	legacy := []manifest.FileInfo{pf("logs/dt=x/d", 0, 10, "fp"), pf("logs/dt=x/e", 1, 10, "fp")}
	pt := ago(30 * time.Hour)
	plans := p.planPartition("dt=x", cat(t1, t2, legacy), pt, unitNow, "fp", nil, nil)
	if len(plans) != 2 {
		t.Fatalf("plans = %d, want 2 (1001/0 and legacy; 1002/0 has a lone file): %+v", len(plans), plans)
	}
	seen := map[string]bool{}
	for _, pl := range plans {
		seen[pl.tenant] = true
		if len(pl.files) != 2 {
			t.Fatalf("plan with %d files", len(pl.files))
		}
	}
	if !seen["1001/0"] || !seen["default"] {
		t.Fatalf("tenants planned: %v", seen)
	}

	// Freezing one of 1001/0's two files (its listed class is Glacier) leaves a
	// lone file: no plan for it.
	var frozenReasons []string
	t1[0].StorageClass = "GLACIER"
	plans = p.planPartition("dt=x", cat(t1, t2, legacy), pt, unitNow, "fp", nil, func(r string) { frozenReasons = append(frozenReasons, r) })
	if len(plans) != 1 || plans[0].tenant != "default" {
		t.Fatalf("after freezing: %+v", plans)
	}
	if fmt.Sprint(frozenReasons) != "[storage_class]" {
		t.Fatalf("frozenSeen = %v", frozenReasons)
	}
}

func TestPlanPartition_SeparateBucketsAreSeparateGroups(t *testing.T) {
	// Guards group = prefix + bucket: the compactor writes one output per
	// bucket, so a file in a second bucket must not count toward the first's merge.
	p := unitPolicy()
	a := pf("1001/0/logs/dt=x/a", 0, 10, "fp")
	b := pf("1001/0/logs/dt=x/b", 0, 10, "fp")
	b.Bucket = "other"
	if plans := p.planPartition("dt=x", []manifest.FileInfo{a, b}, ago(30*time.Hour), unitNow, "fp", nil, nil); len(plans) != 0 {
		t.Fatalf("files in different buckets merged: %+v", plans)
	}
}
