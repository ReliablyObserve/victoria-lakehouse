package compaction

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestStorageHealth_CompactionProperties drives the shipped-defaults scheduler through days of
// random multi-tenant traffic with the planner's clock advancing per scan, and
// after EVERY scan checks, against an oracle of everything ingested:
//
//	P1  storage invariants (manifest <-> bucket agree, no orphans beyond owed deletes)
//	P2  the served rows are exactly the ingested rows: none lost, none twice
//	P3  no row sits under another tenant's key prefix or bucket
//
// and, once ingest stops and the clock is past DailyRollupAge:
//
//	P4  scans reach 0 compactions and then stay at 0 (convergence, stability)
//	P5  every (tenant group, partition) holds at most one non-mature file of the
//	    majority schema, and nothing is awaiting a delete.
//
// Traffic: numeric tenants plus legacy un-prefixed keys and a second bucket,
// late data into hours that closed long ago, files at random levels, and some
// mature-sized files (a manifest size override; the rollup must leave them).
// Seeds: see the defaults below; LH_COMPACTION_PROPERTY_SEEDS overrides (CI: 30).
func TestStorageHealth_CompactionProperties(t *testing.T) {
	// Default sizes keep the package inside the CI time budget: Parquet
	// encode/decode is ~25x slower under -race, so the race run takes 1 seed
	// per signal, a plain run 10, -short 6 (non-race). The storage-health CI
	// job sets LH_COMPACTION_PROPERTY_SEEDS=30.
	seeds := 10
	if raceEnabled {
		seeds = 1
	}
	if testing.Short() && !raceEnabled {
		seeds = 6
	}
	if v, err := strconv.Atoi(os.Getenv("LH_COMPACTION_PROPERTY_SEEDS")); err == nil && v > 0 {
		seeds = v
	}
	bothModes(t, func(t *testing.T, mode config.Mode) {
		for seed := 1; seed <= seeds; seed++ {
			seed := seed
			// Alternate which signal runs a seed so each mode sees every seed
			// shape across the full run: every seed runs in both modes.
			t.Run(fmt.Sprintf("seed%02d", seed), func(t *testing.T) { runCompactionProperty(t, mode, int64(seed)) })
		}
	})
}

const (
	propStep       = time.Hour // planner clock advance per scan
	propIngestSpan = 30 * time.Hour
)

type propTenant struct {
	name   string // key prefix tenant ("" = legacy)
	bucket string
	rate   float64 // chance of a flush per scan
}

func runCompactionProperty(t *testing.T, mode config.Mode, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	clock := start
	old := planClock
	planClock = func() time.Time { return clock }
	t.Cleanup(func() { planClock = old })

	w := newPlanWorld(t, mode)
	l := newLedger(w)
	sched := w.shippedScheduler()

	tenants := []propTenant{
		{"1001/0", "", 0.3 + rng.Float64()*0.3},
		{"1002/0", "", 0.1 + rng.Float64()*0.2},
		{"1003/7", "", rng.Float64() * 0.1},
		{"1001/0", "b2", 0.1 + rng.Float64()*0.1}, // same tenant, second bucket
		{"", "", 0.1}, // legacy keys
	}
	tag := func(pt propTenant) string {
		name := pt.name
		if name == "" {
			name = "legacy"
		}
		return name + "@" + pt.bucket
	}
	ingest := func(pt propTenant, at time.Time, level int, size int64) {
		part := partitionAt(at)
		prefix := pt.name + "/" + string(mode)
		if pt.name == "" {
			prefix = string(mode)
		}
		l.seq++
		key := fmt.Sprintf("%s/%s/batch-L%d-s%d-%05d.parquet", prefix, part, level, seed, l.seq)
		l.put(key, tag(pt), pt.bucket, part, level, 1+rng.Intn(3), size)
	}

	scans := 0
	step := func(stage string) int {
		scans++
		n, err := sched.Scan(context.Background())
		if err != nil {
			t.Fatalf("seed %d %s scan %d: %v", seed, stage, scans, err)
		}
		l.check(fmt.Sprintf("seed %d %s scan %d (clock %s)", seed, stage, scans, clock.Format("01-02 15:04")), true)
		return n
	}

	// Phase 1: ingest and compact together.
	for clock.Sub(start) < propIngestSpan {
		clock = clock.Add(propStep)
		for _, pt := range tenants {
			if rng.Float64() < pt.rate {
				// A burst of flushes into the current hour; bursts of ten or
				// more reach the L0 threshold inside an open hour.
				lvl := rng.Intn(10) / 9 * rng.Intn(4) // mostly L0, occasionally L0-L3
				for k := 1 + rng.Intn(12); k > 0; k-- {
					ingest(pt, clock, lvl, 0)
				}
			}
		}
		// Late data into an hour that closed long ago.
		if rng.Float64() < 0.12 {
			ingest(tenants[rng.Intn(len(tenants))], clock.Add(-time.Duration(2+rng.Intn(60))*time.Hour), 0, 0)
		}
		// A mature-sized file (size override; tiny object) at a random level >= 1.
		if rng.Float64() < 0.04 {
			ingest(tenants[rng.Intn(len(tenants))], clock.Add(-time.Duration(2+rng.Intn(40))*time.Hour), 1+rng.Intn(3), matureBytes+int64(rng.Intn(1<<20)))
		}
		step("ingest")
	}

	// Phase 2: ingest stops; the clock passes DailyRollupAge for every hour.
	clock = clock.Add(config.Default().Compaction.DailyRollupAge + 2*time.Hour)
	settled := false
	for i := 0; i < 400; i++ {
		if step("settle") == 0 {
			settled = true
			break
		}
	}
	if !settled {
		t.Fatalf("seed %d: no convergence after 400 settle scans", seed)
	}
	for i := 0; i < 20; i++ {
		if n := step("stable"); n != 0 {
			t.Fatalf("seed %d: scan %d after convergence compacted %d merges (churn)", seed, i+1, n)
		}
	}

	t.Logf("seed %d: scans=%d ingested_objects=%d rows=%d final_objects=%d", seed, scans, l.seq, len(l.want), w.m.TotalFiles())
	// P5: at most one non-mature file of the majority schema per group, and no
	// outstanding deletes.
	l.check(fmt.Sprintf("seed %d final", seed), false)
	for part, files := range w.m.AllFiles() {
		small := map[string]int{}
		for _, f := range files {
			if f.Size < matureBytes {
				small[groupID(f)]++
			}
		}
		for g, n := range small {
			if n > 1 {
				t.Fatalf("seed %d: %s group %s holds %d non-mature files after convergence", seed, part, g, n)
			}
		}
	}
	if rk := w.m.RetiredKeys(); func() bool {
		for _, r := range rk {
			if r.Reclaim {
				return true
			}
		}
		return false
	}() {
		t.Fatalf("seed %d: deletes still owed after convergence: %v", seed, rk)
	}
}

// groupID is the compaction group of a file: tenant prefix + bucket.
func groupID(f manifest.FileInfo) string {
	return manifest.CompactionGroupPrefix(f.Key) + "|" + f.Bucket
}
