package compaction

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// Concurrency over ONE manifest and ONE pool: two schedulers in one process.
//
// Scope: HRW ownership is still decided per partition and is unchanged by the
// per-tenant planning fix. Several pods with separate manifests on one bucket
// (each pod's manifest unaware of the others' merges) is a different problem,
// tracked in #290, and is deliberately not asserted here.

const concurrencyRounds = 3

// TestStorageHealth_Concurrent_TwoSchedulersRaceScan: both schedulers plan the same files
// and race to merge them. Guards the publish step (ReplaceFiles only if every
// source is still registered): exactly one merge per source set wins, the loser
// abandons and deletes its output, and no row is lost or served twice.
func TestStorageHealth_Concurrent_TwoSchedulersRaceScan(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		l := newLedger(w)
		a, b := w.schedulerOn(w.pool), w.schedulerOn(w.pool)
		for round := 0; round < concurrencyRounds; round++ {
			p := partitionAt(time.Now().Add(-time.Duration(3+round) * time.Hour))
			for _, tenant := range []string{"1001/0", "1002/0", "1003/7"} {
				l.tenantL0(tenant, p, 10+round%3)
			}
			var wg sync.WaitGroup
			for _, s := range []*Scheduler{a, b, a, b} {
				wg.Add(1)
				go func(s *Scheduler) {
					defer wg.Done()
					if _, err := s.Scan(context.Background()); err != nil {
						t.Errorf("scan: %v", err)
					}
				}(s)
			}
			wg.Wait()
			l.check("round", true)
		}
		converge(t, l, a, "after race")
	})
}

// TestStorageHealth_Concurrent_ScanForceAndTierA: a scheduled scan, a manual force and a
// Tier A steal hit the same partitions together. Guards that every one of the
// three paths goes through the same atomic publish: whoever loses abandons.
func TestStorageHealth_Concurrent_ScanForceAndTierA(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w := newPlanWorld(t, mode)
		l := newLedger(w)
		scanSched, forceSched := w.schedulerOn(w.pool), w.schedulerOn(w.pool)
		sweep := tierAWorld(w, nil, nil)
		for round := 0; round < concurrencyRounds; round++ {
			p := partitionAt(time.Now().Add(-time.Duration(3+round) * time.Hour))
			for _, tenant := range []string{"1001/0", "1002/0"} {
				l.tenantL0(tenant, p, 12)
			}
			w.m.MarkAttempt(p, time.Now().Add(-10*time.Minute)) // stale: Tier A may steal
			var wg sync.WaitGroup
			wg.Add(3)
			go func() {
				defer wg.Done()
				if _, err := scanSched.Scan(context.Background()); err != nil {
					t.Errorf("scan: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				// Losing (nothing left to merge, or an abandoned publish) is fine.
				_, _ = forceSched.ForceCompactPartition(context.Background(), p, 0)
			}()
			go func() {
				defer wg.Done()
				if _, err := sweep.RunTierA(context.Background()); err != nil {
					t.Errorf("tier a: %v", err)
				}
			}()
			wg.Wait()
			l.check("round", true)
		}
		converge(t, l, scanSched, "after mixed race")
	})
}
