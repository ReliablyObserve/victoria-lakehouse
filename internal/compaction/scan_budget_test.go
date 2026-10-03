package compaction

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

func budgetWorld(t *testing.T, mode config.Mode, tenants int) (*planWorld, string) {
	w := newPlanWorld(t, mode)
	p := partitionAt(time.Now().Add(-3 * time.Hour))
	for i := 0; i < tenants; i++ {
		w.add(fmt.Sprintf("%d/0", 1001+i), p, 0, 10, 2, nil)
	}
	return w, p
}

func tenantFileCount(w *planWorld, p, tenant string) int {
	n := 0
	for _, f := range w.m.FilesForPartition(p) {
		if len(f.Key) > len(tenant) && f.Key[:len(tenant)+1] == tenant+"/" {
			n++
		}
	}
	return n
}

// TestScanBudget_OneMergePerScanServesEveryTenant guards the scan budget and
// the fair-share cursor: with a budget no merge can fit in, a scan runs exactly
// one merge (the first always runs), the metric counts the cut-off scans, and
// over N scans each of N tenants is merged once, so a tenant behind the
// budget is not starved. Without the budget check one scan runs all N merges;
// without the cursor moving, the same tenant would win every scan.
func TestScanBudget_OneMergePerScanServesEveryTenant(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		const tenants = 5
		w, p := budgetWorld(t, mode, tenants)
		s := w.shippedScheduler()
		s.scanBudget = time.Nanosecond
		before := metrics.CompactionScanBudgetExhausted.Get()
		merged := map[string]bool{}
		for scan := 1; scan <= tenants; scan++ {
			n, err := s.Scan(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if n != 1 {
				t.Fatalf("scan %d: merges=%d, want exactly 1", scan, n)
			}
			newly := 0
			for i := 0; i < tenants; i++ {
				tn := fmt.Sprintf("%d/0", 1001+i)
				if tenantFileCount(w, p, tn) == 1 && !merged[tn] {
					merged[tn] = true
					newly++
				}
			}
			if newly != 1 {
				t.Fatalf("scan %d served %d new tenants, want 1 (cursor must move on)", scan, newly)
			}
		}
		if len(merged) != tenants {
			t.Fatalf("served %d of %d tenants in %d scans", len(merged), tenants, tenants)
		}
		// Scans 1..N-1 had more than one plan picked and were cut off.
		if got := metrics.CompactionScanBudgetExhausted.Get() - before; got != tenants-1 {
			t.Fatalf("scan_budget_exhausted_total grew by %d, want %d", got, tenants-1)
		}
		if n, _ := s.Scan(context.Background()); n != 0 {
			t.Fatalf("settled scan merged %d", n)
		}
	})
}

// TestScanBudget_NegativeDisables guards the opt-out: a negative budget never
// cuts a scan, so all tenants merge in one scan and the metric stays put.
func TestScanBudget_NegativeDisables(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w, _ := budgetWorld(t, mode, 5)
		sched := NewScheduler(SchedulerConfig{
			Manifest: w.m, Pool: w.pool, Ownership: soleOwnerResolver(), FairShare: NewFairShareScheduler(1),
			Policy: shippedPolicy(), Prefix: string(mode) + "/", Mode: mode, ScanBudget: -1,
			MaxConcurrent: 1, RowGroupSize: 1000, CompressionLevel: 3, CurrentSchemaFingerprint: planFP,
			CompactionConfig: config.Default().Compaction,
		})
		if sched.scanBudget >= 0 {
			t.Fatalf("scanBudget = %v, want negative kept", sched.scanBudget)
		}
		before := metrics.CompactionScanBudgetExhausted.Get()
		if n, err := sched.Scan(context.Background()); err != nil || n != 5 {
			t.Fatalf("merges=%d err=%v, want 5 in one scan", n, err)
		}
		if metrics.CompactionScanBudgetExhausted.Get() != before {
			t.Fatal("budget metric moved with the budget disabled")
		}
	})
}

// TestScanBudget_DefaultIsTheInterval guards the default: 0 means the scan
// interval (the 5m default interval when that is unset too); an explicit value
// is kept.
func TestScanBudget_DefaultIsTheInterval(t *testing.T) {
	mk := func(interval, budget time.Duration) *Scheduler {
		return NewScheduler(SchedulerConfig{Ownership: soleOwnerResolver(), Interval: interval, ScanBudget: budget})
	}
	if got := mk(7*time.Minute, 0).scanBudget; got != 7*time.Minute {
		t.Fatalf("default budget = %v, want the 7m interval", got)
	}
	if got := mk(0, 0).scanBudget; got != 5*time.Minute {
		t.Fatalf("default budget with default interval = %v, want 5m", got)
	}
	if got := mk(7*time.Minute, 90*time.Second).scanBudget; got != 90*time.Second {
		t.Fatalf("explicit budget = %v", got)
	}
}

// TestFairShareAdvance guards Advance: it moves the cursor by n (and ignores
// n <= 0), so after a cut-off scan that served k tenants the next scan starts
// at the first one it did not reach.
func TestFairShareAdvance(t *testing.T) {
	f := NewFairShareScheduler(1)
	plans := []mergePlan{{tenant: "a", partition: "p"}, {tenant: "b", partition: "p"}, {tenant: "c", partition: "p"}}
	first := func() string { return f.PickCandidates(plans, 1)[0].tenant }
	if got := first(); got != "a" {
		t.Fatalf("first pick %s", got)
	}
	// The call above moved the cursor to 1; Advance(1) skips b.
	f.Advance(0)
	f.Advance(-3)
	f.Advance(1)
	if got := first(); got != "c" {
		t.Fatalf("after Advance(1) the next pick is %s, want c", got)
	}
}
