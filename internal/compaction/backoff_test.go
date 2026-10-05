package compaction

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func bp(partition, group string) mergePlan { return mergePlan{partition: partition, group: group} }

// TestPlanBackoff_DoublesAndCaps guards the wait: the scan interval for the
// first failure, doubled for each consecutive failure, never more than an
// hour. Without the doubling a permanently failing merge is retried at full
// rate; without the cap a transient outage would park a plan for days.
func TestPlanBackoff_DoublesAndCaps(t *testing.T) {
	var b planBackoff
	p := bp("dt=1/hour=00", "1/0/logs/|")
	now := unitNow
	interval := 20 * time.Second
	want := []time.Duration{20 * time.Second, 40 * time.Second, 80 * time.Second, 160 * time.Second}
	for i, w := range want {
		b.failed(p, now, interval)
		if got := b.state[backoffKey(p)].until.Sub(now); got != w {
			t.Fatalf("failure %d: wait %v, want %v", i+1, got, w)
		}
	}
	for i := 0; i < 30; i++ {
		b.failed(p, now, interval)
	}
	if got := b.state[backoffKey(p)].until.Sub(now); got != maxPlanBackoff {
		t.Fatalf("wait after many failures = %v, want the %v cap", got, maxPlanBackoff)
	}
	// A non-positive interval falls back to one minute.
	var z planBackoff
	z.failed(p, now, 0)
	if got := z.state[backoffKey(p)].until.Sub(now); got != time.Minute {
		t.Fatalf("zero interval wait = %v, want 1m", got)
	}
}

// TestPlanBackoff_FilterSuccessAndPrune guards the lifecycle of an entry:
// plans in backoff are dropped and others kept in order; the entry expires
// into a retry once its time passes but stays until the plan succeeds; success
// clears it (a later failure starts again at one interval); and an entry whose
// plan no longer exists is pruned so the map stays bounded.
func TestPlanBackoff_FilterSuccessAndPrune(t *testing.T) {
	var b planBackoff
	bad, ok1, ok2 := bp("p1", "g"), bp("p2", "g"), bp("p1", "h")
	interval := time.Minute

	// Nothing recorded: the same slice comes back untouched.
	if got := b.filter([]mergePlan{bad, ok1}, unitNow); len(got) != 2 {
		t.Fatalf("empty backoff dropped plans: %v", got)
	}

	b.failed(bad, unitNow, interval)
	got := b.filter([]mergePlan{ok1, bad, ok2}, unitNow.Add(30*time.Second))
	if len(got) != 2 || got[0].partition != "p2" || got[1].group != "h" {
		t.Fatalf("filter inside the backoff = %+v, want the two other plans in order", got)
	}
	// After the wait the plan is tried again, and the entry is kept.
	if got := b.filter([]mergePlan{bad}, unitNow.Add(61*time.Second)); len(got) != 1 {
		t.Fatal("plan not retried after its backoff")
	}
	if len(b.state) != 1 {
		t.Fatal("entry must stay until the plan succeeds or disappears")
	}
	// A second failure doubles from the kept count.
	b.failed(bad, unitNow.Add(61*time.Second), interval)
	if got := b.state[backoffKey(bad)].until.Sub(unitNow.Add(61 * time.Second)); got != 2*time.Minute {
		t.Fatalf("second failure wait = %v, want 2m", got)
	}
	// Success clears it; the next failure starts over.
	b.succeeded(bad)
	if len(b.state) != 0 {
		t.Fatal("success did not clear the entry")
	}
	b.failed(bad, unitNow, interval)
	if got := b.state[backoffKey(bad)].until.Sub(unitNow); got != interval {
		t.Fatalf("failure after success waited %v, want one interval", got)
	}
	// The plan disappears from the plans: the entry is pruned.
	if got := b.filter([]mergePlan{ok1}, unitNow); len(got) != 1 {
		t.Fatal("filter dropped an unrelated plan")
	}
	if len(b.state) != 0 {
		t.Fatalf("entry of a vanished plan kept: %v", b.state)
	}
}

// TestScan_PoisonPlanDoesNotStarveTenant guards the backoff in the scheduler
// (R7): a recent partition whose merge fails every time (a download error, e.g.
// InvalidObjectState) wins the tenant's slot by debt on the first scan; the
// next scan must skip it and merge the same tenant's other partition. Without
// the backoff the poison plan is re-picked first on every scan and the tenant
// never compacts. The poison plan is retried after the interval, with a
// doubling wait.
func TestScan_PoisonPlanDoesNotStarveTenant(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		clock := time.Now()
		old := planClock
		planClock = func() time.Time { return clock }
		t.Cleanup(func() { planClock = old })

		w := newPlanWorld(t, mode)
		pPoison := partitionAt(clock.Add(-5 * time.Hour))
		pGood := partitionAt(clock.Add(-3 * time.Hour))
		w.add("1001/0", pPoison, 0, 12, 2, nil) // more files: larger debt, picked first
		w.add("1001/0", pGood, 0, 10, 2, nil)
		attempts := 0
		fp := &faultPool{mockPool: w.pool}
		fp.downloadErr = func(key string) error {
			if strings.Contains(key, pPoison) {
				attempts++
				return errors.New("InvalidObjectState: object is in GLACIER")
			}
			return nil
		}
		s := w.schedulerOn(fp)
		for i := 0; i < 2; i++ {
			if _, err := s.Scan(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if got := len(w.m.FilesForPartition(pGood)); got != 1 {
			t.Fatalf("tenant starved behind a failing plan: %d files in its other partition after 2 scans", got)
		}
		if attempts != 1 {
			t.Fatalf("poison plan attempted %d times in 2 scans, want 1 (second scan inside the backoff)", attempts)
		}
		if got := len(w.m.FilesForPartition(pPoison)); got != 12 {
			t.Fatalf("failed merge changed the partition: %d files", got)
		}
		// After the interval the plan is retried, then waits twice as long.
		clock = clock.Add(config.Default().Compaction.Interval + time.Second)
		_, _ = s.Scan(context.Background())
		if attempts != 2 {
			t.Fatalf("attempts after the interval = %d, want 2", attempts)
		}
		clock = clock.Add(config.Default().Compaction.Interval + time.Second) // inside the doubled wait
		_, _ = s.Scan(context.Background())
		if attempts != 2 {
			t.Fatalf("retried inside the doubled wait: attempts=%d", attempts)
		}
		// The store recovers: the plan merges once its wait is over.
		fp.set(func() { fp.downloadErr = nil })
		clock = clock.Add(config.Default().Compaction.Interval + time.Second)
		if n, _ := s.Scan(context.Background()); n != 1 {
			t.Fatalf("recovered plan merged %d, want 1", n)
		}
		if len(s.backoff.state) != 0 {
			t.Fatalf("backoff entry kept after success: %v", s.backoff.state)
		}
	})
}
