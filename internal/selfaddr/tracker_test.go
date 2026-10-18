package selfaddr

import (
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

func trackerEnv(clock *time.Time, mu *sync.Mutex, ip *string) Env {
	return Env{
		Now: func() time.Time { mu.Lock(); defer mu.Unlock(); return *clock },
		IPs: func() []netip.Addr { mu.Lock(); defer mu.Unlock(); return []netip.Addr{netip.MustParseAddr(*ip)} },
	}
}

func TestTracker_ResolvesMemoisesAndReportsGauges(t *testing.T) {
	var mu sync.Mutex
	clock := time.Unix(1000, 0)
	ip := "10.1.2.3"
	calls := 0
	env := trackerEnv(&clock, &mu, &ip)
	inner := env.IPs
	env.IPs = func() []netip.Addr { calls++; return inner() }
	tr := NewTracker("unit-a", ":9428", env)
	members := []string{"10.1.2.3:9428", "10.1.2.4:9428"}

	res := tr.Resolve(members)
	if !res.OK() || res.Self != "10.1.2.3:9428" {
		t.Fatalf("Resolve = %+v", res)
	}
	if tr.Resolved() != 1 || metrics.RingSelfResolved.Get("unit-a") != 1 {
		t.Errorf("resolved gauge = %d / %d, want 1", tr.Resolved(), metrics.RingSelfResolved.Get("unit-a"))
	}
	if tr.InPeers() != 0 {
		t.Errorf("in_peers before SetInPeers = %d, want 0", tr.InPeers())
	}
	tr.SetInPeers(true)
	if tr.InPeers() != 1 || metrics.RingSelfInPeers.Get("unit-a") != 1 {
		t.Errorf("in_peers = %d / %d, want 1", tr.InPeers(), metrics.RingSelfInPeers.Get("unit-a"))
	}
	tr.SetInPeers(false)
	if metrics.RingSelfInPeers.Get("unit-a") != 0 {
		t.Error("in_peers gauge not lowered")
	}

	// Same list inside the memo: no recomputation.
	before := calls
	for i := 0; i < 100; i++ {
		tr.Resolve(members)
	}
	if calls != before {
		t.Errorf("the Env was read %d times for an unchanged list", calls-before)
	}
	// After the memo expires the Env is read again and a changed address wins.
	mu.Lock()
	clock = clock.Add(memoTTL + time.Second)
	ip = "10.1.2.4"
	mu.Unlock()
	if res := tr.Resolve(members); res.Self != "10.1.2.4:9428" {
		t.Errorf("after the memo expired Self = %q, want 10.1.2.4:9428", res.Self)
	}
	// A different list is not served from the memo.
	if res := tr.Resolve([]string{"10.1.2.4:9428"}); res.Members != 1 || !res.OK() {
		t.Errorf("a changed list was served from the memo: %+v", res)
	}

	// Unresolved: gauge 0.
	if res := tr.Resolve([]string{"10.9.9.9:9428"}); res.OK() || tr.Resolved() != 0 || metrics.RingSelfResolved.Get("unit-a") != 0 {
		t.Errorf("unresolved reported as resolved: %+v gauge=%d", res, metrics.RingSelfResolved.Get("unit-a"))
	}
	// An empty list (nothing discovered) is "not in the ring": the gauge reads 0,
	// also on a tracker that has never seen a member.
	if res := tr.Resolve([]string{"10.1.2.4:9428"}); !res.OK() || tr.Resolved() != 1 {
		t.Fatalf("setup: not resolved: %+v", res)
	}
	tr.Resolve(nil)
	if tr.Resolved() != 0 || metrics.RingSelfResolved.Get("unit-a") != 0 {
		t.Errorf("an empty list left the gauge at %d / %d, want 0 (a pod that knows no peers is not in the ring)", tr.Resolved(), metrics.RingSelfResolved.Get("unit-a"))
	}
	tr2 := NewTracker("unit-b", ":9428", env)
	metrics.RingSelfResolved.Set("unit-b", 1)
	tr2.Resolve(nil)
	if metrics.RingSelfResolved.Get("unit-b") != 0 || tr2.Resolved() != 0 {
		t.Error("an empty list on a new tracker did not publish 0")
	}
}

func TestTracker_LogsOncePerChange(t *testing.T) {
	var mu sync.Mutex
	clock := time.Unix(1000, 0)
	ip := "10.1.2.3"
	tr := NewTracker("unit-c", ":9428", trackerEnv(&clock, &mu, &ip))
	many := []string{"a:1", "b:1", "c:1", "d:1", "e:1", "f:1", "g:1"}
	for i := 0; i < 3; i++ {
		tr.Resolve(many)
		mu.Lock()
		clock = clock.Add(memoTTL + time.Second)
		mu.Unlock()
	}
	if got := sample(many); got != "[a:1 b:1 c:1 d:1 e:1 ... +2]" {
		t.Errorf("sample = %q", got)
	}
	if got := sample(many[:2]); got != "[a:1 b:1]" {
		t.Errorf("sample = %q", got)
	}
	if got := describe(Result{Matches: 2}); got == describe(Result{}) {
		t.Error("ambiguous and not-listed read the same")
	}
	// logged state is only replaced on a change.
	tr.mu.Lock()
	first := tr.logged
	tr.mu.Unlock()
	tr.Resolve([]string{"10.1.2.3:9428"})
	tr.mu.Lock()
	second := tr.logged
	tr.mu.Unlock()
	if first == "" || first == second {
		t.Errorf("logged state did not change from %q to %q", first, second)
	}
}
