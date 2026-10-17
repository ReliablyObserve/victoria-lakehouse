package selfaddr

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeSrc struct {
	mu      sync.Mutex
	now     time.Time
	env     map[string]string
	host    string
	hostErr error
	ifaces  []net.Addr
	ifErr   error
	lookups atomic.Int64
	answer  map[string][]netip.Addr
	lkErr   error
	gate    chan struct{} // when set, lookups wait for it
}

func (f *fakeSrc) source() sysSource {
	return sysSource{
		getenv: func(k string) string { f.mu.Lock(); defer f.mu.Unlock(); return f.env[k] },
		hostname: func() (string, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.host, f.hostErr
		},
		interfaces: func() ([]net.Addr, error) { f.mu.Lock(); defer f.mu.Unlock(); return f.ifaces, f.ifErr },
		lookup: func(ctx context.Context, host string) ([]netip.Addr, error) {
			f.lookups.Add(1)
			if f.gate != nil {
				select {
				case <-f.gate:
				case <-ctx.Done():
				}
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.answer[host], f.lkErr
		},
		now: func() time.Time { f.mu.Lock(); defer f.mu.Unlock(); return f.now },
	}
}

func (f *fakeSrc) advance(d time.Duration) { f.mu.Lock(); f.now = f.now.Add(d); f.mu.Unlock() }

func ipnet(s string) *net.IPNet {
	ip, n, _ := net.ParseCIDR(s)
	n.IP = ip
	return n
}

func TestSystemEnv_AddressesAndNames(t *testing.T) {
	f := &fakeSrc{
		now:    time.Unix(1000, 0),
		env:    map[string]string{"POD_IP": "10.1.2.3", "POD_IPS": "10.1.2.3, fd00::5 ,bogus,,127.0.0.1", "POD_NAME": "lh-0"},
		host:   "node-1",
		ifaces: []net.Addr{ipnet("10.1.2.3/24"), ipnet("fe80::1/64"), ipnet("127.0.0.1/8"), ipnet("::1/128"), &net.UnixAddr{Name: "x"}},
	}
	env := newSystemEnv(f.source())
	got := map[netip.Addr]bool{}
	for _, a := range env.IPs() {
		got[a] = true
	}
	for _, want := range []string{"10.1.2.3", "fd00::5", "fe80::1"} {
		if !got[netip.MustParseAddr(want)] {
			t.Errorf("IPs() lacks %s: %v", want, env.IPs())
		}
	}
	for _, bad := range []string{"127.0.0.1", "::1"} {
		if got[netip.MustParseAddr(bad)] {
			t.Errorf("IPs() holds loopback %s", bad)
		}
	}
	if h := env.Hostnames(); len(h) != 2 || h[0] != "lh-0" || h[1] != "node-1" {
		t.Errorf("Hostnames() = %v, want [lh-0 node-1] (POD_NAME first)", h)
	}

	// The end-to-end result: the StatefulSet member is this pod.
	if self, ok := Resolve(":9428", []string{"lh-0.svc:9428", "lh-1.svc:9428"}, env); !ok || self != "lh-0.svc:9428" {
		t.Errorf("Resolve = (%q, %v), want lh-0.svc:9428", self, ok)
	}
}

func TestSystemEnv_CachedForAMinute(t *testing.T) {
	f := &fakeSrc{now: time.Unix(1000, 0), env: map[string]string{"POD_IP": "10.1.2.3"}, host: "h"}
	env := newSystemEnv(f.source())
	if got := env.IPs(); len(got) != 1 {
		t.Fatalf("IPs() = %v", got)
	}
	f.mu.Lock()
	f.env["POD_IP"] = "10.9.9.9" // the pod got a new address
	f.mu.Unlock()
	f.advance(30 * time.Second)
	if got := env.IPs(); got[0] != netip.MustParseAddr("10.1.2.3") {
		t.Errorf("IPs() re-read after 30s: %v, want the cached value", got)
	}
	f.advance(31 * time.Second)
	if got := env.IPs(); got[0] != netip.MustParseAddr("10.9.9.9") {
		t.Errorf("IPs() after a minute = %v, want 10.9.9.9", got)
	}
}

func TestSystemEnv_SourceErrors(t *testing.T) {
	f := &fakeSrc{now: time.Unix(1000, 0), env: map[string]string{}, hostErr: errors.New("no hostname"), ifErr: errors.New("no interfaces")}
	env := newSystemEnv(f.source())
	if got := env.IPs(); len(got) != 0 {
		t.Errorf("IPs() = %v, want none", got)
	}
	if got := env.Hostnames(); len(got) != 0 {
		t.Errorf("Hostnames() = %v, want none", got)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestSystemEnv_LookupHostIsAsyncAndCached(t *testing.T) {
	f := &fakeSrc{
		now:    time.Unix(1000, 0),
		env:    map[string]string{},
		answer: map[string][]netip.Addr{"a.test": {netip.MustParseAddr("::ffff:10.0.0.9")}},
		gate:   make(chan struct{}),
	}
	env := newSystemEnv(f.source())
	if got := env.LookupHost("a.test"); got != nil {
		t.Fatalf("first LookupHost = %v, want nil (the lookup runs in the background)", got)
	}
	waitFor(t, "the lookup to start", func() bool { return f.lookups.Load() == 1 })
	// A second ask while it is in flight does not start another.
	env.LookupHost("a.test")
	close(f.gate)
	waitFor(t, "the answer", func() bool { return len(env.LookupHost("a.test")) == 1 })
	if got := env.LookupHost("a.test"); got[0] != netip.MustParseAddr("10.0.0.9") {
		t.Errorf("LookupHost = %v, want 10.0.0.9 (unmapped)", got)
	}
	if n := f.lookups.Load(); n != 1 {
		t.Errorf("%d lookups for one name inside the TTL, want 1", n)
	}
	// After the TTL it refreshes, still returning the old answer meanwhile.
	f.advance(2 * time.Minute)
	f.mu.Lock()
	f.answer["a.test"] = []netip.Addr{netip.MustParseAddr("10.0.0.10")}
	f.mu.Unlock()
	if got := env.LookupHost("a.test"); len(got) != 1 || got[0] != netip.MustParseAddr("10.0.0.9") {
		t.Errorf("LookupHost during refresh = %v, want the previous answer", got)
	}
	waitFor(t, "the refresh", func() bool {
		g := env.LookupHost("a.test")
		return len(g) == 1 && g[0] == netip.MustParseAddr("10.0.0.10")
	})
}

func TestSystemEnv_LookupFailureKeepsTheLastAnswer(t *testing.T) {
	f := &fakeSrc{now: time.Unix(1000, 0), env: map[string]string{}, answer: map[string][]netip.Addr{"a.test": {netip.MustParseAddr("10.0.0.9")}}}
	env := newSystemEnv(f.source())
	env.LookupHost("a.test")
	waitFor(t, "first answer", func() bool { return len(env.LookupHost("a.test")) == 1 })
	f.advance(2 * time.Minute)
	f.mu.Lock()
	f.lkErr = errors.New("servfail")
	f.mu.Unlock()
	env.LookupHost("a.test")
	waitFor(t, "failed refresh", func() bool { return f.lookups.Load() >= 2 })
	time.Sleep(20 * time.Millisecond)
	if got := env.LookupHost("a.test"); len(got) != 1 {
		t.Errorf("after a failed refresh LookupHost = %v, want the last answer", got)
	}
}

func TestSystemEnv_LookupIsBounded(t *testing.T) {
	f := &fakeSrc{now: time.Unix(1000, 0), env: map[string]string{}, gate: make(chan struct{})}
	env := newSystemEnv(f.source())
	for i := 0; i < 50; i++ {
		env.LookupHost("h" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + ".test")
	}
	waitFor(t, "lookups to start", func() bool { return f.lookups.Load() == dnsParallel })
	time.Sleep(20 * time.Millisecond)
	if n := f.lookups.Load(); n != dnsParallel {
		t.Errorf("%d lookups in flight, want at most %d", n, dnsParallel)
	}
	close(f.gate)
	waitFor(t, "lookups to drain", func() bool { return len(env.LookupHost("haa.test")) == 0 && f.lookups.Load() >= dnsParallel })
}

func TestSystemEnv_CacheEviction(t *testing.T) {
	f := &fakeSrc{now: time.Unix(1000, 0), env: map[string]string{}}
	s := &sysEnv{src: f.source(), dns: map[string]*dnsEntry{}, sem: make(chan struct{}, dnsParallel)}
	now := f.now
	for i := 0; i < dnsMaxEntries; i++ {
		s.dns[string(rune(0x4e00+i))] = &dnsEntry{used: now, fetched: now}
	}
	// Full of fresh names: a new one is not cached and not looked up.
	if got := s.lookupHost("new.test"); got != nil || len(s.dns) != dnsMaxEntries {
		t.Fatalf("full cache grew or answered: %v, %d entries", got, len(s.dns))
	}
	// Idle names are dropped to make room.
	f.advance(dnsIdle + time.Second)
	s.lookupHost("new.test")
	if _, ok := s.dns["new.test"]; !ok || len(s.dns) > 2 {
		t.Errorf("idle names were not evicted: %d entries", len(s.dns))
	}
	// A result arriving for a name that was evicted is dropped.
	s.fetch("gone.test")
}

func TestSystemEnv_RealProcess(t *testing.T) {
	env := SystemEnv()
	_ = env.IPs()
	_ = env.Hostnames()
	if env.Now == nil || env.LookupHost == nil {
		t.Fatal("SystemEnv has nil functions")
	}
	// The shared env is built once.
	a, b := SharedSystemEnv(), SharedSystemEnv()
	if a.Now == nil || b.Now == nil {
		t.Fatal("SharedSystemEnv has nil functions")
	}
	// The real resolver answers (or fails) without blocking the caller.
	start := time.Now()
	env.LookupHost("localhost")
	if time.Since(start) > time.Second {
		t.Errorf("LookupHost blocked for %v", time.Since(start))
	}
	waitFor(t, "localhost to resolve", func() bool { return len(env.LookupHost("localhost")) > 0 })
}
