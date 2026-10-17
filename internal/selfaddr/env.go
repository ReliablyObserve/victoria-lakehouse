package selfaddr

import (
	"context"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	// envTTL is how long the interface addresses and the hostnames are reused.
	envTTL = time.Minute
	// dnsTTL is how long a member hostname's addresses are reused.
	dnsTTL = time.Minute
	// dnsTimeout bounds one background lookup.
	dnsTimeout = 2 * time.Second
	// dnsParallel bounds the lookups in flight.
	dnsParallel = 8
	// dnsMaxEntries bounds the cache; names are member hostnames, a handful per pod.
	dnsMaxEntries = 4096
	// dnsIdle drops a name nobody asked for this long.
	dnsIdle = 10 * time.Minute
)

// sysSource is where the production Env reads from; tests replace each piece.
type sysSource struct {
	getenv     func(string) string
	hostname   func() (string, error)
	interfaces func() ([]net.Addr, error)
	lookup     func(ctx context.Context, host string) ([]netip.Addr, error)
	now        func() time.Time
}

func realSource() sysSource {
	return sysSource{
		getenv:     os.Getenv,
		hostname:   os.Hostname,
		interfaces: net.InterfaceAddrs,
		lookup: func(ctx context.Context, host string) ([]netip.Addr, error) {
			return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		},
		now: time.Now,
	}
}

// SystemEnv is the Env of the running process. The interface addresses, the
// environment and the hostname are read at most once a minute; member hostnames
// are resolved in the background (2 s timeout, 8 in flight, cached for a
// minute), so no call blocks on the network.
func SystemEnv() Env { return newSystemEnv(realSource()) }

var (
	sharedOnce sync.Once
	shared     Env
)

// SharedSystemEnv is SystemEnv built once per process, so every ring shares one
// cache of the interfaces and of the DNS answers.
func SharedSystemEnv() Env {
	sharedOnce.Do(func() { shared = SystemEnv() })
	return shared
}

type dnsEntry struct {
	addrs    []netip.Addr
	fetched  time.Time
	used     time.Time
	inflight bool
}

type sysEnv struct {
	src sysSource

	mu        sync.Mutex
	refreshed time.Time
	ips       []netip.Addr
	names     []string
	dns       map[string]*dnsEntry
	sem       chan struct{}
}

func newSystemEnv(src sysSource) Env {
	s := &sysEnv{src: src, dns: map[string]*dnsEntry{}, sem: make(chan struct{}, dnsParallel)}
	return Env{
		Now:        src.now,
		Hostnames:  s.hostnames,
		IPs:        s.addrs,
		LookupHost: s.lookupHost,
	}
}

// refreshLocked re-reads the environment, the hostname and the interfaces when
// the last read is older than envTTL.
func (s *sysEnv) refreshLocked() {
	now := s.src.now()
	if !s.refreshed.IsZero() && now.Sub(s.refreshed) < envTTL {
		return
	}
	s.refreshed = now

	var ips []netip.Addr
	add := func(a netip.Addr) {
		if n, ok := normalize(a); ok && !n.IsLoopback() && !n.IsUnspecified() {
			ips = append(ips, n)
		}
	}
	for _, v := range []string{s.src.getenv("POD_IP"), s.src.getenv("POD_IPS")} {
		for _, f := range strings.Split(v, ",") {
			if a, ok := parseAddr(strings.TrimSpace(f)); ok {
				add(a)
			}
		}
	}
	if addrs, err := s.src.interfaces(); err == nil {
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok {
				if ip, ok := netip.AddrFromSlice(ipn.IP); ok {
					add(ip)
				}
			}
		}
	}
	s.ips = ips

	// POD_NAME is the pod's name even under hostNetwork, where os.Hostname is the node's.
	names := []string{s.src.getenv("POD_NAME")}
	if h, err := s.src.hostname(); err == nil {
		names = append(names, h)
	}
	s.names = names[:0]
	for _, n := range names {
		if n = strings.TrimSpace(n); n != "" {
			s.names = append(s.names, n)
		}
	}
}

func (s *sysEnv) addrs() []netip.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshLocked()
	return s.ips
}

func (s *sysEnv) hostnames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshLocked()
	return s.names
}

// lookupHost returns what host last resolved to and refreshes it in the
// background when it is older than dnsTTL.
func (s *sysEnv) lookupHost(host string) []netip.Addr {
	now := s.src.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.dns[host]
	if e == nil {
		if len(s.dns) >= dnsMaxEntries {
			s.evictLocked(now)
			if len(s.dns) >= dnsMaxEntries {
				return nil
			}
		}
		e = &dnsEntry{}
		s.dns[host] = e
	}
	e.used = now
	if !e.inflight && (e.fetched.IsZero() || now.Sub(e.fetched) >= dnsTTL) {
		select {
		case s.sem <- struct{}{}:
			e.inflight = true
			go s.fetch(host)
		default: // too many lookups in flight; the next call tries again
		}
	}
	return e.addrs
}

func (s *sysEnv) fetch(host string) {
	ctx, cancel := context.WithTimeout(context.Background(), dnsTimeout)
	addrs, err := s.src.lookup(ctx, host)
	cancel()
	<-s.sem

	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.dns[host]
	if e == nil {
		return
	}
	e.inflight = false
	e.fetched = s.src.now()
	if err == nil {
		e.addrs = e.addrs[:0:0]
		for _, a := range addrs {
			if n, ok := normalize(a); ok {
				e.addrs = append(e.addrs, n)
			}
		}
	}
}

func (s *sysEnv) evictLocked(now time.Time) {
	for h, e := range s.dns {
		if !e.inflight && now.Sub(e.used) >= dnsIdle {
			delete(s.dns, h)
		}
	}
}
