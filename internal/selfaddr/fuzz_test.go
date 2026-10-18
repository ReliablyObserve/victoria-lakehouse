package selfaddr

import (
	"net"
	"net/netip"
	"testing"
)

// FuzzSelfaddrResolve: whatever the listen address, members, hostname and
// addresses look like, Resolve does not panic, names at most one member, names
// only a member of the list whose port is the listen port (or the listen address
// itself), and gives the same answer for the same members in another order.
func FuzzSelfaddrResolve(f *testing.F) {
	for _, s := range [][6]string{
		{":9428", "10.1.2.3:9428", "10.1.2.4:9428", "lh-0.svc:9428", "lh-0", "10.1.2.3"},
		{"[::]:9428", "[fd00::5]:9428", "fd00--5.svc:9428", "10-1-2-3.svc:9428", "x", "fd00::5"},
		{"0.0.0.0:10428", "[fe80::1%eth0]:10428", "a.b.c.:10428", ":10428", "A", "fe80::1%eth0"},
		{"", "", "", "", "", ""},
		{":9428", ":9428", "10.1.2.3:9428", "10.1.2.3:9428", "lh-0", "10.1.2.3"},
	} {
		f.Add(s[0], s[1], s[2], s[3], s[4], s[5])
	}
	f.Fuzz(func(t *testing.T, listen, m1, m2, m3, hostname, ip string) {
		env := Env{
			Hostnames: func() []string { return []string{hostname} },
			IPs: func() []netip.Addr {
				if a, err := netip.ParseAddr(ip); err == nil {
					return []netip.Addr{a}
				}
				return nil
			},
			LookupHost: func(h string) []netip.Addr {
				if a, err := netip.ParseAddr(ip); err == nil && len(h)%2 == 0 {
					return []netip.Addr{a}
				}
				return nil
			},
		}
		members := []string{m1, m2, m3}
		res := Match(listen, members, env)
		self, ok := Resolve(listen, members, env)

		if ok != res.OK() || self != res.Self {
			t.Fatalf("Resolve (%q,%v) disagrees with Match %+v", self, ok, res)
		}
		if res.Matches > res.Members || res.Members > len(members) {
			t.Fatalf("counts out of range: %+v for %d members", res, len(members))
		}
		if !ok {
			if self != "" {
				t.Fatalf("unresolved but self = %q", self)
			}
			return
		}
		if res.Matches != 1 {
			t.Fatalf("ok with %d matches", res.Matches)
		}
		in := false
		for _, m := range members {
			in = in || m == self
		}
		if !in {
			t.Fatalf("self %q is not one of the members %q", self, members)
		}
		if self != listen {
			_, lp, err := net.SplitHostPort(listen)
			if err != nil {
				t.Fatalf("resolved %q for an unparsable listen address %q", self, listen)
			}
			if _, mp, err := net.SplitHostPort(self); err != nil || mp != lp {
				t.Fatalf("resolved %q whose port is not the listen port %q", self, lp)
			}
		}
		// The order of discovery does not change who this pod is.
		rev := []string{m3, m2, m1}
		if s2, ok2 := Resolve(listen, rev, env); !ok2 || s2 != self {
			t.Fatalf("reversed members: (%q,%v), want (%q,true)", s2, ok2, self)
		}
	})
}
