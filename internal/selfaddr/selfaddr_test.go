package selfaddr

import (
	"net/netip"
	"testing"
)

func ips(s ...string) func() []netip.Addr {
	return func() []netip.Addr {
		out := make([]netip.Addr, 0, len(s))
		for _, v := range s {
			out = append(out, netip.MustParseAddr(v))
		}
		return out
	}
}

func names(s ...string) func() []string { return func() []string { return s } }

func TestSelfaddr_Forms(t *testing.T) {
	sts := []string{
		"lh-insert-0.lh-insert-headless.ns.svc.cluster.local:9428",
		"lh-insert-1.lh-insert-headless.ns.svc.cluster.local:9428",
		"lh-insert-2.lh-insert-headless.ns.svc.cluster.local:9428",
	}
	v4 := []string{"10.1.2.3:9428", "10.1.2.4:9428", "10.1.2.5:9428"}

	for _, tc := range []struct {
		name    string
		listen  string
		members []string
		env     Env
		want    string // "" = unresolved
		reason  string
	}{
		{"ipv4 by pod ip", ":9428", v4, Env{IPs: ips("10.1.2.4")}, "10.1.2.4:9428", "matched"},
		{"ipv4 by interface list holding several addresses", ":9428", v4, Env{IPs: ips("192.168.0.9", "10.1.2.5")}, "10.1.2.5:9428", "matched"},
		{"wildcard host 0.0.0.0", "0.0.0.0:9428", v4, Env{IPs: ips("10.1.2.3")}, "10.1.2.3:9428", "matched"},
		{"wildcard host [::]", "[::]:9428", v4, Env{IPs: ips("10.1.2.3")}, "10.1.2.3:9428", "matched"},
		{"listen host is the pod ip", "10.1.2.4:9428", v4, Env{}, "10.1.2.4:9428", "matched"},
		{"listen host is the pod ip, member written differently", "10.1.2.4:9428", []string{"[::ffff:10.1.2.4]:9428", "10.1.2.5:9428"}, Env{}, "[::ffff:10.1.2.4]:9428", "matched"},
		{"listen host is a specific ip other than the member", "10.1.2.9:9428", v4, Env{}, "", "not_listed"},
		{"ipv6 literal member", ":9428", []string{"[fd00::5]:9428", "[fd00::6]:9428"}, Env{IPs: ips("fd00::6")}, "[fd00::6]:9428", "matched"},
		{"ipv6 written differently", ":9428", []string{"[fd00:0:0:0:0:0:0:5]:9428", "[fd00::6]:9428"}, Env{IPs: ips("fd00::5")}, "[fd00:0:0:0:0:0:0:5]:9428", "matched"},
		{"ipv6 zone in the member", ":9428", []string{"[fe80::1%eth0]:9428", "[fe80::2%eth0]:9428"}, Env{IPs: ips("fe80::2")}, "[fe80::2%eth0]:9428", "matched"},
		{"ipv6 zone on the pod address", ":9428", []string{"[fe80::1]:9428", "[fe80::2]:9428"}, Env{IPs: ips("fe80::2%eth0")}, "[fe80::2]:9428", "matched"},
		{"ipv4-mapped member", ":9428", []string{"[::ffff:10.1.2.3]:9428", "10.1.2.4:9428"}, Env{IPs: ips("10.1.2.3")}, "[::ffff:10.1.2.3]:9428", "matched"},
		{"statefulset fqdn by hostname", ":9428", sts, Env{Hostnames: names("lh-insert-1")}, sts[1], "matched"},
		{"hostname is itself an fqdn", ":9428", sts, Env{Hostnames: names("lh-insert-2.lh-insert-headless.ns.svc.cluster.local")}, sts[2], "matched"},
		{"hostname case does not matter", ":9428", sts, Env{Hostnames: names("LH-INSERT-0")}, sts[0], "matched"},
		{"second hostname (POD_NAME under hostNetwork)", ":9428", sts, Env{Hostnames: names("node-17", "lh-insert-1")}, sts[1], "matched"},
		{"fqdn with a trailing dot", ":9428", []string{"lh-insert-0.svc.ns.svc.cluster.local.:9428", "lh-insert-1.svc.ns.svc.cluster.local.:9428"}, Env{Hostnames: names("lh-insert-1")}, "lh-insert-1.svc.ns.svc.cluster.local.:9428", "matched"},
		{"short hostname member", ":9428", []string{"lh-insert-0:9428", "lh-insert-1:9428"}, Env{Hostnames: names("lh-insert-1")}, "lh-insert-1:9428", "matched"},
		{"dashed ipv4", ":9428", []string{"10-1-2-3.svc.ns.svc.cluster.local:9428", "10-1-2-4.svc.ns.svc.cluster.local:9428"}, Env{IPs: ips("10.1.2.4")}, "10-1-2-4.svc.ns.svc.cluster.local:9428", "matched"},
		{"dashed ipv6 compressed", ":9428", []string{"fd00--5.svc.ns.svc.cluster.local:9428", "fd00--6.svc.ns.svc.cluster.local:9428"}, Env{IPs: ips("fd00::6")}, "fd00--6.svc.ns.svc.cluster.local:9428", "matched"},
		{"dashed ipv6 expanded", ":9428", []string{"fd00-0-0-0-0-0-0-5.svc:9428", "fd00-0-0-0-0-0-0-6.svc:9428"}, Env{IPs: ips("fd00::5")}, "fd00-0-0-0-0-0-0-5.svc:9428", "matched"},
		{"dashed label of another pod", ":9428", []string{"10-1-2-3.svc:9428"}, Env{IPs: ips("10.1.2.9")}, "", "not_listed"},
		{"exact listen address among the members", ":9428", []string{":9428"}, Env{}, ":9428", "matched"},
		{"exact beats everything else", ":9428", []string{":9428", "10.1.2.3:9428"}, Env{IPs: ips("10.1.2.3")}, ":9428", "matched"},
		{"a member on another port is not this pod", ":9428", []string{"10.1.2.3:9999"}, Env{IPs: ips("10.1.2.3")}, "", "not_listed"},
		{"hostname on another port is not this pod", ":9428", []string{"lh-insert-0.svc:9999"}, Env{Hostnames: names("lh-insert-0")}, "", "not_listed"},
		{"traces port", ":10428", []string{"10.1.2.3:10428", "10.1.2.3:9428"}, Env{IPs: ips("10.1.2.3")}, "10.1.2.3:10428", "matched"},
		{"two members match: unresolved", ":9428", []string{"10.1.2.3:9428", "10.1.2.4:9428"}, Env{IPs: ips("10.1.2.3", "10.1.2.4")}, "", "ambiguous"},
		{"ip and hostname match different members: unresolved", ":9428", []string{"10.1.2.3:9428", "lh-insert-1.svc:9428"}, Env{IPs: ips("10.1.2.3"), Hostnames: names("lh-insert-1")}, "", "ambiguous"},
		{"one member matched by ip and by hostname counts once", ":9428", []string{"10.1.2.3:9428", "lh-insert-1.svc:9428"}, Env{IPs: ips("10.1.2.3"), Hostnames: names("lh-insert-9")}, "10.1.2.3:9428", "matched"},
		{"a repeated member counts once", ":9428", []string{"10.1.2.3:9428", "10.1.2.3:9428"}, Env{IPs: ips("10.1.2.3")}, "10.1.2.3:9428", "matched"},
		{"none match: unresolved", ":9428", v4, Env{IPs: ips("192.168.1.1")}, "", "not_listed"},
		{"no addresses known", ":9428", v4, Env{}, "", "not_listed"},
		{"loopback never identifies a pod", ":9428", []string{"127.0.0.1:9428"}, Env{IPs: ips("127.0.0.1")}, "", "not_listed"},
		{"POD_IP only, no interfaces", ":9428", v4, Env{IPs: ips("10.1.2.3")}, "10.1.2.3:9428", "matched"},
		{"no members", ":9428", nil, Env{IPs: ips("10.1.2.3")}, "", "no_members"},
		{"listen without a port", "9428", v4, Env{IPs: ips("10.1.2.3")}, "", "not_listed"},
		{"listen with an empty port", "host:", v4, Env{IPs: ips("10.1.2.3")}, "", "not_listed"},
		{"member that is not host:port", ":9428", []string{"10.1.2.3", "nonsense"}, Env{IPs: ips("10.1.2.3")}, "", "not_listed"},
		{"a hostname label that only starts like ours", ":9428", sts, Env{Hostnames: names("lh-insert")}, "", "not_listed"},
		{"hostname equal to the whole fqdn, label differs", ":9428", []string{"a.lh-insert-1.svc:9428"}, Env{Hostnames: names("lh-insert-1")}, "", "not_listed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			self, ok := Resolve(tc.listen, tc.members, tc.env)
			if self != tc.want || ok != (tc.want != "") {
				t.Errorf("Resolve = (%q, %v), want (%q, %v)", self, ok, tc.want, tc.want != "")
			}
			if got := Match(tc.listen, tc.members, tc.env).Reason(); got != tc.reason {
				t.Errorf("Reason = %q, want %q", got, tc.reason)
			}
		})
	}
}

func TestSelfaddr_DNSRule(t *testing.T) {
	members := []string{"a.example.test:9428", "b.example.test:9428"}
	look := func(m map[string][]string) func(string) []netip.Addr {
		return func(h string) []netip.Addr {
			var out []netip.Addr
			for _, v := range m[h] {
				out = append(out, netip.MustParseAddr(v))
			}
			return out
		}
	}
	t.Run("a member that resolves to a local address", func(t *testing.T) {
		env := Env{IPs: ips("10.0.0.2"), LookupHost: look(map[string][]string{"a.example.test": {"10.0.0.1"}, "b.example.test": {"10.0.0.2"}})}
		if self, ok := Resolve(":9428", members, env); !ok || self != members[1] {
			t.Errorf("Resolve = (%q, %v), want %q", self, ok, members[1])
		}
	})
	t.Run("not cached yet: unresolved", func(t *testing.T) {
		env := Env{IPs: ips("10.0.0.2"), LookupHost: look(nil)}
		if self, ok := Resolve(":9428", members, env); ok {
			t.Errorf("Resolve = (%q, true), want unresolved while the lookup is not cached", self)
		}
	})
	t.Run("two members resolve to this pod: unresolved", func(t *testing.T) {
		env := Env{IPs: ips("10.0.0.2"), LookupHost: look(map[string][]string{"a.example.test": {"10.0.0.2"}, "b.example.test": {"10.0.0.2"}})}
		if _, ok := Resolve(":9428", members, env); ok {
			t.Error("Resolve = ok with two members resolving to this pod, want unresolved")
		}
	})
	t.Run("not asked when a cheaper rule matched", func(t *testing.T) {
		asked := 0
		env := Env{IPs: ips("10.0.0.2"), LookupHost: func(string) []netip.Addr { asked++; return nil }}
		if _, ok := Resolve(":9428", []string{"10.0.0.2:9428", "a.example.test:9428"}, env); !ok {
			t.Fatal("not resolved")
		}
		if asked != 0 {
			t.Errorf("the DNS cache was asked %d times although an IP matched", asked)
		}
	})
	t.Run("an ip literal is never looked up", func(t *testing.T) {
		asked := 0
		env := Env{LookupHost: func(string) []netip.Addr { asked++; return nil }}
		Resolve(":9428", []string{"10.0.0.7:9428"}, env)
		if asked != 0 {
			t.Errorf("looked up an IP literal %d times", asked)
		}
	})
}

func TestSelfaddr_DashedAddr(t *testing.T) {
	for label, want := range map[string]string{
		"10-1-2-3":           "10.1.2.3",
		"fd00--5":            "fd00::5",
		"fd00-0-0-0-0-0-0-5": "fd00::5",
		"1-2-3-4-5-6-7-8":    "1:2:3:4:5:6:7:8",
	} {
		got, ok := dashedAddr(label)
		if !ok || got != netip.MustParseAddr(want) {
			t.Errorf("dashedAddr(%q) = %v, %v; want %s", label, got, ok, want)
		}
	}
	for _, label := range []string{"", "lh-insert-0", "10", "10-1-2", "10-1-2-3-4", "300-1-2-3", "a-b", "-"} {
		if got, ok := dashedAddr(label); ok {
			t.Errorf("dashedAddr(%q) = %v, want no address", label, got)
		}
	}
}

func TestSelfaddr_ResultReasons(t *testing.T) {
	if (Result{Matches: 1}).Reason() != "matched" || (Result{Matches: 2}).Reason() != "ambiguous" ||
		(Result{}).Reason() != "no_members" || (Result{Members: 3}).Reason() != "not_listed" {
		t.Error("unexpected reason strings")
	}
}
