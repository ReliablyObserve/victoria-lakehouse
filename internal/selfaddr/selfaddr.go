// Package selfaddr finds this pod in a list of ring members.
//
// A pod knows its listen address (":9428", "0.0.0.0:9428", "[::]:9428"). Service
// discovery lists the pods behind a headless service as IP:port (A records) or
// hostname:port (SRV records of a StatefulSet, or dashed-IP names). The two
// never compare equal, so a pod that looks for its listen address in the list
// never finds itself: it hashes as an extra member next to its own entry, and
// every consumer of the ring (compaction ownership, the peer cache) disagrees
// with its peers about who owns what.
//
// Match decides which member, if any, is this pod. The rules are cheap string
// and IP comparisons over an Env that is cached, so a decision never touches
// the network or the interfaces. A pod that cannot be identified is reported as
// unresolved and callers fail closed: they own nothing rather than guess.
package selfaddr

import (
	"net"
	"net/netip"
	"strings"
	"time"
)

// Env is what the pod knows about itself. Every function is optional (a nil
// function knows nothing) and must be cheap and safe for concurrent use: the
// production Env (SystemEnv) caches its answers.
type Env struct {
	// Now is the clock the Tracker uses for its memo; nil means time.Now.
	Now func() time.Time
	// Hostnames are the names this pod answers to: POD_NAME and os.Hostname().
	// Only the first DNS label is compared.
	Hostnames func() []string
	// IPs are the addresses of this pod: POD_IP / POD_IPS and the interfaces.
	IPs func() []netip.Addr
	// LookupHost returns the addresses a member hostname resolved to, from a
	// cache. It never blocks: a name that is not cached yet returns nil and is
	// filled in the background, so the answer improves on a later call.
	LookupHost func(host string) []netip.Addr
}

func (e Env) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

// Result is the outcome of looking for this pod in a member list.
type Result struct {
	// Self is the member that is this pod; empty unless Matches == 1.
	Self string
	// Matches is the number of distinct members that match this pod.
	Matches int
	// Members is the number of distinct members that were examined.
	Members int
}

// OK reports whether exactly one member is this pod.
func (r Result) OK() bool { return r.Matches == 1 }

// Reason says why a pod is, or is not, identified.
func (r Result) Reason() string {
	switch {
	case r.Matches == 1:
		return "matched"
	case r.Matches > 1:
		return "ambiguous"
	case r.Members == 0:
		return "no_members"
	default:
		return "not_listed"
	}
}

// Resolve returns the member of members that is this pod. ok is false when none
// matches or several do; a caller then treats the pod as not in the ring.
func Resolve(listen string, members []string, env Env) (self string, ok bool) {
	r := Match(listen, members, env)
	return r.Self, r.OK()
}

// Match looks for this pod in members. The pod's port (from listen) must equal
// the member's port, then any of these identifies a member:
//
//  1. the member is the listen address exactly (a pod that is alone in its ring);
//  2. the member's host is an IP literal that is one of this pod's addresses
//     (IPv4 and IPv6, zones stripped, IPv4-mapped IPv6 unmapped);
//  3. the first DNS label of the member's host is the first label of one of the
//     pod's hostnames (StatefulSet SRV: lh-insert-0.svc.ns.svc.cluster.local);
//  4. the first label is a dashed IP ("10-1-2-3", "fd00--5") of this pod;
//  5. the member's host resolves, in the Env's cache, to one of this pod's
//     addresses. Tried only when 1 to 4 matched nothing.
//
// Members are counted once however many rules match them.
func Match(listen string, members []string, env Env) Result {
	uniq := distinct(members)
	res := Result{Members: len(uniq)}
	if len(uniq) == 0 {
		return res
	}
	for _, m := range uniq {
		if m == listen {
			res.Self, res.Matches = m, 1
			return res
		}
	}
	host, port, err := net.SplitHostPort(listen)
	if err != nil || port == "" {
		return res
	}
	local := localSet(host, env)
	var names []string
	if env.Hostnames != nil {
		for _, h := range env.Hostnames() {
			if l := firstLabel(h); l != "" {
				names = append(names, l)
			}
		}
	}

	type cand struct {
		member, host string
	}
	var unresolvedNames []cand
	count := func(m string) {
		res.Matches++
		if res.Matches == 1 {
			res.Self = m
		} else {
			res.Self = ""
		}
	}
	for _, m := range uniq {
		mh, mp, err := net.SplitHostPort(m)
		if err != nil || mp != port {
			continue
		}
		if ip, ok := parseAddr(mh); ok {
			if local.has(ip) {
				count(m)
			}
			continue
		}
		label := firstLabel(mh)
		if label == "" {
			continue
		}
		if matchesAny(label, names) {
			count(m)
			continue
		}
		if ip, ok := dashedAddr(label); ok {
			if local.has(ip) {
				count(m)
			}
			continue
		}
		unresolvedNames = append(unresolvedNames, cand{member: m, host: strings.TrimSuffix(mh, ".")})
	}
	if res.Matches == 0 && env.LookupHost != nil {
		for _, c := range unresolvedNames {
			for _, a := range env.LookupHost(c.host) {
				if ip, ok := normalize(a); ok && local.has(ip) {
					count(c.member)
					break
				}
			}
		}
	}
	return res
}

// addrSet is the set of this pod's addresses.
type addrSet []netip.Addr

func (s addrSet) has(a netip.Addr) bool {
	for _, x := range s {
		if x == a {
			return true
		}
	}
	return false
}

// localSet is the pod's addresses: the Env's, plus the listen host when it is a
// specific address. Loopback and unspecified addresses never identify a pod.
func localSet(listenHost string, env Env) addrSet {
	var out addrSet
	add := func(a netip.Addr) {
		if n, ok := normalize(a); ok && !n.IsLoopback() && !n.IsUnspecified() && !out.has(n) {
			out = append(out, n)
		}
	}
	if env.IPs != nil {
		for _, a := range env.IPs() {
			add(a)
		}
	}
	if a, ok := parseAddr(listenHost); ok {
		add(a)
	}
	return out
}

// normalize drops the zone and unmaps an IPv4-mapped IPv6 address so every
// textual form of an address compares equal.
func normalize(a netip.Addr) (netip.Addr, bool) {
	if !a.IsValid() {
		return netip.Addr{}, false
	}
	return a.WithZone("").Unmap(), true
}

// parseAddr parses an IP literal (any zone is dropped).
func parseAddr(s string) (netip.Addr, bool) {
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Addr{}, false
	}
	return normalize(a)
}

// firstLabel is the first DNS label of host, lower-cased, without a trailing dot.
func firstLabel(host string) string {
	host = strings.TrimSuffix(host, ".")
	if i := strings.IndexByte(host, '.'); i >= 0 {
		host = host[:i]
	}
	return strings.ToLower(host)
}

func matchesAny(label string, names []string) bool {
	for _, n := range names {
		if n == label {
			return true
		}
	}
	return false
}

// dashedAddr reads a pod-style DNS label back into an address: "10-1-2-3" is
// 10.1.2.3, "fd00--5" is fd00::5. The label is parsed rather than the local
// addresses formatted, so any dash form a resolver produces that parses to the
// same address matches.
func dashedAddr(label string) (netip.Addr, bool) {
	if !strings.Contains(label, "-") {
		return netip.Addr{}, false
	}
	if a, err := netip.ParseAddr(strings.ReplaceAll(label, "-", ".")); err == nil && a.Is4() {
		return a, true
	}
	if a, err := netip.ParseAddr(strings.ReplaceAll(label, "-", ":")); err == nil {
		return normalize(a)
	}
	return netip.Addr{}, false
}

// distinct returns members without repeats, in order.
func distinct(members []string) []string {
	if len(members) < 2 {
		return members
	}
	seen := make(map[string]struct{}, len(members))
	out := make([]string, 0, len(members))
	for _, m := range members {
		if _, dup := seen[m]; dup {
			continue
		}
		seen[m] = struct{}{}
		out = append(out, m)
	}
	return out
}
