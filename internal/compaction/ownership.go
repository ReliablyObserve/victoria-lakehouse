// Package compaction owns partition assignment, scheduling, and orphan
// cleanup. The OwnershipResolver type in this file replaces the
// election-based leader gate and CRC32 partition sharding with a single
// Highest-Random-Weight (HRW / rendezvous) hashing primitive over the
// live peer set advertised by internal/peercache.
//
// for the architectural rationale (why HRW over consistent
// hashing, why xxhash over CRC32). Ownership hashes over the whole writer list,
// never a per-availability-zone subset.
package compaction

import (
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/selfaddr"
)

// OwnershipResolver decides which pod owns which compaction partition
// using HRW hashing over the live peer set. It is safe for concurrent
// use; all state is either immutable after construction or accessed via
// the Peers / Stabilizing / IsDraining callbacks (which the embedder
// must make concurrency-safe — peercache already is).
type OwnershipResolver struct {
	// Self is this pod's listen address (":9428", "0.0.0.0:9428",
	// "10.0.1.42:9428"). Discovery lists the pods by IP or by hostname, so Self
	// is not looked up in the peer list as it is: internal/selfaddr finds the
	// member that is this pod (by IP, hostname, dashed-IP name or DNS). A pod
	// that cannot be found owns nothing, and the lakehouse_ring_self_resolved
	// gauge says so.
	Self string

	// Env is what selfaddr knows about this pod (its addresses and hostnames).
	// The zero value means the process's own (selfaddr.SharedSystemEnv); tests
	// inject theirs.
	Env selfaddr.Env

	// Peers returns the current full peer set. Called on every
	// ownership decision so ring updates are picked up immediately;
	// implementation must be cheap and concurrency-safe (peercache's
	// Members() satisfies both).
	Peers func() []string

	// Stabilizing returns true while the ring is in its post-change
	// stabilization window. While true OwnsPartition returns false
	// unconditionally — callers must defer mutating work until the
	// window closes. peercache.PeerCache.IsStabilizing is the
	// canonical implementation.
	Stabilizing func() bool

	// IsDraining returns true for peers that have advertised
	// X-Lakehouse-Draining via the peer-cache HTTP layer. Draining
	// peers are filtered out of HRW ownership BEFORE they disappear
	// from DNS (spec §11.2 and edge case 31).
	IsDraining func(peer string) bool

	// hashFn is exposed only via WithHashFunc for tests that want
	// deterministic, collision-engineerable hashes. Default xxhash.
	hashFn func(string) uint64

	// tracker resolves this pod in the peer list, once per list, and publishes
	// lakehouse_ring_self_resolved / lakehouse_ring_self_in_peers.
	trackerOnce sync.Once
	tracker     *selfaddr.Tracker
}

// NewOwnershipResolver constructs a resolver with the default xxhash
// hash function. Callers must populate at minimum Self and Peers; the
// other callbacks have safe defaults.
func NewOwnershipResolver(self string, peers func() []string) *OwnershipResolver {
	return &OwnershipResolver{
		Self:   self,
		Peers:  peers,
		hashFn: xxhashString,
	}
}

// xxhashString is the default hash used by the resolver — xxh64 of the
// input bytes (no fancy seed). Already in the binary via parquet-go's
// dictionary hashing, so no new transitive dep.
func xxhashString(s string) uint64 { return xxhash.Sum64String(s) }

// WithHashFunc swaps the hash for tests that need deterministic
// tie-breaks or collisions. Returns the same resolver for chaining.
func (r *OwnershipResolver) WithHashFunc(h func(string) uint64) *OwnershipResolver {
	r.hashFn = h
	return r
}

// hashFor falls back to xxhash if the field was never set (zero-value
// resolver case in tests). Always non-nil after the first call.
func (r *OwnershipResolver) hashFor() func(string) uint64 {
	if r.hashFn != nil {
		return r.hashFn
	}
	r.hashFn = xxhashString
	return r.hashFn
}

// selfTracker returns the resolver's selfaddr.Tracker, built on first use from
// Self and Env.
func (r *OwnershipResolver) selfTracker() *selfaddr.Tracker {
	r.trackerOnce.Do(func() {
		env := r.Env
		if env.IPs == nil && env.Hostnames == nil && env.LookupHost == nil {
			env = selfaddr.SharedSystemEnv()
		}
		r.tracker = selfaddr.NewTracker("compaction", r.Self, env)
	})
	return r.tracker
}

// SelfInPeersGauge is lakehouse_ring_self_in_peers{ring="compaction"} as 0/1: 1
// only when this pod was identified as exactly one member of the peer list and
// that member is in the set ownership hashes over.
func (r *OwnershipResolver) SelfInPeersGauge() int64 { return r.selfTracker().InPeers() }

// SelfResolvedGauge is lakehouse_ring_self_resolved{ring="compaction"} as 0/1.
func (r *OwnershipResolver) SelfResolvedGauge() int64 { return r.selfTracker().Resolved() }

// hrwWeight combines peer and partition with a single xxh64 call. We
// concatenate with a unit-separator byte (0x1F) that cannot appear in
// either input (partitions are [a-z0-9=/-], peers are host:port), so
// no collision can be engineered by choosing inputs. See spec §2.1.2.
func hrwWeight(peer, partition string, h func(string) uint64) uint64 {
	// Avoid an extra allocation: build the digest input on the stack
	// for typical key sizes (peer ≤ ~32 chars, partition ≤ ~32 chars).
	// Falls back to heap for larger inputs.
	var buf [128]byte
	if len(peer)+1+len(partition) <= len(buf) {
		n := copy(buf[:], peer)
		buf[n] = 0x1F
		m := copy(buf[n+1:], partition)
		return h(string(buf[:n+1+m]))
	}
	return h(peer + "\x1F" + partition)
}

// peerWeight is a small (peer, weight) record used while ranking. Kept
// outside RankedOwners so the slice can be reused (currently isn't —
// the function is called rarely enough that the per-call allocation is
// not on the hot path).
type peerWeight struct {
	peer   string
	weight uint64
}

// peersForOwnership is the peer set ownership hashes over: the discovered list
// without the peers that advertise they are draining. It is the same list on
// every pod, whatever availability zone a pod runs in: compaction reads and
// writes S3 only, so a pod's zone gains nothing, and a per-zone subset would give
// every partition one owner per zone. Returns nil when no live peer is
// available; the caller treats that as "nobody can own anything right now" and
// skips work (edge case 10).
func (r *OwnershipResolver) peersForOwnership() []string {
	return r.effectivePeers(r.rawPeers())
}

// rawPeers is the peer list exactly as discovery returned it.
func (r *OwnershipResolver) rawPeers() []string {
	if r.Peers == nil {
		return nil
	}
	return r.Peers()
}

// view is one consistent look at the ring: the set ownership hashes over, this
// pod's member in it, and whether this pod may own anything. The pod is found
// in the raw list (a draining pod is still itself), and may own only if that
// member is in the set. A pod that cannot be identified owns nothing: it never
// invents a member for itself, which is what made partitions ownerless or
// owned by several writers when the listen address was added to the list.
func (r *OwnershipResolver) view() (peers []string, self string, owns bool) {
	raw := r.rawPeers()
	tr := r.selfTracker()
	res := tr.Resolve(raw)
	peers = r.effectivePeers(raw)
	if res.OK() {
		self = res.Self
		for _, p := range peers {
			if p == self {
				owns = true
				break
			}
		}
	}
	tr.SetInPeers(owns)
	return peers, self, owns
}

// SelfMember is this pod's entry in the peer list, and false when the pod is
// not in the set ownership hashes over (not identified, ambiguous, draining).
func (r *OwnershipResolver) SelfMember() (string, bool) {
	_, self, owns := r.view()
	return self, owns
}

// effectivePeers is raw without the draining peers.
func (r *OwnershipResolver) effectivePeers(raw []string) []string {
	return r.filterDraining(raw)
}

// filterDraining returns the input minus any peers for which
// IsDraining returns true. Returns a fresh slice to avoid mutating the
// caller's input.
func (r *OwnershipResolver) filterDraining(in []string) []string {
	if r.IsDraining == nil {
		if len(in) == 0 {
			return nil
		}
		out := make([]string, len(in))
		copy(out, in)
		return out
	}
	out := make([]string, 0, len(in))
	for _, p := range in {
		if !r.IsDraining(p) {
			out = append(out, p)
		}
	}
	return out
}

// RankedOwners returns every peer ranked by HRW weight for the given
// partition, primary first. Used by OrphanSweep Tier A to find the
// secondary owner that should reclaim a stalled partition.
//
// Behaviour:
//   - Empty peer set (DNS down at startup): returns nil. Callers treat
//     this as "nobody owns anything" and skip work.
//   - Single peer: returns that peer, whether or not it is this pod (a pod
//     that is not identified in the list is reported by the
//     lakehouse_ring_self_resolved gauge, and OwnsPartition is false for it).
//   - Hash collision (vanishingly rare with 64-bit xxhash): break the
//     tie by lex-comparing peer strings so the ordering is deterministic
//     across the cluster.
func (r *OwnershipResolver) RankedOwners(partition string) []string {
	peers := r.peersForOwnership()
	if len(peers) == 0 {
		return nil
	}

	ranked := make([]peerWeight, 0, len(peers))
	h := r.hashFor()
	for _, p := range peers {
		ranked = append(ranked, peerWeight{peer: p, weight: hrwWeight(p, partition, h)})
	}
	// Highest weight wins. Lex tiebreak makes the order stable across
	// pods when xxh64 collides (probability ~2^-64 per pair, so this
	// branch is mostly there for fuzz/property tests).
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].weight != ranked[j].weight {
			return ranked[i].weight > ranked[j].weight
		}
		return ranked[i].peer < ranked[j].peer
	})

	out := make([]string, len(ranked))
	for i, rw := range ranked {
		out[i] = rw.peer
	}
	return out
}

// OwnsPartition returns true when this pod is the primary owner of the
// given partition. Returns false:
//   - while the ring is stabilizing (spec §3.1 cases 3 and 22);
//   - when the peer set is empty (edge case 10);
//   - when this pod is not the highest-weight ranked owner;
//   - when this pod is not identified as exactly one member of the peer set
//     (lakehouse_ring_self_resolved is 0), or is identified but filtered out
//     of the set it hashes over (draining).
//
// Updates the ring self gauges when the decision changes.
func (r *OwnershipResolver) OwnsPartition(partition string) bool {
	if r.Stabilizing != nil && r.Stabilizing() {
		return false
	}

	peers, self, owns := r.view()
	if len(peers) == 0 || !owns {
		return false
	}
	owners := r.rankFromPeers(partition, peers)
	if len(owners) == 0 {
		return false
	}
	return owners[0] == self
}

// OwnerOf returns the primary owner peer for the given partition, or
// the empty string when the peer set is empty. Useful for diagnostic
// log lines and Tier A's "stolen from primary" message.
func (r *OwnershipResolver) OwnerOf(partition string) string {
	owners := r.RankedOwners(partition)
	if len(owners) == 0 {
		return ""
	}
	return owners[0]
}

// SecondaryOwner returns the second-ranked HRW owner — the pod that
// Tier A promotes when the primary falls behind by 3×Interval. Returns
// the primary owner when fewer than two peers are available (degenerate
// single-pod cluster).
func (r *OwnershipResolver) SecondaryOwner(partition string) string {
	owners := r.RankedOwners(partition)
	if len(owners) == 0 {
		return ""
	}
	if len(owners) == 1 {
		return owners[0]
	}
	return owners[1]
}

// TertiaryOwner returns the third-ranked HRW owner. Falls back to the
// secondary (then the primary) when the peer set has fewer than three
// members. Used by the sweep when both primary and secondary appear
// unhealthy.
func (r *OwnershipResolver) TertiaryOwner(partition string) string {
	owners := r.RankedOwners(partition)
	switch {
	case len(owners) == 0:
		return ""
	case len(owners) == 1:
		return owners[0]
	case len(owners) == 2:
		return owners[1]
	default:
		return owners[2]
	}
}

// IsStabilizing surfaces the Stabilizing callback as a method so
// scheduler/sweep code can defer entire scans cheaply (vs. enumerating
// thousands of partitions only to skip each).
func (r *OwnershipResolver) IsStabilizing() bool {
	if r.Stabilizing == nil {
		return false
	}
	return r.Stabilizing()
}

// rankFromPeers is a thin helper that takes a pre-filtered peer list
// and produces the same HRW ranking as RankedOwners. Used by
// OwnsPartition to avoid recomputing peersForOwnership twice.
func (r *OwnershipResolver) rankFromPeers(partition string, peers []string) []string {
	ranked := make([]peerWeight, 0, len(peers))
	h := r.hashFor()
	for _, p := range peers {
		ranked = append(ranked, peerWeight{peer: p, weight: hrwWeight(p, partition, h)})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].weight != ranked[j].weight {
			return ranked[i].weight > ranked[j].weight
		}
		return ranked[i].peer < ranked[j].peer
	})
	out := make([]string, len(ranked))
	for i, rw := range ranked {
		out[i] = rw.peer
	}
	return out
}
