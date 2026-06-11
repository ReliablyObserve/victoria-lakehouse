// Package compaction owns partition assignment, scheduling, and orphan
// cleanup. The OwnershipResolver type in this file replaces the
// election-based leader gate and CRC32 partition sharding with a single
// Highest-Random-Weight (HRW / rendezvous) hashing primitive over the
// live peer set advertised by internal/peercache.
//
// for the architectural rationale (why HRW over consistent
// hashing, why xxhash over CRC32) and §12.1 for the AZ-stratification
// design.
package compaction

import (
	"sort"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

// OwnershipResolver decides which pod owns which compaction partition
// using HRW hashing over the live peer set. It is safe for concurrent
// use; all state is either immutable after construction or accessed via
// the Peers / Stabilizing / IsDraining callbacks (which the embedder
// must make concurrency-safe — peercache already is).
type OwnershipResolver struct {
	// Self is this pod's address as it appears in the peer list
	// (typically cfg.ListenAddr verbatim, e.g. ":9428" or
	// "10.0.1.42:9428"). If Self does not match what peers see in
	// discovery, HRW silently picks the "wrong self" and this pod
	// never owns anything — the SelfInPeers gauge is meant to surface
	// that misconfiguration loudly. See spec §2.1.2 and §8.1 R1.
	Self string

	// Peers returns the current full peer set. Called on every
	// ownership decision so ring updates are picked up immediately;
	// implementation must be cheap and concurrency-safe (peercache's
	// Members() satisfies both).
	Peers func() []string

	// SameAZPeers, when non-nil, returns the subset of peers in the
	// same AZ as Self. When nil, AZ stratification is disabled and
	// HRW runs over the full Peers() set. Used to keep cross-AZ
	// traffic bounded (spec §12.1).
	SameAZPeers func() []string

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

	// SelfInPeers is a 0/1 gauge updated on every ownership decision.
	// When this pod is missing from its own peer set the value drops
	// to 0 — operators page on `lakehouse_compaction_ownership_self_in_peers
	// == 0 for 5m`. See spec §6.1.
	selfInPeers atomic.Int64
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

// SelfInPeersGauge returns the most recent self-in-peers result as a
// 0/1 int. Wiring helper for the metrics layer; callers can wrap this
// in a metrics.NewGaugeFunc.
func (r *OwnershipResolver) SelfInPeersGauge() int64 { return r.selfInPeers.Load() }

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

// peersForOwnership decides whether to run HRW over the same-AZ subset
// or the full peer set, then filters out draining peers. Returns the
// peer slice to feed into ranking.
//
// AZ stratification (spec §12.1):
//   - If SameAZPeers is configured AND returns a non-empty list once
//     draining peers are removed, use that subset.
//   - Otherwise fall back to the full (draining-filtered) peer set.
//
// Returns nil when no live peer is available — caller treats that as
// "nobody can own anything right now" and skips work (edge case 10).
func (r *OwnershipResolver) peersForOwnership() []string {
	rawAll := []string{}
	if r.Peers != nil {
		rawAll = r.Peers()
	}

	all := r.filterDraining(rawAll)

	if r.SameAZPeers != nil {
		rawAZ := r.SameAZPeers()
		azFiltered := r.filterDraining(rawAZ)
		if len(azFiltered) > 0 {
			return azFiltered
		}
	}

	return all
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
//   - Single peer: returns [self] (or [otherPeer] if Self isn't in the
//     ring — the unexpected case that the SelfInPeers gauge catches).
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
//   - when self is missing from the peer set entirely (SelfInPeers gauge
//     captures the misconfiguration).
//
// Updates the SelfInPeers gauge as a side effect on every call.
func (r *OwnershipResolver) OwnsPartition(partition string) bool {
	if r.Stabilizing != nil && r.Stabilizing() {
		return false
	}

	peers := r.peersForOwnership()
	r.recordSelfInPeers(peers)

	if len(peers) == 0 {
		return false
	}
	owners := r.rankFromPeers(partition, peers)
	if len(owners) == 0 {
		return false
	}
	return owners[0] == r.Self
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

// recordSelfInPeers ticks the SelfInPeers gauge to 1 when Self appears
// in the (already filtered) peer list, 0 otherwise. Called from every
// OwnsPartition decision so the gauge tracks the current ground truth.
func (r *OwnershipResolver) recordSelfInPeers(peers []string) {
	for _, p := range peers {
		if p == r.Self {
			r.selfInPeers.Store(1)
			return
		}
	}
	r.selfInPeers.Store(0)
}
