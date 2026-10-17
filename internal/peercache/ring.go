package peercache

import (
	"hash/crc32"
	"sort"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/selfaddr"
)

const defaultVnodes = 150

type Ring struct {
	mu          sync.RWMutex
	vnodes      int
	keys        []uint32
	ring        map[uint32]string
	members     map[string]bool
	selfAddr    string
	self        *selfaddr.Tracker
	selfMember  string // the member that is this pod; empty while it is not identified
	sameAZRing  map[uint32]string
	sameAZKeys  []uint32
	hasZoneInfo bool
}

func NewRing(selfAddr string, vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = defaultVnodes
	}
	return &Ring{
		vnodes:     vnodes,
		ring:       make(map[uint32]string),
		members:    make(map[string]bool),
		selfAddr:   selfAddr,
		self:       selfaddr.NewTracker("peercache", selfAddr, selfaddr.SharedSystemEnv()),
		sameAZRing: make(map[uint32]string),
	}
}

// SetSelfEnv replaces what the ring knows about this pod (tests; production
// uses the process's own). The next Set resolves with it.
func (r *Ring) SetSelfEnv(env selfaddr.Env) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.self = selfaddr.NewTracker("peercache", r.selfAddr, env)
	r.selfMember = ""
}

// resolveSelf finds this pod among peers. selfAddr is the pod's listen address
// (":9428") and discovery lists pods by IP or hostname, so a plain comparison
// would never say "this key is mine" and the pod would fetch its own keys over
// HTTP. A pod that is not identified keeps asking its peers, which is slower
// but always right. Called without r.mu held (it takes no network time).
func (r *Ring) resolveSelf(peers []string) string {
	r.mu.RLock()
	tr := r.self
	r.mu.RUnlock()
	res := tr.Resolve(peers)
	if res.Members > 0 {
		tr.SetInPeers(res.OK())
	}
	if res.OK() {
		return res.Self
	}
	return ""
}

func (r *Ring) isSelf(peer string) bool { return peer != "" && peer == r.selfMember }

func (r *Ring) Set(peers []string) {
	self := r.resolveSelf(peers)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.selfMember = self

	r.ring = make(map[uint32]string)
	r.members = make(map[string]bool)
	r.keys = nil

	for _, peer := range peers {
		r.members[peer] = true
		for i := 0; i < r.vnodes; i++ {
			h := hashKey(peer, i)
			r.ring[h] = peer
			r.keys = append(r.keys, h)
		}
	}

	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

func (r *Ring) Lookup(key string) (peer string, isLocal bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.keys) == 0 {
		return r.selfAddr, true
	}

	h := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
	if idx >= len(r.keys) {
		idx = 0
	}

	peer = r.ring[r.keys[idx]]
	return peer, r.isSelf(peer)
}

func (r *Ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	members := make([]string, 0, len(r.members))
	for m := range r.members {
		members = append(members, m)
	}
	sort.Strings(members)
	return members
}

func (r *Ring) MemberCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.members)
}

func (r *Ring) SetWithZones(peerZones map[string]string, selfZone string) {
	peers := make([]string, 0, len(peerZones))
	for peer := range peerZones {
		peers = append(peers, peer)
	}
	sort.Strings(peers)
	self := r.resolveSelf(peers)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.selfMember = self

	r.ring = make(map[uint32]string)
	r.members = make(map[string]bool)
	r.keys = nil
	r.sameAZRing = make(map[uint32]string)
	r.sameAZKeys = nil
	r.hasZoneInfo = selfZone != ""

	for peer, zone := range peerZones {
		r.members[peer] = true
		for i := 0; i < r.vnodes; i++ {
			h := hashKey(peer, i)
			r.ring[h] = peer
			r.keys = append(r.keys, h)

			if zone == selfZone {
				r.sameAZRing[h] = peer
				r.sameAZKeys = append(r.sameAZKeys, h)
			}
		}
	}

	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
	sort.Slice(r.sameAZKeys, func(i, j int) bool { return r.sameAZKeys[i] < r.sameAZKeys[j] })
}

func (r *Ring) LookupAZ(key string) (peer string, isLocal bool, isSameAZ bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.keys) == 0 {
		return r.selfAddr, true, true
	}

	if !r.hasZoneInfo || len(r.sameAZKeys) == 0 {
		h := crc32.ChecksumIEEE([]byte(key))
		idx := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
		if idx >= len(r.keys) {
			idx = 0
		}
		peer = r.ring[r.keys[idx]]
		return peer, r.isSelf(peer), true
	}

	h := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(r.sameAZKeys), func(i int) bool { return r.sameAZKeys[i] >= h })
	if idx >= len(r.sameAZKeys) {
		idx = 0
	}
	peer = r.sameAZRing[r.sameAZKeys[idx]]
	return peer, r.isSelf(peer), true
}

// SameAZMembers returns the subset of members in the same AZ as self.
// Returns the full member list when zone info is unavailable (treating
// every peer as same-AZ so consumers degrade safely). The returned
// slice is freshly allocated and sorted; safe to mutate.
//
// Used by internal/compaction.OwnershipResolver for AZ-stratified HRW
// (spec §12.1).
func (r *Ring) SameAZMembers() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.hasZoneInfo {
		out := make([]string, 0, len(r.members))
		for m := range r.members {
			out = append(out, m)
		}
		sort.Strings(out)
		return out
	}

	seen := make(map[string]struct{}, len(r.sameAZRing))
	for _, peer := range r.sameAZRing {
		seen[peer] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for peer := range seen {
		out = append(out, peer)
	}
	sort.Strings(out)
	return out
}

// HasZoneInfo reports whether the ring was populated with zone data.
// When false, callers should treat the cluster as zone-agnostic.
func (r *Ring) HasZoneInfo() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.hasZoneInfo
}

func (r *Ring) MemberCountByZone() (sameAZ, crossAZ int) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.hasZoneInfo {
		return len(r.members), 0
	}

	sameAZMembers := make(map[string]bool)
	for _, peer := range r.sameAZRing {
		sameAZMembers[peer] = true
	}
	sameAZ = len(sameAZMembers)
	crossAZ = len(r.members) - sameAZ
	return sameAZ, crossAZ
}

func hashKey(peer string, vnode int) uint32 {
	b := []byte(peer)
	v := uint32(vnode)                                           // #nosec G115 -- intentional truncation for hash input
	b = append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v)) // #nosec G115
	return crc32.ChecksumIEEE(b)
}
