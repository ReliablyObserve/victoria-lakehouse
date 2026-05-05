package peercache

import (
	"hash/crc32"
	"sort"
	"sync"
)

const defaultVnodes = 150

type Ring struct {
	mu       sync.RWMutex
	vnodes   int
	keys     []uint32
	ring     map[uint32]string
	members  map[string]bool
	selfAddr string
}

func NewRing(selfAddr string, vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = defaultVnodes
	}
	return &Ring{
		vnodes:   vnodes,
		ring:     make(map[uint32]string),
		members:  make(map[string]bool),
		selfAddr: selfAddr,
	}
}

func (r *Ring) Set(peers []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

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
	return peer, peer == r.selfAddr
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

func hashKey(peer string, vnode int) uint32 {
	b := []byte(peer)
	v := uint32(vnode)                                           // #nosec G115 -- intentional truncation for hash input
	b = append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v)) // #nosec G115
	return crc32.ChecksumIEEE(b)
}
