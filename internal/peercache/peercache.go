package peercache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

type PeerCache struct {
	ring       *Ring
	authKey    string
	httpClient *http.Client
	selfAZ     string

	hits   atomic.Uint64
	misses atomic.Uint64
	errors atomic.Uint64

	rcm *ringChangeManager
}

// DefaultStabilizeDuration is used when no explicit stabilize duration is provided.
const DefaultStabilizeDuration = 60 * time.Second

func New(selfAddr, authKey string, timeout time.Duration, maxConns int) *PeerCache {
	transport := &http.Transport{
		MaxIdleConnsPerHost: maxConns,
		IdleConnTimeout:     90 * time.Second,
	}
	return &PeerCache{
		ring:    NewRing(selfAddr, defaultVnodes),
		authKey: authKey,
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		rcm: newRingChangeManager(DefaultStabilizeDuration),
	}
}

// NewWithStabilize creates a PeerCache with a custom ring stabilization duration.
func NewWithStabilize(selfAddr, authKey string, timeout time.Duration, maxConns int, stabilizeDuration time.Duration) *PeerCache {
	pc := New(selfAddr, authKey, timeout, maxConns)
	pc.rcm = newRingChangeManager(stabilizeDuration)
	return pc
}

func (pc *PeerCache) UpdatePeers(peers []string) {
	oldMembers := pc.ring.Members()
	pc.ring.Set(peers)
	newMembers := pc.ring.Members()

	events := pc.rcm.detectChanges(oldMembers, newMembers)
	if len(events) > 0 {
		pc.rcm.processChanges(events, oldMembers, len(newMembers))
	}
}

func (pc *PeerCache) Lookup(key string) (peer string, isLocal bool) {
	return pc.ring.Lookup(key)
}

func (pc *PeerCache) Fetch(ctx context.Context, peer, key string) ([]byte, bool, error) {
	url := fmt.Sprintf("http://%s/internal/cache/fetch?key=%s", peer, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, false, err
	}
	if pc.authKey != "" {
		req.Header.Set("X-Peer-Auth-Key", pc.authKey)
	}

	resp, err := pc.httpClient.Do(req)
	if err != nil {
		pc.errors.Add(1)
		return nil, false, err
	}
	defer func() { _ = resp.Body.Close() }()

	// Detect draining peer from response header
	if resp.Header.Get("X-Lakehouse-Draining") != "" {
		pc.rcm.RecordDraining(peer)
		logger.Infof("peer %s is draining (X-Lakehouse-Draining header detected)", peer)
	}

	if resp.StatusCode == http.StatusNotFound {
		pc.misses.Add(1)
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		pc.errors.Add(1)
		body, _ := io.ReadAll(resp.Body)
		return nil, false, fmt.Errorf("peer %s returned %d: %s", peer, resp.StatusCode, string(body))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		pc.errors.Add(1)
		return nil, false, fmt.Errorf("read peer response: %w", err)
	}

	pc.hits.Add(1)
	return data, true, nil
}

func (pc *PeerCache) Has(ctx context.Context, peer, key string) (bool, error) {
	url := fmt.Sprintf("http://%s/internal/cache/has?key=%s", peer, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	if pc.authKey != "" {
		req.Header.Set("X-Peer-Auth-Key", pc.authKey)
	}

	resp, err := pc.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	_ = resp.Body.Close()

	return resp.StatusCode == http.StatusOK, nil
}

type Stats struct {
	Members int
	Hits    uint64
	Misses  uint64
	Errors  uint64
}

func (pc *PeerCache) Stats() Stats {
	return Stats{
		Members: pc.ring.MemberCount(),
		Hits:    pc.hits.Load(),
		Misses:  pc.misses.Load(),
		Errors:  pc.errors.Load(),
	}
}

func (pc *PeerCache) Members() []string {
	return pc.ring.Members()
}

func (pc *PeerCache) UpdatePeersWithZones(peerZones map[string]string, selfAZ string) {
	pc.selfAZ = selfAZ
	oldMembers := pc.ring.Members()
	pc.ring.SetWithZones(peerZones, selfAZ)
	newMembers := pc.ring.Members()

	events := pc.rcm.detectChanges(oldMembers, newMembers)
	if len(events) > 0 {
		pc.rcm.processChanges(events, oldMembers, len(newMembers))
	}
}

func (pc *PeerCache) LookupAZ(key string) (peer string, isLocal bool, isSameAZ bool) {
	return pc.ring.LookupAZ(key)
}

func (pc *PeerCache) SelfAZ() string { return pc.selfAZ }

// OnRingChange registers a callback to be invoked when ring membership changes.
// Callbacks are called asynchronously to avoid blocking the refresh loop.
func (pc *PeerCache) OnRingChange(fn func(RingChangeEvent)) {
	pc.rcm.OnRingChange(fn)
}

// IsStabilizing returns true if the ring is in a stabilization period after a change.
func (pc *PeerCache) IsStabilizing() bool {
	return pc.rcm.IsStabilizing()
}

// IsShadowMember returns true if the peer is in the shadow set during stabilization.
func (pc *PeerCache) IsShadowMember(peer string) bool {
	return pc.rcm.IsShadowMember(peer)
}

// ShadowMembers returns the shadow member set active during stabilization. Returns nil otherwise.
func (pc *PeerCache) ShadowMembers() []string {
	return pc.rcm.ShadowMembers()
}

// IsDraining returns true if the peer has sent an X-Lakehouse-Draining header.
func (pc *PeerCache) IsDraining(peer string) bool {
	return pc.rcm.IsDraining(peer)
}

// DrainingPeers returns all peers that have indicated they are draining.
func (pc *PeerCache) DrainingPeers() map[string]time.Time {
	return pc.rcm.DrainingPeers()
}

type StatsAZ struct {
	Stats
	SelfAZ         string
	SameAZMembers  int
	CrossAZMembers int
}

func (pc *PeerCache) StatsAZ() StatsAZ {
	s := pc.Stats()
	sameAZ, crossAZ := pc.ring.MemberCountByZone()
	return StatsAZ{
		Stats:          s,
		SelfAZ:         pc.selfAZ,
		SameAZMembers:  sameAZ,
		CrossAZMembers: crossAZ,
	}
}

type Handler struct {
	mu      sync.RWMutex
	cache   map[string][]byte
	authKey string
	selfAZ  string
}

func NewHandler(authKey, selfAZ string) *Handler {
	return &Handler{
		cache:   make(map[string][]byte),
		authKey: authKey,
		selfAZ:  selfAZ,
	}
}

func (h *Handler) SetSelfAZ(az string) {
	h.mu.Lock()
	h.selfAZ = az
	h.mu.Unlock()
}

func (h *Handler) Put(key string, data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	h.cache[key] = cp
}

func (h *Handler) Get(key string) ([]byte, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	data, ok := h.cache[key]
	if !ok {
		return nil, false
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return cp, true
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.authKey != "" {
		if r.Header.Get("X-Peer-Auth-Key") != h.authKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	if r.URL.Path == "/internal/cache/stats" {
		h.mu.RLock()
		az := h.selfAZ
		h.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		azJSON, _ := json.Marshal(az)
		_, _ = fmt.Fprintf(w, `{"az":%s}`, azJSON)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key parameter", http.StatusBadRequest)
		return
	}

	switch r.URL.Path {
	case "/internal/cache/fetch":
		data, ok := h.Get(key)
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)

	case "/internal/cache/has":
		_, ok := h.Get(key)
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)

	default:
		http.NotFound(w, r)
	}
}
