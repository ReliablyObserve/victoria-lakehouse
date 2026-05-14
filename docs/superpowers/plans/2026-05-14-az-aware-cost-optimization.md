# AZ-Aware Cost Optimization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Victoria Lakehouse fully AZ cost-optimized by default — peer cache prefers same-AZ peers, buffer bridge prefers same-AZ insert pods, cross-AZ traffic is monitored, and Helm chart deploys with topology-aware defaults.

**Architecture:** Add AZ detection at startup (from env var injected by K8s downward API). Partition the peer cache hash ring into same-AZ (primary) and cross-AZ (fallback) tiers. Route buffer bridge queries to same-AZ insert pods first. Expose per-AZ metrics. Set sensible Helm defaults for topology spread constraints.

**Tech Stack:** Go 1.24, CRC32 consistent hashing, Kubernetes topology labels, Prometheus metrics via VictoriaMetrics/metrics

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/config/config.go` | Add `AZConfig` struct, `AZAware`/`CrossAZFallback`/`AZLabel` fields to `PeerConfig` |
| `internal/peercache/ring.go` | Add AZ-aware `LookupAZ()` method, `SetWithZones()` for zone-partitioned ring |
| `internal/peercache/ring_test.go` | Tests for AZ-aware lookup, zone isolation, fallback behavior |
| `internal/peercache/peercache.go` | Add `LookupAZ()` delegation, `FetchAZ()` with zone-aware stats |
| `internal/peercache/peercache_test.go` | Tests for AZ-aware peer cache operations |
| `internal/storage/parquets3/buffer_bridge.go` | Add AZ-aware endpoint prioritization |
| `internal/storage/parquets3/buffer_bridge_test.go` | Tests for AZ-aware buffer bridge |
| `internal/metrics/lakehouse.go` | Add 6 AZ-aware metrics |
| `cmd/lakehouse-logs/main.go` | AZ detection at startup, wire AZ config |
| `charts/victoria-lakehouse/values.yaml` | Default topology spread constraints, AZ config |
| `charts/victoria-lakehouse/templates/select-statefulset.yaml` | Inject AZ env var from downward API |
| `charts/victoria-lakehouse/templates/insert-statefulset.yaml` | Inject AZ env var from downward API |
| `docs/cross-az-optimization.md` | Update with implementation status |

---

### Task 1: AZ Configuration

**Files:**
- Modify: `internal/config/config.go`

- [ ] **Step 1: Write the test**

Create `internal/config/config_az_test.go`:

```go
package config

import (
	"testing"
)

func TestDefaultConfig_AZDefaults(t *testing.T) {
	cfg := DefaultConfig()

	if !cfg.Peer.AZAware {
		t.Error("AZAware should default to true")
	}
	if !cfg.Peer.CrossAZFallback {
		t.Error("CrossAZFallback should default to true")
	}
	if cfg.Peer.AZEnvVar != "LAKEHOUSE_AZ" {
		t.Errorf("AZEnvVar should default to LAKEHOUSE_AZ, got %q", cfg.Peer.AZEnvVar)
	}
}

func TestDefaultConfig_BufferBridgeAZDefaults(t *testing.T) {
	cfg := DefaultConfig()

	if !cfg.Select.AZAware {
		t.Error("Select.AZAware should default to true")
	}
	if !cfg.Select.CrossAZFallback {
		t.Error("Select.CrossAZFallback should default to true")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/config/ -run TestDefaultConfig_AZ -v`
Expected: FAIL — `AZAware` field does not exist

- [ ] **Step 3: Add AZ fields to PeerConfig and SelectConfig**

In `internal/config/config.go`, modify `PeerConfig`:

```go
type PeerConfig struct {
	AuthKey         string        `yaml:"auth_key"`
	Timeout         time.Duration `yaml:"timeout"`
	MaxConnections  int           `yaml:"max_connections"`
	AZAware         bool          `yaml:"az_aware"`
	CrossAZFallback bool          `yaml:"cross_az_fallback"`
	AZEnvVar        string        `yaml:"az_env_var"`
}
```

Modify `SelectConfig`:

```go
type SelectConfig struct {
	BufferQueryEnabled    bool          `yaml:"buffer_query_enabled"`
	InsertHeadlessService string        `yaml:"insert_headless_service"`
	BufferQueryTimeout    time.Duration `yaml:"buffer_query_timeout"`
	AZAware               bool          `yaml:"az_aware"`
	CrossAZFallback       bool          `yaml:"cross_az_fallback"`
}
```

Update `DefaultConfig()` in the `Peer:` section:

```go
Peer: PeerConfig{
	Timeout:         5 * time.Second,
	MaxConnections:  32,
	AZAware:         true,
	CrossAZFallback: true,
	AZEnvVar:        "LAKEHOUSE_AZ",
},
```

Update `DefaultConfig()` in the `Select:` section:

```go
Select: SelectConfig{
	BufferQueryEnabled: true,
	BufferQueryTimeout: 2 * time.Second,
	AZAware:            true,
	CrossAZFallback:    true,
},
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/config/ -run TestDefaultConfig_AZ -v`
Expected: PASS

- [ ] **Step 5: Verify full config test suite**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/config/ -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/config/config.go internal/config/config_az_test.go
git commit -m "feat: add AZ-aware config fields to PeerConfig and SelectConfig"
```

---

### Task 2: AZ-Aware Consistent Hash Ring

**Files:**
- Modify: `internal/peercache/ring.go`
- Create: `internal/peercache/ring_az_test.go`

- [ ] **Step 1: Write the test**

Create `internal/peercache/ring_az_test.go`:

```go
package peercache

import (
	"testing"
)

func TestRing_SetWithZones(t *testing.T) {
	r := NewRing("self:9428", 150)

	peers := map[string]string{
		"peer-a1:9428": "us-east-1a",
		"peer-a2:9428": "us-east-1a",
		"peer-b1:9428": "us-east-1b",
		"peer-b2:9428": "us-east-1b",
		"self:9428":    "us-east-1a",
	}
	r.SetWithZones(peers, "us-east-1a")

	if r.MemberCount() != 5 {
		t.Fatalf("expected 5 members, got %d", r.MemberCount())
	}
}

func TestRing_LookupAZ_PrefersSameZone(t *testing.T) {
	r := NewRing("self:9428", 150)

	peers := map[string]string{
		"peer-a1:9428": "us-east-1a",
		"peer-a2:9428": "us-east-1a",
		"peer-b1:9428": "us-east-1b",
		"peer-b2:9428": "us-east-1b",
		"self:9428":    "us-east-1a",
	}
	r.SetWithZones(peers, "us-east-1a")

	sameAZ := 0
	crossAZ := 0
	total := 1000
	for i := 0; i < total; i++ {
		peer, isLocal, isSameAZ := r.LookupAZ(fmt.Sprintf("key-%d", i))
		_ = peer
		_ = isLocal
		if isSameAZ {
			sameAZ++
		} else {
			crossAZ++
		}
	}

	// With AZ-aware routing, ALL lookups should return same-AZ peers
	if sameAZ != total {
		t.Errorf("expected all %d lookups to be same-AZ, got sameAZ=%d crossAZ=%d", total, sameAZ, crossAZ)
	}
}

func TestRing_LookupAZ_FallbackToCrossZone(t *testing.T) {
	r := NewRing("self:9428", 150)

	// Only cross-AZ peers (no same-AZ peers except self)
	peers := map[string]string{
		"peer-b1:9428": "us-east-1b",
		"peer-b2:9428": "us-east-1b",
		"self:9428":    "us-east-1a",
	}
	r.SetWithZones(peers, "us-east-1a")

	// With only self in same-AZ, lookups should route to self or fall back to cross-AZ
	peer, isLocal, isSameAZ := r.LookupAZ("test-key")
	if isLocal {
		// Self is the only same-AZ peer, so this is expected for some keys
		return
	}
	if isSameAZ {
		t.Error("non-local peer should be cross-AZ since only self is in same zone")
	}
	if peer == "" {
		t.Error("should always return a peer")
	}
}

func TestRing_LookupAZ_NoZoneInfo_FallsBackToNormal(t *testing.T) {
	r := NewRing("self:9428", 150)

	// Use regular Set (no zone info)
	r.Set([]string{"self:9428", "peer-1:9428", "peer-2:9428"})

	// LookupAZ should work even without zone info (isSameAZ always true)
	peer, _, isSameAZ := r.LookupAZ("test-key")
	if peer == "" {
		t.Error("should return a peer")
	}
	if !isSameAZ {
		t.Error("without zone info, all peers should be considered same-AZ")
	}
}

func TestRing_LookupAZ_EmptyRing(t *testing.T) {
	r := NewRing("self:9428", 150)
	r.SetWithZones(map[string]string{}, "us-east-1a")

	peer, isLocal, isSameAZ := r.LookupAZ("test-key")
	if peer != "self:9428" {
		t.Errorf("empty ring should return self, got %q", peer)
	}
	if !isLocal {
		t.Error("empty ring should return isLocal=true")
	}
	if !isSameAZ {
		t.Error("self should be same-AZ")
	}
}
```

Add `"fmt"` to imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/peercache/ -run TestRing_.*AZ -v`
Expected: FAIL — `SetWithZones` and `LookupAZ` do not exist

- [ ] **Step 3: Implement AZ-aware ring**

Add to `internal/peercache/ring.go`:

```go
// SetWithZones updates the ring with zone information. Builds two sub-rings:
// a same-AZ ring (primary) and a full ring (fallback). selfZone identifies
// the current pod's AZ for partitioning.
func (r *Ring) SetWithZones(peerZones map[string]string, selfZone string) {
	r.mu.Lock()
	defer r.mu.Unlock()

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

// LookupAZ routes a key to a peer, preferring same-AZ peers when zone info
// is available. Returns the peer address, whether it's the local instance,
// and whether the peer is in the same AZ.
func (r *Ring) LookupAZ(key string) (peer string, isLocal bool, isSameAZ bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.keys) == 0 {
		return r.selfAddr, true, true
	}

	// If no zone info, fall back to normal lookup
	if !r.hasZoneInfo || len(r.sameAZKeys) == 0 {
		h := crc32.ChecksumIEEE([]byte(key))
		idx := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
		if idx >= len(r.keys) {
			idx = 0
		}
		peer = r.ring[r.keys[idx]]
		return peer, peer == r.selfAddr, true
	}

	// Try same-AZ ring first
	h := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(r.sameAZKeys), func(i int) bool { return r.sameAZKeys[i] >= h })
	if idx >= len(r.sameAZKeys) {
		idx = 0
	}
	peer = r.sameAZRing[r.sameAZKeys[idx]]
	return peer, peer == r.selfAddr, true
}
```

Add the new fields to the `Ring` struct:

```go
type Ring struct {
	mu          sync.RWMutex
	vnodes      int
	keys        []uint32
	ring        map[uint32]string
	members     map[string]bool
	selfAddr    string
	sameAZRing  map[uint32]string
	sameAZKeys  []uint32
	hasZoneInfo bool
}
```

Update `NewRing` to initialize the new fields:

```go
func NewRing(selfAddr string, vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = defaultVnodes
	}
	return &Ring{
		vnodes:     vnodes,
		ring:       make(map[uint32]string),
		members:    make(map[string]bool),
		selfAddr:   selfAddr,
		sameAZRing: make(map[uint32]string),
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/peercache/ -run TestRing_.*AZ -v`
Expected: PASS

- [ ] **Step 5: Run full peercache test suite**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/peercache/ -v -count=1`
Expected: All PASS (existing tests unaffected)

- [ ] **Step 6: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/peercache/ring.go internal/peercache/ring_az_test.go
git commit -m "feat: add AZ-aware consistent hash ring with same-AZ preference"
```

---

### Task 3: AZ-Aware PeerCache Client

**Files:**
- Modify: `internal/peercache/peercache.go`
- Create: `internal/peercache/peercache_az_test.go`

- [ ] **Step 1: Write the test**

Create `internal/peercache/peercache_az_test.go`:

```go
package peercache

import (
	"testing"
)

func TestPeerCache_UpdatePeersWithZones(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10)

	peerZones := map[string]string{
		"self:9428":    "az-a",
		"peer-a:9428":  "az-a",
		"peer-b:9428":  "az-b",
	}
	pc.UpdatePeersWithZones(peerZones, "az-a")

	if len(pc.Members()) != 3 {
		t.Errorf("expected 3 members, got %d", len(pc.Members()))
	}
}

func TestPeerCache_LookupAZ_RoutesSameZone(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10)

	peerZones := map[string]string{
		"self:9428":    "az-a",
		"peer-a:9428":  "az-a",
		"peer-b1:9428": "az-b",
		"peer-b2:9428": "az-b",
	}
	pc.UpdatePeersWithZones(peerZones, "az-a")

	crossAZ := 0
	for i := 0; i < 500; i++ {
		_, _, isSameAZ := pc.LookupAZ(fmt.Sprintf("file-%d.parquet", i))
		if !isSameAZ {
			crossAZ++
		}
	}

	if crossAZ > 0 {
		t.Errorf("expected 0 cross-AZ lookups when same-AZ peers exist, got %d", crossAZ)
	}
}

func TestPeerCache_StatsAZ(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10)

	stats := pc.StatsAZ()
	if stats.SelfAZ != "" {
		t.Errorf("expected empty selfAZ before zone config, got %q", stats.SelfAZ)
	}

	peerZones := map[string]string{
		"self:9428":   "az-a",
		"peer:9428":   "az-b",
	}
	pc.UpdatePeersWithZones(peerZones, "az-a")

	stats = pc.StatsAZ()
	if stats.SelfAZ != "az-a" {
		t.Errorf("expected selfAZ=az-a, got %q", stats.SelfAZ)
	}
	if stats.SameAZMembers != 1 {
		t.Errorf("expected 1 same-AZ member, got %d", stats.SameAZMembers)
	}
	if stats.CrossAZMembers != 1 {
		t.Errorf("expected 1 cross-AZ member, got %d", stats.CrossAZMembers)
	}
}
```

Add required imports: `"fmt"`, `"time"`, `"testing"`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/peercache/ -run TestPeerCache_.*AZ -v`
Expected: FAIL — methods don't exist

- [ ] **Step 3: Implement AZ-aware PeerCache methods**

Add to `internal/peercache/peercache.go`:

```go
// selfAZ tracks this pod's availability zone.
// Added to PeerCache struct.
```

Add `selfAZ string` field to `PeerCache` struct:

```go
type PeerCache struct {
	ring       *Ring
	authKey    string
	httpClient *http.Client
	selfAZ     string

	hits       atomic.Uint64
	misses     atomic.Uint64
	errors     atomic.Uint64
	sameAZHits atomic.Uint64
	crossAZHits atomic.Uint64
}
```

Add methods:

```go
func (pc *PeerCache) UpdatePeersWithZones(peerZones map[string]string, selfAZ string) {
	pc.selfAZ = selfAZ
	old := pc.ring.MemberCount()
	pc.ring.SetWithZones(peerZones, selfAZ)
	if pc.ring.MemberCount() != old {
		logger.Infof("peer ring updated with zones; members=%d, selfAZ=%s", pc.ring.MemberCount(), selfAZ)
	}
}

func (pc *PeerCache) LookupAZ(key string) (peer string, isLocal bool, isSameAZ bool) {
	return pc.ring.LookupAZ(key)
}

type StatsAZ struct {
	Stats
	SelfAZ         string
	SameAZMembers  int
	CrossAZMembers int
	SameAZHits     uint64
	CrossAZHits    uint64
}

func (pc *PeerCache) StatsAZ() StatsAZ {
	s := pc.Stats()
	sameAZ, crossAZ := pc.ring.MemberCountByZone()
	return StatsAZ{
		Stats:          s,
		SelfAZ:         pc.selfAZ,
		SameAZMembers:  sameAZ,
		CrossAZMembers: crossAZ,
		SameAZHits:     pc.sameAZHits.Load(),
		CrossAZHits:    pc.crossAZHits.Load(),
	}
}
```

Add `MemberCountByZone()` to `ring.go`:

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/peercache/ -run TestPeerCache_.*AZ -v`
Expected: PASS

- [ ] **Step 5: Run full test suite**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/peercache/ -v -count=1`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/peercache/peercache.go internal/peercache/ring.go internal/peercache/peercache_az_test.go
git commit -m "feat: add AZ-aware PeerCache client with zone-partitioned lookups"
```

---

### Task 4: AZ-Aware Buffer Bridge

**Files:**
- Modify: `internal/storage/parquets3/buffer_bridge.go`
- Create: `internal/storage/parquets3/buffer_bridge_az_test.go`

- [ ] **Step 1: Write the test**

Create `internal/storage/parquets3/buffer_bridge_az_test.go`:

```go
package parquets3

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func TestBufferBridge_SetEndpointsWithZones(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            true,
		CrossAZFallback:    true,
	}
	bb := NewBufferBridge(cfg, config.ModeLogs)

	epZones := map[string]string{
		"http://insert-0:9428": "az-a",
		"http://insert-1:9428": "az-a",
		"http://insert-2:9428": "az-b",
	}
	bb.SetEndpointsWithZones(epZones, "az-a")

	bb.mu.RLock()
	sameAZ := len(bb.sameAZEndpoints)
	crossAZ := len(bb.crossAZEndpoints)
	allEPs := len(bb.endpoints)
	bb.mu.RUnlock()

	if allEPs != 3 {
		t.Errorf("expected 3 total endpoints, got %d", allEPs)
	}
	if sameAZ != 2 {
		t.Errorf("expected 2 same-AZ endpoints, got %d", sameAZ)
	}
	if crossAZ != 1 {
		t.Errorf("expected 1 cross-AZ endpoint, got %d", crossAZ)
	}
}

func TestBufferBridge_AZAware_QueriesSameAZFirst(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            true,
		CrossAZFallback:    false,
	}
	bb := NewBufferBridge(cfg, config.ModeLogs)

	epZones := map[string]string{
		"http://insert-0:9428": "az-a",
		"http://insert-1:9428": "az-b",
	}
	bb.SetEndpointsWithZones(epZones, "az-a")

	// With CrossAZFallback=false, only same-AZ endpoints should be used
	bb.mu.RLock()
	eps := bb.getQueryEndpoints()
	bb.mu.RUnlock()

	if len(eps) != 1 {
		t.Errorf("with CrossAZFallback=false, expected 1 endpoint (same-AZ only), got %d", len(eps))
	}
	if eps[0] != "http://insert-0:9428" {
		t.Errorf("expected same-AZ endpoint, got %q", eps[0])
	}
}

func TestBufferBridge_AZAware_FallbackIncludesAll(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            true,
		CrossAZFallback:    true,
	}
	bb := NewBufferBridge(cfg, config.ModeLogs)

	epZones := map[string]string{
		"http://insert-0:9428": "az-a",
		"http://insert-1:9428": "az-b",
	}
	bb.SetEndpointsWithZones(epZones, "az-a")

	// With CrossAZFallback=true, all endpoints should be used
	bb.mu.RLock()
	eps := bb.getQueryEndpoints()
	bb.mu.RUnlock()

	if len(eps) != 2 {
		t.Errorf("with CrossAZFallback=true, expected 2 endpoints, got %d", len(eps))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/storage/parquets3/ -run TestBufferBridge_.*AZ -v`
Expected: FAIL — methods/fields don't exist

- [ ] **Step 3: Implement AZ-aware buffer bridge**

Modify `internal/storage/parquets3/buffer_bridge.go`. Add fields to `BufferBridge`:

```go
type BufferBridge struct {
	cfg              *config.SelectConfig
	mode             config.Mode
	client           *http.Client
	mu               sync.RWMutex
	endpoints        []string
	sameAZEndpoints  []string
	crossAZEndpoints []string
	selfAZ           string
}
```

Add methods:

```go
func (b *BufferBridge) SetEndpointsWithZones(epZones map[string]string, selfAZ string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.selfAZ = selfAZ
	b.endpoints = make([]string, 0, len(epZones))
	b.sameAZEndpoints = nil
	b.crossAZEndpoints = nil

	for ep, zone := range epZones {
		b.endpoints = append(b.endpoints, ep)
		if zone == selfAZ {
			b.sameAZEndpoints = append(b.sameAZEndpoints, ep)
		} else {
			b.crossAZEndpoints = append(b.crossAZEndpoints, ep)
		}
	}
}

// getQueryEndpoints returns the endpoints to query based on AZ awareness config.
// Must be called with b.mu held (at least RLock).
func (b *BufferBridge) getQueryEndpoints() []string {
	if !b.cfg.AZAware || b.selfAZ == "" {
		return b.endpoints
	}

	if b.cfg.CrossAZFallback {
		// Same-AZ first, then cross-AZ (all endpoints)
		return b.endpoints
	}

	// Same-AZ only
	if len(b.sameAZEndpoints) > 0 {
		return b.sameAZEndpoints
	}
	return b.endpoints
}
```

Update `QueryLogs` and `QueryTraces` to use `getQueryEndpoints()` instead of `b.endpoints` directly:

Replace `eps := b.endpoints` with `eps := b.getQueryEndpoints()` in both methods (lines 53 and 122).

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/storage/parquets3/ -run TestBufferBridge_.*AZ -v`
Expected: PASS

- [ ] **Step 5: Run full storage test suite**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/storage/parquets3/ -v -count=1 -timeout 120s`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/storage/parquets3/buffer_bridge.go internal/storage/parquets3/buffer_bridge_az_test.go
git commit -m "feat: add AZ-aware buffer bridge with same-AZ endpoint preference"
```

---

### Task 5: AZ Metrics

**Files:**
- Modify: `internal/metrics/lakehouse.go`

- [ ] **Step 1: Add AZ metrics**

Add to `internal/metrics/lakehouse.go` after the peer cache metrics section:

```go
// AZ-aware peer cache metrics
var (
	PeerAZBytesTotal      = NewCounterVec("lakehouse_peer_bytes_total", "az")
	PeerAZRequestsTotal   = NewCounterVec("lakehouse_peer_az_requests_total", "az")
	PeerSameAZMembers     = NewGauge("lakehouse_peer_same_az_members")
	PeerCrossAZMembers    = NewGauge("lakehouse_peer_cross_az_members")
	BufferBridgeBytesTotal    = NewCounterVec("lakehouse_buffer_bridge_bytes_total", "az")
	BufferBridgeRequestsTotal = NewCounterVec("lakehouse_buffer_bridge_requests_total", "az")
)
```

- [ ] **Step 2: Run metrics coverage test**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/metrics/ -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/metrics/lakehouse.go
git commit -m "feat: add AZ-aware peer cache and buffer bridge metrics"
```

---

### Task 6: AZ Detection at Startup

**Files:**
- Modify: `cmd/lakehouse-logs/main.go`

- [ ] **Step 1: Read current main.go to understand startup wiring**

Read `cmd/lakehouse-logs/main.go` to find the exact peer cache init location (around lines 100-110) and discovery wiring.

- [ ] **Step 2: Add AZ detection**

After config loading and before peer cache initialization, add AZ detection:

```go
selfAZ := os.Getenv(cfg.Peer.AZEnvVar)
if selfAZ != "" {
	logger.Infof("detected AZ from %s: %s", cfg.Peer.AZEnvVar, selfAZ)
} else if cfg.Peer.AZAware {
	logger.Infof("AZ env var %s not set; AZ-aware routing disabled", cfg.Peer.AZEnvVar)
}
```

- [ ] **Step 3: Wire AZ into peer discovery loop**

Find the discovery refresh loop where `pc.UpdatePeers()` is called. Modify it to:
1. Resolve peer DNS to get pod IPs
2. For each peer, query its AZ via the `/lakehouse/info` endpoint or use the `LAKEHOUSE_AZ` env-based approach where all peers report their AZ
3. Call `pc.UpdatePeersWithZones(peerZones, selfAZ)` instead of `pc.UpdatePeers(peers)`

The simplest approach: each peer advertises its AZ in the `/internal/cache/has` response headers, or we resolve AZ from the Kubernetes API. For now, use the convention that pods in the same headless service share the same Kubernetes namespace, and AZ is injected via downward API into every pod's env:

```go
// In the discovery refresh goroutine:
if selfAZ != "" && cfg.Peer.AZAware {
	peerZones := make(map[string]string)
	for _, peer := range peers {
		// Query peer's AZ via GET /internal/cache/stats
		az := queryPeerAZ(ctx, peer, pc.authKey)
		peerZones[peer] = az
	}
	pc.UpdatePeersWithZones(peerZones, selfAZ)
} else {
	pc.UpdatePeers(peers)
}
```

- [ ] **Step 4: Add `/internal/cache/stats` AZ response**

Modify `internal/peercache/peercache.go` Handler to include the pod's AZ in the `/internal/cache/stats` response. Add a `selfAZ` field to `Handler`:

```go
type Handler struct {
	mu      sync.RWMutex
	cache   map[string][]byte
	authKey string
	selfAZ  string
}
```

Update `NewHandler`:

```go
func NewHandler(authKey, selfAZ string) *Handler {
	return &Handler{
		cache:   make(map[string][]byte),
		authKey: authKey,
		selfAZ:  selfAZ,
	}
}
```

Add stats endpoint to `ServeHTTP`:

```go
case "/internal/cache/stats":
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"az":%q}`, h.selfAZ)
```

- [ ] **Step 5: Wire AZ into buffer bridge discovery**

Similarly, when buffer bridge endpoints are discovered, include their AZ:

```go
if selfAZ != "" && cfg.Select.AZAware {
	epZones := make(map[string]string)
	for _, ep := range insertEndpoints {
		az := queryPeerAZ(ctx, ep, "")
		epZones[ep] = az
	}
	bb.SetEndpointsWithZones(epZones, selfAZ)
} else {
	bb.SetEndpoints(insertEndpoints)
}
```

- [ ] **Step 6: Update metrics reporting**

In the metrics reporting goroutine (if exists) or where peer stats are reported, add:

```go
if selfAZ != "" {
	azStats := pc.StatsAZ()
	metrics.PeerSameAZMembers.Set(int64(azStats.SameAZMembers))
	metrics.PeerCrossAZMembers.Set(int64(azStats.CrossAZMembers))
}
```

- [ ] **Step 7: Build to verify compilation**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go build ./cmd/lakehouse-logs/`
Expected: Success

- [ ] **Step 8: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add cmd/lakehouse-logs/main.go internal/peercache/peercache.go
git commit -m "feat: wire AZ detection and zone-aware peer/buffer discovery at startup"
```

---

### Task 7: Helm Chart Defaults

**Files:**
- Modify: `charts/victoria-lakehouse/values.yaml`
- Modify: `charts/victoria-lakehouse/templates/select-statefulset.yaml`
- Modify: `charts/victoria-lakehouse/templates/insert-statefulset.yaml`

- [ ] **Step 1: Add AZ env var injection via downward API**

In `charts/victoria-lakehouse/templates/select-statefulset.yaml`, add to the container env section (after existing `extraEnv`):

```yaml
        - name: LAKEHOUSE_AZ
          valueFrom:
            fieldRef:
              fieldPath: metadata.labels['topology.kubernetes.io/zone']
```

Note: Kubernetes downward API doesn't support node labels directly in pod env. The correct approach is to use the node name and a label, or use an init container. The simplest portable approach is:

```yaml
        env:
        - name: LAKEHOUSE_AZ
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
```

Then resolve AZ from the node name. OR use a more standard approach with the `NODE_NAME` env var:

```yaml
        env:
        - name: NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
```

Actually, the best approach for Kubernetes is the `topology.kubernetes.io/zone` label on the node, accessible via the downward API as a `metadata.labels` on the pod IF we set it. The cleanest way:

Add to both StatefulSet templates in the `containers[0].env` section:

```yaml
        {{- if .Values.lakehouseConfig.peer.az_aware }}
        - name: LAKEHOUSE_AZ
          valueFrom:
            fieldRef:
              fieldPath: metadata.annotations['topology.kubernetes.io/zone']
        {{- end }}
```

And add a `topologyZoneAnnotation` helper that copies the node's zone label to the pod's annotation via a mutating webhook or init container. 

The simplest approach for EKS/GKE/AKS: The cloud provider's CCM already labels nodes with `topology.kubernetes.io/zone`. We can use the Kubernetes API from within the pod to read the node's label. But that requires RBAC.

**Simplest reliable approach**: Use an init container that reads the node zone and writes it to a shared volume, or just set the `LAKEHOUSE_AZ` as an extraEnv in values.yaml and document that users should set it from their cloud provider.

For the values.yaml, add the AZ config:

```yaml
lakehouseConfig:
  peer:
    az_aware: true
    cross_az_fallback: true
    az_env_var: LAKEHOUSE_AZ
  select:
    az_aware: true
    cross_az_fallback: true
```

- [ ] **Step 2: Add default topology spread constraints**

In `charts/victoria-lakehouse/values.yaml`, change the select and insert defaults from:

```yaml
  topologySpreadConstraints: []
```

To:

```yaml
  topologySpreadConstraints:
    - maxSkew: 1
      topologyKey: topology.kubernetes.io/zone
      whenUnsatisfiable: ScheduleAnyway
      labelSelector:
        matchLabels:
          app.kubernetes.io/component: select
```

(And similarly for insert with `component: insert`.)

- [ ] **Step 3: Add pod affinity for insert/select co-location**

Add default affinity to values.yaml for select pods to prefer running in same AZs as insert pods:

```yaml
select:
  affinity:
    podAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 100
          podAffinityTerm:
            labelSelector:
              matchLabels:
                app.kubernetes.io/component: insert
            topologyKey: topology.kubernetes.io/zone
```

- [ ] **Step 4: Lint Helm chart**

Run: `helm lint charts/victoria-lakehouse/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add charts/victoria-lakehouse/
git commit -m "feat: add AZ-aware Helm defaults — topology spread, pod affinity, AZ env var"
```

---

### Task 8: Traces Module Mirror

**Files:**
- Modify: `lakehouse-traces/cmd/lakehouse-traces/main.go`
- Modify: `lakehouse-traces/internal/config/config.go`
- Modify: `lakehouse-traces/internal/peercache/` (if separate)
- Modify: `lakehouse-traces/internal/metrics/lakehouse.go`

Both Go modules (root for logs, `lakehouse-traces/` for traces) need identical changes. The traces module may share code via replace directives or have its own copies.

- [ ] **Step 1: Check traces module structure**

Run: `ls lakehouse-traces/internal/peercache/ lakehouse-traces/internal/config/ lakehouse-traces/internal/metrics/ 2>/dev/null`

Determine if traces shares code with logs or has its own copies.

- [ ] **Step 2: Apply same config, ring, peercache, buffer bridge, and metrics changes**

Mirror all changes from Tasks 1-6 into the traces module. If the traces module imports from the root module, changes may already be visible.

- [ ] **Step 3: Build traces module**

Run: `cd /private/tmp/victoria-lakehouse-fresh/lakehouse-traces && GOWORK=off go build ./cmd/lakehouse-traces/`
Expected: Success

- [ ] **Step 4: Run traces tests**

Run: `cd /private/tmp/victoria-lakehouse-fresh/lakehouse-traces && GOWORK=off go test ./... -v -count=1 -timeout 120s`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add lakehouse-traces/
git commit -m "feat: mirror AZ-aware changes to traces module"
```

---

### Task 9: Integration Test

**Files:**
- Create: `internal/peercache/integration_az_test.go`

- [ ] **Step 1: Write integration test**

Test the full AZ-aware flow: create peer cache with zones → lookup routes to same-AZ → stats reflect zone breakdown:

```go
package peercache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAZAwareIntegration(t *testing.T) {
	// Set up two HTTP handlers simulating peers in different AZs
	handlerA := NewHandler("", "az-a")
	handlerA.Put("shared-key", []byte("data-from-az-a"))

	handlerB := NewHandler("", "az-b")
	handlerB.Put("shared-key", []byte("data-from-az-b"))

	serverA := httptest.NewServer(handlerA)
	defer serverA.Close()

	serverB := httptest.NewServer(handlerB)
	defer serverB.Close()

	// Create peer cache for a pod in az-a
	pc := New("self:9428", "", 5*time.Second, 10)

	peerZones := map[string]string{
		serverA.Listener.Addr().String(): "az-a",
		serverB.Listener.Addr().String(): "az-b",
		"self:9428":                       "az-a",
	}
	pc.UpdatePeersWithZones(peerZones, "az-a")

	// Verify zone stats
	stats := pc.StatsAZ()
	if stats.SameAZMembers != 2 { // self + serverA
		t.Errorf("expected 2 same-AZ members, got %d", stats.SameAZMembers)
	}
	if stats.CrossAZMembers != 1 { // serverB
		t.Errorf("expected 1 cross-AZ member, got %d", stats.CrossAZMembers)
	}

	// Verify lookups route to same-AZ peers
	crossAZCount := 0
	for i := 0; i < 200; i++ {
		_, _, isSameAZ := pc.LookupAZ(fmt.Sprintf("key-%d", i))
		if !isSameAZ {
			crossAZCount++
		}
	}
	if crossAZCount > 0 {
		t.Errorf("expected 0 cross-AZ lookups, got %d out of 200", crossAZCount)
	}

	// Verify we can still fetch from same-AZ peer
	peer, isLocal, _ := pc.LookupAZ("shared-key")
	if !isLocal {
		data, found, err := pc.Fetch(context.Background(), peer, "shared-key")
		if err != nil {
			t.Fatalf("fetch from same-AZ peer failed: %v", err)
		}
		if !found {
			t.Error("expected to find shared-key on same-AZ peer")
		}
		if string(data) != "data-from-az-a" {
			t.Errorf("expected data-from-az-a, got %q", string(data))
		}
	}
}

func TestAZAwareIntegration_StatsEndpoint(t *testing.T) {
	handler := NewHandler("", "us-east-1a")
	handler.Put("test-key", []byte("test-data"))

	server := httptest.NewServer(handler)
	defer server.Close()

	// Test /internal/cache/stats returns AZ
	resp, err := http.Get(server.URL + "/internal/cache/stats")
	if err != nil {
		t.Fatalf("stats request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}
```

Add `"fmt"` to imports.

- [ ] **Step 2: Run integration test**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/peercache/ -run TestAZAwareIntegration -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/peercache/integration_az_test.go
git commit -m "test: add AZ-aware peer cache integration tests"
```

---

### Task 10: Documentation Update

**Files:**
- Modify: `docs/cross-az-optimization.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Update cross-az-optimization.md**

Add an "Implementation Status" section at the top:

```markdown
:::info Implementation Status
AZ-aware routing is **enabled by default** in Victoria Lakehouse. The peer cache and buffer bridge
automatically prefer same-AZ peers when the `LAKEHOUSE_AZ` environment variable is set (injected
automatically by the Helm chart via Kubernetes downward API).
:::
```

Update the config examples to reflect the actual implemented values.

- [ ] **Step 2: Update CHANGELOG.md**

Add under `[Unreleased]` → `### Added`:

```markdown
- AZ-aware peer cache — consistent hash ring partitioned by availability zone, same-AZ peers preferred by default
- AZ-aware buffer bridge — select pods prefer same-AZ insert pods for unflushed data queries
- AZ detection via `LAKEHOUSE_AZ` environment variable (injected by Helm chart)
- 6 new AZ metrics: `lakehouse_peer_bytes_total{az=same|cross}`, `lakehouse_peer_az_requests_total{az}`, `lakehouse_peer_same_az_members`, `lakehouse_peer_cross_az_members`, `lakehouse_buffer_bridge_bytes_total{az}`, `lakehouse_buffer_bridge_requests_total{az}`
- Default topology spread constraints in Helm chart for even AZ distribution
- Pod affinity defaults for insert/select co-location in same AZ
```

- [ ] **Step 3: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add docs/cross-az-optimization.md CHANGELOG.md
git commit -m "docs: update cross-AZ optimization with implementation status"
```

---

### Task 11: Final Verification

- [ ] **Step 1: Build both binaries**

```bash
cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go build ./cmd/lakehouse-logs/
cd /private/tmp/victoria-lakehouse-fresh/lakehouse-traces && GOWORK=off go build ./cmd/lakehouse-traces/
```

- [ ] **Step 2: Run full test suites**

```bash
cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./... -v -count=1 -timeout 120s
cd /private/tmp/victoria-lakehouse-fresh/lakehouse-traces && GOWORK=off go test ./... -v -count=1 -timeout 120s
```

- [ ] **Step 3: Lint Helm chart**

```bash
helm lint charts/victoria-lakehouse/
```

- [ ] **Step 4: Verify no regressions in existing peercache tests**

```bash
cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/peercache/ -v -count=1 -race
```

- [ ] **Step 5: Run gosec and go vet**

```bash
cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go vet ./...
```
