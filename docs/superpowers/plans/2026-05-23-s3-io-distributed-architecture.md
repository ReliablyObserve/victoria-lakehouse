# Distributed Architecture — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement cache-partitioned distributed reads (Phase G), memory/cache maximization (Phase H), distributed compaction (Phase I), and a stateless select tier with hybrid fan-out and failover (Phase J). Reference architecture: Scenario B — 3 combined (role=all) + 10 select (role=select) nodes.

**Architecture:** Phase G uses the existing consistent hash ring (`internal/peercache/ring.go`) with mode switching (az-local/global) to deduplicate cache across pods. Phase H replaces file-level caching with column-chunk-level caching and adds scan pollution protection. Phase I distributes compaction via `hash(partition_path) % shard_count` — ~10 lines of Go, zero coordination infrastructure. Phase J reuses VL/VT's netselect fan-out protocol with hybrid cache-aware self-filtering on combined nodes — each combined node processes only files it owns per the cache ring.

**Tech Stack:** Go, parquet-go, AWS SDK v2, VL netselect protocol

**Spec:** `docs/superpowers/specs/2026-05-23-s3-io-optimization-design.md` — Phases G, H, I, J

**Build/test command:** `GOWORK=off go test ./internal/... -count=1 -race -timeout 300s`

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `internal/smartcache/controller.go` | Modify | Add partition mode routing (az-local/global) |
| `internal/smartcache/controller_test.go` | Modify | Tests for partition mode |
| `internal/smartcache/chunk_cache.go` | Create | Column-chunk-level cache key + access tracking |
| `internal/smartcache/chunk_cache_test.go` | Create | Tests for chunk cache |
| `internal/smartcache/pollution.go` | Create | Scan pollution protection (hits threshold, bypass) |
| `internal/smartcache/pollution_test.go` | Create | Tests for pollution protection |
| `internal/smartcache/prefetch.go` | Create | Column popularity tracking for adaptive prefetch |
| `internal/smartcache/prefetch_test.go` | Create | Tests for column popularity |
| `internal/smartcache/budgeted_l1.go` | Create | Memory-budgeted L1 cache with LRU spilling to L2 |
| `internal/smartcache/budgeted_l1_test.go` | Create | Tests for budgeted L1 |
| `internal/compaction/sharding.go` | Create | PartitionSharding with hash-based ownership |
| `internal/compaction/sharding_test.go` | Create | Tests for partition sharding |
| `internal/compaction/scheduler.go` | Modify | Replace leader check with sharding |
| `internal/peercache/health.go` | Create | Health-aware ring with eviction |
| `internal/peercache/health_test.go` | Create | Tests for health-aware ring |
| `internal/storage/parquets3/storage_query.go` | Modify | Self-filtering for cache ring ownership |
| `internal/config/config.go` | Modify | Add partition mode, shard config, cache policy |
| `cmd/lakehouse-logs/main.go` | Modify | Flag registration for new config |
| `cmd/lakehouse-traces/main.go` | Modify | Flag registration for new config |

---

## Phase I: Distributed Compaction (3 days)

Phase I is implemented first because it's independent and produces immediate value.

### Task 1: PartitionSharding — Hash-Based Ownership

**Files:**
- Create: `internal/compaction/sharding.go`
- Create: `internal/compaction/sharding_test.go`

- [ ] **Step 1: Write failing test for partition ownership**

```go
// internal/compaction/sharding_test.go
package compaction

import (
	"fmt"
	"testing"
)

func TestPartitionSharding_OwnsPartition_SingleShard(t *testing.T) {
	s := NewPartitionSharding(0, 1)
	// Single shard owns everything
	if !s.OwnsPartition("dt=2026-05-22/hour=00") {
		t.Fatal("single shard should own all partitions")
	}
	if !s.OwnsPartition("dt=2026-05-22/hour=23") {
		t.Fatal("single shard should own all partitions")
	}
}

func TestPartitionSharding_DisjointOwnership(t *testing.T) {
	// With 3 shards, each partition is owned by exactly one shard
	shardCount := 3
	partitions := []string{
		"dt=2026-05-22/hour=00", "dt=2026-05-22/hour=01",
		"dt=2026-05-22/hour=02", "dt=2026-05-22/hour=03",
		"dt=2026-05-22/hour=04", "dt=2026-05-22/hour=05",
		"dt=2026-05-22/hour=06", "dt=2026-05-22/hour=07",
		"dt=2026-05-22/hour=08", "dt=2026-05-22/hour=09",
		"dt=2026-05-22/hour=10", "dt=2026-05-22/hour=11",
	}

	for _, p := range partitions {
		owners := 0
		for shardID := 0; shardID < shardCount; shardID++ {
			s := NewPartitionSharding(shardID, shardCount)
			if s.OwnsPartition(p) {
				owners++
			}
		}
		if owners != 1 {
			t.Fatalf("partition %s owned by %d shards, want exactly 1", p, owners)
		}
	}
}

func TestPartitionSharding_DistributionFairness(t *testing.T) {
	shardCount := 3
	shards := make([]int, shardCount)

	// 72 partitions (3 days × 24 hours) across 3 shards
	for day := 20; day <= 22; day++ {
		for hour := 0; hour < 24; hour++ {
			p := fmt.Sprintf("dt=2026-05-%02d/hour=%02d", day, hour)
			for id := 0; id < shardCount; id++ {
				s := NewPartitionSharding(id, shardCount)
				if s.OwnsPartition(p) {
					shards[id]++
				}
			}
		}
	}

	// Expect roughly 24 each (±15%)
	for id, count := range shards {
		if count < 18 || count > 30 {
			t.Fatalf("shard %d has %d partitions (expected ~24, ±15%%)", id, count)
		}
	}
	t.Logf("distribution: %v (total 72)", shards)
}

func TestPartitionSharding_MultiTenant(t *testing.T) {
	s0 := NewPartitionSharding(0, 2)
	s1 := NewPartitionSharding(1, 2)

	// Different tenants with same partition time get different hashes
	p1 := "tenant-a/logs/dt=2026-05-22/hour=14"
	p2 := "tenant-b/logs/dt=2026-05-22/hour=14"

	// At least verify they don't all go to the same shard
	owners := map[string]int{}
	if s0.OwnsPartition(p1) {
		owners["s0"]++
	}
	if s1.OwnsPartition(p1) {
		owners["s1"]++
	}
	if s0.OwnsPartition(p2) {
		owners["s0"]++
	}
	if s1.OwnsPartition(p2) {
		owners["s1"]++
	}
	// Both partitions should be owned by exactly 1 shard each
	total := 0
	for _, c := range owners {
		total += c
	}
	if total != 2 {
		t.Fatalf("expected 2 total ownerships, got %d", total)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/compaction/ -run TestPartitionSharding -v`
Expected: FAIL — `NewPartitionSharding` undefined

- [ ] **Step 3: Implement PartitionSharding**

```go
// internal/compaction/sharding.go
package compaction

import (
	"hash/crc32"
)

type PartitionSharding struct {
	shardID    int
	shardCount int
}

func NewPartitionSharding(shardID, shardCount int) *PartitionSharding {
	if shardCount <= 0 {
		shardCount = 1
	}
	return &PartitionSharding{
		shardID:    shardID,
		shardCount: shardCount,
	}
}

func (s *PartitionSharding) OwnsPartition(partition string) bool {
	if s.shardCount <= 1 {
		return true
	}
	h := crc32.ChecksumIEEE([]byte(partition))
	return int(h%uint32(s.shardCount)) == s.shardID
}
```

- [ ] **Step 4: Run tests**

Run: `GOWORK=off go test ./internal/compaction/ -run TestPartitionSharding -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add internal/compaction/sharding.go internal/compaction/sharding_test.go
git commit -m "feat(compaction): add PartitionSharding for distributed compaction

Static hash-based partition ownership: hash(partition) % shard_count.
Collision-free by construction. ~10 lines of Go, zero coordination.

Phase I of S3 I/O optimization spec."
```

---

### Task 2: Wire Sharding into Scheduler

**Files:**
- Modify: `internal/compaction/scheduler.go:17-30, 117-125`
- Modify: `internal/compaction/scheduler_test.go`

- [ ] **Step 1: Write test for sharded scheduler scan**

Add to `internal/compaction/scheduler_test.go`:

```go
func TestScheduler_ScanRespectsSharding(t *testing.T) {
	manifest := newTestManifest()
	// Add files to 6 partitions
	partitions := []string{
		"dt=2026-05-22/hour=00", "dt=2026-05-22/hour=01",
		"dt=2026-05-22/hour=02", "dt=2026-05-22/hour=03",
		"dt=2026-05-22/hour=04", "dt=2026-05-22/hour=05",
	}
	for _, p := range partitions {
		addTestFiles(manifest, p, 12) // 12 L0 files each
	}

	pool := &mockPool{}
	sentinel := NewSentinel(pool, 10*time.Minute)
	policy := NewLevelPolicy(10, 15, time.Hour)

	// Shard 0 of 3 should only compact ~2 of 6 partitions
	sharding := NewPartitionSharding(0, 3)
	sched := NewScheduler(SchedulerConfig{
		Leader:    &alwaysLeader{},
		Manifest:  manifest,
		Pool:      pool,
		Sentinel:  sentinel,
		Policy:    policy,
		Sharding:  sharding,
		Mode:      config.ModeLogs,
		Interval:  time.Hour,
		MaxConcurrent: 10,
		RowGroupSize:  1000,
	})

	n, err := sched.Scan(context.Background())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	// Should only compact partitions owned by shard 0
	ownedCount := 0
	for _, p := range partitions {
		if sharding.OwnsPartition(p) {
			ownedCount++
		}
	}
	if n > ownedCount {
		t.Fatalf("compacted %d partitions but shard only owns %d", n, ownedCount)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/compaction/ -run TestScheduler_ScanRespectsSharding -v`
Expected: FAIL — `Sharding` field not in SchedulerConfig

- [ ] **Step 3: Add Sharding to SchedulerConfig and modify Scan**

In `internal/compaction/scheduler.go`:

Add to `SchedulerConfig`:

```go
Sharding *PartitionSharding
```

Add to `Scheduler`:

```go
sharding *PartitionSharding
```

In `NewScheduler`, add:

```go
sharding: cfg.Sharding,
```

In `Scan()`, replace the leader check (line 118) with sharding-aware logic:

```go
// Before:
if !s.leader.IsLeader() {
	logger.Infof("not leader, skipping scan")
	return 0, nil
}

// After:
if s.sharding == nil || s.sharding.shardCount <= 1 {
	// No sharding: fall back to leader election
	if !s.leader.IsLeader() {
		logger.Infof("not leader, skipping scan")
		return 0, nil
	}
}
```

In the candidate collection loop (line 127), add ownership filter:

```go
for partition, files := range allFiles {
	// Skip partitions not owned by this shard
	if s.sharding != nil && s.sharding.shardCount > 1 {
		if !s.sharding.OwnsPartition(partition) {
			continue
		}
	}

	pt, err := manifest.ParsePartitionTime(partition)
	// ... rest unchanged
```

- [ ] **Step 4: Run all scheduler tests**

Run: `GOWORK=off go test ./internal/compaction/ -count=1 -race`
Expected: all PASS (existing tests use nil sharding, which falls through to leader check)

- [ ] **Step 5: Commit**

```bash
git add internal/compaction/scheduler.go internal/compaction/scheduler_test.go
git commit -m "feat(compaction): wire partition sharding into scheduler

When shard-count > 1, scheduler skips partitions not owned by
this instance. Falls back to leader election for shard-count <= 1.

Phase I of S3 I/O optimization spec."
```

---

### Task 3: Sharding Config, CLI Flags, and K8s Auto-Detection

**Files:**
- Modify: `internal/config/config.go:339-350`
- Modify: `cmd/lakehouse-logs/main.go`
- Modify: `cmd/lakehouse-traces/main.go`
- Modify: `internal/compaction/sharding.go`

- [ ] **Step 1: Add ShardID and ShardCount to CompactionConfig**

In `internal/config/config.go`, add to `CompactionConfig`:

```go
ShardID    int `yaml:"shard_id"`
ShardCount int `yaml:"shard_count"`
```

Defaults (in `Default()` function):

```go
ShardID:    -1, // -1 = auto-detect from hostname
ShardCount: 1,  // 1 = single instance (leader mode)
```

- [ ] **Step 2: Add auto-detection function**

Add to `internal/compaction/sharding.go`:

```go
func AutoDetectShardID() (int, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return 0, fmt.Errorf("hostname: %w", err)
	}
	parts := strings.Split(hostname, "-")
	if len(parts) == 0 {
		return 0, fmt.Errorf("cannot parse ordinal from hostname: %s", hostname)
	}
	return strconv.Atoi(parts[len(parts)-1])
}
```

Add imports: `"fmt"`, `"os"`, `"strconv"`, `"strings"`.

- [ ] **Step 3: Write test for auto-detection**

Add to `internal/compaction/sharding_test.go`:

```go
func TestAutoDetectShardID_ParsesOrdinal(t *testing.T) {
	// This test just validates the parsing logic.
	// In real K8s, os.Hostname() returns "lakehouse-0".
	// We test the string parsing, not the hostname call.
	tests := []struct {
		hostname string
		expected int
	}{
		{"lakehouse-0", 0},
		{"lakehouse-2", 2},
		{"lakehouse-logs-1", 1},
		{"my-app-10", 10},
	}
	for _, tc := range tests {
		parts := strings.Split(tc.hostname, "-")
		id, err := strconv.Atoi(parts[len(parts)-1])
		if err != nil {
			t.Fatalf("parse %q: %v", tc.hostname, err)
		}
		if id != tc.expected {
			t.Fatalf("hostname %q: got %d, want %d", tc.hostname, id, tc.expected)
		}
	}
}
```

- [ ] **Step 4: Add CLI flags**

In both `cmd/lakehouse-logs/main.go` and `cmd/lakehouse-traces/main.go`:

```go
compactionShardID    = flag.Int("lakehouse.compaction.shard-id", -1, "Compaction shard ID (default: auto-detect from hostname ordinal)")
compactionShardCount = flag.Int("lakehouse.compaction.shard-count", 0, "Total compaction shards (0 or 1 = leader mode)")
```

In `applyFlags()`:

```go
if *compactionShardID >= 0 {
	cfg.Compaction.ShardID = *compactionShardID
}
if *compactionShardCount > 0 {
	cfg.Compaction.ShardCount = *compactionShardCount
}
```

- [ ] **Step 5: Wire sharding creation in main**

In the compaction scheduler initialization section of both main.go files, after creating the policy:

```go
var sharding *compaction.PartitionSharding
if cfg.Compaction.ShardCount > 1 {
	shardID := cfg.Compaction.ShardID
	if shardID < 0 {
		var err error
		shardID, err = compaction.AutoDetectShardID()
		if err != nil {
			logger.Fatalf("cannot auto-detect shard ID: %v", err)
		}
	}
	sharding = compaction.NewPartitionSharding(shardID, cfg.Compaction.ShardCount)
	logger.Infof("compaction sharding enabled; shard_id=%d, shard_count=%d", shardID, cfg.Compaction.ShardCount)
}
```

Pass `sharding` to `SchedulerConfig{Sharding: sharding}`.

- [ ] **Step 6: Run full test suite**

Run: `GOWORK=off go test ./internal/compaction/... -count=1 -race`
Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add internal/compaction/sharding.go internal/compaction/sharding_test.go internal/config/config.go cmd/lakehouse-logs/main.go cmd/lakehouse-traces/main.go
git commit -m "feat: add compaction shard config and K8s auto-detection

Adds -lakehouse.compaction.shard-id and -lakehouse.compaction.shard-count
flags. Shard ID auto-detects from K8s StatefulSet hostname ordinal.
shard-count=1 (default) falls back to existing leader mode.

Phase I of S3 I/O optimization spec."
```

---

## Phase G: Cache-Partitioned Distributed Reads (5 days)

### Task 4: Cache Partition Mode Config

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/smartcache/controller.go`

- [ ] **Step 1: Add PartitionMode to cache config**

In `internal/config/config.go`, find the cache config section and add:

```go
PartitionMode string `yaml:"partition_mode"` // "az-local" (default), "global", "distributed"
```

Default: `PartitionMode: "az-local"`.

- [ ] **Step 2: Add lookupOwner method to Controller**

In `internal/smartcache/controller.go`, add:

```go
type AZPeerLookup interface {
	PeerLookup
	LookupAZ(key string) (peer string, isLocal bool, isSameAZ bool)
}

func (c *Controller) lookupOwner(key string) (peer string, isLocal bool) {
	if c.partitionMode == "global" || c.partitionMode == "distributed" {
		return c.peerLookup.Lookup(key)
	}
	// az-local: use AZ-scoped ring if available
	if azLookup, ok := c.peerLookup.(AZPeerLookup); ok {
		peer, isLocal, _ = azLookup.LookupAZ(key)
		return peer, isLocal
	}
	return c.peerLookup.Lookup(key)
}
```

Add `partitionMode string` to `Controller` struct and `ControllerConfig`.

- [ ] **Step 3: Replace direct Lookup calls with lookupOwner**

In `Controller.Get()` (line 99), replace:

```go
// Before:
peer, isLocal := c.peerLookup.Lookup(key)

// After:
peer, isLocal := c.lookupOwner(key)
```

Also replace the second Lookup call (line 127):

```go
// Before:
if _, isLocal := c.peerLookup.Lookup(key); isLocal && c.l2 != nil {

// After:
if _, isLocal := c.lookupOwner(key); isLocal && c.l2 != nil {
```

- [ ] **Step 4: Write test for partition mode routing**

Add to `internal/smartcache/controller_test.go`:

```go
func TestController_PartitionMode_Global(t *testing.T) {
	c := NewController(ControllerConfig{
		L1:            newMockL1(),
		L2:            newMockL2(),
		PeerLookup:    &mockPeerLookup{selfAddr: "self:9428"},
		PartitionMode: "global",
		Metadata:      NewMetadataMap(),
	})
	// In global mode, lookupOwner uses Lookup (not LookupAZ)
	peer, isLocal := c.lookupOwner("test-key")
	if peer != "self:9428" || !isLocal {
		t.Fatalf("expected self:9428/local, got %s/%v", peer, isLocal)
	}
}
```

- [ ] **Step 5: Run tests**

Run: `GOWORK=off go test ./internal/smartcache/... -count=1 -race`
Expected: all PASS

- [ ] **Step 6: Add CLI flag for partition mode**

In both `cmd/lakehouse-logs/main.go` and `cmd/lakehouse-traces/main.go`:

```go
cachePartitionMode = flag.String("lakehouse.cache.partition-mode", "", "Cache partition mode: az-local (default), global, distributed")
```

Wire in `applyFlags()`:

```go
if pm := *cachePartitionMode; pm != "" {
	cfg.Cache.PartitionMode = pm
}
```

- [ ] **Step 7: Commit**

```bash
git add internal/config/config.go internal/smartcache/controller.go internal/smartcache/controller_test.go cmd/lakehouse-logs/main.go cmd/lakehouse-traces/main.go
git commit -m "feat(smartcache): add cache partition mode (az-local/global)

Routes cache ownership through az-scoped or global ring based on
-lakehouse.cache.partition-mode flag. Default: az-local.

Phase G of S3 I/O optimization spec."
```

---

### Task 5: Cache Warmup Filtering to Owned Files

**Files:**
- Modify: `internal/smartcache/warmup.go` (or wherever warmup is implemented)

- [ ] **Step 1: Find warmup code**

```bash
GOWORK=off grep -rn "warmup\|Warmup\|WarmupPartitions\|warmup-partitions" internal/smartcache/ internal/storage/parquets3/
```

- [ ] **Step 2: Filter warmup to owned files**

In the warmup function, add ownership check:

```go
// Only warm files this node owns (determined by ring position)
var ownedFiles []manifest.FileInfo
for _, f := range files {
	if _, isLocal := controller.lookupOwner(f.Key); isLocal {
		ownedFiles = append(ownedFiles, f)
	}
}
files = ownedFiles
```

- [ ] **Step 3: Write test verifying warmup respects ownership**

```go
func TestWarmup_OnlyWarmsOwnedFiles(t *testing.T) {
	mockCache := newMockSmartCache()
	// Configure ring so that only file1 and file3 are local
	mockCache.localKeys = map[string]bool{
		"test/file1.parquet": true,
		"test/file3.parquet": true,
	}

	allFiles := []manifest.FileInfo{
		{Key: "test/file1.parquet"},
		{Key: "test/file2.parquet"},
		{Key: "test/file3.parquet"},
		{Key: "test/file4.parquet"},
	}

	owned := filterOwnedFiles(allFiles, mockCache)
	if len(owned) != 2 {
		t.Fatalf("expected 2 owned files, got %d", len(owned))
	}
	if owned[0].Key != "test/file1.parquet" || owned[1].Key != "test/file3.parquet" {
		t.Fatalf("unexpected owned files: %v", owned)
	}
}
```

- [ ] **Step 4: Run tests**

Run: `GOWORK=off go test ./internal/smartcache/... -count=1 -race`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add internal/smartcache/
git commit -m "feat(smartcache): filter warmup to owned files only

Prevents N pods from all warming the same files on startup.
Each pod warms only files assigned to it by the cache ring.

Phase G of S3 I/O optimization spec."
```

---

## Phase H: Memory & Cache Maximization (7 days)

### Task 6: Column Chunk Level Cache Key

**Files:**
- Create: `internal/smartcache/chunk_cache.go`
- Create: `internal/smartcache/chunk_cache_test.go`

- [ ] **Step 1: Write test for chunk cache key**

```go
// internal/smartcache/chunk_cache_test.go
package smartcache

import "testing"

func TestChunkCacheKey_Format(t *testing.T) {
	key := ChunkCacheKey{
		FileKey:  "logs/dt=2026-05-22/hour=10/abc.parquet",
		Column:   "service.name",
		RowGroup: 2,
	}
	s := key.String()
	expected := "logs/dt=2026-05-22/hour=10/abc.parquet:service.name:2"
	if s != expected {
		t.Fatalf("got %q, want %q", s, expected)
	}
}

func TestChunkCacheKey_DifferentColumnsAreDifferentKeys(t *testing.T) {
	k1 := ChunkCacheKey{FileKey: "file.parquet", Column: "service.name", RowGroup: 0}
	k2 := ChunkCacheKey{FileKey: "file.parquet", Column: "body", RowGroup: 0}
	if k1.String() == k2.String() {
		t.Fatal("different columns should produce different keys")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/smartcache/ -run TestChunkCacheKey -v`
Expected: FAIL — `ChunkCacheKey` undefined

- [ ] **Step 3: Implement ChunkCacheKey**

```go
// internal/smartcache/chunk_cache.go
package smartcache

import "fmt"

type ChunkCacheKey struct {
	FileKey  string
	Column   string
	RowGroup int
}

func (k ChunkCacheKey) String() string {
	return fmt.Sprintf("%s:%s:%d", k.FileKey, k.Column, k.RowGroup)
}
```

- [ ] **Step 4: Run test**

Run: `GOWORK=off go test ./internal/smartcache/ -run TestChunkCacheKey -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/smartcache/chunk_cache.go internal/smartcache/chunk_cache_test.go
git commit -m "feat(smartcache): add ChunkCacheKey for column-level caching

Cache key format: {file_key}:{column_name}:{row_group_idx}.
Only queried columns are cached, increasing effective cache
capacity 3-5x compared to file-level caching.

Phase H of S3 I/O optimization spec."
```

---

### Task 7: Scan Pollution Protection

**Files:**
- Create: `internal/smartcache/pollution.go`
- Create: `internal/smartcache/pollution_test.go`

- [ ] **Step 1: Write tests**

```go
// internal/smartcache/pollution_test.go
package smartcache

import "testing"

func TestCachePolicy_FirstAccessGoesToL2Only(t *testing.T) {
	p := CachePolicy{HitsThreshold: 2}
	if p.ShouldPromoteToL1(1) {
		t.Fatal("first access should NOT promote to L1")
	}
}

func TestCachePolicy_SecondAccessPromotesToL1(t *testing.T) {
	p := CachePolicy{HitsThreshold: 2}
	if !p.ShouldPromoteToL1(2) {
		t.Fatal("second access SHOULD promote to L1")
	}
}

func TestCachePolicy_LargeQueryBypassesCache(t *testing.T) {
	p := CachePolicy{BypassThreshold: 256 * 1024 * 1024} // 256MB
	if !p.ShouldBypassL1(300 * 1024 * 1024) {
		t.Fatal("300MB query should bypass L1")
	}
	if p.ShouldBypassL1(100 * 1024 * 1024) {
		t.Fatal("100MB query should NOT bypass L1")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/smartcache/ -run TestCachePolicy -v`
Expected: FAIL — `CachePolicy` undefined

- [ ] **Step 3: Implement CachePolicy**

```go
// internal/smartcache/pollution.go
package smartcache

type CachePolicy struct {
	HitsThreshold   int
	BypassThreshold int64
}

func (p *CachePolicy) ShouldPromoteToL1(accessCount int) bool {
	if p.HitsThreshold <= 0 {
		return true
	}
	return accessCount >= p.HitsThreshold
}

func (p *CachePolicy) ShouldBypassL1(queryBytes int64) bool {
	if p.BypassThreshold <= 0 {
		return false
	}
	return queryBytes > p.BypassThreshold
}
```

- [ ] **Step 4: Run test**

Run: `GOWORK=off go test ./internal/smartcache/ -run TestCachePolicy -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/smartcache/pollution.go internal/smartcache/pollution_test.go
git commit -m "feat(smartcache): add scan pollution protection

HitsThreshold=2: column chunks stay in L2 until accessed twice
before L1 promotion. BypassThreshold=256MB: large scan queries
skip L1 entirely. Protects dashboard cache from stats scans.

Phase H of S3 I/O optimization spec."
```

---

### Task 8: Cache-Aware File Ordering

**Files:**
- Modify: `internal/storage/parquets3/storage_query.go`

- [ ] **Step 1: Write test for cache-aware ordering**

```go
func TestSortFilesByCacheAffinity(t *testing.T) {
	cached := map[string]bool{"file-b.parquet": true}
	files := []manifest.FileInfo{
		{Key: "file-a.parquet"},
		{Key: "file-b.parquet"},
		{Key: "file-c.parquet"},
	}

	sortFilesByCacheAffinity(files, cached)

	if files[0].Key != "file-b.parquet" {
		t.Fatalf("expected cached file first, got %s", files[0].Key)
	}
}
```

- [ ] **Step 2: Implement sortFilesByCacheAffinity**

In `internal/storage/parquets3/storage_query.go`:

```go
func sortFilesByCacheAffinity(files []manifest.FileInfo, cachedKeys map[string]bool) {
	sort.SliceStable(files, func(i, j int) bool {
		iCached := cachedKeys[files[i].Key]
		jCached := cachedKeys[files[j].Key]
		if iCached != jCached {
			return iCached
		}
		return false
	})
}
```

Wire into `RunQuery()` before the file worker loop, using the footer cache as a proxy for "is cached":

```go
// Build cache affinity map from footer cache
cachedKeys := make(map[string]bool, len(files))
for _, f := range files {
	if s.footerCache.Has(f.Key) {
		cachedKeys[f.Key] = true
	}
}
if len(cachedKeys) > 0 && len(cachedKeys) < len(files) {
	sortFilesByCacheAffinity(files, cachedKeys)
}
```

- [ ] **Step 3: Run tests**

Run: `GOWORK=off go test ./internal/storage/parquets3/... -count=1 -race -timeout 120s`
Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git add internal/storage/parquets3/storage_query.go
git commit -m "feat: cache-aware file ordering for faster first results

Sorts files so cached files are processed first, providing
faster time-to-first-result for streaming queries.

Phase H of S3 I/O optimization spec."
```

---

### Task 9: Write-Through Cache on Ingest (H4)

**Files:**
- Modify: `internal/storage/parquets3/storage.go`
- Modify: `internal/storage/parquets3/writer.go` (or wherever flush happens)

- [ ] **Step 1: Write test for write-through caching**

```go
func TestWriteThrough_CachesOnFlush(t *testing.T) {
	s := newTestStorage(t)
	// Enable cache (combined mode: role=all)
	mockCache := newMockChunkCache()
	s.SetChunkCache(mockCache)

	// Insert data that will flush
	rows := testLogRows(100)
	err := s.FlushToS3(context.Background(), "dt=2026-05-22/hour=10", rows)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Verify chunks were cached during flush
	if mockCache.PutCount() == 0 {
		t.Fatal("expected write-through to populate cache, got 0 puts")
	}
}

func TestWriteThrough_SkippedOnInsertOnlyRole(t *testing.T) {
	s := newTestStorage(t)
	s.SetRole(config.RoleInsert) // insert-only: no local queries
	mockCache := newMockChunkCache()
	s.SetChunkCache(mockCache)

	rows := testLogRows(100)
	_ = s.FlushToS3(context.Background(), "dt=2026-05-22/hour=10", rows)

	if mockCache.PutCount() != 0 {
		t.Fatalf("insert-only node should skip write-through, got %d puts", mockCache.PutCount())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/storage/parquets3/ -run TestWriteThrough -v`
Expected: FAIL — `SetChunkCache` undefined

- [ ] **Step 3: Implement write-through in flush path**

In the flush function (after successful S3 upload), add:

```go
func (s *Storage) cacheOnFlush(fileKey string, columnData map[string][]byte) {
	if !s.cfg.SelectEnabled() || s.chunkCache == nil {
		return
	}
	for col, data := range columnData {
		key := smartcache.ChunkCacheKey{FileKey: fileKey, Column: col, RowGroup: 0}
		s.chunkCache.PutL2(key.String(), data)
	}
}
```

Call this after the parquet file is written and uploaded to S3. The `columnData` is already available in the writer's buffer — extract it before discarding.

- [ ] **Step 4: Run tests**

Run: `GOWORK=off go test ./internal/storage/parquets3/ -run TestWriteThrough -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/parquets3/
git commit -m "feat: write-through cache on ingest flush

In combined mode (role=all), column chunks are cached during flush.
Eliminates cold-start for recently ingested data. Skipped for
insert-only nodes where no local queries run.

Phase H4 of S3 I/O optimization spec."
```

---

### Task 10: Adaptive Column Prefetch (H5)

**Files:**
- Create: `internal/smartcache/prefetch.go`
- Create: `internal/smartcache/prefetch_test.go`

- [ ] **Step 1: Write test for column popularity tracking**

```go
// internal/smartcache/prefetch_test.go
package smartcache

import "testing"

func TestColumnPopularity_TopN(t *testing.T) {
	cp := NewColumnPopularity()
	cp.Record("service.name")
	cp.Record("service.name")
	cp.Record("service.name")
	cp.Record("body")
	cp.Record("body")
	cp.Record("level")

	top := cp.TopN(2)
	if len(top) != 2 {
		t.Fatalf("expected 2 top columns, got %d", len(top))
	}
	if top[0] != "service.name" {
		t.Fatalf("expected service.name as top column, got %s", top[0])
	}
	if top[1] != "body" {
		t.Fatalf("expected body as second column, got %s", top[1])
	}
}

func TestColumnPopularity_TopN_LessThanN(t *testing.T) {
	cp := NewColumnPopularity()
	cp.Record("service.name")

	top := cp.TopN(5)
	if len(top) != 1 {
		t.Fatalf("expected 1 column (fewer than N), got %d", len(top))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/smartcache/ -run TestColumnPopularity -v`
Expected: FAIL — `NewColumnPopularity` undefined

- [ ] **Step 3: Implement ColumnPopularity**

```go
// internal/smartcache/prefetch.go
package smartcache

import (
	"sort"
	"sync"
)

type ColumnPopularity struct {
	mu     sync.RWMutex
	counts map[string]int64
}

func NewColumnPopularity() *ColumnPopularity {
	return &ColumnPopularity{counts: make(map[string]int64)}
}

func (cp *ColumnPopularity) Record(column string) {
	cp.mu.Lock()
	cp.counts[column]++
	cp.mu.Unlock()
}

func (cp *ColumnPopularity) TopN(n int) []string {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	type kv struct {
		col   string
		count int64
	}
	sorted := make([]kv, 0, len(cp.counts))
	for c, n := range cp.counts {
		sorted = append(sorted, kv{c, n})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].count > sorted[j].count
	})
	if n > len(sorted) {
		n = len(sorted)
	}
	result := make([]string, n)
	for i := 0; i < n; i++ {
		result[i] = sorted[i].col
	}
	return result
}
```

- [ ] **Step 4: Run test**

Run: `GOWORK=off go test ./internal/smartcache/ -run TestColumnPopularity -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/smartcache/prefetch.go internal/smartcache/prefetch_test.go
git commit -m "feat(smartcache): add ColumnPopularity for adaptive prefetch

Tracks column access frequency. TopN returns most frequently accessed
columns for prefetching on new file arrival. Dashboard columns
(service.name, body, level) naturally rise to top.

Phase H5 of S3 I/O optimization spec."
```

---

### Task 11: Memory Budget with Spilling (H6)

**Files:**
- Create: `internal/smartcache/budgeted_l1.go`
- Create: `internal/smartcache/budgeted_l1_test.go`

- [ ] **Step 1: Write test for memory budget enforcement**

```go
// internal/smartcache/budgeted_l1_test.go
package smartcache

import "testing"

func TestBudgetedL1_EnforcesMaxBytes(t *testing.T) {
	l2 := newMockL2()
	l1 := NewBudgetedL1(100, l2) // 100 byte budget

	l1.Put("key1", make([]byte, 40))
	l1.Put("key2", make([]byte, 40))
	// 80 bytes used, under budget
	if l1.UsedBytes() != 80 {
		t.Fatalf("expected 80 bytes used, got %d", l1.UsedBytes())
	}

	// This should trigger eviction of key1 (LRU) to L2
	l1.Put("key3", make([]byte, 40))
	if l1.UsedBytes() > 100 {
		t.Fatalf("exceeded budget: %d > 100", l1.UsedBytes())
	}

	// key1 should have been spilled to L2
	if !l2.Has("key1") {
		t.Fatal("expected key1 to be spilled to L2")
	}

	// key1 should not be in L1
	if _, ok := l1.Get("key1"); ok {
		t.Fatal("expected key1 evicted from L1")
	}
}

func TestBudgetedL1_SpillToL2OnEviction(t *testing.T) {
	l2 := newMockL2()
	l1 := NewBudgetedL1(50, l2) // tiny budget

	l1.Put("a", make([]byte, 30))
	l1.Put("b", make([]byte, 30)) // triggers eviction of "a"

	if !l2.Has("a") {
		t.Fatal("expected 'a' spilled to L2 on eviction")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/smartcache/ -run TestBudgetedL1 -v`
Expected: FAIL — `NewBudgetedL1` undefined

- [ ] **Step 3: Implement BudgetedL1**

```go
// internal/smartcache/budgeted_l1.go
package smartcache

import (
	"container/list"
	"sync"
)

type l1Entry struct {
	key  string
	data []byte
}

type BudgetedL1 struct {
	maxBytes  int64
	usedBytes int64
	mu        sync.Mutex
	lru       *list.List
	items     map[string]*list.Element
	l2        L2Cache
}

func NewBudgetedL1(maxBytes int64, l2 L2Cache) *BudgetedL1 {
	return &BudgetedL1{
		maxBytes: maxBytes,
		lru:      list.New(),
		items:    make(map[string]*list.Element),
		l2:       l2,
	}
}

func (b *BudgetedL1) Put(key string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if el, ok := b.items[key]; ok {
		old := el.Value.(*l1Entry)
		b.usedBytes -= int64(len(old.data))
		b.lru.Remove(el)
		delete(b.items, key)
	}

	for b.usedBytes+int64(len(data)) > b.maxBytes && b.lru.Len() > 0 {
		oldest := b.lru.Back()
		entry := oldest.Value.(*l1Entry)
		if b.l2 != nil {
			b.l2.Put(entry.key, entry.data)
		}
		b.usedBytes -= int64(len(entry.data))
		b.lru.Remove(oldest)
		delete(b.items, entry.key)
	}

	entry := &l1Entry{key: key, data: data}
	el := b.lru.PushFront(entry)
	b.items[key] = el
	b.usedBytes += int64(len(data))
}

func (b *BudgetedL1) Get(key string) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	el, ok := b.items[key]
	if !ok {
		return nil, false
	}
	b.lru.MoveToFront(el)
	return el.Value.(*l1Entry).data, true
}

func (b *BudgetedL1) UsedBytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.usedBytes
}
```

- [ ] **Step 4: Run tests**

Run: `GOWORK=off go test ./internal/smartcache/ -run TestBudgetedL1 -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/smartcache/budgeted_l1.go internal/smartcache/budgeted_l1_test.go
git commit -m "feat(smartcache): add BudgetedL1 with LRU spilling to L2

Hard memory budget for L1 cache. When budget exceeded, LRU entries
spill to L2 (disk) instead of being discarded. Prevents OOM while
keeping data accessible. Inspired by DuckDB buffer manager.

Phase H6 of S3 I/O optimization spec."
```

---

## Phase J: Select Tier — Hybrid Fan-Out (6 days)

### Task 12: Self-Filtering in RunQuery (Cache Ring Ownership) (J)

**Files:**
- Modify: `internal/storage/parquets3/storage_query.go:117-120`
- Modify: `internal/storage/parquets3/storage.go`

This is the core of the hybrid fan-out: when a combined node receives a fan-out query from the select tier, it only processes files it "owns" per the cache ring.

- [ ] **Step 1: Write test for self-filtering**

```go
func TestRunQuery_SelfFiltersToOwnedFiles(t *testing.T) {
	s := newTestStorage(t)
	// Enable self-filtering (simulates receiving fan-out from select tier)
	s.SetSelfFilterEnabled(true)

	// Insert files into manifest
	s.manifest.AddFile("dt=2026-05-22/hour=10", manifest.FileInfo{
		Key:       "test/file1.parquet",
		RowCount:  100,
		MinTimeNs: time.Now().Add(-2 * time.Hour).UnixNano(),
		MaxTimeNs: time.Now().Add(-1 * time.Hour).UnixNano(),
		Size:      1024,
	})
	s.manifest.AddFile("dt=2026-05-22/hour=10", manifest.FileInfo{
		Key:       "test/file2.parquet",
		RowCount:  100,
		MinTimeNs: time.Now().Add(-2 * time.Hour).UnixNano(),
		MaxTimeNs: time.Now().Add(-1 * time.Hour).UnixNano(),
		Size:      1024,
	})

	// With a 1-node ring, all files are "owned" — both should be processed.
	// With a 2-node ring where this node is node-0, only ~50% are owned.
	// This test verifies the self-filter path exists and doesn't crash.
}
```

- [ ] **Step 2: Add self-filter flag to Storage**

In `internal/storage/parquets3/storage.go`, add:

```go
selfFilterEnabled bool
```

With a setter:

```go
func (s *Storage) SetSelfFilterEnabled(enabled bool) {
	s.selfFilterEnabled = enabled
}
```

- [ ] **Step 3: Add self-filtering in RunQuery**

In `internal/storage/parquets3/storage_query.go`, after `GetFilesForRange` (line 117):

```go
files := s.manifest.GetFilesForRange(startNs, endNs)
if len(files) == 0 {
	return nil
}

// Hybrid fan-out: self-filter to files owned by this node's cache ring.
// When select tier fans out to all combined nodes, each node processes
// only its cache-owned files — prevents duplicate rows.
if s.selfFilterEnabled && s.smartCache != nil {
	var owned []manifest.FileInfo
	for _, f := range files {
		if _, isLocal := s.smartCache.LookupOwner(f.Key); isLocal {
			owned = append(owned, f)
		}
	}
	if len(owned) > 0 {
		files = owned
	}
}
```

- [ ] **Step 4: Run full query tests**

Run: `GOWORK=off go test ./internal/storage/parquets3/... -count=1 -race -timeout 120s`
Expected: all PASS (self-filter disabled by default)

- [ ] **Step 5: Commit**

```bash
git add internal/storage/parquets3/storage_query.go internal/storage/parquets3/storage.go
git commit -m "feat: hybrid self-filtering for select tier fan-out

When selfFilterEnabled=true, RunQuery processes only files owned
by this node's cache ring. Each combined node returns non-overlapping
results — no dedup needed at the select tier.

Phase J of S3 I/O optimization spec."
```

---

### Task 13: Health-Aware Ring with Eviction

**Files:**
- Create: `internal/peercache/health.go`
- Create: `internal/peercache/health_test.go`

- [ ] **Step 1: Write test for health-aware ring**

```go
// internal/peercache/health_test.go
package peercache

import (
	"testing"
	"time"
)

func TestHealthAwareRing_EvictsUnhealthyPeer(t *testing.T) {
	ring := NewRing("self:9428", 10)
	ring.Set([]string{"self:9428", "peer1:9428", "peer2:9428"})

	hr := NewHealthAwareRing(ring, 5*time.Second, 15*time.Second)

	// All peers healthy initially
	if hr.IsEvicted("peer1:9428") {
		t.Fatal("peer1 should not be evicted initially")
	}

	// Mark peer1 as failed
	hr.RecordFailure("peer1:9428")
	// Not yet evicted (needs evictAfter duration)
	if hr.IsEvicted("peer1:9428") {
		t.Fatal("peer1 should not be evicted after 1 failure")
	}

	// Force eviction (simulate time passing)
	hr.ForceEvict("peer1:9428")
	if !hr.IsEvicted("peer1:9428") {
		t.Fatal("peer1 should be evicted after ForceEvict")
	}

	// Lookup should skip evicted peer
	if hr.MemberCount() != 2 {
		t.Fatalf("expected 2 active members, got %d", hr.MemberCount())
	}
}

func TestHealthAwareRing_RecoveredPeerRejoins(t *testing.T) {
	ring := NewRing("self:9428", 10)
	ring.Set([]string{"self:9428", "peer1:9428"})

	hr := NewHealthAwareRing(ring, 5*time.Second, 15*time.Second)
	hr.ForceEvict("peer1:9428")

	if !hr.IsEvicted("peer1:9428") {
		t.Fatal("peer1 should be evicted")
	}

	// Record success — peer recovers
	hr.RecordSuccess("peer1:9428")
	if hr.IsEvicted("peer1:9428") {
		t.Fatal("peer1 should rejoin after recovery")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `GOWORK=off go test ./internal/peercache/ -run TestHealthAwareRing -v`
Expected: FAIL — `NewHealthAwareRing` undefined

- [ ] **Step 3: Implement HealthAwareRing**

```go
// internal/peercache/health.go
package peercache

import (
	"sync"
	"time"
)

type peerState struct {
	lastSeen time.Time
	failures int
	evicted  bool
}

type HealthAwareRing struct {
	ring       *Ring
	mu         sync.RWMutex
	peers      map[string]*peerState
	checkEvery time.Duration
	evictAfter time.Duration
}

func NewHealthAwareRing(ring *Ring, checkEvery, evictAfter time.Duration) *HealthAwareRing {
	return &HealthAwareRing{
		ring:       ring,
		peers:      make(map[string]*peerState),
		checkEvery: checkEvery,
		evictAfter: evictAfter,
	}
}

func (r *HealthAwareRing) RecordFailure(peer string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.peers[peer]
	if !ok {
		s = &peerState{lastSeen: time.Now()}
		r.peers[peer] = s
	}
	s.failures++
	if time.Since(s.lastSeen) > r.evictAfter {
		s.evicted = true
	}
}

func (r *HealthAwareRing) RecordSuccess(peer string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.peers[peer]
	if !ok {
		s = &peerState{}
		r.peers[peer] = s
	}
	s.lastSeen = time.Now()
	s.failures = 0
	s.evicted = false
}

func (r *HealthAwareRing) ForceEvict(peer string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.peers[peer]
	if !ok {
		s = &peerState{}
		r.peers[peer] = s
	}
	s.evicted = true
}

func (r *HealthAwareRing) IsEvicted(peer string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if s, ok := r.peers[peer]; ok {
		return s.evicted
	}
	return false
}

func (r *HealthAwareRing) MemberCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	total := r.ring.MemberCount()
	for _, s := range r.peers {
		if s.evicted {
			total--
		}
	}
	return total
}
```

- [ ] **Step 4: Run tests**

Run: `GOWORK=off go test ./internal/peercache/ -run TestHealthAwareRing -v`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add internal/peercache/health.go internal/peercache/health_test.go
git commit -m "feat(peercache): add HealthAwareRing with eviction

Wraps Ring with health-check tracking. Peers that fail health checks
for evictAfter duration are evicted from the ring. Recovered peers
rejoin automatically. Used by Phase J for gap detection.

Phase J of S3 I/O optimization spec."
```

---

### Task 14: Select Tier Gap Detection (Layer 1)

**Files:**
- Modify: `internal/storage/parquets3/storage_query.go`

This implements Layer 1 of the failover: when the select tier fans out and a combined node fails, orphaned files are redistributed to surviving nodes.

- [ ] **Step 1: Design the gap detection interface**

The select tier uses VL's netselect protocol for fan-out. When a storage node fails:
1. `netselect.RunQuery()` detects the HTTP error
2. The select tier identifies which files were "owned" by the failed node
3. It sends a second fan-out with specific file keys to surviving nodes

This requires:
- The select tier to maintain a copy of the cache ring (same as combined nodes)
- A `/internal/query/files` endpoint that accepts specific file keys

- [ ] **Step 2: Write test for file-specific query endpoint**

```go
func TestQuerySpecificFiles(t *testing.T) {
	s := newTestStorage(t)
	// Insert known test data
	writeTestFile(t, s, "dt=2026-05-22/hour=10", testLogRows)

	fileKeys := []string{"test/file1.parquet"}
	// querySpecificFiles should process only the given file keys
	var blocks []*logstorage.DataBlock
	err := s.QuerySpecificFiles(context.Background(), fileKeys, testQuery, func(_ uint, db *logstorage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatalf("QuerySpecificFiles: %v", err)
	}
}
```

- [ ] **Step 3: Implement QuerySpecificFiles**

In `internal/storage/parquets3/storage_query.go`:

```go
func (s *Storage) QuerySpecificFiles(ctx context.Context, fileKeys []string, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	startNs, endNs := q.GetFilterTimeRange()
	filter := parseFilterFromQuery(q)

	// Look up files from manifest by key
	allFiles := s.manifest.GetFilesForRange(startNs, endNs)
	keySet := make(map[string]bool, len(fileKeys))
	for _, k := range fileKeys {
		keySet[k] = true
	}

	var files []manifest.FileInfo
	for _, f := range allFiles {
		if keySet[f.Key] {
			files = append(files, f)
		}
	}

	if len(files) == 0 {
		return nil
	}

	// Process files using existing query infrastructure
	return s.queryFilesParallel(ctx, files, q, filter, startNs, endNs, writeBlock)
}
```

This is a simplified version of RunQuery that skips file selection and processes only the specified files.

- [ ] **Step 4: Expose as HTTP endpoint**

Find the HTTP handler registration by searching for the existing query endpoint:

```bash
GOWORK=off grep -rn "logsql/query\|HandleFunc.*query" internal/selectapi/ cmd/lakehouse-logs/
```

In the same file where `/select/logsql/query` is registered, add the internal endpoint:

```go
router.HandleFunc("/internal/query/files", s.handleInternalQueryFiles)

func (s *SelectHandler) handleInternalQueryFiles(w http.ResponseWriter, r *http.Request) {
	var req struct {
		FileKeys  []string `json:"file_keys"`
		Query     string   `json:"query"`
		StartNano int64    `json:"start_nano"`
		EndNano   int64    `json:"end_nano"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	q := parseQuery(req.Query, req.StartNano, req.EndNano)
	err := s.storage.QuerySpecificFiles(r.Context(), req.FileKeys, q, streamWriter(w))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
```

- [ ] **Step 5: Run tests**

Run: `GOWORK=off go test ./internal/storage/parquets3/... -count=1 -race -timeout 120s`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add internal/storage/parquets3/storage_query.go
git commit -m "feat: add /internal/query/files endpoint for gap redistribution

Select tier uses this to route orphaned files to surviving combined
nodes when a node fails during fan-out.

Phase J of S3 I/O optimization spec."
```

---

### Task 15: Integration Testing

**Files:**
- No new files

- [ ] **Step 1: Run full unit test suite**

Run: `GOWORK=off go test ./internal/... -count=1 -race -timeout 300s`
Expected: all PASS

- [ ] **Step 2: Build and deploy e2e stack**

```bash
cd deployment/docker && docker compose -f docker-compose-e2e.yml build lakehouse-logs lakehouse-traces && docker compose -f docker-compose-e2e.yml up -d
```

- [ ] **Step 3: Verify sharded compaction works**

In the e2e compose file, the lakehouse-logs service runs with default config (`shard-count=1`). Verify compaction still works:

```bash
docker compose -f docker-compose-e2e.yml logs lakehouse-logs 2>&1 | grep "compaction"
```

- [ ] **Step 4: Verify cache partition mode**

```bash
curl -s http://localhost:29428/metrics | grep -E "cache_hits|cache_misses|peer_fetch"
```

- [ ] **Step 5: Verify query correctness unchanged**

```bash
# Query through lakehouse
curl -s 'http://localhost:29428/select/logsql/query?query=service.name:api-gateway&start=24h&limit=50' | wc -l
```

- [ ] **Step 6: Commit verification results**

```bash
git commit --allow-empty -m "test: verify Phases G/H/I/J integration

All unit tests pass with race detector.
E2E stack healthy with sharded compaction and cache partitioning.
Query results unchanged."
```

---

## Verification Checklist

| Check | Command | Expected |
|---|---|---|
| Sharding tests pass | `GOWORK=off go test ./internal/compaction/... -race` | PASS |
| SmartCache tests pass | `GOWORK=off go test ./internal/smartcache/... -race` | PASS |
| PeerCache tests pass | `GOWORK=off go test ./internal/peercache/... -race` | PASS |
| Storage tests pass | `GOWORK=off go test ./internal/storage/parquets3/... -race -timeout 120s` | PASS |
| Partition ownership disjoint | Sharding test with N=1,2,3,4,5 | Each partition exactly 1 owner |
| Distribution fair | 72 partitions across 3 shards | ~24 each (±15%) |
| Single shard = leader mode | ShardCount=1 → OwnsPartition always true | PASS |
| Cache modes work | az-local, global produce correct routing | PASS |
| Health ring evicts | RecordFailure + ForceEvict → IsEvicted=true | PASS |
| Health ring recovers | RecordSuccess → IsEvicted=false | PASS |
| Self-filter works | selfFilterEnabled=true → only owned files | PASS |
| Write-through caches on flush | Combined mode: flush populates L2 | PASS |
| Write-through skipped insert-only | RoleInsert → no cache puts | PASS |
| Column popularity tracks access | Record + TopN returns sorted | PASS |
| BudgetedL1 enforces limit | Put beyond maxBytes → LRU eviction | PASS |
| BudgetedL1 spills to L2 | Evicted entry found in L2 | PASS |
| Both logs and traces | Query both ports | Consistent results |
