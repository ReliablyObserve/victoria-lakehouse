package compaction

import (
	"fmt"
	"hash/crc32"
	"os"
	"strconv"
	"strings"
)

// PartitionSharding assigns partition ownership across compaction shards
// using a deterministic CRC32 hash. Each partition is owned by exactly
// one shard, enabling conflict-free distributed compaction.
type PartitionSharding struct {
	shardID    int
	shardCount int
}

// NewPartitionSharding creates a new PartitionSharding instance.
// shardCount <= 0 is treated as 1 (single-shard mode).
func NewPartitionSharding(shardID, shardCount int) *PartitionSharding {
	if shardCount <= 0 {
		shardCount = 1
	}
	return &PartitionSharding{
		shardID:    shardID,
		shardCount: shardCount,
	}
}

// AutoDetectShardID detects the shard ID from the K8s StatefulSet hostname ordinal.
// K8s StatefulSet pods have hostnames like "lakehouse-logs-0", "lakehouse-logs-1", etc.
// The ordinal is the numeric suffix after the last "-".
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

// OwnsPartition returns true if this shard owns the given partition key.
// Ownership is determined by: crc32(partition) % shardCount == shardID.
func (s *PartitionSharding) OwnsPartition(partition string) bool {
	if s.shardCount <= 1 {
		return true
	}
	h := crc32.ChecksumIEEE([]byte(partition))
	return int(h%uint32(s.shardCount)) == s.shardID
}
