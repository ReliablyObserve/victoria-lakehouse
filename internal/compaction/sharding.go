package compaction

import (
	"hash/crc32"
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

// OwnsPartition returns true if this shard owns the given partition key.
// Ownership is determined by: crc32(partition) % shardCount == shardID.
func (s *PartitionSharding) OwnsPartition(partition string) bool {
	if s.shardCount <= 1 {
		return true
	}
	h := crc32.ChecksumIEEE([]byte(partition))
	return int(h%uint32(s.shardCount)) == s.shardID
}
