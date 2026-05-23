package compaction

import (
	"fmt"
	"testing"
)

func TestPartitionSharding_OwnsPartition_SingleShard(t *testing.T) {
	s := NewPartitionSharding(0, 1)
	if !s.OwnsPartition("dt=2026-05-22/hour=00") {
		t.Fatal("single shard should own all partitions")
	}
	if !s.OwnsPartition("dt=2026-05-22/hour=23") {
		t.Fatal("single shard should own all partitions")
	}
}

func TestPartitionSharding_DisjointOwnership(t *testing.T) {
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

	for id, count := range shards {
		if count < 12 || count > 36 {
			t.Fatalf("shard %d has %d partitions (expected ~24, ±50%%)", id, count)
		}
	}
	t.Logf("distribution: %v (total 72)", shards)
}

func TestPartitionSharding_MultiTenant(t *testing.T) {
	s0 := NewPartitionSharding(0, 2)
	s1 := NewPartitionSharding(1, 2)

	p1 := "tenant-a/logs/dt=2026-05-22/hour=14"
	p2 := "tenant-b/logs/dt=2026-05-22/hour=14"

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

	total := 0
	for _, c := range owners {
		total += c
	}
	if total != 2 {
		t.Fatalf("expected 2 total ownerships, got %d", total)
	}
}
