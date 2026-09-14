package compaction

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestScheduler_ScanRetriesDeletesOfRetiredObjects: an object this node
// superseded or abandoned whose delete failed stays retired in the manifest,
// and every scan retries the delete until it lands.
func TestScheduler_ScanRetriesDeletesOfRetiredObjects(t *testing.T) {
	const partition = "dt=2026-07-05/hour=01"
	owed := "logs/" + partition + "/merged-source.parquet"
	notOwed := "logs/" + partition + "/removed-by-a-peer.parquet"

	pool := &gatedPool{mockPool: newMockPool()}
	pool.put(owed, []byte("x"))
	pool.put(notOwed, []byte("x"))
	m := manifest.New("test-bucket", "")
	m.Retire(owed, "logs/"+partition+"/compacted.parquet", true)
	m.RemoveFile(partition, notOwed)

	sched := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: pool,
		Ownership: NewOwnershipResolver("self", staticPeers("self")),
		Policy:    NewLevelPolicy(10, 10, 0),
		Prefix:    "logs/", Mode: config.ModeLogs, Interval: time.Minute,
		RowGroupSize: 100, CompressionLevel: 1, MaxConcurrent: 1,
	})

	pool.setFailDelete(func(k string) bool { return k == owed })
	if _, err := sched.Scan(context.Background()); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if pool.get(owed) == nil || !m.IsRetired(owed) {
		t.Fatal("a failed retry leaves the object retired for the next scan")
	}

	pool.setFailDelete(nil)
	if _, err := sched.Scan(context.Background()); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if pool.get(owed) != nil || m.IsRetired(owed) {
		t.Fatal("the scan must delete the owed object and forget it")
	}
	if pool.get(notOwed) == nil {
		t.Fatal("an object removed on another component's behalf is not this node's to delete")
	}

	// A draining scheduler starts no new work, reclaim included.
	m.Retire(owed, "x", true)
	pool.put(owed, []byte("x"))
	sched.Drain()
	if _, err := sched.Scan(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Scan while draining: %v", err)
	}
	if pool.get(owed) == nil {
		t.Fatal("a draining scheduler must not delete anything")
	}
}
