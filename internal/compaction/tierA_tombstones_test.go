package compaction

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// tombstoneMergeWorld: two source files of one partition, each holding one
// "keep" and one "drop" row (service.name), and a tombstone on the drop rows
// in the given delete mode. Works for both signals.
func tombstoneMergeWorld(t *testing.T, mode config.Mode, tsMode string, createdAgo time.Duration) (*planWorld, *delete.TombstoneStore, string, []string) {
	w := newPlanWorld(t, mode)
	w.m = manifest.New("test-bucket", "")
	partition := "dt=2026-07-01/hour=03"
	var keys []string
	for i, tag := range []string{"a", "b"} {
		key := fmt.Sprintf("%s/src-%s.parquet", partition, tag)
		ts := int64(1000 * (i + 1))
		var data []byte
		var err error
		if mode == config.ModeTraces {
			data, err = writeCompactedTraces([]schema.TraceRow{
				{TimestampUnixNano: ts, TraceID: "keep-" + tag, SpanID: "k" + tag, ServiceName: "keep"},
				{TimestampUnixNano: ts + 1, TraceID: "drop-" + tag, SpanID: "d" + tag, ServiceName: "drop"},
			}, 100, 1)
		} else {
			data, err = writeCompactedLogs([]schema.LogRow{
				{TimestampUnixNano: ts, Body: "keep-" + tag, ServiceName: "keep"},
				{TimestampUnixNano: ts + 1, Body: "drop-" + tag, ServiceName: "drop"},
			}, 100, 1)
		}
		if err != nil {
			t.Fatal(err)
		}
		w.pool.put(key, data)
		w.m.AddFile(partition, manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 2, MinTimeNs: ts, MaxTimeNs: ts + 1})
		keys = append(keys, key)
	}
	markListed(t, w.m, keys)
	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		Tenants: []delete.TenantRef{{}}, ID: "ts-tierA", Query: `service.name:="drop"`,
		StartNs: 0, EndNs: 1 << 40, AffectedKeys: keys, CreatedAt: time.Now().Add(-createdAgo),
		Mode: tsMode, Reaped: map[string]bool{},
	})
	return w, store, partition, keys
}

func outputRowIDs(t *testing.T, w *planWorld, key string) []string {
	ids, err := rowIDs(w.mode, w.pool.get(key))
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

// TestMergePaths_ApplyTombstones: the scheduled merge and the Tier A steal
// must both drop the rows of a permanent tombstone and carry the rows of a
// hide-mode one forward, and a permanent tombstone whose rows were filtered
// completes. Before F5 the steal built its Compactor without the tombstone
// store and copied deleted rows into the merged output.
func TestMergePaths_ApplyTombstones(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		for _, path := range []string{"scheduler", "tierA"} {
			// permanent_in_delay: the tombstone is younger than rewrite_delay, the
			// un-delete window, so its rows are carried forward and the tombstone
			// must be transferred to the output that now holds them.
			for _, tsMode := range []string{"permanent", "hide", "permanent_in_delay"} {
				t.Run(path+"/"+tsMode, func(t *testing.T) {
					mode2, created, delay := tsMode, time.Hour, time.Duration(0)
					if tsMode == "permanent_in_delay" {
						mode2, created, delay = "permanent", time.Minute, time.Hour
					}
					w, store, part, _ := tombstoneMergeWorld(t, mode, mode2, created)
					switch path {
					case "scheduler":
						s := NewScheduler(SchedulerConfig{
							Manifest: w.m, Pool: w.pool, Ownership: soleOwnerResolver(), FairShare: NewFairShareScheduler(1),
							Policy: shippedPolicy(), Mode: mode, Interval: time.Minute, MaxConcurrent: 1, RowGroupSize: 100,
							Tombstones: store, TombstoneRewriteDelay: delay, CompactionConfig: config.Default().Compaction,
						})
						if n, err := s.Scan(context.Background()); err != nil || n != 1 {
							t.Fatalf("scan: n=%d err=%v", n, err)
						}
					case "tierA":
						lp := &listingPool{mockPool: w.pool, mtimes: map[string]time.Time{}}
						ranker := func(s string) uint64 {
							if strings.HasPrefix(s, "pod-A") {
								return 100
							}
							return 50
						}
						rB := NewOwnershipResolver("pod-B", staticPeers("pod-A", "pod-B")).WithHashFunc(ranker)
						w.m.MarkAttempt(part, time.Now().Add(-time.Hour))
						sw := NewOrphanSweep(OrphanSweepConfig{
							Manifest: w.m, Pool: lp, Ownership: rB, Policy: shippedPolicy(), Lister: lp,
							Mode: mode, Interval: time.Minute, RowGroupSize: 100, CompressionLevel: 1,
							TierAStalenessMultiplier: 3, Tombstones: store, TombstoneRewriteDelay: delay,
						})
						if n, err := sw.RunTierA(context.Background()); err != nil || n != 1 {
							t.Fatalf("tier A: n=%d err=%v", n, err)
						}
					}
					files := w.m.FilesForPartition(part)
					if len(files) != 1 {
						t.Fatalf("files after merge: %d", len(files))
					}
					ids := outputRowIDs(t, w, files[0].Key)
					dropped := 0
					for _, id := range ids {
						if strings.HasPrefix(id, "drop-") {
							dropped++
						}
					}
					ts, still := store.Get("ts-tierA")
					switch tsMode {
					case "permanent":
						if len(ids) != 2 || dropped != 0 {
							t.Fatalf("%s: output rows %v, want only the 2 kept rows", path, ids)
						}
						if still {
							t.Fatalf("%s: tombstone must complete once its rows are filtered out of a clean output", path)
						}
					case "hide", "permanent_in_delay":
						if len(ids) != 4 || dropped != 2 {
							t.Fatalf("%s: output rows %v, want all 4 (hide mode is reversible)", path, ids)
						}
						if !still {
							t.Fatalf("%s: a tombstone whose rows are carried forward must not complete", path)
						}
						named := false
						for _, k := range ts.AffectedKeys {
							if k == files[0].Key {
								named = true
							}
						}
						if tsMode == "permanent_in_delay" && !named {
							t.Fatalf("%s: tombstone does not name the output that still holds its rows (AffectedKeys=%v)", path, ts.AffectedKeys)
						}
					}
				})
			}
		}
	})
}
