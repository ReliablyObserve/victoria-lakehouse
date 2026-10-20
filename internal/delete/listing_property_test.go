package delete

import (
	"fmt"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
)

// Whatever a listing returns, the view only ever loses a tombstone on positive
// evidence, and no pod deletes or overwrites anything: a short listing, taken at
// any size, on any pod, at any poll, changes nothing the pod enforces. (The test
// keeps its name from the scheduler it replaced: the property is the same one,
// "an incomplete listing completes and deletes nothing".)
func TestProperty_TombstoneSync_NeverForgetsOverShortListings(t *testing.T) {
	const tombstones = 12
	state := uint64(0x9e3779b97f4a7c15)
	next := func(n int) int {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		return int(state % uint64(n))
	}
	for round := 0; round < 40; round++ {
		b := deletetest.NewBucket()
		writer, _ := newSyncedPod(t, b, "writer")
		for i := 0; i < tombstones; i++ {
			create(t, writer, fmt.Sprintf("d%d", i))
		}
		reader, rpod := newSyncedPod(t, b, "reader", func(o *Options) { o.CacheDir = t.TempDir() })
		for step := 0; step < 6; step++ {
			rpod.ResetCounts()
			if n := next(tombstones + 2); n > 0 {
				rpod.ShortListOnce(n)
			}
			mustSync(t, reader, next(2) == 0)
			if c := rpod.Counts(); c.Deletes != 0 || c.Puts != 0 {
				t.Fatalf("round %d step %d: a short listing made the pod write: %+v", round, step, c)
			}
			for i := 0; i < tombstones; i++ {
				if !has(reader, fmt.Sprintf("d%d", i)) {
					t.Fatalf("round %d step %d: d%d left the view on a short listing", round, step, i)
				}
			}
		}
		for _, op := range b.Log() {
			if op.Kind == "DELETE" {
				t.Fatalf("round %d: %s deleted %s", round, op.Pod, op.Key)
			}
		}
		if got := len(b.Keys(tombPrefix())); got != tombstones {
			t.Fatalf("round %d: the bucket holds %d keys, want %d", round, got, tombstones)
		}
	}
}
