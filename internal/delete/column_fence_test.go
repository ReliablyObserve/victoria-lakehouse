package delete

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
)

// The forward fence (a delete rewrite must not drop a column the running code
// does not model) held while deletes rewrote objects. They no longer can, which
// is the fence's strongest form: this is its remaining assertion, kept under the
// fence's file and rows (lh.delete.fence_unknown_columns_*). Compaction, the only
// rewriter left, keeps its own fence tests in internal/compaction.
// A delete only ever writes under the tombstone prefix: whatever the modes, the
// un-deletes, the stop_task calls, the syncs and the retention GC do, no data
// object of the bucket is created, overwritten or deleted. (The delete used to
// rewrite objects; it no longer can.)
func TestTombstones_NeverWriteOutsideTheTombstonePrefix(t *testing.T) {
	b := deletetest.NewBucket()
	objects := map[string][]byte{
		"logs/dt=2026-05-02/hour=10/a.parquet":        []byte("PAR1 a PAR1"),
		"logs/dt=2026-05-02/hour=11/b.parquet":        []byte("PAR1 b PAR1"),
		"1001/0/logs/dt=2026-05-02/hour=10/c.parquet": []byte("PAR1 c PAR1"),
	}
	for k, v := range objects {
		b.Put(k, v)
	}
	logBefore := len(b.Log())
	w, _ := newSyncedPod(t, b, "writer", withRetention, maintenance)
	r, _ := newSyncedPod(t, b, "reader", withRetention)
	for i, mode := range []string{"hide", "permanent", "auto", "hide"} {
		ts := hideTombstone(fmt.Sprintf("d%d", i))
		ts.Mode = mode
		ts.EndNs = b.Now().UnixNano()
		mustCreate(t, w, ts)
	}
	_ = w.Undelete(context.Background(), "d0")
	_ = w.Undelete(context.Background(), "d1") // refused: irreversible
	for _, id := range []string{"d0", "d1", "nope"} {
		_ = StopTask(context.Background(), w, id)
	}
	mustSync(t, r, true)
	b.Advance(testMaxTTL + 2*retentionMargin) // everything expires: the GC runs
	mustSync(t, w, true)
	mustSync(t, r, true)

	if len(deletes(b)) == 0 {
		t.Fatal("fixture: the GC should have collected the expired tombstones")
	}
	for _, op := range b.Log()[logBefore:] {
		if (op.Kind == "PUT" || op.Kind == "DELETE") && !strings.Contains(op.Key, "_tombstones/") {
			t.Fatalf("%s did %s on %s, outside the tombstone prefix", op.Pod, op.Kind, op.Key)
		}
	}
	for k, v := range objects {
		got, ok := b.Body(k)
		if !ok || !bytes.Equal(got, v) {
			t.Fatalf("data object %s changed", k)
		}
	}
}
