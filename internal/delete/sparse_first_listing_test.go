package delete

import (
	"fmt"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
)

// A pod restarting from its disk copy whose first listing is sparse (it lists
// one key of many, claiming to be complete) keeps enforcing every delete it
// knew: nothing it holds is forgotten without a HEAD that answers 404, so
// deleted rows do not come back. (The name is that of the regression it keeps.)
func TestTombstoneSync_SparseFirstListingAfterRestartKeepsDeletedRowsHidden(t *testing.T) {
	dir := t.TempDir()
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	const n = 8
	for i := 0; i < n; i++ {
		create(t, writer, fmt.Sprintf("d%d", i))
	}
	old, _ := newSyncedPod(t, b, "reader", func(o *Options) { o.CacheDir = dir })
	if old.Count() != n {
		t.Fatalf("fixture: the first pod holds %d, want %d", old.Count(), n)
	}

	restarted, pod := newPod(t, b, "reader", func(o *Options) { o.CacheDir = dir })
	pod.ShortListOnce(1) // a sparse first listing
	mustSync(t, restarted, true)
	for i := 0; i < n; i++ {
		if !has(restarted, fmt.Sprintf("d%d", i)) {
			t.Fatalf("d%d is no longer enforced after a sparse first listing: its rows would come back", i)
		}
	}
	if c := pod.Counts(); c.Puts != 0 || c.Deletes != 0 {
		t.Fatalf("the restarting pod wrote to the bucket: %+v", c)
	}
	// The next complete listing settles the view with no change.
	mustSync(t, restarted, true)
	if restarted.Count() != n {
		t.Fatalf("after a complete listing the view holds %d, want %d", restarted.Count(), n)
	}
}
