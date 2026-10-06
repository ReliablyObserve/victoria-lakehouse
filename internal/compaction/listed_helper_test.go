package compaction

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// listCompletely applies a complete bucket listing that matches the manifest's
// current files, as a healthy refresh would: destructive decisions that read
// "absent from the manifest" as "gone" (#418) need one before they act.
func listCompletely(t testing.TB, m *manifest.Manifest) {
	t.Helper()
	var objs []manifest.ListedObject
	for _, files := range m.AllFiles() {
		for _, fi := range files {
			objs = append(objs, manifest.ListedObject{Key: fi.Key, Size: fi.Size})
		}
	}
	if !m.ApplyListing(objs, time.Now()) {
		t.Fatal("fixture: the listing was rejected")
	}
}

// retireAlways lets reconcileTombstones complete every tombstone it touches, for
// tests about the bookkeeping rather than the retirement gate.
func retireAlways(delete.Tombstone) bool { return true }
