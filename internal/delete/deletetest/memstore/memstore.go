// Package memstore builds tombstone views over an in-memory bucket for the tests
// of packages that use delete.TombstoneStore (the storage adapters, the read
// path, the binaries). It lives apart from deletetest because it imports the
// delete package, whose own tests import deletetest.
package memstore

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
)

// Prefix is the signal prefix the views live under.
const Prefix = "logs/"

// New returns a view that has completed its first sync against a fresh, empty
// bucket: creates, un-deletes and reads work, and nothing else is shared.
func New(t testing.TB) *delete.TombstoneStore {
	t.Helper()
	return Pods(t, deletetest.NewBucket(), "pod")[0]
}

// Pods returns one synced view per name over one shared bucket: several pods of
// one deployment.
func Pods(t testing.TB, b *deletetest.Bucket, names ...string) []*delete.TombstoneStore {
	t.Helper()
	out := make([]*delete.TombstoneStore, 0, len(names))
	for _, name := range names {
		s := delete.NewTombstoneStore()
		pod := b.Pod(name)
		must(t, s.Attach(delete.Options{Backend: pod, Prefix: Prefix, Interval: time.Minute, Now: pod.Now}))
		must(t, s.SyncNow(context.Background(), true))
		out = append(out, s)
	}
	return out
}

// Create stores a tombstone through the view and fails the test on an error.
func Create(t testing.TB, s *delete.TombstoneStore, ts delete.Tombstone) delete.Tombstone {
	t.Helper()
	got, err := s.Create(context.Background(), ts)
	if err != nil {
		t.Fatalf("create %s: %v", ts.ID, err)
	}
	return got
}

// must fails the test on an error.
func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
