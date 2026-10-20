package memstore

import (
	"context"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
)

func TestMemstore_PodsShareOneBucket(t *testing.T) {
	b := deletetest.NewBucket()
	pods := Pods(t, b, "a", "b")
	if len(pods) != 2 {
		t.Fatalf("%d pods", len(pods))
	}
	Create(t, pods[0], delete.Tombstone{ID: "x", Query: "error", StartNs: 0, EndNs: 1 << 40, Mode: "hide", Tenants: []delete.TenantRef{{AccountID: 1}}})
	if _, ok := pods[0].Get("x"); !ok {
		t.Fatal("the creating pod does not enforce its delete")
	}
	if len(b.Keys(Prefix)) == 0 {
		t.Fatal("the delete was not written to the shared bucket")
	}
}

func TestMemstore_NewIsSynced(t *testing.T) {
	s := New(t)
	if s.Count() != 0 {
		t.Fatal("a fresh view is not empty")
	}
	if _, ok := s.ViewAge(); !ok {
		t.Fatal("New returned a view that has not completed its first sync")
	}
}

// failingTB records a Fatalf instead of ending the test.
type failingTB struct {
	testing.TB
	failed bool
}

func (f *failingTB) Helper() {}
func (f *failingTB) Fatalf(string, ...any) {
	f.failed = true
	panic(f)
}
func (f *failingTB) Fatal(...any) {
	f.failed = true
	panic(f)
}

func TestMemstore_CreateFailsTheTestOnAnError(t *testing.T) {
	s := New(t)
	f := &failingTB{TB: t}
	func() {
		defer func() { _ = recover() }()
		Create(f, s, delete.Tombstone{ID: "bad"}) // names no tenant: refused
	}()
	if !f.failed {
		t.Fatal("Create did not fail the test for a refused tombstone")
	}
}

func TestMemstore_MustFailsOnAnError(t *testing.T) {
	f := &failingTB{TB: t}
	func() {
		defer func() { _ = recover() }()
		must(f, context.Canceled)
	}()
	if !f.failed {
		t.Fatal("must did not fail the test")
	}
	must(t, nil)
}
