package pmeta

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func newTestStore() *Store {
	s := NewStore()
	d := NewDict()
	s.SetDict(d)
	s.Register(FacetFieldCatalog, NewFieldCatalogFactory(d))
	s.Register(FacetFileMeta, NewFileMetaFactory())
	s.Register(FacetBloom, NewBloomFactory(0)) // 0 → default fpRate branch
	return s
}

func contrib(part, key, svc string) FileContribution {
	return FileContribution{
		Partition: part, FileKey: key, RowCount: 1,
		Labels:      map[string][]string{"service.name": {svc}},
		BloomValues: map[string][]string{"service.name": {svc}},
	}
}

// TestStoreSurface_KeysDictPartitionsReplay covers the small Store surface the
// wiring relies on: BundleKey format, SetDict-inclusive ResidentBytes,
// Partitions listing, Put, and the OnFileReplay quiet-dirty contract.
func TestStoreSurface_KeysDictPartitionsReplay(t *testing.T) {
	s := newTestStore()
	s.SetPrefix("logs/")
	if k := s.BundleKey("dt=2026-06-09/hour=01"); k != "logs/dt=2026-06-09/hour=01/_pmeta.bundle" {
		t.Fatalf("BundleKey = %q", k)
	}

	// Replay is QUIET: content lands, nothing is dirty (manifest-derived state
	// is already durable — dirty replays caused a full-fleet PUT storm).
	s.OnFileReplay(contrib("p1", "f1", "svc-a"))
	if got := s.DirtyPartitions(); len(got) != 0 {
		t.Fatalf("replay must not dirty bundles, got %v", got)
	}
	if v := s.FieldValues("p1", "service.name", "", 0); len(v) != 1 || v[0] != "svc-a" {
		t.Fatalf("replayed content missing: %v", v)
	}
	// Flush IS dirty.
	s.OnFileFlush(contrib("p2", "f2", "svc-b"))
	if got := s.DirtyPartitions(); len(got) != 1 || got[0] != "p2" {
		t.Fatalf("DirtyPartitions = %v, want [p2]", got)
	}

	parts := s.Partitions()
	if len(parts) != 2 {
		t.Fatalf("Partitions = %v", parts)
	}

	// ResidentBytes includes the dict (interned strings counted once globally).
	if s.ResidentBytes() <= 0 {
		t.Fatal("ResidentBytes should count bundles + dict")
	}

	// Put installs a decoded bundle wholesale (empty-store path).
	fresh := newTestStore()
	var buf bytes.Buffer
	if err := s.Bundle("p1").Encode(&buf); err != nil {
		t.Fatal(err)
	}
	dec, _, err := DecodeBundle(bytes.NewReader(buf.Bytes()), fresh.Registry())
	if err != nil {
		t.Fatal(err)
	}
	fresh.Put(dec)
	if v := fresh.FieldValues("p1", "service.name", "", 0); len(v) != 1 {
		t.Fatalf("Put-installed bundle not readable: %v", v)
	}
}

// TestCatalogAbsorb_HighCardAndValues covers the catalog warm-merge absorb: the
// other side's high-card status is unioned (once not-enumerable, always), and
// its plain values merge.
func TestCatalogAbsorb_HighCardAndValues(t *testing.T) {
	d := NewDict()
	mk := func() *fieldCatalogFacet {
		return NewFieldCatalogFactoryCapped(d, 0, nil)("p").(*fieldCatalogFacet)
	}
	live, other := mk(), mk()
	live.Merge(FileContribution{Labels: map[string][]string{"env": {"prod"}}})
	other.Merge(FileContribution{Labels: map[string][]string{"env": {"dev"}, "pod": {"a", "b"}}})
	other.Merge(FileContribution{TruncatedFields: []string{"pod"}}) // pod → high-card on other

	live.absorbFacet(other)
	if got := live.Values("env", "", 0); len(got) != 2 {
		t.Fatalf("env union = %v, want [dev prod]", got)
	}
	if !live.IsHighCard("pod") {
		t.Fatal("absorb must union high-card status")
	}
	// Wrong-type absorb is a no-op, not a panic.
	live.absorbFacet(NewFileMetaFactory()("p"))
	bf := NewBloomFactory(0.01)("p").(*bloomFacet)
	bf.absorbFacet(NewFileMetaFactory()("p")) // bloom wrong-type branch
}

// failingStore fails PUTs — covers the PersistDirty error path: the bundle must
// STAY dirty so the next cycle retries (the PUT-failure-stays-dirty contract).
type failingStore struct{ fail bool }

func (f *failingStore) GetObject(ctx context.Context, key string) ([]byte, error) {
	return nil, ErrNotFound
}
func (f *failingStore) PutObject(ctx context.Context, key string, data []byte) error {
	if f.fail {
		return errors.New("synthetic PUT failure")
	}
	return nil
}

func TestPersistDirty_PutFailureStaysDirty(t *testing.T) {
	s := newTestStore()
	s.SetPrefix("logs/")
	s.OnFileFlush(contrib("p", "f", "svc"))

	os := &failingStore{fail: true}
	n, err := s.PersistDirty(context.Background(), os)
	if err == nil || n != 0 {
		t.Fatalf("PersistDirty = (%d, %v), want (0, error)", n, err)
	}
	if got := s.DirtyPartitions(); len(got) != 1 {
		t.Fatal("bundle must stay dirty after a failed PUT (retry next cycle)")
	}
	// Recovery: the next cycle persists and clears.
	os.fail = false
	if n, err := s.PersistDirty(context.Background(), os); err != nil || n != 1 {
		t.Fatalf("retry = (%d, %v), want (1, nil)", n, err)
	}
	if got := s.DirtyPartitions(); len(got) != 0 {
		t.Fatalf("still dirty after successful persist: %v", got)
	}
	// Cancelled context stops early with the count so far.
	s.OnFileFlush(contrib("p", "f2", "svc2"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.PersistDirty(ctx, os); err == nil {
		t.Fatal("cancelled ctx must error")
	}
}

// failWriter errors after n bytes — covers the Encode error branches.
type failWriter struct{ left int }

func (w *failWriter) Write(p []byte) (int, error) {
	if w.left -= len(p); w.left < 0 {
		return 0, errors.New("synthetic write failure")
	}
	return len(p), nil
}

func TestEncode_WriterErrorsPropagate(t *testing.T) {
	s := newTestStore()
	s.OnFileFlush(contrib("p", "f", strings.Repeat("v", 64)))
	b := s.Bundle("p")
	var full bytes.Buffer
	if err := b.Encode(&full); err != nil {
		t.Fatal(err)
	}
	// Fail at every prefix length: every write site's error branch fires at
	// least once; Encode must return an error, never panic.
	for n := 0; n < full.Len(); n += 7 {
		if err := b.Encode(&failWriter{left: n}); err == nil {
			t.Fatalf("Encode with writer failing at %d bytes must error", n)
		}
	}
}
