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

// partFailStore fails the PUT of one partition's bundle only.
type partFailStore struct{ bad string }

func (f *partFailStore) GetObject(ctx context.Context, key string) ([]byte, error) {
	return nil, ErrNotFound
}
func (f *partFailStore) PutObject(ctx context.Context, key string, data []byte) error {
	if strings.Contains(key, f.bad) {
		return errors.New("synthetic PUT failure")
	}
	return nil
}

// PersistDirtyAll is the shutdown write: a partition whose PUT fails costs only
// its own bundle, which stays dirty; the others are written.
func TestPersistDirtyAll_OneFailureDoesNotStopTheOthers(t *testing.T) {
	s := newTestStore()
	s.SetPrefix("logs/")
	for _, p := range []string{"pa", "pb", "pc"} {
		s.OnFileFlush(contrib(p, "f", "svc"))
	}
	n, errs, untried := s.PersistDirtyAll(context.Background(), &partFailStore{bad: "pb"})
	if n != 2 || len(errs) != 1 || !strings.Contains(errs[0].Error(), `"pb"`) || len(untried) != 0 {
		t.Fatalf("PersistDirtyAll = (%d, %v, %v), want (2, one error naming pb, none untried)", n, errs, untried)
	}
	if got := s.DirtyPartitions(); len(got) != 1 || got[0] != "pb" {
		t.Fatalf("dirty after the write = %v, want only pb", got)
	}
	if n, errs, untried := s.PersistDirtyAll(context.Background(), &partFailStore{bad: "\x00"}); n != 1 || len(errs) != 0 || len(untried) != 0 {
		t.Fatalf("second pass = (%d, %v, %v), want (1, none, none)", n, errs, untried)
	}
	// Nothing dirty: nothing written, no error.
	if n, errs, untried := s.PersistDirtyAll(context.Background(), &partFailStore{bad: "\x00"}); n != 0 || len(errs) != 0 || len(untried) != 0 {
		t.Fatalf("clean pass = (%d, %v, %v), want nothing", n, errs, untried)
	}
}

// cancelAfterStore cancels the context when the n-th PUT has been made.
type cancelAfterStore struct {
	n      int
	puts   int
	cancel context.CancelFunc
}

func (c *cancelAfterStore) GetObject(context.Context, string) ([]byte, error) {
	return nil, ErrNotFound
}
func (c *cancelAfterStore) PutObject(context.Context, string, []byte) error {
	c.puts++
	if c.puts == c.n {
		c.cancel()
	}
	return nil
}

// When the bound ends the pass, every bundle it did not try is reported by name, so a caller's
// count of what was not written is not short by the partitions after the cut.
func TestPersistDirtyAll_ReportsTheBundlesTheBoundCut(t *testing.T) {
	s := newTestStore()
	s.SetPrefix("logs/")
	for _, p := range []string{"pa", "pb", "pc", "pd"} {
		s.OnFileFlush(contrib(p, "f", "svc"))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n, errs, untried := s.PersistDirtyAll(ctx, &cancelAfterStore{n: 2, cancel: cancel})
	if n != 2 || len(errs) != 0 || len(untried) != 2 || untried[0] != "pc" || untried[1] != "pd" {
		t.Fatalf("PersistDirtyAll = (%d, %v, %v), want (2, none, [pc pd])", n, errs, untried)
	}
	if got := s.DirtyPartitions(); len(got) != 2 {
		t.Fatalf("dirty after the cut = %v, want the two untried", got)
	}
	// An already ended context tries nothing and reports every dirty bundle.
	n, _, untried = s.PersistDirtyAll(ctx, &cancelAfterStore{n: 99, cancel: cancel})
	if n != 0 || len(untried) != 2 {
		t.Fatalf("with an ended context: written %d, untried %v, want 0 and both", n, untried)
	}
}

// midPutStore makes a new contribution to a partition while its bundle is being written.
type midPutStore struct {
	s    *Store
	once bool
}

func (m *midPutStore) GetObject(context.Context, string) ([]byte, error) { return nil, ErrNotFound }
func (m *midPutStore) PutObject(context.Context, string, []byte) error {
	if !m.once {
		m.once = true
		m.s.OnFileFlush(contrib("pa", "arrives-during-the-put", "svc2"))
	}
	return nil
}

// A contribution that arrives while a bundle is being written is not in the bytes written, so the
// partition must stay dirty: the dirty flag is cleared only up to the generation the encode saw.
// Clearing up to the current generation instead loses the update at the next restart.
func TestPersist_ContributionDuringThePutKeepsThePartitionDirty(t *testing.T) {
	for name, persist := range map[string]func(*Store, ObjectStore){
		"PersistDirty": func(s *Store, os ObjectStore) { _, _ = s.PersistDirty(context.Background(), os) },
		"PersistDirtyAll": func(s *Store, os ObjectStore) {
			_, _, _ = s.PersistDirtyAll(context.Background(), os)
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestStore()
			s.SetPrefix("logs/")
			s.OnFileFlush(contrib("pa", "f", "svc"))
			persist(s, &midPutStore{s: s})
			if got := s.DirtyPartitions(); len(got) != 1 || got[0] != "pa" {
				t.Fatalf("dirty after a PUT with a contribution in flight = %v, want [pa]", got)
			}
			persist(s, &failingStore{}) // the next write carries it and clears the flag
			if got := s.DirtyPartitions(); len(got) != 0 {
				t.Fatalf("dirty after the next write = %v, want none", got)
			}
		})
	}
}
