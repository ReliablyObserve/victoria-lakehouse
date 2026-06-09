package pmeta

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"
	"testing"
)

// kvFacet is a minimal test facet: field "svc" -> sorted value list, encoded
// deterministically so byte-identity (golden) assertions hold.
type kvFacet struct {
	partition string
	vals      map[string]struct{}
}

func newKVFacet(partition string) Facet {
	return &kvFacet{partition: partition, vals: map[string]struct{}{}}
}

func (f *kvFacet) Kind() FacetKind { return FacetFieldCatalog }

func (f *kvFacet) Merge(c FileContribution) {
	for _, v := range c.Labels["svc"] {
		f.vals[v] = struct{}{}
	}
}

func (f *kvFacet) sorted() []string {
	out := make([]string, 0, len(f.vals))
	for v := range f.vals {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func (f *kvFacet) Encode(w io.Writer) error {
	for _, v := range f.sorted() {
		var l [4]byte
		binary.BigEndian.PutUint32(l[:], uint32(len(v)))
		if _, err := w.Write(l[:]); err != nil {
			return err
		}
		if _, err := io.WriteString(w, v); err != nil {
			return err
		}
	}
	return nil
}

func (f *kvFacet) Decode(r io.Reader) error {
	for {
		var l [4]byte
		if _, err := io.ReadFull(r, l[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		buf := make([]byte, binary.BigEndian.Uint32(l[:]))
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}
		f.vals[string(buf)] = struct{}{}
	}
}

func (f *kvFacet) EstimateBytes() int64 { return int64(len(f.vals) * 16) }

func reg() map[FacetKind]FacetFactory {
	return map[FacetKind]FacetFactory{FacetFieldCatalog: newKVFacet}
}

func makeBundle(t *testing.T, part string, vals ...string) *Bundle {
	t.Helper()
	b := NewBundle(part)
	f := newKVFacet(part)
	f.Merge(FileContribution{Labels: map[string][]string{"svc": vals}})
	b.Set(f)
	return b
}

func TestBundle_RoundTrip(t *testing.T) {
	b := makeBundle(t, "logs/dt=2026-06-09/hour=10", "api-gateway", "order-service", "user-service")
	var buf bytes.Buffer
	if err := b.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, res, err := DecodeBundle(&buf, reg())
	if err != nil {
		t.Fatalf("DecodeBundle: %v", err)
	}
	if len(res.Skipped) != 0 {
		t.Fatalf("unexpected skipped facets: %v", res.Skipped)
	}
	if got.Partition != b.Partition {
		t.Fatalf("partition = %q, want %q", got.Partition, b.Partition)
	}
	f, ok := got.Get(FacetFieldCatalog)
	if !ok {
		t.Fatal("field-catalog facet missing after round-trip")
	}
	want := []string{"api-gateway", "order-service", "user-service"}
	if gotv := f.(*kvFacet).sorted(); !equal(gotv, want) {
		t.Fatalf("values = %v, want %v", gotv, want)
	}
	if got.Dirty() {
		t.Fatal("freshly-decoded bundle must not be dirty")
	}
}

func TestBundle_DeterministicEncoding(t *testing.T) {
	// Same logical content must encode byte-identically (golden-test contract).
	a := makeBundle(t, "p", "b", "a", "c")
	b := makeBundle(t, "p", "c", "b", "a")
	var ba, bb bytes.Buffer
	if err := a.Encode(&ba); err != nil {
		t.Fatal(err)
	}
	if err := b.Encode(&bb); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ba.Bytes(), bb.Bytes()) {
		t.Fatal("encoding is not deterministic across insertion order")
	}
}

func TestDecode_SkipsCorruptFacet(t *testing.T) {
	// Flip a byte in the facet payload — CRC must catch it and SKIP the facet
	// (self-heal), not fail the whole bundle.
	b := makeBundle(t, "p", "api-gateway")
	var buf bytes.Buffer
	if err := b.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	raw[len(raw)-1] ^= 0xFF // corrupt last payload byte

	got, res, err := DecodeBundle(bytes.NewReader(raw), reg())
	if err != nil {
		t.Fatalf("DecodeBundle must not error on a corrupt facet: %v", err)
	}
	if len(res.Skipped) != 1 || res.Skipped[0] != FacetFieldCatalog {
		t.Fatalf("Skipped = %v, want [FacetFieldCatalog] (corrupt facet -> rebuild)", res.Skipped)
	}
	if _, ok := got.Get(FacetFieldCatalog); ok {
		t.Fatal("corrupt facet must not be installed")
	}
}

func TestDecode_SkipsUnknownKind(t *testing.T) {
	// A facet whose kind isn't registered is skipped (forward/backward compat).
	b := makeBundle(t, "p", "api-gateway")
	var buf bytes.Buffer
	if err := b.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	emptyReg := map[FacetKind]FacetFactory{} // nothing registered
	_, res, err := DecodeBundle(&buf, emptyReg)
	if err != nil {
		t.Fatalf("DecodeBundle: %v", err)
	}
	if len(res.Skipped) != 1 {
		t.Fatalf("Skipped = %v, want one unknown-kind skip", res.Skipped)
	}
}

func TestDecode_BadMagicErrors(t *testing.T) {
	_, _, err := DecodeBundle(bytes.NewReader([]byte("NOPE!")), reg())
	if err == nil {
		t.Fatal("expected error on bad magic")
	}
}

func TestStore_FlushAndRebuild(t *testing.T) {
	s := NewStore()
	s.Register(FacetFieldCatalog, newKVFacet)

	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "f1", Labels: map[string][]string{"svc": {"api-gateway"}}})
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "f2", Labels: map[string][]string{"svc": {"order-service"}}})

	if dp := s.DirtyPartitions(); len(dp) != 1 || dp[0] != "p" {
		t.Fatalf("DirtyPartitions = %v, want [p]", dp)
	}
	f, ok := s.Get("p", FacetFieldCatalog)
	if !ok {
		t.Fatal("facet missing after flush")
	}
	if got := f.(*kvFacet).sorted(); !equal(got, []string{"api-gateway", "order-service"}) {
		t.Fatalf("merged values = %v", got)
	}

	// Rebuild a fresh store from the same file contributions → identical facet.
	s2 := NewStore()
	s2.Register(FacetFieldCatalog, newKVFacet)
	s2.Rebuild("p", []FileContribution{
		{FileKey: "f1", Labels: map[string][]string{"svc": {"api-gateway"}}},
		{FileKey: "f2", Labels: map[string][]string{"svc": {"order-service"}}},
	})
	f2, _ := s2.Get("p", FacetFieldCatalog)
	if got := f2.(*kvFacet).sorted(); !equal(got, []string{"api-gateway", "order-service"}) {
		t.Fatalf("rebuilt values = %v (self-heal must reproduce the facet)", got)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
