package pmeta

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/parquet-go/parquet-go/bloom"
)

// summaryFor builds a summary of one file of n ids g<k>-<i> sorted, in groups of per rows,
// with a real split-block filter per group.
func summaryFor(file string, n, per int) (*IDFile, []string) {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("%s-%06d", file, i)
	}
	s := &IDFile{Rows: int64(n)}
	for lo := 0; lo < n; lo += per {
		hi := min(lo+per, n)
		f := bloom.MakeSplitBlockFilter(make([]byte, 256))
		for _, id := range ids[lo:hi] {
			f.Insert(bloom.XXH64{}.Sum64([]byte(id)))
		}
		s.Groups = append(s.Groups, IDGroup{Rows: uint32(hi - lo), MinID: ids[lo], MaxID: ids[hi-1], MinTime: int64(lo), MaxTime: int64(hi), Filter: f.Bytes()})
	}
	return s, ids
}

func idStore() *Store {
	s := NewStore()
	s.Register(FacetIDSummary, NewIDSummaryFactory())
	return s
}

// A file whose summary has no row group that can hold the id is left out; one that has, and
// one without a summary, are kept (no summary is never a reason to skip a file).
func TestIDSummary_MayContain(t *testing.T) {
	s := idStore()
	a, aIDs := summaryFor("a", 300, 100)
	b, bIDs := summaryFor("b", 300, 100)
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "a", IDSummary: a})
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "b", IDSummary: b})
	keys := []string{"a", "b", "unknown"}
	got, ok := s.IDSummaryMayContain("p", keys, aIDs[150])
	if !ok || fmt.Sprint(got) != "[a unknown]" {
		t.Errorf("an id of a: %v %v, want [a unknown]", got, ok)
	}
	got, _ = s.IDSummaryMayContain("p", keys, bIDs[299])
	if fmt.Sprint(got) != "[b unknown]" {
		t.Errorf("an id of b: %v", got)
	}
	// outside every fence
	got, _ = s.IDSummaryMayContain("p", keys, "zzz")
	if fmt.Sprint(got) != "[unknown]" {
		t.Errorf("an id beyond the fences: %v", got)
	}
	// inside a's fence, in none of its groups: only the filter can say no (about 1 % of the time)
	miss := 0
	for i := 0; i < 2000; i++ {
		id := fmt.Sprintf("a-%06dx", i%300) // sorts between two ids of a
		if k, _ := s.IDSummaryMayContain("p", []string{"a"}, id); len(k) == 0 {
			miss++
		}
	}
	if miss < 1900 {
		t.Errorf("the filters ruled out only %d of 2000 absent ids", miss)
	}
	if _, ok := s.IDSummaryMayContain("other", keys, "x"); ok {
		t.Error("a partition without a facet answered")
	}
	if files, groups, bytes := s.IDSummaryStats("p"); files != 2 || groups != 6 || bytes < 2*3*256 {
		t.Errorf("stats %d files %d groups %d bytes", files, groups, bytes)
	}
}

func TestIDSummary_EncodeDecodeRoundTripAndTheBundle(t *testing.T) {
	s := idStore()
	a, aIDs := summaryFor("a", 250, 100)
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "a", IDSummary: a})
	b := s.Bundle("p")
	var buf bytes.Buffer
	if err := b.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	// determinism: a second encoding of the same state is the same bytes (the bundle CRC)
	var buf2 bytes.Buffer
	if err := b.Encode(&buf2); err != nil || !bytes.Equal(buf.Bytes(), buf2.Bytes()) {
		t.Fatal("two encodings differ")
	}
	got, res, err := DecodeBundle(bytes.NewReader(buf.Bytes()), s.Registry())
	if err != nil || len(res.Skipped) != 0 {
		t.Fatalf("decode: %v skipped %v", err, res.Skipped)
	}
	s2 := idStore()
	s2.Put(got)
	for _, id := range []string{aIDs[0], aIDs[120], aIDs[249]} {
		if k, ok := s2.IDSummaryMayContain("p", []string{"a"}, id); !ok || len(k) != 1 {
			t.Errorf("after the round trip %s is not found: %v %v", id, k, ok)
		}
	}
	if k, _ := s2.IDSummaryMayContain("p", []string{"a"}, "b-nothing"); len(k) != 0 {
		t.Errorf("an id beyond the fences passed: %v", k)
	}
}

func TestIDSummary_RemoveAbsorbAndEmpty(t *testing.T) {
	s := idStore()
	a, _ := summaryFor("a", 100, 100)
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "a", IDSummary: a})
	// a contribution without a summary adds nothing
	s.OnFileFlush(FileContribution{Partition: "p", FileKey: "none"})
	f, _ := s.Get("p", FacetIDSummary)
	if n := len(f.(*idSummaryFacet).byKey); n != 1 {
		t.Fatalf("%d entries, want 1", n)
	}
	// warm-merge: the decoded content joins the live one, the live entry wins a clash
	other := NewIDSummaryFactory()("p").(*idSummaryFacet)
	c, _ := summaryFor("c", 50, 50)
	other.Merge(FileContribution{FileKey: "c", IDSummary: c})
	other.Merge(FileContribution{FileKey: "a", IDSummary: &IDFile{Rows: 1, Groups: []IDGroup{{Rows: 1, MinID: "q", MaxID: "q"}}}})
	f.(*idSummaryFacet).absorbFacet(other)
	if len(f.(*idSummaryFacet).byKey) != 2 || f.(*idSummaryFacet).byKey["a"].Rows != 100 {
		t.Errorf("absorb: %+v", f.(*idSummaryFacet).byKey)
	}
	s.RemoveFiles("p", []string{"a", "c"})
	if _, ok := s.IDSummaryMayContain("p", []string{"a"}, "a-000001"); ok {
		t.Error("an empty facet must not answer (the caller falls back to the files)")
	}
	// a group with neither fences nor filter can hold anything
	g := IDGroup{Rows: 3}
	if !g.mayContain("whatever") {
		t.Error("a group without fences or filter must keep the file")
	}
}

// Whatever bytes a bundle holds, decoding never panics, never allocates beyond the
// bytes it was given, and a payload it accepts re-encodes to itself.
func FuzzIDSummaryDecode(f *testing.F) {
	s, _ := summaryFor("seed", 120, 50)
	facet := NewIDSummaryFactory()("p").(*idSummaryFacet)
	facet.Merge(FileContribution{FileKey: "k", IDSummary: s})
	var seed bytes.Buffer
	_ = facet.Encode(&seed)
	f.Add(seed.Bytes())
	f.Add([]byte(nil))
	f.Add([]byte{1})
	f.Add([]byte{1, 0xff, 0xff, 0xff, 0xff, 0x0f})
	f.Add([]byte{1, 1, 1, 'k', 0, 1, 1, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0x0f})
	f.Fuzz(func(t *testing.T, data []byte) {
		files, err := decodeIDSummary(data)
		if err != nil {
			return
		}
		again := NewIDSummaryFactory()("p").(*idSummaryFacet)
		again.byKey = files
		var out bytes.Buffer
		if err := again.Encode(&out); err != nil {
			t.Fatal(err)
		}
		files2, err := decodeIDSummary(out.Bytes())
		if err != nil || len(files2) != len(files) {
			t.Fatalf("an accepted payload does not survive a round trip: %v", err)
		}
	})
}

func TestIDSummary_DecodeRefusesLies(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	s, _ := summaryFor("x", 200, 100)
	facet := NewIDSummaryFactory()("p").(*idSummaryFacet)
	facet.Merge(FileContribution{FileKey: "k", IDSummary: s})
	var buf bytes.Buffer
	_ = facet.Encode(&buf)
	good := buf.Bytes()
	if _, err := decodeIDSummary(good); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		b := append([]byte(nil), good...)
		b[rng.Intn(len(b))] ^= byte(1 + rng.Intn(255))
		_, _ = decodeIDSummary(b) // may or may not be accepted; must not panic
	}
	for n := 0; n < len(good); n += 7 {
		if _, err := decodeIDSummary(good[:n]); err == nil && n != len(good) {
			t.Errorf("a truncated payload of %d bytes was accepted", n)
		}
	}
}
