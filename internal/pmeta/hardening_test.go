package pmeta

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
)

// tf is a generic test facet of a configurable kind, encoding a sorted value set
// deterministically. Used to build multi-facet bundles for isolation tests.
type tf struct {
	kind FacetKind
	vals map[string]struct{}
}

func tfFactory(k FacetKind) FacetFactory {
	return func(string) Facet { return &tf{kind: k, vals: map[string]struct{}{}} }
}

func (f *tf) Kind() FacetKind { return f.kind }

func (f *tf) Merge(c FileContribution) {
	for _, v := range c.Labels["v"] {
		f.vals[v] = struct{}{}
	}
}

func (f *tf) sorted() []string {
	out := make([]string, 0, len(f.vals))
	for v := range f.vals {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func (f *tf) Encode(w io.Writer) error {
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

func (f *tf) Decode(r io.Reader) error {
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

func (f *tf) EstimateBytes() int64 { return int64(len(f.vals) * 16) }

func twoFacets() (*Bundle, map[FacetKind]FacetFactory) {
	b := NewBundle("p")
	a := &tf{kind: FacetBloom, vals: map[string]struct{}{}}
	a.Merge(FileContribution{Labels: map[string][]string{"v": {"aaa", "bbb"}}})
	c := &tf{kind: FacetLabels, vals: map[string]struct{}{}}
	c.Merge(FileContribution{Labels: map[string][]string{"v": {"xxx", "yyy", "zzz"}}})
	b.Set(a)
	b.Set(c)
	reg := map[FacetKind]FacetFactory{
		FacetBloom:  tfFactory(FacetBloom),
		FacetLabels: tfFactory(FacetLabels),
	}
	return b, reg
}

// payloadStart is the byte offset where facet payloads begin:
// magic(5) + partLen(2) + partition + facetCount(1) + tocCRC(4) + TOC(count*10).
func payloadStart(part string, facetCount int) int {
	// magic[5] partLen[2] partition headerCRC[4] facetCount[1]... layout v3:
	// magic[5] + partLen[2] + partition + facetCount[1] + headerCRC[4] + tocCRC[4] + TOC
	return 5 + 2 + len(part) + 1 + 4 + 4 + facetCount*tocEntrySize
}

// TestDecode_PayloadCorruptionIsolated is the core safeguard: corrupting one
// facet's payload must SKIP only that facet — the others still load, and the
// stream never desyncs (lengths come from the CRC-protected TOC).
func TestDecode_PayloadCorruptionIsolated(t *testing.T) {
	for _, victim := range []FacetKind{FacetBloom, FacetLabels} {
		b, reg := twoFacets()
		var buf bytes.Buffer
		if err := b.Encode(&buf); err != nil {
			t.Fatal(err)
		}
		raw := buf.Bytes()

		// Corrupt the first byte of the victim's payload. Facets are sorted by
		// kind, so FacetBloom(1) payload precedes FacetLabels(3) payload.
		start := payloadStart("p", 2)
		off := start
		if victim == FacetLabels {
			// bloom payload is 2 values: 2 × (4 + 3) = 14 bytes.
			off = start + 14
		}
		raw[off] ^= 0xFF

		got, res, err := DecodeBundle(bytes.NewReader(raw), reg)
		if err != nil {
			t.Fatalf("victim=%d: must not error on isolated payload corruption: %v", victim, err)
		}
		if len(res.Skipped) != 1 || res.Skipped[0] != victim {
			t.Fatalf("victim=%d: Skipped=%v, want exactly [%d]", victim, res.Skipped, victim)
		}
		// The OTHER facet must have survived — proves no desync.
		other := FacetLabels
		if victim == FacetLabels {
			other = FacetBloom
		}
		if _, ok := got.Get(other); !ok {
			t.Fatalf("victim=%d: sibling facet %d was lost (stream desynced)", victim, other)
		}
		if _, ok := got.Get(victim); ok {
			t.Fatalf("victim=%d: corrupt facet must not be installed", victim)
		}
	}
}

// TestDecode_CorruptTOC_RebuildsWholePartition: a corrupt TOC is caught by the
// TOC CRC and returns a structural error (caller rebuilds the whole partition).
func TestDecode_CorruptTOC_RebuildsWholePartition(t *testing.T) {
	b, reg := twoFacets()
	var buf bytes.Buffer
	if err := b.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	// TOC starts after magic(5)+partLen(2)+"p"(1)+facetCount(1)+tocCRC(4) = 13.
	raw[13+2] ^= 0xFF // flip a len byte inside the TOC
	_, _, err := DecodeBundle(bytes.NewReader(raw), reg)
	if err == nil {
		t.Fatal("corrupt TOC must return a structural error (whole-partition rebuild)")
	}
}

// TestDecode_TruncatedNeverPanics: truncating at every offset must error
// cleanly, never panic, never hang.
func TestDecode_TruncatedNeverPanics(t *testing.T) {
	b, reg := twoFacets()
	var buf bytes.Buffer
	if err := b.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	full := buf.Bytes()
	for i := 0; i < len(full); i++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic decoding truncated bundle at %d: %v", i, r)
				}
			}()
			_, _, _ = DecodeBundle(bytes.NewReader(full[:i]), reg)
		}()
	}
}

func TestDecode_EmptyBundle(t *testing.T) {
	b := NewBundle("p")
	var buf bytes.Buffer
	if err := b.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	got, res, err := DecodeBundle(&buf, map[FacetKind]FacetFactory{})
	if err != nil || len(res.Skipped) != 0 || got.Partition != "p" {
		t.Fatalf("empty bundle round-trip: err=%v skipped=%v part=%q", err, res.Skipped, got.Partition)
	}
}

func TestDecode_GarbageNeverPanics(t *testing.T) {
	reg := map[FacetKind]FacetFactory{FacetBloom: tfFactory(FacetBloom)}
	cases := [][]byte{
		nil, {}, {0}, []byte("LHPM"), []byte("LHPM\x02"),
		[]byte("LHPM\x02\xff\xff"),               // huge partLen, truncated
		append([]byte("LHPM\x02\x00\x00"), 0xff), // 0 partition, facetCount=255, no TOC
		bytes.Repeat([]byte{0xff}, 64),           // random
		append(bundleMagic[:], bytes.Repeat([]byte{0}, 8)...),
	}
	for i, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic on garbage case %d: %v", i, r)
				}
			}()
			_, _, _ = DecodeBundle(bytes.NewReader(c), reg)
		}()
	}
}

// TestStore_ConcurrentFlushEncode runs flush, encode and dirty-scan concurrently
// on the same partition — must be race-free under `go test -race`.
func TestStore_ConcurrentFlushEncode(t *testing.T) {
	s := NewStore()
	s.Register(FacetBloom, tfFactory(FacetBloom))
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				s.OnFileFlush(FileContribution{
					Partition: "p",
					Labels:    map[string][]string{"v": {fmt.Sprintf("%d-%d", g, i)}},
				})
				var buf bytes.Buffer
				_ = s.Bundle("p").Encode(&buf) // concurrent encode while others flush
				_ = s.DirtyPartitions()
			}
		}(g)
	}
	wg.Wait()
}

// FuzzDecodeBundle: arbitrary bytes must never panic, hang, or OOM (the size
// caps bound allocation). Mirrors the repo's decoder-fuzz pattern.
func FuzzDecodeBundle(f *testing.F) {
	b, _ := twoFacets()
	var seed bytes.Buffer
	_ = b.Encode(&seed)
	f.Add(seed.Bytes())
	f.Add([]byte(nil))
	f.Add(bundleMagic[:])
	reg := map[FacetKind]FacetFactory{
		FacetBloom:  tfFactory(FacetBloom),
		FacetLabels: tfFactory(FacetLabels),
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = DecodeBundle(bytes.NewReader(data), reg) // must simply not panic
	})
}

// FuzzRoundTrip: any partition + value set that Encodes must Decode back exactly,
// with zero skipped facets.
func FuzzRoundTrip(f *testing.F) {
	f.Add("p", "a\x00b\x00c")
	f.Add("logs/dt=2026-06-09/hour=10", "api\x00web")
	f.Fuzz(func(t *testing.T, part, joined string) {
		if len(part) > maxPartitionLen {
			t.Skip()
		}
		fc := &tf{kind: FacetBloom, vals: map[string]struct{}{}}
		for _, v := range strings.Split(joined, "\x00") {
			if v != "" {
				fc.vals[v] = struct{}{}
			}
		}
		b := NewBundle(part)
		b.Set(fc)
		var buf bytes.Buffer
		if err := b.Encode(&buf); err != nil {
			t.Skip()
		}
		got, res, err := DecodeBundle(&buf, map[FacetKind]FacetFactory{FacetBloom: tfFactory(FacetBloom)})
		if err != nil || len(res.Skipped) != 0 {
			t.Fatalf("clean round-trip failed: err=%v skipped=%v", err, res.Skipped)
		}
		gf, ok := got.Get(FacetBloom)
		if !ok {
			t.Fatal("facet lost in round-trip")
		}
		if !equal(gf.(*tf).sorted(), fc.sorted()) {
			t.Fatalf("round-trip mismatch: %v vs %v", gf.(*tf).sorted(), fc.sorted())
		}
	})
}
