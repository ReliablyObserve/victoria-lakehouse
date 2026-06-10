package pmeta

import (
	"bytes"
	"sort"
	"testing"
)

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// sampleContribs is a small but realistic set of per-file label contributions.
func sampleContribs() []FileContribution {
	return []FileContribution{
		{FileKey: "f1", Labels: map[string][]string{
			"service.name": {"api-gateway", "order-service"},
			"level":        {"ERROR", "INFO"},
			"environment":  {"prod"},
		}},
		{FileKey: "f2", Labels: map[string][]string{
			"service.name": {"api-gateway", "user-service"},
			"level":        {"WARN"},
			"environment":  {"prod", "staging"},
		}},
		{FileKey: "f3", Labels: map[string][]string{
			"service.name": {"payment-service"},
			"level":        {"ERROR"},
		}},
	}
}

// groundTruth computes the exact distinct values per field directly from the
// contributions — the "reality" the catalog must reproduce.
func groundTruth(cs []FileContribution) map[string]map[string]bool {
	gt := map[string]map[string]bool{}
	for _, c := range cs {
		for fld, vals := range c.Labels {
			if gt[fld] == nil {
				gt[fld] = map[string]bool{}
			}
			for _, v := range vals {
				gt[fld][v] = true
			}
		}
	}
	return gt
}

func buildCatalog(cs []FileContribution) (*Dict, *fieldCatalogFacet) {
	d := NewDict()
	f := NewFieldCatalogFactory(d)("p").(*fieldCatalogFacet)
	for _, c := range cs {
		f.Merge(c)
	}
	return d, f
}

// TestFieldCatalog_ParityWithGroundTruth: the catalog's values + field names
// must EXACTLY match the distinct set present in the data — no missing values,
// no extras. This is the core dropdown-parity guarantee.
// TestFieldCatalog_CardinalityCap: a field over the threshold (or in alwaysSketch)
// becomes high-card — values stop being stored (RAM bound) and aren't enumerable,
// but the field name is still listed.
func TestFieldCatalog_CardinalityCap(t *testing.T) {
	d := NewDict()
	f := NewFieldCatalogFactoryCapped(d, 3, []string{"trace_id"})("p").(*fieldCatalogFacet)

	// Under the cap → exact, low-card.
	f.Merge(FileContribution{Labels: map[string][]string{"service.name": {"a", "b"}}})
	if f.IsHighCard("service.name") {
		t.Fatal("service.name (2 values) must be low-card")
	}
	if got := f.Values("service.name", "", 0); !equal(got, []string{"a", "b"}) {
		t.Fatalf("low-card values = %v", got)
	}

	// Over the cap (3) → high-card: not enumerable.
	f.Merge(FileContribution{Labels: map[string][]string{"pod": {"p1", "p2", "p3", "p4"}}})
	if !f.IsHighCard("pod") {
		t.Fatal("pod (4 > 3) must be high-card")
	}
	if got := f.Values("pod", "", 0); got != nil {
		t.Fatalf("high-card field must not enumerate, got %v", got)
	}

	// Forced sketch field → high-card immediately.
	f.Merge(FileContribution{Labels: map[string][]string{"trace_id": {"x"}}})
	if !f.IsHighCard("trace_id") {
		t.Fatal("trace_id must be forced high-card (alwaysSketch)")
	}
	if got := f.Values("trace_id", "", 0); got != nil {
		t.Fatalf("forced-sketch field must not enumerate, got %v", got)
	}

	// Fields() still lists high-card fields (they are valid field names).
	flds := f.Fields()
	want := map[string]bool{"service.name": true, "pod": true, "trace_id": true}
	if len(flds) != len(want) {
		t.Fatalf("Fields() = %v, want all of %v", flds, want)
	}
	for _, fn := range flds {
		if !want[fn] {
			t.Fatalf("unexpected field %q", fn)
		}
	}
}

func TestFieldCatalog_ParityWithGroundTruth(t *testing.T) {
	cs := sampleContribs()
	gt := groundTruth(cs)
	_, f := buildCatalog(cs)

	// field_names parity
	wantFields := make(map[string]bool)
	for fld := range gt {
		wantFields[fld] = true
	}
	if got := f.Fields(); !equal(got, sortedKeys(wantFields)) {
		t.Fatalf("Fields() = %v, want %v", got, sortedKeys(wantFields))
	}

	// field_values parity, per field
	for fld, set := range gt {
		want := sortedKeys(set)
		if got := f.Values(fld, "", 0); !equal(got, want) {
			t.Fatalf("Values(%q) = %v, want %v", fld, got, want)
		}
	}
	// a field that doesn't exist returns nothing
	if got := f.Values("nope", "", 0); len(got) != 0 {
		t.Fatalf("Values(nope) = %v, want empty", got)
	}
}

// TestFieldCatalog_TypeaheadExact: substring filtering is exact and capped.
func TestFieldCatalog_TypeaheadExact(t *testing.T) {
	_, f := buildCatalog(sampleContribs())
	if got := f.Values("service.name", "service", 0); !equal(got, []string{"order-service", "payment-service", "user-service"}) {
		t.Fatalf("typeahead 'service' = %v", got)
	}
	if got := f.Values("service.name", "api", 0); !equal(got, []string{"api-gateway"}) {
		t.Fatalf("typeahead 'api' = %v", got)
	}
	if got := f.Values("service.name", "", 2); len(got) != 2 {
		t.Fatalf("limit=2 returned %d values", len(got))
	}
}

// TestFieldCatalog_BundleRoundTripParity: encode through the bundle codec and
// decode into a FRESH dict — the persisted catalog must answer identically to
// the resident one (persisted == resident).
func TestFieldCatalog_BundleRoundTripParity(t *testing.T) {
	cs := sampleContribs()
	_, f := buildCatalog(cs)

	b := NewBundle("p")
	b.Set(f)
	var buf bytes.Buffer
	if err := b.Encode(&buf); err != nil {
		t.Fatal(err)
	}

	reg := map[FacetKind]FacetFactory{FacetFieldCatalog: NewFieldCatalogFactory(NewDict())}
	got, res, err := DecodeBundle(&buf, reg)
	if err != nil || len(res.Skipped) != 0 {
		t.Fatalf("decode: err=%v skipped=%v", err, res.Skipped)
	}
	gfAny, ok := got.Get(FacetFieldCatalog)
	if !ok {
		t.Fatal("catalog facet missing after round-trip")
	}
	gf := gfAny.(*fieldCatalogFacet)

	for fld := range groundTruth(cs) {
		if a, b := f.Values(fld, "", 0), gf.Values(fld, "", 0); !equal(a, b) {
			t.Fatalf("persisted!=resident for %q: %v vs %v", fld, b, a)
		}
	}
	if !equal(f.Fields(), gf.Fields()) {
		t.Fatalf("Fields() differ after round-trip: %v vs %v", f.Fields(), gf.Fields())
	}
}

// TestFieldCatalog_DeterministicEncode: same content, different merge order →
// byte-identical encoding (golden-test contract for the per-facet parity gate).
func TestFieldCatalog_DeterministicEncode(t *testing.T) {
	cs := sampleContribs()
	rev := make([]FileContribution, len(cs))
	for i := range cs {
		rev[len(cs)-1-i] = cs[i]
	}
	_, f1 := buildCatalog(cs)
	_, f2 := buildCatalog(rev)
	var a, b bytes.Buffer
	if err := f1.Encode(&a); err != nil {
		t.Fatal(err)
	}
	if err := f2.Encode(&b); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Fatal("catalog encoding is not deterministic across merge order")
	}
}

// TestFieldCatalog_SelfHealRebuildParity: a corrupt catalog facet is skipped on
// load, then rebuilt from the partition's files — and the rebuilt catalog must
// answer IDENTICALLY to the original (rebuild == original).
func TestFieldCatalog_SelfHealRebuildParity(t *testing.T) {
	cs := sampleContribs()
	_, orig := buildCatalog(cs)

	b := NewBundle("p")
	b.Set(orig)
	var buf bytes.Buffer
	if err := b.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	raw[len(raw)-1] ^= 0xFF // corrupt the catalog payload

	reg := map[FacetKind]FacetFactory{FacetFieldCatalog: NewFieldCatalogFactory(NewDict())}
	got, res, err := DecodeBundle(bytes.NewReader(raw), reg)
	if err != nil {
		t.Fatalf("decode must not error on isolated corruption: %v", err)
	}
	if len(res.Skipped) != 1 || res.Skipped[0] != FacetFieldCatalog {
		t.Fatalf("expected catalog skipped for rebuild, got %v", res.Skipped)
	}
	if _, ok := got.Get(FacetFieldCatalog); ok {
		t.Fatal("corrupt catalog must not be installed")
	}

	// Rebuild from the partition's files (the self-heal path).
	s := NewStore()
	s.Register(FacetFieldCatalog, NewFieldCatalogFactory(NewDict()))
	s.Rebuild("p", cs)
	rebuiltAny, ok := s.Get("p", FacetFieldCatalog)
	if !ok {
		t.Fatal("rebuild produced no catalog")
	}
	rebuilt := rebuiltAny.(*fieldCatalogFacet)
	for fld := range groundTruth(cs) {
		if a, b := orig.Values(fld, "", 0), rebuilt.Values(fld, "", 0); !equal(a, b) {
			t.Fatalf("rebuild!=original for %q: %v vs %v", fld, b, a)
		}
	}
}

// TestFieldCatalog_HighCardRoundTrip: the payload's high-card section must
// round-trip the cardinality-cap state. A field that crossed the threshold stays
// non-enumerable after Encode→Decode — even into an UNCAPPED facet, so the state
// provably comes from the payload, not the receiving facet's threshold — and new
// Merges must NOT re-accumulate its values (a decoded facet would otherwise serve
// a truncated list as authoritative). The normal field round-trips exactly.
func TestFieldCatalog_HighCardRoundTrip(t *testing.T) {
	src := NewFieldCatalogFactoryCapped(NewDict(), 3, nil)("p").(*fieldCatalogFacet)
	src.Merge(FileContribution{Labels: map[string][]string{
		"service.name": {"a", "b"},               // normal: under the cap
		"pod":          {"p1", "p2", "p3", "p4"}, // crosses the cap (4 > 3) → high-card
	}})
	if !src.IsHighCard("pod") || src.IsHighCard("service.name") {
		t.Fatal("precondition: pod must be high-card, service.name low-card")
	}

	var buf bytes.Buffer
	if err := src.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	got := NewFieldCatalogFactory(NewDict())("p").(*fieldCatalogFacet) // threshold 0 = unlimited
	if err := got.Decode(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}

	if !got.IsHighCard("pod") {
		t.Fatal("high-card state lost in round-trip")
	}
	if vals := got.Values("pod", "", 0); vals != nil {
		t.Fatalf("decoded high-card field must not enumerate, got %v", vals)
	}
	if vals := got.Values("service.name", "", 0); !equal(vals, []string{"a", "b"}) {
		t.Fatalf("normal field values = %v, want [a b]", vals)
	}

	// New contributions must NOT resurrect the capped field's values.
	got.Merge(FileContribution{Labels: map[string][]string{"pod": {"p9"}}})
	if vals := got.Values("pod", "", 0); vals != nil {
		t.Fatalf("merge after decode re-accumulated high-card values: %v", vals)
	}
	if !got.IsHighCard("pod") {
		t.Fatal("pod must stay high-card after new merges")
	}
	// The high-card field is still a valid field NAME.
	if flds := got.Fields(); !equal(flds, []string{"pod", "service.name"}) {
		t.Fatalf("Fields() = %v, want [pod service.name]", flds)
	}
}

// TestMerge_TruncatedFieldsMarksHighCard: a field whose per-file value list hit
// the extractor cap (TruncatedFields) becomes non-enumerable from that
// contribution on — even though Labels carries values for it in the SAME
// contribution — and later, non-truncated contributions do not resurrect it. The
// catalog never serves a possibly-incomplete list as authoritative.
func TestMerge_TruncatedFieldsMarksHighCard(t *testing.T) {
	f := NewFieldCatalogFactory(NewDict())("p").(*fieldCatalogFacet)
	f.Merge(FileContribution{
		Labels:          map[string][]string{"f": {"a", "b"}, "ok": {"x"}},
		TruncatedFields: []string{"f"},
	})
	if !f.IsHighCard("f") {
		t.Fatal("truncated field must be high-card")
	}
	if vals := f.Values("f", "", 0); vals != nil {
		t.Fatalf("truncated field must not enumerate its (incomplete) values, got %v", vals)
	}
	// The sibling field in the same contribution is unaffected.
	if vals := f.Values("ok", "", 0); !equal(vals, []string{"x"}) {
		t.Fatalf("sibling field values = %v, want [x]", vals)
	}

	// A later contribution WITHOUT the truncation flag must not resurrect it.
	f.Merge(FileContribution{Labels: map[string][]string{"f": {"c"}}})
	if vals := f.Values("f", "", 0); vals != nil {
		t.Fatalf("later contribution resurrected truncated field: %v", vals)
	}
	if !f.IsHighCard("f") {
		t.Fatal("field must stay high-card after later contributions")
	}
}

// FuzzFieldCatalogDecode: arbitrary bytes fed straight into the catalog facet's
// Decode must never panic, hang, or over-allocate (the per-value length cap and
// the prealloc-hint cap bound allocation; every count is otherwise bounded by
// the payload bytes running out). The bundle codec's CRC normally shields this
// path, but Decode must hold on its own. Seeded with a valid Encode output so
// the corpus starts inside the format.
func FuzzFieldCatalogDecode(f *testing.F) {
	_, seedFacet := buildCatalog(sampleContribs())
	var seed bytes.Buffer
	if err := seedFacet.Encode(&seed); err != nil {
		f.Fatal(err)
	}
	f.Add(seed.Bytes())
	f.Add([]byte(nil))
	f.Add([]byte{0, 0, 0, 0})             // 0 fields, missing high-card section
	f.Add([]byte{0xff, 0xff, 0xff, 0xff}) // huge fieldCount, no payload
	f.Fuzz(func(t *testing.T, data []byte) {
		fc := NewFieldCatalogFactory(NewDict())("p").(*fieldCatalogFacet)
		_ = fc.Decode(bytes.NewReader(data)) // must simply not panic
	})
}
