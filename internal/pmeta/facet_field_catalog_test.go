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
