package conformance

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/report"
)

func featureTestRegistry() *registry.Registry {
	rows := []registry.Row{
		{ID: "lh.claimed.pending", Origin: registry.OriginLHAddition, Expect: registry.ExpectPass, Pending: true},
		{ID: "lh.claimed.executing", Origin: registry.OriginLHAddition, Expect: registry.ExpectPass},
		{ID: "lh.orphan.row", Origin: registry.OriginLHAddition, Expect: registry.ExpectPass, Pending: true},
		{ID: "vl.native.row", Origin: registry.OriginNative, Expect: registry.ExpectPass},
	}
	reg := &registry.Registry{Rows: rows, ByID: map[string]*registry.Row{}}
	for i := range reg.Rows {
		reg.ByID[reg.Rows[i].ID] = &reg.Rows[i]
	}
	return reg
}

func featureSet(features ...registry.Feature) *registry.FeatureSet {
	set := &registry.FeatureSet{Features: features, ByID: map[string]*registry.Feature{}}
	for i := range set.Features {
		set.ByID[set.Features[i].ID] = &set.Features[i]
	}
	return set
}

func newFeature(id string, status registry.Status) registry.Feature {
	return registry.Feature{
		ID: id, Title: id, Status: status, Since: "v1.0.0", Area: "storage",
		Surfaces: []registry.FeatureSurface{"api"}, Highlight: "h", Description: "d",
	}
}

func TestCheckFeatures_RowsWithoutFeature(t *testing.T) {
	claims := newFeature("lh.feature.storage.a", registry.StatusShipped)
	claims.Rows = []string{"lh.claimed.pending", "lh.claimed.executing"}

	d := CheckFeatures(featureSet(claims), featureTestRegistry(), nil)
	if len(d.RowsWithoutFeature) != 1 || d.RowsWithoutFeature[0] != "lh.orphan.row" {
		t.Fatalf("RowsWithoutFeature = %v, want only lh.orphan.row (native rows are out of scope)", d.RowsWithoutFeature)
	}
	if !strings.Contains(strings.Join(d.HardFailures(), "\n"), "belongs to no feature") {
		t.Error("an orphan Lakehouse row must be a hard failure naming the fix")
	}
}

func TestCheckFeatures_RowClaimedTwice(t *testing.T) {
	a := newFeature("lh.feature.storage.a", registry.StatusShipped)
	a.Rows = []string{"lh.claimed.pending"}
	b := newFeature("lh.feature.storage.b", registry.StatusShipped)
	b.Rows = []string{"lh.claimed.pending"}

	d := CheckFeatures(featureSet(a, b), featureTestRegistry(), nil)
	if len(d.RowsInManyFeatures) != 1 {
		t.Fatalf("RowsInManyFeatures = %v", d.RowsInManyFeatures)
	}
	if !strings.Contains(d.RowsInManyFeatures[0], "lh.feature.storage.a, lh.feature.storage.b") {
		t.Errorf("the message must name both claimants: %q", d.RowsInManyFeatures[0])
	}
}

func TestCheckFeatures_UnknownRowRef(t *testing.T) {
	a := newFeature("lh.feature.storage.a", registry.StatusShipped)
	a.Rows = []string{"lh.does.not.exist"}
	d := CheckFeatures(featureSet(a), featureTestRegistry(), nil)
	if len(d.UnknownRowRefs) != 1 {
		t.Fatalf("UnknownRowRefs = %v", d.UnknownRowRefs)
	}
}

func TestCheckFeatures_ShippedNeedsRowOrTest(t *testing.T) {
	bare := newFeature("lh.feature.storage.bare", registry.StatusShipped)
	withTest := newFeature("lh.feature.storage.tested", registry.StatusShipped)
	withTest.Tests = []string{"whatever_test.go"}
	planned := newFeature("lh.feature.storage.planned", registry.StatusPlanned)

	d := CheckFeatures(featureSet(bare, withTest, planned), featureTestRegistry(), nil)
	if len(d.ShippedUnverified) != 1 || d.ShippedUnverified[0] != "lh.feature.storage.bare" {
		t.Fatalf("ShippedUnverified = %v", d.ShippedUnverified)
	}
	if !strings.Contains(strings.Join(d.HardFailures(), "\n"), "shipped but cites neither") {
		t.Error("rule (b) must be a hard failure")
	}
}

func TestCheckFeatures_UnshippedRowsMustBePending(t *testing.T) {
	wip := newFeature("lh.feature.storage.wip", registry.StatusInProgress)
	wip.Rows = []string{"lh.claimed.executing"}
	ok := newFeature("lh.feature.storage.ok", registry.StatusInProgress)
	ok.Rows = []string{"lh.claimed.pending"}

	d := CheckFeatures(featureSet(wip, ok), featureTestRegistry(), nil)
	if len(d.RowsMustBePending) != 1 || !strings.Contains(d.RowsMustBePending[0], "lh.feature.storage.wip") {
		t.Fatalf("RowsMustBePending = %v", d.RowsMustBePending)
	}
}

func TestCheckFeatures_UnmappedChangelogBullets(t *testing.T) {
	a := newFeature("lh.feature.storage.a", registry.StatusShipped)
	a.Tests = []string{"x_test.go"}
	a.ChangelogBullets = []string{"Mapped thing."}

	bullets := []registry.ChangelogBullet{
		{Version: "1.0.0", LeadIn: "Mapped thing.", Line: 10},
		{Version: "1.0.0", LeadIn: "Unmapped thing.", Line: 20},
		{Version: "1.0.0", LeadIn: "", Text: "a bullet with no bold lead-in", Line: 30},
	}
	d := CheckFeatures(featureSet(a), featureTestRegistry(), bullets)
	if len(d.UnmappedBullets) != 1 || d.UnmappedBullets[0].LeadIn != "Unmapped thing." {
		t.Fatalf("UnmappedBullets = %+v", d.UnmappedBullets)
	}
	msg := strings.Join(d.HardFailures(), "\n")
	if !strings.Contains(msg, "Unmapped thing.") || !strings.Contains(msg, "line 20") {
		t.Errorf("the failure must quote the bullet and its line: %q", msg)
	}
}

func TestFeatureDrift_Summary(t *testing.T) {
	a := newFeature("lh.feature.storage.a", registry.StatusShipped)
	a.Rows = []string{"lh.claimed.pending"}
	d := CheckFeatures(featureSet(a), featureTestRegistry(), nil)
	s := d.Summary()
	for _, want := range []string{"rows without feature", "shipped unverified", "unmapped changelog bullets", "verification gaps"} {
		if !strings.Contains(s, want) {
			t.Errorf("summary %q missing %q", s, want)
		}
	}
	if len(d.VerificationGaps) != 1 {
		t.Errorf("a shipped feature whose only row is pending is a verification gap: %v", d.VerificationGaps)
	}
}

// TestFeatures_RealCatalog is the gate: the committed catalog, registry and
// changelog must satisfy every feature rule. It is the same check
// `confgen -check` runs, so a failure here is a failure of `make
// conformance-check`.
func TestFeatures_RealCatalog(t *testing.T) {
	root := ".."
	root = filepath.Join(root, "..")

	set, err := registry.LoadFeatures(filepath.Join(root, "tests", "conformance", "registry", "features"), root)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.LoadDir(filepath.Join(root, "tests", "conformance", "registry", "rows"))
	if err != nil {
		t.Fatal(err)
	}
	bullets, err := registry.ParseChangelogAdded(filepath.Join(root, "CHANGELOG.md"))
	if err != nil {
		t.Fatal(err)
	}

	d := CheckFeatures(set, reg, bullets)
	for _, msg := range d.HardFailures() {
		t.Errorf("feature catalog: %s", msg)
	}
	t.Logf("feature catalog: %d features, %s", len(set.Features), d.Summary())

	// The generated documents must be current.
	wantFeatures := report.RenderFeatures(set, reg, report.FeaturesDocDir)
	haveFeatures, err := os.ReadFile(filepath.Join(root, report.FeaturesDocDir, "features.md"))
	if err != nil {
		t.Fatalf("read docs/features.md: %v", err)
	}
	if string(haveFeatures) != wantFeatures {
		t.Error("docs/features.md is stale — run: make conformance-gen")
	}

	readme, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	wantReadme, err := report.ReplaceMarkedBlock(string(readme), report.ReadmeFeaturesBegin, report.ReadmeFeaturesEnd, report.RenderReadmeFeatures(set))
	if err != nil {
		t.Fatalf("README.md feature block: %v", err)
	}
	if string(readme) != wantReadme {
		t.Error("the README Key Features block is stale — run: make conformance-gen")
	}
}

// TestFeatureDrift_HardFailuresCoverEveryRule asserts that each rule produces
// its own actionable message: a CI log that says only "the feature gate
// failed" would leave the author guessing which rule and which feature.
func TestFeatureDrift_HardFailuresCoverEveryRule(t *testing.T) {
	d := FeatureDrift{
		RowsWithoutFeature: []string{"lh.orphan.row"},
		RowsInManyFeatures: []string{"lh.shared.row: lh.feature.storage.a, lh.feature.storage.b"},
		UnknownRowRefs:     []string{"lh.feature.storage.a -> lh.ghost.row"},
		ShippedUnverified:  []string{"lh.feature.storage.bare"},
		RowsMustBePending:  []string{"lh.feature.storage.wip -> lh.claimed.executing"},
		UnmappedBullets:    []registry.ChangelogBullet{{Version: "1.2.3", LeadIn: "Orphan bullet.", Line: 42}},
	}
	msgs := d.HardFailures()
	if len(msgs) != 6 {
		t.Fatalf("got %d messages, want one per violation:\n%s", len(msgs), strings.Join(msgs, "\n"))
	}
	joined := strings.Join(msgs, "\n")
	for _, want := range []string{
		"lh.orphan.row",
		"more than one feature",
		"does not exist",
		"lh.feature.storage.bare",
		"mark the row `pending: true`",
		"Orphan bullet.",
		"tests/conformance/registry/features/",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("hard failures missing %q:\n%s", want, joined)
		}
	}
}

func TestFeatureDrift_NoFailuresWhenClean(t *testing.T) {
	if msgs := (FeatureDrift{}).HardFailures(); len(msgs) != 0 {
		t.Fatalf("a clean drift report must produce no failures, got %v", msgs)
	}
}
