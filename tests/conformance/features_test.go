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
		ID: id, Title: id, Status: status, Area: "storage",
		Surfaces: []registry.FeatureSurface{"api"}, Highlight: "h", Description: "d",
	}
}

// materializeRelease applies the release workflow's changelog step
// (scripts/ci/materialize_unreleased.sh): `## [Unreleased]` becomes an empty
// [Unreleased] section followed by `## [<version>]`, which now holds every
// bullet that was unreleased.
func materializeRelease(changelog, version string) string {
	return strings.Replace(changelog, "## [Unreleased]", "## [Unreleased]\n\n## ["+version+"] - 2026-09-14", 1)
}

const gateChangelog = `# Changelog

## [Unreleased]

### Added

- **Brand new.** text.

## [1.1.0] - 2026-09-01

### Added

- **Rebuilt.** text.

## [1.0.0] - 2026-08-01

### Added

- **Original.** text.
`

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
	d := CheckFeatures(featureSet(a), featureTestRegistry(), &registry.Changelog{Versions: []string{"1.0.0"}, Added: bullets})
	if len(d.UnmappedBullets) != 1 || d.UnmappedBullets[0].LeadIn != "Unmapped thing." {
		t.Fatalf("UnmappedBullets = %+v", d.UnmappedBullets)
	}
	msg := strings.Join(d.HardFailures(), "\n")
	if !strings.Contains(msg, "Unmapped thing.") || !strings.Contains(msg, "line 20") {
		t.Errorf("the failure must quote the bullet and its line: %q", msg)
	}
}

// TestCheckFeatures_UnknownBullets covers the other direction of rule (d): a
// claimed lead-in that no `### Added` bullet carries (a typo, or a bullet
// reworded after the claim) would silently drop the feature's releases from
// the generated documents.
func TestCheckFeatures_UnknownBullets(t *testing.T) {
	a := newFeature("lh.feature.storage.a", registry.StatusShipped)
	a.Tests = []string{"x_test.go"}
	a.ChangelogBullets = []string{"Original.", "Orginal."}

	d := CheckFeatures(featureSet(a), featureTestRegistry(), registry.ParseChangelogBytes([]byte(gateChangelog)))
	if len(d.UnknownBullets) != 1 || d.UnknownBullets[0] != "lh.feature.storage.a -> Orginal." {
		t.Fatalf("UnknownBullets = %v", d.UnknownBullets)
	}
	if !strings.Contains(strings.Join(d.HardFailures(), "\n"), "copy the bullet's bold lead-in exactly") {
		t.Error("an unknown claimed lead-in must be a hard failure naming the fix")
	}
	if d := CheckFeatures(featureSet(a), featureTestRegistry(), nil); len(d.UnknownBullets) != 2 {
		t.Errorf("against a nil (empty) changelog every claim is unknown, got %v", d.UnknownBullets)
	}
}

// TestCheckFeatures_ReleaseConflicts covers rule (f): CHANGELOG.md is the only
// source of release information, so a declared `since:`/`changelog:` may only
// say what the changelog agrees with.
func TestCheckFeatures_ReleaseConflicts(t *testing.T) {
	cl := registry.ParseChangelogBytes([]byte(gateChangelog))
	withBullets := func(since string, bullets ...string) registry.Feature {
		f := newFeature("lh.feature.storage.f", registry.StatusShipped)
		f.Tests = []string{"x_test.go"}
		f.Since = since
		f.ChangelogBullets = bullets
		return f
	}
	withoutBullets := func(since string, changelog ...string) registry.Feature {
		f := newFeature("lh.feature.storage.f", registry.StatusShipped)
		f.Tests = []string{"x_test.go"}
		f.Since = since
		f.Changelog = changelog
		return f
	}

	cases := []struct {
		name    string
		feature registry.Feature
		want    string // "" means no conflict
	}{
		{"derived first release", withBullets("", "Original.", "Rebuilt."), ""},
		{"an override naming another claimed release", withBullets("v1.1.0", "Original.", "Rebuilt."), ""},
		{"an override repeating the derived release", withBullets("v1.0.0", "Original.", "Rebuilt."), "repeats the first release"},
		{"an override naming an unclaimed release", withBullets("v1.1.0", "Original."), "is not the release of any of its changelog bullets (1.0.0)"},
		{"an override on a feature that is not released yet", withBullets("v1.1.0", "Brand new."), "(Unreleased)"},
		{"an override whose bullets are all unknown", withBullets("v1.1.0", "Gone."), "(none found in CHANGELOG.md)"},
		{"a declared release without bullets", withoutBullets("v1.0.0", "1.1.0"), ""},
		{"a declared since that is no release", withoutBullets("v9.9.9"), "`since: v9.9.9` is not a released version heading"},
		{"a declared changelog version that is no release", withoutBullets("", "9.9.9"), "`changelog: 9.9.9` is not a released version heading"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := CheckFeatures(featureSet(tc.feature), featureTestRegistry(), cl)
			got := strings.Join(d.ReleaseConflicts, "\n")
			switch {
			case tc.want == "" && got != "":
				t.Errorf("want no conflict, got %q", got)
			case tc.want != "" && !strings.Contains(got, tc.want):
				t.Errorf("conflicts %q do not contain %q", got, tc.want)
			case tc.want != "" && !strings.Contains(strings.Join(d.HardFailures(), "\n"), tc.want):
				t.Errorf("a release conflict must be a hard failure")
			}
		})
	}
}

// TestCheckFeatures_MaterializationKeepsTheGateGreen is the catalog half of
// the release-workflow guarantee: the release PR only rewrites CHANGELOG.md,
// so the gate must be clean both before and after `[Unreleased]` moves under
// a version heading — and after a second release on top — without any
// catalog change.
func TestCheckFeatures_MaterializationKeepsTheGateGreen(t *testing.T) {
	fresh := newFeature("lh.feature.storage.fresh", registry.StatusShipped)
	fresh.Tests = []string{"x_test.go"}
	fresh.ChangelogBullets = []string{"Brand new."}
	rebuilt := newFeature("lh.feature.storage.rebuilt", registry.StatusShipped)
	rebuilt.Tests = []string{"x_test.go"}
	rebuilt.Since = "v1.1.0" // the rebuild, keeping the original's bullet as history
	rebuilt.ChangelogBullets = []string{"Original.", "Rebuilt."}
	set := featureSet(fresh, rebuilt)

	noRows := &registry.Registry{ByID: map[string]*registry.Row{}}
	released := materializeRelease(gateChangelog, "1.2.0")
	releasedTwice := materializeRelease(released, "1.2.1")
	for name, text := range map[string]string{"before": gateChangelog, "after one release": released, "after two releases": releasedTwice} {
		d := CheckFeatures(set, noRows, registry.ParseChangelogBytes([]byte(text)))
		if msgs := d.HardFailures(); len(msgs) != 0 {
			t.Errorf("%s: the gate must stay clean without a catalog change, got:\n%s", name, strings.Join(msgs, "\n"))
		}
	}
}

func TestFeatureDrift_Summary(t *testing.T) {
	a := newFeature("lh.feature.storage.a", registry.StatusShipped)
	a.Rows = []string{"lh.claimed.pending"}
	d := CheckFeatures(featureSet(a), featureTestRegistry(), nil)
	s := d.Summary()
	for _, want := range []string{"rows without feature", "shipped unverified", "unmapped changelog bullets", "coverage gaps"} {
		if !strings.Contains(s, want) {
			t.Errorf("summary %q missing %q", s, want)
		}
	}
	if len(d.VerificationGaps) != 1 {
		t.Errorf("a shipped feature whose only row is pending is a coverage gap: %v", d.VerificationGaps)
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
	changelog, err := registry.ParseChangelog(filepath.Join(root, "CHANGELOG.md"))
	if err != nil {
		t.Fatal(err)
	}

	d := CheckFeatures(set, reg, changelog)
	for _, msg := range d.HardFailures() {
		t.Errorf("feature catalog: %s", msg)
	}
	t.Logf("feature catalog: %d features, %s", len(set.Features), d.Summary())

	// The generated documents must be current. docs/features.md is compared
	// the way `confgen -check` compares it: release references rendered
	// before the newest release was materialized are still current.
	wantFeatures := report.RenderFeatures(set, reg, changelog, report.FeaturesDocDir)
	haveFeatures, err := os.ReadFile(filepath.Join(root, report.FeaturesDocDir, "features.md"))
	if err != nil {
		t.Fatalf("read docs/features.md: %v", err)
	}
	if !wantFeatures.Accepts(haveFeatures) {
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
		UnknownBullets:     []string{"lh.feature.storage.a -> Ghost bullet."},
		ReleaseConflicts:   []string{"feature lh.feature.storage.a: `since: v9.9.9` is not a released version heading in CHANGELOG.md"},
	}
	msgs := d.HardFailures()
	if len(msgs) != 8 {
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
		"Ghost bullet.",
		"`since: v9.9.9`",
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
