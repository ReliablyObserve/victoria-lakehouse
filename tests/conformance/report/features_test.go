package report

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
)

func testRegistry() *registry.Registry {
	rows := []registry.Row{
		{ID: "lh.a.pending", Origin: registry.OriginLHAddition, Expect: registry.ExpectPass, Pending: true},
		{ID: "lh.b.executing", Origin: registry.OriginLHAddition, Expect: registry.ExpectPass},
	}
	reg := &registry.Registry{Rows: rows, ByID: map[string]*registry.Row{}}
	for i := range reg.Rows {
		reg.ByID[reg.Rows[i].ID] = &reg.Rows[i]
	}
	return reg
}

func testSet(features ...registry.Feature) *registry.FeatureSet {
	set := &registry.FeatureSet{Features: features, ByID: map[string]*registry.Feature{}}
	for i := range set.Features {
		set.ByID[set.Features[i].ID] = &set.Features[i]
	}
	return set
}

func feature(id string, area registry.Area, status registry.Status) registry.Feature {
	return registry.Feature{
		ID: id, Title: "Title of " + id, Status: status, Since: "v1.0.0", Area: area,
		Surfaces:    []registry.FeatureSurface{"api"},
		Highlight:   "**" + id + "**: highlight.",
		Description: "Description of " + id + ".",
	}
}

func TestStatusOf_Icons(t *testing.T) {
	reg := testRegistry()

	withTest := feature("lh.feature.storage.a", "storage", registry.StatusShipped)
	withTest.Tests = []string{"internal/x/x_test.go"}
	if st := StatusOf(&withTest, reg); st.Icon != IconShipped || !st.Verified {
		t.Errorf("a linked test must count as executing verification: %+v", st)
	}

	onlyPending := feature("lh.feature.storage.b", "storage", registry.StatusShipped)
	onlyPending.Rows = []string{"lh.a.pending"}
	st := StatusOf(&onlyPending, reg)
	if st.Icon != IconDeclared || st.Verified {
		t.Errorf("a pending-only row must not count as executing: %+v", st)
	}
	if len(st.Rows) != 1 {
		t.Errorf("cited row was not resolved: %+v", st)
	}

	executingRow := feature("lh.feature.storage.c", "storage", registry.StatusShipped)
	executingRow.Rows = []string{"lh.b.executing"}
	if st := StatusOf(&executingRow, reg); st.Icon != IconShipped {
		t.Errorf("a non-pending row must count as executing: %+v", st)
	}

	missing := feature("lh.feature.storage.d", "storage", registry.StatusShipped)
	missing.Rows = []string{"lh.nope"}
	st = StatusOf(&missing, reg)
	if len(st.Missing) != 1 || st.Icon != IconDeclared {
		t.Errorf("an unknown row must be reported as missing: %+v", st)
	}

	inProgress := feature("lh.feature.storage.e", "storage", registry.StatusInProgress)
	inProgress.Tests = []string{"internal/x/x_test.go"}
	if st := StatusOf(&inProgress, reg); st.Icon != IconInProgress {
		t.Errorf("in-progress icon wins over verification: %+v", st)
	}
	planned := feature("lh.feature.storage.f", "storage", registry.StatusPlanned)
	if st := StatusOf(&planned, reg); st.Icon != IconPlanned {
		t.Errorf("planned icon: %+v", st)
	}
}

func TestVerificationGaps(t *testing.T) {
	gap := feature("lh.feature.storage.b", "storage", registry.StatusShipped)
	gap.Rows = []string{"lh.a.pending"}
	ok := feature("lh.feature.storage.a", "storage", registry.StatusShipped)
	ok.Tests = []string{"internal/x/x_test.go"}
	planned := feature("lh.feature.storage.c", "storage", registry.StatusPlanned)

	got := VerificationGaps(testSet(gap, ok, planned), testRegistry())
	if len(got) != 1 || got[0] != "lh.feature.storage.b" {
		t.Fatalf("VerificationGaps = %v, want only the pending-only shipped feature", got)
	}
}

func TestRenderFeatures(t *testing.T) {
	gap := feature("lh.feature.storage.b", "storage", registry.StatusShipped)
	gap.Rows = []string{"lh.a.pending"}
	ok := feature("lh.feature.ui.a", "ui", registry.StatusShipped)
	ok.Tests = []string{"internal/x/x_test.go#TestX"}
	ok.Bench = []string{"count_total"}
	ok.Docs = []string{"docs/x.md#y"}
	ok.Changelog = []string{"1.0.0"}
	ok.Notes = "A note."
	planned := feature("lh.feature.query.p", "query", registry.StatusPlanned)

	out := RenderFeatures(testSet(gap, ok, planned), testRegistry(), nil, "").String()

	for _, want := range []string{
		"# Lakehouse features (GENERATED",
		"## Summary",
		"| Area | " + IconShipped + " covered by a test | " + IconDeclared + " declared only |",
		"| **Total** | **1** | **1** | **0** | **1** | **3** |",
		// ✅ must say what it asserts: a linked test exists, not that anything ran.
		"Legend: " + IconShipped + " shipped and covered — the catalog links at least one regression test (the file and the test function are checked to exist; the catalog does not run them)",
		"(none yet: every registry row the catalog cites is still `pending`, so today " + IconShipped + " means \"a linked test\")",
		"## Coverage gaps\n\nShipped features with no linked test, whose only verification is a declared, not-yet-executed registry row.",
		"- `lh.feature.storage.b`",
		"## Storage (1)",
		"## Query (1)",
		"## UI (1)",
		"### " + IconShipped + " Title of lh.feature.ui.a",
		"rows: `lh.a.pending` (pass, pending)",
		"tests: `internal/x/x_test.go#TestX`",
		"bench: `count_total`",
		"- Docs: `docs/x.md#y`",
		"- Changelog: `1.0.0`",
		"- Note: A note.",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered features missing %q", want)
		}
	}
	if strings.Contains(out, "None: every shipped feature") {
		t.Error("the gap list must not claim there are no gaps when there is one")
	}
	for _, stale := range []string{IconShipped + " verified", "Verification gaps", "executes today"} {
		if strings.Contains(out, stale) {
			t.Errorf("rendered features still say %q, which overstates what a linked test proves", stale)
		}
	}
}

func TestRenderFeatures_NoGapsAndNoVerification(t *testing.T) {
	ok := feature("lh.feature.ui.a", "ui", registry.StatusShipped)
	ok.Tests = []string{"internal/x/x_test.go"}
	planned := feature("lh.feature.ui.p", "ui", registry.StatusPlanned)
	out := RenderFeatures(testSet(ok, planned), testRegistry(), nil, "").String()
	if !strings.Contains(out, "None: every shipped feature links at least one test or a registry row that executes.") {
		t.Error("with no gaps the list must say so explicitly")
	}
	if !strings.Contains(out, "Verification: none linked yet") {
		t.Error("a planned feature with nothing linked must say so")
	}
}

// TestRenderFeatures_LegendTracksExecutingRows keeps the legend honest when
// the conformance runner starts executing rows: "none yet" must disappear as
// soon as a cited row is no longer pending.
func TestRenderFeatures_LegendTracksExecutingRows(t *testing.T) {
	f := feature("lh.feature.storage.c", "storage", registry.StatusShipped)
	f.Rows = []string{"lh.b.executing"}
	out := RenderFeatures(testSet(f), testRegistry(), nil, "").String()
	if strings.Contains(out, "none yet") {
		t.Errorf("a cited executing row must drop the \"none yet\" clause:\n%s", out)
	}
	if !strings.Contains(out, "or a registry row the conformance runner executes ·") {
		t.Error("the legend must still name executing rows as a way to be covered")
	}
}

func TestRenderReadmeFeatures(t *testing.T) {
	a := feature("lh.feature.storage.a", "storage", registry.StatusShipped)
	a.ReadmeSection = "Write Path"
	b := feature("lh.feature.storage.b", "storage", registry.StatusInProgress)
	b.ReadmeSection = "Write Path"
	c := feature("lh.feature.query.c", "query", registry.StatusPlanned)
	c.ReadmeSection = "Read Path"
	hidden := feature("lh.feature.ops.h", "ops", registry.StatusShipped)

	out := RenderReadmeFeatures(testSet(a, b, c, hidden))

	if !strings.HasPrefix(out, "### Write Path\n") {
		t.Fatalf("sections must follow README order, got:\n%s", out)
	}
	if strings.Contains(out, "lh.feature.ops.h") {
		t.Error("a feature without readme_section must not appear in the README block")
	}
	if !strings.Contains(out, "- "+IconInProgress+" *(in progress)* ") {
		t.Error("in-progress features must be marked in the README")
	}
	if !strings.Contains(out, "- "+IconPlanned+" *(planned)* ") {
		t.Error("planned features must be marked in the README")
	}
	if i, j := strings.Index(out, "### Write Path"), strings.Index(out, "### Read Path"); i > j {
		t.Error("Write Path must precede Read Path")
	}
	if !strings.HasSuffix(out, "\n") || strings.HasSuffix(out, "\n\n") {
		t.Error("the block must end with exactly one newline so the output is byte-stable")
	}
}

func TestReplaceMarkedBlock(t *testing.T) {
	doc := "before\n" + ReadmeFeaturesBegin + "\nold\n" + ReadmeFeaturesEnd + "\nafter\n"
	got, err := ReplaceMarkedBlock(doc, ReadmeFeaturesBegin, ReadmeFeaturesEnd, "new\n")
	if err != nil {
		t.Fatal(err)
	}
	want := "before\n" + ReadmeFeaturesBegin + "\nnew\n" + ReadmeFeaturesEnd + "\nafter\n"
	if got != want {
		t.Fatalf("got:\n%q\nwant:\n%q", got, want)
	}

	// Idempotent: replacing again with the same block is a no-op.
	again, err := ReplaceMarkedBlock(got, ReadmeFeaturesBegin, ReadmeFeaturesEnd, "new\n")
	if err != nil || again != want {
		t.Fatalf("not idempotent: %q (%v)", again, err)
	}
}

func TestReplaceMarkedBlock_Errors(t *testing.T) {
	cases := map[string]string{
		"no begin": "only " + ReadmeFeaturesEnd,
		"no end":   "only " + ReadmeFeaturesBegin,
		"inverted": ReadmeFeaturesEnd + "\n" + ReadmeFeaturesBegin,
	}
	for name, doc := range cases {
		if _, err := ReplaceMarkedBlock(doc, ReadmeFeaturesBegin, ReadmeFeaturesEnd, "x"); err == nil {
			t.Errorf("%s: want an error rather than a silently rewritten document", name)
		}
	}
}

func TestAreaTitle(t *testing.T) {
	cases := map[registry.Area]string{"ui": "UI", "ops": "Ops", "storage": "Storage", "tenancy": "Tenancy"}
	for in, want := range cases {
		if got := areaTitle(in); got != want {
			t.Errorf("areaTitle(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRenderFeatures_UnreleasedAndMissingRow(t *testing.T) {
	f := feature("lh.feature.ops.new", "ops", registry.StatusShipped)
	f.Since = ""
	f.ChangelogBullets = []string{"Brand new."}
	f.Rows = []string{"lh.a.pending", "lh.does.not.exist"}
	f.Surfaces = []registry.FeatureSurface{"cli", "api"}

	out := RenderFeatures(testSet(f), testRegistry(), registry.ParseChangelogBytes([]byte(releaseChangelog)), "").String()
	if !strings.Contains(out, "since: the release after v1.1.0 · surfaces: cli, api") {
		t.Errorf("an unreleased feature must be rendered as shipping in the release after the newest one, surfaces in declaration order:\n%s", out)
	}
	if strings.Contains(out, "unreleased") {
		t.Error("\"unreleased\" stops being true once the release workflow materializes the changelog; it must never be rendered")
	}
	if strings.Contains(out, "lh.does.not.exist") {
		t.Error("a row the registry lacks must not be rendered as verification")
	}
}

func TestRenderFeatures_SummaryCountsInProgress(t *testing.T) {
	wip := feature("lh.feature.storage.wip", "storage", registry.StatusInProgress)
	wip.Tests = []string{"internal/x/x_test.go"}
	out := RenderFeatures(testSet(wip), testRegistry(), nil, "").String()
	if !strings.Contains(out, "| Storage | 0 | 0 | 1 | 0 | 1 |") {
		t.Errorf("an in-progress feature must be counted in its own column:\n%s", out)
	}
	if !strings.Contains(out, "### "+IconInProgress+" ") {
		t.Error("in-progress features must carry their icon in the heading")
	}
}

func TestRelinkMarkdown(t *testing.T) {
	cases := []struct{ name, in, outDir, want string }{
		{
			name:   "repo-relative target becomes docs-relative",
			in:     "See [Persistence & Durability](docs/durability.md).",
			outDir: "docs",
			want:   "See [Persistence & Durability](durability.md).",
		},
		{
			name:   "anchor is preserved",
			in:     "See [Bloom](docs/bloom-index.md#age-based-tiering).",
			outDir: "docs",
			want:   "See [Bloom](bloom-index.md#age-based-tiering).",
		},
		{
			name:   "nested output directory walks up",
			in:     "See [Bloom](docs/bloom-index.md).",
			outDir: "docs/architecture",
			want:   "See [Bloom](../bloom-index.md).",
		},
		{
			name:   "a file outside the output directory keeps a path that reaches it",
			in:     "See [the chart](charts/victoria-lakehouse/values.yaml).",
			outDir: "docs",
			want:   "See [the chart](../charts/victoria-lakehouse/values.yaml).",
		},
		{
			name:   "root output is unchanged",
			in:     "See [Persistence & Durability](docs/durability.md).",
			outDir: "",
			want:   "See [Persistence & Durability](docs/durability.md).",
		},
		{
			name:   "external links and in-page anchors are left alone",
			in:     "[site](https://example.com/x) [mail](mailto:a@b.c) [here](#section) [abs](/x/y)",
			outDir: "docs",
			want:   "[site](https://example.com/x) [mail](mailto:a@b.c) [here](#section) [abs](/x/y)",
		},
		{
			name:   "several links in one line",
			in:     "[a](docs/a.md) and [b](docs/b.md#c)",
			outDir: "docs",
			want:   "[a](a.md) and [b](b.md#c)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := RelinkMarkdown(tc.in, tc.outDir); got != tc.want {
				t.Errorf("RelinkMarkdown(%q, %q) = %q, want %q", tc.in, tc.outDir, got, tc.want)
			}
		})
	}
}

// TestRenderFeatures_RelinksHighlightLinks is the regression lock for the
// documentation-site build: a highlight authored for README.md
// (repo-root-relative) must come out of the docs/ render resolvable from
// docs/, or Docusaurus fails the build on a broken link.
func TestRenderFeatures_RelinksHighlightLinks(t *testing.T) {
	f := feature("lh.feature.ingest.a", "ingest", registry.StatusShipped)
	f.Tests = []string{"internal/x/x_test.go"}
	f.ReadmeSection = "Write Path"
	f.Highlight = "**Durability**: see [Persistence & Durability](docs/durability.md)."
	f.Description = "More in [the bloom index](docs/bloom-index.md#architecture)."

	out := RenderFeatures(testSet(f), testRegistry(), nil, FeaturesDocDir).String()
	if strings.Contains(out, "](docs/") {
		t.Errorf("a link inside docs/features.md must not be repo-root-relative:\n%s", out)
	}
	for _, want := range []string{"](durability.md)", "](bloom-index.md#architecture)"} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered doc missing %q", want)
		}
	}

	readme := RenderReadmeFeatures(testSet(f))
	if !strings.Contains(readme, "](docs/durability.md)") {
		t.Errorf("the README block must keep repo-root-relative links:\n%s", readme)
	}
}

// releaseChangelog has an unreleased entry above two releases, like
// CHANGELOG.md between two releases.
const releaseChangelog = `# Changelog

## [Unreleased]

### Added

- **Brand new.** text.
- **Extended.** text.

## [1.1.0] - 2026-09-01

### Added

- **Rebuilt.** text.

## [1.0.0] - 2026-08-01

### Added

- **Original.** text.
- **Superseded.** text.
`

// materializeRelease applies the release workflow's changelog step
// (scripts/ci/materialize_unreleased.sh): `## [Unreleased]` becomes an empty
// [Unreleased] section followed by `## [<version>]`, which now holds every
// bullet that was unreleased.
func materializeRelease(changelog, version string) string {
	return strings.Replace(changelog, "## [Unreleased]", "## [Unreleased]\n\n## ["+version+"] - 2026-09-14", 1)
}

func TestDocument_Accepts(t *testing.T) {
	d := &Document{}
	d.WriteString("since: ")
	d.release(releaseText{exact: "v1.2.0", alts: []string{"the release after v1.1.0"}})
	d.WriteString(" · end\n")

	if got := d.String(); got != "since: v1.2.0 · end\n" {
		t.Fatalf("String = %q: it must render the exact form", got)
	}
	for have, want := range map[string]bool{
		"since: v1.2.0 · end\n":                   true,  // exact
		"since: the release after v1.1.0 · end\n": true,  // older and still true
		"since: the release after v1.0.0 · end\n": false, // not the release 1.2.0 follows
		"since: v1.1.0 · end\n":                   false, // a different release
		"since: v1.2.0 · end":                     false, // fixed text differs (missing newline)
		"since: v1.2.0 · end\nextra":              false, // trailing bytes
		"since: v1.2.0 · END\n":                   false, // fixed text differs
		"":                                        false,
	} {
		if got := d.Accepts([]byte(have)); got != want {
			t.Errorf("Accepts(%q) = %v, want %v", have, got, want)
		}
	}
}

// TestDocument_AcceptsTriesEveryRendering covers renderings that are prefixes
// of one another: the first one that matches may leave the rest of the
// document unmatched, so the next one must still be tried.
func TestDocument_AcceptsTriesEveryRendering(t *testing.T) {
	d := &Document{}
	d.release(releaseText{exact: "a", alts: []string{"ab"}})
	fmt.Fprintf(d, "%s", "c")
	for have, want := range map[string]bool{"ac": true, "abc": true, "abd": false, "bc": false} {
		if got := d.Accepts([]byte(have)); got != want {
			t.Errorf("Accepts(%q) = %v, want %v", have, got, want)
		}
	}
}

func TestRenderRelease(t *testing.T) {
	cl := registry.ParseChangelogBytes([]byte(releaseChangelog))
	cases := []struct {
		version string
		style   releaseStyle
		want    releaseText
	}{
		{"1.1.0", styleSince, releaseText{exact: "v1.1.0", alts: []string{"the release after v1.0.0"}}},
		{"1.1.0", styleList, releaseText{exact: "`1.1.0`", alts: []string{"the release after `1.0.0`"}}},
		{"1.0.0", styleSince, releaseText{exact: "v1.0.0", alts: []string{"the first release"}}},
		{registry.ChangelogUnreleased, styleSince, releaseText{exact: "the release after v1.1.0"}},
		{registry.ChangelogUnreleased, styleList, releaseText{exact: "the release after `1.1.0`"}},
	}
	for _, tc := range cases {
		got := renderRelease(cl, tc.version, tc.style)
		if got.exact != tc.want.exact || strings.Join(got.alts, "|") != strings.Join(tc.want.alts, "|") {
			t.Errorf("renderRelease(%q, %d) = %+v, want %+v", tc.version, tc.style, got, tc.want)
		}
	}

	nothingReleased := registry.ParseChangelogBytes([]byte("## [Unreleased]\n\n### Added\n\n- **First.** text.\n"))
	if got := renderRelease(nothingReleased, registry.ChangelogUnreleased, styleSince); got.exact != "the first release" || len(got.alts) != 0 {
		t.Errorf("an entry before any release = %+v, want \"the first release\"", got)
	}
}

// releaseFeatures is one feature per way a feature gets its releases.
func releaseFeatures() *registry.FeatureSet {
	fresh := feature("lh.feature.storage.fresh", "storage", registry.StatusShipped)
	fresh.Since = ""
	fresh.Tests = []string{"internal/x/x_test.go"}
	fresh.ChangelogBullets = []string{"Brand new."}

	extended := feature("lh.feature.storage.extended", "storage", registry.StatusShipped)
	extended.Since = ""
	extended.Tests = []string{"internal/x/x_test.go"}
	extended.ChangelogBullets = []string{"Original.", "Extended."}

	rebuilt := feature("lh.feature.storage.rebuilt", "storage", registry.StatusShipped)
	rebuilt.Since = "v1.1.0" // the rebuild; the superseded implementation's bullet stays as history
	rebuilt.Tests = []string{"internal/x/x_test.go"}
	rebuilt.ChangelogBullets = []string{"Superseded.", "Rebuilt."}

	declared := feature("lh.feature.storage.declared", "storage", registry.StatusShipped)
	declared.Tests = []string{"internal/x/x_test.go"}
	declared.Changelog = []string{"1.0.0"}

	planned := feature("lh.feature.query.planned", "query", registry.StatusPlanned)
	planned.Since = ""

	return testSet(fresh, extended, rebuilt, declared, planned)
}

func TestRenderFeatures_ReleasesFromChangelog(t *testing.T) {
	out := RenderFeatures(releaseFeatures(), testRegistry(), registry.ParseChangelogBytes([]byte(releaseChangelog)), "").String()
	for _, want := range []string{
		// Every bullet unreleased: the release after the newest one.
		"`lh.feature.storage.fresh` · status: shipped · since: the release after v1.1.0 · surfaces: api",
		"- Changelog: the release after `1.1.0`\n",
		// Released and unreleased bullets: since is the oldest; the list runs oldest first.
		"`lh.feature.storage.extended` · status: shipped · since: v1.0.0 · surfaces: api",
		"- Changelog: `1.0.0`, the release after `1.1.0`\n",
		// A declared override wins over the oldest bullet; the list is still derived.
		"`lh.feature.storage.rebuilt` · status: shipped · since: v1.1.0 · surfaces: api",
		"- Changelog: `1.0.0`, `1.1.0`\n",
		// A feature without bullets keeps what it declares.
		"`lh.feature.storage.declared` · status: shipped · since: v1.0.0 · surfaces: api",
		// Nothing released and nothing declared: no since at all.
		"`lh.feature.query.planned` · status: planned · surfaces: api",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered features missing %q", want)
		}
	}
}

// TestRenderFeatures_ReleaseMaterialization is the end-to-end model of a
// release: the release workflow's PR rewrites only CHANGELOG.md. The committed
// docs/features.md must stay current for that PR (it is not regenerated), a
// regeneration must pick up the new versions with no catalog change, and a
// document that says anything untrue must still be rejected.
func TestRenderFeatures_ReleaseMaterialization(t *testing.T) {
	set, reg := releaseFeatures(), testRegistry()
	render := func(changelog string) *Document {
		return RenderFeatures(set, reg, registry.ParseChangelogBytes([]byte(changelog)), "")
	}

	committed := []byte(render(releaseChangelog).String())

	released := materializeRelease(releaseChangelog, "1.2.0")
	after := render(released)
	if string(committed) == after.String() {
		t.Fatal("after a release the exact rendering must name the new version, without any catalog change")
	}
	for _, want := range []string{
		"`lh.feature.storage.fresh` · status: shipped · since: v1.2.0 · surfaces: api",
		"- Changelog: `1.2.0`\n",
		"- Changelog: `1.0.0`, `1.2.0`\n",
	} {
		if !strings.Contains(after.String(), want) {
			t.Errorf("regenerated document missing %q", want)
		}
	}

	if !after.Accepts(committed) {
		t.Error("the release PR does not regenerate docs/features.md; its pre-release rendering is still true and must be accepted")
	}
	if !after.Accepts([]byte(after.String())) {
		t.Error("the exact rendering must be accepted")
	}
	if again := render(released); again.String() != after.String() {
		t.Error("regeneration after the release must be byte-stable")
	}

	releasedTwice := materializeRelease(released, "1.2.1")
	twice := render(releasedTwice)
	if !twice.Accepts(committed) {
		t.Error("a second release without regeneration must still accept the original rendering")
	}
	if !twice.Accepts([]byte(after.String())) {
		t.Error("a second release must accept the rendering regenerated after the first")
	}

	// What must still be rejected after the release.
	edited := releaseFeatures()
	edited.Features[0].Highlight = "**edited**: a catalog change nobody regenerated."
	if RenderFeatures(edited, reg, registry.ParseChangelogBytes([]byte(released)), "").Accepts(committed) {
		t.Error("a catalog change must make the committed document stale")
	}
	for name, have := range map[string]string{
		"an unreleased entry claimed for the wrong release": strings.Replace(string(committed), "since: the release after v1.1.0", "since: the release after v1.0.0", 1),
		"a released version replaced by another":            strings.Replace(string(committed), "since: v1.0.0 · surfaces: api\n\n**lh.feature.storage.extended**", "since: v1.1.0 · surfaces: api\n\n**lh.feature.storage.extended**", 1),
	} {
		if have == string(committed) {
			t.Fatalf("%s: the mutation did not apply", name)
		}
		if after.Accepts([]byte(have)) {
			t.Errorf("%s: an untrue release reference must be rejected", name)
		}
	}

	// A release PR assembled by hand that files the entry under a release
	// that does not directly follow 1.1.0 makes "the release after v1.1.0"
	// untrue, so the committed document is stale and must be regenerated.
	const skipped = `# Changelog

## [Unreleased]

## [1.3.0] - 2026-09-15

### Added

- **Brand new.** text.
- **Extended.** text.

## [1.2.0] - 2026-09-14

### Fixed

- **Something else.** text.

## [1.1.0] - 2026-09-01

### Added

- **Rebuilt.** text.

## [1.0.0] - 2026-08-01

### Added

- **Original.** text.
- **Superseded.** text.
`
	if got := registry.ParseChangelogBytes([]byte(skipped)).FeatureVersions(&set.Features[0]); len(got) != 1 || got[0] != "1.3.0" {
		t.Fatalf("fixture: the entry must sit under 1.3.0 only, got %v", got)
	}
	if render(skipped).Accepts(committed) {
		t.Error("an entry filed two releases later no longer shipped in \"the release after v1.1.0\"; the document must be stale")
	}
}

// TestRenderFeatures_FirstReleaseMaterialization covers a changelog with no
// release yet: "the first release" must stay true after it happens.
func TestRenderFeatures_FirstReleaseMaterialization(t *testing.T) {
	f := feature("lh.feature.storage.first", "storage", registry.StatusShipped)
	f.Since = ""
	f.Tests = []string{"internal/x/x_test.go"}
	f.ChangelogBullets = []string{"First."}
	set, reg := testSet(f), testRegistry()
	before := "# Changelog\n\n## [Unreleased]\n\n### Added\n\n- **First.** text.\n"

	committed := RenderFeatures(set, reg, registry.ParseChangelogBytes([]byte(before)), "").String()
	if !strings.Contains(committed, "since: the first release · surfaces") {
		t.Fatalf("an entry before any release must read \"the first release\":\n%s", committed)
	}
	after := RenderFeatures(set, reg, registry.ParseChangelogBytes([]byte(materializeRelease(before, "0.1.0"))), "")
	if !strings.Contains(after.String(), "since: v0.1.0 · surfaces") {
		t.Error("after the first release the exact version must be rendered")
	}
	if !after.Accepts([]byte(committed)) {
		t.Error("\"the first release\" is still true after the first release and must be accepted")
	}
}
