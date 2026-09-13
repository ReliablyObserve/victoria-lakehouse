package report

import (
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

	out := RenderFeatures(testSet(gap, ok, planned), testRegistry(), "")

	for _, want := range []string{
		"# Lakehouse features (GENERATED",
		"## Summary",
		"| **Total** | **1** | **1** | **0** | **1** | **3** |",
		"## Verification gaps",
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
}

func TestRenderFeatures_NoGapsAndNoVerification(t *testing.T) {
	ok := feature("lh.feature.ui.a", "ui", registry.StatusShipped)
	ok.Tests = []string{"internal/x/x_test.go"}
	planned := feature("lh.feature.ui.p", "ui", registry.StatusPlanned)
	out := RenderFeatures(testSet(ok, planned), testRegistry(), "")
	if !strings.Contains(out, "None: every shipped feature") {
		t.Error("with no gaps the list must say so explicitly")
	}
	if !strings.Contains(out, "Verification: none linked yet") {
		t.Error("a planned feature with nothing linked must say so")
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
	f.Since = registry.SinceUnreleased
	f.Rows = []string{"lh.a.pending", "lh.does.not.exist"}
	f.Surfaces = []registry.FeatureSurface{"cli", "api"}

	out := RenderFeatures(testSet(f), testRegistry(), "")
	if !strings.Contains(out, "since: unreleased") {
		t.Error("an unreleased feature must be rendered as such")
	}
	if !strings.Contains(out, "surfaces: cli, api") {
		t.Error("surfaces must be listed in declaration order")
	}
	if strings.Contains(out, "lh.does.not.exist") {
		t.Error("a row the registry lacks must not be rendered as verification")
	}
}

func TestRenderFeatures_SummaryCountsInProgress(t *testing.T) {
	wip := feature("lh.feature.storage.wip", "storage", registry.StatusInProgress)
	wip.Tests = []string{"internal/x/x_test.go"}
	out := RenderFeatures(testSet(wip), testRegistry(), "")
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

	out := RenderFeatures(testSet(f), testRegistry(), FeaturesDocDir)
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
