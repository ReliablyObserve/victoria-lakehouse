package registry

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const fixtureRepo = "testdata/features/repo"

func validFeature() Feature {
	return Feature{
		ID:          "lh.feature.storage.example",
		Title:       "Example",
		Status:      StatusShipped,
		Since:       "v1.2.3",
		Area:        "storage",
		Surfaces:    []FeatureSurface{"storage"},
		Highlight:   "**Example**: one line.",
		Description: "A paragraph.",
	}
}

func TestFeatureValidate_Valid(t *testing.T) {
	f := validFeature()
	f.ReadmeSection = "Write Path"
	f.Bench = []string{"count_total"}
	f.Changelog = []string{"1.2.3", "Unreleased"}
	f.ChangelogBullets = []string{"Something."}
	if err := f.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestFeatureValidate_Rejects(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Feature)
		want string
	}{
		{"bad id", func(f *Feature) { f.ID = "storage.example" }, "must match"},
		{"id area mismatch", func(f *Feature) { f.ID = "lh.feature.query.example" }, "must start with"},
		{"empty title", func(f *Feature) { f.Title = "  " }, "title required"},
		{"bad status", func(f *Feature) { f.Status = "done" }, "status"},
		{"bad area", func(f *Feature) { f.ID, f.Area = "lh.feature.nope.x", "nope" }, "area"},
		{"bad since", func(f *Feature) { f.Since = "1.2.3" }, "since"},
		{"bad readme section", func(f *Feature) { f.ReadmeSection = "Nope" }, "readme_section"},
		{"no surfaces", func(f *Feature) { f.Surfaces = nil }, "surfaces required"},
		{"bad surface", func(f *Feature) { f.Surfaces = []FeatureSurface{"telepathy"} }, "surfaces:"},
		{"dup surface", func(f *Feature) { f.Surfaces = []FeatureSurface{"api", "api"} }, "duplicate"},
		{"bad bench", func(f *Feature) { f.Bench = []string{"not_a_scenario"} }, "bench:"},
		{"bad changelog version", func(f *Feature) { f.Changelog = []string{"v1.2.3"} }, "changelog:"},
		{"empty changelog bullet", func(f *Feature) { f.ChangelogBullets = []string{"  "} }, "changelog_bullets"},
		{"no highlight", func(f *Feature) { f.Highlight = "" }, "highlight required"},
		{"multiline highlight", func(f *Feature) { f.Highlight = "a\nb" }, "single line"},
		{"no description", func(f *Feature) { f.Description = "" }, "description required"},
		{"empty ref", func(f *Feature) { f.Tests = []string{" "} }, "empty reference"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := validFeature()
			tc.mut(&f)
			err := f.Validate()
			if err == nil {
				t.Fatalf("want error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestAreaIndex_UnknownIsLast(t *testing.T) {
	if got := AreaIndex("storage"); got >= len(Areas) {
		t.Fatalf("known area ranked %d", got)
	}
	if got := AreaIndex("nope"); got != len(Areas) {
		t.Fatalf("unknown area ranked %d, want %d", got, len(Areas))
	}
}

func TestLoadFeatures_Valid(t *testing.T) {
	set, err := LoadFeatures("testdata/features/valid", fixtureRepo)
	if err != nil {
		t.Fatalf("LoadFeatures: %v", err)
	}
	if len(set.Features) != 2 {
		t.Fatalf("got %d features, want 2", len(set.Features))
	}
	// Report order is Areas order: storage before query would be wrong;
	// Areas lists ingest, storage, query, so storage comes first.
	if set.Features[0].ID != "lh.feature.storage.example" {
		t.Fatalf("first feature %s, want the storage one (Areas order)", set.Features[0].ID)
	}
	if set.ByID["lh.feature.query.example"] == nil {
		t.Fatal("ByID missing the query feature")
	}
	if got := set.LeadIns()["Example bullet."]; got != "lh.feature.storage.example" {
		t.Fatalf("LeadIns gave %q", got)
	}
	if n := len(set.ByArea("storage")); n != 1 {
		t.Fatalf("ByArea(storage) = %d, want 1", n)
	}
	if n := len(set.ByArea("ui")); n != 0 {
		t.Fatalf("ByArea(ui) = %d, want 0", n)
	}
}

func TestLoadFeatures_Rejects(t *testing.T) {
	cases := []struct{ dir, want string }{
		{"testdata/features/dup", "duplicate id"},
		{"testdata/features/invalid", "status \"done\" invalid"},
		{"testdata/features/unknownkey", "field hilight not found"},
		{"testdata/features/badref", "no such file"},
		{"testdata/features/badlink", "markdown link"},
		{"testdata/features/dupbullet", "claimed by both"},
	}
	for _, tc := range cases {
		t.Run(filepath.Base(tc.dir), func(t *testing.T) {
			_, err := LoadFeatures(tc.dir, fixtureRepo)
			if err == nil {
				t.Fatalf("want error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

func TestLoadFeatures_SkipsRefChecksWithoutRoot(t *testing.T) {
	if _, err := LoadFeatures("testdata/features/badref", ""); err != nil {
		t.Fatalf("with an empty repo root, reference checks must be skipped: %v", err)
	}
}

func TestLoadFeatures_MissingDir(t *testing.T) {
	if _, err := LoadFeatures("testdata/features/nope", ""); err == nil {
		t.Fatal("want error for a missing dir")
	}
	if _, err := LoadFeatures("testdata/features/valid/a.yaml", ""); err == nil {
		t.Fatal("want error when the path is a file")
	}
}

func TestCheckTestRef(t *testing.T) {
	ok := []string{
		"internal/x/x_test.go",
		"internal/x/x_test.go#TestExample",
		"docs/example.md#heading", // non-Go: word match
	}
	for _, ref := range ok {
		if err := CheckTestRef(fixtureRepo, ref); err != nil {
			t.Errorf("CheckTestRef(%q): %v", ref, err)
		}
	}
	bad := map[string]string{
		"":                                 "empty path",
		"/abs/path.go":                     "repo-relative",
		"../escape.go":                     "repo-relative",
		"internal/x":                       "is a directory",
		"internal/x/missing_test.go":       "no such file",
		"internal/x/x_test.go#TestMissing": "no `func TestMissing(`",
		"docs/example.md#nonsense":         "does not mention",
	}
	for ref, want := range bad {
		err := CheckTestRef(fixtureRepo, ref)
		if err == nil {
			t.Errorf("CheckTestRef(%q): want error containing %q", ref, want)
			continue
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("CheckTestRef(%q) = %q, want %q", ref, err, want)
		}
	}
}

func TestCheckDocRef(t *testing.T) {
	if err := CheckDocRef(fixtureRepo, "docs/example.md"); err != nil {
		t.Fatalf("bare doc path: %v", err)
	}
	if err := CheckDocRef(fixtureRepo, "docs/example.md#a-heading"); err != nil {
		t.Fatalf("valid anchor: %v", err)
	}
	bad := map[string]string{
		"":                        "empty path",
		"/abs.md":                 "repo-relative",
		"docs":                    "is a directory",
		"docs/missing.md":         "no such file",
		"docs/example.md#no-such": "no heading",
	}
	for ref, want := range bad {
		err := CheckDocRef(fixtureRepo, ref)
		if err == nil {
			t.Errorf("CheckDocRef(%q): want error containing %q", ref, want)
			continue
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("CheckDocRef(%q) = %q, want %q", ref, err, want)
		}
	}
}

func TestSlugifyHeading(t *testing.T) {
	cases := map[string]string{
		"A heading":                       "a-heading",
		"Level 2: Manifest fast path":     "level-2-manifest-fast-path",
		"`GET /api/v1/bloom/status`":      "get-apiv1bloomstatus",
		"Tenant-isolated pmeta (Phase E)": "tenant-isolated-pmeta-phase-e",
		"under_score kept":                "under_score-kept",
		"  Trimmed  ":                     "trimmed",
	}
	for in, want := range cases {
		if got := SlugifyHeading(in); got != want {
			t.Errorf("SlugifyHeading(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestBenchScenarios_MatchRunScript keeps the allow-list honest: it is
// derived from LOG_QUERIES and TRACE_QUERIES in the benchmark script, so a
// renamed or added scenario there must be reflected here (otherwise a
// feature could cite a benchmark that no longer runs, or a real one would be
// rejected).
func TestBenchScenarios_MatchRunScript(t *testing.T) {
	data, err := os.ReadFile("../../../scripts/bench/run.sh")
	if err != nil {
		t.Skipf("benchmark script not readable: %v", err)
	}
	want := map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		for _, prefix := range []string{"LOG_QUERIES=", "TRACE_QUERIES="} {
			if !strings.HasPrefix(line, prefix) {
				continue
			}
			list := strings.Trim(strings.TrimPrefix(line, prefix), `"`)
			for _, q := range strings.Fields(list) {
				want[q] = true
			}
		}
	}
	if len(want) == 0 {
		t.Fatal("no LOG_QUERIES/TRACE_QUERIES found in scripts/bench/run.sh")
	}
	for q := range want {
		if !BenchScenarios[q] {
			t.Errorf("scripts/bench/run.sh has scenario %q that registry.BenchScenarios lacks", q)
		}
	}
	for q := range BenchScenarios {
		if !want[q] {
			t.Errorf("registry.BenchScenarios has %q, which scripts/bench/run.sh no longer runs", q)
		}
	}
}

// TestFeatures_RealCatalogLoads loads the committed catalog against the real
// repo, so every referenced test and doc must exist on disk.
func TestFeatures_RealCatalogLoads(t *testing.T) {
	root := "../../.."
	set, err := LoadFeatures(filepath.Join(root, "tests", "conformance", "registry", "features"), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(set.Features) < 60 {
		t.Fatalf("catalog has %d features; it is meant to cover the whole product", len(set.Features))
	}

	data, err := os.ReadFile(filepath.Join(root, "CHANGELOG.md"))
	if err != nil {
		t.Fatal(err)
	}
	versions := map[string]bool{}
	for _, v := range ChangelogVersions(data) {
		versions[v] = true
	}
	for i := range set.Features {
		f := &set.Features[i]
		if f.Since != SinceUnreleased && !versions[strings.TrimPrefix(f.Since, "v")] {
			t.Errorf("feature %s: since %q is not a CHANGELOG version heading", f.ID, f.Since)
		}
		for _, v := range f.Changelog {
			if v != "Unreleased" && !versions[v] {
				t.Errorf("feature %s: changelog %q is not a CHANGELOG version heading", f.ID, v)
			}
		}
	}
}

func TestLoadFeatures_BadYAML(t *testing.T) {
	_, err := LoadFeatures("testdata/features/badyaml", "")
	if err == nil {
		t.Fatal("want a decode error for malformed YAML")
	}
	if !strings.Contains(err.Error(), "feature catalog invalid") {
		t.Fatalf("error %q", err)
	}
}

// TestCheckRefs_UnreadableFile covers the branch where a reference's file
// exists but cannot be read: the check must report it rather than treat an
// unreadable file as a satisfied reference.
func TestCheckRefs_UnreadableFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "docs", "secret.md")
	if err := os.WriteFile(path, []byte("# Heading\n"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
	if _, err := os.ReadFile(path); err == nil {
		t.Skip("running with permission to read a 0000 file (root?)")
	}
	if err := CheckDocRef(dir, "docs/secret.md#heading"); err == nil {
		t.Error("CheckDocRef must surface an unreadable file")
	}
	if err := CheckTestRef(dir, "docs/secret.md#Heading"); err == nil {
		t.Error("CheckTestRef must surface an unreadable file")
	}
}

// TestLoadFeatures_UnreadablePaths covers the two I/O failure branches of the
// loader: a catalog file that exists but cannot be read, and a catalog
// directory that cannot be walked. Both must fail loudly — an unreadable
// catalog is not an empty one.
func TestLoadFeatures_UnreadablePaths(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "a.yaml")
	if err := os.WriteFile(file, []byte("- id: lh.feature.storage.x\n"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(file, 0o600) })
	if _, err := os.ReadFile(file); err == nil {
		t.Skip("running with permission to read a 0000 file (root?)")
	}
	if _, err := LoadFeatures(dir, ""); err == nil {
		t.Error("an unreadable catalog file must fail the load")
	}

	sub := filepath.Join(t.TempDir(), "features")
	if err := os.MkdirAll(sub, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(sub, 0o700) })
	if _, err := os.ReadDir(sub); err == nil {
		t.Skip("running with permission to read a 0000 dir (root?)")
	}
	if _, err := LoadFeatures(sub, ""); err == nil {
		t.Error("an unwalkable catalog directory must fail the load")
	}
}

func TestMarkdownLinkTargets(t *testing.T) {
	in := "[a](docs/a.md) [ext](https://example.com) [mail](mailto:x@y.z) [anchor](#z) [abs](/p) [b](docs/b.md#c)"
	got := MarkdownLinkTargets(in)
	want := []string{"docs/a.md", "docs/b.md#c"}
	if len(got) != len(want) {
		t.Fatalf("MarkdownLinkTargets = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("MarkdownLinkTargets = %v, want %v", got, want)
		}
	}
	if n := len(MarkdownLinkTargets("no links here")); n != 0 {
		t.Fatalf("want no targets, got %d", n)
	}
}

func TestCheckMarkdownLinks(t *testing.T) {
	if err := CheckMarkdownLinks(fixtureRepo, "see [it](docs/example.md#a-heading) and [ext](https://example.com)"); err != nil {
		t.Fatalf("valid links: %v", err)
	}
	if err := CheckMarkdownLinks(fixtureRepo, "see [gone](docs/missing.md)"); err == nil {
		t.Error("a link to a missing file must fail — the docs site would break on it")
	}
	if err := CheckMarkdownLinks(fixtureRepo, "see [bad anchor](docs/example.md#nope)"); err == nil {
		t.Error("a link to a missing anchor must fail")
	}
}
