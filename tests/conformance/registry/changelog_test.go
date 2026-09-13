package registry

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

const sampleChangelog = `# Changelog

## [Unreleased]

### Added

- **New thing.** It does things.

## [1.1.0] - 2026-01-02

### Added

- **A wrapped lead-in that continues
  on the next line (PR #1)** — the rest of the bullet.

  1. **A nested bold item** that must not become the lead-in.

- a bullet with no bold lead-in at all
- **Second thing.** More text.

### Fixed

- **Not an Added bullet.** Ignored.

---

## [1.0.0] - 2026-01-01

### Changed

- **Also ignored.**
`

func TestParseChangelogAdded(t *testing.T) {
	got := ParseChangelogAddedBytes([]byte(sampleChangelog))
	if len(got) != 4 {
		for _, b := range got {
			t.Logf("%s: %q", b.Version, b.LeadIn)
		}
		t.Fatalf("got %d Added bullets, want 4", len(got))
	}

	if got[0].Version != "Unreleased" || got[0].LeadIn != "New thing." {
		t.Errorf("first bullet: %+v", got[0])
	}
	if got[1].Version != "1.1.0" {
		t.Errorf("second bullet version %q", got[1].Version)
	}
	if want := "A wrapped lead-in that continues on the next line (PR #1)"; got[1].LeadIn != want {
		t.Errorf("wrapped lead-in = %q, want %q", got[1].LeadIn, want)
	}
	if !strings.Contains(got[1].Text, "A nested bold item") {
		t.Errorf("the nested sub-item must stay part of the bullet text: %q", got[1].Text)
	}
	if got[2].LeadIn != "" {
		t.Errorf("a bullet without bold must have no lead-in, got %q", got[2].LeadIn)
	}
	if got[3].LeadIn != "Second thing." {
		t.Errorf("fourth bullet lead-in %q", got[3].LeadIn)
	}
	if got[0].Line == 0 {
		t.Error("bullets must carry their line number for the failure message")
	}
}

func TestParseChangelogAdded_UnterminatedBold(t *testing.T) {
	got := ParseChangelogAddedBytes([]byte("## [1.0.0]\n\n### Added\n\n- **never closed\n"))
	if len(got) != 1 {
		t.Fatalf("got %d bullets", len(got))
	}
	if got[0].LeadIn != "" {
		t.Fatalf("unterminated bold must yield no lead-in, got %q", got[0].LeadIn)
	}
}

func TestChangelogVersions(t *testing.T) {
	got := ChangelogVersions([]byte(sampleChangelog))
	want := []string{"Unreleased", "1.1.0", "1.0.0"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestParseChangelogAdded_File(t *testing.T) {
	if _, err := ParseChangelogAdded("testdata/features/nope.md"); err == nil {
		t.Fatal("want an error for a missing changelog")
	}
	bullets, err := ParseChangelogAdded("../../../CHANGELOG.md")
	if err != nil {
		t.Fatal(err)
	}
	if len(bullets) == 0 {
		t.Fatal("the real changelog has Added bullets")
	}
	seen := map[string]bool{}
	for _, b := range bullets {
		if b.LeadIn == "" {
			continue
		}
		if seen[b.LeadIn] {
			t.Errorf("duplicate changelog lead-in %q — the gate matches on it, so it must be unique", b.LeadIn)
		}
		seen[b.LeadIn] = true
	}
}

func TestNormalizeSpace(t *testing.T) {
	if got := normalizeSpace("  a \n b\t c  "); got != "a b c" {
		t.Fatalf("normalizeSpace = %q", got)
	}
}

func TestLeadIn_Empty(t *testing.T) {
	if got := leadIn("no bold here"); got != "" {
		t.Fatalf("leadIn = %q", got)
	}
	if got := leadIn("****"); got != "" {
		t.Fatalf("empty bold must yield no lead-in, got %q", got)
	}
}

func TestParseChangelogAdded_RealFileIsReadable(t *testing.T) {
	if _, err := os.Stat("../../../CHANGELOG.md"); err != nil {
		t.Fatalf("CHANGELOG.md must exist at the repo root: %v", err)
	}
}

func TestChangelog_Releases(t *testing.T) {
	cl := ParseChangelogBytes([]byte(sampleChangelog))

	if got := strings.Join(cl.Released(), ","); got != "1.1.0,1.0.0" {
		t.Errorf("Released = %q, want 1.1.0,1.0.0 (newest first, Unreleased excluded)", got)
	}
	if got := cl.Newest(); got != "1.1.0" {
		t.Errorf("Newest = %q", got)
	}
	if !cl.IsReleased("1.0.0") || cl.IsReleased(ChangelogUnreleased) || cl.IsReleased("9.9.9") {
		t.Error("IsReleased must hold for released headings only")
	}
	for v, want := range map[string]string{
		"1.1.0":             "1.0.0",
		"1.0.0":             "", // the oldest release follows nothing
		ChangelogUnreleased: "", // not a release
		"9.9.9":             "", // not a heading
	} {
		if got := cl.Previous(v); got != want {
			t.Errorf("Previous(%q) = %q, want %q", v, got, want)
		}
	}
}

func TestChangelog_FeatureVersions(t *testing.T) {
	cl := ParseChangelogBytes([]byte(sampleChangelog))

	f := &Feature{ChangelogBullets: []string{"New thing.", "Second thing.", "A wrapped lead-in that continues on the next line (PR #1)"}}
	if got := strings.Join(cl.FeatureVersions(f), ","); got != "1.1.0,Unreleased" {
		t.Errorf("FeatureVersions = %q, want 1.1.0,Unreleased (distinct, oldest first)", got)
	}
	if !cl.HasLeadIn("Second thing.") || cl.HasLeadIn("Not an Added bullet.") {
		t.Error("HasLeadIn must match `### Added` lead-ins only")
	}

	unknown := &Feature{ChangelogBullets: []string{"No such bullet."}}
	if got := cl.FeatureVersions(unknown); len(got) != 0 {
		t.Errorf("a lead-in the changelog lacks contributes no version, got %v", got)
	}
	if got := cl.FeatureVersions(&Feature{}); len(got) != 0 {
		t.Errorf("a feature without bullets has no derived versions, got %v", got)
	}

	var none *Changelog
	if none.FeatureVersions(f) != nil || none.HasLeadIn("New thing.") || none.Newest() != "" || none.Released() != nil {
		t.Error("a nil changelog is an empty one")
	}
}

func TestChangelog_BulletBeforeAnyVersionHeadingHasNoRelease(t *testing.T) {
	cl := ParseChangelogBytes([]byte("# Changelog\n\n### Added\n\n- **Orphan.** text.\n\n## [1.0.0]\n\n### Added\n\n- **Released.** text.\n"))
	f := &Feature{ChangelogBullets: []string{"Orphan.", "Released."}}
	if got := strings.Join(cl.FeatureVersions(f), ","); got != "1.0.0" {
		t.Errorf("FeatureVersions = %q, want only the release the bullet sits under", got)
	}
}

func TestParseChangelog_File(t *testing.T) {
	if _, err := ParseChangelog("testdata/features/nope.md"); err == nil {
		t.Fatal("want an error for a missing changelog")
	}
	cl, err := ParseChangelog("../../../CHANGELOG.md")
	if err != nil {
		t.Fatal(err)
	}
	if len(cl.Added) == 0 || cl.Newest() == "" {
		t.Fatal("the real changelog has releases and Added bullets")
	}
}

// TestChangelog_RealHeadingsAreOrderedAndUnique pins what "the release a
// version directly follows" relies on: CHANGELOG.md lists each release once,
// newest first, which is also how the release workflow inserts a new heading.
func TestChangelog_RealHeadingsAreOrderedAndUnique(t *testing.T) {
	data, err := os.ReadFile("../../../CHANGELOG.md")
	if err != nil {
		t.Fatal(err)
	}
	released := ParseChangelogBytes(data).Released()
	seen := map[string]bool{}
	for i, v := range released {
		if seen[v] {
			t.Errorf("version %s has two headings", v)
		}
		seen[v] = true
		if i > 0 && !semverLess(v, released[i-1]) {
			t.Errorf("version %s is listed below %s but is not older", v, released[i-1])
		}
	}
}

// semverLess compares two X.Y.Z versions numerically.
func semverLess(a, b string) bool {
	pa, pb := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < 3 && i < len(pa) && i < len(pb); i++ {
		na, _ := strconv.Atoi(pa[i])
		nb, _ := strconv.Atoi(pb[i])
		if na != nb {
			return na < nb
		}
	}
	return false
}
