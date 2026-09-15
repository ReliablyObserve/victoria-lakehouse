package registry_test

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
)

// A `## [x.y.z]` section claims that its bullets describe what tag vx.y.z
// contains. Nothing checked that, and the claim is easy to break WITHOUT anyone
// making a mistake:
//
// a release is cut from one commit, and the release-metadata PR that files
// `[Unreleased]` under the new heading is prepared later — so any bullet that
// landed on main in between is filed under a tag whose tree does not have it.
// The reader is then told a change shipped in a release that does not contain
// it, which is worse than no changelog: it is a changelog that is wrong in a
// direction nobody re-checks.
//
// The invariant here is the weakest one that catches that: a bullet filed under
// `[x.y.z]` must already appear in CHANGELOG.md AT TAG vx.y.z — where the
// feature PR that introduced it would have left it, under `[Unreleased]`.
// Section-level equality with `[Unreleased]`-at-tag is too strict to be useful:
// releases here are also backfilled, consolidated and annotated by hand, and 31
// of 74 tagged sections legitimately differ that way.
//
// Existing violations are baselined rather than fixed in one sweep — the
// baseline only shrinks, like tests/parity/known_failures.txt. A NEW violation
// fails this test.

var changelogTagBaselineFile = filepath.Join("testdata", "changelog_tag_baseline.txt")

var (
	versionHeading = regexp.MustCompile(`^## \[([0-9][^\]]*)\]`)
	mdHeading      = regexp.MustCompile(`^#{1,6}\s`)
)

// changelogBullet is one top-level entry: its `- **` marker line plus every
// wrapped and paragraph-broken continuation, whitespace-normalised.
func changelogBullets(md string) []string {
	var out []string
	var cur []string
	blanks := 0
	flush := func() {
		if len(cur) > 0 {
			out = append(out, strings.Join(strings.Fields(strings.Join(cur, " ")), " "))
			cur = nil
		}
	}
	for _, line := range strings.Split(md, "\n") {
		s := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(s, "- **"):
			flush()
			cur = []string{s}
			blanks = 0
		case s == "":
			blanks++
		case len(cur) > 0 && !mdHeading.MatchString(s):
			// A blank line inside an entry is a paragraph break when what
			// follows is indented; only unindented text ends the entry.
			if blanks > 0 && !strings.HasPrefix(line, " ") {
				flush()
			} else {
				cur = append(cur, s)
			}
			blanks = 0
		default:
			flush()
			blanks = 0
		}
	}
	flush()
	return out
}

// leadIn is the bold title, which is what identifies an entry across rewraps.
func leadIn(bullet string) string {
	parts := strings.Split(bullet, "**")
	if len(parts) > 1 {
		return parts[1]
	}
	if len(bullet) > 60 {
		return bullet[:60]
	}
	return bullet
}

func git(t *testing.T, root string, args ...string) (string, bool) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = root
	out, err := cmd.Output()
	return string(out), err == nil
}

func loadTagBaseline(t *testing.T, root string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	f, err := os.Open(filepath.Join(root, "tests", "conformance", "registry", changelogTagBaselineFile))
	if err != nil {
		if os.IsNotExist(err) {
			return out
		}
		t.Fatalf("open baseline: %v", err)
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out[line] = true
	}
	return out
}

// TestChangelogSectionsDescribeTheirTag is the gate.
func TestChangelogSectionsDescribeTheirTag(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := git(t, root, "rev-parse", "--git-dir"); !ok {
		t.Skip("not a git checkout")
	}
	tagList, ok := git(t, root, "tag")
	if !ok {
		t.Skip("cannot list tags")
	}
	tags := map[string]bool{}
	for _, tg := range strings.Fields(tagList) {
		tags[tg] = true
	}
	if len(tags) == 0 {
		t.Skip("no tags fetched — this gate needs them (actions/checkout fetch-depth: 0)")
	}

	data, err := os.ReadFile(filepath.Join(root, "CHANGELOG.md"))
	if err != nil {
		t.Fatal(err)
	}
	baseline := loadTagBaseline(t, root)

	// Walk the file section by section, keeping each released section's body.
	var version string
	var body []string
	checked, skipped := 0, 0
	var violations, stale []string

	check := func() {
		if version == "" {
			return
		}
		tag := "v" + version
		if !tags[tag] {
			skipped++
			return
		}
		atTag, ok := git(t, root, "show", tag+":CHANGELOG.md")
		if !ok {
			skipped++
			return
		}
		flat := strings.Join(strings.Fields(atTag), " ")
		for _, b := range changelogBullets(strings.Join(body, "\n")) {
			checked++
			key := version + " :: " + leadIn(b)
			present := strings.Contains(flat, leadIn(b))
			switch {
			case present && baseline[key]:
				stale = append(stale, key)
			case !present && !baseline[key]:
				violations = append(violations, key)
			}
		}
	}

	for _, line := range strings.Split(string(data), "\n") {
		if m := versionHeading.FindStringSubmatch(line); m != nil {
			check()
			version, body = m[1], nil
			continue
		}
		if strings.HasPrefix(line, "## [") { // [Unreleased] — nothing to compare against
			check()
			version, body = "", nil
			continue
		}
		if version != "" {
			body = append(body, line)
		}
	}
	check()

	if checked == 0 {
		t.Fatal("no bullets were checked against a tag — the gate would pass vacuously")
	}
	sort.Strings(violations)
	for _, v := range violations {
		t.Errorf("CHANGELOG.md: %s\n"+
			"  this entry is filed under a release whose tag does not contain it: the text is absent from\n"+
			"  CHANGELOG.md at that tag, so the change landed after the release was cut.\n"+
			"  File it under the release that does contain it, or — if it is genuinely part of that\n"+
			"  release — add the line above to %s with the reason.", v, changelogTagBaselineFile)
	}
	sort.Strings(stale)
	for _, s := range stale {
		t.Errorf("%s: %q no longer violates the rule — delete the line, the baseline only shrinks",
			changelogTagBaselineFile, s)
	}
	t.Logf("checked %d bullets across tagged sections (%d sections skipped: no tag or unreadable), %d baselined",
		checked, skipped, len(baseline))
	_ = fmt.Sprint
}
