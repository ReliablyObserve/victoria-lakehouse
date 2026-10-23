package registry_test

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
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
// `[x.y.z]` must already appear AT TAG vx.y.z — where the feature PR that
// introduced it would have left it: in CHANGELOG.md under `[Unreleased]` (before
// changelog fragments), or in a changelog.d/ fragment file (since). With
// fragments the tag commit carries the fragment files and the release assembles
// them into CHANGELOG.md afterwards, so a bullet merged after the release
// commit is never in that release's fragments and cannot be filed under it.
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

// fragmentsAtTag is the text of every changelog.d/ file of the tag's tree (empty before
// fragments existed): what the release assembles into its CHANGELOG.md section.
func fragmentsAtTag(t *testing.T, root, tag string) string {
	t.Helper()
	list, ok := git(t, root, "ls-tree", "-r", "--name-only", tag, "--", "changelog.d")
	if !ok {
		return ""
	}
	var b strings.Builder
	for _, path := range strings.Fields(list) {
		// only fragment-named files: the README's prose must not make a lead-in "present at the tag"
		if !registry.FragmentName.MatchString(filepath.Base(path)) {
			continue
		}
		if text, ok := git(t, root, "show", tag+":"+path); ok {
			b.WriteString(text)
			b.WriteString("\n")
		}
	}
	return b.String()
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
		atTag += "\n" + fragmentsAtTag(t, root, tag)
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
			"  CHANGELOG.md and changelog.d/ at that tag, so the change landed after the release was cut.\n"+
			"  New changes are documented by a fragment in changelog.d/ (changelog.d/README.md), which the release\n"+
			"  files under the tag that contains it; to correct a section by hand, move the bullet to the release\n"+
			"  whose tag does contain it, or — if it is genuinely part of this one — add the line above to %s\n"+
			"  with the reason.", v, changelogTagBaselineFile)
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

// TestFragmentsAtTag pins what the tag gate counts as "in the release": the fragment files of the
// tag's tree, and nothing that was merged afterwards (the incident this gate guards against).
func TestFragmentsAtTag(t *testing.T) {
	root := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false"}, args...)...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	write := func(rel, text string) {
		t.Helper()
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	run("init", "-q", "-b", "main")
	write("CHANGELOG.md", "# c\n")
	run("add", "-A")
	run("commit", "-q", "-m", "before fragments")
	run("tag", "v0.1.0")
	write("changelog.d/1.fixed.md", "- **In the release.** t\n")
	write("changelog.d/README.md", "- **Readme prose lead-in.** t\n")
	run("add", "-A")
	run("commit", "-q", "-m", "fragment")
	run("tag", "v0.2.0")
	write("changelog.d/2.fixed.md", "- **Merged after the tag.** t\n")
	run("add", "-A")
	run("commit", "-q", "-m", "later fragment")

	if got := fragmentsAtTag(t, root, "v0.1.0"); strings.TrimSpace(got) != "" {
		t.Errorf("a tag from before fragments has none, got %q", got)
	}
	got := fragmentsAtTag(t, root, "v0.2.0")
	if !strings.Contains(got, "In the release.") {
		t.Errorf("the fragment in the tag's tree is part of the release, got %q", got)
	}
	if strings.Contains(got, "Readme prose lead-in.") {
		t.Errorf("the README is not a fragment: its prose must not make a lead-in present at the tag, got %q", got)
	}
	if strings.Contains(got, "Merged after the tag.") {
		t.Errorf("a fragment merged after the tag is not part of the release, got %q", got)
	}
}

var releaseTagName = regexp.MustCompile(`^v(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})$`)

// fragmentBlobs is path -> blob id of the fragment-named regular files of changelog.d/ at rev.
func fragmentBlobs(t *testing.T, root, rev string) map[string]string {
	t.Helper()
	out := map[string]string{}
	list, ok := git(t, root, "ls-tree", "-r", rev, "--", "changelog.d")
	if !ok {
		return out
	}
	for _, line := range strings.Split(list, "\n") {
		meta, path, found := strings.Cut(line, "\t")
		if !found {
			continue
		}
		f := strings.Fields(meta)
		if len(f) == 3 && f[0] == "100644" && registry.FragmentName.MatchString(filepath.Base(path)) {
			out[path] = f[2]
		}
	}
	return out
}

// abandonedAfter is how many releases with a section may follow a release without one before its
// metadata pull request is called closed: the serial protocol keeps one or two pending, and a section
// merged out of order can trail a release or two.
const abandonedAfter = 3

// leftBehindFragments lists the fragments of HEAD that outlived their release. The fragment is
// byte-identical at the OLDEST release tag T that holds it, and either
//   - `## [T]` exists in HEAD's CHANGELOG.md: the metadata pull request merged (it wrote the section)
//     and the file is still here, so every later release would skip it; or
//   - `## [T]` does NOT exist while at least abandonedAfter newer releases have a section: the
//     metadata pull request of T was closed or never merged, and the fragment is in no section.
//
// A release without a section and fewer newer sections is merely pending.
func leftBehindFragments(t *testing.T, root string) []string {
	t.Helper()
	head := fragmentBlobs(t, root, "HEAD")
	if len(head) == 0 {
		return nil
	}
	changelog, ok := git(t, root, "show", "HEAD:CHANGELOG.md")
	if !ok {
		return nil
	}
	sections := map[string]bool{}
	var sectionTags []string
	for _, v := range registry.ChangelogVersions([]byte(changelog)) {
		if releaseTagName.MatchString("v" + v) {
			sections["v"+v] = true
			sectionTags = append(sectionTags, "v"+v)
		}
	}
	newerSections := func(tag string) int {
		n := 0
		for _, st := range sectionTags {
			if semverLess(tag, st) {
				n++
			}
		}
		return n
	}
	tagList, _ := git(t, root, "tag")
	var tags []string
	for _, tg := range strings.Fields(tagList) {
		if releaseTagName.MatchString(tg) {
			tags = append(tags, tg)
		}
	}
	sort.Slice(tags, func(i, j int) bool { return semverLess(tags[i], tags[j]) })
	var out []string
	found := map[string]bool{}
	for _, tg := range tags { // oldest first: the first tag that holds the bytes is the one that released them
		for path, blob := range fragmentBlobs(t, root, tg) {
			if head[path] != blob || found[path] {
				continue
			}
			found[path] = true
			switch {
			case sections[tg]:
				out = append(out, fmt.Sprintf("%s: released in %s, whose section is already in CHANGELOG.md", path, tg))
			case newerSections(tg) >= abandonedAfter:
				out = append(out, fmt.Sprintf("%s: released in %s, which has no section in CHANGELOG.md although %d newer releases do: "+
					"its metadata PR was closed or never merged: restore the section or file it by hand", path, tg, newerSections(tg)))
			}
		}
	}
	sort.Strings(out)
	return out
}

func semverLess(a, b string) bool {
	pa, pb := strings.Split(a[1:], "."), strings.Split(b[1:], ".")
	for i := 0; i < 3; i++ {
		x, _ := strconv.Atoi(pa[i])
		y, _ := strconv.Atoi(pb[i])
		if x != y {
			return x < y
		}
	}
	return false
}

// TestNoReleasedFragmentIsLeftBehind is the backstop for a fragment that outlived its release-metadata
// pull request (the section was filed by hand or by a merge, and the file was not removed).
func TestNoReleasedFragmentIsLeftBehind(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := git(t, root, "rev-parse", "--git-dir"); !ok {
		t.Skip("not a git checkout")
	}
	if tags, _ := git(t, root, "tag"); strings.TrimSpace(tags) == "" {
		t.Skip("no tags fetched — this gate needs them (actions/checkout fetch-depth: 0)")
	}
	for _, left := range leftBehindFragments(t, root) {
		t.Errorf("changelog.d/ %s.\n"+
			"  Clear it in a pull request with a docs: or chore: subject (a feat:, fix: or perf: subject asks for a new fragment):\n"+
			"  - if the section of that release already holds the fragment's bullet, delete the file;\n"+
			"  - if it does not (the metadata pull request was closed, or the section was written without it), file the\n"+
			"    bullet under the section [T] of that release and delete the file in the SAME pull request: a delete-only\n"+
			"    pull request is refused by the changelog gate when the bullet is not in the section.", left)
	}
}

// TestLeftBehindFragments pins the backstop on a fixture repository.
func TestLeftBehindFragments(t *testing.T) {
	root := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false"}, args...)...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	write := func(rel, text string) {
		t.Helper()
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	changelog := func(sections ...string) {
		t.Helper()
		text := "# c\n\n## [Unreleased]\n"
		for _, v := range sections {
			text += "\n## [" + v + "] - 2026-01-01\n"
		}
		write("CHANGELOG.md", text)
	}
	release := func(tag string) {
		t.Helper()
		write("rel.txt", tag)
		run("add", "-A")
		run("commit", "-q", "-m", tag)
		run("tag", tag)
	}
	run("init", "-q", "-b", "main")
	changelog()
	release("v0.1.0")
	write("changelog.d/stuck.fixed.md", "- **Stuck.** t\n")
	write("changelog.d/README.md", "- **Readme prose.** t\n")
	if err := os.Symlink("stuck.fixed.md", filepath.Join(root, "changelog.d", "link.fixed.md")); err != nil {
		t.Fatal(err)
	}
	release("v0.2.0") // takes stuck.fixed.md; its section gets filed, the file is left behind
	release("v0.3.0") // the fragment is byte-identical here too: reported once, for v0.2.0
	write("changelog.d/fresh.fixed.md", "- **Fresh.** t\n")
	release("v0.4.0") // takes fresh.fixed.md; its metadata pull request is pending
	run("tag", "foo-stray")
	run("tag", "v0.6.0-rc1")

	// no section yet for any release: every released fragment is merely pending, however old
	if got := leftBehindFragments(t, root); len(got) != 0 {
		t.Fatalf("a release without a section is pending, not left behind: %v", got)
	}
	// v0.2.0's section exists and stuck.fixed.md is still here, as are 2 releases after it (no grace count)
	// the bytes are at v0.2.0 (section exists) and at v0.3.0 (none): the FIRST tag decides, so it is reported
	changelog("0.2.0")
	run("add", "-A")
	run("commit", "-q", "-m", "section of 0.2.0 only")
	if got := leftBehindFragments(t, root); len(got) != 1 || !strings.Contains(got[0], "stuck.fixed.md: released in v0.2.0, whose section") {
		t.Fatalf("the first tag with the bytes decides: %v", got)
	}
	changelog("0.3.0", "0.2.0") // v0.3.0 holds the same bytes and has a section too: still reported once, for v0.2.0
	run("add", "-A")
	run("commit", "-q", "-m", "sections of 0.2.0 and 0.3.0")
	got := leftBehindFragments(t, root)
	if len(got) != 1 || got[0] != "changelog.d/stuck.fixed.md: released in v0.2.0, whose section is already in CHANGELOG.md" {
		t.Fatalf("want exactly the stuck fragment, once, for v0.2.0: %v", got)
	}
	// the pending release's own fragment, the symlink and the README are never reported
	changelog("0.4.0", "0.3.0", "0.2.0")
	run("add", "-A")
	run("commit", "-q", "-m", "section of 0.4.0")
	got = leftBehindFragments(t, root)
	if len(got) != 2 || !strings.Contains(got[0], "fresh.fixed.md: released in v0.4.0") || !strings.Contains(got[1], "stuck.fixed.md") {
		t.Fatalf("want fresh (v0.4.0) and stuck (v0.2.0): %v", got)
	}
	for _, g := range got {
		if strings.Contains(g, "link") || strings.Contains(g, "README") {
			t.Errorf("unexpected: %s", g)
		}
	}
	// once removed, nothing is reported
	run("rm", "-q", "changelog.d/stuck.fixed.md", "changelog.d/fresh.fixed.md")
	run("commit", "-q", "-m", "remove them")
	if got := leftBehindFragments(t, root); len(got) != 0 {
		t.Errorf("removed fragments are not left behind: %v", got)
	}

	// The abandoned metadata pull request: R1 (takes lost.fixed.md), R2, then the sections of R2, R3, R4 and
	// R5 merge but R1's never does. One or two newer sections are tolerated, the third is not.
	write("changelog.d/lost.fixed.md", "- **Lost.** t\n")
	release("v0.10.0") // R1
	release("v0.11.0")
	release("v0.12.0")
	release("v0.13.0")
	release("v0.14.0")
	changelog("0.11.0", "0.12.0")
	run("add", "-A")
	run("commit", "-q", "-m", "two newer sections")
	if got := leftBehindFragments(t, root); len(got) != 0 {
		t.Fatalf("two newer sections are tolerated: %v", got)
	}
	changelog("0.11.0", "0.12.0", "0.13.0")
	run("add", "-A")
	run("commit", "-q", "-m", "three newer sections")
	got = leftBehindFragments(t, root)
	if len(got) != 1 || !strings.HasPrefix(got[0], "changelog.d/lost.fixed.md: released in v0.10.0, which has no section in CHANGELOG.md although 3 newer releases do: its metadata PR was closed or never merged: restore the section or file it by hand") {
		t.Fatalf("want the abandoned fragment named with its releasing tag: %v", got)
	}
	// the FIRST tag decides, not the previous one: lost.fixed.md is byte-identical at v0.11.0, which has a
	// section, yet it is reported as abandoned for v0.10.0, once
	if len(got) != 1 {
		t.Fatalf("reported once: %v", got)
	}
	// filing it by hand and removing the file clears it
	run("rm", "-q", "changelog.d/lost.fixed.md")
	run("commit", "-q", "-m", "filed by hand")
	if got := leftBehindFragments(t, root); len(got) != 0 {
		t.Errorf("cleared: %v", got)
	}
}
