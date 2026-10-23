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

// releasedFragmentGrace is how many newer releases a release's metadata pull request may be behind
// before a fragment it should have removed is reported. The serial release protocol keeps at most one
// metadata pull request pending, and a burst of merges can put a second release on top of it; a
// fragment that is still there three releases later was released and never removed.
const releasedFragmentGrace = 3

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

// leftBehindFragments lists the fragments of HEAD that are byte-identical at a release tag more than
// `grace` releases older than the newest one: released, yet never removed by a metadata pull request.
func leftBehindFragments(t *testing.T, root string, grace int) []string {
	t.Helper()
	tagList, _ := git(t, root, "tag")
	var tags []string
	for _, tg := range strings.Fields(tagList) {
		if releaseTagName.MatchString(tg) {
			tags = append(tags, tg)
		}
	}
	sort.Slice(tags, func(i, j int) bool { return semverLess(tags[i], tags[j]) })
	if len(tags) <= grace {
		return nil
	}
	head := fragmentBlobs(t, root, "HEAD")
	if len(head) == 0 {
		return nil
	}
	var out []string
	reported := map[string]bool{}
	for _, tg := range tags[:len(tags)-grace] {
		for path, blob := range fragmentBlobs(t, root, tg) {
			if head[path] == blob && !reported[path] {
				reported[path] = true
				out = append(out, fmt.Sprintf("%s: byte-identical at %s, %d releases older than %s", path, tg,
					len(tags)-1-indexOf(tags, tg), tags[len(tags)-1]))
			}
		}
	}
	sort.Strings(out)
	return out
}

func indexOf(list []string, s string) int {
	for i, v := range list {
		if v == s {
			return i
		}
	}
	return -1
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

// TestNoReleasedFragmentIsLeftBehind is the backstop for a release-metadata pull request that never
// merged (or whose job failed): the release took the fragment, so its CHANGELOG.md section is missing
// and the file would sit in changelog.d/ for good.
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
	for _, left := range leftBehindFragments(t, root, releasedFragmentGrace) {
		t.Errorf("changelog.d/ %s.\n"+
			"  That release took the fragment, but its release-metadata pull request never removed it, so the\n"+
			"  fragment is in no CHANGELOG.md section. Find the pull request \"chore: release metadata for <tag>\":\n"+
			"  merge it, or if it was lost, move the bullet into the section of that release by hand and delete the file.", left)
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
	release := func(tag string) {
		t.Helper()
		write("rel.txt", tag)
		run("add", "-A")
		run("commit", "-q", "-m", tag)
		run("tag", tag)
	}
	run("init", "-q", "-b", "main")
	release("v0.1.0")
	write("changelog.d/stuck.fixed.md", "- **Stuck.** t\n")
	write("changelog.d/README.md", "- **Readme prose.** t\n")
	release("v0.2.0") // takes stuck.fixed.md; its metadata PR never merges
	release("v0.3.0")
	release("v0.4.0")
	write("changelog.d/fresh.fixed.md", "- **Fresh.** t\n")
	release("v0.5.0") // takes fresh.fixed.md; its metadata PR is pending
	run("tag", "foo-stray")
	run("tag", "v0.6.0-rc1")

	got := leftBehindFragments(t, root, 3)
	if len(got) != 1 || !strings.HasPrefix(got[0], "changelog.d/stuck.fixed.md: byte-identical at v0.2.0, 3 releases older than v0.5.0") {
		t.Fatalf("want exactly the stuck fragment, got %v", got)
	}
	if got := leftBehindFragments(t, root, 4); len(got) != 0 {
		t.Errorf("a larger grace tolerates it: %v", got)
	}
	// the pending release's own fragment, the README and stray tags are never reported
	for _, g := range leftBehindFragments(t, root, 1) {
		if strings.Contains(g, "fresh") || strings.Contains(g, "README") {
			t.Errorf("unexpected: %s", g)
		}
	}
	// once the metadata pull request removes it, nothing is reported
	run("rm", "-q", "changelog.d/stuck.fixed.md")
	run("commit", "-q", "-m", "chore: release metadata for v0.2.0")
	if got := leftBehindFragments(t, root, 3); len(got) != 0 {
		t.Errorf("removed fragments are not left behind: %v", got)
	}
}
