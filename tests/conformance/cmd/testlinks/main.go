// Command testlinks is the registry gate for tests: every Test or Fuzz
// function a PR adds must be linked from a registry row or feature, and no
// reference to a removed or renamed test may be left behind.
//
//	testlinks -base <merge-base rev> [-repo .] [-head HEAD] [-product <reason>] [-report-dir <dir>]
//	testlinks -report-unlinked        # calibration: how many existing tests are unlinked
//
// Exit 0 when the gate holds, 1 when it does not, 2 on a tool error.
//
// The hard rules are decided from content alone, with no exemption:
//
//	Rule 1  a product change makes a real registry content change (-product is set);
//	Rule 2  every added Test or Fuzz function is linked, no reference to a removed one is left;
//	Rule 3  parity fixes ship locks, locks are never weakened.
//
// Edits of lock code (files whose change could alter what a lock compares) are not
// failures: they are listed in the report, because the pull request's required
// review covers them. With -report-dir the command writes rules.md (one table row
// per rule), fixes.md (one "how to fix" line per failure) and details.md (lock-affecting
// files, registry changes) for
// scripts/ci/check_registry_touch.sh, which renders the single report.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
)

func main() { os.Exit(run()) }

func git(repo string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = repo
	return cmd.Output()
}

const (
	lockCellsPath = "tests/parity/lock_cells.txt"
	rowsDir       = "tests/conformance/registry/rows"
	featuresDir   = "tests/conformance/registry/features"
)

// listFiles lists the files under dir at rev through git, refusing symlinks and
// submodules: the gate reads blobs only, and a symlinked registry file would make
// the registry say whatever its target says.
func listFiles(repo, rev, dir string) ([]string, error) {
	raw, err := git(repo, "ls-tree", "-r", "-z", rev, "--", dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, rec := range strings.Split(string(raw), "\x00") {
		if rec == "" {
			continue
		}
		meta, path, ok := strings.Cut(rec, "\t")
		if !ok {
			return nil, fmt.Errorf("unexpected ls-tree record %q", rec)
		}
		if strings.HasPrefix(meta, "120000") || strings.HasPrefix(meta, "160000") {
			return nil, fmt.Errorf("%s at %s is a symlink or submodule: registry files must be regular files", path, rev)
		}
		out = append(out, path)
	}
	return out, nil
}

// entries loads every registry entry of a directory at a revision.
func entries(repo, rev, dir string, show func(string) func(string) []byte) (map[string]map[string]any, error) {
	files, err := listFiles(repo, rev, dir)
	if err != nil {
		return nil, err
	}
	out := map[string]map[string]any{}
	for _, f := range files {
		if strings.HasSuffix(f, ".yaml") {
			if err := registry.ParseEntries(show(rev)(f), out); err != nil {
				return nil, fmt.Errorf("%s at %s: %w", f, rev, err)
			}
		}
	}
	return out, nil
}

// headRefs reads every test reference of the registry from the head revision's blobs.
func headRefs(repo, head string, show func(string) func(string) []byte) ([]registry.TestRef, error) {
	var out []registry.TestRef
	for _, dir := range []string{rowsDir, featuresDir} {
		files, err := listFiles(repo, head, dir)
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			if !strings.HasSuffix(f, ".yaml") {
				continue
			}
			refs, err := registry.TestRefsFromYAML(show(head)(f))
			if err != nil {
				return nil, fmt.Errorf("%s: %w", f, err)
			}
			out = append(out, refs...)
		}
	}
	return out, nil
}

// packageFiles lists the .go files directly in dir at rev, regular files only.
func packageFiles(repo, rev string) registry.PackageFiles {
	return func(dir string) []string {
		raw, err := git(repo, "ls-tree", "-z", rev, strings.TrimSuffix(dir, "/")+"/")
		if err != nil {
			return nil
		}
		var out []string
		for _, rec := range strings.Split(string(raw), "\x00") {
			meta, path, ok := strings.Cut(rec, "\t")
			if ok && strings.HasPrefix(meta, "100") && strings.HasSuffix(path, ".go") {
				out = append(out, path)
			}
		}
		return out
	}
}

// treeFiles lists every regular file under prefix (a directory or one file) at rev.
func treeFiles(repo, rev string) registry.TreeFiles {
	return func(prefix string) []string {
		raw, err := git(repo, "ls-tree", "-r", "-z", rev, "--", prefix)
		if err != nil {
			return nil
		}
		var out []string
		for _, rec := range strings.Split(string(raw), "\x00") {
			meta, path, ok := strings.Cut(rec, "\t")
			if ok && strings.HasPrefix(meta, "100") {
				out = append(out, path)
			}
		}
		return out
	}
}

// registryChanges loads the rows and features at both revisions, and returns the
// summary text and the ids of the entries the PR changed in a way that is more than
// prose.
func registryChanges(repo, base, head string, show func(string) func(string) []byte) (string, []string, error) {
	var sets []registry.ChangeSet
	var substantive []string
	for _, kind := range []struct{ name, dir string }{{"rows", rowsDir}, {"features", featuresDir}} {
		b, err := entries(repo, base, kind.dir, show)
		if err != nil {
			return "", nil, err
		}
		h, err := entries(repo, head, kind.dir, show)
		if err != nil {
			return "", nil, err
		}
		added, changed, removed := registry.ChangedEntries(b, h)
		sets = append(sets, registry.ChangeSet{Kind: kind.name, Added: added, Changed: changed, Removed: removed})
		substantive = append(substantive, registry.SubstantiveChanges(b, h)...)
	}
	return registry.SummarizeChanges(sets), substantive, nil
}

// rule is the verdict of one hard rule, for the console and the report.
type rule struct {
	id      string
	title   string
	skipped string   // why the rule does not apply to this PR ("" when it does)
	note    string   // what a passing verdict rests on
	fails   []string // one entry per failure group
	fixes   []string // one line per failure group: how to fix it
	lines   [][]string
	review  int // files the verdict PASS hands to the required review (listed in the report)
}

func (r *rule) verdict() string {
	switch {
	case len(r.fails) > 0:
		return "FAIL"
	case r.skipped != "":
		return "n/a"
	case r.review == 1:
		return "PASS - 1 lock-affecting file needs review (listed below)"
	case r.review > 1:
		return fmt.Sprintf("PASS - %d lock-affecting files need review (listed below)", r.review)
	}
	return "PASS"
}

// fail prints a failure group the way the gate always has (an annotation, the
// offending lines) and records it for the report.
func (r *rule) fail(headline string, lines []string, fix string) {
	// headline and lines can hold author-controlled text (paths, row ids, test names): escaped as
	// workflow-command data / inert log lines, never printed raw
	fmt.Println("::error::" + registry.WorkflowData(headline))
	for _, l := range lines {
		fmt.Println("  " + registry.LogSafe(l))
	}
	fmt.Println("  how to fix: " + fix)
	r.fails = append(r.fails, headline)
	r.fixes = append(r.fixes, fix)
	r.lines = append(r.lines, lines)
}

// maxAnnotations bounds the inline annotations of one kind; the rest is one "... and N more" line.
const maxAnnotations = 10

// maxReportLines bounds the offending lines the report quotes per failure group.
const maxReportLines = 25

func writeReport(dir string, rules []*rule, lockAffecting []string, summary string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	var rows strings.Builder
	for _, r := range rules {
		detail := r.skipped
		if detail == "" {
			detail = r.note
		}
		if len(r.fails) > 0 {
			detail = strings.Join(r.fails, "; ")
		}
		fmt.Fprintf(&rows, "| %s | %s | %s |\n", r.id+": "+r.title, r.verdict(), registry.MDEscape(detail))
	}
	if err := os.WriteFile(filepath.Join(dir, "rules.md"), []byte(rows.String()), 0o600); err != nil {
		return err
	}
	var fixes strings.Builder
	for _, r := range rules {
		for _, f := range r.fixes {
			fmt.Fprintf(&fixes, "- **%s**: %s\n", r.id, f)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "fixes.md"), []byte(fixes.String()), 0o600); err != nil {
		return err
	}
	var d strings.Builder
	var what strings.Builder
	for _, r := range rules {
		for i, f := range r.fails {
			fmt.Fprintf(&what, "**%s**: %s\n\n", r.id, f)
			ls := r.lines[i]
			for k, l := range ls {
				if k == maxReportLines {
					fmt.Fprintf(&what, "- … and %d more (see the job log)\n", len(ls)-maxReportLines)
					break
				}
				fmt.Fprintf(&what, "- `%s`\n", registry.MDEscape(l))
			}
			what.WriteString("\n")
		}
	}
	if what.Len() > 0 {
		d.WriteString("### What failed\n\n" + what.String())
	}
	d.WriteString("### Lock-affecting files (informational, covered by the required review)\n\n")
	if len(lockAffecting) == 0 {
		d.WriteString("none\n\n")
	} else {
		d.WriteString("These files belong to a package that holds a registry lock, or to the lock suites' runtime configuration. The gate lists them so the reviewer sees them; it does not block them.\n\n")
		for _, l := range lockAffecting {
			path, reason, _ := strings.Cut(l, ": ")
			d.WriteString("- `" + registry.MDEscape(path) + "`: " + registry.MDEscape(reason) + "\n")
		}
		d.WriteString("\n")
	}
	d.WriteString(summary + "\n")
	if err := os.WriteFile(filepath.Join(dir, "review-count"), []byte(fmt.Sprintf("%d\n", len(lockAffecting))), 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "details.md"), []byte(d.String()), 0o600)
}

func run() int {
	repo := flag.String("repo", ".", "repository root")
	base := flag.String("base", "", "merge-base revision the PR is compared against")
	head := flag.String("head", "HEAD", "head revision")
	report := flag.Bool("report-unlinked", false, "report existing unlinked tests and exit")
	reportDir := flag.String("report-dir", "", "write rules.md and details.md here for the gate's single report")
	product := flag.String("product", "", "why the PR counts as changing product code (empty: it does not); it then must change a registry row or feature by more than prose")
	flag.Parse()

	if *report {
		refs, err := registry.CollectTestRefs(
			filepath.Join(*repo, "tests/conformance/registry/rows"),
			filepath.Join(*repo, "tests/conformance/registry/features"))
		if err != nil {
			fmt.Fprintln(os.Stderr, "testlinks:", err)
			return 2
		}
		total, un, err := registry.UnlinkedTests(*repo, refs)
		if err != nil {
			fmt.Fprintln(os.Stderr, "testlinks:", err)
			return 2
		}
		pkgs := map[string]int{}
		for _, t := range un {
			pkgs[t.Pkg]++
		}
		fmt.Printf("in-scope Test/Fuzz functions: %d; unlinked: %d (%.1f%%) across %d packages\n",
			total, len(un), 100*float64(len(un))/float64(max(total, 1)), len(pkgs))
		return 0
	}
	if *base == "" {
		fmt.Fprintln(os.Stderr, "testlinks: -base is required")
		return 2
	}

	// NUL-separated: git prints a path with a space unquoted and a non-ASCII path in octal quotes, so
	// splitting the plain output on white space would lose the first and quote the second
	out, err := git(*repo, "diff", "--no-renames", "--name-only", "-z", *base, *head)
	if err != nil {
		fmt.Fprintln(os.Stderr, "testlinks: git diff:", err)
		return 2
	}
	var changed []string
	for _, f := range strings.Split(string(out), "\x00") {
		if f != "" {
			changed = append(changed, f)
		}
	}
	blobs := newBlobReader(*repo)
	defer blobs.Close()
	show := func(rev string) func(string) []byte {
		return func(p string) []byte { return blobs.Show(rev, p) }
	}
	refs, err := headRefs(*repo, *head, show)
	if err != nil {
		fmt.Fprintln(os.Stderr, "testlinks:", err)
		return 2
	}
	added, err := registry.DiffAddedTests(changed, show(*base), show(*head))
	if err != nil {
		fmt.Fprintln(os.Stderr, "testlinks:", err)
		return 2
	}
	r1 := &rule{id: "Rule 1", title: "a product change changes the registry"}
	r2 := &rule{id: "Rule 2", title: "new tests are linked, no stale references"}
	r3 := &rule{id: "Rule 3", title: "parity fixes ship locks, locks are not weakened"}
	if *product == "" {
		r1.skipped = "not a product change"
	}
	var missing []registry.AddedTest
	for _, t := range added {
		if !registry.Linked(t, refs) {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		var lines []string
		for _, t := range missing {
			lines = append(lines, t.File+"#"+t.Name)
		}
		r2.fail("this PR adds tests that no conformance registry row or feature references", lines,
			"add each to `refs.tests` of the row it proves (tests/conformance/registry/rows/) or to `tests:` of its feature (tests/conformance/registry/features/), as `path/to/file_test.go#TestName` (tests/conformance/README.md, 'Linking tests')")
	}

	// Parity locks. The allowlist file is the one the parity workflow hands to
	// the ratchet, read at BOTH revisions: moving or deleting the file, or
	// pointing the workflow elsewhere, would hide entries from a path-based diff.
	const parityWorkflow = ".github/workflows/parity.yaml"
	var weakened []string
	basePath := registry.AllowlistPath(show(*base)(parityWorkflow))
	headPath := registry.AllowlistPath(show(*head)(parityWorkflow))
	switch {
	case basePath == "" || headPath == "":
		weakened = append(weakened, parityWorkflow+" has no --allowlist argument at one of the revisions")
	case basePath != headPath:
		weakened = append(weakened, fmt.Sprintf("the parity workflow's --allowlist changed from %s to %s", basePath, headPath))
	}
	if headPath != "" && show(*head)(headPath) == nil {
		weakened = append(weakened, "the parity allowlist "+headPath+" is missing at head (deleted or renamed)")
	}
	snap := func(rev, allowlist string) (registry.ParitySnapshot, error) {
		sn := registry.ParitySnapshot{Rows: map[string]registry.RowLite{}}
		sn.Allowlist = registry.ParseAllowlist(show(rev)(allowlist))
		sn.Resolved = registry.ParseResolved(show(rev)("docs/parity-and-gaps.md"))
		files, err := listFiles(*repo, rev, rowsDir)
		if err != nil {
			return sn, err
		}
		floors, err := registry.ParseLockCells(show(rev)(lockCellsPath))
		if err != nil {
			return sn, fmt.Errorf("%s at %s: %w", lockCellsPath, rev, err)
		}
		sn.Floors = floors
		for _, f := range files {
			if strings.HasSuffix(f, ".yaml") {
				if err := registry.ParseRowsLenient(show(rev)(f), sn.Rows); err != nil {
					return sn, fmt.Errorf("%s at %s: %w", f, rev, err)
				}
			}
		}
		return sn, nil
	}
	allow := basePath
	if allow == "" {
		allow = "tests/parity/known_failures.txt"
	}
	bs, err1 := snap(*base, allow)
	hs, err2 := snap(*head, allow)
	hs.RefExists = func(ref string) bool { return registry.RefResolves(show(*head), ref) }
	bs.FileTests = func(path string) []string {
		names, err := registry.TestFuncs(show(*base)(path))
		if err != nil {
			return nil
		}
		return names
	}
	if err1 != nil || err2 != nil {
		fmt.Fprintln(os.Stderr, "testlinks: parity snapshot:", err1, err2)
		return 2
	}
	modified, err := registry.ModifiedParityTests(changed, show(*base), show(*head))
	if err != nil {
		fmt.Fprintln(os.Stderr, "testlinks:", err)
		return 2
	}
	pv := registry.ParityCheck(bs, hs, modified)
	pv.Weakenings = append(weakened, pv.Weakenings...)
	if len(pv.Weakenings) > 0 {
		r3.fail("this PR weakens a parity lock", pv.Weakenings,
			"undo the weakening: keep the allowlist entry removed, keep every `expect: pass` row and its test references, keep every cell floor in tests/parity/lock_cells.txt (a renamed lock test keeps at least the old floor); a fix adds cells, it never lowers them")
	}
	if len(pv.Problems) > 0 {
		var lines []string
		for _, t := range pv.Triggers {
			lines = append(lines, "parity fix because: "+t)
		}
		for _, p := range pv.Problems {
			lines = append(lines, "missing: "+p)
		}
		r3.fail("this is a parity-fix PR but it does not ship its locks", lines,
			"add the lock: an `expect: pass` row with an exact compare whose refs.tests names a tests/parity test this PR adds or modifies, plus its floor in tests/parity/lock_cells.txt (tests/conformance/README.md, 'Parity fixes ship locks')")
	}
	// A lock's test file must keep running: no changed build constraint, no new Skip.
	lockFiles := map[string]bool{}
	for _, snapRows := range []map[string]registry.RowLite{bs.Rows, hs.Rows} {
		for _, r := range snapRows {
			if r.Exact() {
				for _, t := range r.Tests {
					lockFiles[strings.SplitN(t, "#", 2)[0]] = true
				}
			}
		}
	}
	var lockList []string
	for f := range lockFiles {
		lockList = append(lockList, f)
	}
	if lw := registry.LockFileWeakenings(lockList, show(*base), show(*head)); len(lw) > 0 {
		r3.fail("this PR changes a test file that a registry lock references so that it may stop running", lw,
			"remove the new t.Skip / build-constraint edit from the lock's test file, or move the test out of the lock's file")
	}
	// A TestMain that exits early silences every test of its package: the packages that
	// hold a lock may not change theirs.
	var mainFiles []string
	dirs := map[string]bool{}
	for _, f := range lockList {
		dirs[filepath.ToSlash(filepath.Dir(f))] = true
	}
	for d := range dirs {
		for _, rev := range []string{*base, *head} {
			files, err := listFiles(*repo, rev, d+"/")
			if err != nil {
				continue // a missing directory has no TestMain
			}
			for _, f := range files {
				if filepath.ToSlash(filepath.Dir(f)) == d {
					mainFiles = append(mainFiles, f)
				}
			}
		}
	}
	if tm := registry.TestMainChanges(mainFiles, show(*base), show(*head)); len(tm) > 0 {
		r3.fail("this PR changes the TestMain of a package that holds a registry lock", tm,
			"revert the TestMain change: a TestMain that can exit early silences every test of its package, locks included")
	}
	// Load-time code: a lock package may not gain an init() or a package-level var (it runs before
	// any test and can exit, skip or repoint shared state). A content rule, hard like TestMain.
	if iv := registry.LockInitVarAdditions(registry.LockTestRefs(bs.Rows), show(*base), show(*head), packageFiles(*repo, *base), packageFiles(*repo, *head)); len(iv) > 0 {
		r3.fail("this PR adds init() or a package-level var to a package that holds a registry lock", iv,
			"move the setup into the test function or a helper func the test calls: a package-level var initializer and init() run before every test of the package, can call os.Exit and can repoint shared state, which would silence the locks")
	}
	// Lock-affecting files: every existing file of a package that holds a lock test, the
	// lock suites' runtime config, and new files there that declare vars, init or TestMain.
	// Informational: the report lists them for the required review; they do not fail.
	lc, lerr := registry.LockCodeChanges(registry.LockTestRefs(bs.Rows), registry.LockTestRefs(hs.Rows),
		show(*base), show(*head), packageFiles(*repo, *base), packageFiles(*repo, *head), treeFiles(*repo, *base), treeFiles(*repo, *head))
	if lerr != nil {
		fmt.Fprintln(os.Stderr, "testlinks: lock code:", lerr)
		return 2
	}
	if len(lc) > 0 {
		fmt.Println("lock-affecting files in this PR (listed in the report, not a failure):")
		var paths []string
		for _, w := range lc {
			fmt.Println("  " + registry.LogSafe(w))
			p, _, _ := strings.Cut(w, ": ")
			paths = append(paths, p)
		}
		// inline in the PR's Files tab: the approval of the required reviewer is the control here
		for _, a := range registry.WorkflowWarnings(paths, "lock-affecting file: the owner's approval is the control", maxAnnotations) {
			fmt.Println(a)
		}
	}
	summary, substantive, err := registryChanges(*repo, *base, *head, show)
	if err != nil {
		fmt.Fprintln(os.Stderr, "testlinks: registry summary:", err)
		return 2
	}
	fmt.Print(summary)
	if *product != "" {
		if len(substantive) == 0 {
			r1.fail(fmt.Sprintf("this PR changes product behaviour (%s) but makes no real content change under tests/conformance/registry/rows/ or tests/conformance/registry/features/", *product),
				[]string{"title, notes, description, highlight, differ_note and refs.doc edits are prose and do not count; comment-only and whitespace edits do not either."},
				"add or change a row or feature (its refs, targets, compare, request ...) that describes the new behaviour (tests/conformance/README.md, 'Registry gate on every PR')")
		}
	}
	if stale := registry.StaleRefsBlob(show(*head), refs, changed); len(stale) > 0 {
		r2.fail("registry references point at tests this PR removed, renamed or moved", stale,
			"update or drop the reference (tests/conformance/README.md, 'Linking tests')")
	}
	r1.note = fmt.Sprintf("%d registry entries changed by more than prose", len(substantive))
	r2.note = fmt.Sprintf("%d tests added, all linked; no reference to a removed test", len(added))
	r3.note = fmt.Sprintf("no lock weakened; parity-fix PR: %v", pv.Fix)
	r3.review = len(lc)
	if *reportDir != "" {
		if err := writeReport(*reportDir, []*rule{r1, r2, r3}, lc, summary); err != nil {
			fmt.Fprintln(os.Stderr, "testlinks: report:", err)
			return 2
		}
	}
	if len(r1.fails)+len(r2.fails)+len(r3.fails) > 0 {
		return 1
	}
	fmt.Printf("testlinks OK (%d tests added, all linked; parity-fix PR: %v)\n", len(added), pv.Fix)
	return 0
}
