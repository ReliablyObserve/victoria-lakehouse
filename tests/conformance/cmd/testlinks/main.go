// Command testlinks is the registry gate for tests: every Test or Fuzz
// function a PR adds must be linked from a registry row or feature, and no
// reference to a removed or renamed test may be left behind.
//
//	testlinks -base <merge-base rev> [-repo .] [-head HEAD] [-product <reason>]
//	testlinks -report-unlinked        # calibration: how many existing tests are unlinked
//
// Exit 0 when the gate holds, 1 when it does not, 2 on a tool error.
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

// registryChanges loads the rows and features at both revisions, prints the
// summary (stdout and the job summary), and returns the ids of the entries the
// PR changed in a way that is more than prose.
func registryChanges(repo, base, head string, show func(string) func(string) []byte) ([]string, error) {
	var sets []registry.ChangeSet
	var substantive []string
	for _, kind := range []struct{ name, dir string }{{"rows", rowsDir}, {"features", featuresDir}} {
		b, err := entries(repo, base, kind.dir, show)
		if err != nil {
			return nil, err
		}
		h, err := entries(repo, head, kind.dir, show)
		if err != nil {
			return nil, err
		}
		added, changed, removed := registry.ChangedEntries(b, h)
		sets = append(sets, registry.ChangeSet{Kind: kind.name, Added: added, Changed: changed, Removed: removed})
		substantive = append(substantive, registry.SubstantiveChanges(b, h)...)
	}
	summary := registry.SummarizeChanges(sets)
	fmt.Print(summary)
	if path := os.Getenv("GITHUB_STEP_SUMMARY"); path != "" {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
		if err != nil {
			return nil, err
		}
		_, werr := f.WriteString(summary + "\n")
		if cerr := f.Close(); werr == nil {
			werr = cerr
		}
		if werr != nil {
			return nil, werr
		}
	}
	return substantive, nil
}

func run() int {
	repo := flag.String("repo", ".", "repository root")
	base := flag.String("base", "", "merge-base revision the PR is compared against")
	head := flag.String("head", "HEAD", "head revision")
	report := flag.Bool("report-unlinked", false, "report existing unlinked tests and exit")
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

	out, err := git(*repo, "diff", "--no-renames", "--name-only", *base, *head)
	if err != nil {
		fmt.Fprintln(os.Stderr, "testlinks: git diff:", err)
		return 2
	}
	changed := strings.Fields(string(out))
	show := func(rev string) func(string) []byte {
		return func(p string) []byte {
			b, err := git(*repo, "show", rev+":"+p)
			if err != nil {
				return nil
			}
			return b
		}
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
	failed := false
	var missing []registry.AddedTest
	for _, t := range added {
		if !registry.Linked(t, refs) {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		failed = true
		fmt.Println("::error::this PR adds tests that no conformance registry row or feature references")
		for _, t := range missing {
			fmt.Printf("  %s#%s\n", t.File, t.Name)
		}
		fmt.Println("add each to `refs.tests` of the row it proves (tests/conformance/registry/rows/) or to `tests:` of its feature")
		fmt.Println("(tests/conformance/registry/features/), as `path/to/file_test.go#TestName`; see tests/conformance/README.md, 'Linking tests'")
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
		failed = true
		fmt.Println("::error::this PR weakens a parity lock (only the owner can allow that: label registry-exempt + 'Registry: none — <reason>' in the PR body)")
		for _, w := range pv.Weakenings {
			fmt.Println("  " + w)
		}
	}
	if len(pv.Problems) > 0 {
		failed = true
		fmt.Println("::error::this is a parity-fix PR but it does not ship its locks")
		for _, t := range pv.Triggers {
			fmt.Println("  parity fix because: " + t)
		}
		for _, p := range pv.Problems {
			fmt.Println("  missing: " + p)
		}
		fmt.Println("see tests/conformance/README.md, 'Parity fixes ship locks'")
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
		failed = true
		fmt.Println("::error::this PR changes a test file that a registry lock references so that it may stop running (only the owner can allow that)")
		for _, w := range lw {
			fmt.Println("  " + w)
		}
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
		failed = true
		fmt.Println("::error::this PR changes the TestMain of a package that holds a registry lock (only the owner can allow that)")
		for _, w := range tm {
			fmt.Println("  " + w)
		}
	}
	substantive, err := registryChanges(*repo, *base, *head, show)
	if err != nil {
		fmt.Fprintln(os.Stderr, "testlinks: registry summary:", err)
		return 2
	}
	if *product != "" && len(substantive) == 0 {
		failed = true
		fmt.Printf("::error::this PR changes product behaviour (%s) but makes no real content change under tests/conformance/registry/rows/ or tests/conformance/registry/features/\n", *product)
		fmt.Println("  title, notes, description, highlight, differ_note and refs.doc edits are prose and do not count; comment-only and whitespace edits do not either.")
		fmt.Println("  add or change a row or feature (its refs, targets, compare, request ...) that describes the new behaviour.")
		fmt.Println("  see tests/conformance/README.md, section 'Registry gate on every PR'.")
		fmt.Println("  a PR with genuinely nothing to cover is exempted by the owner only: label 'registry-exempt' plus a 'Registry: none — <reason>' line in the PR body.")
	}
	if stale := registry.StaleRefsBlob(show(*head), refs, changed); len(stale) > 0 {
		failed = true
		fmt.Println("::error::registry references point at tests this PR removed, renamed or moved")
		for _, s := range stale {
			fmt.Println("  " + s)
		}
		fmt.Println("update or drop the reference (tests/conformance/README.md, 'Linking tests')")
	}
	if failed {
		return 1
	}
	fmt.Printf("testlinks OK (%d tests added, all linked; parity-fix PR: %v)\n", len(added), pv.Fix)
	return 0
}
