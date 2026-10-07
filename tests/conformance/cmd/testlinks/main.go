// Command testlinks is the registry gate for tests: every Test or Fuzz
// function a PR adds must be linked from a registry row or feature, and no
// reference to a removed or renamed test may be left behind.
//
//	testlinks -base <merge-base rev> [-repo .] [-head HEAD]
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

func run() int {
	repo := flag.String("repo", ".", "repository root")
	base := flag.String("base", "", "merge-base revision the PR is compared against")
	head := flag.String("head", "HEAD", "head revision")
	report := flag.Bool("report-unlinked", false, "report existing unlinked tests and exit")
	flag.Parse()

	refs, err := registry.CollectTestRefs(
		filepath.Join(*repo, "tests/conformance/registry/rows"),
		filepath.Join(*repo, "tests/conformance/registry/features"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "testlinks:", err)
		return 2
	}
	if *report {
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
	added := registry.DiffAddedTests(changed, show(*base), show(*head))
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
	// Parity locks: a parity fix ships differential tests and an exact row; a
	// weakening (allowlist entry added, exact row loosened) always fails.
	snap := func(rev string) (registry.ParitySnapshot, error) {
		sn := registry.ParitySnapshot{Rows: map[string]registry.RowLite{}}
		sn.Allowlist = registry.ParseAllowlist(show(rev)("tests/parity/known_failures.txt"))
		sn.Resolved = registry.ParseResolved(show(rev)("docs/parity-and-gaps.md"))
		files, err := git(*repo, "ls-tree", "-r", "--name-only", rev, "--", "tests/conformance/registry/rows")
		if err != nil {
			return sn, err
		}
		for _, f := range strings.Fields(string(files)) {
			if strings.HasSuffix(f, ".yaml") {
				if err := registry.ParseRowsLenient(show(rev)(f), sn.Rows); err != nil {
					return sn, fmt.Errorf("%s at %s: %w", f, rev, err)
				}
			}
		}
		return sn, nil
	}
	bs, err1 := snap(*base)
	hs, err2 := snap(*head)
	if err1 != nil || err2 != nil {
		fmt.Fprintln(os.Stderr, "testlinks: parity snapshot:", err1, err2)
		return 2
	}
	var parityTests []string
	for _, c := range changed {
		if registry.IsParityTestFile(c) {
			parityTests = append(parityTests, c)
		}
	}
	pv := registry.ParityCheck(bs, hs, parityTests)
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
	// The stale check reads the files on disk, so it needs HEAD checked out.
	if stale := registry.StaleRefs(*repo, refs, changed); len(stale) > 0 {
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
