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
	fmt.Printf("testlinks OK (%d tests added, all linked)\n", len(added))
	return 0
}
