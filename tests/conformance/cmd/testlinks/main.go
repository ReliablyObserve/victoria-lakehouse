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

const (
	rowsDir     = "tests/conformance/registry/rows"
	featuresDir = "tests/conformance/registry/features"
)

// entries loads every registry entry of a directory at a revision.
func entries(repo, rev, dir string, show func(string) func(string) []byte) (map[string]map[string]any, error) {
	files, err := git(repo, "ls-tree", "-r", "--name-only", rev, "--", dir)
	if err != nil {
		return nil, err
	}
	out := map[string]map[string]any{}
	for _, f := range strings.Fields(string(files)) {
		if strings.HasSuffix(f, ".yaml") {
			if err := registry.ParseEntries(show(rev)(f), out); err != nil {
				return nil, fmt.Errorf("%s at %s: %w", f, rev, err)
			}
		}
	}
	return out, nil
}

// printRegistryChanges lists the rows and features the PR adds, changes or
// removes, on stdout and in the job summary, so a reviewer sees what the
// registry change actually is.
func printRegistryChanges(repo, base, head string, show func(string) func(string) []byte) error {
	var sb strings.Builder
	sb.WriteString("### Registry changes in this PR\n\n")
	seen := false
	for _, kind := range []struct{ name, dir string }{{"rows", rowsDir}, {"features", featuresDir}} {
		b, err := entries(repo, base, kind.dir, show)
		if err != nil {
			return err
		}
		h, err := entries(repo, head, kind.dir, show)
		if err != nil {
			return err
		}
		added, changed, removed := registry.ChangedEntries(b, h)
		for _, g := range []struct {
			label string
			ids   []string
		}{{"added", added}, {"changed", changed}, {"removed", removed}} {
			if len(g.ids) == 0 {
				continue
			}
			seen = true
			fmt.Fprintf(&sb, "- %s %s (%d): %s\n", kind.name, g.label, len(g.ids), strings.Join(g.ids, ", "))
		}
	}
	if !seen {
		sb.WriteString("none\n")
	}
	fmt.Print(sb.String())
	if path := os.Getenv("GITHUB_STEP_SUMMARY"); path != "" {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = f.WriteString(sb.String() + "\n")
		return err
	}
	return nil
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
		files, err := git(*repo, "ls-tree", "-r", "--name-only", rev, "--", rowsDir)
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
	allow := basePath
	if allow == "" {
		allow = "tests/parity/known_failures.txt"
	}
	bs, err1 := snap(*base, allow)
	hs, err2 := snap(*head, allow)
	if err1 != nil || err2 != nil {
		fmt.Fprintln(os.Stderr, "testlinks: parity snapshot:", err1, err2)
		return 2
	}
	modified := map[string]bool{}
	for _, c := range changed {
		if !registry.IsParityTestFile(c) {
			continue
		}
		names, err := registry.ModifiedTests(show(*base)(c), show(*head)(c))
		if err != nil {
			fmt.Fprintf(os.Stderr, "testlinks: %s: %v\n", c, err)
			return 2
		}
		for _, n := range names {
			modified[c+"#"+n] = true
		}
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
	if err := printRegistryChanges(*repo, *base, *head, show); err != nil {
		fmt.Fprintln(os.Stderr, "testlinks: registry summary:", err)
		return 2
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
