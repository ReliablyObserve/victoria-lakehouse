package conformance

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
)

// srcTestRoot resolves the repository root, failing the test rather than
// skipping: the .src files are tracked, so they are always readable.
func srcTestRoot(t *testing.T) string {
	t.Helper()
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatalf("RepoRoot: %v", err)
	}
	return root
}

// allSrcFuncs parses every .src file in every patch directory.
func allSrcFuncs(t *testing.T, root string) map[string][]SrcFunc {
	t.Helper()
	out := map[string][]SrcFunc{}
	for _, d := range SrcPatchDirs {
		paths, err := SrcFilesIn(root, d.Name)
		if err != nil {
			t.Fatalf("glob %s: %v", d.Name, err)
		}
		if len(paths) == 0 {
			t.Fatalf("patches/%s holds no .src files — the guard would pass vacuously", d.Name)
		}
		for _, p := range paths {
			fns, err := ParseSrcFuncs(p)
			if err != nil {
				t.Fatalf("%v", err)
			}
			out[d.Name] = append(out[d.Name], fns...)
		}
	}
	return out
}

// TestSrcFuncsAreClassified requires every top-level function in every .src
// file to declare, in its doc comment, whether it carries upstream code.
//
// This is the property the patches/*.patch files get for free and the .src
// files do not: a patch stops applying when its context moves, a copied
// function keeps compiling. An unmarked function is an unreviewed one.
func TestSrcFuncsAreClassified(t *testing.T) {
	root := srcTestRoot(t)
	byDir := allSrcFuncs(t, root)

	total := 0
	for dir, fns := range byDir {
		for _, f := range fns {
			total++
			switch f.Marker {
			case MarkerCopy, MarkerTypeSet:
				if f.Divergence == "" {
					t.Errorf("patches/%s/%s:%d: %s is marked %s with no %s: note — a copy nobody wrote a reason for is a copy nobody reviewed",
						dir, f.File, f.Line, f.Name, f.Marker, MarkerDivergence)
				}
				if f.Arg == "" {
					t.Errorf("patches/%s/%s:%d: %s: %s: needs an argument", dir, f.File, f.Line, f.Name, f.Marker)
				}
			case MarkerOriginal:
				if f.Arg == "" {
					t.Errorf("patches/%s/%s:%d: %s: %s: needs a reason", dir, f.File, f.Line, f.Name, f.Marker)
				}
			case "":
				t.Errorf("patches/%s/%s:%d: %s carries no upstream marker.\n"+
					"Every function in a .src file compiles inside upstream's own package, so a copy of upstream code here\n"+
					"never fails to build when upstream changes. Add one of:\n"+
					"  // %s: <file inside the upstream tree> <func>   (+ // %s: <what it does not carry over>)\n"+
					"  // %s: <kind>                                    (+ // %s: <what the enumeration is for>)\n"+
					"  // %s: <why this has no upstream counterpart>",
					dir, f.File, f.Line, f.Name,
					MarkerCopy, MarkerDivergence, MarkerTypeSet, MarkerDivergence, MarkerOriginal)
			default:
				t.Errorf("patches/%s/%s:%d: %s: unknown marker %q", dir, f.File, f.Line, f.Name, f.Marker)
			}
		}
	}
	if total == 0 {
		t.Fatal("no .src functions parsed — the guard would pass vacuously")
	}
	t.Logf("classified %d .src functions across %d patch directories", total, len(byDir))
}

// TestSrcCopiesHaveBaselines requires a committed baseline for every marked
// copy and typeset, in every patch directory whose .src file declares it.
func TestSrcCopiesHaveBaselines(t *testing.T) {
	root := srcTestRoot(t)
	byDir := allSrcFuncs(t, root)

	want := map[string]bool{}
	for _, d := range SrcPatchDirs {
		for _, f := range byDir[d.Name] {
			if f.Marker != MarkerCopy && f.Marker != MarkerTypeSet {
				continue
			}
			p := BaselinePath(root, d.Name, f.File, f.Name)
			want[p] = true
			if _, err := os.Stat(p); err != nil {
				t.Errorf("patches/%s/%s: %s is marked %s but has no baseline at %s — run UPDATE_UPSTREAM_COPIES=1 go test ./tests/conformance/ -run TestSrcCopiesMatchVendoredUpstream and review the result",
					d.Name, f.File, f.Name, f.Marker, mustRel(root, p))
			}
		}
	}

	// A baseline nobody claims is a stale review record: it would keep passing
	// after the marker it belongs to was deleted.
	found, err := filepath.Glob(filepath.Join(root, "patches", "upstream-copies", "*", "*.txt"))
	if err != nil {
		t.Fatalf("glob baselines: %v", err)
	}
	for _, p := range found {
		if !want[p] {
			t.Errorf("%s has no marked function claiming it — delete it with the marker it belonged to", mustRel(root, p))
		}
	}
	if len(found) == 0 {
		t.Fatal("no baselines committed — the drift guard would pass vacuously")
	}
	t.Logf("%d baselines, all claimed", len(found))
}

// TestSrcCopiesMatchVendoredUpstream is the drift guard itself: for every
// marked copy, the upstream function as it reads in the vendored tree at the
// current pin must equal the reviewed baseline.
//
// It fails when upstream changed a function a .src file copies, when upstream
// renamed or removed it (which a copy survives — it is a separate function in
// a separate file), and when a pin moved without the copy being re-read.
//
// Set UPDATE_UPSTREAM_COPIES=1 to re-record the baselines after reading the
// upstream change; `git diff patches/upstream-copies/` then shows exactly what
// moved, which is the review.
func TestSrcCopiesMatchVendoredUpstream(t *testing.T) {
	root := srcTestRoot(t)
	byDir := allSrcFuncs(t, root)
	dirs := inventory.DefaultDirs(root)
	pins := map[string]string{
		"VL_VERSION_LOGS":  dirs.VLVersion,
		"VL_COMMIT_TRACES": dirs.VLCommitTraces,
		"VT_VERSION":       dirs.VTVersion,
	}
	update := os.Getenv("UPDATE_UPSTREAM_COPIES") == "1"

	checked := 0
	for _, d := range SrcPatchDirs {
		tree := filepath.Join(root, filepath.FromSlash(d.Tree))
		if _, err := os.Stat(tree); err != nil {
			if os.Getenv("CONFORMANCE_REQUIRE_DEPS") == "1" {
				t.Fatalf("%s missing — run make deps-logs deps-traces deps-vt", d.Tree)
			}
			t.Skipf("%s missing — run make deps-logs deps-traces deps-vt (or set CONFORMANCE_REQUIRE_DEPS=1 to fail)", d.Tree)
		}
		pin := pins[d.PinVar]
		if pin == "" {
			t.Fatalf("Makefile pin %s not readable", d.PinVar)
		}

		for _, f := range byDir[d.Name] {
			var upstreamName, body string
			switch f.Marker {
			case MarkerCopy:
				file, fn, err := f.UpstreamFile()
				if err != nil {
					t.Errorf("%v", err)
					continue
				}
				upstreamName = file + " " + fn
				body, err = ExtractUpstreamFunc(tree, file, fn)
				if err != nil {
					t.Errorf("patches/%s/%s: %s copies %s, which is no longer there at pin %s: %v\n"+
						"Upstream renaming or deleting a copied function does not break the build here — the copy is a separate\n"+
						"function in a separate file. Re-read upstream and either follow the rename or record why the copy stays.",
						d.Name, f.File, f.Name, upstreamName, pin, err)
					continue
				}
			case MarkerTypeSet:
				upstreamName = f.Arg
				set, err := ExtractTypeSet(tree, f.Arg)
				if err != nil {
					t.Errorf("patches/%s/%s: %s: %v", d.Name, f.File, f.Name, err)
					continue
				}
				body = strings.Join(set, "\n")
			default:
				continue
			}

			path := BaselinePath(root, d.Name, f.File, f.Name)
			next := Baseline{Upstream: upstreamName, Tree: d.Tree, Pin: pin, Body: body}
			if update {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatalf("mkdir: %v", err)
				}
				if err := os.WriteFile(path, []byte(FormatBaseline(next)), 0o644); err != nil {
					t.Fatalf("write %s: %v", path, err)
				}
				t.Logf("recorded %s at pin %s", mustRel(root, path), pin)
				checked++
				continue
			}

			have, err := ParseBaseline(path)
			if err != nil {
				t.Errorf("%v", err)
				continue
			}
			checked++
			if have.Upstream != upstreamName {
				t.Errorf("%s: baseline records upstream %q, marker says %q", mustRel(root, path), have.Upstream, upstreamName)
			}
			if have.Pin != pin {
				t.Errorf("%s: baseline was reviewed at pin %s, the Makefile's %s is now %s.\n"+
					"Re-read the upstream function at the new pin, then re-record with UPDATE_UPSTREAM_COPIES=1.",
					mustRel(root, path), have.Pin, d.PinVar, pin)
				continue
			}
			if have.Body != strings.TrimRight(body, "\n") {
				t.Errorf("patches/%s/%s: %s has drifted from %s at pin %s.\n%s\n"+
					"The copy still compiles, which is exactly why this guard exists. Read the upstream change, decide whether\n"+
					"the copy must follow it, update the // %s: note if it must not, then re-record with\n"+
					"UPDATE_UPSTREAM_COPIES=1 go test ./tests/conformance/ -run TestSrcCopiesMatchVendoredUpstream",
					d.Name, f.File, f.Name, upstreamName, pin, LineDiff(have.Body, strings.TrimRight(body, "\n")), MarkerDivergence)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no copies checked — the drift guard would pass vacuously")
	}
	if update {
		t.Errorf("UPDATE_UPSTREAM_COPIES=1 re-recorded %d baselines; review `git diff patches/upstream-copies/` and re-run without it", checked)
		return
	}
	t.Logf("%d copies match their vendored upstream", checked)
}

// TestSrcCopiesAreEqualAcrossVLPatchDirs keeps the two VictoriaLogs patch
// directories' markers identical. scripts/ci/check_patches_equal.sh already
// enforces byte-equality of the .src files themselves; this asserts the
// consequence the drift guard depends on — the same function is marked the
// same way in both, so both pins are really being checked and not just one.
func TestSrcCopiesAreEqualAcrossVLPatchDirs(t *testing.T) {
	root := srcTestRoot(t)
	byDir := allSrcFuncs(t, root)
	index := func(fns []SrcFunc) map[string]string {
		m := map[string]string{}
		for _, f := range fns {
			m[f.File+"."+f.Name] = f.Marker + " " + f.Arg
		}
		return m
	}
	logs, traces := index(byDir["vl-logs"]), index(byDir["vl-traces"])
	keys := map[string]bool{}
	for k := range logs {
		keys[k] = true
	}
	for k := range traces {
		keys[k] = true
	}
	var sorted []string
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		if logs[k] != traces[k] {
			t.Errorf("%s: vl-logs marker %q != vl-traces marker %q", k, logs[k], traces[k])
		}
	}
	if len(sorted) == 0 {
		t.Fatal("no vl-* .src functions parsed")
	}
}

func mustRel(root, p string) string {
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return p
	}
	return filepath.ToSlash(rel)
}
