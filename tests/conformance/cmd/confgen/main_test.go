package main

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
)

// TestReadExisting_Missing proves a missing file is reported as "no bytes,
// no error" — the existing behavior a caller relies on to treat it as
// stale (mismatching any non-empty want) rather than a hard failure.
func TestReadExisting_Missing(t *testing.T) {
	data, err := readExisting(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err != nil {
		t.Fatalf("missing file must not be an error, got: %v", err)
	}
	if data != nil {
		t.Fatalf("missing file must read as nil, got: %v", data)
	}
}

// TestReadExisting_Present proves a real file's bytes are returned as-is.
func TestReadExisting_Present(t *testing.T) {
	p := filepath.Join(t.TempDir(), "present.yaml")
	if err := os.WriteFile(p, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	data, err := readExisting(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("got %q, want %q", data, "hello")
	}
}

// TestReadExisting_OtherErrorSurfaces proves that an error other than
// "file does not exist" (here: EISDIR, from pointing readExisting at a
// directory) is returned to the caller rather than silently discarded —
// the bug this test guards against would have misreported a permission
// error or EISDIR the same way as a simple "not written yet" file.
func TestReadExisting_OtherErrorSurfaces(t *testing.T) {
	dir := t.TempDir()
	_, err := readExisting(dir)
	if err == nil {
		t.Fatal("reading a directory must return an error, got nil")
	}
	if os.IsNotExist(err) {
		t.Fatalf("a directory's read error must not look like os.IsNotExist: %v", err)
	}
}

// TestBuildPlan_RealRepo derives every generated document from the real
// repository: the four documents must be planned, the feature gate must be
// clean, and a second derivation must produce identical bytes (byte-stability
// is what makes `-check` a reliable gate rather than a source of churn).
func TestBuildPlan_RealRepo(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	p, err := buildPlan(root)
	if err != nil {
		if os.Getenv("CONFORMANCE_REQUIRE_DEPS") == "1" {
			t.Fatal(err)
		}
		t.Skipf("cannot build the plan (run make deps-logs deps-traces deps-vt): %v", err)
	}

	var names []string
	for _, f := range p.files {
		names = append(names, filepath.Base(f.path))
		if len(f.want) == 0 {
			t.Errorf("%s planned as empty", f.path)
		}
		// Only docs/features.md carries release references; the release
		// workflow's changelog-only PR depends on -check accepting their
		// older, still-true rendering there.
		if isFeaturesDoc := filepath.Base(f.path) == "features.md"; isFeaturesDoc != (f.accepts != nil) {
			t.Errorf("%s: release-reference acceptance wired = %v, want %v", f.path, f.accepts != nil, isFeaturesDoc)
		} else if f.accepts != nil && f.state(f.want) != fileExact {
			t.Errorf("%s: the planned bytes must be exact", f.path)
		}
	}
	for _, want := range []string{"inventory.generated.yaml", "UPSTREAM_COVERAGE.md", "features.md", "README.md"} {
		if !slices.Contains(names, want) {
			t.Errorf("plan is missing %s (have %v)", want, names)
		}
	}
	if p.featureCount == 0 {
		t.Error("the feature catalog is empty")
	}
	if len(p.featureFailures) != 0 {
		t.Errorf("feature gate failures: %v", p.featureFailures)
	}
	if p.featureSummary == "" {
		t.Error("the summary is what CI logs; it must never be empty")
	}

	again, err := buildPlan(root)
	if err != nil {
		t.Fatal(err)
	}
	for i := range p.files {
		if !bytes.Equal(p.files[i].want, again.files[i].want) {
			t.Errorf("%s is not byte-stable across two derivations", p.files[i].path)
		}
	}
}

func TestBuildPlan_BadRoot(t *testing.T) {
	if _, err := buildPlan(t.TempDir()); err == nil {
		t.Fatal("a directory that is not the repo root must fail rather than generate empty documents")
	}
}

func TestGenFileState(t *testing.T) {
	plain := genFile{path: "plain", want: []byte("exact")}
	tolerant := genFile{path: "tolerant", want: []byte("exact"), accepts: func(have []byte) bool {
		return string(have) == "exact" || string(have) == "older but true"
	}}
	cases := []struct {
		name string
		f    genFile
		have string
		want fileState
	}{
		{"identical bytes", plain, "exact", fileExact},
		{"different bytes, no acceptance", plain, "older but true", fileStale},
		{"identical bytes with acceptance", tolerant, "exact", fileExact},
		{"accepted older rendering", tolerant, "older but true", fileAccepted},
		{"rejected by acceptance", tolerant, "wrong", fileStale},
	}
	for _, tc := range cases {
		if got := tc.f.state([]byte(tc.have)); got != tc.want {
			t.Errorf("%s: state = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestCheckFiles drives the -check comparison: a stale file fails the check
// and is named, a file current only through still-true release references
// passes with a note, an exact file is silent, and an unreadable file is an
// error rather than a verdict.
func TestCheckFiles(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	accepts := func(have []byte) bool { return string(have) == "new" || string(have) == "old but true" }
	exact := genFile{path: write("exact.md", "new"), want: []byte("new"), accepts: accepts}
	accepted := genFile{path: write("accepted.md", "old but true"), want: []byte("new"), accepts: accepts}
	stale := genFile{path: write("stale.md", "wrong"), want: []byte("new"), accepts: accepts}
	missing := genFile{path: filepath.Join(dir, "missing.md"), want: []byte("new")}

	var out bytes.Buffer
	isStale, err := checkFiles(&out, []genFile{exact, accepted})
	if err != nil || isStale {
		t.Fatalf("exact + accepted: stale=%v err=%v, want a passing check", isStale, err)
	}
	if !strings.Contains(out.String(), "current: "+accepted.path) || strings.Contains(out.String(), exact.path) {
		t.Errorf("only the accepted file gets a note, got:\n%s", out.String())
	}

	out.Reset()
	isStale, err = checkFiles(&out, []genFile{exact, stale, missing})
	if err != nil || !isStale {
		t.Fatalf("stale + missing: stale=%v err=%v, want a failing check", isStale, err)
	}
	for _, want := range []string{"stale: " + stale.path, "stale: " + missing.path} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("check output missing %q:\n%s", want, out.String())
		}
	}

	if _, err := checkFiles(&out, []genFile{{path: dir, want: []byte("x")}}); err == nil {
		t.Error("an unreadable path must be an error, not a stale or current verdict")
	}
}
