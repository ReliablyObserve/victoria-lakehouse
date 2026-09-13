package main

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
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
