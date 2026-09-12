package registry

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDir_Valid(t *testing.T) {
	reg, err := LoadDir("testdata/valid")
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if len(reg.Rows) != 2 || reg.ByID["lh.stats.overview.schema"] == nil {
		t.Fatalf("unexpected rows: %+v", reg.Rows)
	}
	// Verify ordering: native first
	if reg.Rows[0].ID != "vl.select.query.wildcard" {
		t.Fatalf("expected native row first, got %s", reg.Rows[0].ID)
	}
	// Verify ByID references point to rows at their indices
	for i, r := range reg.Rows {
		if reg.ByID[r.ID] != &reg.Rows[i] {
			t.Fatalf("ByID[%s] does not reference rows[%d]", r.ID, i)
		}
	}
	if n := reg.Native(); len(n) != 1 || n[0].ID != "vl.select.query.wildcard" {
		t.Fatalf("Native(): %+v", n)
	}
	keys := reg.UpstreamKeys()
	if ids := keys["route:/select/logsql/query"]; len(ids) != 1 {
		t.Fatalf("UpstreamKeys: %v", keys)
	}
}

func TestLoadDir_DuplicateID(t *testing.T) {
	_, err := LoadDir("testdata/invalid")
	if err == nil || !strings.Contains(err.Error(), "duplicate id") {
		t.Fatalf("want duplicate id error, got %v", err)
	}
}

func TestLoadDir_Missing(t *testing.T) {
	if _, err := LoadDir("testdata/nope"); err == nil {
		t.Fatal("want error for missing dir")
	}
}

func TestLoadDir_NotADirectory(t *testing.T) {
	_, err := LoadDir("testdata/valid/a.yaml")
	if err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("want 'not a directory' error, got %v", err)
	}
}

func TestLoadDir_BadYAML(t *testing.T) {
	_, err := LoadDir("testdata/badyaml")
	if err == nil || !strings.Contains(err.Error(), "registry invalid:") || !strings.Contains(err.Error(), "mapping.yaml") {
		t.Fatalf("want registry invalid with mapping.yaml, got %v", err)
	}
}

func TestLoadDir_OrderNativeShimAddition(t *testing.T) {
	reg, err := LoadDir("testdata/order")
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	// Expected order: native, shim, addition (regardless of walk/id order)
	expectedIDs := []string{
		"vl.select.c_third.basic", // native
		"lh.shim.b_second.x",      // lh-shim
		"lh.stats.a_first.schema", // lh-addition
	}
	if len(reg.Rows) != len(expectedIDs) {
		t.Fatalf("expected %d rows, got %d", len(expectedIDs), len(reg.Rows))
	}
	for i, expectedID := range expectedIDs {
		if reg.Rows[i].ID != expectedID {
			t.Fatalf("rows[%d]: expected %s, got %s", i, expectedID, reg.Rows[i].ID)
		}
	}
}

func TestLoadDir_MultiFileErrorsAggregated(t *testing.T) {
	_, err := LoadDir("testdata/multi")
	if err == nil {
		t.Fatal("expected error for multi-file errors")
	}
	errMsg := err.Error()
	if !strings.Contains(errMsg, "registry invalid:") {
		t.Fatalf("error should contain 'registry invalid:', got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "a.yaml") || !strings.Contains(errMsg, "b.yaml") {
		t.Fatalf("error should mention both a.yaml and b.yaml, got: %s", errMsg)
	}
	// Verify entries are sorted (a.yaml before b.yaml)
	aIdx := strings.Index(errMsg, "a.yaml")
	bIdx := strings.Index(errMsg, "b.yaml")
	if aIdx < 0 || bIdx < 0 || aIdx > bIdx {
		t.Fatalf("entries not sorted (a.yaml before b.yaml): %s", errMsg)
	}
}

func TestLoadDir_UnknownKeyRejected(t *testing.T) {
	_, err := LoadDir("testdata/typo")
	if err == nil {
		t.Fatal("expected error for unknown key")
	}
	if !strings.Contains(err.Error(), "pendng") {
		t.Fatalf("error should mention the typo 'pendng', got: %s", err.Error())
	}
}

func TestLoadDir_MultiDocumentYAML(t *testing.T) {
	reg, err := LoadDir("testdata/multidoc")
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if len(reg.Rows) != 2 {
		t.Fatalf("expected 2 rows from multi-document file, got %d", len(reg.Rows))
	}
	// First document should be native, second should be lh-addition
	if reg.Rows[0].Origin != OriginNative {
		t.Fatalf("rows[0]: expected native origin, got %s", reg.Rows[0].Origin)
	}
	if reg.Rows[1].Origin != OriginLHAddition {
		t.Fatalf("rows[1]: expected lh-addition origin, got %s", reg.Rows[1].Origin)
	}
}

// TestLoadDir_WalkErrorWrappedOnce proves a filesystem-walk error (here: an
// unreadable subdirectory) is wrapped with "registry dir %q: ..." exactly
// once. The walk callback used to wrap it too, so the outer wrap around
// filepath.WalkDir's return value doubled the prefix into "registry dir %q:
// registry dir %q: ...".
func TestLoadDir_WalkErrorWrappedOnce(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("running as root: permission bits do not block directory reads")
	}
	dir := t.TempDir()
	unreadable := filepath.Join(dir, "unreadable")
	if err := os.Mkdir(unreadable, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(unreadable, "a.yaml"), []byte("[]"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unreadable, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(unreadable, 0o755) })

	_, err := LoadDir(dir)
	if err == nil {
		t.Fatal("expected an error from the unreadable subdirectory")
	}
	msg := err.Error()
	wantPrefix := fmt.Sprintf("registry dir %q:", dir)
	if n := strings.Count(msg, "registry dir "); n != 1 {
		t.Fatalf(`expected "registry dir " to appear exactly once, got %d in: %s`, n, msg)
	}
	if !strings.HasPrefix(msg, wantPrefix) {
		t.Fatalf("expected error to start with %q, got: %s", wantPrefix, msg)
	}
}
