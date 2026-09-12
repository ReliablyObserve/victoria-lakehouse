package inventory

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func names(items []Item, kind string) []string {
	var out []string
	for _, it := range items {
		if it.Kind == kind {
			out = append(out, it.Name)
		}
	}
	sort.Strings(out)
	return out
}

func TestExtractVLRoutes_Fixture(t *testing.T) {
	items, err := ExtractVLRoutes("testdata/mini-vl")
	if err != nil {
		t.Fatal(err)
	}
	got := names(items, "route")
	want := []string{"/delete/run_task", "/insert/datadog/api/v1/validate", "/insert/datadog/api/v2/logs",
		"/insert/jsonline", "/insert/loki/", "/insert/loki/api/v1/push",
		"/internal/force_flush", "/internal/select/hits", "/internal/select/query",
		"/select/buildinfo", "/select/logsql/hits", "/select/logsql/query", "/select/tenant_ids", "/select/vmalert/"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
	// Verify dispatch gate /select/ is not in results
	for _, it := range items {
		if it.Name == "/select/" {
			t.Fatalf("dispatch gate /select/ should be filtered, got: %v", it)
		}
	}
	// Verify multi-value case: both datadog routes extracted
	foundDatadogV1 := false
	foundDatadogV2 := false
	for _, it := range items {
		if it.Name == "/insert/datadog/api/v1/validate" {
			foundDatadogV1 = true
		}
		if it.Name == "/insert/datadog/api/v2/logs" {
			foundDatadogV2 = true
		}
	}
	if !foundDatadogV1 || !foundDatadogV2 {
		t.Fatalf("multi-value case routes not found: v1=%v, v2=%v", foundDatadogV1, foundDatadogV2)
	}
	for _, it := range items {
		if it.Source == "" {
			t.Fatalf("item %v has no source", it)
		}
	}
}

func TestExtractVTRoutes_Fixture(t *testing.T) {
	items, err := ExtractVTRoutes("testdata/mini-vt")
	if err != nil {
		t.Fatal(err)
	}
	got := names(items, "route")
	want := []string{"/insert/native", "/insert/opentelemetry/",
		"/internal/force_merge", "/internal/select/trace",
		"/select/buildinfo", "/select/jaeger/", "/select/jaeger/api/services", "/select/jaeger/api/services/",
		"/select/jaeger/api/traces/", "/select/logsql/hits", "/select/logsql/query", "/select/logsql/tail",
		"/select/tempo/", "/select/tempo/api/search", "/select/tempo/api/traces/", "/select/tempo/api/v2/search/tags",
		"/select/vmui", "/select/vmui/"}
	if len(got) != len(want) {
		t.Fatalf("got %d routes, want %d: got %v want %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}

	// Verify compound condition: /select/jaeger/api/services/ (prefix+suffix combination)
	foundCompound := false
	for _, it := range items {
		if it.Name == "/select/jaeger/api/services/" {
			foundCompound = true
			break
		}
	}
	if !foundCompound {
		t.Fatalf("compound condition route /select/jaeger/api/services/ not found in %v", got)
	}

	// Verify dispatcher routes are present
	for _, dispatcher := range []string{"/select/jaeger/", "/select/tempo/"} {
		found := false
		for _, it := range items {
			if it.Name == dispatcher {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("dispatcher route %s not found in %v", dispatcher, got)
		}
	}

	// Verify /select/tempo/api/traces/ is present with correct source
	for _, it := range items {
		if it.Name == "/select/tempo/api/traces/" && it.Kind == "route" {
			if it.Source != "app/vtselect/traces/tempo/tempo.go" {
				t.Fatalf("/select/tempo/api/traces/ has source %q, want app/vtselect/traces/tempo/tempo.go", it.Source)
			}
			return
		}
	}
	t.Fatalf("/select/tempo/api/traces/ not found in items")
}

func TestExtractVLRoutes_MissingDir(t *testing.T) {
	if _, err := ExtractVLRoutes("testdata/does-not-exist"); err == nil {
		t.Fatal("want error")
	}
}

func TestExtractVLRoutes_UnreadableSubdir(t *testing.T) {
	// Skip if running as root
	if os.Geteuid() == 0 {
		t.Skip("test requires non-root user")
	}

	// Copy testdata/mini-vl to temp directory
	tmpDir := t.TempDir()
	srcDir := "testdata/mini-vl"
	dstDir := filepath.Join(tmpDir, "mini-vl")

	err := filepath.Walk(srcDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(srcDir, path)
		dst := filepath.Join(dstDir, rel)
		if info.IsDir() {
			return os.MkdirAll(dst, 0755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(dst, data, 0644)
	})
	if err != nil {
		t.Fatalf("setup copy: %v", err)
	}

	// Create locked subdirectory
	lockedDir := filepath.Join(dstDir, "app", "vlinsert", "locked")
	if err := os.MkdirAll(lockedDir, 0755); err != nil {
		t.Fatalf("mkdir locked: %v", err)
	}
	lockedFile := filepath.Join(lockedDir, "locked.go")
	if err := os.WriteFile(lockedFile, []byte("package locked\nfunc f() {}"), 0644); err != nil {
		t.Fatalf("write locked file: %v", err)
	}

	// Set up cleanup to restore permissions
	t.Cleanup(func() {
		os.Chmod(lockedDir, 0755)
	})

	// chmod 0000 the directory
	if err := os.Chmod(lockedDir, 0000); err != nil {
		t.Fatalf("chmod locked: %v", err)
	}

	// Extract should fail with walk error
	_, err = ExtractVLRoutes(dstDir)
	if err == nil {
		t.Fatal("want error for unreadable subdir")
	}
	if !strings.Contains(err.Error(), "walk") {
		t.Fatalf("error should mention 'walk', got: %v", err)
	}
}
