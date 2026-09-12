package inventory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func has(items []Item, kind, name string) bool {
	for _, it := range items {
		if it.Kind == kind && it.Name == name {
			return true
		}
	}
	return false
}

func hasSource(items []Item, kind, name, source string) bool {
	for _, it := range items {
		if it.Kind == kind && it.Name == name && it.Source == source {
			return true
		}
	}
	return false
}

func TestExtractEngine_Fixture(t *testing.T) {
	items, err := ExtractEngine("testdata/mini-vl")
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range [][2]string{{"pipe", "stats"}, {"pipe", "coalesce"}, {"filter", "range"}, {"filter", "in"}, {"stats", "quantile"}, {"stats", "count"}} {
		if !has(items, w[0], w[1]) {
			t.Fatalf("missing %v in %v", w, items)
		}
	}
	if has(items, "pipe", "stats_test") {
		t.Fatal("test files must be ignored")
	}
	if has(items, "filter", "test") {
		t.Fatal("filter_test.go must be ignored")
	}
	if has(items, "pipe", "stats_local") {
		t.Fatal("pipe_stats_local.go variant must be ignored")
	}
	if has(items, "filter", "generic") {
		t.Fatal("filter_generic.go (internal wrapper) must not be surfaced")
	}
	if has(items, "pipe", "pack") {
		t.Fatal("pipe_pack.go (not in parser table) must not be surfaced")
	}
	if has(items, "stats", "json_values_topk") {
		t.Fatal("stats_json_values_topk.go (not in parser table) must not be surfaced")
	}
	// Verify exact source path
	if !hasSource(items, "pipe", "coalesce", "lib/logstorage/pipe_coalesce.go") {
		t.Fatal("pipe coalesce source path incorrect")
	}
}

func TestExtractTraceQL_Fixture(t *testing.T) {
	items, err := ExtractTraceQL("testdata/mini-vt")
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{"rate", "count_over_time", "histogram_over_time"} {
		if !has(items, "traceql", w) {
			t.Fatalf("missing traceql %s in %v", w, items)
		}
	}
}

func TestExtractFlags_Fixture(t *testing.T) {
	items, err := ExtractFlags("testdata/mini-vl", []string{"app/vlselect"}, map[string]bool{"app/vlselect": false}, "vl")
	if err != nil {
		t.Fatal(err)
	}
	if !has(items, "flag", "search.maxConcurrentRequests") || !has(items, "flag", "search.maxQueueDuration") {
		t.Fatalf("flags missing: %v", items)
	}
	for _, it := range items {
		if it.Linked {
			t.Fatalf("vlselect is not linked into LH; got Linked=true for %v", it)
		}
	}
}

func TestExtractEngine_Missing(t *testing.T) {
	_, err := ExtractEngine("testdata/nonexistent")
	if err == nil {
		t.Fatal("expected error for missing directory")
	}
}

func TestExtractTraceQL_Missing(t *testing.T) {
	_, err := ExtractTraceQL("testdata/nonexistent")
	if err == nil {
		t.Fatal("expected error for missing directory")
	}
}

func TestExtractFlags_Missing(t *testing.T) {
	_, err := ExtractFlags("testdata/nonexistent", []string{"app/vlselect"}, map[string]bool{"app/vlselect": false}, "vl")
	if err == nil {
		t.Fatal("expected error for missing directory")
	}
}

func TestExtractFlags_Linked(t *testing.T) {
	items, err := ExtractFlags("testdata/mini-vl", []string{"app/vlselect"}, map[string]bool{"app/vlselect": true}, "vl")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) == 0 {
		t.Fatal("expected items from vlselect")
	}
	for _, it := range items {
		if !it.Linked {
			t.Fatalf("app/vlselect marked as linked should have Linked=true for %v", it)
		}
	}
}

func TestExtractFlags_VLInsert(t *testing.T) {
	items, err := ExtractFlags("testdata/mini-vl", []string{"app/vlinsert"}, map[string]bool{"app/vlinsert": false}, "vl")
	if err != nil {
		t.Fatal(err)
	}
	if !has(items, "flag", "logLevel") {
		t.Fatalf("logLevel flag missing from vlinsert subdirectory: %v", items)
	}
}

func TestExtractFlags_SafeWrappers(t *testing.T) {
	items, err := ExtractFlags("testdata/mini-vt", []string{"app/vtstorage"}, map[string]bool{"app/vtstorage": true}, "vt")
	if err != nil {
		t.Fatal(err)
	}
	if !has(items, "flag", "retentionPeriod") {
		t.Fatalf("retentionPeriod flag from safe wrapper missing: %v", items)
	}
	// The fixture mentions "retentionPeriod" twice — once via the
	// safeRetentionDuration(...) definition (which flagRe matches) and once
	// via a flag.Lookup(...) call (which flagRe does not match at all,
	// since Lookup isn't one of its alternatives) — so it must be extracted
	// exactly once, not zero and not duplicated.
	count := 0
	for _, it := range items {
		if it.Kind == "flag" && it.Name == "retentionPeriod" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected retentionPeriod extracted exactly once, got %d: %v", count, items)
	}
}

func TestExtractFlags_VTInsert(t *testing.T) {
	items, err := ExtractFlags("testdata/mini-vt", []string{"app/vtinsert"}, map[string]bool{"app/vtinsert": true}, "vt")
	if err != nil {
		t.Fatal(err)
	}
	if !has(items, "flag", "insert.maxFieldsPerLine") {
		t.Fatalf("insert.maxFieldsPerLine flag missing from vtinsert subdirectory: %v", items)
	}
}

func TestExtractFlags_VTSelect(t *testing.T) {
	items, err := ExtractFlags("testdata/mini-vt", []string{"app/vtselect/logsql", "app/vtselect/internalselect"}, map[string]bool{"app/vtselect/logsql": false, "app/vtselect/internalselect": false}, "vt")
	if err != nil {
		t.Fatal(err)
	}
	if !has(items, "flag", "search.maxQueryLen") {
		t.Fatalf("search.maxQueryLen flag missing from vtselect/logsql: %v", items)
	}
	if !has(items, "flag", "internalselect.maxConcurrentRequests") {
		t.Fatalf("internalselect.maxConcurrentRequests flag missing from vtselect/internalselect: %v", items)
	}
}

// TestExtractEngine_MissingParserTable proves ExtractEngine surfaces the
// error from parserTableNames when lib/logstorage/pipe.go (used to
// validate which pipe names are real) is missing, instead of silently
// treating it as "no pipes" — a missing/misplaced parser table should
// never be indistinguishable from a legitimately empty pipe list.
func TestExtractEngine_MissingParserTable(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "lib", "logstorage"), 0o755); err != nil {
		t.Fatal(err)
	}
	// lib/logstorage exists (so ExtractEngine's initial ReadDir succeeds)
	// but pipe.go does not, so parserTableNames' os.ReadFile must fail.
	_, err := ExtractEngine(dir)
	if err == nil {
		t.Fatal("expected an error when lib/logstorage/pipe.go is missing")
	}
	if !strings.Contains(err.Error(), "pipe.go") {
		t.Fatalf("expected the error to mention pipe.go, got: %v", err)
	}
}
