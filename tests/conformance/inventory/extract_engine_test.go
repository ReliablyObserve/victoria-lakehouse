package inventory

import "testing"

func has(items []Item, kind, name string) bool {
	for _, it := range items {
		if it.Kind == kind && it.Name == name {
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
	items, err := ExtractFlags("testdata/mini-vl", []string{"app/vlselect"}, map[string]bool{"app/vlselect": false})
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
	_, err := ExtractFlags("testdata/nonexistent", []string{"app/vlselect"}, map[string]bool{"app/vlselect": false})
	if err == nil {
		t.Fatal("expected error for missing directory")
	}
}

func TestExtractFlags_Linked(t *testing.T) {
	items, err := ExtractFlags("testdata/mini-vl", []string{"app/vlselect"}, map[string]bool{"app/vlselect": true})
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range items {
		if !it.Linked {
			t.Fatalf("app/vlselect marked as linked should have Linked=true for %v", it)
		}
	}
}

func TestExtractFlags_VLInsert(t *testing.T) {
	items, err := ExtractFlags("testdata/mini-vl", []string{"app/vlinsert"}, map[string]bool{"app/vlinsert": false})
	if err != nil {
		t.Fatal(err)
	}
	if !has(items, "flag", "logLevel") {
		t.Fatalf("logLevel flag missing from vlinsert subdirectory: %v", items)
	}
}
