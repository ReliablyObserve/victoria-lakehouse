package inventory

import (
	"sort"
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
	want := []string{"/delete/run_task", "/insert/jsonline", "/insert/loki/", "/insert/loki/api/v1/push",
		"/select/buildinfo", "/select/logsql/hits", "/select/logsql/query", "/select/tenant_ids", "/select/vmalert/"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
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
	for _, w := range []string{"/select/jaeger/api/services", "/select/jaeger/api/traces/", "/select/tempo/api/search",
		"/select/tempo/api/v2/search/tags", "/select/tempo/api/traces/", "/insert/native", "/insert/opentelemetry/"} {
		if sort.SearchStrings(got, w) >= len(got) || got[sort.SearchStrings(got, w)] != w {
			t.Fatalf("missing %s in %v", w, got)
		}
	}
}

func TestExtractVLRoutes_MissingDir(t *testing.T) {
	if _, err := ExtractVLRoutes("testdata/does-not-exist"); err == nil {
		t.Fatal("want error")
	}
}
