package main

import (
	"reflect"
	"testing"
)

func TestParseAllowlistSkipsCommentsAndOverrides(t *testing.T) {
	raw := `# header
cache.bloom_ttl

  query.file_workers  
override helm-values:query.file_workers 8 (code 64) — reason
stats.*
`
	got := parseAllowlist(raw)
	want := map[string]bool{"cache.bloom_ttl": true, "query.file_workers": true, "stats.*": true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("parseAllowlist = %v, want %v", got, want)
	}
}

func TestAllowedHonoursSectionWildcards(t *testing.T) {
	allow := map[string]bool{"stats.*": true, "cache.bloom_ttl": true}
	for path, want := range map[string]bool{
		"stats.enabled":   true,
		"cache.bloom_ttl": true,
		"cache.page_ttl":  false,
		"statsx.enabled":  false,
	} {
		if got := allowed(path, allow); got != want {
			t.Errorf("allowed(%q) = %v, want %v", path, got, want)
		}
	}
}
