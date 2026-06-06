package compaction

import (
	"reflect"
	"sort"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestMergeFileLabels_Union pins the compactor's label-propagation
// invariant: every (field, value) carried by ANY input file must
// surface on the merged output. Regressing this back to "no labels
// on compacted output" silently re-introduces the ~80% undercount
// on field-equality filters like service.name:="api-gateway" — the
// inverted label index has no entry for the compacted file, the
// fast path skips it, and the row scan never sees its data.
func TestMergeFileLabels_Union(t *testing.T) {
	files := []manifest.FileInfo{
		{
			Key: "a",
			Labels: map[string][]string{
				"service.name":       {"api-gateway", "payment-service"},
				"k8s.namespace.name": {"production"},
			},
		},
		{
			Key: "b",
			Labels: map[string][]string{
				"service.name":       {"order-service"},
				"k8s.namespace.name": {"staging"},
			},
		},
		{
			Key:    "c-unlabeled",
			Labels: nil,
		},
	}

	got := mergeFileLabels(files)

	want := map[string][]string{
		"service.name":       {"api-gateway", "order-service", "payment-service"},
		"k8s.namespace.name": {"production", "staging"},
	}

	if len(got) != len(want) {
		t.Fatalf("merged fields = %v, want %v", got, want)
	}
	for field, vals := range got {
		sort.Strings(vals)
		got[field] = vals
	}
	for field, vals := range want {
		sort.Strings(vals)
		want[field] = vals
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("merged labels mismatch:\n got=%v\nwant=%v", got, want)
	}
}

// TestMergeFileLabels_AllNilInputs_ReturnsNil keeps the contract
// strict: when nothing came in, nothing comes out — the manifest's
// rebuildIndex/indexFileLabels both treat a nil map as "this file
// is not in the inverted index" so the row-level filter takes the
// conservative include-and-let-the-match-decide path. Returning
// an empty (non-nil) map here would still index nothing but would
// burn a heap allocation per compacted file.
func TestMergeFileLabels_AllNilInputs_ReturnsNil(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "a", Labels: nil},
		{Key: "b", Labels: nil},
	}
	if got := mergeFileLabels(files); got != nil {
		t.Errorf("mergeFileLabels with all-nil inputs returned %v, want nil", got)
	}
}

// TestMergeFileLabels_EmptyInput_ReturnsNil pins the zero-input
// edge case so callers never see a non-nil empty map.
func TestMergeFileLabels_EmptyInput_ReturnsNil(t *testing.T) {
	if got := mergeFileLabels(nil); got != nil {
		t.Errorf("mergeFileLabels(nil) = %v, want nil", got)
	}
	if got := mergeFileLabels([]manifest.FileInfo{}); got != nil {
		t.Errorf("mergeFileLabels([]) = %v, want nil", got)
	}
}

// TestMergeFileLabels_DeduplicatesAcrossInputs guards against
// per-field value bloat when multiple inputs carry overlapping
// label sets — common for compaction output where most inputs
// share the same small label vocabulary.
func TestMergeFileLabels_DeduplicatesAcrossInputs(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "a", Labels: map[string][]string{"service.name": {"x", "y"}}},
		{Key: "b", Labels: map[string][]string{"service.name": {"y", "z"}}},
		{Key: "c", Labels: map[string][]string{"service.name": {"x", "z"}}},
	}
	got := mergeFileLabels(files)
	if vals, ok := got["service.name"]; !ok {
		t.Fatalf("service.name missing from merged: %v", got)
	} else {
		sort.Strings(vals)
		if !reflect.DeepEqual(vals, []string{"x", "y", "z"}) {
			t.Errorf("service.name values = %v, want [x y z] dedup'd", vals)
		}
	}
}
