package vtstorageadapter

import (
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// The substring filter applies to every value before the limit: a limit that
// came first would drop matching values behind non-matching ones, and the empty
// value (which now takes a slot) would push them out.
func TestValuesFilteredThenLimited(t *testing.T) {
	all := []logstorage.ValueWithHits{{Value: "", Hits: 9}, {Value: "a", Hits: 8}, {Value: "vx", Hits: 3}, {Value: "vy", Hits: 2}}
	fetch := func(limit uint64) ([]logstorage.ValueWithHits, error) {
		if limit == 0 || int(limit) >= len(all) {
			return all, nil
		}
		return all[:limit], nil
	}
	got, err := valuesFilteredThenLimited("v", 1, fetch)
	if err != nil {
		t.Fatal(err)
	}
	if want := []logstorage.ValueWithHits{{Value: "vx", Hits: 0}}; !reflect.DeepEqual(got, want) {
		t.Errorf("filter v limit 1 = %v, want %v (hits zeroed past the limit, as upstream)", got, want)
	}
	if got, _ = valuesFilteredThenLimited("v", 0, fetch); len(got) != 2 {
		t.Errorf("filter v, no limit = %v, want vx and vy", got)
	}
	if got, _ = valuesFilteredThenLimited("zzz", 5, fetch); len(got) != 0 {
		t.Errorf("filter matching nothing = %v, want none", got)
	}
	// No filter: the limit goes to the store untouched.
	var seen uint64
	_, _ = valuesFilteredThenLimited("", 7, func(l uint64) ([]logstorage.ValueWithHits, error) { seen = l; return nil, nil })
	if seen != 7 {
		t.Errorf("limit passed to the store = %d, want 7", seen)
	}
}

// Upstream GetStreamFieldValues with a substring filter: filter, sort by hits
// desc then value, truncate to limit, zero the hits. The higher-hit value is kept.
func TestValuesFilteredThenLimited_KeepsTopHitsThenZeroes(t *testing.T) {
	// what the store's GetStreamFieldValues(limit=0) returns: sorted by hits.
	all := []logstorage.ValueWithHits{{Value: "vy", Hits: 30}, {Value: "vx", Hits: 2}, {Value: "a", Hits: 1}}
	fetch := func(limit uint64) ([]logstorage.ValueWithHits, error) { return all, nil }
	got, err := valuesFilteredThenLimited("v", 1, fetch)
	if err != nil {
		t.Fatal(err)
	}
	if want := []logstorage.ValueWithHits{{Value: "vy", Hits: 0}}; !reflect.DeepEqual(got, want) {
		t.Errorf("stream_field_values filter=v limit=1 = %v, upstream keeps %v (top by hits, then zeroed)", got, want)
	}
}
