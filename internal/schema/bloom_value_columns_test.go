package schema

import "testing"

// bloomValueExclusions are HasBloom columns deliberately NOT value-extracted to
// the partition _bloom.bin. See bloom_value_columns.go: span_id is unique per span
// (~1.25 TB/PB of _bloom.bin), its Parquet row-group bloom still covers the rare
// span-only lookup, and its distinct count survives via the tap-fed catalog HLL.
var bloomValueExclusions = map[string]bool{"span_id": true}

// TestBloomValueColumns_MatchSchema_Logs / _Traces are the drift guards: the
// value-extraction SoT must equal the schema's HasBloom set minus the documented
// exclusions. Adding HasBloom to a column in the registry without a value accessor
// here (or vice versa) fails the build — the inconsistency that left span_id /
// k8s.* with Parquet blooms but no _bloom.bin file-level pruning.
func TestBloomValueColumns_MatchSchema_Logs(t *testing.T) {
	got := map[string]bool{}
	for _, c := range LogBloomValueColumns {
		got[c.Name] = true
	}
	assertBloomValueParity(t, LogBloomColumns(), got)
}

func TestBloomValueColumns_MatchSchema_Traces(t *testing.T) {
	got := map[string]bool{}
	for _, c := range TraceBloomValueColumns {
		got[c.Name] = true
	}
	assertBloomValueParity(t, TraceBloomColumns(), got)
}

func assertBloomValueParity(t *testing.T, schemaBloom []string, sot map[string]bool) {
	t.Helper()
	want := map[string]bool{}
	for _, c := range schemaBloom {
		want[c] = true
	}
	for c := range want {
		if bloomValueExclusions[c] {
			if sot[c] {
				t.Errorf("%q is in bloomValueExclusions yet also has a value accessor — pick one", c)
			}
			continue
		}
		if !sot[c] {
			t.Errorf("schema HasBloom column %q has no value accessor: its _bloom.bin entry is missing so it cannot prune at the file level. Add it to {Log,Trace}BloomValueColumns or document it in bloomValueExclusions.", c)
		}
	}
	for c := range sot {
		if !want[c] {
			t.Errorf("value accessor for %q which is not HasBloom in the schema — stale, remove it", c)
		}
	}
}
