package schema

import (
	"encoding/json"
	"testing"
)

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

// TestBloomValueColumns_AccessorsReadNamedField is the correctness half the parity
// guards above cannot see: those check only that the right NAMES exist, not that
// each Get accessor reads the right FIELD. A copy-paste slip like
// {"host.name", func(r) { return r.K8sPodName }} passes parity yet would feed the
// WRONG column's values into _bloom.bin — the exact silent-pruning bug this file
// exists to prevent. Each bloom column name equals its field's json tag, so seeding
// only that field by name and asserting the accessor echoes it pins accessor↔field.
func TestBloomValueColumns_AccessorsReadNamedField_Logs(t *testing.T) {
	for _, c := range LogBloomValueColumns {
		var r LogRow
		seed := c.Name + "-v"
		if err := json.Unmarshal([]byte(`{"`+c.Name+`":"`+seed+`"}`), &r); err != nil {
			t.Fatalf("seed %q: %v", c.Name, err)
		}
		if got := c.Get(&r); got != seed {
			t.Errorf("LogBloomValueColumns[%q].Get read %q, want %q — accessor reads a field not tagged %q", c.Name, got, seed, c.Name)
		}
	}
}

func TestBloomValueColumns_AccessorsReadNamedField_Traces(t *testing.T) {
	for _, c := range TraceBloomValueColumns {
		var r TraceRow
		seed := c.Name + "-v"
		if err := json.Unmarshal([]byte(`{"`+c.Name+`":"`+seed+`"}`), &r); err != nil {
			t.Fatalf("seed %q: %v", c.Name, err)
		}
		if got := c.Get(&r); got != seed {
			t.Errorf("TraceBloomValueColumns[%q].Get read %q, want %q — accessor reads a field not tagged %q", c.Name, got, seed, c.Name)
		}
	}
}
