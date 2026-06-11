package schema

import "testing"

// TestDedicatedColumns_AccessorsReturnCopies verifies the accessors hand back a
// copy so callers cannot mutate the registry source of truth.
func TestDedicatedColumns_AccessorsReturnCopies(t *testing.T) {
	for _, tc := range []struct {
		name string
		get  func() []FieldMapping
		src  []FieldMapping
	}{
		{"logs", LogDedicatedColumns, logDedicatedColumns},
		{"traces", TraceDedicatedColumns, traceDedicatedColumns},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.get()
			if len(got) != len(tc.src) {
				t.Fatalf("len = %d, want %d", len(got), len(tc.src))
			}
			if len(got) == 0 {
				t.Fatal("expected at least one dedicated column")
			}
			orig := got[0].ParquetColumn
			got[0].ParquetColumn = "MUTATED"
			if tc.src[0].ParquetColumn == "MUTATED" {
				t.Error("accessor returned a view, not a copy — caller mutated the source")
			}
			got[0].ParquetColumn = orig
		})
	}
}

// TestDedicatedColumns_FoldedIntoProfiles verifies init() appended every Tier-1
// column to its signal profile and that they resolve both ways.
func TestDedicatedColumns_FoldedIntoProfiles(t *testing.T) {
	r := NewRegistry(LogsProfile)
	for _, m := range logDedicatedColumns {
		if got := r.ResolveFromParquet(m.ParquetColumn); got == nil {
			t.Errorf("logs: dedicated column %q not resolvable from parquet name", m.ParquetColumn)
		}
		if got := r.ResolveToParquet(m.InternalName); got == nil {
			t.Errorf("logs: dedicated column %q not resolvable from internal name %q", m.ParquetColumn, m.InternalName)
		}
	}
	rt := NewRegistry(TracesProfile)
	for _, m := range traceDedicatedColumns {
		if got := rt.ResolveFromParquet(m.ParquetColumn); got == nil {
			t.Errorf("traces: dedicated column %q not resolvable from parquet name", m.ParquetColumn)
		}
	}
}

// TestDedicatedColumns_TraceInternalNamesPrefixed verifies the traces dedicated
// columns carry the VT stream-tag prefix on their InternalName (so registry
// resolution + label index agree with traceRowToFields' prefixed emission),
// while logs dedicated columns stay bare.
func TestDedicatedColumns_TraceInternalNamesPrefixed(t *testing.T) {
	for _, m := range traceDedicatedColumns {
		switch m.MapColumn {
		case "span.attributes":
			if want := "span_attr:" + m.ParquetColumn; m.InternalName != want {
				t.Errorf("trace span column %q: InternalName = %q, want %q", m.ParquetColumn, m.InternalName, want)
			}
		case "resource.attributes":
			if want := "resource_attr:" + m.ParquetColumn; m.InternalName != want {
				t.Errorf("trace resource column %q: InternalName = %q, want %q", m.ParquetColumn, m.InternalName, want)
			}
		}
	}
	for _, m := range logDedicatedColumns {
		if m.InternalName != m.ParquetColumn {
			t.Errorf("log column %q: InternalName = %q, want bare %q", m.ParquetColumn, m.InternalName, m.ParquetColumn)
		}
	}
}

// TestBloomColumns_DerivedFromHasBloom verifies the bloom set is exactly the
// HasBloom-flagged promoted columns plus the legacy service.name/trace_id, plus
// any operator slot blooms — sorted and de-duplicated.
func TestBloomColumns_DerivedFromHasBloom(t *testing.T) {
	set := make(map[string]bool)
	for _, c := range LogBloomColumns() {
		set[c] = true
	}
	// legacy always-on
	for _, c := range []string{"service.name", "trace_id"} {
		if !set[c] {
			t.Errorf("LogBloomColumns missing legacy %q", c)
		}
	}
	// every HasBloom promoted column present
	r := NewRegistry(LogsProfile)
	for _, m := range r.PromotedColumns() {
		if m.HasBloom && !set[m.ParquetColumn] {
			t.Errorf("LogBloomColumns missing HasBloom column %q", m.ParquetColumn)
		}
		if !m.HasBloom && set[m.ParquetColumn] && m.ParquetColumn != "service.name" && m.ParquetColumn != "trace_id" {
			t.Errorf("LogBloomColumns includes non-HasBloom column %q (wasted bloom)", m.ParquetColumn)
		}
	}
}

// TestBloomColumns_SlotBloomsAppended verifies operator Tier-2 slot blooms are
// folded in (de-duplicated, sorted).
func TestBloomColumns_SlotBloomsAppended(t *testing.T) {
	withSlot := LogBloomColumns("ded_s01", "ded_s01")
	count := 0
	for _, c := range withSlot {
		if c == "ded_s01" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("ded_s01 appears %d times, want 1 (deduped)", count)
	}
	// sorted
	for i := 1; i < len(withSlot); i++ {
		if withSlot[i-1] > withSlot[i] {
			t.Errorf("bloom set not sorted at %d: %q > %q", i, withSlot[i-1], withSlot[i])
		}
	}
}

// TestDedicatedSlots_Constants pins the Tier-2 slot contract.
func TestDedicatedSlots_Constants(t *testing.T) {
	if DedicatedSlotCount != len(DedicatedSlotColumns) {
		t.Errorf("DedicatedSlotCount=%d != len(DedicatedSlotColumns)=%d", DedicatedSlotCount, len(DedicatedSlotColumns))
	}
	seen := make(map[string]bool)
	for _, c := range DedicatedSlotColumns {
		if seen[c] {
			t.Errorf("duplicate slot column %q", c)
		}
		seen[c] = true
	}
	if DedicatedSlotsMetaKey == "" {
		t.Error("DedicatedSlotsMetaKey must be non-empty (footer KV key)")
	}
}

// TestTraceBloomColumns_Set verifies the traces bloom set carries the legacy
// pair plus the HasBloom Tier-1 trace columns.
func TestTraceBloomColumns_Set(t *testing.T) {
	set := make(map[string]bool)
	for _, c := range TraceBloomColumns() {
		set[c] = true
	}
	for _, c := range []string{"service.name", "trace_id", "url.full", "container.id"} {
		if !set[c] {
			t.Errorf("TraceBloomColumns missing %q", c)
		}
	}
	// low-card descriptors must NOT be bloomed
	for _, c := range []string{"k8s.cluster.name", "telemetry.sdk.name", "cloud.account.id"} {
		if set[c] {
			t.Errorf("TraceBloomColumns includes low-card %q (wasted bloom)", c)
		}
	}
}

// TestSlotMapping_RoundTrip verifies Marshal/Unmarshal round-trips a mapping and
// that empty/garbage inputs degrade safely to nil (never panic).
func TestSlotMapping_RoundTrip(t *testing.T) {
	m := SlotMapping{"ded_s01": "tenant_id", "ded_s02": "feature_flag"}
	b := MarshalSlotMapping(m)
	if b == nil {
		t.Fatal("MarshalSlotMapping returned nil for non-empty mapping")
	}
	got := UnmarshalSlotMapping(b)
	if len(got) != len(m) {
		t.Fatalf("round-trip len = %d, want %d", len(got), len(m))
	}
	for k, v := range m {
		if got[k] != v {
			t.Errorf("round-trip[%q] = %q, want %q", k, got[k], v)
		}
	}
	// empty mapping → nil (writer skips the KV)
	if MarshalSlotMapping(nil) != nil || MarshalSlotMapping(SlotMapping{}) != nil {
		t.Error("empty mapping must marshal to nil")
	}
	// garbage → nil, no panic
	for _, bad := range [][]byte{nil, {}, []byte("not json"), []byte("{"), []byte("[]"), []byte("{}"), []byte(`{"k":1}`)} {
		if got := UnmarshalSlotMapping(bad); got != nil {
			t.Errorf("UnmarshalSlotMapping(%q) = %v, want nil", bad, got)
		}
	}
}

// FuzzSlotMapping ensures the footer-KV slot decoder never panics on arbitrary
// bytes (it's parsed from untrusted Parquet footers of any provenance).
func FuzzSlotMapping(f *testing.F) {
	f.Add([]byte(`{"ded_s01":"tenant_id"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`garbage`))
	f.Add([]byte(``))
	f.Fuzz(func(t *testing.T, data []byte) {
		got := UnmarshalSlotMapping(data) // must not panic
		if got != nil {
			// re-marshal must also be safe and round-trip-stable
			if b := MarshalSlotMapping(got); b != nil {
				if again := UnmarshalSlotMapping(b); len(again) != len(got) {
					t.Errorf("re-marshal unstable: %d → %d", len(got), len(again))
				}
			}
		}
	})
}
