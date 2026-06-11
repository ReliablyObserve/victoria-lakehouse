package vlstorage

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestTier2_SlotRoundTrip proves the custom-attribute slot path end to end at
// the ingest layer: a configured custom key routes to a slot column (out of the
// map), and an UNconfigured key still lands in the map. Read-back by the
// configured name is covered in the parquets3 emission tests.
func TestTier2_SlotRoundTrip(t *testing.T) {
	prev := activeSlotResolver
	defer func() { activeSlotResolver = prev }()

	SetSlotResolver(schema.NewSlotResolver([]schema.SlotAttr{
		{Name: "tenant_id", Bloom: true},
		{Name: "feature_flag", Bloom: false},
	}))

	var row schema.LogRow
	mapFieldToRow(&row, "tenant_id", "acme-42")
	mapFieldToRow(&row, "feature_flag", "dark-mode")
	mapFieldToRow(&row, "unconfigured.custom", "stays-in-map")

	// tenant_id -> ded_s01, feature_flag -> ded_s02 (config order)
	if got := schema.LogSlotValue(&row, "ded_s01"); got != "acme-42" {
		t.Errorf("ded_s01 = %q, want acme-42", got)
	}
	if got := schema.LogSlotValue(&row, "ded_s02"); got != "dark-mode" {
		t.Errorf("ded_s02 = %q, want dark-mode", got)
	}
	// configured keys must NOT be in the map
	if _, in := row.LogAttributes["tenant_id"]; in {
		t.Error("tenant_id leaked into LogAttributes (should be in a slot)")
	}
	// unconfigured key stays in the map
	if row.LogAttributes["unconfigured.custom"] != "stays-in-map" {
		t.Error("unconfigured key must remain in LogAttributes")
	}

	// nil resolver: everything goes to the map (no custom promotion)
	SetSlotResolver(nil)
	var row2 schema.LogRow
	mapFieldToRow(&row2, "tenant_id", "x")
	if row2.LogAttributes["tenant_id"] != "x" {
		t.Error("with nil resolver, custom keys must fall through to the map")
	}
	if schema.LogSlotValue(&row2, "ded_s01") != "" {
		t.Error("nil resolver must not populate slots")
	}
}
