package vlstorage

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestTier2_TraceSlotRouting proves the custom-attribute slot path for traces:
// a configured key routes to a slot column (out of the resource/span map) via
// both mapResourceAttr and mapSpanAttr; unconfigured keys stay in the map; a nil
// resolver routes everything to the map.
func TestTier2_TraceSlotRouting(t *testing.T) {
	prev := activeSlotResolver
	defer func() { activeSlotResolver = prev }()

	SetSlotResolver(schema.NewSlotResolver([]schema.SlotAttr{
		{Name: "tenant_id", Bloom: true},
		{Name: "deploy_color", Bloom: false},
	}))

	// resource-scope custom attr → slot
	var r1 schema.TraceRow
	mapResourceAttr(&r1, "tenant_id", "acme-7")
	mapResourceAttr(&r1, "unconfigured.res", "in-map")
	if got := schema.TraceSlotValue(&r1, "ded_s01"); got != "acme-7" {
		t.Errorf("resource tenant_id → ded_s01 = %q, want acme-7", got)
	}
	if _, in := r1.ResourceAttributes["tenant_id"]; in {
		t.Error("tenant_id leaked into ResourceAttributes")
	}
	if r1.ResourceAttributes["unconfigured.res"] != "in-map" {
		t.Error("unconfigured resource key must stay in the map")
	}

	// span-scope custom attr → slot
	var r2 schema.TraceRow
	mapSpanAttr(&r2, "deploy_color", "green")
	mapSpanAttr(&r2, "unconfigured.span", "in-map")
	if got := schema.TraceSlotValue(&r2, "ded_s02"); got != "green" {
		t.Errorf("span deploy_color → ded_s02 = %q, want green", got)
	}
	if _, in := r2.SpanAttributes["deploy_color"]; in {
		t.Error("deploy_color leaked into SpanAttributes")
	}

	// nil resolver: everything to the map
	SetSlotResolver(nil)
	var r3 schema.TraceRow
	mapResourceAttr(&r3, "tenant_id", "x")
	mapSpanAttr(&r3, "deploy_color", "y")
	if r3.ResourceAttributes["tenant_id"] != "x" || r3.SpanAttributes["deploy_color"] != "y" {
		t.Error("with nil resolver, custom keys must fall through to the maps")
	}
}
