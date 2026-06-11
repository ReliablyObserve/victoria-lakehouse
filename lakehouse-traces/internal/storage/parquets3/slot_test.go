package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestTier2_TraceRowToFields_SlotRemap(t *testing.T) {
	prev := activeSlotResolver
	defer func() { activeSlotResolver = prev }()
	SetSlotResolver(schema.NewSlotResolver([]schema.SlotAttr{{Name: "tenant_id", Bloom: true}}))
	var r schema.TraceRow
	r.TraceID = "t1"
	schema.SetTraceSlot(&r, "ded_s01", "acme-9")
	got := map[string]bool{}
	for _, f := range traceRowToFields(&r, nil) {
		if s, ok := f.value.(string); ok && s != "" {
			got[f.name] = true
		}
	}
	if !got["tenant_id"] {
		t.Errorf("slot not emitted under configured name; got %v", got)
	}
	if got["ded_s01"] {
		t.Error("raw slot name leaked")
	}
	SetSlotResolver(nil)
}

func TestTier2_TraceBloomColumns_WithSlots(t *testing.T) {
	r := schema.NewSlotResolver([]schema.SlotAttr{{Name: "tenant_id", Bloom: true}})
	set := map[string]bool{}
	for _, c := range schema.TraceBloomColumns(r.BloomSlots()...) {
		set[c] = true
	}
	if !set["ded_s01"] {
		t.Error("operator slot bloom not folded into TraceBloomColumns")
	}
}
