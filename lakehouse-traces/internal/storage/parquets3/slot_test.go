package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestTier2_TraceRowToFields_SlotRemap(t *testing.T) {
	prev := activeSlotResolver
	defer func() { activeSlotResolver = prev }()
	var r schema.TraceRow
	r.TraceID = "t1"
	schema.SetTraceSlot(&r, "ded_s01", "acme-9")
	// traceRowToFields emits slots RAW; the file-scan wrapper (withFileSlots)
	// renames per-file from the footer KV. Emulate it with a file mapping.
	emit := withFileSlots(traceRowToFields, schema.SlotMapping{"ded_s01": "tenant_id"})
	got := map[string]bool{}
	for _, f := range emit(&r, nil) {
		if s, ok := f.value.(string); ok && s != "" {
			got[f.name] = true
		}
	}
	if !got["tenant_id"] {
		t.Errorf("slot not renamed to configured name; got %v", got)
	}
	if got["ded_s01"] {
		t.Error("raw slot name leaked through the wrapper")
	}
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
