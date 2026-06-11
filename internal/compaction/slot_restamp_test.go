package compaction

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestCompaction_RestampSlotMapping verifies that compacted files re-stamp the
// Tier-2 slot→name footer KV (so merged files stay self-describing) and fold the
// operator slot blooms into the bloom set.
func TestCompaction_RestampSlotMapping(t *testing.T) {
	prev := activeSlotResolver
	defer func() { activeSlotResolver = prev }()
	SetSlotResolver(schema.NewSlotResolver([]schema.SlotAttr{{Name: "tenant_id", Bloom: true}}))

	rows := []schema.LogRow{{TimestampUnixNano: 1, Body: "m", DedS01: "acme"}}
	data, err := writeCompactedLogs(rows, 1000, 0)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	var got string
	for _, kv := range f.Metadata().KeyValueMetadata {
		if kv.Key == schema.DedicatedSlotsMetaKey {
			got = kv.Value
		}
	}
	if got == "" {
		t.Fatal("compacted file is missing the dedicated-slots footer KV")
	}
	if m := schema.UnmarshalSlotMapping([]byte(got)); m["ded_s01"] != "tenant_id" {
		t.Errorf("compacted slot mapping = %v, want ded_s01→tenant_id", m)
	}

	// No resolver → no footer KV (nil-safe).
	SetSlotResolver(nil)
	data2, _ := writeCompactedLogs(rows, 1000, 0)
	f2, _ := parquet.OpenFile(bytes.NewReader(data2), int64(len(data2)))
	for _, kv := range f2.Metadata().KeyValueMetadata {
		if kv.Key == schema.DedicatedSlotsMetaKey {
			t.Error("no resolver: compacted file must not carry a slot mapping")
		}
	}
}
