package parquets3

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// slotRow is a minimal schema carrying one populated spare slot column.
type slotRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
	Body              string `parquet:"body"`
	DedS01            string `parquet:"ded_s01,optional,dict"`
}

// TestTier2_FieldNames_SlotRemap verifies that a Tier-2 slot column surfaces in
// the label index (→ field_names / the schema tab) under its operator-configured
// name from the file's footer KV — never the raw ded_sNN — and that an unmapped
// slot is omitted entirely (no raw leak).
func TestTier2_FieldNames_SlotRemap(t *testing.T) {
	write := func(t *testing.T, withMapping bool) *parquet.File {
		dir := t.TempDir()
		path := filepath.Join(dir, "slot.parquet")
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		opts := []parquet.WriterOption{parquet.Compression(&parquet.Zstd)}
		if withMapping {
			kv := schema.MarshalSlotMapping(schema.SlotMapping{"ded_s01": "tenant_id"})
			opts = append(opts, parquet.KeyValueMetadata(schema.DedicatedSlotsMetaKey, string(kv)))
		}
		w := parquet.NewGenericWriter[slotRow](f, opts...)
		_, _ = w.Write([]slotRow{{TimestampUnixNano: 1, Body: "m", DedS01: "acme-co"}})
		_ = w.Close()
		_ = f.Close()
		data, _ := os.ReadFile(path)
		pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		return pf
	}

	t.Run("mapped→configured name", func(t *testing.T) {
		s := testStorage()
		s.updateLabelIndex(write(t, true))
		names := map[string]bool{}
		for _, n := range s.labelIndex.GetFieldNames() {
			names[n] = true
		}
		if !names["tenant_id"] {
			t.Errorf("slot not surfaced under configured name 'tenant_id'; have %v", s.labelIndex.GetFieldNames())
		}
		if names["ded_s01"] {
			t.Error("raw slot column 'ded_s01' leaked into field_names")
		}
	})

	t.Run("unmapped slot omitted", func(t *testing.T) {
		s := testStorage()
		s.updateLabelIndex(write(t, false))
		for _, n := range s.labelIndex.GetFieldNames() {
			if n == "ded_s01" {
				t.Error("unmapped slot 'ded_s01' must not appear in field_names")
			}
		}
	})
}
