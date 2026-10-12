package parquets3

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A Tier-2 slot column is a field of the rows under its configured name on the
// field_names endpoint too (the label index test above pins the in-RAM list):
// the rows carry tenant_id, never the raw ded_s01, and an object without the
// mapping lists no slot at all.
func TestTier2_FieldNames_SlotRemapOnTheEndpoint(t *testing.T) {
	base := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC).UnixNano()
	file := func(withMapping bool) []byte {
		var buf bytes.Buffer
		opts := []parquet.WriterOption{parquet.Compression(&parquet.Zstd)}
		if withMapping {
			opts = append(opts, parquet.KeyValueMetadata(schema.DedicatedSlotsMetaKey, string(schema.MarshalSlotMapping(schema.SlotMapping{"ded_s01": "tenant_id"}))))
		}
		w := parquet.NewGenericWriter[slotRow](&buf, opts...)
		if _, err := w.Write([]slotRow{
			{TimestampUnixNano: base + 1000, Body: "m1", DedS01: "acme-co"},
			{TimestampUnixNano: base + 2000, Body: "m2", DedS01: "acme-co"},
		}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	for _, withMapping := range []bool{true, false} {
		mock := newMockS3Server()
		s := testStorageWithS3(t, mock.url())
		data := file(withMapping)
		key := "logs/dt=2026-06-09/hour=10/slot.parquet"
		mock.putFile(key, data)
		s.manifest.AddFile("dt=2026-06-09/hour=10", manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: 2, MinTimeNs: base + 1000, MaxTimeNs: base + 2000})
		got, err := s.GetFieldNames(context.Background(), nil, mustParseQueryWithTime(t, "*", base, base+4000))
		mock.close()
		if err != nil {
			t.Fatal(err)
		}
		m := namesToHits(got)
		if _, raw := m["ded_s01"]; raw {
			t.Errorf("mapping=%v: raw slot column ded_s01 leaked into field_names: %v", withMapping, m)
		}
		if withMapping && m["tenant_id"] != 2 {
			t.Errorf("mapping=true: tenant_id hits = %d, want 2: %v", m["tenant_id"], m)
		}
		if !withMapping {
			if _, listed := m["tenant_id"]; listed {
				t.Errorf("mapping=false: tenant_id listed without a mapping: %v", m)
			}
		}
	}
}
