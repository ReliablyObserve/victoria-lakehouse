package stats

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestHandleFields_StorageAndMetadata locks the Storage Details tab's data
// source: /stats/fields returns a per-field row carrying BOTH the exact on-S3
// storage bytes (from the size-stats aggregate's FieldSizes) and the exact pmeta
// metadata bytes (from MetadataBytesByField), sorted by storage descending.
func TestHandleFields_StorageAndMetadata(t *testing.T) {
	agg := NewStatsAggregate()
	// Two live files carrying per-column bytes; full coverage so storageScale == 1
	// and reported storage equals the raw per-field sum.
	agg.OnAdd("p", manifest.FileInfo{
		Key: "100/1/a.parquet", Size: 1000, RowCount: 10,
		ColumnBytes: map[string]int64{"service.name": 700, "level": 300},
	})
	agg.OnAdd("p", manifest.FileInfo{
		Key: "100/1/b.parquet", Size: 500, RowCount: 5,
		ColumnBytes: map[string]int64{"service.name": 500},
	})

	li := cache.NewLabelIndex()
	li.AddWithTenant("service.name", []string{"api", "web", "db"}, "100:1")
	li.AddWithTenant("level", []string{"info", "warn"}, "100:1")

	metaByField := map[string]int64{"service.name": 4096, "level": 512}

	api := NewAPI(APIConfig{
		Registry:             NewTenantRegistry("node-1"),
		Manifest:             manifest.New("test-bucket", "data/"),
		LabelIndex:           li,
		Mode:                 "logs",
		Bucket:               "test-bucket",
		BloomColumns:         []string{"service.name"},
		StatsAggregate:       agg,
		PmetaCardinality:     func(f string) uint64 { return map[string]uint64{"service.name": 3, "level": 2}[f] },
		MetadataBytesByField: func() map[string]int64 { return metaByField },
	})

	rec := doGet(t, api, "/lakehouse/api/v1/stats/fields")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp FieldsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Fields) == 0 {
		t.Fatalf("no fields returned")
	}

	byName := make(map[string]FieldStorageEntry, len(resp.Fields))
	for _, f := range resp.Fields {
		byName[f.Name] = f
	}

	sn, ok := byName["service.name"]
	if !ok {
		t.Fatalf("service.name missing from response: %+v", resp.Fields)
	}
	// At full coverage storage is the exact per-field sum (700+500).
	if sn.StorageBytes != 1200 {
		t.Errorf("service.name storage_bytes = %d, want 1200", sn.StorageBytes)
	}
	if sn.MetadataBytes != 4096 {
		t.Errorf("service.name metadata_bytes = %d, want 4096", sn.MetadataBytes)
	}
	if !sn.HasBloom {
		t.Errorf("service.name has_bloom = false, want true (it's a bloom column)")
	}
	if sn.Cardinality != 3 {
		t.Errorf("service.name cardinality = %d, want 3 (pmeta source)", sn.Cardinality)
	}

	lvl := byName["level"]
	if lvl.StorageBytes != 300 || lvl.MetadataBytes != 512 {
		t.Errorf("level = {storage %d, metadata %d}, want {300, 512}", lvl.StorageBytes, lvl.MetadataBytes)
	}

	// Sorted by storage descending: service.name (1200) before level (300).
	if resp.Fields[0].Name != "service.name" {
		t.Errorf("first field = %q, want service.name (largest storage first)", resp.Fields[0].Name)
	}
}

// TestHandleFields_NilMetadataSafe: a nil MetadataBytesByField func must not
// panic — every field's metadata_bytes is simply 0.
func TestHandleFields_NilMetadataSafe(t *testing.T) {
	agg := NewStatsAggregate()
	agg.OnAdd("p", manifest.FileInfo{
		Key: "1/1/a.parquet", Size: 100, RowCount: 1,
		ColumnBytes: map[string]int64{"f": 100},
	})
	li := cache.NewLabelIndex()
	li.AddWithTenant("f", []string{"x"}, "1:1")

	api := NewAPI(APIConfig{
		Registry:       NewTenantRegistry("node-1"),
		Manifest:       manifest.New("b", "data/"),
		LabelIndex:     li,
		Mode:           "logs",
		StatsAggregate: agg,
		// MetadataBytesByField intentionally nil.
	})

	rec := doGet(t, api, "/lakehouse/api/v1/stats/fields")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp FieldsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, f := range resp.Fields {
		if f.MetadataBytes != 0 {
			t.Errorf("field %q metadata_bytes = %d, want 0 (nil func)", f.Name, f.MetadataBytes)
		}
	}
}

// TestHandleFields_MethodNotAllowed: non-GET is rejected.
func TestHandleFields_MethodNotAllowed(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	rec := doMethod(t, api, http.MethodPost, "/lakehouse/api/v1/stats/fields")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST status = %d, want 405", rec.Code)
	}
}
