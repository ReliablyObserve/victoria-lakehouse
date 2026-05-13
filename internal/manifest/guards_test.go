package manifest

import (
	"encoding/json"
	"testing"
	"time"
)

func TestGuard_FileInfoStorageClassFields(t *testing.T) {
	fi := FileInfo{
		Key:            "test/dt=2026-05-13/hour=10/file.parquet",
		Size:           1024,
		StorageClass:   "STANDARD",
		ClassCheckedAt: time.Now(),
		ClassSource:    "write",
		CreatedAt:      time.Now(),
	}

	data, err := json.Marshal(fi)
	if err != nil {
		t.Fatal(err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}

	requiredFields := map[string]string{
		"key":              "key",
		"size":             "size",
		"storage_class":    "StorageClass",
		"class_checked_at": "ClassCheckedAt",
		"class_source":     "ClassSource",
		"created_at":       "CreatedAt",
	}

	for jsonKey, fieldName := range requiredFields {
		if _, ok := m[jsonKey]; !ok {
			t.Errorf("FileInfo JSON missing %q (field %s) — manifest persistence broken", jsonKey, fieldName)
		}
	}
}

func TestGuard_FileInfoStorageClassOmitEmpty(t *testing.T) {
	fi := FileInfo{
		Key:  "test.parquet",
		Size: 100,
	}
	data, _ := json.Marshal(fi)
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}

	// String fields with omitempty are omitted when zero
	stringFields := []string{"storage_class", "class_source"}
	for _, f := range stringFields {
		if _, ok := m[f]; ok {
			t.Errorf("FileInfo with zero %q should omit from JSON (omitempty)", f)
		}
	}

	// time.Time with omitempty: Go marshals zero time as "0001-01-01T00:00:00Z",
	// NOT omitted — this is standard Go behavior. Guard that the tags exist.
	timeFields := []string{"class_checked_at", "created_at"}
	for _, f := range timeFields {
		if _, ok := m[f]; !ok {
			t.Errorf("FileInfo zero time %q was omitted — tag may have changed", f)
		}
	}
}

func TestGuard_FileInfoJSONRoundTrip(t *testing.T) {
	original := FileInfo{
		Key:            "prefix/dt=2026-05-13/hour=10/abc.parquet",
		Size:           2048,
		RowCount:       500,
		MinTimeNs:      1000,
		MaxTimeNs:      9999,
		RawBytes:       4096,
		StorageClass:   "GLACIER",
		ClassCheckedAt: time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC),
		ClassSource:    "lifecycle",
		CreatedAt:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}

	var restored FileInfo
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}

	if restored.Key != original.Key {
		t.Error("Key lost")
	}
	if restored.Size != original.Size {
		t.Error("Size lost")
	}
	if restored.StorageClass != original.StorageClass {
		t.Error("StorageClass lost")
	}
	if restored.ClassSource != original.ClassSource {
		t.Error("ClassSource lost")
	}
	if !restored.CreatedAt.Equal(original.CreatedAt) {
		t.Error("CreatedAt lost")
	}
	if !restored.ClassCheckedAt.Equal(original.ClassCheckedAt) {
		t.Error("ClassCheckedAt lost")
	}
}

func TestGuard_CompressionRatioNeverNegative(t *testing.T) {
	cases := []FileInfo{
		{Size: 0, RawBytes: 0},
		{Size: 100, RawBytes: 0},
		{Size: 0, RawBytes: 100},
		{Size: -1, RawBytes: 100},
		{Size: 100, RawBytes: -1},
		{Size: 100, RawBytes: 200},
	}
	for i, fi := range cases {
		r := fi.CompressionRatio()
		if r < 0 {
			t.Errorf("case %d: CompressionRatio=%f is negative", i, r)
		}
	}
}

func TestGuard_ExtractPartitionStability(t *testing.T) {
	cases := map[string]string{
		"prefix/logs/dt=2026-05-13/hour=10/abc.parquet":   "dt=2026-05-13/hour=10",
		"100/1/logs/dt=2026-01-01/hour=00/f.parquet":      "dt=2026-01-01/hour=00",
	}
	for key, want := range cases {
		got := ExtractPartition(key)
		if got != want {
			t.Errorf("ExtractPartition(%q)=%q want %q", key, got, want)
		}
	}
}
