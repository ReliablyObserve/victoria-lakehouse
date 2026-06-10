package manifest

import (
	"fmt"
	"reflect"
	"testing"
)

func TestFileMeta_ApplyTo(t *testing.T) {
	t.Run("applies all fields to empty FileInfo", func(t *testing.T) {
		fi := FileInfo{Key: "test.parquet", Size: 1000}
		fm := FileMeta{
			RowCount:          500,
			MinTimeNs:         1000000,
			MaxTimeNs:         2000000,
			RawBytes:          5000,
			SchemaFingerprint: "abc123",
			Labels:            map[string][]string{"service": {"api"}},
		}
		fm.ApplyTo(&fi)

		if fi.RowCount != 500 {
			t.Errorf("RowCount = %d, want 500", fi.RowCount)
		}
		if fi.MinTimeNs != 1000000 {
			t.Errorf("MinTimeNs = %d, want 1000000", fi.MinTimeNs)
		}
		if fi.MaxTimeNs != 2000000 {
			t.Errorf("MaxTimeNs = %d, want 2000000", fi.MaxTimeNs)
		}
		if fi.RawBytes != 5000 {
			t.Errorf("RawBytes = %d, want 5000", fi.RawBytes)
		}
		if fi.SchemaFingerprint != "abc123" {
			t.Errorf("SchemaFingerprint = %q, want abc123", fi.SchemaFingerprint)
		}
		if len(fi.Labels["service"]) != 1 || fi.Labels["service"][0] != "api" {
			t.Errorf("Labels = %v, want {service: [api]}", fi.Labels)
		}
	})

	t.Run("does not overwrite existing values", func(t *testing.T) {
		fi := FileInfo{
			Key:               "test.parquet",
			RowCount:          100,
			MinTimeNs:         500000,
			MaxTimeNs:         600000,
			RawBytes:          2000,
			SchemaFingerprint: "existing",
			Labels:            map[string][]string{"env": {"prod"}},
		}
		fm := FileMeta{
			RowCount:          999,
			MinTimeNs:         111111,
			MaxTimeNs:         222222,
			RawBytes:          9999,
			SchemaFingerprint: "new",
			Labels:            map[string][]string{"service": {"api"}},
		}
		fm.ApplyTo(&fi)

		if fi.RowCount != 100 {
			t.Errorf("RowCount = %d, want 100 (original)", fi.RowCount)
		}
		if fi.MinTimeNs != 500000 {
			t.Errorf("MinTimeNs = %d, want 500000 (original)", fi.MinTimeNs)
		}
		if fi.Labels["env"][0] != "prod" {
			t.Errorf("Labels should keep original, got %v", fi.Labels)
		}
	})

	t.Run("partial apply with zero source fields", func(t *testing.T) {
		fi := FileInfo{Key: "test.parquet"}
		fm := FileMeta{RowCount: 42, MinTimeNs: 0, MaxTimeNs: 100}
		fm.ApplyTo(&fi)

		if fi.RowCount != 42 {
			t.Errorf("RowCount = %d, want 42", fi.RowCount)
		}
		if fi.MinTimeNs != 0 {
			t.Errorf("MinTimeNs = %d, want 0 (zero source shouldn't apply)", fi.MinTimeNs)
		}
		if fi.MaxTimeNs != 100 {
			t.Errorf("MaxTimeNs = %d, want 100", fi.MaxTimeNs)
		}
	})
}

func TestFileInfoToMeta_RoundTrip(t *testing.T) {
	fi := FileInfo{
		Key:               "partition/file.parquet",
		Size:              10000,
		RowCount:          500,
		MinTimeNs:         1716000000000000000,
		MaxTimeNs:         1716003600000000000,
		RawBytes:          50000,
		SchemaFingerprint: "sha256abc",
		Labels:            map[string][]string{"service.name": {"api-gateway", "worker"}},
	}

	fm := FileInfoToMeta(fi)

	if fm.RowCount != fi.RowCount {
		t.Errorf("RowCount mismatch: %d != %d", fm.RowCount, fi.RowCount)
	}
	if fm.MinTimeNs != fi.MinTimeNs {
		t.Errorf("MinTimeNs mismatch")
	}
	if fm.MaxTimeNs != fi.MaxTimeNs {
		t.Errorf("MaxTimeNs mismatch")
	}
	if len(fm.Labels["service.name"]) != 2 {
		t.Errorf("Labels not preserved: %v", fm.Labels)
	}
}

func TestFileMetaSidecar_MarshalUnmarshal(t *testing.T) {
	sc := &FileMetaSidecar{
		Files: map[string]FileMeta{
			"dt=2026-05-20/hour=11/abc.parquet": {
				RowCount:  1000,
				MinTimeNs: 1716000000000000000,
				MaxTimeNs: 1716003600000000000,
				RawBytes:  100000,
				Labels:    map[string][]string{"level": {"INFO", "ERROR"}},
			},
			"dt=2026-05-20/hour=11/def.parquet": {
				RowCount:  2000,
				MinTimeNs: 1716003600000000000,
				MaxTimeNs: 1716007200000000000,
			},
		},
	}

	data, err := MarshalFileMetaSidecar(sc)
	if err != nil {
		t.Fatal(err)
	}

	parsed, err := UnmarshalFileMetaSidecar(data)
	if err != nil {
		t.Fatal(err)
	}

	if len(parsed.Files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(parsed.Files))
	}

	abc := parsed.Files["dt=2026-05-20/hour=11/abc.parquet"]
	if abc.RowCount != 1000 {
		t.Errorf("abc RowCount = %d, want 1000", abc.RowCount)
	}
	if abc.MinTimeNs != 1716000000000000000 {
		t.Errorf("abc MinTimeNs mismatch")
	}
	if len(abc.Labels["level"]) != 2 {
		t.Errorf("abc Labels = %v, want 2 levels", abc.Labels)
	}

	def := parsed.Files["dt=2026-05-20/hour=11/def.parquet"]
	if def.RowCount != 2000 {
		t.Errorf("def RowCount = %d, want 2000", def.RowCount)
	}
}

func TestFileMetaSidecar_UnmarshalInvalid(t *testing.T) {
	_, err := UnmarshalFileMetaSidecar([]byte("not json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestFileMetaSidecar_CompactSize(t *testing.T) {
	sc := &FileMetaSidecar{
		Files: make(map[string]FileMeta, 100),
	}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("0/0/logs/dt=2026-05-20/hour=%02d/%016x.parquet", i%24, i)
		sc.Files[key] = FileMeta{
			RowCount:  int64(1000 + i),
			MinTimeNs: 1716000000000000000 + int64(i)*3600000000000,
			MaxTimeNs: 1716000000000000000 + int64(i+1)*3600000000000,
			RawBytes:  int64(100000 + i*1000),
		}
	}

	data, err := MarshalFileMetaSidecar(sc)
	if err != nil {
		t.Fatal(err)
	}

	bytesPerFile := len(data) / 100
	t.Logf("100 files: %d bytes total, ~%d bytes/file", len(data), bytesPerFile)
	if bytesPerFile > 300 {
		t.Errorf("sidecar too large: %d bytes/file, want < 300", bytesPerFile)
	}
}

func TestMetadataSidecarKey(t *testing.T) {
	key := MetadataSidecarKey("0/0/logs/", "dt=2026-05-20/hour=11")
	want := "0/0/logs/dt=2026-05-20/hour=11/_file_metadata.json"
	if key != want {
		t.Errorf("key = %q, want %q", key, want)
	}
}

type mockFileMetaProvider struct{ m map[string]FileMeta }

func (p mockFileMetaProvider) FileMeta(partition, key string) (FileMeta, bool) {
	fm, ok := p.m[partition+"|"+key]
	return fm, ok
}

// TestEnrichFromProvider is the file-meta read-flip's unit gate: the manifest
// enriches FileInfo from the (in-RAM) provider exactly like LoadSidecars does from
// the sidecar, and files with no/empty provider metadata are NOT counted (so the
// caller can fall back to the sidecar for them).
func TestEnrichFromProvider(t *testing.T) {
	m := New("bucket", "logs/")
	m.AddFile("p", FileInfo{Key: "logs/p/a.parquet"})
	m.AddFile("p", FileInfo{Key: "logs/p/b.parquet"})

	prov := mockFileMetaProvider{m: map[string]FileMeta{
		"p|logs/p/a.parquet": {RowCount: 42, MinTimeNs: 10, MaxTimeNs: 20, RawBytes: 99, SchemaFingerprint: "sf"},
		"p|logs/p/b.parquet": {}, // present but empty (RowCount 0) → must NOT count
	}}

	n, uncovered := m.EnrichFromProvider(prov)
	if n != 1 {
		t.Fatalf("enriched=%d, want 1 (only the file with real metadata)", n)
	}
	// b stays RowCount==0 (provider had it but empty) → partition p is uncovered,
	// so the caller falls back to the sidecar for ONLY partition p.
	if len(uncovered) != 1 || uncovered[0] != "p" {
		t.Fatalf("uncovered=%v, want [p] (b still lacks metadata)", uncovered)
	}
	var a FileInfo
	for _, fi := range m.FilesForPartition("p") {
		if fi.Key == "logs/p/a.parquet" {
			a = fi
		}
	}
	if a.RowCount != 42 || a.MinTimeNs != 10 || a.MaxTimeNs != 20 || a.SchemaFingerprint != "sf" {
		t.Fatalf("a not enriched from provider: %+v", a)
	}
	// nil provider is a no-op.
	if n, unc := m.EnrichFromProvider(nil); n != 0 || unc != nil {
		t.Fatalf("nil provider should enrich 0/nil, got %d/%v", n, unc)
	}
}

// TestEnrichFromProvider_FullyCoveredNoUncovered: when the provider covers every
// file with real metadata, uncovered is empty → the caller skips LoadSidecars
// entirely (the S3-op win).
func TestEnrichFromProvider_FullyCoveredNoUncovered(t *testing.T) {
	m := New("bucket", "logs/")
	m.AddFile("p", FileInfo{Key: "logs/p/a.parquet"})
	m.AddFile("q", FileInfo{Key: "logs/q/c.parquet"})
	prov := mockFileMetaProvider{m: map[string]FileMeta{
		"p|logs/p/a.parquet": {RowCount: 1, MinTimeNs: 1},
		"q|logs/q/c.parquet": {RowCount: 2, MinTimeNs: 2},
	}}
	n, uncovered := m.EnrichFromProvider(prov)
	if n != 2 || len(uncovered) != 0 {
		t.Fatalf("enriched=%d uncovered=%v, want 2 / [] (bundle covers all → no sidecar fallback)", n, uncovered)
	}
}

// TestRefresh_PreservesEveryEnrichmentField is the reflection-based regression
// guard for the refresh merge: a FULLY-populated FileInfo must survive an S3-list
// refresh with EVERY field intact. The previous field-by-field preserve list
// silently dropped any field someone forgot to add (LabelAggregates, added by
// PERF-2 after the list was written, was wiped on every 30s refresh — killing the
// count-pushdown fast path). Reflection means a NEW FileInfo field added without
// thought fails this test instead of silently regressing.
func TestRefresh_PreservesEveryEnrichmentField(t *testing.T) {
	m := New("bucket", "logs/")
	full := FileInfo{
		Key:               "logs/dt=2026-06-10/hour=10/a.parquet",
		Bucket:            "bucket",
		Size:              1234,
		RowCount:          42,
		MinTimeNs:         100,
		MaxTimeNs:         200,
		RawBytes:          9999,
		SchemaFingerprint: "sf-1",
		CompactionLevel:   2,
		StorageClass:      "STANDARD",
		Labels:            map[string][]string{"service.name": {"a", "b"}},
		LabelAggregates:   map[string]map[string]int64{"service.name": {"a": 30, "b": 12}},
		ColumnStats:       map[string]ColumnMinMax{},
	}
	// Fill any remaining zero-valued exported fields with non-zero values via
	// reflection where possible, so new fields are exercised automatically.
	rv := reflect.ValueOf(&full).Elem()
	for i := 0; i < rv.NumField(); i++ {
		f := rv.Field(i)
		if !f.CanSet() || !f.IsZero() {
			continue
		}
		switch f.Kind() {
		case reflect.String:
			f.SetString("x")
		case reflect.Int, reflect.Int32, reflect.Int64:
			f.SetInt(7)
		case reflect.Uint32, reflect.Uint64:
			f.SetUint(7)
		case reflect.Bool:
			f.SetBool(true)
		}
	}
	m.AddFile("dt=2026-06-10/hour=10", full)

	// Simulate the refresh merge: a bare list-derived entry for the same key.
	refreshed := map[string][]FileInfo{
		"dt=2026-06-10/hour=10": {{Key: full.Key, Bucket: "bucket", Size: 1234}},
	}
	m.mu.Lock()
	m.mergeRefreshedFilesLocked(refreshed)
	m.files = refreshed
	m.mu.Unlock()

	got := m.FilesForPartition("dt=2026-06-10/hour=10")
	if len(got) != 1 {
		t.Fatalf("files = %d, want 1", len(got))
	}
	gv := reflect.ValueOf(got[0])
	fv := reflect.ValueOf(full)
	for i := 0; i < gv.NumField(); i++ {
		name := gv.Type().Field(i).Name
		if !reflect.DeepEqual(gv.Field(i).Interface(), fv.Field(i).Interface()) {
			t.Errorf("refresh dropped/changed FileInfo.%s: got %v, want %v",
				name, gv.Field(i).Interface(), fv.Field(i).Interface())
		}
	}
}
