package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGoldenCompare_Match(t *testing.T) {
	dir := t.TempDir()
	golden := filepath.Join(dir, "test.golden")
	if err := os.WriteFile(golden, []byte(`{"status":"ok"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	err := CompareGolden(golden, []byte(`{"status":"ok"}`))
	if err != nil {
		t.Errorf("expected match, got error: %v", err)
	}
}

func TestGoldenCompare_Mismatch(t *testing.T) {
	dir := t.TempDir()
	golden := filepath.Join(dir, "test.golden")
	if err := os.WriteFile(golden, []byte(`{"status":"ok"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	err := CompareGolden(golden, []byte(`{"status":"changed"}`))
	if err == nil {
		t.Error("expected mismatch error, got nil")
	}
}

func TestGoldenCompare_MissingFile_Creates(t *testing.T) {
	dir := t.TempDir()
	golden := filepath.Join(dir, "new.golden")

	err := CompareGolden(golden, []byte(`{"new":"data"}`))
	if err != nil {
		t.Errorf("first run should create golden file, got: %v", err)
	}

	data, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"new":"data"}` {
		t.Errorf("golden file content = %q, want %q", string(data), `{"new":"data"}`)
	}
}

func TestGoldenUpdate(t *testing.T) {
	dir := t.TempDir()
	golden := filepath.Join(dir, "update.golden")
	if err := os.WriteFile(golden, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	UpdateGolden(golden, []byte("new"))

	data, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "new" {
		t.Errorf("updated content = %q, want %q", string(data), "new")
	}
}

func TestGoldenCompareJSON_IgnoresWhitespace(t *testing.T) {
	dir := t.TempDir()
	golden := filepath.Join(dir, "json.golden")
	if err := os.WriteFile(golden, []byte("{\"a\":1,\"b\":2}"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := CompareGoldenJSON(golden, []byte("{\n  \"a\": 1,\n  \"b\": 2\n}"))
	if err != nil {
		t.Errorf("JSON comparison should ignore whitespace, got: %v", err)
	}
}

func TestCompareGoldenJSON_InvalidActual(t *testing.T) {
	dir := t.TempDir()
	golden := filepath.Join(dir, "json.golden")
	err := CompareGoldenJSON(golden, []byte("not-json"))
	if err == nil {
		t.Error("expected error for invalid actual JSON, got nil")
	}
}

func TestCompareGoldenJSON_Mismatch(t *testing.T) {
	dir := t.TempDir()
	golden := filepath.Join(dir, "json.golden")
	if err := os.WriteFile(golden, []byte(`{"a":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	err := CompareGoldenJSON(golden, []byte(`{"a":2}`))
	if err == nil {
		t.Error("expected mismatch error, got nil")
	}
}

func TestCompareGoldenJSON_MissingFile_Creates(t *testing.T) {
	dir := t.TempDir()
	golden := filepath.Join(dir, "new.json.golden")
	err := CompareGoldenJSON(golden, []byte(`{"key":"value"}`))
	if err != nil {
		t.Errorf("missing golden should create file, got: %v", err)
	}
	data, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"key":"value"}` {
		t.Errorf("created golden = %q, want %q", string(data), `{"key":"value"}`)
	}
}

func TestCompareGoldenJSON_InvalidGolden(t *testing.T) {
	dir := t.TempDir()
	golden := filepath.Join(dir, "bad.golden")
	if err := os.WriteFile(golden, []byte("not-json"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := CompareGoldenJSON(golden, []byte(`{"a":1}`))
	if err == nil {
		t.Error("expected error for invalid golden JSON, got nil")
	}
}
