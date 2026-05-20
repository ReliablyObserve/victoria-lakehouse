package parquets3

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestReadRowGroupProjected_ReadsSelectedColumns(t *testing.T) {
	rows := []schema.LogRow{
		{
			TimestampUnixNano: 1000000000,
			Body:              "test message",
			SeverityText:      "INFO",
			ServiceName:       "api",
			TraceID:           "abc123",
			HostName:          "host-1",
		},
		{
			TimestampUnixNano: 2000000000,
			Body:              "another message",
			SeverityText:      "ERROR",
			ServiceName:       "web",
			TraceID:           "def456",
			HostName:          "host-2",
		},
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&buf)
	_, err := w.Write(rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	rgs := f.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("no row groups")
	}

	// Project to timestamp + service.name only
	wantCols := map[string]bool{
		"timestamp_unix_nano": true,
		"service.name":        true,
	}

	result, err := readRowGroupProjected(f, rgs[0], wantCols)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}

	// Each row should have exactly 2 fields (timestamp + service.name)
	for i, row := range result {
		if len(row) != 2 {
			t.Errorf("row %d: expected 2 fields, got %d: %+v", i, len(row), row)
		}
		hasTimestamp := false
		hasService := false
		for _, fld := range row {
			switch fld.name {
			case "timestamp_unix_nano":
				hasTimestamp = true
			case "service.name":
				hasService = true
			}
		}
		if !hasTimestamp {
			t.Errorf("row %d: missing timestamp_unix_nano", i)
		}
		if !hasService {
			t.Errorf("row %d: missing service.name", i)
		}
	}

	// Verify values
	for _, fld := range result[0] {
		if fld.name == "timestamp_unix_nano" {
			if ts, ok := fld.value.(int64); !ok || ts != 1000000000 {
				t.Errorf("row 0: expected timestamp 1000000000, got %v", fld.value)
			}
		}
		if fld.name == "service.name" {
			if svc, ok := fld.value.(string); !ok || svc != "api" {
				t.Errorf("row 0: expected service.name 'api', got %v", fld.value)
			}
		}
	}
	for _, fld := range result[1] {
		if fld.name == "timestamp_unix_nano" {
			if ts, ok := fld.value.(int64); !ok || ts != 2000000000 {
				t.Errorf("row 1: expected timestamp 2000000000, got %v", fld.value)
			}
		}
		if fld.name == "service.name" {
			if svc, ok := fld.value.(string); !ok || svc != "web" {
				t.Errorf("row 1: expected service.name 'web', got %v", fld.value)
			}
		}
	}
}

func TestReadRowGroupProjected_NilWantCols(t *testing.T) {
	result, err := readRowGroupProjected(nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Error("expected nil result for nil wantCols")
	}
}

func TestReadRowGroupProjected_NoMatchingColumns(t *testing.T) {
	rows := []schema.LogRow{{TimestampUnixNano: 1000, Body: "test"}}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&buf)
	_, _ = w.Write(rows)
	_ = w.Close()

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	rgs := f.RowGroups()

	wantCols := map[string]bool{"nonexistent_column": true}
	result, err := readRowGroupProjected(f, rgs[0], wantCols)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Errorf("expected nil result for non-matching columns, got %d rows", len(result))
	}
}

func TestReadRowGroupProjected_SingleColumn(t *testing.T) {
	rows := []schema.LogRow{
		{TimestampUnixNano: 100, Body: "msg1", SeverityText: "WARN"},
		{TimestampUnixNano: 200, Body: "msg2", SeverityText: "DEBUG"},
		{TimestampUnixNano: 300, Body: "msg3", SeverityText: "INFO"},
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&buf)
	_, _ = w.Write(rows)
	_ = w.Close()

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	rgs := f.RowGroups()

	wantCols := map[string]bool{"severity_text": true}
	result, err := readRowGroupProjected(f, rgs[0], wantCols)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}

	expected := []string{"WARN", "DEBUG", "INFO"}
	for i, row := range result {
		if len(row) != 1 {
			t.Errorf("row %d: expected 1 field, got %d", i, len(row))
			continue
		}
		if row[0].name != "severity_text" {
			t.Errorf("row %d: expected field name 'severity_text', got %q", i, row[0].name)
		}
		if row[0].value != expected[i] {
			t.Errorf("row %d: expected %q, got %v", i, expected[i], row[0].value)
		}
	}
}

func TestReadRowGroupProjected_EmptyWantCols(t *testing.T) {
	rows := []schema.LogRow{{TimestampUnixNano: 1000, Body: "test"}}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&buf)
	_, _ = w.Write(rows)
	_ = w.Close()

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	rgs := f.RowGroups()

	wantCols := map[string]bool{}
	result, err := readRowGroupProjected(f, rgs[0], wantCols)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Errorf("expected nil result for empty wantCols, got %d rows", len(result))
	}
}
