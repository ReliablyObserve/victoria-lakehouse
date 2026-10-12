package testutil

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestColumnEncodings_ReportsEveryTopLevelColumn(t *testing.T) {
	type row struct {
		Ts    int64  `parquet:"ts,delta"`
		Name  string `parquet:"name,dict"`
		Plain int64  `parquet:"plain"`
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[row](&buf)
	rows := make([]row, 100)
	for i := range rows {
		rows[i] = row{Ts: int64(i), Name: "a", Plain: int64(i)}
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	encs := ColumnEncodings(t, buf.Bytes())
	if len(encs) != 3 {
		t.Fatalf("columns %v", encs)
	}
	if !HasEncoding(encs, "ts", "DELTA_BINARY_PACKED") {
		t.Errorf("ts encodings %v", encs["ts"])
	}
	if HasEncoding(encs, "plain", "DELTA_BINARY_PACKED") || !HasEncoding(encs, "plain", "PLAIN") {
		t.Errorf("plain encodings %v", encs["plain"])
	}
	if !HasEncoding(encs, "name", "RLE_DICTIONARY") {
		t.Errorf("name encodings %v", encs["name"])
	}
	if HasEncoding(encs, "missing", "PLAIN") {
		t.Error("an unknown column has no encodings")
	}
}

func TestColumnEncodings_EmptyFileHasNoColumns(t *testing.T) {
	type row struct {
		Ts int64 `parquet:"ts"`
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[row](&buf)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if encs := ColumnEncodings(t, buf.Bytes()); len(encs) != 0 {
		t.Fatalf("empty file reports %v", encs)
	}
}
