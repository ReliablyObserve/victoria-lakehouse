package schema

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// severity_number is the nullable column of #274: NULL means the field was
// absent, 0 means an explicit 0. Compaction and the delete rewriter read and
// write whole objects through parquet.GenericReader/Writer[LogRow], so the
// round trip must keep the distinction, and an object written before the column
// was nullable (required INT32) must still be readable.

func TestLogRowSeverityNumber_RoundTripKeepsNullAndZero(t *testing.T) {
	in := []LogRow{
		{TimestampUnixNano: 1, Body: "absent"},
		{TimestampUnixNano: 2, Body: "zero", SeverityNumber: Int32Ptr(0)},
		{TimestampUnixNano: 3, Body: "nine", SeverityNumber: Int32Ptr(9)},
		{TimestampUnixNano: 4, Body: "negative", SeverityNumber: Int32Ptr(-1)},
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[LogRow](&buf)
	if _, err := w.Write(in); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if c := f.Root().Column("severity_number"); c == nil || !c.Optional() {
		t.Fatal("severity_number must be an optional column")
	}
	out := make([]LogRow, len(in))
	r := parquet.NewGenericReader[LogRow](bytes.NewReader(buf.Bytes()))
	if n, _ := r.Read(out); n != len(in) {
		t.Fatalf("read %d rows, want %d", n, len(in))
	}
	for i := range in {
		if (in[i].SeverityNumber == nil) != (out[i].SeverityNumber == nil) ||
			Int32Value(in[i].SeverityNumber) != Int32Value(out[i].SeverityNumber) {
			t.Errorf("row %q: severity_number %v read back as %v", in[i].Body, in[i].SeverityNumber, out[i].SeverityNumber)
		}
	}
}

func TestLogRowSeverityNumber_ReadsAnObjectWithARequiredColumn(t *testing.T) {
	type legacy struct {
		TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
		Body              string `parquet:"body"`
		SeverityNumber    int32  `parquet:"severity_number"`
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[legacy](&buf)
	if _, err := w.Write([]legacy{{1, "a", 0}, {2, "b", 17}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out := make([]LogRow, 2)
	r := parquet.NewGenericReader[LogRow](bytes.NewReader(buf.Bytes()))
	if n, _ := r.Read(out); n != 2 {
		t.Fatalf("read %d rows, want 2", n)
	}
	// A pre-existing zero is a stored value: it stays "0" (no rewrite promise).
	if out[0].SeverityNumber == nil || *out[0].SeverityNumber != 0 || out[1].SeverityNumber == nil || *out[1].SeverityNumber != 17 {
		t.Errorf("legacy values read back as %v and %v, want 0 and 17", out[0].SeverityNumber, out[1].SeverityNumber)
	}
}

func TestInt32Value(t *testing.T) {
	if Int32Value(nil) != 0 || Int32Value(Int32Ptr(7)) != 7 {
		t.Fatal("Int32Value must deref, and give 0 for nil")
	}
}
