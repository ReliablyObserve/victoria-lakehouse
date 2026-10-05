package schema

import (
	"bytes"
	"testing"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

func TestIsLowerHex(t *testing.T) {
	for _, c := range []struct {
		s           string
		hex, hexTok bool
	}{
		{"", true, false},
		{"0", true, true},
		{"0123456789abcdef", true, true},
		{"4bf92f3577b34da6a3ce929d0e0bf736", true, true},
		{"ABCDEF", false, false},
		{"abcdeg", false, false},
		{"abc-def", false, false},
		{"4bf92f35-77b3-4da6-a3ce-929d0e0bf736", false, false},
		{"S/kvNXezTaajzpKdDvc2Aw==", false, false},
		{"abc_def", false, false},
		{"abc def", false, false},
		{"café", false, false},
		{"/", false, false}, {":", false, false}, {"`", false, false}, {"g", false, false},
	} {
		if got := IsLowerHex(c.s); got != c.hex {
			t.Errorf("IsLowerHex(%q) = %v, want %v", c.s, got, c.hex)
		}
		if got := IsLowerHexToken(c.s); got != c.hexTok {
			t.Errorf("IsLowerHexToken(%q) = %v, want %v", c.s, got, c.hexTok)
		}
	}
}

func TestRowsTraceIDHex(t *testing.T) {
	if !LogRowsTraceIDHex(nil) || !TraceRowsTraceIDHex(nil) {
		t.Error("no rows: vacuously attested")
	}
	if !LogRowsTraceIDHex([]LogRow{{TraceID: "ab12"}, {TraceID: ""}}) {
		t.Error("hex and empty ids must attest")
	}
	if LogRowsTraceIDHex([]LogRow{{TraceID: "ab12"}, {TraceID: "x-y"}}) {
		t.Error("one non-hex log row must not attest")
	}
	if !TraceRowsTraceIDHex([]TraceRow{{TraceID: "ab12"}, {TraceID: ""}}) {
		t.Error("hex and empty span ids must attest")
	}
	if TraceRowsTraceIDHex([]TraceRow{{TraceID: "ab12"}, {TraceID: "AB12"}}) {
		t.Error("one uppercase span id must not attest")
	}
}

func writeKV(t *testing.T, opts ...parquet.WriterOption) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[LogRow](&buf, opts...)
	if _, err := w.Write([]LogRow{{TraceID: "ab"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestTraceIDHexFooter: the writer option records ASCII "1"/"0" (valid UTF-8,
// so every Parquet reader can read the footer), only "1" attests, and a file
// without the key or that cannot be opened attests nothing.
func TestTraceIDHexFooter(t *testing.T) {
	for _, attested := range []bool{true, false} {
		data := writeKV(t, TraceIDHexOption(attested))
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		v, ok := f.Lookup(TraceIDHexMetaKey)
		if !ok || !utf8.ValidString(v) || (v != "1" && v != "0") || (v == "1") != attested {
			t.Fatalf("attested=%v: footer value %q (present=%v)", attested, v, ok)
		}
		if FooterTraceIDHex(f.Metadata()) != attested || FileTraceIDHex(data) != attested {
			t.Fatalf("attested=%v read back wrong", attested)
		}
	}
	if FileTraceIDHex(writeKV(t)) {
		t.Error("a file without the key must not attest")
	}
	if FileTraceIDHex([]byte("not parquet")) || FooterTraceIDHex(nil) {
		t.Error("garbage must not attest")
	}
	for _, v := range []string{"true", "yes", "01", " 1", ""} {
		md := &format.FileMetaData{KeyValueMetadata: []format.KeyValue{{Key: TraceIDHexMetaKey, Value: v}}}
		if FooterTraceIDHex(md) {
			t.Errorf("value %q must not attest", v)
		}
	}
}

func TestCompactedTraceIDHex(t *testing.T) {
	yes, no, absent := writeKV(t, TraceIDHexOption(true)), writeKV(t, TraceIDHexOption(false)), writeKV(t)
	for _, c := range []struct {
		name    string
		inputs  [][]byte
		rowsHex bool
		want    bool
	}{
		{"all_yes", [][]byte{yes, yes}, true, true},
		{"one_no", [][]byte{yes, no}, true, false},
		{"one_absent", [][]byte{yes, absent}, true, false},
		{"rows_not_hex", [][]byte{yes, yes}, false, false},
		{"no_inputs", nil, true, true},
	} {
		if got := CompactedTraceIDHex(c.inputs, c.rowsHex); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}
