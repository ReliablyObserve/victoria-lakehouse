package tokenbloom

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestBloomRoundTripHasNoFalseNegatives(t *testing.T) {
	var bodies []string
	for i := 0; i < 500; i++ {
		bodies = append(bodies, fmt.Sprintf("request %d from checkout-%d failed with code %d", i, i%13, i%7))
	}
	key, value := BuildMetadata(bodies, 3)
	if key != "_bloom_body_rg_3" {
		t.Fatalf("key = %q", key)
	}
	var b TokenBloom
	if err := b.UnmarshalBinary(value); err != nil {
		t.Fatal(err)
	}
	for _, body := range bodies {
		for _, tok := range Tokenize(body) {
			if !b.Test(tok) {
				t.Fatalf("token %q of an indexed body is missing", tok)
			}
		}
	}
	if b.Test("definitely-not-there-token-xyz") && b.Test("another-absent-token-qrs") && b.Test("third-absent-token-wvu") {
		t.Error("three absent tokens all test present: the filter is saturated")
	}
}

// RowGroupOptions numbers the row groups of the rows it is given, whatever the
// source of those rows: the key of row group i is the index in THIS file.
func TestRowGroupOptionsIndexTheRowGroupsOfTheFileBeingWritten(t *testing.T) {
	rows := make([]schema.LogRow, 25)
	for i := range rows {
		rows[i].TimestampUnixNano = int64(i)
		if i >= 10 && i < 20 { // row group 1 has no body at all
			continue
		}
		rows[i].Body = fmt.Sprintf("word%d", i)
	}
	opts := RowGroupOptions(len(rows), 10, func(i int) string { return rows[i].Body })
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&buf, append([]parquet.WriterOption{parquet.MaxRowsPerRowGroup(10)}, opts...)...)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.RowGroups()) != 3 {
		t.Fatalf("%d row groups, want 3", len(f.RowGroups()))
	}
	kv := map[string]string{}
	for _, e := range f.Metadata().KeyValueMetadata {
		kv[e.Key] = e.Value
	}
	if _, ok := kv["_bloom_body_rg_1"]; ok {
		t.Error("a row group without a body has a bloom")
	}
	for _, rg := range []int{0, 2} {
		v, ok := kv[fmt.Sprintf(KeyFormat, rg)]
		if !ok {
			t.Fatalf("row group %d has no bloom", rg)
		}
		var b TokenBloom
		if err := b.UnmarshalBinary([]byte(v)); err != nil {
			t.Fatal(err)
		}
		for i := rg * 10; i < (rg+1)*10 && i < len(rows); i++ {
			if rows[i].Body != "" && !b.Test(strings.ToLower(rows[i].Body)) {
				t.Errorf("row group %d bloom misses %q", rg, rows[i].Body)
			}
		}
	}
}

func TestRowGroupOptionsDegenerateInputs(t *testing.T) {
	if got := RowGroupOptions(10, 0, func(int) string { return "x y" }); got != nil {
		t.Error("row group size 0 must write nothing, not loop")
	}
	if got := RowGroupOptions(0, 10, func(int) string { return "x y" }); got != nil {
		t.Error("no rows must write nothing")
	}
}

func TestUnmarshalRejectsTruncatedData(t *testing.T) {
	_, v := BuildMetadata([]string{"hello world"}, 0)
	var b TokenBloom
	for _, n := range []int{0, 3, 5, len(v) - 1} {
		if err := b.UnmarshalBinary(v[:n]); err == nil {
			t.Errorf("accepted %d of %d bytes", n, len(v))
		}
	}
}
