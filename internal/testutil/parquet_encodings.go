package testutil

import (
	"bytes"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// ColumnEncodings returns, per top-level leaf column, the encodings recorded in
// the footer's first row group (the same field pyarrow exposes as
// ColumnChunkMetaData.encodings). An empty file returns an empty map.
func ColumnEncodings(tb testing.TB, data []byte) map[string][]string {
	tb.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		tb.Fatalf("open parquet: %v", err)
	}
	out := map[string][]string{}
	md := f.Metadata()
	if len(md.RowGroups) == 0 {
		return out
	}
	for _, c := range md.RowGroups[0].Columns {
		if len(c.MetaData.PathInSchema) != 1 {
			continue
		}
		encs := make([]string, 0, len(c.MetaData.Encoding))
		for _, e := range c.MetaData.Encoding {
			encs = append(encs, format.Encoding(e).String())
		}
		slices.Sort(encs)
		out[c.MetaData.PathInSchema[0]] = encs
	}
	return out
}

// HasEncoding reports whether the column's footer lists the encoding.
func HasEncoding(encs map[string][]string, column, encoding string) bool {
	return slices.Contains(encs[column], encoding)
}
