// Package parquetfooter is the one place that knows the physical layout of the
// end of a Parquet object: the page-index stripe (ColumnIndex and OffsetIndex
// sections), the Thrift footer and the 8-byte trailer. Writers use it to learn,
// from the object they just wrote, how many bytes at the end of the object a
// reader needs (Inspect); readers use it to fetch exactly those bytes in one
// ranged read (Read).
//
// Both binaries share it, so the logs and the traces read paths, the flush,
// compaction and delete-rewrite writers all measure and fetch footers the same
// way.
package parquetfooter

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// TrailerLen is the Parquet trailer: a 4-byte little-endian footer length and
// the 4-byte magic "PAR1".
const TrailerLen = 8

// Length decodes the footer length from the last TrailerLen bytes of an object
// (excluding the trailer itself) and checks the magic.
func Length(tail8 []byte) (int, error) {
	if len(tail8) < TrailerLen {
		return 0, fmt.Errorf("need %d bytes, got %d", TrailerLen, len(tail8))
	}
	tail8 = tail8[len(tail8)-TrailerLen:]
	if magic := string(tail8[4:8]); magic != "PAR1" {
		return 0, fmt.Errorf("not a parquet file (magic=%q)", magic)
	}
	return int(binary.LittleEndian.Uint32(tail8[0:4])), nil
}

// Info is what a writer learns from the footer of the object it has just
// written, in one parse.
type Info struct {
	// ColumnBytes is the compressed bytes of each top-level column, summed over
	// the row groups (nil when the object has no row group).
	ColumnBytes map[string]int64
	// BloomBytes is the on-disk size of the footer column blooms.
	BloomBytes int64
	// Tail is the number of bytes at the end of the object a reader needs to
	// have the whole footer and its page-index stripe: from the first
	// ColumnIndex/OffsetIndex section (or the footer when there is none) to the
	// end of the object. 0 when the object cannot be parsed.
	Tail int32
}

// Inspect parses the footer of a Parquet object held in memory (no column data
// is read) and returns what the writers record in the manifest. Any error
// leaves the fields zero: the flush degrades to "no per-column sizes, no tail
// length" rather than failing, and a reader falls back to the prefetch window.
func Inspect(data []byte) Info {
	// SkipPageIndex: the page index is not needed for anything Inspect reports
	// (the offsets of its sections are in the footer metadata), and decoding every
	// column and offset index of a large object is most of what OpenFile costs.
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.SkipPageIndex(true))
	if err != nil {
		return Info{}
	}
	var info Info
	for _, rg := range f.RowGroups() {
		for _, cc := range rg.ColumnChunks() {
			if bf := cc.BloomFilter(); bf != nil {
				info.BloomBytes += bf.Size()
			}
		}
	}
	md := f.Metadata()
	if md == nil {
		return info
	}
	if footerLen, err := Length(lastN(data, TrailerLen)); err == nil {
		info.Tail = tailLen(int64(len(data)), footerLen, md)
	}
	if len(md.RowGroups) == 0 {
		return info
	}
	cols := make(map[string]int64)
	for i := range md.RowGroups {
		for j := range md.RowGroups[i].Columns {
			cm := md.RowGroups[i].Columns[j].MetaData
			if len(cm.PathInSchema) == 0 {
				continue
			}
			cols[cm.PathInSchema[0]] += cm.TotalCompressedSize
		}
	}
	if len(cols) > 0 {
		info.ColumnBytes = cols
	}
	return info
}

func lastN(b []byte, n int) []byte {
	if len(b) < n {
		return b
	}
	return b[len(b)-n:]
}

// StripeStart is the offset of the first page-index section (ColumnIndex or
// OffsetIndex) of any column chunk in md, or -1 when the object has no page
// index.
func StripeStart(md *format.FileMetaData) int64 {
	if md == nil {
		return -1
	}
	start := int64(-1)
	for i := range md.RowGroups {
		for j := range md.RowGroups[i].Columns {
			c := &md.RowGroups[i].Columns[j]
			for _, o := range [...]int64{c.ColumnIndexOffset, c.OffsetIndexOffset} {
				if o > 0 && (start < 0 || o < start) {
					start = o
				}
			}
		}
	}
	return start
}

// tailLen is the byte count from the page-index stripe (when it sits before the
// footer) to the end of an object of the given size; 0 if it does not fit an
// int32 or is inconsistent.
func tailLen(size int64, footerLen int, md *format.FileMetaData) int32 {
	start := size - int64(footerLen) - TrailerLen
	if ps := StripeStart(md); ps > 0 && ps < start {
		start = ps
	}
	n := size - start
	if start < 0 || n < TrailerLen || n > MaxHint {
		return 0
	}
	return int32(n)
}
