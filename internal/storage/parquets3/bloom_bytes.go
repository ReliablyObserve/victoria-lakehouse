package parquets3

import (
	"bytes"

	"github.com/parquet-go/parquet-go"
)

// footerBloomBytes sums the encoded size of every column-chunk bloom filter in a
// written Parquet file — the on-disk footprint of the FOOTER blooms — recorded in
// the manifest at flush time so the compaction stats can report bloom storage cost
// without reading any file. Best-effort: 0 if the bytes can't be parsed.
func footerBloomBytes(data []byte) int64 {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return 0
	}
	var total int64
	for _, rg := range f.RowGroups() {
		for _, cc := range rg.ColumnChunks() {
			if bf := cc.BloomFilter(); bf != nil {
				total += bf.Size()
			}
		}
	}
	return total
}
