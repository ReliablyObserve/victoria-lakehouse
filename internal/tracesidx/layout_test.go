package tracesidx

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestPrintLayout(t *testing.T) {
	rows := randRows(rand.New(rand.NewSource(3)), 60000)
	data, _, _ := Marshal(rows, 3)
	f, _ := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.SkipPageIndex(true))
	for rg, g := range f.Metadata().RowGroups {
		for i, c := range g.Columns {
			t.Logf("rg%d col%d %v data@%d ci@%d+%d oi@%d+%d bloom@%d+%d", rg, i, c.MetaData.PathInSchema, c.MetaData.DataPageOffset, c.ColumnIndexOffset, c.ColumnIndexLength, c.OffsetIndexOffset, c.OffsetIndexLength, c.MetaData.BloomFilterOffset, c.MetaData.BloomFilterLength)
		}
	}
	t.Logf("size %d", len(data))
}
