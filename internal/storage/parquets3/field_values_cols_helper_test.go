package parquets3

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func fvEmptyColumnSizes(b *testing.B, m *mockS3Server) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, data := range m.files {
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			continue
		}
		sizes := map[string]int64{}
		for _, rg := range f.Metadata().RowGroups {
			for _, c := range rg.Columns {
				sizes[c.MetaData.PathInSchema[0]] += c.MetaData.TotalCompressedSize
			}
		}
		b.Logf("file %s size=%d cols=%v", k, len(data), sizes)
	}
}
