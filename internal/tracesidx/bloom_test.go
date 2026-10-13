package tracesidx

import (
	"bytes"
	"fmt"

	"github.com/parquet-go/parquet-go"
)

// checkBloomImpl reads the object with the stock library and asks the standard bloom
// filter of every row group about every id of the rows that group holds (no false
// negative), and about ids that are not there (a false positive rate near 1 %).
func checkBloomImpl(data []byte, rows []Row) error {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return err
	}
	idCol := -1
	for i, c := range f.Schema().Columns() {
		if c[0] == IDField {
			idCol = i
		}
	}
	sorted := append([]Row(nil), rows...)
	Sort(sorted)
	pos := 0
	var fp, probes int
	for _, g := range f.RowGroups() {
		bf := g.ColumnChunks()[idCol].BloomFilter()
		if bf == nil {
			return fmt.Errorf("no bloom filter")
		}
		n := int(g.NumRows())
		for _, r := range sorted[pos : pos+n] {
			ok, err := bf.Check(parquet.ValueOf(r.TraceID))
			if err != nil || !ok {
				return fmt.Errorf("false negative for %s: %v", r.TraceID, err)
			}
		}
		pos += n
		for i := 0; i < 2000; i++ {
			ok, _ := bf.Check(parquet.ValueOf(fmt.Sprintf("absent-%d-%d", pos, i)))
			probes++
			if ok {
				fp++
			}
		}
	}
	if rate := float64(fp) / float64(probes); rate > 0.05 {
		return fmt.Errorf("false positive rate %.3f", rate)
	}
	return nil
}
