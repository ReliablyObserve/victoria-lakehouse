package parquets3

import (
	"strings"

	"github.com/parquet-go/parquet-go"
)

// prewhereFilter reads only the filter columns from a row group and returns
// a bitmap indicating which rows match all predicates. Returns nil if no
// filtering can be done (no checks, missing columns, etc).
func prewhereFilter(f *parquet.File, rg parquet.RowGroup, pdf *PushDownFilter) []bool {
	numRows := int(rg.NumRows())
	if numRows == 0 || pdf == nil || len(pdf.Checks) == 0 {
		return nil
	}

	cols := rg.ColumnChunks()
	var bitmap []bool

	for _, check := range pdf.Checks {
		colIdx := check.ColIdx
		if colIdx < 0 {
			colIdx = findColumnIndex(f.Root(), check.Column)
		}
		if colIdx < 0 || colIdx >= len(cols) {
			continue
		}

		pages := cols[colIdx].Pages()
		if bitmap == nil {
			bitmap = make([]bool, numRows)
			for i := range bitmap {
				bitmap[i] = true
			}
		}

		rowIdx := 0
		buf := make([]parquet.Value, 256)
		for {
			page, err := pages.ReadPage()
			if err != nil || page == nil {
				break
			}
			values := page.Values()
			for {
				n, readErr := values.ReadValues(buf)
				for i := 0; i < n && rowIdx < numRows; i++ {
					if bitmap[rowIdx] {
						bitmap[rowIdx] = valueMatchesCheck(buf[i], check)
					}
					rowIdx++
				}
				if readErr != nil {
					break
				}
			}
		}
		_ = pages.Close()
	}

	if bitmap == nil {
		return nil
	}

	matchCount := 0
	for _, b := range bitmap {
		if b {
			matchCount++
		}
	}
	if matchCount == numRows {
		return nil
	}

	return bitmap
}

func valueMatchesCheck(v parquet.Value, check PushDownCheck) bool {
	s := parquetValueToString(v)
	switch check.Op {
	case PushDownExact:
		return s == check.Value
	case PushDownPrefix:
		return strings.HasPrefix(s, check.Value)
	case PushDownGreaterThan:
		return s > check.Value
	case PushDownLessThan:
		return s < check.Value
	default:
		return true
	}
}
