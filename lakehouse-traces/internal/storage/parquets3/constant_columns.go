package parquets3

import (
	"github.com/parquet-go/parquet-go"
)

type constantColumn struct {
	name  string
	value any
}

// detectConstantColumns examines column-index stats for a row group and returns
// columns where min == max across all pages (i.e. every value is identical).
// The caller can skip deserializing these columns and inject the constant value.
func detectConstantColumns(f *parquet.File, rg parquet.RowGroup, wantCols map[string]bool) []constantColumn {
	if len(wantCols) == 0 {
		return nil
	}

	cols := rg.ColumnChunks()
	root := f.Root()
	var constants []constantColumn

	for name := range wantCols {
		colIdx := findColumnIndex(root, name)
		if colIdx < 0 || colIdx >= len(cols) {
			continue
		}

		cidx, err := cols[colIdx].ColumnIndex()
		if err != nil || cidx == nil {
			continue
		}

		numPages := cidx.NumPages()
		if numPages == 0 {
			continue
		}

		minVal := cidx.MinValue(0)
		maxVal := cidx.MaxValue(0)

		if minVal.IsNull() || maxVal.IsNull() {
			continue
		}

		if parquet.Equal(minVal, maxVal) {
			allEqual := true
			for p := 1; p < numPages; p++ {
				pMin := cidx.MinValue(p)
				pMax := cidx.MaxValue(p)
				if !parquet.Equal(pMin, minVal) || !parquet.Equal(pMax, minVal) {
					allEqual = false
					break
				}
			}
			if allEqual {
				constants = append(constants, constantColumn{
					name:  name,
					value: parquetValueToInterface(minVal),
				})
			}
		}
	}

	return constants
}
