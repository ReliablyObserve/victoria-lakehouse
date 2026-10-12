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

		// Min and max ignore NULL cells. A page that holds NULLs next to values
		// is not constant: injecting its min/max would give the NULL rows a
		// value they never had (an absent severity_number turned into the
		// neighbouring row's value, issue #274).
		hasNulls := false
		for p := 0; p < numPages; p++ {
			if cidx.NullPage(p) || cidx.NullCount(p) > 0 {
				hasNulls = true
				break
			}
		}
		if hasNulls {
			continue
		}

		// ByteArray (variable-length string/binary) min/max may be
		// truncated by the writer per Apache Parquet's PageIndex spec.
		// Treating a truncated min == max as a constant injects the
		// truncated bytes as the row value, which surfaces as e.g.
		// "notification-ser" in /select/jaeger/api/services. Skip the
		// optimization for ByteArray; fixed-width kinds (Int64, Float,
		// FixedLenByteArray, etc.) are not truncated and remain safe.
		if minVal.Kind() == parquet.ByteArray {
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
