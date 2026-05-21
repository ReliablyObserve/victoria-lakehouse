package parquets3

import (
	"io"

	"github.com/parquet-go/parquet-go"
)

// readRowGroupProjected reads only the columns in wantCols from the row group.
// If wantCols is nil or empty, returns (nil, nil) — caller should use the full
// typed reader instead.
// Returns a slice of rows, where each row is a slice of fields for the projected columns.
func readRowGroupProjected(f *parquet.File, rg parquet.RowGroup, wantCols map[string]bool) ([][]field, error) {
	return readRowGroupProjectedBitmap(f, rg, wantCols, nil)
}

// readRowGroupProjectedBitmap is like readRowGroupProjected but applies a
// pre-where bitmap filter: only rows where bitmap[i]==true are included.
// If bitmap is nil, all rows are included.
func readRowGroupProjectedBitmap(f *parquet.File, rg parquet.RowGroup, wantCols map[string]bool, bitmap []bool) ([][]field, error) {
	if len(wantCols) == 0 {
		return nil, nil
	}

	pqSchema := f.Schema()
	allCols := pqSchema.Columns()

	var colIndices []int
	var colNames []string
	seen := make(map[string]bool)
	for i, path := range allCols {
		name := path[0]
		if wantCols[name] && !seen[name] {
			colIndices = append(colIndices, i)
			colNames = append(colNames, name)
			seen[name] = true
		}
	}

	if len(colIndices) == 0 {
		return nil, nil
	}

	rows := rg.Rows()
	defer func() { _ = rows.Close() }()

	buf := make([]parquet.Row, 256)
	var result [][]field
	rowIdx := 0
	for {
		n, err := rows.ReadRows(buf)
		for i := 0; i < n; i++ {
			if bitmap != nil && rowIdx < len(bitmap) && !bitmap[rowIdx] {
				rowIdx++
				continue
			}
			rowIdx++
			row := buf[i]
			fields := make([]field, 0, len(colIndices))
			for ci, colIdx := range colIndices {
				for _, v := range row {
					if v.Column() == colIdx {
						fields = append(fields, field{
							name:  colNames[ci],
							value: parquetValueToInterface(v),
						})
						break
					}
				}
			}
			result = append(result, fields)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return result, nil
}

func parquetValueToInterface(v parquet.Value) any {
	switch v.Kind() {
	case parquet.ByteArray:
		return string(v.ByteArray())
	case parquet.Int64:
		return v.Int64()
	case parquet.Int32:
		return int64(v.Int32())
	case parquet.Double:
		return v.Double()
	case parquet.Boolean:
		return v.Boolean()
	case parquet.FixedLenByteArray:
		return string(v.ByteArray())
	default:
		return v.String()
	}
}
