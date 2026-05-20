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
	if len(wantCols) == 0 {
		return nil, nil
	}

	pqSchema := f.Schema()
	allCols := pqSchema.Columns()

	// Build column index mask. For MAP columns (e.g. resource.attributes),
	// the schema expands into nested paths like ["resource.attributes", "key_value", "key"].
	// We match only on path[0] (the top-level column name) and deduplicate.
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
	defer rows.Close()

	buf := make([]parquet.Row, 256)
	var result [][]field
	for {
		n, err := rows.ReadRows(buf)
		for i := 0; i < n; i++ {
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
