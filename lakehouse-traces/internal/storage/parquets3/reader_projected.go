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

type colSpec struct {
	name   string
	idx    int
	isMap  bool
	keyIdx int
	valIdx int
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

	type leafInfo struct {
		indices []int
		paths   [][]string
	}
	leafMap := make(map[string]*leafInfo)
	for i, path := range allCols {
		name := path[0]
		if !wantCols[name] {
			continue
		}
		li, ok := leafMap[name]
		if !ok {
			li = &leafInfo{}
			leafMap[name] = li
		}
		li.indices = append(li.indices, i)
		li.paths = append(li.paths, path)
	}

	var specs []colSpec
	for name, li := range leafMap {
		if len(li.indices) == 1 {
			specs = append(specs, colSpec{name: name, idx: li.indices[0]})
		} else if len(li.indices) >= 2 {
			keyIdx, valIdx := -1, -1
			for j, p := range li.paths {
				if len(p) >= 3 && p[2] == "key" {
					keyIdx = li.indices[j]
				} else if len(p) >= 3 && p[2] == "value" {
					valIdx = li.indices[j]
				}
			}
			if keyIdx >= 0 && valIdx >= 0 {
				specs = append(specs, colSpec{name: name, isMap: true, keyIdx: keyIdx, valIdx: valIdx})
			} else {
				specs = append(specs, colSpec{name: name, idx: li.indices[0]})
			}
		}
	}

	if len(specs) == 0 {
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
			fields := make([]field, 0, len(specs))
			for _, spec := range specs {
				if spec.isMap {
					var keys, vals []string
					for _, v := range row {
						col := v.Column()
						if col == spec.keyIdx {
							keys = append(keys, parquetValueToString(v))
						} else if col == spec.valIdx {
							vals = append(vals, parquetValueToString(v))
						}
					}
					m := make(map[string]string, len(keys))
					for j := 0; j < len(keys) && j < len(vals); j++ {
						m[keys[j]] = vals[j]
					}
					fields = append(fields, field{
						name:  spec.name,
						value: m,
					})
				} else {
					for _, v := range row {
						if v.Column() == spec.idx {
							fields = append(fields, field{
								name:  spec.name,
								value: parquetValueToInterface(v),
							})
							break
						}
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
