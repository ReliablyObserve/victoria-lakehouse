package parquets3

import (
	"io"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// readRowGroupColumnar reads projected columns directly into DataBlock columns,
// bypassing the []field intermediate representation. For scalar columns, values
// are read and formatted in a single pass per column. MAP columns are handled
// by reading key/value leaf columns and assembling per-row maps.
// slots is the file's ded_sNN -> configured attribute name binding (from the
// Parquet footer KV); aliasCols holds the promoted Parquet column names the
// query spelled directly. Together they decide how each column is named and
// which columns surface at all — see queryFieldName / emitParquetNameAlias.
func readRowGroupColumnar(
	f *parquet.File,
	rg parquet.RowGroup,
	wantCols map[string]bool,
	reg *schema.Registry,
	startNs, endNs int64,
	bitmap []bool,
	slots schema.SlotMapping,
	aliasCols map[string]bool,
) *logstorage.DataBlock {
	if len(wantCols) == 0 {
		return nil
	}

	pqSchema := f.Schema()
	allCols := pqSchema.Columns()
	chunks := rg.ColumnChunks()
	numRows := int(rg.NumRows())

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

	if len(leafMap) == 0 {
		return nil
	}

	// First pass: read timestamp column to build row filter mask.
	// This determines which rows pass the time range filter.
	var rowMask []bool
	var tsValues []int64
	tsIdx := -1
	for i, path := range allCols {
		if path[0] == "timestamp_unix_nano" {
			tsIdx = i
			break
		}
	}
	if tsIdx >= 0 {
		tsValues = readInt64Column(chunks[tsIdx], numRows)
		rowMask = make([]bool, numRows)
		for i, ts := range tsValues {
			if bitmap != nil && i < len(bitmap) && !bitmap[i] {
				continue
			}
			if ts >= startNs && ts <= endNs {
				rowMask[i] = true
			}
		}
	} else {
		rowMask = make([]bool, numRows)
		for i := range rowMask {
			if bitmap != nil && i < len(bitmap) && !bitmap[i] {
				continue
			}
			rowMask[i] = true
		}
	}

	// Count passing rows for pre-allocation.
	passCount := 0
	for _, pass := range rowMask {
		if pass {
			passCount++
		}
	}
	if passCount == 0 {
		return nil
	}

	var blockCols []logstorage.BlockColumn

	// Collect scalar column names so MAP expansion can skip duplicates.
	scalarNames := make(map[string]bool)
	for name, li := range leafMap {
		if len(li.indices) == 1 {
			scalarNames[name] = true
		}
	}

	for name, li := range leafMap {
		if len(li.indices) == 1 {
			// Scalar column. queryFieldName is the shared rule with the
			// row-oriented path (projectedFieldsToDataBlock): it drops
			// bookkeeping columns and unmapped slots and resolves the
			// emitted field name (VT's resource_attr:/span_attr: prefix
			// included), so the two paths cannot drift.
			internalName, ok := queryFieldName(name, reg, slots)
			if !ok {
				continue
			}

			values := readScalarColumnFormatted(chunks[li.indices[0]], numRows, rowMask, passCount, internalName, reg)
			if values != nil {
				blockCols = append(blockCols, logstorage.BlockColumn{
					Name:   internalName,
					Values: values,
				})
				// Alias emission for promoted columns whose parquet name
				// differs from the internal name (e.g. parquet
				// `service.name` ↔ internal `resource_attr:service.name`).
				// A user-typed filter spelling the parquet dialect must
				// resolve to a column the DataBlock actually carries;
				// without this, `service.name:="X"` resolves to a column
				// that doesn't exist in the block and matches zero rows
				// even though `_stream:{resource_attr:service.name="X"}`
				// finds 78k rows in the same time window (a5576bf, pinned
				// by TestColdHotParity_FieldEqByParquetName). Gated on the
				// query actually spelling the parquet name so a wildcard
				// span list carries the VT field names alone.
				if emitParquetNameAlias(name, internalName, aliasCols) {
					blockCols = append(blockCols, logstorage.BlockColumn{
						Name:   name,
						Values: values,
					})
				}
			}
		} else if len(li.indices) >= 2 {
			// MAP column: find key and value leaf indices.
			keyIdx, valIdx := -1, -1
			for j, p := range li.paths {
				if len(p) >= 3 && p[2] == "key" {
					keyIdx = li.indices[j]
				} else if len(p) >= 3 && p[2] == "value" {
					valIdx = li.indices[j]
				}
			}
			if keyIdx >= 0 && valIdx >= 0 {
				mapCols := readMapColumnToBlockCols(chunks[keyIdx], chunks[valIdx], numRows, rowMask, passCount, name, scalarNames, reg.TopLevelMapKeys())
				blockCols = append(blockCols, mapCols...)
			}
		}
	}

	if len(blockCols) == 0 {
		return nil
	}

	db := &logstorage.DataBlock{}
	db.SetColumns(blockCols)
	return db
}

// readInt64Column reads all values from an int64 column chunk.
func readInt64Column(chunk parquet.ColumnChunk, numRows int) []int64 {
	result := make([]int64, 0, numRows)
	pages := chunk.Pages()
	defer func() { _ = pages.Close() }()

	buf := make([]parquet.Value, 256)
	for {
		page, err := pages.ReadPage()
		if err != nil {
			if err == io.EOF {
				break
			}
			return result
		}
		vr := page.Values()
		for {
			n, err := vr.ReadValues(buf[:])
			for i := 0; i < n; i++ {
				result = append(result, buf[i].Int64())
			}
			if err != nil {
				break
			}
		}
	}
	return result
}

// readScalarColumnFormatted reads a scalar column and formats values directly
// to strings. Returns nil when no row in the group carries a value for the
// column: a column that is empty for every row is a field that does not exist
// on any of these rows, and hot VictoriaTraces never reports such a field.
func readScalarColumnFormatted(
	chunk parquet.ColumnChunk,
	numRows int,
	rowMask []bool,
	passCount int,
	internalName string,
	reg *schema.Registry,
) []string {
	pages := chunk.Pages()
	defer func() { _ = pages.Close() }()

	values := make([]string, 0, passCount)
	buf := make([]parquet.Value, 256)
	rowIdx := 0
	nonEmpty := false

	for {
		page, err := pages.ReadPage()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nonEmptyOrNil(values, nonEmpty)
		}
		vr := page.Values()
		for {
			n, readErr := vr.ReadValues(buf[:])
			for i := 0; i < n; i++ {
				if rowIdx < len(rowMask) && rowMask[rowIdx] {
					// NULL and empty cells format to "" (see
					// parquetValueToInterface): the span simply has no such
					// field, matching the typed path's appendIfSet.
					v := parquetValueToInterface(buf[i])
					formatted := reg.FormatField(internalName, v)
					if formatted != "" {
						nonEmpty = true
					}
					values = append(values, formatted)
				}
				rowIdx++
			}
			if readErr != nil {
				break
			}
		}
	}
	return nonEmptyOrNil(values, nonEmpty)
}

// nonEmptyOrNil returns values only when at least one row carried a value.
func nonEmptyOrNil(values []string, nonEmpty bool) []string {
	if !nonEmpty {
		return nil
	}
	return values
}

// readMapColumnToBlockCols reads MAP key/value columns and produces per-attribute BlockColumns.
func readMapColumnToBlockCols(
	keyChunk, valChunk parquet.ColumnChunk,
	numRows int,
	rowMask []bool,
	passCount int,
	mapColName string,
	promotedKeys map[string]bool,
	topLevelKeys map[string]bool,
) []logstorage.BlockColumn {
	// Resolved once per column; mapAttrFieldNameWithPrefix applies the shared
	// naming rule per attribute.
	prefix := mapColumnToAttrPrefix(mapColName)

	// Read all key and value entries with their repetition levels
	// to reconstruct per-row maps.
	type kvEntry struct {
		key string
		row int
	}

	keyPages := keyChunk.Pages()
	defer func() { _ = keyPages.Close() }()

	valPages := valChunk.Pages()
	defer func() { _ = valPages.Close() }()

	keyBuf := make([]parquet.Value, 256)
	valBuf := make([]parquet.Value, 256)

	// Read all keys with row tracking via repetition levels.
	var keys []kvEntry
	rowIdx := 0
	for {
		page, err := keyPages.ReadPage()
		if err != nil {
			break
		}
		vr := page.Values()
		for {
			n, readErr := vr.ReadValues(keyBuf[:])
			for i := 0; i < n; i++ {
				if keyBuf[i].RepetitionLevel() == 0 && len(keys) > 0 {
					rowIdx++
				}
				keys = append(keys, kvEntry{
					key: parquetValueToString(keyBuf[i]),
					row: rowIdx,
				})
			}
			if readErr != nil {
				break
			}
		}
	}

	// Read values.
	var vals []string
	for {
		page, err := valPages.ReadPage()
		if err != nil {
			break
		}
		vr := page.Values()
		for {
			n, readErr := vr.ReadValues(valBuf[:])
			for i := 0; i < n; i++ {
				vals = append(vals, parquetValueToString(valBuf[i]))
			}
			if readErr != nil {
				break
			}
		}
	}

	// Pre-compute row → pass index mapping.
	rowToPass := make([]int, len(rowMask))
	passIdx := 0
	for i, pass := range rowMask {
		rowToPass[i] = passIdx
		if pass {
			passIdx++
		}
	}

	// Group by attribute name.
	type attrCol struct {
		values []string
	}
	attrMap := make(map[string]*attrCol)
	var attrOrder []string

	for i, kv := range keys {
		if kv.row >= len(rowMask) || !rowMask[kv.row] {
			continue
		}
		if i >= len(vals) || vals[i] == "" {
			continue
		}
		if promotedKeys[kv.key] {
			continue
		}
		// Same naming rule the scalar columns go through: a MAP key that
		// spells a reserved internal name never becomes a field.
		attrName, ok := mapAttrFieldNameWithPrefix(prefix, kv.key, topLevelKeys)
		if !ok {
			continue
		}
		ac, ok := attrMap[attrName]
		if !ok {
			ac = &attrCol{values: make([]string, passCount)}
			attrMap[attrName] = ac
			attrOrder = append(attrOrder, attrName)
		}
		pi := rowToPass[kv.row]
		if pi < passCount {
			ac.values[pi] = vals[i]
		}
	}

	result := make([]logstorage.BlockColumn, 0, len(attrOrder))
	for _, name := range attrOrder {
		ac := attrMap[name]
		result = append(result, logstorage.BlockColumn{
			Name:   name,
			Values: ac.values,
		})
	}
	return result
}
