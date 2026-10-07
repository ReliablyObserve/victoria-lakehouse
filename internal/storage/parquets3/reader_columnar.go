package parquets3

import (
	"io"
	"sort"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// readRowGroupColumnar reads projected columns directly into DataBlock columns,
// bypassing the []field intermediate representation. For scalar columns, values
// are read and formatted in a single pass per column. MAP columns are handled
// by reading key/value leaf columns and assembling per-row maps.
// slots is the file's ded_sNN -> configured attribute name binding (from the
// Parquet footer KV); it decides how Tier-2 spare slots are named and which of
// them are dropped, exactly as remapSlotFields does on the typed path.
// readRowGroupColumnar reads every attribute of the MAP columns it expands; see
// readRowGroupColumnarKeys to read only some.
func readRowGroupColumnar(
	f *parquet.File,
	rg parquet.RowGroup,
	wantCols map[string]bool,
	reg *schema.Registry,
	startNs, endNs int64,
	bitmap []bool,
	slots schema.SlotMapping,
) *logstorage.DataBlock {
	return readRowGroupColumnarKeys(f, rg, wantCols, reg, startNs, endNs, bitmap, slots, nil)
}

// readRowGroupColumnarKeys is readRowGroupColumnar that, when onlyKeys is not
// nil, expands from each MAP column only the attributes whose raw key is in
// the set (a request for one attribute then does not stringify every key and
// value of the map). Scalar columns are unaffected.
func readRowGroupColumnarKeys(
	f *parquet.File,
	rg parquet.RowGroup,
	wantCols map[string]bool,
	reg *schema.Registry,
	startNs, endNs int64,
	bitmap []bool,
	slots schema.SlotMapping,
	onlyKeys map[string]struct{},
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

	for _, name := range orderedLeafNames(leafMap) {
		li := leafMap[name]
		if len(li.indices) == 1 {
			// Scalar column. queryFieldName is the shared rule with the
			// row-oriented path (projectedFieldsToDataBlock): it drops
			// bookkeeping columns and unmapped slots and resolves the
			// emitted field name, so the two paths cannot drift.
			internalName, ok := queryFieldName(name, reg, slots)
			if !ok {
				continue
			}
			// A restricted read (onlyKeys) that does not name _time needs the
			// timestamps for the row mask only, read above: formatting every
			// one of them to a string is two allocations per row for nothing.
			// The column stays, empty, so the block still counts its rows.
			if _, want := onlyKeys["_time"]; onlyKeys != nil && !want && internalName == "_time" {
				blockCols = append(blockCols, logstorage.BlockColumn{Name: internalName, Values: make([]string, passCount)})
				continue
			}

			values := readScalarColumnFormatted(chunks[li.indices[0]], numRows, rowMask, passCount, internalName, reg)
			if values != nil {
				blockCols = append(blockCols, logstorage.BlockColumn{
					Name:   internalName,
					Values: values,
				})
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
				mapCols := readMapColumnToBlockColsKeys(chunks[keyIdx], chunks[valIdx], numRows, rowMask, passCount, name, scalarNames, onlyKeys, reg)
				blockCols = append(blockCols, mapCols...)
			}
		}
	}

	if len(blockCols) == 0 {
		return nil
	}
	blockCols = mergeDuplicateColumns(blockCols)

	db := &logstorage.DataBlock{}
	db.SetColumns(blockCols)
	orderColumnsLikeUpstream(db)
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
// on any of these rows, and hot VictoriaLogs never reports such a field.
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
					// parquetValueToInterface): the row simply has no such
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
	registries ...*schema.Registry,
) []logstorage.BlockColumn {
	return readMapColumnToBlockColsKeys(keyChunk, valChunk, numRows, rowMask, passCount, mapColName, promotedKeys, nil, registries...)
}

// readMapColumnToBlockColsKeys is readMapColumnToBlockCols that, with onlyKeys
// not nil, keeps only the entries whose raw key is in the set: the other keys
// and their values are never turned into strings.
func readMapColumnToBlockColsKeys(
	keyChunk, valChunk parquet.ColumnChunk,
	numRows int,
	rowMask []bool,
	passCount int,
	mapColName string,
	promotedKeys map[string]bool,
	onlyKeys map[string]struct{},
	registries ...*schema.Registry,
) []logstorage.BlockColumn {
	// Resolved once per column; mapAttrFieldNameWithPrefix applies the shared
	// naming rule per attribute.

	// Read all key and value entries with their repetition levels
	// to reconstruct per-row maps.
	type kvEntry struct {
		key string
		row int
		pos int // index of the entry among all entries of the column chunk
	}

	keyPages := keyChunk.Pages()
	defer func() { _ = keyPages.Close() }()

	valPages := valChunk.Pages()
	defer func() { _ = valPages.Close() }()

	keyBuf := make([]parquet.Value, 256)
	valBuf := make([]parquet.Value, 256)

	// Read the keys with row tracking via repetition levels. With onlyKeys
	// set, an entry whose key is not in the set is counted (rows and positions
	// still advance) but neither kept nor turned into a string.
	var keys []kvEntry
	rowIdx, pos := 0, 0
	for {
		page, err := keyPages.ReadPage()
		if err != nil {
			break
		}
		vr := page.Values()
		for {
			n, readErr := vr.ReadValues(keyBuf[:])
			for i := 0; i < n; i++ {
				if keyBuf[i].RepetitionLevel() == 0 && pos > 0 {
					rowIdx++
				}
				if onlyKeys != nil {
					if keyBuf[i].IsNull() {
						pos++
						continue
					}
					if _, ok := onlyKeys[string(keyBuf[i].ByteArray())]; !ok {
						pos++
						continue
					}
				}
				keys = append(keys, kvEntry{
					key: parquetValueToString(keyBuf[i]),
					row: rowIdx,
					pos: pos,
				})
				pos++
			}
			if readErr != nil {
				break
			}
		}
	}

	// Read the values of the kept entries only (keys is in position order).
	vals := make([]string, len(keys))
	vpos, next := 0, 0
	for next < len(keys) {
		page, err := valPages.ReadPage()
		if err != nil {
			break
		}
		vr := page.Values()
		for {
			n, readErr := vr.ReadValues(valBuf[:])
			for i := 0; i < n && next < len(keys); i++ {
				if vpos == keys[next].pos {
					vals[next] = parquetValueToString(valBuf[i])
					next++
				}
				vpos++
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
		if promotedKeys[kv.key] && (kv.key != "body" || len(registries) == 0 || registries[0] == nil || registries[0].ResolveFromParquet("span.name") == nil) {
			continue
		}
		// Same naming rule the scalar columns go through: a MAP key that
		// spells a reserved internal name never becomes a field.
		attrName, ok := mapAttrFieldName(mapColName, kv.key, registries...)
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

// mergeDuplicateColumns folds columns that carry the same field name into one:
// one attribute key present in two MAP columns (resource and log attributes of
// a log row) is one field, and per row the first non-empty value wins. Without
// it a row that has the key in only one of them reads as carrying it twice,
// once empty. Columns keep the order of their first occurrence.
func mergeDuplicateColumns(cols []logstorage.BlockColumn) []logstorage.BlockColumn {
	first := make(map[string]int, len(cols))
	dup := false
	for i := range cols {
		if _, ok := first[cols[i].Name]; ok {
			dup = true
			break
		}
		first[cols[i].Name] = i
	}
	if !dup {
		return cols
	}
	first = make(map[string]int, len(cols))
	out := make([]logstorage.BlockColumn, 0, len(cols))
	for _, c := range cols {
		i, ok := first[c.Name]
		if !ok {
			first[c.Name] = len(out)
			out = append(out, c)
			continue
		}
		base := out[i].Values
		merged := make([]string, len(base))
		copy(merged, base)
		for r, v := range c.Values {
			if r < len(merged) && merged[r] == "" {
				merged[r] = v
			}
		}
		out[i].Values = merged
	}
	return out
}

// orderedLeafNames lists the projected top-level columns in a fixed order: the
// attribute MAP columns resource, log, span, scope first (the order OTLP and hot
// VictoriaLogs/VictoriaTraces see them in), then everything else by name. Where
// one attribute key is in two MAP columns the first occurrence wins
// (mergeDuplicateColumns), so the answer must not depend on Go's map order.
func orderedLeafNames[V any](m map[string]V) []string {
	rank := map[string]int{"resource.attributes": 0, "log.attributes": 1, "span.attributes": 2, "scope.attributes": 3}
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Slice(names, func(a, b int) bool {
		ra, oka := rank[names[a]]
		rb, okb := rank[names[b]]
		switch {
		case oka && okb:
			return ra < rb
		case oka != okb:
			return !oka // plain columns first, as before; maps keep their relative order
		}
		return names[a] < names[b]
	})
	return names
}
