package bloomindex

// NewFileBloomIndex creates a bloom index for a single file's column values.
// Reuses the existing Index and Filter types — no new data structures.
func NewFileBloomIndex(columnValues map[string][]string, fpRate float64) *Index {
	idx := New()
	const fileKey = "_" // single-file index uses a placeholder key
	cols := make(map[string]*Filter, len(columnValues))
	for col, vals := range columnValues {
		if ShouldSkipBloom(len(vals)) {
			continue
		}
		f := NewFilter(len(vals), fpRate)
		for _, v := range vals {
			f.Add(v)
		}
		cols[col] = f
	}
	if len(cols) > 0 {
		idx.AddColumns(fileKey, cols)
	}
	return idx
}

// FileBloomMayContain checks if a file-level bloom sidecar might contain
// the given column=value. Returns true (assume present) if no filter
// exists for the column.
func FileBloomMayContain(idx *Index, column, value string) bool {
	const fileKey = "_"
	result := idx.MayContain([]string{fileKey}, column, value)
	return len(result) > 0
}

// FileBloomMayContainAll checks multiple column/value pairs against a
// file-level bloom. Returns true if ALL values might be present.
func FileBloomMayContainAll(idx *Index, checks []ColumnCheck) bool {
	const fileKey = "_"
	result := idx.MayContainAll([]string{fileKey}, checks)
	return len(result) > 0
}
