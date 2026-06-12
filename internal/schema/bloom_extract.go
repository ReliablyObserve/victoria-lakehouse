package schema

// The pmeta FILE-LEVEL bloom set is the single source of truth in
// LogBloomValueColumns / TraceBloomValueColumns (see bloom_value_columns.go),
// derived from the Parquet HasBloom set so it can never drift. Flush
// (internal/storage/parquets3/bloom_build.go) AND compaction
// (internal/compaction/compactor.go) extract through these same SoT columns, so a
// compacted file's combined bloom covers exactly the columns its inputs did.

// ExtractLogBloomValues collects the distinct values of every bloomed log column
// (per LogBloomValueColumns) across the given rows. Called on a compaction's merged
// rows it yields the UNION across all merged inputs — the combined bloom that lets
// the compacted file stay file-level bloom-prunable. Returns nil for no rows / no
// values. Byte-identical to the flush path, which iterates the same SoT columns.
func ExtractLogBloomValues(rows []LogRow) map[string][]string {
	if len(rows) == 0 {
		return nil
	}
	sets := make(map[string]map[string]bool, len(LogBloomValueColumns))
	for _, c := range LogBloomValueColumns {
		sets[c.Name] = make(map[string]bool)
	}
	for i := range rows {
		for _, c := range LogBloomValueColumns {
			if v := c.Get(&rows[i]); v != "" {
				sets[c.Name][v] = true
			}
		}
	}
	return bloomSetsToMap(sets)
}

// ExtractTraceBloomValues is ExtractLogBloomValues for trace rows, driven by
// TraceBloomValueColumns.
func ExtractTraceBloomValues(rows []TraceRow) map[string][]string {
	if len(rows) == 0 {
		return nil
	}
	sets := make(map[string]map[string]bool, len(TraceBloomValueColumns))
	for _, c := range TraceBloomValueColumns {
		sets[c.Name] = make(map[string]bool)
	}
	for i := range rows {
		for _, c := range TraceBloomValueColumns {
			if v := c.Get(&rows[i]); v != "" {
				sets[c.Name][v] = true
			}
		}
	}
	return bloomSetsToMap(sets)
}

func bloomSetsToMap(sets map[string]map[string]bool) map[string][]string {
	result := make(map[string][]string, len(sets))
	for col, vs := range sets {
		if len(vs) == 0 {
			continue
		}
		vals := make([]string, 0, len(vs))
		for v := range vs {
			vals = append(vals, v)
		}
		result[col] = vals
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
