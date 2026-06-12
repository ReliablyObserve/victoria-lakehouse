package schema

// The pmeta bloom set — the columns fed into the per-partition FILE-LEVEL bloom
// (_bloom.bin / pmeta FacetBloom). Deliberately narrower than the per-row-group
// FOOTER bloom set (LogBloomColumns/TraceBloomColumns): only the highest-value
// equality-lookup columns earn a file-level skip bloom (trace_id for the id lookup,
// service.name for the dominant filter). Flush AND compaction extract through these
// same functions so a compacted file's bloom covers exactly what its inputs did.

// ExtractLogBloomValues collects the distinct pmeta-bloom column values across the
// given log rows. Called on a compaction's merged rows it yields the UNION across all
// merged inputs — i.e. the combined bloom that lets the compacted file stay
// file-level bloom-prunable. Returns nil for no rows / no values.
func ExtractLogBloomValues(rows []LogRow) map[string][]string {
	if len(rows) == 0 {
		return nil
	}
	traceIDs := make(map[string]bool)
	services := make(map[string]bool)
	for i := range rows {
		if rows[i].TraceID != "" {
			traceIDs[rows[i].TraceID] = true
		}
		if rows[i].ServiceName != "" {
			services[rows[i].ServiceName] = true
		}
	}
	return bloomSetsToMap(map[string]map[string]bool{
		"trace_id":     traceIDs,
		"service.name": services,
	})
}

// ExtractTraceBloomValues is ExtractLogBloomValues for trace rows.
func ExtractTraceBloomValues(rows []TraceRow) map[string][]string {
	if len(rows) == 0 {
		return nil
	}
	traceIDs := make(map[string]bool)
	services := make(map[string]bool)
	for i := range rows {
		if rows[i].TraceID != "" {
			traceIDs[rows[i].TraceID] = true
		}
		if rows[i].ServiceName != "" {
			services[rows[i].ServiceName] = true
		}
	}
	return bloomSetsToMap(map[string]map[string]bool{
		"trace_id":     traceIDs,
		"service.name": services,
	})
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
