package schema

import "strconv"

// ExtractLogLabels returns field -> distinct values for the low-cardinality
// label fields of a set of log rows: the per-file label SET the manifest's
// inverted index is built from and the pmeta field catalog is fed with.
//
// It lives here, next to the column definitions it draws from, because three
// producers of a manifest entry need it and must agree exactly: the flush
// writer (both modules), the compactor, and the delete rewriter. The rewriter
// in particular cannot reuse the superseded file's labels — a rewrite removes
// rows, and a value that only the removed rows carried would otherwise be fed
// back into the field catalog and served in field_values long after the rows
// are gone.
//
// Each field keeps at most MaxLabelAggregateValues distinct values (the same
// cap the label aggregates use, so the inverted index and the aggregate fast
// path never disagree about which fields are low-cardinality). A field AT the
// cap is treated as incomplete by every pruning path, never as exhaustive.
// Every file holds rows from exactly one tenant, so account_id / project_id are
// single-valued and embedded for the per-tenant retention and lifecycle rules.
func ExtractLogLabels(rows []LogRow) map[string][]string {
	if len(rows) == 0 {
		return nil
	}
	sets := map[string]map[string]bool{}
	for i := range rows {
		// Shared dimensional set (LogLabelColumns) — the SAME columns the
		// per-value aggregates use. High-card id-like columns (trace_id,
		// container.id) are absent by design; bloom filters handle them.
		for _, c := range LogLabelColumns {
			addLabelValue(sets, c.Name, c.Get(&rows[i]))
		}
	}
	addLabelValue(sets, "account_id", strconv.FormatUint(uint64(rows[0].AccountID), 10))
	addLabelValue(sets, "project_id", strconv.FormatUint(uint64(rows[0].ProjectID), 10))
	return labelSetsToLists(sets)
}

// ExtractTraceLabels is ExtractLogLabels for spans.
func ExtractTraceLabels(rows []TraceRow) map[string][]string {
	if len(rows) == 0 {
		return nil
	}
	sets := map[string]map[string]bool{}
	for i := range rows {
		for _, c := range TraceLabelColumns {
			addLabelValue(sets, c.Name, c.Get(&rows[i]))
		}
	}
	addLabelValue(sets, "account_id", strconv.FormatUint(uint64(rows[0].AccountID), 10))
	addLabelValue(sets, "project_id", strconv.FormatUint(uint64(rows[0].ProjectID), 10))
	return labelSetsToLists(sets)
}

func addLabelValue(sets map[string]map[string]bool, field, value string) {
	if value == "" {
		return
	}
	s, ok := sets[field]
	if !ok {
		s = make(map[string]bool)
		sets[field] = s
	}
	if len(s) < MaxLabelAggregateValues {
		s[value] = true
	}
}

func labelSetsToLists(sets map[string]map[string]bool) map[string][]string {
	labels := make(map[string][]string, len(sets))
	for k, vs := range sets {
		vals := make([]string, 0, len(vs))
		for v := range vs {
			vals = append(vals, v)
		}
		labels[k] = vals
	}
	return labels
}
