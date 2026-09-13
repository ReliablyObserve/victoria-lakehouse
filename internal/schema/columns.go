package schema

// ColumnKind classifies a top-level Parquet column by how it may surface as a
// query field on the read paths.
//
// This is the single authoritative classification: every top-level column of
// LogRow and TraceRow falls into exactly one kind, and the read paths consult
// it instead of carrying their own ad-hoc name lists. A column added to a row
// struct without being accounted for here is caught by the guard test in
// columns_test.go rather than silently leaking into query results.
type ColumnKind int

const (
	// ColumnUserVisible marks a column that carries ingested telemetry. It
	// surfaces as a query field under the registry's internal name for that
	// column (or under the column name itself when the registry has no
	// mapping, which is the case for the Tier-1 dedicated log columns).
	ColumnUserVisible ColumnKind = iota

	// ColumnInternal marks a column that exists for storage bookkeeping only.
	// It MUST never surface as a query field: hot VictoriaLogs/VictoriaTraces
	// have no such field, so emitting it makes every cold row differ from its
	// hot twin.
	ColumnInternal

	// ColumnSlot marks a Tier-2 spare slot column (ded_s01..ded_s08). A slot
	// surfaces under the attribute name the file's footer KV binds it to; an
	// unmapped slot is an empty placeholder and surfaces not at all.
	ColumnSlot
)

// InternalColumns lists the top-level Parquet columns that are storage
// bookkeeping rather than telemetry. They are written so every Parquet file
// stays self-describing for external tools (DuckDB/pyarrow see real tenant
// columns), but the query path must not turn them into LogsQL fields.
//
// account_id / project_id are the tenant coordinates; they are carried as
// manifest labels (see parquets3/labels.go) and as the physical partition
// prefix, and are never queryable fields on hot VL/VT.
var InternalColumns = []string{
	"account_id",
	"project_id",
}

var internalColumnSet = func() map[string]struct{} {
	m := make(map[string]struct{}, len(InternalColumns))
	for _, c := range InternalColumns {
		m[c] = struct{}{}
	}
	return m
}()

// IsInternalColumn reports whether a top-level Parquet column is storage
// bookkeeping that must never surface as a query field.
func IsInternalColumn(parquetColumn string) bool {
	_, ok := internalColumnSet[parquetColumn]
	return ok
}

// IsDedicatedSlotColumn reports whether a top-level Parquet column is one of
// the Tier-2 spare slots (ded_s01..ded_s08).
func IsDedicatedSlotColumn(parquetColumn string) bool {
	return len(parquetColumn) == 7 && parquetColumn[:5] == "ded_s"
}

// ClassifyColumn returns the kind of a top-level Parquet column.
func ClassifyColumn(parquetColumn string) ColumnKind {
	switch {
	case IsInternalColumn(parquetColumn):
		return ColumnInternal
	case IsDedicatedSlotColumn(parquetColumn):
		return ColumnSlot
	default:
		return ColumnUserVisible
	}
}
