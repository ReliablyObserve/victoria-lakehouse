package parquets3

import (
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// queryFieldName maps a top-level Parquet column to the LogsQL field name the
// cold read paths must emit it under, reporting false when the column must not
// surface as a query field at all.
//
// This is the single rule both scan paths obey — the columnar fast path
// (readRowGroupColumnar) and the row-oriented slow path
// (projectedFieldsToDataBlock) — so a cold row carries exactly the fields hot
// VictoriaLogs returns for the same ingested row:
//
//   - storage bookkeeping columns (tenant ids) never surface;
//   - a Tier-2 spare slot surfaces under the attribute name the file's footer
//     KV binds it to, and an unmapped slot not at all (same rule the typed path
//     applies in remapSlotFields);
//   - everything else surfaces under the registry's internal name for the
//     column, falling back to the column name itself when the registry has no
//     mapping (the Tier-1 dedicated columns, which logRowToFields also emits
//     under their bare attribute name).
func queryFieldName(parquetCol string, reg *schema.Registry, slots schema.SlotMapping) (string, bool) {
	switch schema.ClassifyColumn(parquetCol) {
	case schema.ColumnInternal:
		return "", false
	case schema.ColumnSlot:
		name, ok := slots[parquetCol]
		if !ok || name == "" {
			return "", false
		}
		return name, true
	}
	if m := reg.ResolveFromParquet(parquetCol); m != nil {
		return m.InternalName, true
	}
	return parquetCol, true
}
