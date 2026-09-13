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
// (projectedFieldsToDataBlock) — so a cold span carries exactly the fields hot
// VictoriaTraces returns for the same ingested span:
//
//   - storage bookkeeping columns (tenant ids) never surface;
//   - a Tier-2 spare slot surfaces under the attribute name the file's footer
//     KV binds it to, and an unmapped slot not at all (same rule the typed path
//     applies in remapSlotFields);
//   - everything else surfaces under the registry's internal name, which for
//     traces carries VT's `resource_attr:` / `span_attr:` prefix (parquet
//     `service.name` -> `resource_attr:service.name`).
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

// emitParquetNameAlias reports whether a promoted column must ALSO be emitted
// under its raw Parquet name next to its VT-prefixed internal name.
//
// Operators routinely type the Parquet spelling (`service.name:="api-gw"`)
// because that is what a Parquet tool shows, and VL's filters match on the field
// names the DataBlock actually carries — so without the alias such a query
// matches zero rows (the a5576bf class of silent-empty bug).
//
// The alias is emitted ONLY when the query spells the Parquet name, either in a
// filter term or in a column-selecting pipe. On every other query — a wildcard
// Grafana span list above all — the row carries the VT field names alone, as hot
// VictoriaTraces returns them. Emitting the alias unconditionally used to double
// the field set of every cold span (`service.name` next to
// `resource_attr:service.name`, `span.name` next to `name`,
// `timestamp_unix_nano` next to `_time`, ...).
func emitParquetNameAlias(parquetCol, internalName string, aliasCols map[string]bool) bool {
	return parquetCol != "" && parquetCol != internalName && aliasCols[parquetCol]
}

// queryParquetNameAliases returns the promoted Parquet column names the query
// spells directly (rather than by their VT internal name), so the scan paths
// know which alias columns the filter and pipe stages need.
func queryParquetNameAliases(queryStr string, reg *schema.Registry, pipeFields []string) map[string]bool {
	var aliases map[string]bool
	add := func(col string) {
		if aliases == nil {
			aliases = make(map[string]bool, 4)
		}
		aliases[col] = true
	}
	for _, fm := range reg.PromotedColumns() {
		if fm.ParquetColumn == fm.InternalName {
			continue
		}
		if referencesField(queryStr, fm.ParquetColumn) {
			add(fm.ParquetColumn)
			continue
		}
		for _, pf := range pipeFields {
			if pf == fm.ParquetColumn {
				add(fm.ParquetColumn)
				break
			}
		}
	}
	return aliases
}
