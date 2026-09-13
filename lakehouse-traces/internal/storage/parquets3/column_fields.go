package parquets3

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// emittableFieldName is THE suppression rule for the cold read paths: every
// field name they are about to emit passes through it, whether it came from a
// top-level Parquet column or from a key inside a MAP column. A name the schema
// classifies as anything but user-visible — a tenant bookkeeping column, a
// reserved Tier-2 slot name — never reaches the query engine, no matter which
// column produced it.
//
// Checking the name that is actually EMITTED (not the column it came from) is
// what makes one rule enough: a span attribute named `account_id` surfaces as
// `span_attr:account_id` and is a legitimate user attribute, while a key VT
// spells at top level (schema.VTTopLevelSpanAttrKeys) would otherwise introduce
// exactly the field a top-level column is forbidden to produce.
func emittableFieldName(name string) bool {
	return name != "" && schema.ClassifyColumn(name) == schema.ColumnUserVisible
}

// queryFieldName maps a top-level Parquet column to the LogsQL field name the
// cold read paths must emit it under, reporting false when the column must not
// surface as a query field at all.
//
// Together with mapAttrFieldName this is the single naming rule both scan paths
// obey — the columnar fast path (readRowGroupColumnar) and the row-oriented slow
// path (projectedFieldsToDataBlock) — so a cold span carries exactly the fields
// hot VictoriaTraces returns for the same ingested span:
//
//   - a Tier-2 spare slot is renamed to the attribute name the file's footer KV
//     binds it to (and dropped when the slot is unbound) — the same rule the
//     typed path applies in remapSlotFields;
//   - everything else resolves through the registry's internal name, which for
//     traces carries VT's `resource_attr:` / `span_attr:` prefix (parquet
//     `service.name` -> `resource_attr:service.name`);
//   - the resulting name must pass emittableFieldName.
func queryFieldName(parquetCol string, reg *schema.Registry, slots schema.SlotMapping) (string, bool) {
	if schema.IsDedicatedSlotColumn(parquetCol) {
		name, ok := slots[parquetCol]
		if !ok || name == "" {
			return "", false
		}
		if !emittableFieldName(name) {
			return "", false
		}
		return name, true
	}
	if !emittableFieldName(parquetCol) {
		return "", false
	}
	if m := reg.ResolveFromParquet(parquetCol); m != nil {
		return m.InternalName, true
	}
	return parquetCol, true
}

// mapAttrFieldName maps one key of a MAP attribute column to the LogsQL field
// name the cold read paths must emit it under, reporting false when that name
// must not surface. Keys VT itself spells at top level (OTLP span metadata such
// as `flags` or `dropped_events_count`) keep their bare name; everything else
// takes the column's VT prefix. The resulting name goes through the same
// emittableFieldName check every top-level column passes, so a MAP key can never
// smuggle in a reserved field name.
//
// A key that spells a Tier-2 slot name is suppressed, not renamed: the slot
// COLUMN already carries that slot's value, and renaming the key would emit a
// second, unrelated value under the configured attribute name.
func mapAttrFieldName(mapCol, key string, topLevelKeys map[string]bool) (string, bool) {
	return mapAttrFieldNameWithPrefix(mapColumnToAttrPrefix(mapCol), key, topLevelKeys)
}

// mapAttrFieldNameWithPrefix is mapAttrFieldName for a caller that has already
// resolved the column's prefix — the columnar MAP reader resolves it once per
// column instead of once per attribute per span. It IS the rule; mapAttrFieldName
// only derives the prefix.
func mapAttrFieldNameWithPrefix(prefix, key string, topLevelKeys map[string]bool) (string, bool) {
	if key == "" {
		return "", false
	}
	name := key
	if !topLevelKeys[key] {
		name = prefix + key
	}
	if !emittableFieldName(name) {
		return "", false
	}
	// Interned here rather than by the caller so the concatenation above never
	// escapes past this frame — the MAP loop runs once per attribute per span.
	return bytesutil.InternString(name), true
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
