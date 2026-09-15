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
// what makes one rule enough: a MAP key `account_id` in a signal whose map
// attributes surface unprefixed would otherwise introduce exactly the field a
// top-level column is forbidden to produce, while the same key under a
// `span_attr:` prefix is a legitimate user attribute and stays.
func emittableFieldName(name string) bool {
	return name != "" && schema.ClassifyColumn(name) == schema.ColumnUserVisible
}

// queryFieldName maps a top-level Parquet column to the LogsQL field name the
// cold read paths must emit it under, reporting false when the column must not
// surface as a query field at all.
//
// Together with mapAttrFieldName this is the single naming rule both scan paths
// obey — the columnar fast path (readRowGroupColumnar) and the row-oriented slow
// path (projectedFieldsToDataBlock) — so a cold row carries exactly the fields
// hot VictoriaLogs returns for the same ingested row:
//
//   - a Tier-2 spare slot is renamed to the attribute name the file's footer KV
//     binds it to (and dropped when the slot is unbound) — the same rule the
//     typed path applies in remapSlotFields;
//   - everything else resolves through the registry's internal name for the
//     column, falling back to the column name itself when the registry has no
//     mapping (the Tier-1 dedicated columns, which logRowToFields also emits
//     under their bare attribute name);
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
// must not surface. The prefix comes from mapColumnToAttrPrefix (empty for the
// signal's own attribute maps, the column name otherwise) and the resulting name
// goes through the same emittableFieldName check every top-level column passes,
// so a MAP key can never smuggle in a reserved field name.
//
// A key that spells a Tier-2 slot name is suppressed, not renamed: the slot
// COLUMN already carries that slot's value, and renaming the key would emit a
// second, unrelated value under the configured attribute name.
func mapAttrFieldName(mapCol, key string) (string, bool) {
	return mapAttrFieldNameWithPrefix(mapColumnToAttrPrefix(mapCol), key)
}

// mapAttrFieldNameWithPrefix is mapAttrFieldName for a caller that has already
// resolved the column's prefix — the columnar MAP reader resolves it once per
// column instead of once per attribute per row. It IS the rule; mapAttrFieldName
// only derives the prefix.
func mapAttrFieldNameWithPrefix(prefix, key string) (string, bool) {
	if key == "" {
		return "", false
	}
	name := prefix + key
	if !emittableFieldName(name) {
		return "", false
	}
	// Interned here rather than by the caller so the concatenation above never
	// escapes past this frame — the MAP loop runs once per attribute per row.
	return bytesutil.InternString(name), true
}
