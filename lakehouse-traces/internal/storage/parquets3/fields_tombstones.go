package parquets3

import (
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// timestampColumn is the Parquet column holding the row timestamp. A tombstone
// is time-bounded, so evaluating one against a row REQUIRES this column to be
// in the projection — without it every row looks like it falls outside the
// tombstone's range and nothing is suppressed.
const timestampColumn = "timestamp_unix_nano"

// tombstone aliases delete.Tombstone for this package. storage_fields.go calls
// the builtin delete(), which a package named "delete" would shadow; the alias
// lets that file name the type without importing it.
type tombstone = delete.Tombstone

// fieldsTombstones returns the tombstones overlapping [startNs, endNs], or nil.
//
// The field-enumeration endpoints (field_names, field_values, streams,
// stream_ids, and the hit counts they carry) used to be entirely tombstone
// blind: only the row-fetch path in storage_query.go applied the filter. A
// hide-mode delete therefore removed rows from the log view while the value it
// deleted stayed in every Grafana dropdown and every field-cardinality panel —
// a "deleted" value the user could still see and still select.
func (s *Storage) fieldsTombstones(startNs, endNs int64) []tombstone {
	if s.tombstones == nil {
		return nil
	}
	ts := s.tombstones.ForRange(startNs, endNs)
	if len(ts) == 0 {
		return nil
	}
	return ts
}

// addTombstoneProjection adds every Parquet column a tombstone predicate needs
// to the column projection: the timestamp, plus whatever fields each
// tombstone's own LogsQL query constrains.
//
// This is the subtle half of applying tombstones on a column-projected scan.
// The projection is built from the TARGET column and the USER's filter; a
// tombstone evaluated against that projection sees its own fields as absent and
// silently matches nothing, which reads as "the delete did not work" rather
// than as an error. Widening costs extra column bytes per scanned file, but
// only while a tombstone actually overlaps the window.
func (s *Storage) addTombstoneProjection(tss []tombstone, projected map[string]bool) {
	if len(tss) == 0 {
		return
	}
	projected[timestampColumn] = true
	for i := range tss {
		f := tss[i].Filter()
		if f == nil {
			continue
		}
		for internalName := range FilterReferencedFields(f) {
			if m := s.registry.ResolveToParquet(internalName); m != nil {
				projected[m.ParquetColumn] = true
			} else {
				projected[internalName] = true
			}
		}
	}
}

// rowTombstoned reports whether a row matches any of the tombstones.
func rowTombstoned(tss []tombstone, fields []logstorage.Field, tsNs int64) bool {
	for i := range tss {
		if tss[i].MatchesFields(fields, tsNs) {
			return true
		}
	}
	return false
}

// noteFieldsScanFallback records that an endpoint gave up a metadata-only fast
// path because a tombstone overlapped the window and the answer had to be
// verified against rows. Operators need this to explain the latency change that
// follows a delete.
func noteFieldsScanFallback(endpoint string) {
	metrics.DeleteFieldsScanFallback.Inc(endpoint)
}
