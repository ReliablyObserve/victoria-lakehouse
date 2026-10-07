package parquets3

import (
	"context"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
)

// mapScanColumns returns the columns a field enumeration must read when the
// target field or a field its filter or tombstones reference can live in a MAP
// attribute column (or a Tier-2 slot), and nil when every column involved is a
// plain top-level leaf.
//
// A MAP column has two leaf chunks (key, value) and its entries are not a row
// position: the positional row scan of scanProjectedFieldValues cannot address
// them, and read the wrong leaf instead (the values of another attribute map,
// or its keys). Such a request is answered through the same columnar reader as
// a query, which expands every attribute under the name a query returns it by,
// so field_values over an attribute equals the values of that field in the
// query result.
func (s *Storage) mapScanColumns(field string, filter *logstorage.Filter, tombstones []tombstone) map[string]bool {
	cols := s.fieldScanColumns(field, filter, tombstones)
	for _, mc := range s.registry.MapColumns() {
		if cols[mc] {
			return cols
		}
	}
	return nil
}

// fieldScanColumns is the column set the query path would read for a request
// over field, its filter and the tombstones.
func (s *Storage) fieldScanColumns(field string, filter *logstorage.Filter, tombstones []tombstone) map[string]bool {
	return neededColumns(s.registry, s.fieldScanFields(field, filter, tombstones))
}

// fieldScanFields lists the fields a field enumeration reads: the target, the
// fields of its filter and those of the tombstones.
func (s *Storage) fieldScanFields(field string, filter *logstorage.Filter, tombstones []tombstone) []string {
	fields := []string{field}
	if filter != nil {
		for name := range FilterReferencedFields(filter) {
			fields = append(fields, name)
		}
	}
	return withTombstoneFields(fields, tombstones)
}

// attrKeyPrefixes are the VictoriaTraces/Lakehouse prefixes that name an
// attribute's MAP column in a field name.
var attrKeyPrefixes = []string{"resource_attr:", "span_attr:", "scope_attr:", "log_attr:"}

// mapScanKeys returns the raw MAP keys a field enumeration needs — the keys the
// target, the filter and the tombstones name, under both their prefixed and
// bare spellings — or nil when the set cannot be bounded (a wildcard field, or
// the traces profile of the logs reader, which renames keys).
func (s *Storage) mapScanKeys(field string, filter *logstorage.Filter, tombstones []tombstone) map[string]struct{} {
	if s.registry.ResolveFromParquet("span.name") != nil {
		return nil
	}
	keys := make(map[string]struct{}, 4)
	if len(tombstones) > 0 {
		keys["_time"] = struct{}{} // a tombstone is bounded by the row's time
	}
	for _, f := range s.fieldScanFields(field, filter, tombstones) {
		if f == "*" || strings.HasSuffix(f, "*") {
			return nil
		}
		keys[f] = struct{}{}
		for _, pre := range attrKeyPrefixes {
			if k, ok := strings.CutPrefix(f, pre); ok {
				keys[k] = struct{}{}
			}
		}
	}
	return keys
}

// scanMapAwareFieldValues counts the values of field in the rows of one object
// that lie in [winLo, winHi] and match the filter and no tombstone, reading
// the columns of cols through the query path's row-group reader.
func (s *Storage) scanMapAwareFieldValues(
	ctx context.Context,
	f *parquet.File,
	planned *s3reader.PlannedFetchReaderAt,
	cols map[string]bool,
	field string,
	filter *logstorage.Filter,
	tombstones []tombstone,
	seen map[string]uint64,
	countEmpty bool,
	winLo, winHi int64,
) error {
	rgIdxs := windowRowGroups(f, winLo, winHi)
	if len(rgIdxs) == 0 {
		return nil
	}
	if planned != nil {
		s.armProjectedPlan(ctx, planned, f, rgIdxs, cols, nil)
	}
	keys := s.mapScanKeys(field, filter, tombstones)
	var mu sync.Mutex
	count := newFieldValueCounterOpts(field, filter, tombstones, &mu, seen, countEmpty)
	rowGroups := f.RowGroups()
	for _, ri := range rgIdxs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.readRowGroupWithProjectionKeys(f, rowGroups[ri], winLo, winHi, cols, nil, count, nil, keys); err != nil {
			return err
		}
	}
	return nil
}
