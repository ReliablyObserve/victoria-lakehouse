package storage

import (
	"context"
	"strconv"
	"sync"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

type fieldNamesQueryKey struct{}

// WithFieldNamesQuery marks the read as a field_names enumeration. The cold
// readers then hand the pipe the column sets upstream's blocks have (see
// EmitFieldNamesStreamBlocks): the query's row filter and time range are applied
// to the rows of a block AFTER its columns are known, as on hot storage.
func WithFieldNamesQuery(ctx context.Context) context.Context {
	return context.WithValue(ctx, fieldNamesQueryKey{}, true)
}

// IsFieldNamesQuery reports whether the read answers a field_names enumeration.
func IsFieldNamesQuery(ctx context.Context) bool {
	v, _ := ctx.Value(fieldNamesQueryKey{}).(bool)
	return v
}

// FieldNamesViaQuery answers /select/logsql/field_names the way upstream does:
// by running its field_names pipe over the rows of the query. read is the
// storage's RunQuery, which owns the tenant scope, the time range, tombstones
// and the merge of the insert buffer, the peers' buffers and the Parquet
// objects; the pipe, its hit rule (a column of a block is credited with every
// row of the block that matches) and its order are upstream's own.
//
// The enumeration reads every row of the window and keeps O(fields) state, so
// the storage's per-query row ceiling (a guard for queries that return rows) does
// not apply to it: RunQuery lifts it for a read marked WithFieldNamesQuery, as a
// field list missing the fields of the rows that were never read must not pass
// for a complete one.
func FieldNamesViaQuery(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, read func(context.Context, []logstorage.TenantID, *logstorage.Query, logstorage.WriteDataBlockFunc) error) ([]logstorage.ValueWithHits, error) {
	qNew := logstorage.QueryWithFieldNames(q)
	qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, tenantIDs, qNew, false, nil)

	var run logstorage.ExternalRunQueryFn
	run = func(qc *logstorage.QueryContext, wb logstorage.WriteDataBlockFunc) error {
		return logstorage.RunQueryExternalWithSubqueries(qc, func(query *logstorage.Query, write logstorage.WriteDataBlockFunc) error {
			rctx := WithFieldNamesQuery(WithAllFieldsHint(qc.Context))
			return read(rctx, qc.TenantIDs, query, write)
		}, run, wb)
	}

	var mu sync.Mutex
	var out []logstorage.ValueWithHits
	err := run(qctx, func(_ uint, db *logstorage.DataBlock) {
		vhs := valuesWithHitsOfBlock(db)
		mu.Lock()
		out = append(out, vhs...)
		mu.Unlock()
	})
	if err != nil {
		return nil, err
	}
	// Upstream's order (hits descending, then name), and one entry per name.
	return logstorage.MergeValuesWithHits([][]logstorage.ValueWithHits{out}, 0, false), nil
}

// valuesWithHitsOfBlock reads the two columns (name, hits) upstream's
// field_names pipe writes.
func valuesWithHitsOfBlock(db *logstorage.DataBlock) []logstorage.ValueWithHits {
	if db == nil || db.RowsCount() == 0 {
		return nil
	}
	var names, hits []string
	for _, c := range db.GetColumns(false) {
		switch c.Name {
		case "name":
			names = c.Values
		case "hits":
			hits = c.Values
		}
	}
	out := make([]logstorage.ValueWithHits, 0, len(names))
	for i, n := range names {
		var h uint64
		if i < len(hits) {
			h, _ = strconv.ParseUint(hits[i], 10, 64)
		}
		out = append(out, logstorage.ValueWithHits{Value: n, Hits: h})
	}
	return out
}

// EmitFieldNamesStreamBlocks gives a physical block the column sets upstream's
// blocks have, before any row filter runs. Upstream never mixes streams in a
// block (inmemoryPart.mustInitFromRows), and a block lists a column only if at
// least one of its rows has a value for it; upstream's field_names credits that
// column with every matching row of the block. A Parquet row group mixes
// streams, so db is split per stream (_stream_id, else _stream) and each part
// keeps only the columns some row of the stream has a value for. Rows keep their
// order within the stream.
func EmitFieldNamesStreamBlocks(db *logstorage.DataBlock, emit func(*logstorage.DataBlock)) {
	if db == nil || db.RowsCount() == 0 {
		return
	}
	cols := db.GetColumns(false)
	var ids, streams []string
	for _, c := range cols {
		switch c.Name {
		case "_stream_id":
			ids = c.Values
		case "_stream":
			streams = c.Values
		}
	}
	type streamKey struct {
		kind  byte
		value string
	}
	index := make(map[streamKey]int)
	var groups [][]int
	for row := 0; row < db.RowsCount(); row++ {
		key := streamKey{}
		if row < len(ids) && ids[row] != "" {
			key = streamKey{1, ids[row]}
		} else if row < len(streams) && streams[row] != "" {
			key = streamKey{2, streams[row]}
		}
		i, ok := index[key]
		if !ok {
			i = len(groups)
			index[key] = i
			groups = append(groups, nil)
		}
		groups[i] = append(groups[i], row)
	}
	for _, rows := range groups {
		out := make([]logstorage.BlockColumn, 0, len(cols))
		for _, c := range cols {
			present := false
			for _, row := range rows {
				if row < len(c.Values) && c.Values[row] != "" {
					present = true
					break
				}
			}
			if !present {
				continue
			}
			values := make([]string, len(rows))
			for i, row := range rows {
				if row < len(c.Values) {
					values[i] = c.Values[row]
				}
			}
			out = append(out, logstorage.BlockColumn{Name: c.Name, Values: values})
		}
		var block logstorage.DataBlock
		block.SetColumns(out)
		emit(&block)
	}
}
