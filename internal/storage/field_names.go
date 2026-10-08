package storage

import (
	"context"
	"math/bits"
	"strconv"
	"strings"
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

type fieldNamesCounterKey struct{}

// IsFieldNamesCounter reports whether the field_names enumeration is counted by
// fieldNamesCounter, which wants the insert buffer's blocks marked
// (MarkFieldNamesBufferBlock); a query with pipes runs upstream's pipe instead
// and must see no extra column.
func IsFieldNamesCounter(ctx context.Context) bool {
	v, _ := ctx.Value(fieldNamesCounterKey{}).(bool)
	return v
}

// IsFieldNamesQuery reports whether the read answers a field_names enumeration.
func IsFieldNamesQuery(ctx context.Context) bool {
	v, _ := ctx.Value(fieldNamesQueryKey{}).(bool)
	return v
}

// FieldNamesViaQuery answers /select/logsql/field_names the way upstream does:
// it reads the rows of the query and credits every field with the matching rows
// of the blocks that list it. read is the storage's RunQuery, which owns the
// tenant scope, the time range, tombstones and the merge of the insert buffer,
// the peers' buffers and the Parquet objects. A query with pipes goes through
// upstream's own field_names pipe after them; a plain filter goes through
// fieldNamesCounter, which applies the same rule.
//
// The enumeration reads every row of the window and keeps bounded state, so the
// storage's per-query row ceiling (a guard for queries that return rows) does
// not apply to it: RunQuery lifts it for a read marked WithFieldNamesQuery, as a
// field list missing the fields of the rows that were never read must not pass
// for a complete one.
func FieldNamesViaQuery(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, read func(context.Context, []logstorage.TenantID, *logstorage.Query, logstorage.WriteDataBlockFunc) error) ([]logstorage.ValueWithHits, error) {
	if !logstorage.QueryHasPipes(q) && !logstorage.QueryHasFilterSubqueries(q) {
		c := newFieldNamesCounter(fieldNamesStreamCap)
		rctx := context.WithValue(WithFieldNamesQuery(WithAllFieldsHint(ctx)), fieldNamesCounterKey{}, true)
		if err := read(rctx, tenantIDs, q, c.add); err != nil {
			return nil, err
		}
		return c.result(), nil
	}

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
	if len(out) == 0 {
		return nil, nil
	}
	// Upstream's order (hits descending, then name), and one entry per name.
	return logstorage.MergeValuesWithHits([][]logstorage.ValueWithHits{out}, 0, false), nil
}

// fieldNamesLayerColumn marks the blocks of the insert buffer for the counter.
// The readers add it to a block served from the buffer (the co-located segments
// or an insert peer's rows) when the read is a field_names enumeration, and the
// counter takes it out again. A stream held both in the buffer and in objects is
// two blocks on hot storage too (an in-memory part is not yet merged into the
// stored ones), so the counter unions the blocks of a stream within a layer, not
// across the buffer and the objects.
const fieldNamesLayerColumn = "\x00layer"

// MarkFieldNamesBufferBlock returns db marked as served from the insert buffer.
func MarkFieldNamesBufferBlock(db *logstorage.DataBlock) *logstorage.DataBlock {
	if db == nil || db.RowsCount() == 0 {
		return db
	}
	cols := db.GetColumns(false)
	marked := make([]logstorage.BlockColumn, 0, len(cols)+1)
	marked = append(marked, cols...)
	marked = append(marked, logstorage.BlockColumn{Name: fieldNamesLayerColumn, Values: make([]string, db.RowsCount())})
	out := &logstorage.DataBlock{}
	out.SetColumns(marked)
	return out
}

// MarkFieldNamesBufferWriter wraps write so every block it receives is marked.
func MarkFieldNamesBufferWriter(write logstorage.WriteDataBlockFunc) logstorage.WriteDataBlockFunc {
	return func(worker uint, db *logstorage.DataBlock) { write(worker, MarkFieldNamesBufferBlock(db)) }
}

// fieldNamesStreamCap bounds the streams fieldNamesCounter tracks (about 150
// bytes each at 40 columns); past it a block is credited on its own.
var fieldNamesStreamCap = 1 << 18

// fieldNamesCounter is upstream's field_names rule over a stream-shaped input:
// a field is credited with the matching rows of every block that lists it.
// Upstream stores a stream in few large blocks, while Parquet objects hold the
// same stream in one block per object and row group (an hour, a flush, a
// compaction level), so crediting each block alone would make the answer depend
// on the object layout: a field that one span of a stream carries would credit
// that stream's rows in one object and not in the next. The counter therefore
// unions the columns of all blocks of a stream (_stream_id, else _stream) and
// credits the field with all the stream's matching rows, which is what a block
// holding the whole stream gives. Past fieldNamesStreamCap distinct streams it
// credits a block on its own, upstream's exact per-block rule, so its memory
// stays bounded.
type fieldNamesCounter struct {
	mu      sync.Mutex
	cap     int
	ids     map[string]int // field name -> column id
	names   []string
	streams map[string]*streamColumns
	blocks  map[string]uint64 // per-block credit past the cap
}

type streamColumns struct {
	rows uint64
	cols []uint64 // bit set over column ids
}

func newFieldNamesCounter(streamCap int) *fieldNamesCounter {
	return &fieldNamesCounter{cap: streamCap, ids: make(map[string]int), streams: make(map[string]*streamColumns), blocks: make(map[string]uint64)}
}

func (c *fieldNamesCounter) add(_ uint, db *logstorage.DataBlock) {
	if db == nil || db.RowsCount() == 0 {
		return
	}
	cols := db.GetColumns(false)
	key := streamKeyOf(cols)
	n := uint64(db.RowsCount())
	if hasLayerColumn(cols) {
		key = "b" + key
		kept := make([]logstorage.BlockColumn, 0, len(cols))
		for _, col := range cols {
			if col.Name != fieldNamesLayerColumn {
				kept = append(kept, col)
			}
		}
		cols = kept
	} else {
		key = "o" + key
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.streams[key]
	if st == nil {
		if len(c.streams) >= c.cap {
			for _, col := range cols {
				name := col.Name
				if name == "" {
					name = "_msg"
				}
				if _, ok := c.blocks[name]; !ok {
					name = strings.Clone(name)
				}
				c.blocks[name] += n
			}
			return
		}
		st = &streamColumns{}
		c.streams[key] = st
	}
	st.rows += n
	for _, col := range cols {
		name := col.Name
		if name == "" {
			name = "_msg"
		}
		id, ok := c.ids[name]
		if !ok {
			// The name may point into a buffer the reader reuses once this
			// call returns (the insert buffer's blocks do): keep a copy.
			name = strings.Clone(name)
			id = len(c.names)
			c.ids[name] = id
			c.names = append(c.names, name)
		}
		for id/64 >= len(st.cols) {
			st.cols = append(st.cols, 0)
		}
		st.cols[id/64] |= 1 << (id % 64)
	}
}

func (c *fieldNamesCounter) result() []logstorage.ValueWithHits {
	c.mu.Lock()
	defer c.mu.Unlock()
	hits := make(map[string]uint64, len(c.names)+len(c.blocks))
	for name, n := range c.blocks {
		hits[name] += n
	}
	for _, st := range c.streams {
		for w, word := range st.cols {
			for ; word != 0; word &= word - 1 {
				hits[c.names[w*64+bits.TrailingZeros64(word)]] += st.rows
			}
		}
	}
	if len(hits) == 0 {
		return nil
	}
	out := make([]logstorage.ValueWithHits, 0, len(hits))
	for name, n := range hits {
		out = append(out, logstorage.ValueWithHits{Value: name, Hits: n})
	}
	return logstorage.MergeValuesWithHits([][]logstorage.ValueWithHits{out}, 0, false)
}

func hasLayerColumn(cols []logstorage.BlockColumn) bool {
	for _, c := range cols {
		if c.Name == fieldNamesLayerColumn {
			return true
		}
	}
	return false
}

// streamKeyOf is the stream a block's rows belong to: its _stream_id, else its
// _stream, else none. The blocks the readers emit hold one stream.
func streamKeyOf(cols []logstorage.BlockColumn) string {
	var stream string
	for _, c := range cols {
		if len(c.Values) == 0 {
			continue
		}
		switch c.Name {
		case "_stream_id":
			if c.Values[0] != "" {
				return "i" + c.Values[0]
			}
		case "_stream":
			stream = c.Values[0]
		}
	}
	if stream != "" {
		return "s" + stream
	}
	return ""
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
