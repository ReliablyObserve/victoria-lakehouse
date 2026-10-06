package storage

import (
	"sort"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// maxConstColumnValueSize mirrors upstream's limit (lib/logstorage/consts.go):
// a column whose single value is longer than this is not a const column.
const maxConstColumnValueSize = 256

// OrderColumnsLikeUpstream puts the columns of a block of raw rows in the order
// upstream VictoriaLogs gives the same rows (blockResult.initColumnsByFilter
// over a block written with sortColumnsByName):
//
//  1. _time, _stream_id, _stream, _msg;
//  2. the other columns whose value is the same in every row of the block
//     (upstream's const columns), by name;
//  3. the remaining columns, by name.
//
// The order matters beyond looks: a sort over all columns (`sort`, `first N`,
// `last N` without `by`) keys each row on its columns in block order, so rows
// that tie on _time are ordered by _stream_id on upstream. Cold blocks were
// built by iterating a map, which gave a different column order on every read
// and so a different tie order (and different rows under a limit) for the same
// query (#427).
//
// Upstream decides constness per block of one stream; a cold block can hold
// several streams, so a column that is const in each stream may count as
// non-const here. That only moves it from group 2 to group 3, and rows that
// tie on every special column share their stream, so they still compare on
// the same columns in the same name order as upstream.
//
// It reorders db's column slice in place and returns at once when the block is
// already in that order. Only blocks of raw rows may go through it: an
// aggregated block (stats output) keeps the column order its producer chose.
func OrderColumnsLikeUpstream(db *logstorage.DataBlock) {
	if db == nil {
		return
	}
	cols := db.GetColumns(false)
	if len(cols) < 2 {
		return
	}
	var buf [64]columnRank
	ranks := buf[:0]
	if len(cols) > len(buf) {
		ranks = make([]columnRank, 0, len(cols))
	}
	sorted := true
	for i := range cols {
		r := columnRank{group: specialColumnRank(cols[i].Name)}
		if r.group == groupOther && isConstColumn(cols[i].Values) {
			r.group = groupConst
		}
		ranks = append(ranks, r)
		if i > 0 && rankLess(cols[i], ranks[i], cols[i-1], ranks[i-1]) {
			sorted = false
		}
	}
	if sorted {
		return
	}
	sort.Sort(&columnSorter{cols: cols, ranks: ranks})
	db.SetColumns(cols)
}

const (
	groupTime = iota
	groupStreamID
	groupStream
	groupMsg
	groupConst
	groupOther
)

type columnRank struct{ group int }

func specialColumnRank(name string) int {
	switch name {
	case "_time":
		return groupTime
	case "_stream_id":
		return groupStreamID
	case "_stream":
		return groupStream
	case "_msg":
		return groupMsg
	}
	return groupOther
}

// isConstColumn is upstream's column.canStoreInConstColumn.
func isConstColumn(values []string) bool {
	if len(values) == 0 {
		return true
	}
	v := values[0]
	if len(v) > maxConstColumnValueSize {
		return false
	}
	for _, x := range values[1:] {
		if x != v {
			return false
		}
	}
	return true
}

func rankLess(a logstorage.BlockColumn, ra columnRank, b logstorage.BlockColumn, rb columnRank) bool {
	if ra.group != rb.group {
		return ra.group < rb.group
	}
	return a.Name < b.Name
}

type columnSorter struct {
	cols  []logstorage.BlockColumn
	ranks []columnRank
}

func (s *columnSorter) Len() int { return len(s.cols) }
func (s *columnSorter) Less(i, j int) bool {
	return rankLess(s.cols[i], s.ranks[i], s.cols[j], s.ranks[j])
}
func (s *columnSorter) Swap(i, j int) {
	s.cols[i], s.cols[j] = s.cols[j], s.cols[i]
	s.ranks[i], s.ranks[j] = s.ranks[j], s.ranks[i]
}
