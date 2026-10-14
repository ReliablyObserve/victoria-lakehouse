package tracesidx

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
)

// GroupSummary is what a membership test of one row group needs: the fences (the smallest
// and largest id), the time bounds of its rows and a copy of its bloom filter's bits (nil
// when the object's filters are too large to keep). internal/pmeta stores it per object.
type GroupSummary struct {
	Rows    uint32
	MinID   string
	MaxID   string
	MinTime int64
	MaxTime int64
	Filter  []byte
}

// Summary is the membership summary of one idx object.
type Summary struct {
	Rows   int64
	Groups []GroupSummary
}

// memSource is a Source over bytes already in memory.
type memSource struct {
	key  string
	data []byte
}

func (m memSource) Key() string { return m.key }
func (m memSource) Size() int64 { return int64(len(m.data)) }
func (m memSource) ReadRange(_ context.Context, _ ReadKind, off, n int64) ([]byte, error) {
	if off < 0 || n < 0 || off+n > int64(len(m.data)) {
		return nil, fmt.Errorf("tracesidx: range %d+%d outside the %d-byte object", off, n, len(m.data))
	}
	return m.data[off : off+n], nil
}

// Summarize reads the membership summary of an object held in memory (the flush and the
// compaction have just written it).
func Summarize(data []byte) (*Summary, error) {
	return NewLocator(1).Summary(context.Background(), memSource{key: "mem", data: data})
}

// Summary returns the membership summary of src, from its footer (and, when small, its
// bloom filters): the rebuild path of a lost summary reads each object's tail, nothing more.
func (l *Locator) Summary(ctx context.Context, src Source) (*Summary, error) {
	e, _, err := l.open(ctx, src)
	if err != nil {
		return nil, err
	}
	timeCol := e.cols[TimeField]
	out := &Summary{Groups: make([]GroupSummary, len(e.meta.RowGroups))}
	for g := range e.meta.RowGroups {
		rg := &e.meta.RowGroups[g]
		gs := GroupSummary{Rows: uint32(max(rg.NumRows, 0)), MinID: string(e.idMin[g]), MaxID: string(e.idMax[g])}
		if st := rg.Columns[timeCol].MetaData.Statistics; len(st.MinValue) == 8 && len(st.MaxValue) == 8 {
			gs.MinTime = int64(binary.LittleEndian.Uint64(st.MinValue))
			gs.MaxTime = int64(binary.LittleEndian.Uint64(st.MaxValue))
		}
		if e.filters != nil && e.filters[g] != nil {
			gs.Filter = slices.Clone(e.filters[g].Bytes())
		}
		out.Rows += rg.NumRows
		out.Groups[g] = gs
	}
	return out, nil
}
