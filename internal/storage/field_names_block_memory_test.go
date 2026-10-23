package storage

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// reusedNames hands out block column names backed by one buffer that every call
// overwrites, the way a reader that reuses its block memory does. Upstream's
// WriteDataBlockFunc may not keep a block's names or values after it returns
// (lib/logstorage/storage_search.go): a string kept past the call reads
// whatever the next block wrote there.
type reusedNames struct{ buf []byte }

func (r *reusedNames) block(rows int, names ...string) *logstorage.DataBlock {
	r.buf = r.buf[:0]
	for _, n := range names {
		r.buf = append(r.buf, n...)
	}
	cols := make([]logstorage.BlockColumn, 0, len(names))
	off := 0
	for _, n := range names {
		values := make([]string, rows)
		for i := range values {
			values[i] = "v"
		}
		cols = append(cols, logstorage.BlockColumn{Name: unsafe.String(&r.buf[off], len(n)), Values: values})
		off += len(n)
	}
	db := &logstorage.DataBlock{}
	db.SetColumns(cols)
	return db
}

// Past the stream cap a block is credited on its own (c.blocks). Crediting a
// name the map already holds must not store the block's copy of the name: a Go
// map assignment replaces the stored key with the one assigned, so the key
// would then read the next block's bytes, the name would be listed under them,
// and its own hits would be lost.
func TestFieldNamesCounter_PastCapKeepsNamesTheReaderReuses(t *testing.T) {
	r := &reusedNames{buf: make([]byte, 0, 64)}
	c := newFieldNamesCounter(0) // every block is past the cap
	c.add(0, r.block(2, "alpha"))
	c.add(0, r.block(3, "alpha"))
	c.add(0, r.block(5, "omega"))
	want := map[string]uint64{"alpha": 5, "omega": 5}
	if got := fnHits(c.result()); !reflect.DeepEqual(got, want) {
		t.Fatalf("hits = %v, want %v", got, want)
	}
}

// The pipe path keeps the (name, hits) rows of upstream's field_names pipe after
// the callback returns, so it copies each name, as upstream's own reader of that
// pipe does (runValuesWithHitsQuery).
func TestValuesWithHitsOfBlock_KeepsNoBlockMemory(t *testing.T) {
	buf := []byte("alphaomega")
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "name", Values: []string{unsafe.String(&buf[0], 5), unsafe.String(&buf[5], 5)}},
		{Name: "hits", Values: []string{"7", "3"}},
	})
	got := valuesWithHitsOfBlock(db)
	copy(buf, "xxxxxyyyyy") // the reader's next block
	want := []logstorage.ValueWithHits{{Value: "alpha", Hits: 7}, {Value: "omega", Hits: 3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
