package parquets3

import (
	"sync"
	"testing"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// arenaBlock builds a DataBlock whose column values point into buf, like a
// block handed out by the co-located logstorage buffer: the memory is only
// valid during the callback and is reused for the next block afterwards.
func arenaBlock(buf []byte, name string, values ...string) *logstorage.DataBlock {
	vals := make([]string, 0, len(values))
	off := 0
	for _, v := range values {
		copy(buf[off:], v)
		vals = append(vals, unsafe.String(&buf[off], len(v)))
		off += len(v)
	}
	var db logstorage.DataBlock
	db.SetColumns([]logstorage.BlockColumn{{Name: name, Values: vals}})
	return &db
}

// The enumeration keys outlive the callback, so they must not alias the
// block's memory: a later block (any query, any tenant) overwrites it.
func TestFieldValueCounter_KeysSurviveBlockReuse(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]uint64{}
	count := newFieldValueCounter("_stream", nil, nil, &mu, seen)

	buf := make([]byte, 256)
	count(0, arenaBlock(buf, "_stream", `{tenant="3:4"}`, `{tenant="3:4"}`, `{tenant="0:4"}`))
	// The buffer reuses the memory for another tenant's rows.
	for i := range buf {
		buf[i] = 'X'
	}
	count(0, arenaBlock(buf, "_stream", `{tenant="9:9"}`))
	for i := range buf {
		buf[i] = 'Y'
	}

	want := map[string]uint64{`{tenant="3:4"}`: 2, `{tenant="0:4"}`: 1, `{tenant="9:9"}`: 1}
	if len(seen) != len(want) {
		t.Fatalf("seen = %v, want %v", seen, want)
	}
	for k, n := range want {
		if seen[k] != n {
			t.Errorf("seen[%q] = %d, want %d (keys alias reused block memory: %v)", k, seen[k], n, seen)
		}
	}
}

func TestFieldValueCounter_IgnoresOtherFieldsAndEmptyValues(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]uint64{}
	count := newFieldValueCounter("tag", nil, nil, &mu, seen)
	buf := make([]byte, 64)
	count(0, arenaBlock(buf, "other", "a", "b"))
	count(0, arenaBlock(buf, "tag", "", "x", ""))
	if len(seen) != 1 || seen["x"] != 1 {
		t.Errorf("seen = %v", seen)
	}
}

// extractTraceIDs keeps the ids it returns past the callback too.
func TestExtractTraceIDs_CopiesBorrowedStrings(t *testing.T) {
	buf := make([]byte, 128)
	db := arenaBlock(buf, "trace_id", "aaaaaaaa", "bbbbbbbb", "aaaaaaaa")
	var dest []string
	extractTraceIDs(db, &dest)
	for i := range buf {
		buf[i] = 'Z'
	}
	if len(dest) != 2 || dest[0] != "aaaaaaaa" || dest[1] != "bbbbbbbb" {
		t.Errorf("dest = %v; trace ids alias reused block memory", dest)
	}
}
