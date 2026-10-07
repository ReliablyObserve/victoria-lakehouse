package schema

import (
	"bytes"
	"sort"
	"sync"

	"github.com/parquet-go/parquet-go"
)

// Forward fence for rewriters.
//
// Compaction and the delete rewriter read an object through
// parquet.NewGenericReader[LogRow|TraceRow] and write the rows back. A column
// that the running code does not model is dropped by that round trip, for good.
// A newer version of Lakehouse may add a column (as span.events_json and
// span.links_json were added); a pod still running the older code must not
// rewrite that object. UnknownColumns lets a rewriter see this before it reads
// the object, so it can leave the object alone.
//
// The fence compares the top-level columns of an object with the columns the
// CURRENT row struct models plus RetiredColumns. It protects every future
// column addition. It cannot protect the transition that introduced it: code
// from before the fence has no fence.

// RetiredColumns are top-level column names that older versions wrote and the
// current row structs no longer model, and that a rewriter may therefore drop.
// Keep this list short and explicit; each entry needs a reason. Empty today:
// no column has been retired.
var RetiredColumns = map[string]struct{}{}

var (
	knownColumnsOnce sync.Once
	knownLogColumns  map[string]struct{}
	knownTraceColumn map[string]struct{}
)

func topLevelColumns[T any]() map[string]struct{} {
	out := make(map[string]struct{})
	for _, f := range parquet.SchemaOf(new(T)).Fields() {
		out[f.Name()] = struct{}{}
	}
	for c := range RetiredColumns {
		out[c] = struct{}{}
	}
	return out
}

func loadKnownColumns() {
	knownColumnsOnce.Do(func() {
		knownLogColumns = topLevelColumns[LogRow]()
		knownTraceColumn = topLevelColumns[TraceRow]()
	})
}

// UnknownColumns returns, sorted, the top-level columns of the Parquet object
// in data that the current row struct for the signal ("logs" or "traces") does
// not model and that are not in RetiredColumns. An object that cannot be opened
// as Parquet returns nil: the caller's own read reports that error.
func UnknownColumns(data []byte, signal string) []string {
	loadKnownColumns()
	known := knownLogColumns
	if signal == "traces" {
		known = knownTraceColumn
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil
	}
	var unknown []string
	for _, fld := range f.Schema().Fields() {
		if _, ok := known[fld.Name()]; !ok {
			unknown = append(unknown, fld.Name())
		}
	}
	sort.Strings(unknown)
	return unknown
}

// FenceLog remembers the objects a fence skipped. The same immutable object is
// re-offered on every tick and the running code does not change its verdict, so
// a remembered key is skipped without downloading it again and is logged once.
type FenceLog struct{ seen sync.Map }

// Has reports whether key was skipped before.
func (l *FenceLog) Has(key string) bool {
	_, ok := l.seen.Load(key)
	return ok
}

// Mark remembers key and reports whether it was new.
func (l *FenceLog) Mark(key string) bool {
	_, loaded := l.seen.LoadOrStore(key, struct{}{})
	return !loaded
}
