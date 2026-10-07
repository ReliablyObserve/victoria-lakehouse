package schema

import (
	"bytes"
	"reflect"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type futureLogRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
	Body              string `parquet:"body"`
	Future            string `parquet:"future.column,optional"`
	AlsoFuture        string `parquet:"also_future,optional"`
}

type futureTraceRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
	TraceID           string `parquet:"trace_id"`
	Future            string `parquet:"future.column,optional"`
}

func writeParquet[T any](t *testing.T, rows []T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestUnknownColumns(t *testing.T) {
	logs := writeParquet(t, []LogRow{{TimestampUnixNano: 1, Body: "b"}})
	traces := writeParquet(t, []TraceRow{{TimestampUnixNano: 1, TraceID: "t", EventsJSON: "[]"}})
	if u := UnknownColumns(logs, "logs"); u != nil {
		t.Errorf("a current logs object has unknown columns %v", u)
	}
	if u := UnknownColumns(traces, "traces"); u != nil {
		t.Errorf("a current traces object has unknown columns %v", u)
	}
	// The span columns belong to traces only.
	if u := UnknownColumns(traces, "logs"); len(u) == 0 {
		t.Error("a traces object read as logs must have unknown columns")
	}
	if u := UnknownColumns(writeParquet(t, []futureLogRow{{1, "b", "x", "y"}}), "logs"); !reflect.DeepEqual(u, []string{"also_future", "future.column"}) {
		t.Errorf("future logs columns = %v", u)
	}
	if u := UnknownColumns(writeParquet(t, []futureTraceRow{{1, "t", "x"}}), "traces"); !reflect.DeepEqual(u, []string{"future.column"}) {
		t.Errorf("future traces columns = %v", u)
	}
	// An object that is not Parquet is the caller's read error, not the fence's.
	if u := UnknownColumns([]byte("not parquet"), "logs"); u != nil {
		t.Errorf("garbage bytes returned %v", u)
	}
}

func TestUnknownColumns_RetiredColumnsAreAllowed(t *testing.T) {
	data := writeParquet(t, []futureTraceRow{{1, "t", "x"}})
	RetiredColumns["future.column"] = struct{}{}
	defer delete(RetiredColumns, "future.column")
	knownColumnsOnce = sync.Once{} // rebuild the known sets with the entry
	defer func() { knownColumnsOnce = sync.Once{} }()
	if u := UnknownColumns(data, "traces"); u != nil {
		t.Errorf("a retired column was reported as unknown: %v", u)
	}
}

func TestFenceLog(t *testing.T) {
	var l FenceLog
	if l.Has("k") {
		t.Fatal("empty log has k")
	}
	if !l.Mark("k") || l.Mark("k") {
		t.Fatal("Mark must be true once")
	}
	if !l.Has("k") {
		t.Fatal("Has after Mark")
	}
}
