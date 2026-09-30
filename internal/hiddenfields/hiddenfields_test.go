package hiddenfields

import (
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func block(names ...string) *logstorage.DataBlock {
	cols := make([]logstorage.BlockColumn, 0, len(names))
	for _, n := range names {
		cols = append(cols, logstorage.BlockColumn{Name: n, Values: []string{"v"}})
	}
	db := &logstorage.DataBlock{}
	db.SetColumns(cols)
	return db
}

func colNames(db *logstorage.DataBlock) []string {
	var out []string
	for _, c := range db.GetColumns(false) {
		out = append(out, c.Name)
	}
	return out
}

func TestWrapWriteBlock(t *testing.T) {
	var got []*logstorage.DataBlock
	sink := func(_ uint, db *logstorage.DataBlock) { got = append(got, db) }

	// No filters: the very same func comes back (no wrapping cost).
	if reflect.ValueOf(WrapWriteBlock(sink, nil)).Pointer() != reflect.ValueOf(sink).Pointer() {
		t.Fatal("empty filters must return the writer unchanged")
	}

	w := WrapWriteBlock(sink, []string{"secret*", "level"})
	w(0, block("_msg", "secret", "secret_token", "level"))
	w(0, block("_msg", "app"))
	w(0, block("secret", "level")) // every column hidden: dropped
	if len(got) != 2 {
		t.Fatalf("got %d blocks, want 2 (all-hidden block dropped)", len(got))
	}
	if !reflect.DeepEqual(colNames(got[0]), []string{"_msg"}) {
		t.Errorf("block 0 columns = %v", colNames(got[0]))
	}
	if !reflect.DeepEqual(colNames(got[1]), []string{"_msg", "app"}) {
		t.Errorf("block 1 columns = %v", colNames(got[1]))
	}
}

func TestFilterValues(t *testing.T) {
	in := []logstorage.ValueWithHits{{Value: "a", Hits: 1}, {Value: "k8s.pod", Hits: 2}, {Value: "k8s.ns", Hits: 3}}
	if got := FilterValues(in, nil); !reflect.DeepEqual(got, in) {
		t.Errorf("no filters: %v", got)
	}
	got := FilterValues(in, []string{"k8s.*"})
	if !reflect.DeepEqual(got, in[:1]) {
		t.Errorf("wildcard: %v", got)
	}
	if got := FilterValues(nil, []string{"x"}); len(got) != 0 {
		t.Errorf("nil input: %v", got)
	}
}
