package tracesidx

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go/bloom"
)

// The summary of an object is a copy of what its footer holds: per row group the rows, the
// fences, the time bounds and the bloom filter, which answers like the object's own: every
// id of the group passes and an id of no row group does not (but for the filter's false
// positives).
func TestSummarizeCopiesTheFooter(t *testing.T) {
	old := rowsPerGroup
	rowsPerGroup = 700
	defer func() { rowsPerGroup = old }()
	rows := make([]Row, 0, 2000)
	for i := 0; i < 2000; i++ {
		id := fmt.Sprintf("%032x", uint64(i)*2654435761)
		rows = append(rows, Row{Time: 1_000_000 + int64(i)*10, Stream: StreamOf(id), TraceID: id, Start: 1_000_000 + int64(i)*10, End: 1_000_000 + int64(i)*10 + 5, Duration: 5})
	}
	data, info, err := Marshal(append([]Row(nil), rows...), 3)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Summarize(data)
	if err != nil {
		t.Fatal(err)
	}
	if s.Rows != 2000 || len(s.Groups) != info.Groups || info.Groups != 3 {
		t.Fatalf("rows %d groups %d (info %d)", s.Rows, len(s.Groups), info.Groups)
	}
	sorted := append([]Row(nil), rows...)
	Sort(sorted)
	var total int64
	for g, gs := range s.Groups {
		lo, hi := g*700, min((g+1)*700, 2000)
		if int(gs.Rows) != hi-lo || gs.MinID != sorted[lo].TraceID || gs.MaxID != sorted[hi-1].TraceID {
			t.Errorf("group %d: %+v", g, gs)
		}
		if gs.MinTime <= 0 || gs.MaxTime < gs.MinTime {
			t.Errorf("group %d time bounds [%d, %d]", g, gs.MinTime, gs.MaxTime)
		}
		if len(gs.Filter) == 0 || len(gs.Filter)%32 != 0 || len(gs.Filter) != info.BloomSize[g] {
			t.Fatalf("group %d: filter of %d bytes, want %d", g, len(gs.Filter), info.BloomSize[g])
		}
		f := bloom.MakeSplitBlockFilter(gs.Filter)
		for i := lo; i < hi; i++ {
			if !f.Check(bloom.XXH64{}.Sum64([]byte(sorted[i].TraceID))) {
				t.Fatalf("group %d: filter misses its own id %s", g, sorted[i].TraceID)
			}
		}
		total += int64(gs.Rows)
	}
	if total != 2000 {
		t.Errorf("rows %d", total)
	}
	// the locator's summary of the same object read by ranges equals the in-memory one
	cnt := newCountSource("k", data)
	s2, err := NewLocator(4).Summary(context.Background(), cnt)
	if err != nil {
		t.Fatal(err)
	}
	if len(s2.Groups) != len(s.Groups) {
		t.Fatalf("%d groups", len(s2.Groups))
	}
	for g := range s.Groups {
		if s.Groups[g].MinID != s2.Groups[g].MinID || s.Groups[g].MinTime != s2.Groups[g].MinTime || !bytes.Equal(s.Groups[g].Filter, s2.Groups[g].Filter) {
			t.Errorf("group %d differs between the two readers", g)
		}
	}
	if cnt.reads > 3 {
		t.Errorf("a summary took %d reads, want the footer tail and the filters (at most 3)", cnt.reads)
	}
}

func TestSummarizeRefusesWhatIsNotAnIdxObject(t *testing.T) {
	if _, err := Summarize([]byte("not a parquet file at all")); err == nil {
		t.Error("garbage was summarised")
	}
	if _, err := Summarize(nil); err == nil {
		t.Error("nothing was summarised")
	}
}
