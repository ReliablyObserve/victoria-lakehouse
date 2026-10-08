package storage

import (
	"context"
	"reflect"
	"testing"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func fnBlock(stream string, rows int, cols map[string]bool) *logstorage.DataBlock {
	// cols maps a column name to whether any row has a value for it.
	names := []string{"_stream_id", "a", "b", "c"}
	var out []logstorage.BlockColumn
	for _, n := range names {
		if n != "_stream_id" {
			if present, ok := cols[n]; !ok || !present {
				continue
			}
		}
		vals := make([]string, rows)
		for i := range vals {
			if n == "_stream_id" {
				vals[i] = stream
			} else if i == 0 {
				vals[i] = "v"
			}
		}
		out = append(out, logstorage.BlockColumn{Name: n, Values: vals})
	}
	db := &logstorage.DataBlock{}
	db.SetColumns(out)
	return db
}

func fnHits(vs []logstorage.ValueWithHits) map[string]uint64 {
	m := map[string]uint64{}
	for _, v := range vs {
		m[v.Value] = v.Hits
	}
	return m
}

// The blocks of one stream, however many objects hold them, count as the one
// block upstream stores: a column some block lists is credited with all the
// stream's rows.
func TestFieldNamesCounter_UnionsTheBlocksOfAStream(t *testing.T) {
	c := newFieldNamesCounter(100)
	c.add(0, fnBlock("s1", 3, map[string]bool{"a": true}))
	c.add(0, fnBlock("s1", 2, map[string]bool{"a": true, "b": true})) // another object of the stream
	c.add(0, fnBlock("s2", 4, map[string]bool{"a": true}))
	want := map[string]uint64{"_stream_id": 9, "a": 9, "b": 5}
	if got := fnHits(c.result()); !reflect.DeepEqual(got, want) {
		t.Fatalf("hits = %v, want %v", got, want)
	}
	// Whatever order the blocks arrive in, the answer is the same.
	d := newFieldNamesCounter(100)
	d.add(0, fnBlock("s2", 4, map[string]bool{"a": true}))
	d.add(0, fnBlock("s1", 2, map[string]bool{"a": true, "b": true}))
	d.add(0, fnBlock("s1", 3, map[string]bool{"a": true}))
	if got := fnHits(d.result()); !reflect.DeepEqual(got, want) {
		t.Fatalf("reordered hits = %v, want %v", got, want)
	}
}

// Past the stream cap a block is credited on its own (upstream's per-block
// rule) and the state stays bounded.
func TestFieldNamesCounter_StreamCapFallsBackToPerBlock(t *testing.T) {
	c := newFieldNamesCounter(1)
	c.add(0, fnBlock("s1", 3, map[string]bool{"a": true}))
	c.add(0, fnBlock("s2", 2, map[string]bool{"b": true}))
	c.add(0, fnBlock("s1", 1, map[string]bool{"c": true})) // a known stream still unions
	if len(c.streams) != 1 {
		t.Fatalf("tracked %d streams, want the cap 1", len(c.streams))
	}
	want := map[string]uint64{"_stream_id": 6, "a": 4, "b": 2, "c": 4}
	if got := fnHits(c.result()); !reflect.DeepEqual(got, want) {
		t.Fatalf("hits = %v, want %v", got, want)
	}
}

func TestFieldNamesCounter_NothingInNothingOut(t *testing.T) {
	c := newFieldNamesCounter(10)
	c.add(0, nil)
	c.add(0, &logstorage.DataBlock{})
	if got := c.result(); got != nil {
		t.Fatalf("result = %v, want none", got)
	}
}

// A block with no _stream_id falls back to _stream, then to one anonymous
// stream.
func TestStreamKeyOf(t *testing.T) {
	cols := func(kv ...string) []logstorage.BlockColumn {
		var out []logstorage.BlockColumn
		for i := 0; i < len(kv); i += 2 {
			out = append(out, logstorage.BlockColumn{Name: kv[i], Values: []string{kv[i+1]}})
		}
		return out
	}
	if a, b := streamKeyOf(cols("_stream_id", "x", "_stream", "{a=b}")), streamKeyOf(cols("_stream", "{a=b}")); a == b || a == "" || b == "" {
		t.Errorf("keys %q %q: the id and the stream text must key apart and neither be empty", a, b)
	}
	if k := streamKeyOf(cols("a", "1")); k != "" {
		t.Errorf("key without a stream = %q, want empty", k)
	}
}

// A read error is returned, and a read that emits nothing answers nothing.
func TestFieldNamesViaQuery_PlainFilter(t *testing.T) {
	q, err := logstorage.ParseQuery("*")
	if err != nil {
		t.Fatal(err)
	}
	got, err := FieldNamesViaQuery(context.Background(), nil, q, func(ctx context.Context, _ []logstorage.TenantID, _ *logstorage.Query, wb logstorage.WriteDataBlockFunc) error {
		if !IsFieldNamesQuery(ctx) || !IsAllFields(ctx) {
			t.Error("the read must carry the field-names and all-fields hints")
		}
		wb(0, fnBlock("s1", 2, map[string]bool{"a": true}))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]uint64{"_stream_id": 2, "a": 2}; !reflect.DeepEqual(fnHits(got), want) {
		t.Fatalf("hits = %v, want %v", fnHits(got), want)
	}
	boom := context.DeadlineExceeded
	if _, err = FieldNamesViaQuery(context.Background(), nil, q, func(context.Context, []logstorage.TenantID, *logstorage.Query, logstorage.WriteDataBlockFunc) error {
		return boom
	}); err != boom {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

// With pipes the answer is upstream's own field_names pipe over the pipes'
// output.
func TestFieldNamesViaQuery_WithPipesUsesTheUpstreamPipe(t *testing.T) {
	q, err := logstorage.ParseQuery("* | fields a")
	if err != nil {
		t.Fatal(err)
	}
	got, err := FieldNamesViaQuery(context.Background(), nil, q, func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, wb logstorage.WriteDataBlockFunc) error {
		wb(0, fnBlock("s1", 2, map[string]bool{"a": true, "b": true}))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]uint64{"a": 2}; !reflect.DeepEqual(fnHits(got), want) {
		t.Fatalf("hits = %v, want %v", fnHits(got), want)
	}
}

func TestEmitFieldNamesStreamBlocks(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_stream_id", Values: []string{"s1", "s2", "s1"}},
		{Name: "a", Values: []string{"x", "", ""}},
		{Name: "b", Values: []string{"", "y", ""}},
		{Name: "c", Values: []string{"", "", ""}},
	})
	var got []map[string]int
	EmitFieldNamesStreamBlocks(db, func(b *logstorage.DataBlock) {
		m := map[string]int{}
		for _, c := range b.GetColumns(false) {
			m[c.Name] = len(c.Values)
		}
		got = append(got, m)
	})
	want := []map[string]int{{"_stream_id": 2, "a": 2}, {"_stream_id": 1, "b": 1}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("blocks = %v, want %v", got, want)
	}
	EmitFieldNamesStreamBlocks(nil, func(*logstorage.DataBlock) { t.Fatal("nil block emitted") })
}

// The insert buffer hands out blocks whose names and values point into memory
// it reuses once the callback returns: the counter keeps its own copies.
func TestFieldNamesCounter_KeepsNoBorrowedStrings(t *testing.T) {
	buf := []byte("borrowed.name")
	stream := []byte("stream-1")
	name := unsafe.String(&buf[0], len(buf))
	sid := unsafe.String(&stream[0], len(stream))
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_stream_id", Values: []string{sid}},
		{Name: name, Values: []string{"v"}},
	})
	for _, capacity := range []int{10, 0} { // tracked and past-the-cap paths
		c := newFieldNamesCounter(capacity)
		c.add(0, db)
		copy(buf, "XXXXXXXXXXXXX")
		copy(stream, "XXXXXXXX")
		want := map[string]uint64{"_stream_id": 1, "borrowed.name": 1}
		if got := fnHits(c.result()); !reflect.DeepEqual(got, want) {
			t.Fatalf("cap %d: hits = %v, want %v", capacity, got, want)
		}
		copy(buf, "borrowed.name")
		copy(stream, "stream-1")
	}
}

// A stream held both in the insert buffer and in objects is two blocks on hot
// storage (an in-memory part is not merged into the stored ones yet): the
// counter unions the blocks of a stream within a layer only.
func TestFieldNamesCounter_BufferAndObjectsAreSeparateBlocks(t *testing.T) {
	c := newFieldNamesCounter(10)
	c.add(0, fnBlock("s1", 3, map[string]bool{"a": true}))
	c.add(0, MarkFieldNamesBufferBlock(fnBlock("s1", 2, map[string]bool{"a": true, "b": true})))
	want := map[string]uint64{"_stream_id": 5, "a": 5, "b": 2}
	if got := fnHits(c.result()); !reflect.DeepEqual(got, want) {
		t.Fatalf("hits = %v, want %v", got, want)
	}
	if MarkFieldNamesBufferBlock(nil) != nil {
		t.Error("a nil block stays nil")
	}
}
