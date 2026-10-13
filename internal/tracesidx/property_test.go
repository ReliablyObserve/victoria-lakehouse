package tracesidx

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// smallGroups cuts objects into many small row groups for the duration of a test, so a
// few thousand rows exercise fences, ids that straddle two groups and page boundaries.
func smallGroups(t *testing.T, n int) {
	t.Helper()
	old := rowsPerGroup
	rowsPerGroup = n
	t.Cleanup(func() { rowsPerGroup = old })
}

// A lookup returns exactly the rows a linear scan finds, for present ids (many rows of
// one id spanning pages and row groups), absent ids between and around the stored ids,
// and the first and last id of every row group.
func TestLocatorEqualsLinearScan(t *testing.T) {
	smallGroups(t, 700)
	for seed := int64(1); seed <= 6; seed++ {
		rng := rand.New(rand.NewSource(seed))
		var rows []Row
		pool := make([]string, 0, 400)
		for i := 0; i < 400; i++ {
			var id [16]byte
			rng.Read(id[:])
			pool = append(pool, hex.EncodeToString(id[:]))
		}
		for i := 0; i < 5000; i++ {
			id := pool[rng.Intn(len(pool))]
			if rng.Intn(50) == 0 {
				id = pool[0] // one id with hundreds of rows
			}
			s := rng.Int63n(1e12)
			rows = append(rows, Row{Time: s, Stream: StreamOf(id), TraceID: id, Start: s, End: s + rng.Int63n(1e9), Duration: rng.Int63n(1e9)})
		}
		data, info, err := Marshal(append([]Row(nil), rows...), 3)
		if err != nil {
			t.Fatal(err)
		}
		if info.Groups < 5 {
			t.Fatalf("only %d row groups", info.Groups)
		}
		want := map[string][]Row{}
		for _, r := range rows {
			want[r.TraceID] = append(want[r.TraceID], r)
		}
		for _, v := range want {
			Sort(v)
		}
		loc := NewLocator(8)
		src := newCountSource(fmt.Sprintf("p%d", seed), data)
		probe := append([]string(nil), pool...)
		probe = append(probe, "", "0", "g", "ffffffffffffffffffffffffffffffff", info.MinID, info.MaxID, info.MinID[:5], info.MaxID+"0")
		sorted := append([]string(nil), pool...)
		sort.Strings(sorted)
		for i := 0; i+1 < len(sorted); i += 7 { // just below and above stored ids
			probe = append(probe, sorted[i]+"0", sorted[i][:31])
		}
		for _, id := range probe {
			got, _, err := loc.Lookup(context.Background(), src, id, WantAll)
			if err != nil {
				t.Fatalf("seed %d id %q: %v", seed, id, err)
			}
			Sort(got)
			if fmt.Sprint(got) != fmt.Sprint(want[id]) {
				t.Fatalf("seed %d id %q: got %d rows %v, want %d rows %v", seed, id, len(got), head(got), len(want[id]), head(want[id]))
			}
		}
	}
}

func head(r []Row) []Row {
	if len(r) > 3 {
		return r[:3]
	}
	return r
}

// The fences (min and max id of every row group) are monotonic and non-overlapping
// (sorted by id alone), which is what makes the zone maps of every engine usable.
func TestRowGroupFencesAreMonotonic(t *testing.T) {
	smallGroups(t, 300)
	rows := randRows(rand.New(rand.NewSource(9)), 4000)
	data, _, err := Marshal(rows, 3)
	if err != nil {
		t.Fatal(err)
	}
	e, _, err := NewLocator(1).open(context.Background(), newCountSource("f", data))
	if err != nil {
		t.Fatal(err)
	}
	for i := range e.idMin {
		if string(e.idMin[i]) > string(e.idMax[i]) {
			t.Fatalf("group %d: min > max", i)
		}
		if i > 0 && string(e.idMax[i-1]) > string(e.idMin[i]) {
			t.Fatalf("groups %d and %d overlap: %s > %s", i-1, i, e.idMax[i-1], e.idMin[i])
		}
	}
}

// The writer is deterministic: the same rows in any arrival order give the same bytes
// (what lets a restarted flush window re-send an object under the same key).
func TestMarshalIsDeterministic(t *testing.T) {
	rows := randRows(rand.New(rand.NewSource(5)), 3000)
	a, _, _ := Marshal(append([]Row(nil), rows...), 3)
	rev := make([]Row, len(rows))
	for i := range rows {
		rev[len(rows)-1-i] = rows[i]
	}
	b, _, _ := Marshal(rev, 3)
	if string(a) != string(b) {
		t.Fatal("the same rows in another order gave different bytes")
	}
}

// The bloom filter is sized by distinct ids, a power of two, and holds every id.
func TestBloomFilterSizeAndMembership(t *testing.T) {
	smallGroups(t, 1000)
	rng := rand.New(rand.NewSource(4))
	rows := randRows(rng, 2500)
	for i := 0; i < 500; i++ { // repeats: distinct < rows
		rows = append(rows, rows[rng.Intn(2500)])
	}
	data, info, err := Marshal(rows, 3)
	if err != nil {
		t.Fatal(err)
	}
	for i, size := range info.BloomSize {
		if size&(size-1) != 0 || size < 32 {
			t.Fatalf("group %d: filter of %d bytes is not a power of two", i, size)
		}
	}
	if err := checkBloom(data, rows); err != nil {
		t.Fatal(err)
	}
}

// With the footer cached, a lookup of an id an object does not hold reads nothing from
// that object (its cached bloom filter rules it out), and a lookup of a stored id reads
// the same few ranges whatever the size of the object.
func TestWarmAbsentLookupReadsNothing(t *testing.T) {
	rng := rand.New(rand.NewSource(12))
	rows := randRows(rng, 20000)
	data, _, err := Marshal(append([]Row(nil), rows...), 3)
	if err != nil {
		t.Fatal(err)
	}
	loc := NewLocator(4)
	src := newCountSource("w", data)
	if _, _, err := loc.Lookup(context.Background(), src, rows[0].TraceID, WantAll); err != nil {
		t.Fatal(err)
	}
	fp, probes := 0, 2000
	for i := 0; i < probes; i++ {
		before := src.reads
		got, _, err := loc.Lookup(context.Background(), src, fmt.Sprintf("absent-%d", i), WantAll)
		if err != nil || len(got) != 0 {
			t.Fatalf("absent id: %v %v", got, err)
		}
		if src.reads != before {
			fp++
		}
	}
	if rate := float64(fp) / float64(probes); rate > 0.03 {
		t.Fatalf("%.1f%% of absent lookups read from the object, want about 1%%", 100*rate)
	}
}

// Without cached filters the row-group fences alone keep a lookup of an id outside the
// object's range from reading anything once the footer is cached.
func TestFencesAloneRuleOutIdsOutsideTheRange(t *testing.T) {
	smallGroups(t, 500)
	rows := randRows(rand.New(rand.NewSource(8)), 3000)
	data, info, err := Marshal(append([]Row(nil), rows...), 3)
	if err != nil {
		t.Fatal(err)
	}
	loc := NewLocator(1)
	src := newCountSource("fence", data)
	if _, _, err := loc.Lookup(context.Background(), src, rows[0].TraceID, WantAll); err != nil {
		t.Fatal(err)
	}
	e, _, _ := loc.open(context.Background(), src)
	e.filters = nil // only the fences
	before := src.reads
	for _, id := range []string{"", "0", info.MaxID + "0", "ffffffffffffffffffffffffffffffffffff", info.MinID[:len(info.MinID)-1]} {
		if info.MinID[:len(info.MinID)-1] == id && id >= info.MinID {
			continue
		}
		got, st, err := loc.Lookup(context.Background(), src, id, WantAll)
		if err != nil || len(got) != 0 {
			t.Fatalf("id %q: %v %v", id, got, err)
		}
		if st.RowGroups != 0 {
			t.Fatalf("id %q outside [%s, %s] consulted %d row groups", id, info.MinID, info.MaxID, st.RowGroups)
		}
	}
	if src.reads != before {
		t.Fatalf("%d reads for ids outside the fences", src.reads-before)
	}
}
