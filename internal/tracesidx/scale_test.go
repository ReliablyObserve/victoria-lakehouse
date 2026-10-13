package tracesidx

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

// TestScaleLookupReads measures the ranged reads of one trace lookup against objects of
// growing size. It writes a table only when T6_SCALE_ROWS (a comma-separated list of
// row counts) is set, because the larger sizes take memory and time:
//
//	T6_SCALE_ROWS=1000,10000,100000,1000000 go test ./internal/tracesidx -run Scale -v
func TestScaleLookupReads(t *testing.T) {
	list := os.Getenv("T6_SCALE_ROWS")
	if list == "" {
		t.Skip("T6_SCALE_ROWS is not set")
	}
	for _, s := range splitComma(list) {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(int64(n)))
		rows := randRows(rng, n)
		ids := make([]string, 0, 300)
		for i := 0; i < 300; i++ {
			ids = append(ids, rows[rng.Intn(n)].TraceID)
		}
		data, info, err := Marshal(rows, 3)
		if err != nil {
			t.Fatal(err)
		}
		want := WantTime | WantStart | WantEnd
		// cold: a new Locator and a new Source per lookup (footer + index + pages).
		var coldReads, coldBytes int
		for _, id := range ids {
			src := newCountSource("scale", data)
			got, st, err := NewLocator(1).Lookup(context.Background(), src, id, want)
			if err != nil || len(got) != 1 {
				t.Fatalf("n=%d: lookup %s: %v %v", n, id, got, err)
			}
			coldReads += st.Reads
			coldBytes += int(st.Bytes)
		}
		// warm footer: one Locator keeps the parsed footer.
		loc := NewLocator(1)
		src := newCountSource("scale", data)
		if _, _, err := loc.Lookup(context.Background(), src, ids[0], want); err != nil {
			t.Fatal(err)
		}
		var warmReads, warmBytes int
		for _, id := range ids {
			_, st, _ := loc.Lookup(context.Background(), src, id, want)
			warmReads += st.Reads
			warmBytes += int(st.Bytes)
		}
		// the footer alone
		fsrc := newCountSource("scale", data)
		_, fst, _ := NewLocator(1).open(context.Background(), fsrc)
		t.Logf("rows=%d object=%d B (%.1f B/row) groups=%d footer reads=%d bytes=%d | cold avg reads=%.1f bytes=%.0f | warm-footer avg reads=%.1f bytes=%.0f",
			n, len(data), float64(len(data))/float64(n), info.Groups, fst.Reads, fst.Bytes,
			float64(coldReads)/float64(len(ids)), float64(coldBytes)/float64(len(ids)),
			float64(warmReads)/float64(len(ids)), float64(warmBytes)/float64(len(ids)))
	}
}

// TestScaleManyObjects measures a lookup across F objects of one hour (the flush
// objects before compaction) without any membership summary: every object's footer is
// read once (cold) and only the objects whose id range holds the id read more.
func TestScaleManyObjects(t *testing.T) {
	if os.Getenv("T6_SCALE_ROWS") == "" {
		t.Skip("T6_SCALE_ROWS is not set")
	}
	const total = 100_000
	for _, f := range []int{1, 8, 32, 128, 512} {
		rng := rand.New(rand.NewSource(int64(f)))
		rows := randRows(rng, total)
		objs := make([][]byte, f)
		for i := 0; i < f; i++ {
			part := append([]Row(nil), rows[i*total/f:(i+1)*total/f]...)
			objs[i], _, _ = Marshal(part, 3)
		}
		ids := make([]string, 100)
		for i := range ids {
			ids[i] = rows[rng.Intn(total)].TraceID
		}
		var cold, warm, coldB, warmB int
		loc := NewLocator(f + 1)
		for round := 0; round < 2; round++ {
			for _, id := range ids {
				for oi, d := range objs {
					src := newCountSource(fmt.Sprintf("o%d", oi), d)
					_, st, err := loc.Lookup(context.Background(), src, id, WantTime|WantStart|WantEnd)
					if err != nil {
						t.Fatal(err)
					}
					if round == 0 {
						cold += st.Reads
						coldB += int(st.Bytes)
					} else {
						warm += st.Reads
						warmB += int(st.Bytes)
					}
				}
			}
		}
		t.Logf("F=%d objects of %d rows: reads per lookup first pass (footers cold for the first id) avg %.1f, second pass (footers cached) avg %.1f; bytes %.0f / %.0f",
			f, total/f, float64(cold)/float64(len(ids)), float64(warm)/float64(len(ids)), float64(coldB)/float64(len(ids)), float64(warmB)/float64(len(ids)))
	}
}

func splitComma(s string) []string {
	var out []string
	cur := ""
	for _, c := range s {
		if c == ',' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(c)
	}
	return append(out, cur)
}
