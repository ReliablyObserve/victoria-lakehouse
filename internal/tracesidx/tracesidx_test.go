package tracesidx

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// countSource is a Source over a byte slice that counts reads.
type countSource struct {
	key   string
	data  []byte
	mu    sync.Mutex
	reads int
	bytes int64
	kinds map[ReadKind]int
}

func newCountSource(key string, data []byte) *countSource {
	return &countSource{key: key, data: data, kinds: map[ReadKind]int{}}
}

func (c *countSource) Key() string { return c.key }
func (c *countSource) Size() int64 { return int64(len(c.data)) }
func (c *countSource) ReadRange(_ context.Context, k ReadKind, off, n int64) ([]byte, error) {
	c.mu.Lock()
	c.reads++
	c.bytes += n
	c.kinds[k]++
	c.mu.Unlock()
	if off < 0 || off+n > int64(len(c.data)) {
		return nil, fmt.Errorf("range %d+%d outside %d", off, n, len(c.data))
	}
	return append([]byte(nil), c.data[off:off+n]...), nil
}

func randRows(rng *rand.Rand, n int) []Row {
	rows := make([]Row, n)
	for i := range rows {
		var id [16]byte
		rng.Read(id[:])
		h := hex.EncodeToString(id[:])
		s := rng.Int63n(86400e9)
		d := rng.Int63n(60e9)
		rows[i] = Row{Time: s, Stream: StreamOf(h), TraceID: h, Start: s, End: s + d, Duration: d}
	}
	return rows
}

func TestMarshalLocateRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, n := range []int{1, 2, 100, 5000, RowsPerGroup + 1000} {
		rows := randRows(rng, n)
		truth := map[string]Row{}
		for _, r := range rows {
			truth[r.TraceID] = r
		}
		data, info, err := Marshal(rows, 3)
		if err != nil {
			t.Fatal(err)
		}
		if info.Rows != int64(n) || info.MinID != rows[0].TraceID || info.MaxID != rows[n-1].TraceID {
			t.Fatalf("info %+v", info)
		}
		loc := NewLocator(4)
		src := newCountSource(fmt.Sprintf("o%d", n), data)
		checked := 0
		for id, want := range truth {
			got, st, err := loc.Lookup(context.Background(), src, id, WantAll)
			if err != nil {
				t.Fatalf("n=%d id=%s: %v", n, id, err)
			}
			if len(got) != 1 || got[0] != want {
				t.Fatalf("n=%d id=%s: got %+v want %+v (%+v)", n, id, got, want, st)
			}
			if checked++; checked >= 300 {
				break
			}
		}
		// Absent ids and ids around the fences.
		for _, id := range []string{"", "0", info.MinID[:len(info.MinID)-1], "ffffffffffffffffffffffffffffffffff", hex.EncodeToString(make([]byte, 16))} {
			got, _, err := loc.Lookup(context.Background(), src, id, WantAll)
			if err != nil {
				t.Fatal(err)
			}
			if _, present := truth[id]; !present && len(got) != 0 {
				t.Fatalf("absent id %q returned %+v", id, got)
			}
		}
		t.Logf("n=%d: object %d B (%.1f B/row), %d groups, reads for %d lookups: %d (%.1f avg), %d B", n, len(data), float64(len(data))/float64(n), info.Groups, checked, src.reads, float64(src.reads)/float64(checked+5), src.bytes)
	}
}

func TestDuplicateIDsAndTiesAcrossPages(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	rows := randRows(rng, 3000)
	dup := rows[10].TraceID
	for i := 0; i < 700; i++ { // one id with more rows than a page holds
		r := rows[10]
		r.Time += int64(i)
		r.Start = r.Time
		rows = append(rows, r)
	}
	data, _, err := Marshal(rows, 3)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := NewLocator(2).Lookup(context.Background(), newCountSource("dup", data), dup, WantAll)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 701 {
		t.Fatalf("got %d rows, want 701", len(got))
	}
}

func checkBloom(data []byte, rows []Row) error {
	return checkBloomImpl(data, rows)
}
