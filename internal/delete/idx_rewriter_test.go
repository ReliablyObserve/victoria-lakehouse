package delete

import (
	"context"
	"encoding/hex"
	"math/rand"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/tracesidx"
)

func idxFixture(n int) []tracesidx.Row {
	rng := rand.New(rand.NewSource(3))
	rows := make([]tracesidx.Row, n)
	for i := range rows {
		var id [16]byte
		rng.Read(id[:])
		h := hex.EncodeToString(id[:])
		s := int64(1_800_000_000_000_000_000) + int64(i)*1_000_000
		d := int64(1_000_000 * (i%7 + 1))
		rows[i] = tracesidx.Row{Time: s, Stream: tracesidx.StreamOf(h), TraceID: h, Start: s, End: s + d, Duration: d}
	}
	return rows
}

// A delete task applies to the trace_id_idx table as it does to every row of the
// storage: the rows its filter matches are removed (filters on index fields, `_time`,
// `_stream`), the rewritten object stays in the table's directory and is a valid
// object of the table; a filter on a span field matches no index row and rewrites
// nothing.
func TestIdxDelete_RewriteRemovesMatchingIndexRows(t *testing.T) {
	pool := newMockRewriterPool()
	rows := idxFixture(400)
	data, _, err := tracesidx.Marshal(append([]tracesidx.Row(nil), rows...), 3)
	if err != nil {
		t.Fatal(err)
	}
	const key = "5/6/traces_idx/dt=2026-10-13/hour=10/abcd-0.parquet"
	pool.Put(key, data)
	rw := NewRewriter(pool, "5/6/traces/", 100, "traces")
	ctx := context.Background()

	victim := rows[7].TraceID
	for _, tc := range []struct {
		name, query string
		removed     int
	}{
		{"one trace by its index id", `trace_id_idx:="` + victim + `"`, 1},
		{"a duration range", `duration:>=5000000`, countIdx(rows, func(r tracesidx.Row) bool { return r.Duration >= 5_000_000 })},
		{"a span field matches no index row", `trace_id:="` + victim + `"`, 0},
		{"the whole index stream", `{trace_id_idx_stream=~".+"}`, len(rows)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := []Tombstone{{Tenants: []TenantRef{{}}, ID: "t", Query: tc.query, StartNs: 0, EndNs: 1 << 62}}
			res, err := rw.Prepare(ctx, key, ts)
			if err != nil {
				t.Fatal(err)
			}
			if int(res.RowsRemoved) != tc.removed {
				t.Fatalf("removed %d rows, want %d", res.RowsRemoved, tc.removed)
			}
			if tc.removed == 0 || tc.removed == len(rows) {
				return // nothing to write, or no replacement
			}
			if !strings.HasPrefix(res.NewKey, "5/6/traces_idx/dt=2026-10-13/hour=10/") {
				t.Fatalf("the replacement %q left the table's directory", res.NewKey)
			}
			if err := rw.Upload(ctx, res); err != nil {
				t.Fatal(err)
			}
			out, _ := pool.Get(res.NewKey)
			got, err := tracesidx.ReadAll(out)
			if err != nil || len(got) != len(rows)-tc.removed {
				t.Fatalf("replacement holds %d rows (%v), want %d", len(got), err, len(rows)-tc.removed)
			}
			for _, r := range got {
				if tc.name == "one trace by its index id" && r.TraceID == victim {
					t.Fatal("the deleted trace's index row is still there")
				}
			}
			if res.MinTimeNs == 0 || res.MaxTimeNs < res.MinTimeNs {
				t.Fatalf("bounds %d..%d", res.MinTimeNs, res.MaxTimeNs)
			}
		})
	}
}

func countIdx(rows []tracesidx.Row, f func(tracesidx.Row) bool) int {
	n := 0
	for _, r := range rows {
		if f(r) {
			n++
		}
	}
	return n
}
