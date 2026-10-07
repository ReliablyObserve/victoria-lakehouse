package parquets3

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// Property: for random single-stream blocks (random column sets, const and
// varying columns, values of 255, 256 and 257 bytes, sparse columns),
// storage.OrderColumnsLikeUpstream puts the columns of any permutation of the
// block in the order upstream's own engine lists them. The reference is the
// insert buffer (an upstream logstorage instance): the order of the block it
// answers with is the hot order for exactly these rows.
func TestOrderColumnsLikeUpstream_PropertyMatchesBufferEngine(t *testing.T) {
	e := newRestartEnv(t)
	base := time.Now().Add(-5 * time.Hour).Truncate(time.Minute)
	names := []string{"a", "b", "alpha", "zeta", "level", "region", "svc_x", "K", "k", "trace", "x.y", "_extra", "~tilde", "0num"}
	sizes := []int{1, 255, 256, 257, 300}
	rnd := rand.New(rand.NewSource(427))

	const iterations = 60
	for it := 0; it < iterations; it++ {
		at := base.Add(time.Duration(it) * time.Minute)
		nrows := 2 + rnd.Intn(6)
		rnd.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
		ncols := 1 + rnd.Intn(9)
		lr := logstorage.GetLogRows([]string{"svc"}, nil, nil, nil, "")
		var desc []string
		kinds := make([]int, ncols)
		cvals := make([]string, ncols)
		for c := 0; c < ncols; c++ {
			kinds[c] = rnd.Intn(3) // 0 const, 1 varying, 2 sparse
			cvals[c] = strings.Repeat("v", sizes[rnd.Intn(len(sizes))])
			desc = append(desc, fmt.Sprintf("%s/%d/%d", names[c], kinds[c], len(cvals[c])))
		}
		for r := 0; r < nrows; r++ {
			fields := []logstorage.Field{
				{Name: "svc", Value: fmt.Sprintf("s%d", it)},
				{Name: "_msg", Value: fmt.Sprintf("m%d-%d", it, r)},
			}
			for c := 0; c < ncols; c++ {
				v := cvals[c]
				switch kinds[c] {
				case 1:
					v = fmt.Sprintf("%s%d", cvals[c], r)
				case 2:
					if r%2 == 1 {
						continue
					}
				}
				fields = append(fields, logstorage.Field{Name: names[c], Value: v})
			}
			lr.MustAdd(logstorage.TenantID{}, at.Add(time.Duration(r)*time.Millisecond).UnixNano(), fields, 1)
		}
		e.segs.MustAddRows(lr)
		logstorage.PutLogRows(lr)
		e.segs.DebugFlush()

		q, err := logstorage.ParseQueryAtTimestamp("*", at.Add(time.Minute).UnixNano())
		if err != nil {
			t.Fatal(err)
		}
		q.AddTimeFilter(at.Add(-time.Second).UnixNano(), at.Add(30*time.Second).UnixNano())
		ids := []logstorage.TenantID{{}}
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, ids, q, false, nil)
		var mu sync.Mutex
		var blocks [][]logstorage.BlockColumn
		wb := func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			defer mu.Unlock()
			cols := db.GetColumns(false)
			cp := make([]logstorage.BlockColumn, len(cols))
			for i, c := range cols {
				cp[i] = logstorage.BlockColumn{Name: strings.Clone(c.Name), Values: cloneStrings(c.Values)}
			}
			blocks = append(blocks, cp)
		}
		searchFn := func(w logstorage.WriteDataBlockFunc) error {
			return e.s.RunQuery(context.Background(), ids, q, w)
		}
		if err := logstorage.RunQueryExternal(qctx, searchFn, wb); err != nil {
			t.Fatal(err)
		}
		if len(blocks) != 1 || len(blocks[0][0].Values) != nrows {
			t.Fatalf("iteration %d: want one block of %d rows, got %d blocks", it, nrows, len(blocks))
		}
		hot := blocks[0]
		want := blockNamesOf(hot)
		for p := 0; p < 8; p++ {
			cols := append([]logstorage.BlockColumn(nil), hot...)
			if p < 4 {
				rnd.Shuffle(len(cols), func(i, j int) { cols[i], cols[j] = cols[j], cols[i] })
			} else {
				shuffleWithinGroups(rnd, cols)
			}
			db := &logstorage.DataBlock{}
			db.SetColumns(cols)
			storage.OrderColumnsLikeUpstream(db)
			if got := blockNamesOf(db.GetColumns(false)); got != want {
				t.Fatalf("iteration %d (columns name/kind/size: %v): OrderColumnsLikeUpstream gave %s, upstream's engine lists %s", it, desc, got, want)
			}
		}
	}
}

func cloneStrings(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = strings.Clone(s)
	}
	return out
}

func blockNamesOf(cols []logstorage.BlockColumn) string {
	var out []string
	for _, c := range cols {
		out = append(out, c.Name)
	}
	return strings.Join(out, ",")
}

// shuffleWithinGroups shuffles the columns only among the positions of their
// own group (special column, const, other), so the groups stay in place and
// only the order inside a group is wrong: the case an already-ordered fast
// path that looks at groups alone would miss.
func shuffleWithinGroups(rnd *rand.Rand, cols []logstorage.BlockColumn) {
	byGroup := map[string][]int{}
	for i, c := range cols {
		g := "other"
		switch {
		case c.Name == "_time" || c.Name == "_stream_id" || c.Name == "_stream" || c.Name == "_msg":
			g = "special-" + c.Name
		case constValues(c.Values):
			g = "const"
		}
		byGroup[g] = append(byGroup[g], i)
	}
	for _, idx := range byGroup {
		vals := make([]logstorage.BlockColumn, len(idx))
		for k, i := range idx {
			vals[k] = cols[i]
		}
		rnd.Shuffle(len(vals), func(a, b int) { vals[a], vals[b] = vals[b], vals[a] })
		for k, i := range idx {
			cols[i] = vals[k]
		}
	}
}

func constValues(vs []string) bool {
	if len(vs) == 0 || len(vs[0]) > 256 {
		return len(vs) == 0
	}
	for _, v := range vs[1:] {
		if v != vs[0] {
			return false
		}
	}
	return true
}
