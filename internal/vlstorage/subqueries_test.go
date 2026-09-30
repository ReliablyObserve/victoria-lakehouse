package vlstorage

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// filterStore serves fixed rows, evaluating the query's own filter on each, as a
// cold tier does.
type filterStore struct {
	mockStore
	rows []map[string]string
}

func (s *filterStore) RunQuery(_ context.Context, _ []logstorage.TenantID, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	f := logstorage.QueryFilter(q)
	var kept []map[string]string
	for _, r := range s.rows {
		var fields []logstorage.Field
		for k, v := range r {
			fields = append(fields, logstorage.Field{Name: k, Value: v})
		}
		if f == nil || f.MatchRow(fields) {
			kept = append(kept, r)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	names := map[string]bool{}
	for _, r := range kept {
		for k := range r {
			names[k] = true
		}
	}
	var cols []logstorage.BlockColumn
	for k := range names {
		vals := make([]string, len(kept))
		for i, r := range kept {
			vals[i] = r[k]
		}
		cols = append(cols, logstorage.BlockColumn{Name: k, Values: vals})
	}
	var db logstorage.DataBlock
	db.SetColumns(cols)
	writeBlock(0, &db)
	return nil
}

func runAdapter(t *testing.T, s *filterStore, query string) []string {
	t.Helper()
	q, err := logstorage.ParseQueryAtTimestamp(query, time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	a := &adapter{store: s}
	var out []string
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, nil)
	err = a.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		cols := db.GetColumns(false)
		for i := 0; i < db.RowsCount(); i++ {
			var parts []string
			for _, c := range cols {
				parts = append(parts, c.Name+"="+c.Values[i])
			}
			sort.Strings(parts)
			out = append(out, strings.Join(parts, ","))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

// The logs binary's cold-tier query path resolves a query's subqueries with
// upstream's own initSubqueries: a `| union` used to panic in its pipe processor,
// and an in() filter inside a pipe matched nothing, because only `join` was
// resolved. (An in() in a query with no pipes at all is a documented gap.)
func TestRunQuery_UnionAndInSubqueriesAreResolved(t *testing.T) {
	s := &filterStore{rows: []map[string]string{
		{"_msg": "a", "level": "error"},
		{"_msg": "b", "level": "warn"},
		{"_msg": "c", "level": "error"},
	}}

	got := runAdapter(t, s, `level:error | union (level:warn) | stats count() n`)
	if strings.Join(got, ";") != "n=3" {
		t.Errorf("union: got %v, want [n=3]", got)
	}
	got = runAdapter(t, s, `_msg:in(level:error | fields _msg) | stats count() n`)
	if strings.Join(got, ";") != "n=2" {
		t.Errorf("in() subquery: got %v, want [n=2]", got)
	}
	got = runAdapter(t, s, `level:error | join by (level) (* | stats by (level) count() c) | fields _msg, c`)
	if strings.Join(got, ";") != "_msg=a,c=2;_msg=c,c=2" {
		t.Errorf("join: got %v", got)
	}
}
