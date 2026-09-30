package vtstorageadapter

import (
	"context"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// hiddenStore emits one block with a visible and a hidden column, and lists a
// hidden and a visible field name.
type hiddenStore struct{ mockStorage }

func (s *hiddenStore) RunQuery(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	var db logstorage.DataBlock
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_msg", Values: []string{"m"}},
		{Name: "secret", Values: []string{"s3"}},
		{Name: "secret_token", Values: []string{"t"}},
	})
	writeBlock(0, &db)
	return nil
}

func (s *hiddenStore) GetFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return []logstorage.ValueWithHits{{Value: "_msg", Hits: 1}, {Value: "secret", Hits: 1}}, nil
}

func (s *hiddenStore) GetStreamFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return []logstorage.ValueWithHits{{Value: "app", Hits: 1}, {Value: "secret", Hits: 1}}, nil
}

// VictoriaTraces' LogsQL handlers (which the traces binary serves /select/logsql
// with) pass hidden_fields_filters through QueryContext.HiddenFieldsFilters; the
// logs adapter has always applied them, and this one must too, with and without
// pipes.
func TestHiddenFieldsAreRemoved(t *testing.T) {
	a := &Adapter{store: &hiddenStore{}}
	for _, query := range []string{"*", "* | fields _msg, secret, secret_token"} {
		q, err := logstorage.ParseQueryAtTimestamp(query, time.Now().UnixNano())
		if err != nil {
			t.Fatal(err)
		}
		qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, []string{"secret*"})
		var cols []string
		leaked := false
		if err := a.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
			for _, c := range db.GetColumns(false) {
				cols = append(cols, c.Name)
				// A pipe that names a hidden field (| fields secret) may still
				// print the column, but never with a value.
				if (c.Name == "secret" || c.Name == "secret_token") && len(c.Values) > 0 && c.Values[0] != "" {
					leaked = true
				}
			}
		}); err != nil {
			t.Fatal(err)
		}
		if leaked {
			t.Errorf("query %q: a hidden column's value reached the caller (columns %v)", query, cols)
		}
		if len(cols) == 0 {
			t.Errorf("query %q: no columns at all, the visible one was lost", query)
		}
	}

	q, _ := logstorage.ParseQueryAtTimestamp("*", time.Now().UnixNano())
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, []string{"secret"})
	names, err := a.GetFieldNames(qctx, "")
	if err != nil || len(names) != 1 || names[0].Value != "_msg" {
		t.Errorf("GetFieldNames = %v, %v; want only _msg", names, err)
	}
	streams, err := a.GetStreamFieldNames(qctx, "")
	if err != nil || len(streams) != 1 || streams[0].Value != "app" {
		t.Errorf("GetStreamFieldNames = %v, %v; want only app", streams, err)
	}

	// Without filters nothing is removed.
	qctx = logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, nil)
	names, _ = a.GetFieldNames(qctx, "")
	if len(names) != 2 {
		t.Errorf("no filters: GetFieldNames = %v, want both names", names)
	}
}
