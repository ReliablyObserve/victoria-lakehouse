package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestQueryColumns_TracesExactMatch(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cols := queryColumns(`trace_id:="abc123"`, reg, nil)

	if cols != nil {
		t.Error("filter-only trace_id query should return nil (all columns for span rendering)")
	}
}

func TestQueryColumns_TracesWildcard(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cols := queryColumns(`*`, reg, nil)
	if cols != nil {
		t.Error("wildcard query should return nil")
	}
}

func TestQueryColumns_TracesWildcardWithSort(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cols := queryColumns(`* | sort by (_time) desc limit 1`, reg, nil)
	if cols != nil {
		t.Error("wildcard with sort/limit pipes should return nil")
	}
}

func TestQueryColumns_TracesServiceName(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cols := queryColumns(`service.name:="api"`, reg, nil)

	if cols != nil {
		t.Error("filter-only query should return nil (all columns)")
	}
}

func TestQueryColumns_TracesEmpty(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cols := queryColumns(``, reg, nil)
	if cols != nil {
		t.Error("empty query should return nil")
	}
}

func TestQueryColumns_TracesSpanName(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cols := queryColumns(`name:="GET /api"`, reg, nil)

	if cols != nil {
		t.Error("filter-only query should return nil (all columns)")
	}
}

func TestQueryColumns_TracesMultipleFilters(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cols := queryColumns(`trace_id:="abc" AND service.name:="api"`, reg, nil)

	if cols != nil {
		t.Error("filter-only query should return nil (all columns)")
	}
}

func TestQueryColumns_TracesWithFieldsPipe(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cols := queryColumns(`trace_id:="abc" | fields _time, trace_id`, reg, []string{"_time", "trace_id"})

	if cols == nil {
		t.Fatal("pipe with fields should return projected columns")
	}
	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp_unix_nano must always be included")
	}
	if !cols["trace_id"] {
		t.Error("trace_id must be included for exact match filter with pipe")
	}
}

func TestQueryColumns_TracesWithStatsPipe(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cols := queryColumns(`service.name:="api" | stats count() by (service.name)`, reg, []string{"service.name"})

	if cols == nil {
		t.Fatal("pipe with stats should return projected columns")
	}
	if !cols["service.name"] {
		t.Error("service.name must be included from by(service.name)")
	}
}

func TestQueryColumns_TracesStatsByName_WithFilter(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cols := queryColumns(
		`service.name:="api" | stats by(name) count() as count`,
		reg,
		[]string{"name"},
	)

	if cols == nil {
		t.Fatal("stats by(name) with filter must return projected columns")
	}
	if !cols["span.name"] {
		t.Error("span.name (VL 'name') must be projected for stats by(name)")
	}
	if !cols["service.name"] {
		t.Error("service.name must be projected for filter")
	}
}
