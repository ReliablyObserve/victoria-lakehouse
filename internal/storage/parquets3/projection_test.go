package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestQueryColumns_ExactMatch(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`trace_id:="abc123"`, reg)

	if cols != nil {
		t.Error("filter-only query should return nil (all columns)")
	}
}

func TestQueryColumns_Wildcard(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`*`, reg)

	if cols != nil {
		t.Error("wildcard query should return nil (read all columns)")
	}
}

func TestQueryColumns_WildcardWithSort(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`* | sort by (_time) desc limit 1`, reg)

	if cols != nil {
		t.Error("wildcard with sort/limit pipes should return nil (sort doesn't select columns)")
	}
}

func TestQueryColumns_FilterWithSort(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`trace_id:="abc" | sort by (_time) desc`, reg)

	if cols != nil {
		t.Error("filter with sort pipe should return nil (sort doesn't select columns)")
	}
}

func TestQueryColumns_MultipleFilters(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`service.name:="api" AND level:="ERROR"`, reg)

	if cols != nil {
		t.Error("filter-only query should return nil (all columns)")
	}
}

func TestQueryColumns_EmptyQuery(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(``, reg)

	if cols != nil {
		t.Error("empty query should return nil (read all columns)")
	}
}

func TestQueryColumns_BodySearch(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`"error connecting"`, reg)

	if cols != nil {
		t.Error("free text search without column-selecting pipes should return nil")
	}
}

func TestQueryColumns_WithFieldsPipe(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`service.name:="api" | fields _time, _msg`, reg)

	if cols == nil {
		t.Fatal("pipe with fields should return projected columns")
	}
	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp_unix_nano must always be included")
	}
	if !cols["service.name"] {
		t.Error("service.name must be included for exact match filter with pipe")
	}
}

func TestQueryColumns_WithStatsPipe(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`service.name:="api" | stats count() by (service.name)`, reg)

	if cols == nil {
		t.Fatal("pipe with stats should return projected columns")
	}
	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp_unix_nano must always be included")
	}
}

func TestHasColumnSelectingPipe(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{`*`, false},
		{`trace_id:="abc"`, false},
		{`* | sort by (_time) desc limit 1`, false},
		{`* | sort by (_time) desc | limit 10`, false},
		{`* | fields _time, _msg`, true},
		{`* | stats count() by (service.name)`, true},
		{`* | uniq by (service.name)`, true},
		{`* | top 10 by (service.name)`, true},
	}
	for _, tt := range tests {
		got := hasColumnSelectingPipe(tt.query)
		if got != tt.want {
			t.Errorf("hasColumnSelectingPipe(%q) = %v, want %v", tt.query, got, tt.want)
		}
	}
}
