package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestQueryColumns_ExactMatch(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`trace_id:="abc123"`, reg, nil)

	if cols != nil {
		t.Error("filter-only query should return nil (all columns)")
	}
}

func TestQueryColumns_Wildcard(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`*`, reg, nil)

	if cols != nil {
		t.Error("wildcard query should return nil (read all columns)")
	}
}

func TestQueryColumns_WildcardWithSort(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`* | sort by (_time) desc limit 1`, reg, nil)

	if cols != nil {
		t.Error("wildcard with sort/limit pipes should return nil (sort doesn't select columns)")
	}
}

func TestQueryColumns_FilterWithSort(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`trace_id:="abc" | sort by (_time) desc`, reg, nil)

	if cols != nil {
		t.Error("filter with sort pipe should return nil (sort doesn't select columns)")
	}
}

func TestQueryColumns_MultipleFilters(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`service.name:="api" AND level:="ERROR"`, reg, nil)

	if cols != nil {
		t.Error("filter-only query should return nil (all columns)")
	}
}

func TestQueryColumns_EmptyQuery(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(``, reg, nil)

	if cols != nil {
		t.Error("empty query should return nil (read all columns)")
	}
}

func TestQueryColumns_BodySearch(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`"error connecting"`, reg, nil)

	if cols != nil {
		t.Error("free text search without column-selecting pipes should return nil")
	}
}

func TestQueryColumns_WithFieldsPipe(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`service.name:="api" | fields _time, _msg`, reg, []string{"_time", "_msg"})

	if cols == nil {
		t.Fatal("pipe with fields should return projected columns")
	}
	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp_unix_nano must always be included")
	}
	if !cols["service.name"] {
		t.Error("service.name must be included for exact match filter with pipe")
	}
	if !cols["body"] {
		t.Error("body must be included from | fields _msg pipe")
	}
}

func TestQueryColumns_WithStatsPipe(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`service.name:="api" | stats count() by (service.name)`, reg, []string{"service.name"})

	if cols == nil {
		t.Fatal("pipe with stats should return projected columns")
	}
	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp_unix_nano must always be included")
	}
	if !cols["service.name"] {
		t.Error("service.name must be included from by(service.name)")
	}
}

// Regression: stats by(level) with a service filter must project
// severity_text (VL's "level") for pipeStats grouping.
func TestQueryColumns_StatsByLevel_WithFilter(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(
		`service.name:="api-gateway" | stats by(level) count() as count`,
		reg,
		[]string{"level"},
	)

	if cols == nil {
		t.Fatal("stats by(level) with filter must return projected columns")
	}
	if !cols["severity_text"] {
		t.Error("severity_text (VL 'level') must be projected for stats by(level)")
	}
	if !cols["service.name"] {
		t.Error("service.name must be projected for filter")
	}
	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp_unix_nano must always be projected")
	}
}

// Regression: _time bucketing in by() must not prevent level projection.
func TestQueryColumns_StatsByLevel_WithTimeGrouping(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(
		`service.name:="api-gateway" | stats by(_time:60000000000, level) count() as count`,
		reg,
		[]string{"_time", "level"},
	)

	if cols == nil {
		t.Fatal("stats by(_time:..., level) with filter must return projected columns")
	}
	if !cols["severity_text"] {
		t.Error("severity_text must be projected for by(level) even with _time bucketing")
	}
	if !cols["service.name"] {
		t.Error("service.name must be projected for filter")
	}
}

func TestQueryColumns_StatsByMultipleFields(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(
		`service.name:="api-gateway" | stats by(level, cloud.region) count() as count`,
		reg,
		[]string{"level", "cloud.region"},
	)

	if cols == nil {
		t.Fatal("stats by multiple fields must return projected columns")
	}
	if !cols["severity_text"] {
		t.Error("severity_text must be projected for by(level)")
	}
	if !cols["cloud.region"] {
		t.Error("cloud.region must be projected for by(cloud.region)")
	}
}

func TestQueryColumns_FieldsPipe_ProjectsNamedFields(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(
		`service.name:="api" | fields level, cloud.region`,
		reg,
		[]string{"level", "cloud.region"},
	)

	if cols == nil {
		t.Fatal("| fields with named fields must return projected columns")
	}
	if !cols["severity_text"] {
		t.Error("severity_text must be projected for | fields level")
	}
	if !cols["cloud.region"] {
		t.Error("cloud.region must be projected for | fields cloud.region")
	}
}

func TestQueryColumns_UniqByLevel(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(
		`service.name:="api" | uniq by(level)`,
		reg,
		[]string{"level"},
	)

	if cols == nil {
		t.Fatal("uniq by(level) with filter must return projected columns")
	}
	if !cols["severity_text"] {
		t.Error("severity_text must be projected for uniq by(level)")
	}
}

func TestQueryColumns_TopByLevel(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(
		`service.name:="api" | top 10 by(level)`,
		reg,
		[]string{"level"},
	)

	if cols == nil {
		t.Fatal("top by(level) with filter must return projected columns")
	}
	if !cols["severity_text"] {
		t.Error("severity_text must be projected for top by(level)")
	}
}

// Stats by(level) without filter should still project only needed columns.
func TestQueryColumns_StatsByLevel_NoFilter(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(
		`_time:[2025-01-01,2025-01-02] | stats by(level) count() as count`,
		reg,
		[]string{"level"},
	)

	if cols == nil {
		t.Fatal("stats by(level) should return projected columns (timestamp + severity_text)")
	}
	if !cols["severity_text"] {
		t.Error("severity_text must be projected for by(level)")
	}
	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp_unix_nano must always be projected")
	}
	if len(cols) != 2 {
		t.Errorf("expected exactly 2 projected columns (timestamp + severity_text), got %d: %v", len(cols), cols)
	}
}

// Regression: pipeFields=nil with no column-selecting pipe → nil (all columns).
func TestQueryColumns_NilPipeFields_NoSelectingPipe(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`service.name:="api" | sort by (_time) desc`, reg, nil)

	if cols != nil {
		t.Error("nil pipeFields without column-selecting pipe must return nil")
	}
}

// Non-nil pipeFields triggers projection even without text-detected pipes.
func TestQueryColumns_PipeFields_OverridesTextDetection(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`service.name:="api"`, reg, []string{"level"})

	if cols == nil {
		t.Fatal("non-nil pipeFields must trigger projection")
	}
	if !cols["severity_text"] {
		t.Error("severity_text must be projected from pipeFields")
	}
	if !cols["service.name"] {
		t.Error("service.name must be projected from filter")
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
