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

// TestQueryColumns_StreamSelectorProjected guards the regression where VL's
// q.String() emits a stream selector without the explicit `_stream:` prefix
// (`{x=y}` instead of `_stream:{x=y}`). Our projection must still include
// the `_stream` column so filterStream.matchRow can evaluate it.
//
// Negative-control: remove the referencesStreamSelector call (or its
// `cols["_stream"] = true` line) from queryColumns and this test must fail.
func TestQueryColumns_StreamSelectorProjected(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)

	cases := []struct {
		name     string
		queryStr string
		pipes    []string
	}{
		{
			name:     "unprefixed stream selector + fields pipe",
			queryStr: `{"resource_attr:service.name"="api-gateway"} | fields _time, trace_id`,
			pipes:    []string{"_time", "trace_id"},
		},
		{
			name:     "explicit _stream: prefix + fields pipe",
			queryStr: `_stream:{resource_attr:service.name="api-gateway"} | fields _time, trace_id`,
			pipes:    []string{"_time", "trace_id"},
		},
		{
			name:     "stream selector after _time + fields pipe (VL serialization shape)",
			queryStr: `_time:[2026-05-30T00:00:00Z,2026-05-30T01:00:00Z] {"resource_attr:service.name"="api-gateway"} | fields _time, trace_id`,
			pipes:    []string{"_time", "trace_id"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cols := queryColumns(tc.queryStr, reg, tc.pipes)
			if cols == nil {
				t.Fatalf("expected projection (pipes present), got nil for query %q", tc.queryStr)
			}
			if !cols["_stream"] {
				t.Errorf("REGRESSION: `_stream` column missing from projection for query %q.\n"+
					"  filterStream.matchRow requires the `_stream` column in the DataBlock.\n"+
					"  Without it, the stream filter silently rejects every row.\n"+
					"  Got cols=%v", tc.queryStr, cols)
			}
			if !cols["trace_id"] {
				t.Errorf("trace_id missing from projection for query %q; got cols=%v",
					tc.queryStr, cols)
			}
		})
	}
}

// TestQueryColumns_QuotedFieldNameProjected guards the regression where VL's
// q.String() wraps field names containing `:` (e.g. `span_attr:http.status_code`)
// in double quotes: `"span_attr:http.status_code":=200`. Our projection must
// detect both bare and quoted forms.
//
// Negative-control: remove the quoted-form patterns from referencesField
// (the two `"`+name+`":...` entries) and this test must fail.
func TestQueryColumns_QuotedFieldNameProjected(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)

	cases := []struct {
		name           string
		queryStr       string
		pipes          []string
		wantParquetCol string
	}{
		{
			name:           "quoted span_attr field + fields pipe",
			queryStr:       `"span_attr:http.status_code":="200" | fields _time, trace_id`,
			pipes:          []string{"_time", "trace_id"},
			wantParquetCol: "http.status_code",
		},
		{
			name:           "quoted span_attr field (unquoted value) + fields pipe (VL serialized shape)",
			queryStr:       `"span_attr:http.status_code":=200 | fields _time, trace_id`,
			pipes:          []string{"_time", "trace_id"},
			wantParquetCol: "http.status_code",
		},
		{
			name:           "bare span_attr field + fields pipe still works",
			queryStr:       `span_attr:http.status_code:=200 | fields _time, trace_id`,
			pipes:          []string{"_time", "trace_id"},
			wantParquetCol: "http.status_code",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cols := queryColumns(tc.queryStr, reg, tc.pipes)
			if cols == nil {
				t.Fatalf("expected projection (pipes present), got nil for query %q", tc.queryStr)
			}
			if !cols[tc.wantParquetCol] {
				t.Errorf("REGRESSION: `%s` column missing from projection for query %q.\n"+
					"  The filter references it but referencesField didn't detect the\n"+
					"  quoted-name shape. The filter would silently reject every row.\n"+
					"  Got cols=%v", tc.wantParquetCol, tc.queryStr, cols)
			}
		})
	}
}
