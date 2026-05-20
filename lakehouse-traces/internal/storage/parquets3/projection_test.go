package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestQueryColumns_TracesExactMatch(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cols := queryColumns(`trace_id:="abc123"`, reg)

	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp_unix_nano must always be included")
	}
	if !cols["trace_id"] {
		t.Error("trace_id must be included")
	}
	if cols["span.name"] {
		t.Error("span.name should not be included when not referenced")
	}
}

func TestQueryColumns_TracesWildcard(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cols := queryColumns(`*`, reg)
	if cols != nil {
		t.Error("wildcard query should return nil")
	}
}

func TestQueryColumns_TracesServiceName(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	// In traces, service.name has InternalName "resource_attr:service.name"
	cols := queryColumns(`service.name:="api"`, reg)
	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp must be included")
	}
	if !cols["service.name"] {
		t.Error("service.name parquet column must be included")
	}
}

func TestQueryColumns_TracesEmpty(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cols := queryColumns(``, reg)
	if cols != nil {
		t.Error("empty query should return nil")
	}
}

func TestQueryColumns_TracesSpanName(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	// span.name parquet column, InternalName is "name"
	cols := queryColumns(`name:="GET /api"`, reg)
	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp must be included")
	}
	if !cols["span.name"] {
		t.Error("span.name parquet column must be included when querying by internal name 'name'")
	}
}

func TestQueryColumns_TracesMultipleFilters(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	cols := queryColumns(`trace_id:="abc" AND service.name:="api"`, reg)

	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp must be included")
	}
	if !cols["trace_id"] {
		t.Error("trace_id must be included")
	}
	if !cols["service.name"] {
		t.Error("service.name must be included")
	}
}
