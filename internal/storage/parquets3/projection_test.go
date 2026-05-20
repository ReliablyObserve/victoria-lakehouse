package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestQueryColumns_ExactMatch(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`trace_id:="abc123"`, reg)

	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp_unix_nano must always be included")
	}
	if !cols["trace_id"] {
		t.Error("trace_id must be included for exact match filter")
	}
	if cols["body"] {
		t.Error("body should not be included when not referenced")
	}
}

func TestQueryColumns_Wildcard(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`*`, reg)

	if cols != nil {
		t.Error("wildcard query should return nil (read all columns)")
	}
}

func TestQueryColumns_MultipleFilters(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	cols := queryColumns(`service.name:="api" AND level:="ERROR"`, reg)

	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp must be included")
	}
	if !cols["service.name"] {
		t.Error("service.name must be included")
	}
	if !cols["severity_text"] {
		t.Error("severity_text must be included (level is InternalName, severity_text is ParquetColumn)")
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

	if !cols["body"] {
		t.Error("body must be included for free text search")
	}
	if !cols["timestamp_unix_nano"] {
		t.Error("timestamp must be included")
	}
}
