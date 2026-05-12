package parquets3

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestParseFilter(t *testing.T) {
	tests := []struct {
		query    string
		wantNil  bool
	}{
		{"*", true},
		{"", true},
		{`service.name:="checkout"`, false},
		{`service.name:="checkout" level:="error"`, false},
		{`service.name:"checkout"`, false},
		{`service.name:~"check.*"`, false},
		{`service.name:="checkout" | limit 10`, false},
		{`NOT service.name:="checkout"`, false},
		{`service.name:="checkout" OR level:="error"`, false},
	}

	for _, tt := range tests {
		f := parseFilter(tt.query)
		if tt.wantNil && f != nil {
			t.Errorf("parseFilter(%q) = non-nil, want nil", tt.query)
		}
		if !tt.wantNil && f == nil {
			t.Errorf("parseFilter(%q) = nil, want non-nil", tt.query)
		}
	}
}

func TestStripPipes(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`service.name:="web" | limit 10`, `service.name:="web"`},
		{`service.name:="web"`, `service.name:="web"`},
		{`field:"val|ue"`, `field:"val|ue"`},
		{`*`, `*`},
	}
	for _, tt := range tests {
		got := stripPipes(tt.input)
		if got != tt.want {
			t.Errorf("stripPipes(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestFilterDataBlock(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "service.name", Values: []string{"checkout", "payment", "checkout", "auth"}},
		{Name: "level", Values: []string{"error", "info", "warn", "error"}},
		{Name: "_msg", Values: []string{"failed", "ok", "slow", "denied"}},
	})

	t.Run("exact match", func(t *testing.T) {
		f := parseFilter(`service.name:="checkout"`)
		result := filterDataBlock(db, f)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.RowsCount() != 2 {
			t.Errorf("got %d rows, want 2", result.RowsCount())
		}
	})

	t.Run("substring match", func(t *testing.T) {
		f := parseFilter(`service.name:"checkout"`)
		result := filterDataBlock(db, f)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.RowsCount() != 2 {
			t.Errorf("got %d rows, want 2", result.RowsCount())
		}
	})

	t.Run("combined AND filters", func(t *testing.T) {
		f := parseFilter(`service.name:="checkout" level:="error"`)
		result := filterDataBlock(db, f)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.RowsCount() != 1 {
			t.Errorf("got %d rows, want 1", result.RowsCount())
		}
	})

	t.Run("OR filter", func(t *testing.T) {
		f := parseFilter(`service.name:="checkout" OR service.name:="auth"`)
		result := filterDataBlock(db, f)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.RowsCount() != 3 {
			t.Errorf("got %d rows, want 3 (checkout x2 + auth x1)", result.RowsCount())
		}
	})

	t.Run("complex OR+AND", func(t *testing.T) {
		f := parseFilter(`(service.name:="checkout" AND level:="error") OR (service.name:="auth" AND level:="error")`)
		result := filterDataBlock(db, f)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.RowsCount() != 2 {
			t.Errorf("got %d rows, want 2", result.RowsCount())
		}
	})

	t.Run("no match", func(t *testing.T) {
		f := parseFilter(`service.name:="nonexistent"`)
		result := filterDataBlock(db, f)
		if result != nil {
			t.Errorf("expected nil result, got %d rows", result.RowsCount())
		}
	})

	t.Run("negated filter", func(t *testing.T) {
		f := parseFilter(`NOT service.name:="checkout"`)
		result := filterDataBlock(db, f)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.RowsCount() != 2 {
			t.Errorf("got %d rows, want 2", result.RowsCount())
		}
	})

	t.Run("nil filter passthrough", func(t *testing.T) {
		result := filterDataBlock(db, nil)
		if result != db {
			t.Error("expected same DataBlock back with nil filter")
		}
	})

	t.Run("bare word matches _msg", func(t *testing.T) {
		f := parseFilter(`denied`)
		result := filterDataBlock(db, f)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.RowsCount() != 1 {
			t.Errorf("got %d rows, want 1", result.RowsCount())
		}
	})

	t.Run("regex match", func(t *testing.T) {
		f := parseFilter(`service.name:~"check.*"`)
		result := filterDataBlock(db, f)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.RowsCount() != 2 {
			t.Errorf("got %d rows, want 2", result.RowsCount())
		}
	})

	t.Run("nil DataBlock passthrough", func(t *testing.T) {
		f := parseFilter(`service.name:="checkout"`)
		result := filterDataBlock(nil, f)
		if result != nil {
			t.Error("expected nil result for nil DataBlock")
		}
	})

	t.Run("empty DataBlock passthrough", func(t *testing.T) {
		empty := &logstorage.DataBlock{}
		f := parseFilter(`service.name:="checkout"`)
		result := filterDataBlock(empty, f)
		if result != empty {
			t.Error("expected same empty DataBlock back")
		}
	})

	t.Run("all rows match returns original", func(t *testing.T) {
		allMatch := &logstorage.DataBlock{}
		allMatch.SetColumns([]logstorage.BlockColumn{
			{Name: "service.name", Values: []string{"checkout", "checkout"}},
		})
		f := parseFilter(`service.name:="checkout"`)
		result := filterDataBlock(allMatch, f)
		if result != allMatch {
			t.Error("expected same DataBlock when all rows match")
		}
	})
}

func TestFilterMatchesRow(t *testing.T) {
	t.Run("nil filter matches all", func(t *testing.T) {
		fields := []logstorage.Field{{Name: "service.name", Value: "checkout"}}
		if !filterMatchesRow(nil, fields) {
			t.Error("nil filter should match all rows")
		}
	})

	t.Run("exact match", func(t *testing.T) {
		f := parseFilter(`service.name:="checkout"`)
		fields := []logstorage.Field{{Name: "service.name", Value: "checkout"}, {Name: "level", Value: "error"}}
		if !filterMatchesRow(f, fields) {
			t.Error("expected match")
		}
	})

	t.Run("exact no match", func(t *testing.T) {
		f := parseFilter(`service.name:="checkout"`)
		fields := []logstorage.Field{{Name: "service.name", Value: "payment"}}
		if filterMatchesRow(f, fields) {
			t.Error("expected no match")
		}
	})

	t.Run("OR match", func(t *testing.T) {
		f := parseFilter(`service.name:="checkout" OR service.name:="payment"`)
		fields := []logstorage.Field{{Name: "service.name", Value: "payment"}}
		if !filterMatchesRow(f, fields) {
			t.Error("expected OR to match second alternative")
		}
	})
}

func TestExtractTraceIDs(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "trace_id", Values: []string{"abc", "def", "abc", "", "ghi"}},
		{Name: "_msg", Values: []string{"m1", "m2", "m3", "m4", "m5"}},
	})

	var ids []string
	extractTraceIDs(db, &ids)

	if len(ids) != 3 {
		t.Errorf("got %d trace IDs, want 3; ids=%v", len(ids), ids)
	}

	seen := make(map[string]bool)
	for _, id := range ids {
		seen[id] = true
	}
	for _, want := range []string{"abc", "def", "ghi"} {
		if !seen[want] {
			t.Errorf("missing trace_id %q", want)
		}
	}
}

func TestExtractTraceIDs_NoColumn(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_msg", Values: []string{"m1"}},
	})

	var ids []string
	extractTraceIDs(db, &ids)

	if len(ids) != 0 {
		t.Errorf("expected 0 trace IDs when no trace_id column, got %d", len(ids))
	}
}
