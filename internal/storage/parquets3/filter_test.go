package parquets3

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"
)

func TestParseFilterPredicates(t *testing.T) {
	tests := []struct {
		query string
		want  int
	}{
		{"*", 0},
		{"", 0},
		{`service.name:="checkout"`, 1},
		{`service.name:="checkout" level:="error"`, 2},
		{`service.name:"checkout"`, 1},
		{`service.name:~"check.*"`, 1},
		{`service.name:="checkout" | limit 10`, 1},
		{`NOT service.name:="checkout"`, 1},
		{`_time:[2024-01-01, 2024-01-02) service.name:="web"`, 1},
		{`error`, 1},
		{`service.name:="checkout" AND level:="error"`, 2},
	}

	for _, tt := range tests {
		preds := parseFilterPredicates(tt.query)
		if len(preds) != tt.want {
			t.Errorf("parseFilterPredicates(%q) = %d predicates, want %d; got %+v", tt.query, len(preds), tt.want, preds)
		}
	}
}

func TestParseFieldPredicate(t *testing.T) {
	tests := []struct {
		tok     string
		wantOp  filterOp
		wantOK  bool
		wantVal string
	}{
		{`service.name:="checkout"`, filterExact, true, "checkout"},
		{`service.name:"checkout"`, filterSubstring, true, "checkout"},
		{`service.name:~"check.*"`, filterRegex, true, "check.*"},
		{`service.name:checkout`, filterSubstring, true, "checkout"},
		{`_time:[2024,2025)`, filterOp(0), false, ""},
		{`*`, filterOp(0), false, ""},
		{`error`, filterSubstring, true, "error"},
	}

	for _, tt := range tests {
		p, ok := parseFieldPredicate(tt.tok, false)
		if ok != tt.wantOK {
			t.Errorf("parseFieldPredicate(%q): ok = %v, want %v", tt.tok, ok, tt.wantOK)
			continue
		}
		if ok && p.op != tt.wantOp {
			t.Errorf("parseFieldPredicate(%q): op = %v, want %v", tt.tok, p.op, tt.wantOp)
		}
		if ok && p.value != tt.wantVal {
			t.Errorf("parseFieldPredicate(%q): value = %q, want %q", tt.tok, p.value, tt.wantVal)
		}
	}
}

func TestParseFieldPredicate_RegexInvalid(t *testing.T) {
	_, ok := parseFieldPredicate(`field:~"[invalid"`, false)
	if ok {
		t.Error("expected invalid regex to return ok=false")
	}
}

func TestFilterPredicateNegation(t *testing.T) {
	preds := parseFilterPredicates(`NOT service.name:="checkout"`)
	if len(preds) != 1 {
		t.Fatalf("expected 1 predicate, got %d", len(preds))
	}
	if !preds[0].negated {
		t.Error("expected negated predicate")
	}
}

func TestExtractQuoted_NoClosingQuote(t *testing.T) {
	got := extractQuoted("no-closing-quote")
	if got != "no-closing-quote" {
		t.Errorf("extractQuoted with no closing quote = %q, want %q", got, "no-closing-quote")
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
		preds := parseFilterPredicates(`service.name:="checkout"`)
		result := filterDataBlock(db, preds)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.RowsCount() != 2 {
			t.Errorf("got %d rows, want 2", result.RowsCount())
		}
	})

	t.Run("substring match", func(t *testing.T) {
		preds := parseFilterPredicates(`service.name:"check"`)
		result := filterDataBlock(db, preds)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.RowsCount() != 2 {
			t.Errorf("got %d rows, want 2", result.RowsCount())
		}
	})

	t.Run("combined filters", func(t *testing.T) {
		preds := parseFilterPredicates(`service.name:="checkout" level:="error"`)
		result := filterDataBlock(db, preds)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.RowsCount() != 1 {
			t.Errorf("got %d rows, want 1", result.RowsCount())
		}
	})

	t.Run("no match", func(t *testing.T) {
		preds := parseFilterPredicates(`service.name:="nonexistent"`)
		result := filterDataBlock(db, preds)
		if result != nil {
			t.Errorf("expected nil result, got %d rows", result.RowsCount())
		}
	})

	t.Run("negated filter", func(t *testing.T) {
		preds := parseFilterPredicates(`NOT service.name:="checkout"`)
		result := filterDataBlock(db, preds)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.RowsCount() != 2 {
			t.Errorf("got %d rows, want 2", result.RowsCount())
		}
	})

	t.Run("nil predicates passthrough", func(t *testing.T) {
		result := filterDataBlock(db, nil)
		if result != db {
			t.Error("expected same DataBlock back with nil predicates")
		}
	})

	t.Run("bare word matches _msg", func(t *testing.T) {
		preds := parseFilterPredicates(`denied`)
		result := filterDataBlock(db, preds)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.RowsCount() != 1 {
			t.Errorf("got %d rows, want 1", result.RowsCount())
		}
	})

	t.Run("regex match", func(t *testing.T) {
		preds := parseFilterPredicates(`service.name:~"check.*"`)
		result := filterDataBlock(db, preds)
		if result == nil {
			t.Fatal("expected non-nil result")
		}
		if result.RowsCount() != 2 {
			t.Errorf("got %d rows, want 2", result.RowsCount())
		}
	})

	t.Run("missing field returns nil", func(t *testing.T) {
		preds := []filterPredicate{{field: "nonexistent", op: filterExact, value: "x"}}
		result := filterDataBlock(db, preds)
		if result != nil {
			t.Errorf("expected nil result for missing field, got %d rows", result.RowsCount())
		}
	})

	t.Run("nil DataBlock passthrough", func(t *testing.T) {
		preds := parseFilterPredicates(`service.name:="checkout"`)
		result := filterDataBlock(nil, preds)
		if result != nil {
			t.Error("expected nil result for nil DataBlock")
		}
	})

	t.Run("empty DataBlock passthrough", func(t *testing.T) {
		empty := &logstorage.DataBlock{}
		preds := parseFilterPredicates(`service.name:="checkout"`)
		result := filterDataBlock(empty, preds)
		if result != empty {
			t.Error("expected same empty DataBlock back")
		}
	})

	t.Run("all rows match returns original", func(t *testing.T) {
		allMatch := &logstorage.DataBlock{}
		allMatch.SetColumns([]logstorage.BlockColumn{
			{Name: "service.name", Values: []string{"checkout", "checkout"}},
		})
		preds := parseFilterPredicates(`service.name:="checkout"`)
		result := filterDataBlock(allMatch, preds)
		if result != allMatch {
			t.Error("expected same DataBlock when all rows match")
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

func TestRowMatchesPredicates(t *testing.T) {
	colNames := []string{"service.name", "level", "_msg"}
	colMap := map[string]int{
		"service.name": 0,
		"level":        1,
		"_msg":         2,
	}

	mkRow := func(svc, level, msg string) parquet.Row {
		return parquet.Row{
			parquet.ValueOf(svc),
			parquet.ValueOf(level),
			parquet.ValueOf(msg),
		}
	}

	t.Run("exact match", func(t *testing.T) {
		preds := parseFilterPredicates(`service.name:="checkout"`)
		if !rowMatchesPredicates(mkRow("checkout", "error", "fail"), colNames, colMap, preds, nil) {
			t.Error("expected match")
		}
		if rowMatchesPredicates(mkRow("payment", "error", "fail"), colNames, colMap, preds, nil) {
			t.Error("expected no match")
		}
	})

	t.Run("combined predicates", func(t *testing.T) {
		preds := parseFilterPredicates(`service.name:="checkout" level:="error"`)
		if !rowMatchesPredicates(mkRow("checkout", "error", "fail"), colNames, colMap, preds, nil) {
			t.Error("expected match for checkout+error")
		}
		if rowMatchesPredicates(mkRow("checkout", "info", "ok"), colNames, colMap, preds, nil) {
			t.Error("expected no match for checkout+info")
		}
	})

	t.Run("negated predicate", func(t *testing.T) {
		preds := parseFilterPredicates(`NOT service.name:="checkout"`)
		if rowMatchesPredicates(mkRow("checkout", "error", "fail"), colNames, colMap, preds, nil) {
			t.Error("expected no match for negated checkout")
		}
		if !rowMatchesPredicates(mkRow("payment", "error", "fail"), colNames, colMap, preds, nil) {
			t.Error("expected match for payment with NOT checkout")
		}
	})

	t.Run("missing field", func(t *testing.T) {
		preds := []filterPredicate{{field: "nonexistent", op: filterExact, value: "x"}}
		if rowMatchesPredicates(mkRow("checkout", "error", "fail"), colNames, colMap, preds, nil) {
			t.Error("expected no match for missing field")
		}
	})

	t.Run("missing field negated", func(t *testing.T) {
		preds := []filterPredicate{{field: "nonexistent", op: filterExact, value: "x", negated: true}}
		if !rowMatchesPredicates(mkRow("checkout", "error", "fail"), colNames, colMap, preds, nil) {
			t.Error("expected match for negated missing field")
		}
	})

	t.Run("no predicates matches all", func(t *testing.T) {
		if !rowMatchesPredicates(mkRow("anything", "any", "any"), colNames, colMap, nil, nil) {
			t.Error("expected match with no predicates")
		}
	})
}

func TestCollectFilteredValues(t *testing.T) {
	mkRow := func(svc, level string) parquet.Row {
		return parquet.Row{
			parquet.ValueOf(svc),
			parquet.ValueOf(level),
		}
	}

	colNames := []string{"service.name", "level"}
	rows := []parquet.Row{
		mkRow("checkout", "error"),
		mkRow("payment", "info"),
		mkRow("checkout", "warn"),
		mkRow("auth", "error"),
	}

	t.Run("no predicates collects all", func(t *testing.T) {
		seen := make(map[string]uint64)
		collectFilteredValues(rows, colNames, 1, nil, nil, seen)
		if len(seen) != 3 {
			t.Errorf("expected 3 unique level values, got %d: %v", len(seen), seen)
		}
	})

	t.Run("with filter collects matching only", func(t *testing.T) {
		seen := make(map[string]uint64)
		preds := parseFilterPredicates(`service.name:="checkout"`)
		collectFilteredValues(rows, colNames, 1, preds, nil, seen)
		if len(seen) != 2 {
			t.Errorf("expected 2 level values for checkout, got %d: %v", len(seen), seen)
		}
		if _, ok := seen["error"]; !ok {
			t.Error("expected 'error' in results")
		}
		if _, ok := seen["warn"]; !ok {
			t.Error("expected 'warn' in results")
		}
	})

	t.Run("filter excludes all", func(t *testing.T) {
		seen := make(map[string]uint64)
		preds := parseFilterPredicates(`service.name:="nonexistent"`)
		collectFilteredValues(rows, colNames, 1, preds, nil, seen)
		if len(seen) != 0 {
			t.Errorf("expected 0 values, got %d: %v", len(seen), seen)
		}
	})
}
