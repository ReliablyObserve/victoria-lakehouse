package protocol

import (
	"bytes"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func makeDataBlock(cols []logstorage.BlockColumn) *logstorage.DataBlock {
	db := &logstorage.DataBlock{}
	if cols != nil {
		db.SetColumns(cols)
	}
	return db
}

func TestMarshalUnmarshalDataBlock(t *testing.T) {
	tests := []struct {
		name string
		db   *logstorage.DataBlock
	}{
		{
			name: "empty",
			db:   makeDataBlock(nil),
		},
		{
			name: "single column single row",
			db: makeDataBlock([]logstorage.BlockColumn{
				{Name: "col1", Values: []string{"val1"}},
			}),
		},
		{
			name: "multiple columns",
			db: makeDataBlock([]logstorage.BlockColumn{
				{Name: "_time", Values: []string{"1000", "2000", "3000"}},
				{Name: "_msg", Values: []string{"hello", "world", "test"}},
				{Name: "level", Values: []string{"info", "info", "info"}},
			}),
		},
		{
			name: "const column optimization",
			db: makeDataBlock([]logstorage.BlockColumn{
				{Name: "service", Values: []string{"api", "api", "api", "api", "api"}},
			}),
		},
		{
			name: "empty string values",
			db: makeDataBlock([]logstorage.BlockColumn{
				{Name: "col", Values: []string{"", ""}},
			}),
		},
		{
			name: "unicode values",
			db: makeDataBlock([]logstorage.BlockColumn{
				{Name: "msg", Values: []string{"日本語", "Ünîcödé"}},
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := MarshalDataBlock(tt.db)
			got, err := UnmarshalDataBlock(data)
			if err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got.RowsCount() != tt.db.RowsCount() {
				t.Errorf("RowsCount = %d, want %d", got.RowsCount(), tt.db.RowsCount())
			}
			gotCols := got.GetColumns(false)
			wantCols := tt.db.GetColumns(false)
			if len(gotCols) != len(wantCols) {
				t.Fatalf("len(Columns) = %d, want %d", len(gotCols), len(wantCols))
			}
			for i, col := range gotCols {
				if col.Name != wantCols[i].Name {
					t.Errorf("column %d name = %q, want %q", i, col.Name, wantCols[i].Name)
				}
				if len(col.Values) != len(wantCols[i].Values) {
					t.Errorf("column %d values len = %d, want %d", i, len(col.Values), len(wantCols[i].Values))
					continue
				}
				for j, v := range col.Values {
					if v != wantCols[i].Values[j] {
						t.Errorf("column %d row %d = %q, want %q", i, j, v, wantCols[i].Values[j])
					}
				}
			}
		})
	}
}

func TestMarshalUnmarshalValueWithHits(t *testing.T) {
	tests := []struct {
		name string
		vals []logstorage.ValueWithHits
	}{
		{"nil", nil},
		{"empty", []logstorage.ValueWithHits{}},
		{
			"single",
			[]logstorage.ValueWithHits{{Value: "service.name", Hits: 42}},
		},
		{
			"multiple",
			[]logstorage.ValueWithHits{
				{Value: "api-gw", Hits: 100},
				{Value: "web", Hits: 200},
				{Value: "db", Hits: 50},
			},
		},
		{
			"zero hits",
			[]logstorage.ValueWithHits{{Value: "empty", Hits: 0}},
		},
		{
			"large hits",
			[]logstorage.ValueWithHits{{Value: "big", Hits: 1<<63 - 1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := MarshalValueWithHits(tt.vals)
			got, err := UnmarshalValueWithHits(data)
			if err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			wantLen := len(tt.vals)
			if tt.vals == nil {
				wantLen = 0
			}
			if len(got) != wantLen {
				t.Fatalf("len = %d, want %d", len(got), wantLen)
			}
			for i, v := range got {
				if v.Value != tt.vals[i].Value {
					t.Errorf("[%d] value = %q, want %q", i, v.Value, tt.vals[i].Value)
				}
				if v.Hits != tt.vals[i].Hits {
					t.Errorf("[%d] hits = %d, want %d", i, v.Hits, tt.vals[i].Hits)
				}
			}
		})
	}
}

func TestMarshalUnmarshalTenantIDs(t *testing.T) {
	tests := []struct {
		name string
		ids  []logstorage.TenantID
	}{
		{"nil", nil},
		{"empty", []logstorage.TenantID{}},
		{
			"single",
			[]logstorage.TenantID{{AccountID: 1, ProjectID: 2}},
		},
		{
			"multiple",
			[]logstorage.TenantID{
				{AccountID: 100, ProjectID: 200},
				{AccountID: 300, ProjectID: 400},
			},
		},
		{
			"max values",
			[]logstorage.TenantID{{AccountID: ^uint32(0), ProjectID: ^uint32(0)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := MarshalTenantIDs(tt.ids)
			got, err := UnmarshalTenantIDs(data)
			if err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			wantLen := len(tt.ids)
			if tt.ids == nil {
				wantLen = 0
			}
			if len(got) != wantLen {
				t.Fatalf("len = %d, want %d", len(got), wantLen)
			}
			for i, id := range got {
				if id != tt.ids[i] {
					t.Errorf("[%d] = %+v, want %+v", i, id, tt.ids[i])
				}
			}
		})
	}
}

func TestDataBlockStreamRoundTrip(t *testing.T) {
	db := makeDataBlock([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{"1000", "2000"}},
		{Name: "_msg", Values: []string{"hello", "world"}},
	})

	var buf bytes.Buffer
	if err := WriteDataBlockStream(&buf, db); err != nil {
		t.Fatal(err)
	}

	got, err := ReadDataBlockStream(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.RowsCount() != 2 {
		t.Errorf("RowsCount = %d, want 2", got.RowsCount())
	}
	gotCols := got.GetColumns(false)
	if len(gotCols) != 2 {
		t.Fatalf("columns = %d, want 2", len(gotCols))
	}
}

func TestUnmarshalDataBlock_Errors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"too short", []byte{0, 0, 0, 1}},
		{"truncated column name", []byte{0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 10}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnmarshalDataBlock(tt.data)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestUnmarshalValueWithHits_Errors(t *testing.T) {
	_, err := UnmarshalValueWithHits([]byte{})
	if err == nil {
		t.Error("expected error for empty data")
	}
}

func TestUnmarshalTenantIDs_Errors(t *testing.T) {
	_, err := UnmarshalTenantIDs([]byte{})
	if err == nil {
		t.Error("expected error for empty data")
	}

	_, err = UnmarshalTenantIDs([]byte{0, 0, 0, 2, 0, 0, 0, 1})
	if err == nil {
		t.Error("expected error for truncated data")
	}
}

func TestAllSame(t *testing.T) {
	tests := []struct {
		vals []string
		want bool
	}{
		{nil, true},
		{[]string{}, true},
		{[]string{"a"}, true},
		{[]string{"a", "a", "a"}, true},
		{[]string{"a", "b"}, false},
		{[]string{"a", "a", "b"}, false},
	}
	for _, tt := range tests {
		if got := allSame(tt.vals); got != tt.want {
			t.Errorf("allSame(%v) = %v, want %v", tt.vals, got, tt.want)
		}
	}
}

func BenchmarkMarshalDataBlock(b *testing.B) {
	cols := make([]logstorage.BlockColumn, 5)
	for i := range cols {
		cols[i].Name = "col"
		vals := make([]string, 1000)
		for j := range vals {
			vals[j] = "value-data-here"
		}
		cols[i].Values = vals
	}
	db := makeDataBlock(cols)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MarshalDataBlock(db)
	}
}

func BenchmarkUnmarshalDataBlock(b *testing.B) {
	cols := []logstorage.BlockColumn{
		{Name: "_time", Values: make([]string, 1000)},
		{Name: "_msg", Values: make([]string, 1000)},
	}
	for i := range cols[0].Values {
		cols[0].Values[i] = "1234567890"
		cols[1].Values[i] = "log message here"
	}
	db := makeDataBlock(cols)
	data := MarshalDataBlock(db)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = UnmarshalDataBlock(data)
	}
}

func BenchmarkMarshalValueWithHits(b *testing.B) {
	vals := make([]logstorage.ValueWithHits, 100)
	for i := range vals {
		vals[i] = logstorage.ValueWithHits{Value: "service-name", Hits: 42}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MarshalValueWithHits(vals)
	}
}
