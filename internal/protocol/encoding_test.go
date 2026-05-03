package protocol

import (
	"bytes"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

func TestMarshalUnmarshalDataBlock(t *testing.T) {
	tests := []struct {
		name string
		db   *storage.DataBlock
	}{
		{
			name: "empty",
			db:   &storage.DataBlock{RowsCount: 0, Columns: nil},
		},
		{
			name: "single column single row",
			db: &storage.DataBlock{
				RowsCount: 1,
				Columns: []storage.BlockColumn{
					{Name: "col1", Values: []string{"val1"}},
				},
			},
		},
		{
			name: "multiple columns",
			db: &storage.DataBlock{
				RowsCount: 3,
				Columns: []storage.BlockColumn{
					{Name: "_time", Values: []string{"1000", "2000", "3000"}},
					{Name: "_msg", Values: []string{"hello", "world", "test"}},
					{Name: "level", Values: []string{"info", "info", "info"}},
				},
			},
		},
		{
			name: "const column optimization",
			db: &storage.DataBlock{
				RowsCount: 5,
				Columns: []storage.BlockColumn{
					{Name: "service", Values: []string{"api", "api", "api", "api", "api"}},
				},
			},
		},
		{
			name: "empty string values",
			db: &storage.DataBlock{
				RowsCount: 2,
				Columns: []storage.BlockColumn{
					{Name: "col", Values: []string{"", ""}},
				},
			},
		},
		{
			name: "unicode values",
			db: &storage.DataBlock{
				RowsCount: 2,
				Columns: []storage.BlockColumn{
					{Name: "msg", Values: []string{"日本語", "Ünîcödé"}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := MarshalDataBlock(tt.db)
			got, err := UnmarshalDataBlock(data)
			if err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got.RowsCount != tt.db.RowsCount {
				t.Errorf("RowsCount = %d, want %d", got.RowsCount, tt.db.RowsCount)
			}
			if len(got.Columns) != len(tt.db.Columns) {
				t.Fatalf("len(Columns) = %d, want %d", len(got.Columns), len(tt.db.Columns))
			}
			for i, col := range got.Columns {
				if col.Name != tt.db.Columns[i].Name {
					t.Errorf("column %d name = %q, want %q", i, col.Name, tt.db.Columns[i].Name)
				}
				if len(col.Values) != len(tt.db.Columns[i].Values) {
					t.Errorf("column %d values len = %d, want %d", i, len(col.Values), len(tt.db.Columns[i].Values))
					continue
				}
				for j, v := range col.Values {
					if v != tt.db.Columns[i].Values[j] {
						t.Errorf("column %d row %d = %q, want %q", i, j, v, tt.db.Columns[i].Values[j])
					}
				}
			}
		})
	}
}

func TestMarshalUnmarshalValueWithHits(t *testing.T) {
	tests := []struct {
		name string
		vals []storage.ValueWithHits
	}{
		{"nil", nil},
		{"empty", []storage.ValueWithHits{}},
		{
			"single",
			[]storage.ValueWithHits{{Value: "service.name", Hits: 42}},
		},
		{
			"multiple",
			[]storage.ValueWithHits{
				{Value: "api-gw", Hits: 100},
				{Value: "web", Hits: 200},
				{Value: "db", Hits: 50},
			},
		},
		{
			"zero hits",
			[]storage.ValueWithHits{{Value: "empty", Hits: 0}},
		},
		{
			"large hits",
			[]storage.ValueWithHits{{Value: "big", Hits: 1<<63 - 1}},
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
		ids  []storage.TenantID
	}{
		{"nil", nil},
		{"empty", []storage.TenantID{}},
		{
			"single",
			[]storage.TenantID{{AccountID: 1, ProjectID: 2}},
		},
		{
			"multiple",
			[]storage.TenantID{
				{AccountID: 100, ProjectID: 200},
				{AccountID: 300, ProjectID: 400},
			},
		},
		{
			"max values",
			[]storage.TenantID{{AccountID: ^uint32(0), ProjectID: ^uint32(0)}},
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
	db := &storage.DataBlock{
		RowsCount: 2,
		Columns: []storage.BlockColumn{
			{Name: "_time", Values: []string{"1000", "2000"}},
			{Name: "_msg", Values: []string{"hello", "world"}},
		},
	}

	var buf bytes.Buffer
	if err := WriteDataBlockStream(&buf, db); err != nil {
		t.Fatal(err)
	}

	got, err := ReadDataBlockStream(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.RowsCount != 2 {
		t.Errorf("RowsCount = %d, want 2", got.RowsCount)
	}
	if len(got.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(got.Columns))
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
	db := &storage.DataBlock{
		RowsCount: 1000,
		Columns:   make([]storage.BlockColumn, 5),
	}
	for i := range db.Columns {
		db.Columns[i].Name = "col"
		vals := make([]string, 1000)
		for j := range vals {
			vals[j] = "value-data-here"
		}
		db.Columns[i].Values = vals
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MarshalDataBlock(db)
	}
}

func BenchmarkUnmarshalDataBlock(b *testing.B) {
	db := &storage.DataBlock{
		RowsCount: 1000,
		Columns: []storage.BlockColumn{
			{Name: "_time", Values: make([]string, 1000)},
			{Name: "_msg", Values: make([]string, 1000)},
		},
	}
	for i := range db.Columns[0].Values {
		db.Columns[0].Values[i] = "1234567890"
		db.Columns[1].Values[i] = "log message here"
	}
	data := MarshalDataBlock(db)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = UnmarshalDataBlock(data)
	}
}

func BenchmarkMarshalValueWithHits(b *testing.B) {
	vals := make([]storage.ValueWithHits, 100)
	for i := range vals {
		vals[i] = storage.ValueWithHits{Value: "service-name", Hits: 42}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MarshalValueWithHits(vals)
	}
}
