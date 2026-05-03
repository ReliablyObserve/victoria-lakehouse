package protocol

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

func forceGC() {
	runtime.GC()
	runtime.GC()
}

func heapInUse() uint64 {
	var m runtime.MemStats
	forceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

func TestMarshalDataBlock_MemLeak(t *testing.T) {
	db := &storage.DataBlock{
		RowsCount: 100,
		Columns: []storage.BlockColumn{
			{Name: "_time", Values: make([]string, 100)},
			{Name: "_msg", Values: make([]string, 100)},
		},
	}
	for i := range db.Columns[0].Values {
		db.Columns[0].Values[i] = fmt.Sprintf("%d", i)
		db.Columns[1].Values[i] = "log message here"
	}

	for i := 0; i < 1000; i++ {
		_ = MarshalDataBlock(db)
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 100_000; i++ {
		_ = MarshalDataBlock(db)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 100K MarshalDataBlock cycles (max allowed %d)", growth, maxGrowth)
	}
}

func TestUnmarshalDataBlock_MemLeak(t *testing.T) {
	db := &storage.DataBlock{
		RowsCount: 100,
		Columns: []storage.BlockColumn{
			{Name: "_time", Values: make([]string, 100)},
			{Name: "_msg", Values: make([]string, 100)},
		},
	}
	for i := range db.Columns[0].Values {
		db.Columns[0].Values[i] = fmt.Sprintf("%d", i)
		db.Columns[1].Values[i] = "log message here"
	}
	data := MarshalDataBlock(db)

	for i := 0; i < 1000; i++ {
		_, _ = UnmarshalDataBlock(data)
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 100_000; i++ {
		_, _ = UnmarshalDataBlock(data)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 100K UnmarshalDataBlock cycles (max allowed %d)", growth, maxGrowth)
	}
}

func TestStreamRoundTrip_MemLeak(t *testing.T) {
	db := &storage.DataBlock{
		RowsCount: 50,
		Columns: []storage.BlockColumn{
			{Name: "col", Values: make([]string, 50)},
		},
	}
	for i := range db.Columns[0].Values {
		db.Columns[0].Values[i] = "value"
	}

	for i := 0; i < 1000; i++ {
		var buf bytes.Buffer
		_ = WriteDataBlockStream(&buf, db)
		_, _ = ReadDataBlockStream(&buf)
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 50_000; i++ {
		var buf bytes.Buffer
		_ = WriteDataBlockStream(&buf, db)
		_, _ = ReadDataBlockStream(&buf)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 50K stream round-trip cycles (max allowed %d)", growth, maxGrowth)
	}
}

func TestValueWithHits_MemLeak(t *testing.T) {
	vals := make([]storage.ValueWithHits, 100)
	for i := range vals {
		vals[i] = storage.ValueWithHits{Value: fmt.Sprintf("v-%d", i), Hits: uint64(i)}
	}

	for i := 0; i < 1000; i++ {
		data := MarshalValueWithHits(vals)
		_, _ = UnmarshalValueWithHits(data)
	}
	forceGC()

	before := heapInUse()

	for i := 0; i < 100_000; i++ {
		data := MarshalValueWithHits(vals)
		_, _ = UnmarshalValueWithHits(data)
	}
	forceGC()

	after := heapInUse()
	growth := int64(after) - int64(before)
	maxGrowth := int64(10 * 1024 * 1024)
	if growth > maxGrowth {
		t.Errorf("heap grew %d bytes after 100K ValueWithHits round-trip cycles (max allowed %d)", growth, maxGrowth)
	}
}
