package cache

import (
	"fmt"
	"testing"
)

func BenchmarkLRU_Put(b *testing.B) {
	c := NewLRU(100 * 1024 * 1024)
	val := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Put(fmt.Sprintf("key-%d", i%10000), val)
	}
}

func BenchmarkLRU_Get_Hit(b *testing.B) {
	c := NewLRU(100 * 1024 * 1024)
	for i := 0; i < 1000; i++ {
		c.Put(fmt.Sprintf("key-%d", i), make([]byte, 1024))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(fmt.Sprintf("key-%d", i%1000))
	}
}

func BenchmarkLRU_Get_Miss(b *testing.B) {
	c := NewLRU(100 * 1024 * 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(fmt.Sprintf("miss-%d", i))
	}
}

func BenchmarkLRU_PutWithEviction(b *testing.B) {
	c := NewLRU(1024)
	val := make([]byte, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Put(fmt.Sprintf("key-%d", i), val)
	}
}

func BenchmarkGroup_Do_NoContention(b *testing.B) {
	g := NewGroup()
	data := []byte("result")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = g.Do(fmt.Sprintf("key-%d", i), func() ([]byte, error) {
			return data, nil
		})
	}
}

func BenchmarkLabelIndex_Add(b *testing.B) {
	idx := NewLabelIndex()
	values := []string{"val1", "val2", "val3"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Add(fmt.Sprintf("field-%d", i%100), values)
	}
}

func BenchmarkLabelIndex_GetFieldNames(b *testing.B) {
	idx := NewLabelIndex()
	for i := 0; i < 200; i++ {
		idx.Add(fmt.Sprintf("field-%d", i), []string{"val"})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.GetFieldNames()
	}
}

func BenchmarkLabelIndex_GetFieldValues(b *testing.B) {
	idx := NewLabelIndex()
	vals := make([]string, 100)
	for i := range vals {
		vals[i] = fmt.Sprintf("val-%d", i)
	}
	idx.Add("field", vals)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.GetFieldValues("field", 10)
	}
}
