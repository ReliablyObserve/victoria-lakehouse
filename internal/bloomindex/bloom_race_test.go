package bloomindex

import (
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentBloomBuilds_Singleflight(t *testing.T) {
	idx := New()

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				f := NewFilter(50, 0.01)
				f.Add(fmt.Sprintf("trace-%d-%d", id, i))
				idx.Add(fmt.Sprintf("file-%d-%d", id, i), "trace_id", f)
			}
		}(g)
	}
	wg.Wait()

	if idx.Len() != 1000 {
		t.Errorf("expected 1000 entries, got %d", idx.Len())
	}
}

func TestConcurrentCacheAccess(t *testing.T) {
	idx := New()
	for i := 0; i < 100; i++ {
		f := NewFilter(50, 0.01)
		f.Add(fmt.Sprintf("trace-%d", i))
		idx.Add(fmt.Sprintf("file%d", i), "trace_id", f)
	}

	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("file%d", i)
	}

	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				idx.MayContain(keys, "trace_id", fmt.Sprintf("trace-%d", i))
				idx.MayContainAll(keys, []ColumnCheck{{"trace_id", fmt.Sprintf("trace-%d", i)}})
				idx.Has(fmt.Sprintf("file%d", i%100))
				idx.Len()
			}
		}(g)
	}

	// Concurrent writes while reading
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				f := NewFilter(10, 0.01)
				f.Add(fmt.Sprintf("new-%d-%d", id, i))
				idx.Add(fmt.Sprintf("newfile-%d-%d", id, i), "trace_id", f)
			}
		}(g)
	}
	wg.Wait()
}

func TestConcurrentMergeFrom(t *testing.T) {
	idx1 := New()
	idx2 := New()

	for i := 0; i < 100; i++ {
		f1 := NewFilter(10, 0.01)
		f1.Add(fmt.Sprintf("a-%d", i))
		idx1.Add(fmt.Sprintf("file-a-%d", i), "trace_id", f1)

		f2 := NewFilter(10, 0.01)
		f2.Add(fmt.Sprintf("b-%d", i))
		idx2.Add(fmt.Sprintf("file-b-%d", i), "trace_id", f2)
	}

	var wg sync.WaitGroup
	// Multiple goroutines reading idx1 while one merges
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				idx1.Len()
				idx1.Has(fmt.Sprintf("file-a-%d", i%100))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		idx1.MergeFrom(idx2)
	}()
	wg.Wait()

	if idx1.Len() < 100 {
		t.Errorf("after concurrent merge: expected ≥100 entries, got %d", idx1.Len())
	}
}
