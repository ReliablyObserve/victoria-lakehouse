package cache

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func TestLRU_ConcurrentReadWrite(t *testing.T) {
	c := NewLRU(1024 * 1024)
	const goroutines = 50
	const ops = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("key-%d-%d", id, i%10)
				val := []byte(fmt.Sprintf("val-%d-%d", id, i))

				c.Put(key, val)
				c.Get(key)
				if i%5 == 0 {
					c.Delete(key)
				}
				if i%50 == 0 {
					c.Stats()
				}
			}
		}(g)
	}
	wg.Wait()

	if c.Size() < 0 {
		t.Errorf("negative size after concurrent ops: %d", c.Size())
	}
}

func TestLRU_ConcurrentEviction(t *testing.T) {
	c := NewLRU(100)
	const goroutines = 20
	const ops = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("k-%d-%d", id, i)
				c.Put(key, make([]byte, 10))
			}
		}(g)
	}
	wg.Wait()

	if c.Size() > c.MaxSize() {
		t.Errorf("size %d exceeds max %d after concurrent evictions", c.Size(), c.MaxSize())
	}
}

func TestDiskCache_ConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 20
	const ops = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("key-%d-%d", id, i%5)
				data := []byte(fmt.Sprintf("data-%d-%d", id, i))

				if _, err := dc.Put(key, data); err != nil {
					t.Errorf("Put(%q): %v", key, err)
					return
				}
				dc.Get(key)
				if i%10 == 0 {
					dc.Delete(key)
				}
			}
		}(g)
	}
	wg.Wait()

	if dc.Size() < 0 {
		t.Errorf("negative disk cache size: %d", dc.Size())
	}
}

func TestGroup_ConcurrentSameKey(t *testing.T) {
	g := NewGroup()
	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	var callCount atomic.Int32
	expected := []byte("result-data")

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			val, err, shared := g.Do("same-key", func() ([]byte, error) {
				callCount.Add(1)
				return expected, nil
			})
			if err != nil {
				t.Errorf("Do: %v", err)
				return
			}
			if string(val) != string(expected) {
				t.Errorf("Do result = %q, want %q", val, expected)
			}
			_ = shared
		}()
	}
	wg.Wait()

	calls := callCount.Load()
	if calls < 1 {
		t.Errorf("expected at least 1 call, got %d", calls)
	}
	if calls > goroutines {
		t.Errorf("more calls (%d) than goroutines (%d)", calls, goroutines)
	}
}

func TestGroup_ConcurrentDifferentKeys(t *testing.T) {
	g := NewGroup()
	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			expected := []byte(fmt.Sprintf("result-%d", id))
			val, err, _ := g.Do(key, func() ([]byte, error) {
				return expected, nil
			})
			if err != nil {
				t.Errorf("Do(%q): %v", key, err)
				return
			}
			if string(val) != string(expected) {
				t.Errorf("Do(%q) = %q, want %q", key, val, expected)
			}
		}(i)
	}
	wg.Wait()

	if g.Inflight() != 0 {
		t.Errorf("inflight = %d, want 0 after all done", g.Inflight())
	}
}

func TestLabelIndex_ConcurrentAddRead(t *testing.T) {
	idx := NewLabelIndex()
	const goroutines = 30
	const ops = 200
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				name := fmt.Sprintf("field-%d", i%20)
				val := fmt.Sprintf("val-%d-%d", id, i)
				idx.Add(name, []string{val})
			}
		}(g)

		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				idx.GetFieldNames()
				idx.GetFieldValues(fmt.Sprintf("field-%d", i%20), 10)
				idx.Len()
			}
		}()
	}
	wg.Wait()

	if idx.Len() <= 0 {
		t.Error("expected non-zero label count after concurrent adds")
	}
}

func TestDiskCache_ConcurrentPutFromPath(t *testing.T) {
	dir := t.TempDir()
	dc, err := NewDiskCache(dir, 1024*1024, 0.8)
	if err != nil {
		t.Fatal(err)
	}

	srcDir := t.TempDir()
	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			srcFile := filepath.Join(srcDir, fmt.Sprintf("src-%d.dat", id))
			data := []byte(fmt.Sprintf("content-%d", id))
			if err := os.WriteFile(srcFile, data, 0o600); err != nil {
				t.Errorf("write src: %v", err)
				return
			}
			key := fmt.Sprintf("from-path-%d", id)
			if err := dc.PutFromPath(key, srcFile); err != nil {
				t.Errorf("PutFromPath(%q): %v", key, err)
			}
		}(g)
	}
	wg.Wait()
}

func TestPersister_ConcurrentSaveLoad(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			idx := NewLabelIndex()
			idx.Add(fmt.Sprintf("field-%d", id), []string{"val"})
			_ = p.SaveLabelIndex(idx)
		}(g)

		go func() {
			defer wg.Done()
			_, _ = p.LoadLabelIndex()
		}()
	}
	wg.Wait()
}

func TestLRU_RandomOps(t *testing.T) {
	c := NewLRU(500)
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("k%d", rng.Intn(50))
		switch rng.Intn(4) {
		case 0:
			c.Put(key, make([]byte, rng.Intn(100)))
		case 1:
			c.Get(key)
		case 2:
			c.Delete(key)
		case 3:
			c.Clear()
		}

		if c.Size() < 0 {
			t.Fatalf("negative size %d at op %d", c.Size(), i)
		}
		if c.Size() > c.MaxSize() && c.Len() > 0 {
			t.Fatalf("size %d exceeds max %d at op %d", c.Size(), c.MaxSize(), i)
		}
	}
}
