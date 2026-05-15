package cache

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestLRU_Race_MaxGoroutines(t *testing.T) {
	c := NewLRU(4096)
	const goroutines = 500
	const ops = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("k-%d", rng.Intn(50))
				switch rng.Intn(6) {
				case 0:
					c.Put(key, make([]byte, rng.Intn(100)+1))
				case 1:
					c.Get(key)
				case 2:
					c.Delete(key)
				case 3:
					c.Clear()
				case 4:
					c.Stats()
				case 5:
					_ = c.Size()
					_ = c.Len()
					_ = c.MaxSize()
				}
				if i%100 == 0 {
					runtime.Gosched()
				}
			}
		}(g)
	}
	wg.Wait()

	if c.Size() < 0 {
		t.Errorf("negative size: %d", c.Size())
	}
}

func TestGroup_Race_MaxGoroutines(t *testing.T) {
	g := NewGroup()
	const goroutines = 500
	const keys = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	var totalCalls atomic.Int64

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id%keys)
			val, err, shared := g.Do(key, func() ([]byte, error) {
				totalCalls.Add(1)
				return []byte("result"), nil
			})
			if err != nil {
				t.Errorf("Do error: %v", err)
			}
			if string(val) != "result" {
				t.Errorf("unexpected val: %q", val)
			}
			_ = shared
		}(i)
	}
	wg.Wait()

	calls := totalCalls.Load()
	if calls < int64(keys) {
		t.Logf("singleflight coalesced: %d calls for %d goroutines on %d keys", calls, goroutines, keys)
	}
	if g.Inflight() != 0 {
		t.Errorf("inflight = %d after all done", g.Inflight())
	}
}

func TestLabelIndex_Race_MaxGoroutines(t *testing.T) {
	idx := NewLabelIndex()
	const goroutines = 200
	const ops = 500
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < ops; i++ {
				field := fmt.Sprintf("f-%d", rng.Intn(30))
				switch rng.Intn(4) {
				case 0:
					idx.Add(field, []string{fmt.Sprintf("v-%d-%d", id, i)})
				case 1:
					idx.GetFieldNames()
				case 2:
					idx.GetFieldValues(field, 10)
				case 3:
					idx.Len()
				}
				if i%50 == 0 {
					runtime.Gosched()
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestLRU_Race_SizeInvariant(t *testing.T) {
	c := NewLRU(2048)
	const goroutines = 100
	const ops = 2000
	var violations atomic.Int64
	var wg sync.WaitGroup
	wg.Add(goroutines + 1)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("k-%d-%d", id, i%20)
				c.Put(key, make([]byte, 50))
				if i%3 == 0 {
					c.Delete(key)
				}
			}
		}(g)
	}

	go func() {
		defer wg.Done()
		for i := 0; i < ops*10; i++ {
			if c.Size() < 0 {
				violations.Add(1)
			}
			runtime.Gosched()
		}
	}()

	wg.Wait()
	if v := violations.Load(); v > 0 {
		t.Errorf("%d negative-size violations detected", v)
	}
}
