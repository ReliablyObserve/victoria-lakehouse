package manifest

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Review regression (#420, #404 round 4, F7): complete listings, partial
// listings, publishes and the CompleteSince/LastCompleteRefresh readers run
// concurrently. Invariants: the generation never goes backwards, and after a
// final complete listing every key is tracked exactly once. Before the fix the
// refresh read the file map after releasing the lock while AddFile wrote it: a
// concurrent map iteration and write, which the race detector reports and the
// Go runtime can turn into a fatal error. Run with -race -count=20.
func TestReview420_ConcurrentRefreshesPublishesAndReaders(t *testing.T) {
	part := refreshPartition
	key := func(acct, i int) string { return fmt.Sprintf("%d/0/logs/%s/f%05d.parquet", acct, part, i) }
	m := New("b", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	var mu sync.Mutex
	bucket := map[string]bool{}
	for i := 0; i < 40; i++ {
		k := key(1+i%2, i)
		bucket[k] = true
		m.AddFile(part, enriched(k, 1))
	}
	listing := func(skipAcct2 bool) []ListedObject {
		mu.Lock()
		defer mu.Unlock()
		var out []ListedObject
		for k := range bucket {
			if skipAcct2 && k[0] == '2' {
				continue
			}
			out = append(out, ListedObject{Key: k, Size: 100})
		}
		return out
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	next := atomic.Int64{}
	next.Store(1000)
	wg.Add(1)
	go func() { // publisher
		defer wg.Done()
		for n := 0; n < 300 && !stop.Load(); n++ {
			i := int(next.Add(1))
			k := key(1+i%2, i)
			mu.Lock()
			bucket[k] = true
			mu.Unlock()
			m.AddFile(part, enriched(k, 1))
		}
	}()
	wg.Add(1)
	go func() { // reader
		defer wg.Done()
		var last uint64
		for !stop.Load() {
			g := m.LastCompleteRefresh().Generation
			if g < last {
				t.Errorf("generation went backwards: %d -> %d", last, g)
				return
			}
			last = g
			_ = m.CompleteSince(time.Now().Add(-time.Second))
			_ = m.Listed()
		}
	}()
	for r := 0; r < 60; r++ {
		start := time.Now()
		objs := listing(r%3 == 1)
		if r%3 == 1 {
			m.ApplyPartialListing(objs, start, []string{"2/"})
		} else {
			m.ApplyListing(objs, start)
		}
	}
	stop.Store(true)
	wg.Wait()
	if !m.ApplyListing(listing(false), time.Now()) {
		t.Fatal("final complete listing rejected")
	}
	seen := map[string]int{}
	for _, fi := range m.AllFiles()[part] {
		seen[fi.Key]++
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("key %s tracked %d times after a final complete listing", k, n)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for k := range bucket {
		if seen[k] != 1 {
			t.Fatalf("live key %s missing after a final complete listing", k)
		}
	}
}
