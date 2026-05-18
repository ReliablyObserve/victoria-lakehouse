package bloomindex

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestBloomCache_GetAndPut(t *testing.T) {
	c := NewBloomCache(1024*1024, nil)

	idx := New()
	idx.Add("f1", "trace_id", filterWith("trace-aaa"))

	c.Put("p1", idx)

	got, err := c.Get(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("cached index should not be nil")
	}
	if got.Len() != 1 {
		t.Errorf("want 1 entry, got %d", got.Len())
	}
}

func TestBloomCache_GetWithLoader(t *testing.T) {
	loadCount := 0
	loader := func(ctx context.Context, partition string) (*Index, error) {
		loadCount++
		idx := New()
		idx.Add("f1", "trace_id", filterWith("loaded-"+partition))
		return idx, nil
	}

	c := NewBloomCache(1024*1024, loader)

	// First get triggers loader
	got, err := c.Get(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("loaded index should not be nil")
	}
	if loadCount != 1 {
		t.Errorf("want 1 load, got %d", loadCount)
	}

	// Second get should be cached
	got2, err := c.Get(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if got2 == nil {
		t.Fatal("cached index should not be nil")
	}
	if loadCount != 1 {
		t.Errorf("should still be 1 load, got %d", loadCount)
	}
}

func TestBloomCache_LoaderError(t *testing.T) {
	loader := func(ctx context.Context, partition string) (*Index, error) {
		return nil, errors.New("s3 unavailable")
	}

	c := NewBloomCache(1024*1024, loader)
	_, err := c.Get(context.Background(), "p1")
	if err == nil {
		t.Error("expected error from loader")
	}
}

func TestBloomCache_LoaderReturnsNil(t *testing.T) {
	loader := func(ctx context.Context, partition string) (*Index, error) {
		return nil, nil
	}

	c := NewBloomCache(1024*1024, loader)
	got, err := c.Get(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("nil from loader should return nil")
	}
}

func TestBloomCache_Invalidate(t *testing.T) {
	c := NewBloomCache(1024*1024, nil)
	idx := New()
	idx.Add("f1", "trace_id", filterWith("a"))
	c.Put("p1", idx)

	if c.Len() != 1 {
		t.Fatal("should have 1 entry")
	}

	c.Invalidate("p1")
	if c.Len() != 0 {
		t.Error("should have 0 entries after invalidate")
	}
}

func TestBloomCache_LRUEviction(t *testing.T) {
	// Small cache: fits ~2 entries
	idx1 := New()
	idx1.Add("f1", "trace_id", filterWith("a"))
	size := len(idx1.Marshal())

	c := NewBloomCache(size*2+10, nil)

	c.Put("p1", idx1)

	idx2 := New()
	idx2.Add("f2", "trace_id", filterWith("b"))
	c.Put("p2", idx2)

	if c.Len() != 2 {
		t.Fatalf("should have 2 entries, got %d", c.Len())
	}

	// Access p1 to make it more recent
	c.Get(context.Background(), "p1")

	// Adding p3 should evict p2 (least recently used)
	idx3 := New()
	idx3.Add("f3", "trace_id", filterWith("c"))
	c.Put("p3", idx3)

	if c.Len() != 2 {
		t.Errorf("should still have 2 entries after eviction, got %d", c.Len())
	}

	// p1 should survive (was recently accessed)
	got, _ := c.Get(context.Background(), "p1")
	if got == nil {
		t.Error("p1 should survive eviction (recently used)")
	}
}

func TestBloomCache_Warm(t *testing.T) {
	loadedPartitions := make(map[string]bool)
	loader := func(ctx context.Context, partition string) (*Index, error) {
		loadedPartitions[partition] = true
		idx := New()
		idx.Add("f", "trace_id", filterWith(partition))
		return idx, nil
	}

	c := NewBloomCache(1024*1024, loader)
	err := c.Warm(context.Background(), []string{"p1", "p2", "p3"})
	if err != nil {
		t.Fatal(err)
	}

	if c.Len() != 3 {
		t.Errorf("want 3 cached, got %d", c.Len())
	}
	for _, p := range []string{"p1", "p2", "p3"} {
		if !loadedPartitions[p] {
			t.Errorf("%s not loaded during warm", p)
		}
	}
}

func TestBloomCache_WarmCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	loader := func(ctx context.Context, partition string) (*Index, error) {
		t.Error("loader should not be called on cancelled context")
		return nil, nil
	}

	c := NewBloomCache(1024*1024, loader)
	err := c.Warm(ctx, []string{"p1", "p2"})
	if err == nil {
		t.Error("expected context error")
	}
}

func TestBloomCache_ConcurrentAccess(t *testing.T) {
	loader := func(ctx context.Context, partition string) (*Index, error) {
		idx := New()
		idx.Add("f", "trace_id", filterWith(partition))
		return idx, nil
	}

	c := NewBloomCache(1024*1024, loader)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				partition := "p" + itoa(j%10)
				c.Get(context.Background(), partition)
				c.Len()
				c.Size()
			}
		}(i)
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c.Invalidate("p" + itoa(id))
		}(i)
	}

	wg.Wait()
}

func TestBloomCache_Size(t *testing.T) {
	c := NewBloomCache(1024*1024, nil)

	if c.Size() != 0 {
		t.Error("empty cache should have size 0")
	}

	idx := New()
	idx.Add("f1", "trace_id", filterWith("a"))
	c.Put("p1", idx)

	if c.Size() == 0 {
		t.Error("non-empty cache should have size > 0")
	}
}
