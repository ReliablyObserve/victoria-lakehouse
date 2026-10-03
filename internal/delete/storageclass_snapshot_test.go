package delete

import (
	"fmt"
	"sync"
	"testing"
)

func TestCompactionSafety_CachedNonRewritableSnapshot(t *testing.T) {
	d := NewStorageClassDetector(nil)
	if got := d.CachedNonRewritableKeys(); got != nil {
		t.Fatal("empty snapshot must be nil")
	}
	if n := testing.AllocsPerRun(100, func() { _ = d.CachedNonRewritableKeys() }); n != 0 {
		t.Fatalf("empty snapshot allocs=%v", n)
	}
	d.SetCache("standard", ClassStandard)
	d.SetCache("intelligent", ClassIntelligentTiering)
	d.SetCache("ia", ClassStandardIA)
	d.SetCache("glacier", ClassGlacier)
	snap := d.CachedNonRewritableKeys()
	if len(snap) != 2 {
		t.Fatalf("snapshot=%v", snap)
	}
	d.SetCache("ia", ClassStandard)
	d.SetCache("added", ClassDeepArchive)
	if _, ok := snap["ia"]; !ok {
		t.Fatal("snapshot changed with detector")
	}
	if _, ok := snap["added"]; ok {
		t.Fatal("snapshot changed with detector")
	}
	delete(snap, "glacier")
	if _, ok := d.CachedNonRewritableKeys()["glacier"]; !ok {
		t.Fatal("snapshot aliases detector")
	}
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := fmt.Sprintf("g%d/%d", g, i%10)
				if i%2 == 0 {
					d.SetCache(key, ClassStandardIA)
				} else {
					d.SetCache(key, ClassStandard)
				}
				s := d.CachedNonRewritableKeys()
				if _, ok := s["standard"]; ok {
					t.Error("rewritable key in snapshot")
				}
			}
		}(g)
	}
	wg.Wait()
}
