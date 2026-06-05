package cache

import (
	"fmt"
	"sync"
	"testing"
)

// TestLabelIndex_LRU_EvictsLeastRecentlyAdded pins the basic cap
// behavior: adding more distinct fields than the cap allows must
// evict the oldest. The most-recently-touched fields must survive.
func TestLabelIndex_LRU_EvictsLeastRecentlyAdded(t *testing.T) {
	idx := NewLabelIndexWithCap(5)

	// Fill exactly to the cap.
	for i := 0; i < 5; i++ {
		idx.Add(fmt.Sprintf("field%d", i), []string{"v"})
	}
	if idx.Len() != 5 {
		t.Fatalf("len at cap = %d, want 5", idx.Len())
	}

	// Touch field0 — it must NOT be evicted on the next overflow.
	idx.Add("field0", []string{"v2"})

	// Add field5 — should push out field1 (oldest non-touched).
	idx.Add("field5", []string{"v"})

	if idx.Len() != 5 {
		t.Errorf("len after eviction = %d, want 5", idx.Len())
	}
	if idx.GetLabelInfo("field0") == nil {
		t.Error("field0 was evicted but it was recently touched")
	}
	if idx.GetLabelInfo("field1") != nil {
		t.Error("field1 survived eviction but it was the oldest")
	}
	if idx.GetLabelInfo("field5") == nil {
		t.Error("newly added field5 missing")
	}
}

// TestLabelIndex_LRU_UnboundedByDefault checks backward compatibility.
// Code paths that don't call NewLabelIndexWithCap or SetMaxFields must
// behave exactly as before (unbounded growth, no eviction).
func TestLabelIndex_LRU_UnboundedByDefault(t *testing.T) {
	idx := NewLabelIndex()
	for i := 0; i < 100; i++ {
		idx.Add(fmt.Sprintf("field%d", i), []string{"v"})
	}
	if idx.Len() != 100 {
		t.Errorf("unbounded index dropped entries: len=%d, want 100", idx.Len())
	}
	if idx.MaxFields() != 0 {
		t.Errorf("MaxFields() = %d, want 0 (unbounded)", idx.MaxFields())
	}
}

// TestLabelIndex_LRU_SetMaxFieldsShrinks pins the runtime
// reconfiguration path: enabling a cap on an already-populated index
// must evict immediately to fit.
func TestLabelIndex_LRU_SetMaxFieldsShrinks(t *testing.T) {
	idx := NewLabelIndex()
	for i := 0; i < 50; i++ {
		idx.Add(fmt.Sprintf("field%d", i), []string{"v"})
	}
	idx.SetMaxFields(10)
	if idx.Len() != 10 {
		t.Errorf("len after SetMaxFields(10) = %d, want 10", idx.Len())
	}
	if idx.MaxFields() != 10 {
		t.Errorf("MaxFields() = %d, want 10", idx.MaxFields())
	}
}

// TestLabelIndex_LRU_SetMaxFieldsZeroDisables pins the disable path:
// SetMaxFields(0) reverts to unbounded growth and no further
// evictions happen.
func TestLabelIndex_LRU_SetMaxFieldsZeroDisables(t *testing.T) {
	idx := NewLabelIndexWithCap(5)
	for i := 0; i < 5; i++ {
		idx.Add(fmt.Sprintf("field%d", i), []string{"v"})
	}

	idx.SetMaxFields(0)
	if idx.MaxFields() != 0 {
		t.Errorf("MaxFields() = %d, want 0", idx.MaxFields())
	}

	// Add 100 more fields; all must survive now that there's no cap.
	for i := 5; i < 105; i++ {
		idx.Add(fmt.Sprintf("field%d", i), []string{"v"})
	}
	if idx.Len() != 105 {
		t.Errorf("len after disabling cap = %d, want 105", idx.Len())
	}
}

// TestLabelIndex_LRU_MaintainedThroughAddWithValueCounts pins that
// the LRU is updated by ALL add paths, not just Add. Otherwise
// AddWithValueCounts (used by the data-page sampling path) would
// silently fail to move labels to the front and they'd get evicted
// despite being actively touched.
func TestLabelIndex_LRU_MaintainedThroughAddWithValueCounts(t *testing.T) {
	idx := NewLabelIndexWithCap(3)
	idx.Add("a", []string{"v"})
	idx.Add("b", []string{"v"})
	idx.Add("c", []string{"v"})

	// Touch 'a' via the value-counts path.
	idx.AddWithValueCounts("a", []string{"v2"}, map[string]int{"v2": 1})

	// Add 'd' — should evict 'b' (oldest), not 'a'.
	idx.Add("d", []string{"v"})

	if idx.GetLabelInfo("a") == nil {
		t.Error("'a' was evicted but it was recently touched via AddWithValueCounts")
	}
	if idx.GetLabelInfo("b") != nil {
		t.Error("'b' survived eviction but it was the oldest")
	}
}

// TestLabelIndex_LRU_RaceConcurrent runs many goroutines hammering
// Add + GetLabelInfo with the cap enabled, under -race. The LRU and
// labels map must stay in sync — every entry in elems must point to
// a list element whose value is a key in labels.
func TestLabelIndex_LRU_RaceConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("race coverage")
	}
	idx := NewLabelIndexWithCap(100)
	var wg sync.WaitGroup

	// Writers: 1000 distinct keys via 10 goroutines.
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				idx.Add(fmt.Sprintf("g%d_k%d", g, i), []string{"v"})
			}
		}(g)
	}
	// Readers: hammer GetLabelInfo concurrently.
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				idx.GetLabelInfo(fmt.Sprintf("g0_k%d", i%100))
			}
		}()
	}
	wg.Wait()

	// Invariant: len(elems) must equal len(labels), and the LRU list
	// length must match too. Otherwise the eviction logic dropped one
	// side and left the other dangling.
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.lru != nil {
		if len(idx.elems) != len(idx.labels) {
			t.Errorf("LRU drift: elems=%d labels=%d", len(idx.elems), len(idx.labels))
		}
		if idx.lru.Len() != len(idx.labels) {
			t.Errorf("LRU drift: lru.Len=%d labels=%d", idx.lru.Len(), len(idx.labels))
		}
	}
	if idx.Len() > 100 {
		t.Errorf("cap exceeded under race: len=%d, want <=100", idx.Len())
	}
}

// TestLabelIndex_LRU_MergeFromRespectsCap pins that merging from a
// larger source index doesn't blow past the cap. Used during S3
// label-index recovery — a node that lost local disk might pull down
// a huge snapshot from S3; we don't want that to OOM the new pod.
func TestLabelIndex_LRU_MergeFromRespectsCap(t *testing.T) {
	dst := NewLabelIndexWithCap(10)
	dst.Add("local-only", []string{"v"})

	src := NewLabelIndex()
	for i := 0; i < 50; i++ {
		src.Add(fmt.Sprintf("src%d", i), []string{"v"})
	}

	dst.MergeFrom(src)

	if dst.Len() > 10 {
		t.Errorf("merge exceeded cap: len=%d, want <=10", dst.Len())
	}
}
