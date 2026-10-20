package delete

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
)

// seedDefinitions writes n definitions straight into the bucket, one per second
// of store time before now, the way n deletes taken over time look.
func seedDefinitions(b *deletetest.Bucket, n int) {
	base := b.Now().Add(-time.Duration(n+10) * time.Second).UnixNano()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("task-%07d", i)
		ts := Tombstone{ID: id, Query: `service.name:="svc-` + fmt.Sprint(i%50) + `" AND level:error`, StartNs: 0, EndNs: base + int64(i)*int64(time.Second),
			FilterAt: base, Mode: "hide", Tenants: []TenantRef{{AccountID: uint32(1000 + i%20)}}}
		body, _ := encodeDefinition(ts)
		b.Put(formatKey(tombPrefix(), tombKey{kind: kindDefinition, timeNs: base + int64(i)*int64(time.Second), id: id}), body)
	}
}

// The S3 requests of the poll do not depend on how many tombstones exist: one
// LIST page of the last ten minutes, plus a GET per new key. The full listing,
// once an hour, is the only part that grows (one page per 1,000 keys), and a warm
// cache removes the GETs of a start. Numbers are printed for the cost table in
// docs/deletion-strategy.md.
func TestScale_PollRequestsAreFlatInTheNumberOfTombstones(t *testing.T) {
	for _, n := range []int{0, 1000, 10000} {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			b := deletetest.NewBucket()
			seedDefinitions(b, n)
			dir := t.TempDir()
			cached := func(o *Options) { o.CacheDir = dir }

			cold, coldPod := newSyncedPod(t, b, "cold", cached)
			if cold.Count() != n {
				t.Fatalf("cold start learned %d of %d", cold.Count(), n)
			}
			cc := coldPod.Counts()
			warm, warmPod := newSyncedPod(t, b, "warm", cached)
			wc := warmPod.Counts()
			if warm.Count() != n {
				t.Fatalf("warm start learned %d of %d", warm.Count(), n)
			}

			// Steady state: an idle poll.
			b.Advance(time.Minute)
			warmPod.ResetCounts()
			mustSync(t, warm, false)
			idle := warmPod.Counts()
			if idle.ListPages != 1 || idle.Gets != 0 || idle.Heads != 0 || idle.Puts != 0 {
				t.Fatalf("an idle poll with N=%d cost %+v, want one LIST page and nothing else", n, idle)
			}
			// The hourly full listing.
			warmPod.ResetCounts()
			mustSync(t, warm, true)
			full := warmPod.Counts()
			wantPages := int64((n + 999) / 1000)
			if wantPages == 0 {
				wantPages = 1
			}
			if full.ListPages != wantPages || full.Gets != 0 {
				t.Fatalf("a full listing with N=%d cost %+v, want %d LIST pages and no GET", n, full, wantPages)
			}
			t.Logf("N=%-6d cold start: %d LIST pages + %d GETs | warm-cache start: %d LIST pages + %d GETs | idle poll: %d LIST page | full listing: %d LIST pages",
				n, cc.ListPages, cc.Gets, wc.ListPages, wc.Gets, idle.ListPages, full.ListPages)
		})
	}
}

// Memory of the view. Assumed ~0.6 KB per tombstone; measured here.
func TestScale_MemoryOfTheView(t *testing.T) {
	for _, n := range []int{1000, 10000, 100000} {
		if n >= 100000 && testing.Short() {
			continue
		}
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			b := deletetest.NewBucket()
			seedDefinitions(b, n)
			pod := b.Pod("reader")
			s := NewTombstoneStore()
			if err := s.Attach(Options{Backend: pod, Prefix: testPrefix}); err != nil {
				t.Fatal(err)
			}
			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)
			mustSync(t, s, true)
			runtime.GC()
			runtime.GC()
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			if s.Count() != n {
				t.Fatalf("learned %d of %d", s.Count(), n)
			}
			grown := int64(after.HeapAlloc) - int64(before.HeapAlloc)
			perTombstone := float64(grown) / float64(n)
			t.Logf("N=%-6d view heap: %.1f MB, %.0f B per tombstone (measured, GC'd)", n, float64(grown)/(1<<20), perTombstone)
			// The assumption was ~600 B; fail only on a clear blow-up.
			if perTombstone > 2048 {
				t.Fatalf("%.0f B per tombstone, want under 2 KB", perTombstone)
			}
			// A query reads the view: ForRange cost stays linear and cheap.
			start := time.Now()
			for i := 0; i < 20; i++ {
				_ = s.ForRange(0, 1<<62)
			}
			t.Logf("N=%-6d ForRange over everything: %v per call", n, time.Since(start)/20)
		})
	}
}

func BenchmarkForRange(b *testing.B) {
	for _, n := range []int{100, 10000} {
		bucket := deletetest.NewBucket()
		seedDefinitions(bucket, n)
		s := NewTombstoneStore()
		if err := s.Attach(Options{Backend: bucket.Pod("p"), Prefix: testPrefix}); err != nil {
			b.Fatal(err)
		}
		if err := s.SyncNow(context.Background(), true); err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("N=%d/overlaps-all", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = s.ForRange(0, 1<<62)
			}
		})
		// The common query: newer than every delete, so none can overlap.
		recent := bucket.Now().UnixNano()
		b.Run(fmt.Sprintf("N=%d/newer-than-all", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if len(s.ForRange(recent, recent+int64(time.Hour))) != 0 {
					b.Fatal("overlap")
				}
			}
		})
	}
}

// A mass expiry is one pass, not one per entry: 100,000 tombstones leave the view
// in one sweep within a time bound, the ordered index, the filter cache and the
// counters agree afterwards, and queries are not blocked behind quadratic work.
func TestScale_MassExpiryIsOnePass(t *testing.T) {
	n := 100000
	if testing.Short() {
		n = 20000
	}
	b := deletetest.NewBucket()
	seedDefinitions(b, n)
	s, _ := newSyncedPod(t, b, "reader", withRetention)
	if s.Count() != n {
		t.Fatalf("fixture: %d of %d", s.Count(), n)
	}
	b.Advance(testMaxTTL + 365*24*time.Hour)
	start := time.Now()
	mustSync(t, s, true)
	took := time.Since(start)
	t.Logf("N=%d: sweep of %d expired tombstones took %v (measured)", n, n, took)
	if took > 5*time.Second {
		t.Fatalf("mass expiry took %v, want a linear pass (under 5s)", took)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.inForce != 0 || len(s.byID) != 0 || len(s.byEnd) != 0 || len(s.filterRefs) != 0 {
		t.Fatalf("view not empty after the sweep: inForce=%d byID=%d byEnd=%d filterRefs=%d", s.inForce, len(s.byID), len(s.byEnd), len(s.filterRefs))
	}
}

// The ordered index, the running count and the filter refcounts stay consistent
// through creates, replaces, un-deletes and removals, in single and batched form.
func TestStore_IndexCountersStayConsistent(t *testing.T) {
	s := NewTombstoneStore()
	mk := func(id string, end int64, q string) Tombstone {
		return Tombstone{ID: id, Query: q, EndNs: end, Tenants: tenant1001(), Mode: "hide"}
	}
	for i := 0; i < 100; i++ {
		s.Add(mk(fmt.Sprintf("t%03d", i), int64(1000-i), fmt.Sprintf("level:=\"q%d\"", i%5)))
	}
	check := func(stage string, want int) {
		t.Helper()
		s.mu.RLock()
		defer s.mu.RUnlock()
		if s.inForce != want || len(s.byID) < want {
			t.Fatalf("%s: inForce=%d byID=%d, want %d in force", stage, s.inForce, len(s.byID), want)
		}
		if len(s.byEnd) != len(s.byID) {
			t.Fatalf("%s: index has %d entries, map %d", stage, len(s.byEnd), len(s.byID))
		}
		for i := 1; i < len(s.byEnd); i++ {
			if entryLess(s.byEnd[i], s.byEnd[i-1]) {
				t.Fatalf("%s: index out of order at %d", stage, i)
			}
		}
		refs := 0
		for _, e := range s.filterRefs {
			refs += e
		}
		if refs != len(s.byID) {
			t.Fatalf("%s: filter refs %d, entries %d", stage, refs, len(s.byID))
		}
	}
	check("initial", 100)
	s.Add(mk("t000", 5, "other")) // replaces
	check("replace", 100)
	s.forget("t001")
	check("single forget", 99)
	var ids []string
	for i := 2; i < 60; i++ {
		ids = append(ids, fmt.Sprintf("t%03d", i))
	}
	s.forgetMany(ids)
	check("batched forget", 99-len(ids))
	s.forgetMany([]string{"nope", "t099"})
	check("forget with unknown id", 99-len(ids)-1)
	if got := s.ForRange(0, 1<<62); len(got) != 99-len(ids)-1 {
		t.Fatalf("ForRange = %d, want %d", len(got), 99-len(ids)-1)
	}
}

// Memory of the parsed filters. A tombstone's filter is parsed once per distinct
// query and kept while some tombstone in the view uses it (evicted with the last
// one), so the cost is per distinct query of the tombstones a query touches, on
// top of the view's own bytes per tombstone.
func TestScale_MemoryOfParsedFilters(t *testing.T) {
	const n = 10000
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < n; i++ {
		ts := Tombstone{ID: fmt.Sprintf("f%d", i), Query: fmt.Sprintf(`service.name:="svc-%d" level:="error"`, i), StartNs: 0, EndNs: 100, FilterAt: 1, Tenants: []TenantRef{{}}, Mode: "hide"}
		if ts.Filter() == nil {
			t.Fatal("a valid query did not parse")
		}
	}
	runtime.GC()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	per := float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)) / n
	t.Logf("%d distinct parsed filters: %.0f B each (measured, GC'd)", n, per)
	if per > 4096 {
		t.Fatalf("%.0f B per parsed filter, want under 4 KB", per)
	}
	parsedFilters.Range(func(k, _ any) bool { parsedFilters.Delete(k); return true })
}
