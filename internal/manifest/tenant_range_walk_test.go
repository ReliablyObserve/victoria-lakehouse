package manifest

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// The per-tenant range lookups (GetFilesForRangeTenant, and
// GetFilesForRangeUntenanted for 0:0's legacy objects) back every scoped read.
// They must return exactly what filtering the unscoped GetFilesForRange by
// tenant would, and their cost must follow the partitions inside the query
// window, not every partition a tenant ever wrote.

func rangeWalkPartition(t time.Time) string {
	return fmt.Sprintf("dt=%s/hour=%02d", t.Format("2006-01-02"), t.Hour())
}

func keysOfFiles(files []FileInfo) []string {
	out := make([]string, 0, len(files))
	for _, fi := range files {
		out = append(out, fi.Key)
	}
	sort.Strings(out)
	return out
}

// filteredWalk is the reference answer: the unscoped walk, filtered by key.
func filteredWalk(m *Manifest, startNs, endNs int64, keep func(key string) bool) []string {
	var out []string
	for _, fi := range m.GetFilesForRange(startNs, endNs) {
		if keep(fi.Key) {
			out = append(out, fi.Key)
		}
	}
	sort.Strings(out)
	return out
}

func equalKeys(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestGetFilesForRangeTenant_MatchesFilteredWalk is a property test: random
// manifests (sparse partitions over two months, several tenants plus legacy
// objects, objects with and without recorded time bounds) queried with random
// and partition-aligned windows, before and after removals and a snapshot
// round-trip (which rebuilds every index wholesale).
func TestGetFilesForRangeTenant_MatchesFilteredWalk(t *testing.T) {
	rnd := rand.New(rand.NewSource(20260914))
	tenants := [][2]string{{"0", "0"}, {"1001", "0"}, {"2002", "7"}, {"3", "4"}}
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	for iteration := 0; iteration < 8; iteration++ {
		m := New("test-bucket", "")
		m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
		var added []struct{ partition, key string }
		for p := 0; p < 24*60; p++ {
			if rnd.Intn(4) != 0 { // sparse: roughly one hour in four holds data
				continue
			}
			hour := base.Add(time.Duration(p) * time.Hour)
			part := rangeWalkPartition(hour)
			for _, tn := range tenants {
				if rnd.Intn(2) == 0 {
					continue
				}
				for i := 0; i < 1+rnd.Intn(3); i++ {
					fi := FileInfo{Key: fmt.Sprintf("%s/%s/logs/%s/f%d.parquet", tn[0], tn[1], part, i), Size: 10, RowCount: 1}
					if rnd.Intn(5) != 0 { // most objects carry bounds; some do not
						lo := hour.Add(time.Duration(rnd.Intn(50)) * time.Minute)
						fi.MinTimeNs, fi.MaxTimeNs = lo.UnixNano(), lo.Add(time.Duration(1+rnd.Intn(9))*time.Minute).UnixNano()
					}
					m.AddFile(part, fi)
					added = append(added, struct{ partition, key string }{part, fi.Key})
				}
			}
			if rnd.Intn(6) == 0 {
				key := fmt.Sprintf("logs/%s/legacy.parquet", part)
				m.AddFile(part, FileInfo{Key: key, Size: 10, RowCount: 1, MinTimeNs: hour.UnixNano(), MaxTimeNs: hour.Add(time.Minute).UnixNano()})
				added = append(added, struct{ partition, key string }{part, key})
			}
		}

		check := func(stage string, m *Manifest) {
			t.Helper()
			for w := 0; w < 40; w++ {
				var startNs, endNs int64
				if w%4 == 0 {
					// Aligned to partition boundaries: the edges the binary search decides.
					s := base.Add(time.Duration(rnd.Intn(24*60)) * time.Hour)
					startNs, endNs = s.UnixNano(), s.Add(time.Duration(1+rnd.Intn(48))*time.Hour).UnixNano()
				} else {
					s := base.Add(time.Duration(rnd.Int63n(int64(60 * 24 * time.Hour))))
					startNs, endNs = s.UnixNano(), s.Add(time.Duration(rnd.Int63n(int64(10*24*time.Hour)))).UnixNano()
				}
				for _, tn := range tenants {
					prefix := tn[0] + "/" + tn[1] + "/"
					got := keysOfFiles(m.GetFilesForRangeTenant(startNs, endNs, tn[0], tn[1]))
					want := filteredWalk(m, startNs, endNs, func(k string) bool { return strings.HasPrefix(k, prefix) })
					if !equalKeys(got, want) {
						t.Fatalf("iteration %d, %s, tenant %s, window [%d,%d]: got %d objects, want %d\ngot  %v\nwant %v",
							iteration, stage, prefix, startNs, endNs, len(got), len(want), got, want)
					}
				}
				gotLegacy := keysOfFiles(m.GetFilesForRangeUntenanted(startNs, endNs))
				wantLegacy := filteredWalk(m, startNs, endNs, func(k string) bool { return strings.HasPrefix(k, "logs/") })
				if !equalKeys(gotLegacy, wantLegacy) {
					t.Fatalf("iteration %d, %s, legacy, window [%d,%d]: got %v, want %v", iteration, stage, startNs, endNs, gotLegacy, wantLegacy)
				}
			}
		}

		check("after adds", m)

		// Remove about a third of the objects, emptying some partitions entirely.
		rnd.Shuffle(len(added), func(i, j int) { added[i], added[j] = added[j], added[i] })
		for _, a := range added[:len(added)/3] {
			m.RemoveFile(a.partition, a.key)
		}
		check("after removals", m)

		path := filepath.Join(t.TempDir(), "manifest.snapshot")
		if err := m.SaveTo(path); err != nil {
			t.Fatalf("SaveTo: %v", err)
		}
		loaded := New("test-bucket", "")
		loaded.SetPrefixTemplate("{AccountID}/{ProjectID}/")
		if err := loaded.LoadFrom(path); err != nil {
			t.Fatalf("LoadFrom: %v", err)
		}
		check("after snapshot load", loaded)
	}
}

// TestGetFilesForRangeTenant_CostFollowsWindow pins the cost of a short query
// on long retention: a one-hour window over a year of hourly partitions must
// not allocate per partition the tenant holds. Walking the tenant's whole
// partition set parsed 8,760 partition keys (~44,000 allocations) per call.
func TestGetFilesForRangeTenant_CostFollowsWindow(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a year of hourly partitions")
	}
	m := New("test-bucket", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	base := time.Date(2025, 9, 1, 0, 0, 0, 0, time.UTC)
	const partitions = 24 * 365
	for p := 0; p < partitions; p++ {
		hour := base.Add(time.Duration(p) * time.Hour)
		part := rangeWalkPartition(hour)
		for _, tn := range []string{"0", "7"} {
			m.AddFile(part, FileInfo{
				Key:       fmt.Sprintf("%s/0/logs/%s/f.parquet", tn, part),
				Size:      10,
				RowCount:  1,
				MinTimeNs: hour.UnixNano(),
				MaxTimeNs: hour.Add(59 * time.Minute).UnixNano(),
			})
		}
		if p%24 == 0 {
			m.AddFile(part, FileInfo{Key: fmt.Sprintf("logs/%s/legacy.parquet", part), Size: 10, RowCount: 1, MinTimeNs: hour.UnixNano(), MaxTimeNs: hour.Add(time.Minute).UnixNano()})
		}
	}
	last := base.Add(time.Duration(partitions-1) * time.Hour)
	startNs, endNs := last.Add(time.Minute).UnixNano(), last.Add(30*time.Minute).UnixNano()

	if n := len(m.GetFilesForRangeTenant(startNs, endNs, "7", "0")); n != 1 {
		t.Fatalf("tenant 7:0 one-hour window selected %d objects, want 1", n)
	}
	const maxAllocs = 16
	if allocs := testing.AllocsPerRun(20, func() { _ = m.GetFilesForRangeTenant(startNs, endNs, "7", "0") }); allocs > maxAllocs {
		t.Errorf("GetFilesForRangeTenant allocates %.0f times for a one-hour window over %d partitions, want at most %d", allocs, partitions, maxAllocs)
	}
	if allocs := testing.AllocsPerRun(20, func() { _ = m.GetFilesForRangeUntenanted(startNs, endNs) }); allocs > maxAllocs {
		t.Errorf("GetFilesForRangeUntenanted allocates %.0f times for a one-hour window over %d partitions, want at most %d", allocs, partitions, maxAllocs)
	}
}
