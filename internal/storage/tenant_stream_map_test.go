package storage

import (
	"math"
	"math/rand"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// The zero value is empty and usable.
func TestTenantStreamMap_ZeroValue(t *testing.T) {
	var m TenantStreamMap[int]
	if _, ok := m.Get(logstorage.TenantID{AccountID: 1}, 7); ok {
		t.Fatal("an empty map found a value")
	}
	n := 0
	m.Each(func(int) { n++ })
	if n != 0 {
		t.Fatalf("an empty map has %d values", n)
	}
}

// The same hash under two tenants is two entries, in either order of insertion,
// and the first tenant seen is not special.
func TestTenantStreamMap_SameHashTwoTenants(t *testing.T) {
	a, b := logstorage.TenantID{AccountID: 1, ProjectID: 2}, logstorage.TenantID{AccountID: 2, ProjectID: 2}
	for _, order := range [][2]logstorage.TenantID{{a, b}, {b, a}} {
		var m TenantStreamMap[string]
		m.Set(order[0], 5, "first")
		if _, ok := m.Get(order[1], 5); ok {
			t.Fatalf("tenant %v found the entry of tenant %v", order[1], order[0])
		}
		m.Set(order[1], 5, "second")
		if v, _ := m.Get(order[0], 5); v != "first" {
			t.Errorf("tenant %v got %q, want first", order[0], v)
		}
		if v, _ := m.Get(order[1], 5); v != "second" {
			t.Errorf("tenant %v got %q, want second", order[1], v)
		}
		n := 0
		m.Each(func(string) { n++ })
		if n != 2 {
			t.Errorf("Each visited %d values, want 2", n)
		}
	}
}

// Against a map keyed by the pair, with tenants including 0:0 and the largest
// ids, and hashes including 0 and the largest.
func TestTenantStreamMap_MatchesAPairKeyedMap(t *testing.T) {
	type key struct {
		t logstorage.TenantID
		h uint64
	}
	tenants := []logstorage.TenantID{{}, {AccountID: 1}, {AccountID: 1, ProjectID: 1}, {ProjectID: 1}, {AccountID: math.MaxUint32, ProjectID: math.MaxUint32}}
	hashes := []uint64{0, 1, 2, math.MaxUint64}
	for seed := int64(1); seed <= 50; seed++ {
		rng := rand.New(rand.NewSource(seed))
		var m TenantStreamMap[int]
		want := map[key]int{}
		for i := 0; i < 40; i++ {
			k := key{tenants[rng.Intn(len(tenants))], hashes[rng.Intn(len(hashes))]}
			if rng.Intn(3) > 0 {
				m.Set(k.t, k.h, i)
				want[k] = i
			}
			for _, tn := range tenants {
				for _, h := range hashes {
					got, ok := m.Get(tn, h)
					w, wok := want[key{tn, h}]
					if ok != wok || got != w {
						t.Fatalf("seed %d step %d: Get(%v,%d) = %d,%v want %d,%v", seed, i, tn, h, got, ok, w, wok)
					}
				}
			}
		}
		sum, wsum := 0, 0
		m.Each(func(v int) { sum += v })
		for _, v := range want {
			wsum += v
		}
		if sum != wsum {
			t.Fatalf("seed %d: Each sums %d, want %d", seed, sum, wsum)
		}
	}
}
