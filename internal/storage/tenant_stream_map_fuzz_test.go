package storage

import (
	"encoding/binary"
	"math"
	"sort"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

type pairKey struct {
	t logstorage.TenantID
	h uint64
}

var fuzzTenants = []logstorage.TenantID{{}, {AccountID: math.MaxUint32}, {ProjectID: math.MaxUint32}, {AccountID: math.MaxUint32, ProjectID: math.MaxUint32}, {AccountID: 1}, {ProjectID: 1}, {AccountID: 1, ProjectID: 1}}
var fuzzHashes = []uint64{0, 1, math.MaxUint64, 1 << 63}

func checkTenantStreamMap(t *testing.T, data []byte) {
	var m TenantStreamMap[int]
	want := map[pairKey]int{}
	for i := 0; i+1 < len(data); i += 2 {
		op, sel := data[i], data[i+1]
		var tn logstorage.TenantID
		if op&0x80 != 0 && i+9 < len(data) { // arbitrary tenant ids
			tn = logstorage.TenantID{AccountID: binary.LittleEndian.Uint32(data[i+2:]), ProjectID: binary.LittleEndian.Uint32(data[i+6:])}
		} else {
			tn = fuzzTenants[int(sel)%len(fuzzTenants)]
		}
		h := fuzzHashes[int(sel>>4)%len(fuzzHashes)]
		k := pairKey{tn, h}
		if op&1 == 0 {
			m.Set(tn, h, i)
			want[k] = i
		} else {
			got, ok := m.Get(tn, h)
			w, wok := want[k]
			if ok != wok || got != w {
				t.Fatalf("op %d Get(%v,%x)=%d,%v want %d,%v", i, tn, h, got, ok, w, wok)
			}
		}
	}
	for k, w := range want {
		if got, ok := m.Get(k.t, k.h); !ok || got != w {
			t.Fatalf("final Get(%v,%x)=%d,%v want %d", k.t, k.h, got, ok, w)
		}
	}
	for _, tn := range fuzzTenants {
		for _, h := range fuzzHashes {
			_, ok := m.Get(tn, h)
			if _, wok := want[pairKey{tn, h}]; ok != wok {
				t.Fatalf("phantom/missing %v %x", tn, h)
			}
		}
	}
	var got, w []int
	m.Each(func(v int) { got = append(got, v) })
	for _, v := range want {
		w = append(w, v)
	}
	sort.Ints(got)
	sort.Ints(w)
	if len(got) != len(w) {
		t.Fatalf("Each len %d want %d", len(got), len(w))
	}
	for i := range got {
		if got[i] != w[i] {
			t.Fatalf("Each multiset differs %v vs %v", got, w)
		}
	}
}

// FuzzTenantStreamMap replays random Set/Get sequences, tenants at the edges of
// the uint32 range included, against a map keyed by the (tenant, hash) pair: a
// Get must return what the pair-keyed map holds, a tenant never sees another
// tenant's entry, and Each visits every stored value exactly once.
func FuzzTenantStreamMap(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0x01, 1, 0x01, 0, 0x11, 1, 0x11})
	f.Add([]byte{0x80, 0, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 3, 1, 3})
	f.Fuzz(func(t *testing.T, data []byte) { checkTenantStreamMap(t, data) })
}
