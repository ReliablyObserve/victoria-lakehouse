package peercache

import (
	"fmt"
	"testing"
)

func FuzzRingLookupAZ(f *testing.F) {
	f.Add("file.parquet")
	f.Add("")
	f.Add("dt=2026-05-02/hour=10/batch.parquet")
	f.Add("a")
	f.Add("\x00\x01\x02")
	f.Add("key with spaces and unicode: 日本語")
	f.Add("very/deep/nested/path/to/file.parquet")

	f.Fuzz(func(t *testing.T, key string) {
		r := NewRing("self:9428", 150)
		peerZones := map[string]string{
			"self:9428":   "az-a",
			"peer-a:9428": "az-a",
			"peer-b:9428": "az-b",
			"peer-c:9428": "az-c",
		}
		r.SetWithZones(peerZones, "az-a")

		peer, isLocal, isSameAZ := r.LookupAZ(key)

		if peer == "" {
			t.Errorf("LookupAZ(%q) returned empty peer", key)
		}
		if !isSameAZ {
			t.Errorf("LookupAZ(%q): with same-AZ peers available, should always route same-AZ", key)
		}
		if isLocal && peer != "self:9428" {
			t.Errorf("isLocal=true but peer=%q (not self)", peer)
		}

		// Consistency: same key → same result
		peer2, isLocal2, isSameAZ2 := r.LookupAZ(key)
		if peer != peer2 || isLocal != isLocal2 || isSameAZ != isSameAZ2 {
			t.Errorf("LookupAZ(%q) inconsistent: (%q,%v,%v) vs (%q,%v,%v)",
				key, peer, isLocal, isSameAZ, peer2, isLocal2, isSameAZ2)
		}
	})
}

func FuzzRingLookupAZ_NoSameAZ(f *testing.F) {
	f.Add("file.parquet")
	f.Add("another-key")
	f.Add("")

	f.Fuzz(func(t *testing.T, key string) {
		r := NewRing("self:9428", 150)
		peerZones := map[string]string{
			"self:9428":   "az-a",
			"peer-b:9428": "az-b",
			"peer-c:9428": "az-c",
		}
		r.SetWithZones(peerZones, "az-a")

		peer, _, isSameAZ := r.LookupAZ(key)
		if peer == "" {
			t.Errorf("LookupAZ(%q) returned empty peer", key)
		}
		// Self is the only same-AZ member, so all lookups go to self
		if !isSameAZ {
			t.Errorf("with only self in az-a, should still report same-AZ for key %q", key)
		}
	})
}

func TestRing_SetWithZones_EmptyZone(t *testing.T) {
	r := NewRing("self:9428", 150)
	peerZones := map[string]string{
		"self:9428":   "",
		"peer-a:9428": "",
	}
	r.SetWithZones(peerZones, "")

	// Empty selfZone = hasZoneInfo=false
	if r.hasZoneInfo {
		t.Error("empty selfZone should set hasZoneInfo=false")
	}
}

func TestRing_SetWithZones_SinglePeer(t *testing.T) {
	r := NewRing("self:9428", 150)
	peerZones := map[string]string{
		"self:9428": "az-a",
	}
	r.SetWithZones(peerZones, "az-a")

	peer, isLocal, isSameAZ := r.LookupAZ("any-key")
	if peer != "self:9428" {
		t.Errorf("expected self, got %q", peer)
	}
	if !isLocal {
		t.Error("single peer should be local")
	}
	if !isSameAZ {
		t.Error("single peer should be same-AZ")
	}
}

func TestRing_SetWithZones_OverwritesPreviousState(t *testing.T) {
	r := NewRing("self:9428", 150)

	r.SetWithZones(map[string]string{
		"self:9428":   "az-a",
		"peer-a:9428": "az-a",
		"peer-b:9428": "az-b",
	}, "az-a")
	if r.MemberCount() != 3 {
		t.Fatalf("expected 3, got %d", r.MemberCount())
	}

	// Overwrite with fewer peers
	r.SetWithZones(map[string]string{
		"self:9428": "az-x",
	}, "az-x")
	if r.MemberCount() != 1 {
		t.Errorf("after overwrite expected 1, got %d", r.MemberCount())
	}
	sameAZ, crossAZ := r.MemberCountByZone()
	if sameAZ != 1 || crossAZ != 0 {
		t.Errorf("expected sameAZ=1 crossAZ=0, got %d/%d", sameAZ, crossAZ)
	}
}

func TestRing_MemberCountByZone_NoZoneInfo(t *testing.T) {
	r := NewRing("self:9428", 150)
	r.Set([]string{"self:9428", "peer:9428"})

	sameAZ, crossAZ := r.MemberCountByZone()
	if sameAZ != 2 {
		t.Errorf("without zone info, all members should be 'same-AZ': got %d", sameAZ)
	}
	if crossAZ != 0 {
		t.Errorf("without zone info, crossAZ should be 0: got %d", crossAZ)
	}
}

func TestRing_MemberCountByZone_ManyZones(t *testing.T) {
	r := NewRing("self:9428", 10) // small vnodes for faster test
	peers := map[string]string{"self:9428": "az-a"}
	for i := 0; i < 20; i++ {
		zone := fmt.Sprintf("az-%c", 'a'+byte(i%4))
		peers[fmt.Sprintf("peer-%d:9428", i)] = zone
	}
	r.SetWithZones(peers, "az-a")

	sameAZ, crossAZ := r.MemberCountByZone()
	total := sameAZ + crossAZ
	if total != 21 {
		t.Errorf("expected 21 total, got %d", total)
	}
	if sameAZ < 1 {
		t.Error("self should always be in same-AZ")
	}
}

func TestRing_LookupAZ_WrapAround(t *testing.T) {
	r := NewRing("self:9428", 1)
	r.SetWithZones(map[string]string{
		"self:9428": "az-a",
		"peer:9428": "az-a",
	}, "az-a")

	// With only 1 vnode each, hash wrap-around is very likely for most keys
	for i := 0; i < 100; i++ {
		peer, _, isSameAZ := r.LookupAZ(fmt.Sprintf("wrap-test-%d", i))
		if peer == "" {
			t.Errorf("empty peer for key wrap-test-%d", i)
		}
		if !isSameAZ {
			t.Errorf("should be same-AZ for wrap-test-%d", i)
		}
	}
}

func TestRing_Lookup_EmptyRing(t *testing.T) {
	r := NewRing("self:9428", 150)

	peer, isLocal := r.Lookup("any-key")
	if peer != "self:9428" {
		t.Errorf("empty ring should return self, got %q", peer)
	}
	if !isLocal {
		t.Error("empty ring should be local")
	}
}

func TestRing_SetWithZones_LargeRing(t *testing.T) {
	r := NewRing("self:9428", 150)
	peers := map[string]string{"self:9428": "az-a"}
	for i := 0; i < 100; i++ {
		peers[fmt.Sprintf("peer-%d:9428", i)] = fmt.Sprintf("az-%c", 'a'+byte(i%3))
	}
	r.SetWithZones(peers, "az-a")

	if r.MemberCount() != 101 {
		t.Errorf("expected 101 members, got %d", r.MemberCount())
	}

	// All lookups should be same-AZ when same-AZ peers exist
	for i := 0; i < 200; i++ {
		_, _, isSameAZ := r.LookupAZ(fmt.Sprintf("key-%d", i))
		if !isSameAZ {
			t.Errorf("lookup %d should be same-AZ in large ring", i)
		}
	}
}
