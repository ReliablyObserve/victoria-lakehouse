package peercache

import (
	"fmt"
	"testing"
)

func TestRing_SetWithZones(t *testing.T) {
	r := NewRing("self:9428", 150)

	peers := map[string]string{
		"peer-a1:9428": "us-east-1a",
		"peer-a2:9428": "us-east-1a",
		"peer-b1:9428": "us-east-1b",
		"peer-b2:9428": "us-east-1b",
		"self:9428":    "us-east-1a",
	}
	r.SetWithZones(peers, "us-east-1a")

	if r.MemberCount() != 5 {
		t.Fatalf("expected 5 members, got %d", r.MemberCount())
	}
}

func TestRing_LookupAZ_PrefersSameZone(t *testing.T) {
	r := NewRing("self:9428", 150)

	peers := map[string]string{
		"peer-a1:9428": "us-east-1a",
		"peer-a2:9428": "us-east-1a",
		"peer-b1:9428": "us-east-1b",
		"peer-b2:9428": "us-east-1b",
		"self:9428":    "us-east-1a",
	}
	r.SetWithZones(peers, "us-east-1a")

	sameAZ := 0
	crossAZ := 0
	total := 1000
	for i := 0; i < total; i++ {
		_, _, isSameAZ := r.LookupAZ(fmt.Sprintf("key-%d", i))
		if isSameAZ {
			sameAZ++
		} else {
			crossAZ++
		}
	}

	if sameAZ != total {
		t.Errorf("expected all %d lookups to be same-AZ, got sameAZ=%d crossAZ=%d", total, sameAZ, crossAZ)
	}
}

func TestRing_LookupAZ_FallbackToCrossZone(t *testing.T) {
	r := NewRing("self:9428", 150)

	peers := map[string]string{
		"peer-b1:9428": "us-east-1b",
		"peer-b2:9428": "us-east-1b",
		"self:9428":    "us-east-1a",
	}
	r.SetWithZones(peers, "us-east-1a")

	peer, isLocal, isSameAZ := r.LookupAZ("test-key")
	if isLocal {
		return
	}
	if isSameAZ {
		t.Error("non-local peer should be cross-AZ since only self is in same zone")
	}
	if peer == "" {
		t.Error("should always return a peer")
	}
}

func TestRing_LookupAZ_NoZoneInfo_FallsBackToNormal(t *testing.T) {
	r := NewRing("self:9428", 150)

	r.Set([]string{"self:9428", "peer-1:9428", "peer-2:9428"})

	peer, _, isSameAZ := r.LookupAZ("test-key")
	if peer == "" {
		t.Error("should return a peer")
	}
	if !isSameAZ {
		t.Error("without zone info, all peers should be considered same-AZ")
	}
}

func TestRing_LookupAZ_EmptyRing(t *testing.T) {
	r := NewRing("self:9428", 150)
	r.SetWithZones(map[string]string{}, "us-east-1a")

	peer, isLocal, isSameAZ := r.LookupAZ("test-key")
	if peer != "self:9428" {
		t.Errorf("empty ring should return self, got %q", peer)
	}
	if !isLocal {
		t.Error("empty ring should return isLocal=true")
	}
	if !isSameAZ {
		t.Error("self should be same-AZ")
	}
}

func TestRing_MemberCountByZone(t *testing.T) {
	r := NewRing("self:9428", 150)

	peers := map[string]string{
		"self:9428":    "az-a",
		"peer-a:9428":  "az-a",
		"peer-b1:9428": "az-b",
		"peer-b2:9428": "az-b",
		"peer-c:9428":  "az-c",
	}
	r.SetWithZones(peers, "az-a")

	sameAZ, crossAZ := r.MemberCountByZone()
	if sameAZ != 2 {
		t.Errorf("expected 2 same-AZ members, got %d", sameAZ)
	}
	if crossAZ != 3 {
		t.Errorf("expected 3 cross-AZ members, got %d", crossAZ)
	}
}
