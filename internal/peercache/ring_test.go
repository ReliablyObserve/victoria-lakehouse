package peercache

import (
	"fmt"
	"testing"
)

func TestRing_Empty(t *testing.T) {
	r := NewRing("self:9428", 0)
	peer, isLocal := r.Lookup("any-key")
	if !isLocal {
		t.Error("empty ring should return local")
	}
	if peer != "self:9428" {
		t.Errorf("peer = %q, want self:9428", peer)
	}
}

func TestRing_SingleMember(t *testing.T) {
	r := NewRing("self:9428", 50)
	r.Set([]string{"self:9428"})

	peer, isLocal := r.Lookup("some-file.parquet")
	if !isLocal {
		t.Error("single member should be local")
	}
	if peer != "self:9428" {
		t.Errorf("peer = %q", peer)
	}
}

func TestRing_ConsistentRouting(t *testing.T) {
	r := NewRing("peer-0:9428", 150)
	r.Set([]string{"peer-0:9428", "peer-1:9428", "peer-2:9428"})

	key := "logs/dt=2026-05-01/hour=10/file-abc.parquet"
	peer1, _ := r.Lookup(key)
	peer2, _ := r.Lookup(key)
	if peer1 != peer2 {
		t.Errorf("inconsistent routing: %q vs %q", peer1, peer2)
	}
}

func TestRing_Distribution(t *testing.T) {
	r := NewRing("peer-0:9428", 150)
	r.Set([]string{"peer-0:9428", "peer-1:9428", "peer-2:9428"})

	counts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("file-%d.parquet", i)
		peer, _ := r.Lookup(key)
		counts[peer]++
	}

	for peer, count := range counts {
		ratio := float64(count) / 1000.0
		if ratio < 0.15 || ratio > 0.55 {
			t.Errorf("peer %s has %d/1000 keys (%.1f%%) — poor distribution", peer, count, ratio*100)
		}
	}

	if len(counts) != 3 {
		t.Errorf("expected 3 peers used, got %d", len(counts))
	}
}

func TestRing_Set_ReplacesOld(t *testing.T) {
	r := NewRing("a:1", 50)
	r.Set([]string{"a:1", "b:1"})

	if r.MemberCount() != 2 {
		t.Fatalf("members = %d, want 2", r.MemberCount())
	}

	r.Set([]string{"c:1", "d:1", "e:1"})
	if r.MemberCount() != 3 {
		t.Fatalf("members = %d, want 3", r.MemberCount())
	}

	members := r.Members()
	for _, m := range members {
		if m == "a:1" || m == "b:1" {
			t.Errorf("old member %q still in ring", m)
		}
	}
}

func TestRing_Members(t *testing.T) {
	r := NewRing("a:1", 50)
	r.Set([]string{"c:1", "a:1", "b:1"})

	members := r.Members()
	if len(members) != 3 {
		t.Fatalf("members = %d, want 3", len(members))
	}
	if members[0] != "a:1" || members[1] != "b:1" || members[2] != "c:1" {
		t.Errorf("members not sorted: %v", members)
	}
}

func TestRing_MemberCount(t *testing.T) {
	r := NewRing("self:1", 50)
	if r.MemberCount() != 0 {
		t.Errorf("initial member count = %d, want 0", r.MemberCount())
	}
	r.Set([]string{"a:1", "b:1"})
	if r.MemberCount() != 2 {
		t.Errorf("member count = %d, want 2", r.MemberCount())
	}
}

func TestRing_StabilityOnAddition(t *testing.T) {
	r := NewRing("a:1", 150)
	r.Set([]string{"a:1", "b:1", "c:1"})

	type assignment struct {
		key  string
		peer string
	}

	var before []assignment
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		peer, _ := r.Lookup(key)
		before = append(before, assignment{key, peer})
	}

	r.Set([]string{"a:1", "b:1", "c:1", "d:1"})

	moved := 0
	for _, a := range before {
		peer, _ := r.Lookup(a.key)
		if peer != a.peer {
			moved++
		}
	}

	maxExpected := 40
	if moved > maxExpected {
		t.Errorf("%d/100 keys moved on adding 1 peer (max expected ~%d)", moved, maxExpected)
	}
}
