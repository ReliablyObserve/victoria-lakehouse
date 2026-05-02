package peercache

import (
	"fmt"
	"sync"
	"testing"
)

func TestRing_ConcurrentLookupDuringSet(t *testing.T) {
	r := NewRing("self:9428", 150)
	r.Set([]string{"self:9428", "peer1:9428", "peer2:9428"})

	const goroutines = 50
	const ops = 1000
	var wg sync.WaitGroup
	wg.Add(goroutines + 5)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("key-%d-%d", id, i)
				peer, _ := r.Lookup(key)
				if peer == "" {
					t.Errorf("Lookup(%q) returned empty peer", key)
					return
				}
			}
		}(g)
	}

	for g := 0; g < 5; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				peers := []string{"self:9428", fmt.Sprintf("peer%d:9428", i%5+1)}
				r.Set(peers)
			}
		}(g)
	}

	wg.Wait()
}

func TestRing_ConcurrentMembersLookup(t *testing.T) {
	r := NewRing("self:9428", 150)
	r.Set([]string{"self:9428", "a:9428", "b:9428", "c:9428"})

	const goroutines = 30
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				switch id % 3 {
				case 0:
					_ = r.Members()
				case 1:
					_ = r.MemberCount()
				default:
					r.Lookup(fmt.Sprintf("k-%d", i))
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestHandler_ConcurrentPutGet(t *testing.T) {
	h := NewHandler("secret")
	const goroutines = 30
	const ops = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("key-%d-%d", id, i%10)
				data := []byte(fmt.Sprintf("data-%d-%d", id, i))
				h.Put(key, data)
				h.Get(key)
			}
		}(g)
	}
	wg.Wait()
}
