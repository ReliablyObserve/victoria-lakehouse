package peercache

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
)

func TestRing_Race_MaxGoroutines(t *testing.T) {
	r := NewRing("self:9428", 150)
	r.Set([]string{"self:9428", "a:9428", "b:9428"})

	const goroutines = 500
	const ops = 500
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < ops; i++ {
				switch rng.Intn(5) {
				case 0:
					peers := []string{
						fmt.Sprintf("peer-%d:9428", rng.Intn(10)),
						fmt.Sprintf("peer-%d:9428", rng.Intn(10)),
						"self:9428",
					}
					r.Set(peers)
				case 1:
					r.Lookup(fmt.Sprintf("key-%d", rng.Intn(100)))
				case 2:
					_ = r.Members()
				case 3:
					_ = r.MemberCount()
				case 4:
					r.Lookup(fmt.Sprintf("k-%d-%d", id, i))
				}
				if i%100 == 0 {
					runtime.Gosched()
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestHandler_Race_MaxGoroutines(t *testing.T) {
	h := NewHandler("test-key")

	const goroutines = 500
	const ops = 500
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("k-%d", rng.Intn(50))
				switch rng.Intn(3) {
				case 0:
					h.Put(key, make([]byte, rng.Intn(100)+1))
				case 1:
					h.Get(key)
				case 2:
					h.Get(fmt.Sprintf("miss-%d", rng.Intn(100)))
				}
				if i%100 == 0 {
					runtime.Gosched()
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestPeerCache_Race_ConcurrentOps(t *testing.T) {
	pc := New("self:9428", "auth", 5000000000, 10, testLogger())

	const goroutines = 200
	const ops = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < ops; i++ {
				switch rng.Intn(5) {
				case 0:
					pc.UpdatePeers([]string{
						fmt.Sprintf("peer-%d:9428", rng.Intn(5)),
						"self:9428",
					})
				case 1:
					pc.Lookup(fmt.Sprintf("key-%d", rng.Intn(50)))
				case 2:
					_ = pc.Stats()
				case 3:
					_ = pc.Members()
				case 4:
					pc.Lookup(fmt.Sprintf("k-%d-%d", id, i))
				}
				if i%50 == 0 {
					runtime.Gosched()
				}
			}
		}(g)
	}
	wg.Wait()
}

func BenchmarkHandler_PutGet(b *testing.B) {
	h := NewHandler("test-key")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k-%d", i%100)
		h.Put(key, []byte("data"))
		h.Get(key)
	}
}

func BenchmarkPeerCache_Lookup(b *testing.B) {
	pc := New("self:9428", "auth", 5000000000, 10, testLogger())
	pc.UpdatePeers([]string{"self:9428", "a:9428", "b:9428", "c:9428"})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pc.Lookup(fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkPeerCache_Stats(b *testing.B) {
	pc := New("self:9428", "auth", 5000000000, 10, testLogger())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pc.Stats()
	}
}
