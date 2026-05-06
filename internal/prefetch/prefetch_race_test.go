package prefetch

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestEngine_Race_MaxGoroutines(t *testing.T) {
	e := NewEngine(8, 100, func(_ context.Context, _ string) error {
		time.Sleep(time.Microsecond)
		return nil
	})
	defer e.Close()

	const goroutines = 500
	const ops = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("k-%d", rng.Intn(100))
				switch rng.Intn(8) {
				case 0:
					e.Enqueue(Task{Key: key, Type: TypeCorrelated})
				case 1:
					e.Enqueue(Task{Key: key, Type: TypeReadAhead})
				case 2:
					e.Enqueue(Task{Key: key, Type: TypeWarmup})
				case 3:
					e.EnqueueCorrelated([]string{key})
				case 4:
					e.EnqueueReadAhead([]string{key})
				case 5:
					e.MarkUseful(key)
				case 6:
					e.Stats()
				case 7:
					_ = e.Active()
					_ = e.QueueLen()
				}
				if i%50 == 0 {
					runtime.Gosched()
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestEngine_Race_EnqueueAndClose(t *testing.T) {
	for trial := 0; trial < 10; trial++ {
		e := NewEngine(4, 50, func(_ context.Context, _ string) error {
			time.Sleep(time.Microsecond)
			return nil
		})

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				e.Enqueue(Task{Key: fmt.Sprintf("k%d", i)})
			}
		}()

		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond)
			e.Close()
		}()

		wg.Wait()
	}
}

func TestEngine_Race_StatsUnderLoad(t *testing.T) {
	e := NewEngine(4, 100, func(_ context.Context, _ string) error {
		return nil
	})
	defer e.Close()

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				e.Enqueue(Task{Key: fmt.Sprintf("k-%d-%d", id, i)})
			}
		}(g)

		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				triggered, completed, errors, useful := e.Stats()
				_ = triggered
				_ = completed
				_ = errors
				_ = useful
				_ = e.Active()
				_ = e.QueueLen()
				runtime.Gosched()
			}
		}()
	}
	wg.Wait()
}

func BenchmarkEngine_EnqueueParallel(b *testing.B) {
	e := NewEngine(8, 1000, func(_ context.Context, _ string) error {
		return nil
	})
	defer e.Close()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			e.Enqueue(Task{Key: fmt.Sprintf("k%d", i), Type: TypeCorrelated})
			i++
		}
	})
}

func BenchmarkEngine_Stats(b *testing.B) {
	e := NewEngine(4, 100, func(_ context.Context, _ string) error {
		return nil
	})
	defer e.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Stats()
	}
}

func BenchmarkEngine_MarkUseful(b *testing.B) {
	e := NewEngine(4, 100, func(_ context.Context, _ string) error {
		return nil
	})
	defer e.Close()

	for i := 0; i < 1000; i++ {
		e.Enqueue(Task{Key: fmt.Sprintf("k%d", i)})
	}
	time.Sleep(100 * time.Millisecond)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.MarkUseful(fmt.Sprintf("k%d", i%1000))
	}
}
