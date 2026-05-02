package cache

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGroup_Do(t *testing.T) {
	g := NewGroup()

	val, err, shared := g.Do("k1", func() ([]byte, error) {
		return []byte("result"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if shared {
		t.Error("first call should not be shared")
	}
	if string(val) != "result" {
		t.Errorf("val = %q, want %q", val, "result")
	}
}

func TestGroup_DoError(t *testing.T) {
	g := NewGroup()

	_, err, _ := g.Do("k1", func() ([]byte, error) {
		return nil, errors.New("fail")
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGroup_DoCoalesces(t *testing.T) {
	g := NewGroup()

	var calls atomic.Int32
	block := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, err, _ := g.Do("k1", func() ([]byte, error) {
				calls.Add(1)
				<-block
				return []byte("result"), nil
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if string(val) != "result" {
				t.Errorf("val = %q, want %q", val, "result")
			}
		}()
	}

	for g.Inflight() < 1 {
		runtime.Gosched()
	}
	time.Sleep(50 * time.Millisecond)

	close(block)
	wg.Wait()

	if calls.Load() != 1 {
		t.Errorf("fn called %d times, want 1 (should coalesce)", calls.Load())
	}
}

func TestGroup_DoDifferentKeys(t *testing.T) {
	g := NewGroup()

	var calls atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			g.Do(k, func() ([]byte, error) {
				calls.Add(1)
				return []byte(k), nil
			})
		}(key)
	}
	wg.Wait()

	if calls.Load() != 5 {
		t.Errorf("fn called %d times, want 5 (different keys)", calls.Load())
	}
}

func TestGroup_DoRemovesAfterDone(t *testing.T) {
	g := NewGroup()

	g.Do("k1", func() ([]byte, error) {
		return []byte("first"), nil
	})

	val, _, shared := g.Do("k1", func() ([]byte, error) {
		return []byte("second"), nil
	})

	if shared {
		t.Error("second call should not be shared (first completed)")
	}
	if string(val) != "second" {
		t.Errorf("val = %q, want %q", val, "second")
	}
}

func TestGroup_Inflight(t *testing.T) {
	g := NewGroup()

	if g.Inflight() != 0 {
		t.Errorf("inflight = %d, want 0", g.Inflight())
	}

	block := make(chan struct{})
	go func() {
		g.Do("k1", func() ([]byte, error) {
			<-block
			return nil, nil
		})
	}()

	for g.Inflight() < 1 {
	}
	if g.Inflight() != 1 {
		t.Errorf("inflight = %d, want 1", g.Inflight())
	}

	close(block)
	for g.Inflight() > 0 {
	}
}
