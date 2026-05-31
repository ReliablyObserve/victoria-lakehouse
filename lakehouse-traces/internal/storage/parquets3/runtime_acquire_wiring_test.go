package parquets3

// Mirror of internal/storage/parquets3/runtime_acquire_wiring_test.go
// in the logs module. Every wiring test in the logs module has a
// corresponding test here so the traces binary observes the same
// load-bearing K8s-style admission semantics as the logs binary.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/resourcebounds"
)

// TestQueryFileWorkers_BoundEnforced (traces) — mirror of the logs
// module test. See that file for the negative-control documentation.
func TestQueryFileWorkers_BoundEnforced(t *testing.T) {
	s := testStorage()

	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 1, LimitCount: 1, Policy: resourcebounds.Fixed,
	}, nil)
	s.bounds = &resourceBoundSet{FileWorkers: bound}

	heldRel, err := bound.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("pre-acquire: %v", err)
	}
	defer heldRel()

	addManifestFiles(s, 3)
	s.cfg.Query.FileWorkers = 3

	startNs, endNs := queryRange(3)
	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = s.RunQuery(ctx, nil, q, noopWriteBlock)

	_, rejected, _, _ := bound.Stats()
	if rejected == 0 {
		t.Fatalf("expected FileWorkers bound rejection under contention; rejected=%d", rejected)
	}
}

// TestQueryFileWorkers_BoundReleases (traces) — mirror.
func TestQueryFileWorkers_BoundReleases(t *testing.T) {
	s := testStorage()
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 2, LimitCount: 2, Policy: resourcebounds.Fixed,
	}, nil)
	s.bounds = &resourceBoundSet{FileWorkers: bound}

	rel1, err := bound.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("pre-acquire 1: %v", err)
	}
	rel2, err := bound.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("pre-acquire 2: %v", err)
	}

	addManifestFiles(s, 3)
	s.cfg.Query.FileWorkers = 3
	startNs, endNs := queryRange(3)
	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = s.RunQuery(ctx, nil, q, noopWriteBlock)

	rel1()
	rel2()
	_, _, _, outCount := bound.Stats()
	if outCount != 0 {
		t.Fatalf("FileWorkers bound leaked: outCount=%d (want 0)", outCount)
	}
}

// TestQueryMaxRows_BoundEnforced (traces) — mirror.
func TestQueryMaxRows_BoundEnforced(t *testing.T) {
	s := testStorage()
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 10, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)
	s.bounds = &resourceBoundSet{QueryMaxRows: bound}

	addManifestFiles(s, 1)
	s.cfg.Query.MaxRows = 8
	s.cfg.Query.FileWorkers = 1

	startNs, endNs := queryRange(1)
	q2 := mustParseQueryWithTime(t, "*", startNs, endNs)

	rel, err := bound.Acquire(context.Background(), 8)
	if err != nil {
		t.Fatalf("pre-reserve: %v", err)
	}
	defer rel()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = s.RunQuery(ctx, nil, q2, noopWriteBlock)

	_, rejected, _, _ := bound.Stats()
	if rejected == 0 {
		t.Fatalf("expected QueryMaxRows rejection when 8+8 > limit=10; rejected=%d", rejected)
	}
}

// TestQueryMaxRows_BoundReleases (traces) — mirror.
func TestQueryMaxRows_BoundReleases(t *testing.T) {
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 100, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)

	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		rel, err := bound.Acquire(ctx, 50)
		if err != nil {
			cancel()
			t.Fatalf("acquire %d: %v", i, err)
		}
		rel()
		cancel()
	}

	acquired, rejected, outBytes, _ := bound.Stats()
	if outBytes != 0 {
		t.Fatalf("QueryMaxRows leaked: outBytes=%d (want 0)", outBytes)
	}
	if rejected != 0 {
		t.Fatalf("QueryMaxRows rejected (want 0): %d", rejected)
	}
	if acquired != 5 {
		t.Fatalf("QueryMaxRows acquired (want 5): %d", acquired)
	}
}

// TestQueryMaxRows_OutlierAdmission (traces) — mirror.
func TestQueryMaxRows_OutlierAdmission(t *testing.T) {
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 100, LimitCount: 0, Policy: resourcebounds.Fixed,
	}, nil)
	const maxRows = int64(1000)
	rel, err := bound.Acquire(context.Background(), maxRows)
	if err != nil {
		t.Fatalf("outlier admit: %v", err)
	}
	defer rel()
	acquired, rejected, outBytes, outCount := bound.Stats()
	if acquired != 1 || rejected != 0 || outBytes != 100 || outCount != 1 {
		t.Errorf("outlier accounting: acquired=%d rejected=%d outBytes=%d outCount=%d", acquired, rejected, outBytes, outCount)
	}
}

// TestQueryFileWorkers_NilBoundIsPassthrough (traces) — mirror.
func TestQueryFileWorkers_NilBoundIsPassthrough(t *testing.T) {
	s := testStorage()
	if s.bounds != nil {
		t.Fatal("testStorage now constructs bounds; update this test")
	}
	addManifestFiles(s, 3)
	s.cfg.Query.FileWorkers = 3
	startNs, endNs := queryRange(3)
	q := mustParseQueryWithTime(t, "*", startNs, endNs)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = s.RunQuery(ctx, nil, q, noopWriteBlock)
}

// TestRuntimeWiring_NoBoundGoroutineLeak (traces) — mirror.
func TestRuntimeWiring_NoBoundGoroutineLeak(t *testing.T) {
	s := testStorage()
	bound := resourcebounds.NewBound(resourcebounds.Config{
		Request: 1, Limit: 2, LimitCount: 2, Policy: resourcebounds.Fixed,
	}, nil)
	s.bounds = &resourceBoundSet{FileWorkers: bound}

	holdA, err := bound.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("hold A: %v", err)
	}
	holdB, err := bound.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatalf("hold B: %v", err)
	}

	addManifestFiles(s, 2)
	s.cfg.Query.FileWorkers = 2

	const N = 20
	var wg sync.WaitGroup
	var ran atomic.Int32
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			startNs, endNs := queryRange(2)
			q := mustParseQueryWithTime(t, "*", startNs, endNs)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			_ = s.RunQuery(ctx, nil, q, noopWriteBlock)
			ran.Add(1)
		}()
	}
	wg.Wait()

	holdA()
	holdB()
	if ran.Load() != N {
		t.Fatalf("only %d/%d queries returned", ran.Load(), N)
	}
	_, _, _, oc := bound.Stats()
	if oc != 0 {
		t.Fatalf("bound leaked %d slots", oc)
	}
}
