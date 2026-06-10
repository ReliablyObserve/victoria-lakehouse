package parquets3

import (
	"context"
	"testing"
	"time"
)

// TestBufferFlusherRun_StopsOnCancel: the flusher loop must exit
// promptly when the context is cancelled (graceful shutdown), before
// ever touching the buffer.
func TestBufferFlusherRun_StopsOnCancel(t *testing.T) {
	f := NewBufferFlusher(nil, nil, t.TempDir(), nil, 0, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		// checkInterval <= 0 exercises the default-interval guard; the
		// cancelled ctx wins before the first tick fires (buffer is nil,
		// so reaching a tick would panic — the test is also a guard that
		// shutdown never touches the buffer).
		f.Run(ctx, 0, time.Now().UnixNano())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("BufferFlusher.Run did not stop on context cancellation")
	}
}

// TestBufferFlusherRun_WatermarkAheadSkipsTicks: with a persisted
// watermark ahead of (now - latencyOffset), ticks must be no-ops (the
// window was already committed — re-flushing would double-write), and
// the loop must still honour cancellation.
func TestBufferFlusherRun_WatermarkAheadSkipsTicks(t *testing.T) {
	dir := t.TempDir()
	f := NewBufferFlusher(nil, nil, dir, nil, 0, 0)

	// Persist a watermark one hour in the future: every tick's flushEnd
	// (now - latencyOffset) is <= last → continue without touching the
	// nil buffer (a regression here panics the test).
	if err := f.saveWatermark(time.Now().Add(time.Hour).UnixNano()); err != nil {
		t.Fatalf("saveWatermark: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		f.Run(ctx, 2*time.Millisecond, time.Now().UnixNano())
		close(done)
	}()
	time.Sleep(30 * time.Millisecond) // let several ticks fire
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("BufferFlusher.Run did not stop after cancellation")
	}
}
