package crosssignal

import (
	"fmt"
	"runtime"
	"testing"
)

func csForceGC() {
	runtime.GC()
	runtime.GC()
}

func csHeapInUse() uint64 {
	var m runtime.MemStats
	csForceGC()
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

func TestMemLeak_Client_EnqueueHintNoEndpoint(t *testing.T) {
	// Client with empty endpoint — all operations are no-ops, no goroutines started
	c := NewClient(ClientConfig{})

	// Warm up
	for i := 0; i < 200; i++ {
		c.EnqueueHint([]string{fmt.Sprintf("trace-%d", i)}, 1000, 2000, "logs")
	}
	csForceGC()

	before := csHeapInUse()

	const iterations = 50000
	for i := 0; i < iterations; i++ {
		c.EnqueueHint([]string{fmt.Sprintf("trace-%d", i%1000)}, 1000, 2000, "logs")
	}

	csForceGC()
	after := csHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d EnqueueHint no-op cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Client_EnqueueHintBatching(t *testing.T) {
	// Client with no endpoint — enqueue is a pure no-op (endpoint == "")
	c := NewClient(ClientConfig{
		MaxBatch: 10,
		Endpoint: "", // no-op mode, avoids needing a real server
	})

	// Warm up
	for i := 0; i < 100; i++ {
		traceIDs := make([]string, 5)
		for j := 0; j < 5; j++ {
			traceIDs[j] = fmt.Sprintf("trace-%d-%d", i, j)
		}
		c.EnqueueHint(traceIDs, 0, 1000, "traces")
	}
	csForceGC()

	before := csHeapInUse()

	const iterations = 10000
	for i := 0; i < iterations; i++ {
		traceIDs := make([]string, 3)
		for j := 0; j < 3; j++ {
			traceIDs[j] = fmt.Sprintf("t-%d-%d", i%100, j)
		}
		c.EnqueueHint(traceIDs, int64(i)*1000, int64(i)*2000, "logs")
	}

	csForceGC()
	after := csHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d batched EnqueueHint cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_Client_SendEvictionHintNoEndpoint(t *testing.T) {
	c := NewClient(ClientConfig{}) // empty endpoint — no-ops

	// Warm up
	for i := 0; i < 100; i++ {
		c.SendEvictionHint([]string{fmt.Sprintf("trace-%d", i)}, "logs")
	}
	csForceGC()

	before := csHeapInUse()

	const iterations = 20000
	for i := 0; i < iterations; i++ {
		c.SendEvictionHint([]string{fmt.Sprintf("trace-%d", i%500)}, "traces")
	}

	csForceGC()
	after := csHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d SendEvictionHint no-op cycles (max %d)", growth, iterations, maxAllowed)
	}
}

func TestMemLeak_PrefetchHint_JSONStruct(t *testing.T) {
	// Test that creating PrefetchHint structs with varying sizes doesn't leak
	csForceGC()
	before := csHeapInUse()

	const iterations = 50000
	for i := 0; i < iterations; i++ {
		h := PrefetchHint{
			TraceIDs:     []string{fmt.Sprintf("tid-%d", i%1000)},
			StartNs:      int64(i) * 1000,
			EndNs:        int64(i)*1000 + 1e9,
			SourceSignal: "logs",
		}
		_ = h.TraceIDs[0] // prevent optimizer
	}

	csForceGC()
	after := csHeapInUse()

	growth := int64(after) - int64(before)
	maxAllowed := int64(5 * 1024 * 1024)
	if growth > maxAllowed {
		t.Errorf("heap grew %d bytes over %d PrefetchHint struct alloc cycles (max %d)", growth, iterations, maxAllowed)
	}
}
