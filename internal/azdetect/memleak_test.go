package azdetect

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"
)

func TestMemLeak_AZDetect_FromEnvCycles(t *testing.T) {
	t.Setenv("TEST_AZ_VAR", "us-east-1a")

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 200
	for c := 0; c < cycles; c++ {
		az := Detect(context.Background(), Options{
			EnvVar:  "TEST_AZ_VAR",
			Timeout: 100 * time.Millisecond,
		})
		if az != "us-east-1a" {
			t.Fatalf("unexpected AZ: %q", az)
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("AZDetect from env: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// Env lookup only, no network: 1MB max
	maxGrowth := uint64(1 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d Detect cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_AZDetect_AWSIMDSCycles(t *testing.T) {
	// Stub IMDS server
	tokenCallCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/latest/api/token" {
			tokenCallCount++
			_, _ = w.Write([]byte("fake-token"))
			return
		}
		if r.URL.Path == "/latest/meta-data/placement/availability-zone" {
			_, _ = w.Write([]byte("eu-west-1b"))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 50
	for c := 0; c < cycles; c++ {
		az, err := detectAWSIMDS(context.Background(), srv.URL, 500*time.Millisecond)
		if err != nil {
			t.Fatalf("detectAWSIMDS failed: %v", err)
		}
		if az != "eu-west-1b" {
			t.Fatalf("unexpected AZ: %q", az)
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("detectAWSIMDS: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// HTTP client with connection reuse: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d IMDS cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}

func TestMemLeak_AZDetect_GCPMetadataCycles(t *testing.T) {
	// Stub GCP metadata server
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/computeMetadata/v1/instance/zone" {
			_, _ = fmt.Fprintf(w, "projects/12345/zones/us-central1-a")
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapBefore := m.HeapInuse

	const cycles = 50
	for c := 0; c < cycles; c++ {
		az, err := detectGCPMetadata(context.Background(), srv.URL, 500*time.Millisecond)
		if err != nil {
			t.Fatalf("detectGCPMetadata failed: %v", err)
		}
		if az != "us-central1-a" {
			t.Fatalf("unexpected AZ: %q", az)
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.ReadMemStats(&m)
	heapAfter := m.HeapInuse

	t.Logf("detectGCPMetadata: heap_before=%dKB, heap_after=%dKB",
		heapBefore/1024, heapAfter/1024)

	// HTTP client: 2MB max
	maxGrowth := uint64(2 * 1024 * 1024)
	if heapAfter > heapBefore+maxGrowth {
		t.Errorf("Possible memory leak: heap grew from %dKB to %dKB after %d GCP metadata cycles",
			heapBefore/1024, heapAfter/1024, cycles)
	}
}
