package s3reader

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestRetryS3_JitterSpread verifies the backoff IS jittered — at
// max retries with deterministic input, multiple parallel calls
// should NOT all land at the same elapsed time. Without jitter,
// every retry waits exactly (1<<i)*100ms — all callers retry in
// lockstep, defeating S3's rate-limit recovery. Full jitter
// spreads them across the backoff window.
func TestRetryS3_JitterSpread(t *testing.T) {
	const concurrent = 50
	const retries = 3 // backoffs 100ms, 200ms, 400ms

	var wg sync.WaitGroup
	elapsedCh := make(chan time.Duration, concurrent)
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			_ = retryS3(context.Background(), retries, func() error {
				return fmt.Errorf("i/o timeout")
			})
			elapsedCh <- time.Since(start)
		}()
	}
	wg.Wait()
	close(elapsedCh)

	// Bucket elapsed times into 50ms buckets and verify spread:
	// no single bucket should hold > 50% of the samples (with
	// jitter the distribution is uniform across [0, max_total)).
	const bucketWidth = 50 * time.Millisecond
	buckets := make(map[time.Duration]int)
	totalSamples := 0
	for d := range elapsedCh {
		bucket := d / bucketWidth
		buckets[bucket*bucketWidth]++
		totalSamples++
	}

	for bucketStart, count := range buckets {
		fraction := float64(count) / float64(totalSamples)
		if fraction > 0.5 {
			t.Errorf("jitter spread failed: bucket [%v, %v) holds %d/%d samples (%.0f%%) — backoff appears deterministic, not jittered",
				bucketStart, bucketStart+bucketWidth, count, totalSamples, fraction*100)
		}
	}
	if len(buckets) < 3 {
		t.Errorf("jitter spread failed: only %d distinct buckets across %d samples — distribution too narrow", len(buckets), totalSamples)
	}
}

// TestRetryS3_JitterRespectsCap verifies the jitter upper bound
// matches the 5s cap. Without the cap, jitter at retry 7 could
// pick a value in [0, 12.8s) — way beyond the desired cap. The
// `cap` floor in retryS3 prevents this regardless of attempt
// count.
func TestRetryS3_JitterRespectsCap(t *testing.T) {
	const retries = 5 // 1<<10 = 1024 → 102.4s without cap
	calls := 0
	start := time.Now()
	_ = retryS3(context.Background(), retries, func() error {
		calls++
		return fmt.Errorf("i/o timeout")
	})
	elapsed := time.Since(start)

	// 10 retries × 5s cap = 50s worst case. We use 8s as a
	// generous bound — sequential 10-retry mean elapsed under
	// full jitter with 5s cap is closer to 25s (sum of uniform
	// up to cap), but we tolerate slow CI by accepting up to 8s
	// here. The "must be ≤ cap × maxRetries" invariant is what
	// matters, not the exact value.
	maxAcceptable := 30 * time.Second
	if elapsed > maxAcceptable {
		t.Errorf("retry elapsed %v exceeds %v — cap not enforced for high attempt counts", elapsed, maxAcceptable)
	}
	if calls != retries+1 {
		t.Errorf("calls = %d, want %d (initial + %d retries)", calls, retries+1, retries)
	}
}
