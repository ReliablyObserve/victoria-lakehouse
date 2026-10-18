package s3reader

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func TestStoreClock_ObserveIgnoresWhatItCannotParse(t *testing.T) {
	var c StoreClock
	if c.Known() {
		t.Fatal("a fresh clock claims to know the store time")
	}
	c.Observe("")
	c.Observe("not a date")
	if c.Known() {
		t.Fatal("an absent or garbled Date header was taken as an observation")
	}
	if d := time.Since(c.Now()); d < 0 || d > time.Second {
		t.Fatalf("without an observation Now() should be the local time, off by %v", d)
	}
}

func TestStoreClock_FollowsTheStoreAndAdvances(t *testing.T) {
	var c StoreClock
	future := time.Now().Add(10 * time.Minute).UTC().Truncate(time.Second)
	c.Observe(future.Format(http.TimeFormat))
	if !c.Known() {
		t.Fatal("a valid Date header was not recorded")
	}
	first := c.Now()
	if diff := first.Sub(future); diff < 0 || diff > 2*time.Second {
		t.Fatalf("Now() = %v, want about %v", first, future)
	}
	time.Sleep(20 * time.Millisecond)
	if !c.Now().After(first) {
		t.Fatal("the store clock does not advance with the local monotonic clock")
	}
}

func TestClientPool_StoreClockAccessors(t *testing.T) {
	var p ClientPool
	if p.StoreClockKnown() {
		t.Fatal("a pool without a clock reports a known store time")
	}
	if d := time.Since(p.StoreNow()); d < 0 || d > time.Second {
		t.Fatalf("StoreNow without a clock should be the local time, off by %v", d)
	}
	p.clock = &StoreClock{}
	if p.StoreClock() != p.clock || p.StoreClockKnown() {
		t.Fatal("StoreClock accessor wrong")
	}
	p.clock.Observe(time.Now().Add(time.Hour).UTC().Format(http.TimeFormat))
	if !p.StoreClockKnown() || p.StoreNow().Before(time.Now().Add(59*time.Minute)) {
		t.Fatal("StoreNow does not follow the observed clock")
	}
}

// dateServer answers every request with an empty 404 and a Date header.
type dateServer struct{ date string }

func (d dateServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if d.date != "" {
		w.Header().Set("Date", d.date)
	} else {
		w.Header()["Date"] = nil // net/http adds one unless the key is present and empty
	}
	w.WriteHeader(http.StatusNotFound)
}

func TestClientPool_LearnsTheStoreClockFromResponses(t *testing.T) {
	future := time.Now().Add(3 * time.Hour).UTC().Truncate(time.Second)
	ts := httptest.NewServer(dateServer{date: future.Format(http.TimeFormat)})
	t.Cleanup(ts.Close)
	pool, err := NewClientPool(context.Background(), &config.S3Config{
		Bucket: "b", Region: "us-east-1", Endpoint: ts.URL, AccessKey: "k", SecretKey: "s", ForcePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if pool.StoreClockKnown() {
		t.Fatal("the clock is known before any request")
	}
	_, _ = pool.Exists(context.Background(), "p/k") // any request carries the Date header, even a 404
	if !pool.StoreClockKnown() {
		t.Fatal("a response with a Date header did not set the store clock")
	}
	if d := pool.StoreNow().Sub(future); d < 0 || d > 3*time.Second {
		t.Fatalf("StoreNow = %v, want about %v", pool.StoreNow(), future)
	}
	// A clone for another bucket shares the clock.
	if !pool.WithBucket("other").StoreClockKnown() {
		t.Fatal("a WithBucket clone does not share the store clock")
	}
}

// A store that sends no Date header leaves the clock unknown: StoreNow is then
// the local time, and callers can tell (StoreClockKnown).
func TestClientPool_NoDateHeaderLeavesTheClockUnknown(t *testing.T) {
	ts := httptest.NewServer(dateServer{})
	t.Cleanup(ts.Close)
	pool, err := NewClientPool(context.Background(), &config.S3Config{
		Bucket: "b", Region: "us-east-1", Endpoint: ts.URL, AccessKey: "k", SecretKey: "s", ForcePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = pool.Exists(context.Background(), "p/k")
	if pool.StoreClockKnown() {
		t.Fatal("the clock is known although no response carried a Date header")
	}
}
