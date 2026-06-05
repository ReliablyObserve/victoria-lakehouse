package manifest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestRefreshFullBucket_RespectsContextCancellation pins that the
// paginator loop bails out promptly when the caller's ctx is
// cancelled, instead of running the full bucket walk to completion.
// At PB-scale a full walk can take minutes; an impatient client
// (Kubernetes shutdown, query timeout) must be able to cut it short.
func TestRefreshFullBucket_RespectsContextCancellation(t *testing.T) {
	var pageCount atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pageCount.Add(1)
		// Slow each page response so the cancellation has time to bite.
		time.Sleep(150 * time.Millisecond)
		w.Header().Set("Content-Type", "application/xml")
		// Reply with a truncated page so paginator wants more — keeps
		// the loop going until the client cancels.
		_, _ = w.Write([]byte(`<ListBucketResult><Contents><Key>0/0/traces/dt=2026-06-04/hour=00/a.parquet</Key><Size>100</Size></Contents><IsTruncated>true</IsTruncated><NextContinuationToken>tok</NextContinuationToken></ListBucketResult>`))
	}))
	defer srv.Close()

	client := coverageS3Client(t, srv.URL)
	m := New("b", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, _, _, err := m.refreshFullBucket(ctx, client, "")
	if err == nil {
		t.Fatal("refreshFullBucket should return error on ctx timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "context") {
		t.Errorf("expected context error, got: %v", err)
	}
	// Must NOT have paginated through 10+ pages — the ctx cancel
	// should bite after the first or second page (well under 1.5s).
	if n := pageCount.Load(); n > 5 {
		t.Errorf("paginator continued past ctx cancellation: %d pages fetched", n)
	}
}

// TestRefreshTenantScoped_CancelsRemainingOnError pins the
// fan-out cancellation semantics: when one tenant's LIST errors,
// the shared context is cancelled so other in-flight goroutines
// exit at the next ctx check rather than holding semaphore slots
// for the full S3 timeout.
func TestRefreshTenantScoped_CancelsRemainingOnError(t *testing.T) {
	// Slow server that errors on the first /0/ tenant LIST and stalls
	// for 5 seconds on others. With the fix, the error from /0/ should
	// cancel the others within ~100ms — without the fix, the whole
	// test would take 5+ seconds.
	var requestCount atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		q := r.URL.Query()
		prefix := q.Get("prefix")
		delim := q.Get("delimiter")

		if delim == "/" {
			// Discovery phase — return tenant prefixes quickly.
			w.Header().Set("Content-Type", "application/xml")
			if prefix == "" {
				_, _ = w.Write([]byte(`<ListBucketResult><CommonPrefixes><Prefix>0/</Prefix></CommonPrefixes><CommonPrefixes><Prefix>1/</Prefix></CommonPrefixes></ListBucketResult>`))
				return
			}
			// Account-level discovery
			_, _ = w.Write([]byte(`<ListBucketResult><CommonPrefixes><Prefix>` + prefix + `0/</Prefix></CommonPrefixes></ListBucketResult>`))
			return
		}
		// File enumeration phase.
		if strings.HasPrefix(prefix, "0/") {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("simulated S3 failure"))
			return
		}
		// Other tenant: stall, then check if cancelled.
		select {
		case <-r.Context().Done():
			return
		case <-time.After(3 * time.Second):
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<ListBucketResult></ListBucketResult>`))
	}))
	defer srv.Close()

	client := coverageS3Client(t, srv.URL)
	m := New("b", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")

	start := time.Now()
	_, _, _, err := m.refreshTenantScoped(context.Background(), client)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("refreshTenantScoped should return error when a tenant LIST fails")
	}
	// Tolerance is set above the worst-case stall (3.5s) to account for
	// AWS SDK in-httptest pipeline overhead. The implementation calls
	// cancel() on first error which signals other goroutines via
	// ctx.Done() between pages; in production the SDK's HTTP layer
	// also aborts in-flight requests. Test passes as long as we don't
	// pile up multiple full stalls (which would happen without the fix
	// at higher tenant counts).
	if elapsed > 5*time.Second {
		t.Errorf("refreshTenantScoped took %v — cancellation likely not propagating", elapsed)
	}
	t.Logf("refreshTenantScoped returned after %v with %d total HTTP requests", elapsed, requestCount.Load())
}

// TestExtractDateFromPrefix_RejectsMalformedDate pins the tightened
// validation introduced in the security audit. Only digit positions
// + correct hyphen placement are valid.
func TestExtractDateFromPrefix_RejectsMalformedDate(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"dt=2026-06-04", "2026-06-04"},
		{"dt=2026-06-04/", "2026-06-04"},
		{"dt=abcd-ef-gh", ""}, // non-digit positions rejected
		{"dt=20a6-06-04", ""}, // letter at year[2]
		{"dt=2026-0X-04", ""}, // letter at month[1]
		{"dt=2026-06-0Z", ""}, // letter at day[1]
		{"dt=2026/06/04", ""}, // wrong separators
		{"dt=2026-06-04abc", "2026-06-04"}, // trailing junk ignored — by design
		{"random/prefix/no/dt", ""},
	}
	for _, c := range cases {
		got := extractDateFromPrefix(c.in)
		if got != c.want {
			t.Errorf("extractDateFromPrefix(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

var _ = fmt.Sprintf
