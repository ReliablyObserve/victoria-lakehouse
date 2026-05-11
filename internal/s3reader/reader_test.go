package s3reader

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"connection reset", fmt.Errorf("connection reset by peer"), true},
		{"i/o timeout", fmt.Errorf("i/o timeout"), true},
		{"not found", fmt.Errorf("NoSuchKey: key not found"), false},
		{"generic error", fmt.Errorf("some random error"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryable(tt.err); got != tt.want {
				t.Errorf("isRetryable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRetryS3_Success(t *testing.T) {
	calls := 0
	err := retryS3(context.Background(), 3, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetryS3_RetriesOnRetryableError(t *testing.T) {
	calls := 0
	err := retryS3(context.Background(), 3, func() error {
		calls++
		if calls < 3 {
			return fmt.Errorf("connection reset by peer")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryS3_NoRetryOnNonRetryable(t *testing.T) {
	calls := 0
	err := retryS3(context.Background(), 3, func() error {
		calls++
		return fmt.Errorf("access denied")
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calls != 1 {
		t.Fatalf("expected 1 call (no retry for non-retryable), got %d", calls)
	}
}

func TestRetryS3_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := retryS3(ctx, 3, func() error {
		calls++
		cancel()
		return fmt.Errorf("connection reset by peer")
	})
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call before cancellation, got %d", calls)
	}
}

func TestRetryS3_ExhaustsRetries(t *testing.T) {
	calls := 0
	start := time.Now()
	err := retryS3(context.Background(), 2, func() error {
		calls++
		return fmt.Errorf("i/o timeout")
	})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}
	if calls != 3 { // initial + 2 retries
		t.Fatalf("expected 3 calls, got %d", calls)
	}
	// With maxRetries=2, backoffs are 100ms and 200ms = 300ms minimum
	if elapsed < 250*time.Millisecond {
		t.Fatalf("expected backoff delays, but elapsed was only %v", elapsed)
	}
}
