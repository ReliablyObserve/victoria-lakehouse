package delete

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

// S3Backend is the Backend over the pod's S3 client pool.
//
// A create is a PutObject with If-None-Match: *. A store that answers 412 or 409
// (ConditionalRequestConflict) says the key exists. A store that rejects the
// header as not implemented (HTTP 501 or 400 NotImplemented) is switched, for the
// life of the process, to a check followed by a plain write: two pods creating
// the same key at the same instant can then both write it. That is harmless for
// the definition (the bytes of one task are identical on every pod) and for an
// un-delete, and it is why a physical removal never relies on that path. The
// switch is exported as lakehouse_delete_conditional_writes (1: conditional
// writes in use, 0: fallback). A store that silently ignores the header cannot
// be told apart from one that honours it; the s3compat suite checks the stores
// the project supports.
type S3Backend struct {
	pool     *s3reader.ClientPool
	fallback atomic.Bool
}

// NewS3Backend wraps pool.
func NewS3Backend(pool *s3reader.ClientPool) *S3Backend {
	metrics.DeleteConditionalWrites.Set(1)
	return &S3Backend{pool: pool}
}

// PutIfAbsent implements Backend.
func (b *S3Backend) PutIfAbsent(ctx context.Context, key string, data []byte) (bool, error) {
	if !b.fallback.Load() {
		// A 409 ConditionalRequestConflict (AWS: another write to the same key is in
		// flight) says nothing about whether the key exists, so it is retried a few
		// times instead of being read as "already exists", which left the caller
		// reading a key that was not there yet.
		var err error
		for i := 0; i < conflictAttempts; i++ {
			err = b.pool.UploadConditional(ctx, key, data, "")
			if err == nil || !isConditionalConflict(err) {
				break
			}
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(time.Duration(i+1) * conflictBackoff):
			}
		}
		switch {
		case err == nil:
			return true, nil
		case isConditionalConflict(err):
			return false, fmt.Errorf("conditional create of %s kept conflicting: %w", key, err)
		case tenant.IsPreconditionFailed(err):
			return false, nil
		case !isConditionalUnsupported(err):
			return false, err
		}
		if b.fallback.CompareAndSwap(false, true) {
			metrics.DeleteConditionalWrites.Set(0)
			logger.Warnf("the object store does not support conditional writes (If-None-Match); tombstone keys are created with a check followed by a write (%s)", err)
		}
	}
	exists, err := b.pool.Exists(ctx, key)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}
	if err := b.pool.Upload(ctx, key, data); err != nil {
		return false, err
	}
	return true, nil
}

// conflictAttempts and conflictBackoff bound the retries of a create that the
// store answers with 409 ConditionalRequestConflict (see PutIfAbsent).
const conflictAttempts = 5

var conflictBackoff = 100 * time.Millisecond

// isConditionalConflict recognises a 409 ConditionalRequestConflict.
func isConditionalConflict(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) && ae.ErrorCode() == "ConditionalRequestConflict" {
		return true
	}
	var re *smithyhttp.ResponseError
	return errors.As(err, &re) && re.HTTPStatusCode() == http.StatusConflict
}

// isConditionalUnsupported recognises a store that does not implement
// conditional writes.
func isConditionalUnsupported(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NotImplemented", "XNotImplemented":
			return true
		}
	}
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == 501 {
		return true
	}
	return strings.Contains(err.Error(), "NotImplemented")
}

// Get implements Backend.
func (b *S3Backend) Get(ctx context.Context, key string) ([]byte, bool, error) {
	data, _, found, err := b.pool.DownloadWithETag(ctx, key)
	return data, found, err
}

// Exists implements Backend.
func (b *S3Backend) Exists(ctx context.Context, key string) (bool, error) {
	return b.pool.Exists(ctx, key)
}

// ListAfter implements Backend.
func (b *S3Backend) ListAfter(ctx context.Context, prefix, startAfter string, fn func(key string) error) error {
	return b.pool.ListAfter(ctx, prefix, startAfter, func(key string, _ time.Time) error { return fn(key) })
}

// Delete implements Backend.
func (b *S3Backend) Delete(ctx context.Context, key string) error {
	return b.pool.Delete(ctx, key)
}
