package delete

import "context"

// Backend is the bucket as the tombstone protocol sees it: five operations. The S3 implementation is NewS3Backend; tests use an
// in-memory one that models conditional creates and paginated listings.
type Backend interface {
	// PutIfAbsent creates key with data unless it already exists
	// (If-None-Match: *). created is false, with a nil error, when it existed.
	// On a store without conditional writes the implementation falls back to a
	// check followed by a plain write, which is weaker (see S3Backend).
	PutIfAbsent(ctx context.Context, key string, data []byte) (created bool, err error)
	// Get reads key; found is false when it does not exist.
	Get(ctx context.Context, key string) (data []byte, found bool, err error)
	// Exists reports whether key exists (HEAD).
	Exists(ctx context.Context, key string) (bool, error)
	// ListAfter lists the keys under prefix that sort after startAfter (empty: all
	// of them), in key order, page by page.
	ListAfter(ctx context.Context, prefix, startAfter string, fn func(key string) error) error
	// Delete removes key. It is called only by retention GC.
	Delete(ctx context.Context, key string) error
}
