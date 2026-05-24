package smartcache

import "fmt"

// ChunkCacheKey identifies a specific column chunk within a Parquet row group.
// It enables column-level caching for selective reads from S3-backed Parquet files.
type ChunkCacheKey struct {
	FileKey  string
	Column   string
	RowGroup int
}

// String returns a canonical string representation of the cache key
// in the format "filekey:column:rowgroup".
func (k ChunkCacheKey) String() string {
	return fmt.Sprintf("%s:%s:%d", k.FileKey, k.Column, k.RowGroup)
}
