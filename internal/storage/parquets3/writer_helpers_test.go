package parquets3

import (
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/parquetwriter"
)

// The shared writer's level mapping and bloom column set, under the short names
// the tests of this package use for them (no production code needs them here).
func zstdLevel(level int) zstd.Level { return parquetwriter.ZstdLevel(level) }

func bloomFilters(cols []string) []parquet.BloomFilterColumn { return parquetwriter.BloomFilters(cols) }
