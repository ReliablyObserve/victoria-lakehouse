package parquets3

import (
	"bytes"
	"io"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/smartcache"
)

// cacheOnFlush stores column chunk data from a freshly-flushed Parquet file
// into the SmartCache L2 tier. This implements write-through caching: when a
// combined node (role=all) flushes data to S3, the column data is also cached
// locally so subsequent queries avoid an S3 round-trip for recently ingested data.
//
// Design constraints:
//   - No-op when sc is nil (insert-only nodes without SmartCache).
//   - Cache errors are non-fatal: logged but never fail the flush.
//   - Each column chunk in each row group gets its own cache entry keyed by
//     ChunkCacheKey{FileKey, Column, RowGroup}.
func cacheOnFlush(sc *smartcache.Controller, fileKey string, data []byte) {
	if sc == nil {
		return
	}
	if len(data) == 0 {
		return
	}

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		logger.Warnf("cacheOnFlush: failed to open parquet for caching; key=%s err=%v", fileKey, err)
		return
	}

	rowGroups := f.RowGroups()
	allCols := f.Schema().Columns()

	for rgIdx, rg := range rowGroups {
		chunks := rg.ColumnChunks()
		for colIdx, path := range allCols {
			if colIdx >= len(chunks) {
				continue
			}
			colName := path[0]

			chunkData, err := readColumnChunkBytes(chunks[colIdx])
			if err != nil {
				logger.Warnf("cacheOnFlush: failed to read column chunk; key=%s col=%s rg=%d err=%v",
					fileKey, colName, rgIdx, err)
				continue
			}
			if len(chunkData) == 0 {
				continue
			}

			cacheKey := smartcache.ChunkCacheKey{
				FileKey:  fileKey,
				Column:   colName,
				RowGroup: rgIdx,
			}
			if err := sc.PutL2(cacheKey.String(), chunkData); err != nil {
				logger.Warnf("cacheOnFlush: L2 put failed; key=%s err=%v", cacheKey.String(), err)
			}
		}
	}
}

// readColumnChunkBytes reads all page data from a column chunk into a byte slice.
func readColumnChunkBytes(chunk parquet.ColumnChunk) ([]byte, error) {
	pages := chunk.Pages()
	defer func() { _ = pages.Close() }()

	var buf bytes.Buffer
	for {
		page, err := pages.ReadPage()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		// Read the page data via encoding.Values.
		ev := page.Data()
		pageData, _ := ev.Data()
		buf.Write(pageData)
	}
	return buf.Bytes(), nil
}
