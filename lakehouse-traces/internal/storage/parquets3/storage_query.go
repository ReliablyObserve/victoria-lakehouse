package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage"
)

func sortFilesByCacheAffinity(files []manifest.FileInfo, cachedKeys map[string]bool) {
	sort.SliceStable(files, func(i, j int) bool {
		iCached := cachedKeys[files[i].Key]
		jCached := cachedKeys[files[j].Key]
		if iCached != jCached {
			return iCached
		}
		return false
	})
}

func (s *Storage) applySelfFilter(files []manifest.FileInfo) []manifest.FileInfo {
	if !s.selfFilterEnabled || s.smartCache == nil {
		return files
	}
	var owned []manifest.FileInfo
	for _, f := range files {
		if _, isLocal := s.smartCache.LookupOwner(f.Key); isLocal {
			owned = append(owned, f)
		}
	}
	if len(owned) > 0 {
		return owned
	}
	return files
}

func (s *Storage) applyCacheAffinity(files []manifest.FileInfo) {
	if s.footerCache == nil {
		return
	}
	cachedKeys := make(map[string]bool, len(files))
	for _, f := range files {
		if s.footerCache.Has(f.Key) {
			cachedKeys[f.Key] = true
		}
	}
	if len(cachedKeys) > 0 && len(cachedKeys) < len(files) {
		sortFilesByCacheAffinity(files, cachedKeys)
	}
}

func (s *Storage) manifestFastPath(files []manifest.FileInfo, startNs, endNs int64, writeBlock logstorage.WriteDataBlockFunc) []manifest.FileInfo {
	var remaining []manifest.FileInfo
	for _, fi := range files {
		if fi.RowCount > 0 && fi.MinTimeNs > 0 && fi.MaxTimeNs > 0 &&
			fi.MinTimeNs >= startNs && fi.MaxTimeNs <= endNs {
			db := s.syntheticManifestBlock(fi)
			if db != nil && db.RowsCount() > 0 {
				writeBlock(0, db)
				metrics.MetadataOnlyFiles.Inc()
			}
		} else {
			remaining = append(remaining, fi)
		}
	}
	if len(remaining) < len(files) {
		logger.Infof("metadata fast path: resolved %d/%d files from manifest, %d remain for S3",
			len(files)-len(remaining), len(files), len(remaining))
	}
	return remaining
}

func (s *Storage) syntheticManifestBlock(fi manifest.FileInfo) *logstorage.DataBlock {
	const maxSyntheticRows = 50_000_000
	n := int(fi.RowCount)
	if n == 0 {
		return nil
	}
	if n > maxSyntheticRows {
		n = maxSyntheticRows
	}

	tsCol := s.registry.TimestampColumn()
	internalName := tsCol
	if m := s.registry.ResolveFromParquet(tsCol); m != nil {
		internalName = m.InternalName
	}

	values := make([]string, n)
	if n == 1 {
		values[0] = s.registry.FormatField(internalName, fi.MinTimeNs)
	} else {
		step := (fi.MaxTimeNs - fi.MinTimeNs) / int64(n-1)
		if step == 0 {
			step = 1
		}
		for i := range values {
			ts := fi.MinTimeNs + int64(i)*step
			if ts > fi.MaxTimeNs {
				ts = fi.MaxTimeNs
			}
			values[i] = s.registry.FormatField(internalName, ts)
		}
	}

	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{{Name: internalName, Values: values}})
	return db
}

func (s *Storage) RunQuery(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	queryStart := time.Now()
	metrics.ConcurrentSelects.Inc()
	defer func() {
		metrics.ConcurrentSelects.Dec()
		elapsed := time.Since(queryStart).Seconds()
		metrics.QueryDuration.Observe(elapsed)
	}()

	startNs, endNs := q.GetFilterTimeRange()

	if boundary := s.discovery.GetHotBoundary(); boundary != nil {
		if time.Unix(0, startNs).After(boundary.MinTime) && time.Unix(0, endNs).Before(boundary.MaxTime) {
			logger.Infof("hot boundary suppression: query within hot range; start=%v, end=%v, hot_min=%v, hot_max=%v",
				time.Unix(0, startNs), time.Unix(0, endNs), boundary.MinTime, boundary.MaxTime)
			return nil
		}
	}

	if !s.manifest.HasDataForRange(startNs, endNs) {
		metrics.ManifestFastPathTotal.Inc()
		logger.Infof("manifest fast path: no data for range; start=%v, end=%v",
			time.Unix(0, startNs), time.Unix(0, endNs))
		return nil
	}

	queryStr := q.String()
	pipeFields := logstorage.GetQueryPipeFields(q)
	filter := parseFilterFromQuery(q)

	var rowsEmitted atomic.Int64
	maxRows := s.cfg.Query.MaxRows

	// Wrap writeBlock to apply LogsQL filter evaluation, tombstone filtering,
	// max_rows enforcement, and panic recovery.
	// Pre-filter runs in each worker goroutine without locks.
	// Only the final writeBlock call is serialized.
	var writeBlockPanic atomic.Bool
	preFilter := func(db *logstorage.DataBlock) *logstorage.DataBlock {
		if writeBlockPanic.Load() {
			return nil
		}
		if maxRows > 0 && rowsEmitted.Load() >= maxRows {
			cancel()
			return nil
		}
		db = filterDataBlock(db, filter)
		if db == nil || db.RowsCount() == 0 {
			return nil
		}
		if s.tombstones != nil {
			db = s.filterTombstonedRows(db, startNs, endNs)
			if db == nil || db.RowsCount() == 0 {
				return nil
			}
		}
		return db
	}

	var wbMu sync.Mutex
	filteredWriteBlock := func(workerID uint, db *logstorage.DataBlock) {
		db = preFilter(db)
		if db == nil {
			return
		}
		rowsEmitted.Add(int64(db.RowsCount()))
		wbMu.Lock()
		func() {
			defer func() {
				if r := recover(); r != nil {
					writeBlockPanic.Store(true)
					logger.Warnf("writeBlock panic recovered (unsupported pipe in query): %v", r)
				}
			}()
			writeBlock(workerID, db)
		}()
		wbMu.Unlock()
	}

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		return nil
	}

	files = s.applySelfFilter(files)
	s.applyCacheAffinity(files)

	hasTombstones := s.tombstones != nil && len(s.tombstones.ForRange(startNs, endNs)) > 0
	if storage.IsTimestampOnly(ctx) && filter == nil && !hasTombstones {
		remaining := s.manifestFastPath(files, startNs, endNs, filteredWriteBlock)
		if len(remaining) == 0 {
			if n := rowsEmitted.Load(); n > 0 {
				metrics.QueryRowsTotal.Add(int(n))
			}
			return nil
		}
		files = remaining
	}

	files = s.preFilterFiles(files, queryStr)
	if len(files) == 0 {
		return nil
	}

	prefetchFooters(ctx, s.pool, files, s.footerCache, 0)

	// Parallel file worker pool
	fileWorkers := s.cfg.Query.FileWorkers
	if fileWorkers <= 0 {
		fileWorkers = 8
	}
	if fileWorkers > len(files) {
		fileWorkers = len(files)
	}

	queryID := fmt.Sprintf("q-%d", queryStart.UnixNano())

	// Pin all files in smart cache before query, defer unpin
	if s.smartCache != nil {
		for _, fi := range files {
			s.smartCache.Pin(fi.Key, queryID)
		}
		defer func() {
			for _, fi := range files {
				s.smartCache.Unpin(fi.Key, queryID)
			}
		}()
	}

	taskCh := make(chan manifest.FileInfo, len(files))
	for _, fi := range files {
		taskCh <- fi
	}
	close(taskCh)

	var wg sync.WaitGroup
	var firstErr atomic.Value

	for i := 0; i < fileWorkers; i++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			for fi := range taskCh {
				if err := ctx.Err(); err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
				if maxRows > 0 && rowsEmitted.Load() >= maxRows {
					return
				}
				if skip, _ := shouldSkipByFooter(ctx, s.pool, fi, queryStr, s.registry, s.footerCache); skip {
					continue
				}
				if s.checkFileBloom(ctx, fi, queryStr) {
					continue
				}
				if err := s.queryFile(ctx, fi, startNs, endNs, queryStr, pipeFields, filteredWriteBlock); err != nil {
					if isFileNotFoundError(err) {
						s.handle404Recovery(ctx, fi, filter, hasTombstones, filteredWriteBlock)
					} else {
						metrics.QueryFileErrorsTotal.Inc()
						logger.Warnf("query file error: %s; key=%s", err, fi.Key)
					}
					continue
				}
			}
		}(i)
	}
	wg.Wait()

	if v := firstErr.Load(); v != nil {
		if err, ok := v.(error); ok {
			if maxRows > 0 && rowsEmitted.Load() >= maxRows {
				// Cancelled due to maxRows — not an error.
			} else {
				return err
			}
		}
	}

	s.queryBufferBridge(ctx, startNs, endNs, filteredWriteBlock)

	return nil
}

func (s *Storage) preFilterFiles(files []manifest.FileInfo, queryStr string) []manifest.FileInfo {
	if s.smartCache != nil {
		if tid := extractExactMatch(queryStr, "trace_id"); tid != "" {
			if cached := s.smartCache.FindFilesByTraceID(tid); len(cached) > 0 {
				cacheSet := make(map[string]bool, len(cached))
				for _, k := range cached {
					cacheSet[k] = true
				}
				var narrowed []manifest.FileInfo
				for _, fi := range files {
					if cacheSet[fi.Key] {
						narrowed = append(narrowed, fi)
					}
				}
				if len(narrowed) > 0 {
					logger.Infof("trace_id fast-path: cache hit for %s, scanning %d files", tid, len(narrowed))
					return narrowed
				}
			}
		}
	}

	files = s.filterFilesByLabels(files, queryStr)
	if len(files) == 0 {
		return nil
	}

	files = s.filterFilesByBloomIndex(files, queryStr)
	return files
}

func (s *Storage) queryBufferBridge(ctx context.Context, startNs, endNs int64, filteredWriteBlock logstorage.WriteDataBlockFunc) {
	if s.bufferBridge == nil {
		return
	}
	switch s.cfg.Mode {
	case config.ModeLogs:
		bufRows, _ := s.bufferBridge.QueryLogs(ctx, startNs, endNs)
		if len(bufRows) > 0 {
			db := s.logRowsToDataBlock(bufRows)
			if db != nil && db.RowsCount() > 0 {
				filteredWriteBlock(0, db)
			}
		}
	case config.ModeTraces:
		bufRows, _ := s.bufferBridge.QueryTraces(ctx, startNs, endNs)
		if len(bufRows) > 0 {
			db := s.traceRowsToDataBlock(bufRows)
			if db != nil && db.RowsCount() > 0 {
				filteredWriteBlock(0, db)
			}
		}
	}
}

// openParquetFile returns a parquet.File for the given FileInfo.
// When a cached footer is available and the query projects few columns,
// it uses S3ReaderAt so parquet-go fetches only the needed column chunks
// via HTTP range requests. Falls back to full download on any error.
func (s *Storage) openParquetFile(ctx context.Context, fi manifest.FileInfo, projectedCols map[string]bool) (*parquet.File, error) {
	// Range-read path: requires footer cache (to know total column count)
	// and a non-empty projection that covers fewer than half the columns.
	if s.footerCache != nil && projectedCols != nil && s.pool != nil {
		if cached, ok := s.footerCache.Get(fi.Key); ok {
			totalCols := len(cached.File.Root().Columns())
			if shouldUseRangeRead(fi.Size, len(projectedCols), totalCols) {
				rawReader := s.pool.NewReaderAt(ctx, fi.Key, fi.Size)
				buffered := s3reader.NewBufferedReaderAt(rawReader, fi.Size, int64(s.cfg.S3.ReadAheadBytes))
				readerAt := s3reader.NewCoalescingReaderAt(buffered, fi.Size, int64(s.cfg.S3.CoalesceGapBytes))
				f, err := parquet.OpenFile(readerAt, fi.Size)
				if err == nil {
					metrics.S3RangeReadsTotal.Inc()
					return f, nil
				}
				// Fall through to full download on error.
			}
		}
	}

	// Full download path (existing behaviour).
	data, err := s.getFileData(ctx, fi.Key, fi.Size)
	if err != nil {
		return nil, fmt.Errorf("get file data %s: %w", fi.Key, err)
	}

	metrics.ParquetFilesOpened.Inc()
	metrics.ParquetColumnBytesRead.Add(len(data))

	if s.footerCache != nil {
		if cached, ok := s.footerCache.Get(fi.Key); ok && cached.FileSize == int64(len(data)) {
			return cached.File, nil
		}
	}

	if s.footerCache != nil {
		cached, f, parseErr := ParseFooterFromData(fi.Key, data)
		if parseErr != nil {
			return nil, parseErr
		}
		s.footerCache.Put(fi.Key, cached)
		return f, nil
	}

	f, parseErr := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if parseErr != nil {
		return nil, fmt.Errorf("open parquet file %s: %w", fi.Key, parseErr)
	}
	return f, nil
}

func (s *Storage) queryFile(ctx context.Context, fi manifest.FileInfo, startNs, endNs int64, queryStr string, pipeFields []string, writeBlock logstorage.WriteDataBlockFunc) error {
	projectedCols := queryColumns(queryStr, s.registry, pipeFields)

	if projectedCols == nil && storage.IsTimestampOnly(ctx) {
		projectedCols = map[string]bool{s.registry.TimestampColumn(): true}
	}

	f, err := s.openParquetFile(ctx, fi, projectedCols)
	if err != nil {
		return err
	}

	if s.labelIndex.Len() == 0 {
		s.updateLabelIndex(f)
	}
	s.updateColumnStats(fi.Key, f)

	tsIdx := findColumnIndex(f.Root(), s.registry.TimestampColumn())
	bloomChecks := resolveBloomCheckIndices(f, s.buildBloomChecks(queryStr))
	pdf := resolvePushDownIndices(f, buildPushDownFilter(queryStr, s.registry))

	var collectedTraceIDs []string
	var traceIDsPtr *[]string
	if s.smartCache != nil {
		traceIDsPtr = &collectedTraceIDs
	}

	rowGroups := f.RowGroups()

	// Extract file-level key-value metadata for token bloom checks.
	searchTokens := extractSearchTokens(queryStr)
	var fileKVMeta map[string]string
	if len(searchTokens) > 0 {
		if meta := f.Metadata(); meta != nil {
			fileKVMeta = make(map[string]string, len(meta.KeyValueMetadata))
			for _, kv := range meta.KeyValueMetadata {
				fileKVMeta[kv.Key] = kv.Value
			}
		}
	}

	// Pre-filter row groups using metadata (time range, bloom, pushdown, token bloom).
	var matchedRGs []parquet.RowGroup
	for rgIdx, rg := range rowGroups {
		if tsIdx >= 0 && !rowGroupMatchesTimeRange(rg, tsIdx, startNs, endNs) {
			metrics.ParquetRowGroupsSkipped.Inc("stats")
			continue
		}
		if s.bloomFilterSkip(f, rg, bloomChecks) {
			metrics.ParquetRowGroupsSkipped.Inc("bloom")
			continue
		}
		if pdf != nil && !rowGroupMatchesFilter(f, rg, pdf) {
			metrics.ParquetRowGroupsSkipped.Inc("pushdown")
			continue
		}
		if tokenBloomSkip(fileKVMeta, rgIdx, searchTokens) {
			metrics.ParquetRowGroupsSkipped.Inc("token_bloom")
			continue
		}
		matchedRGs = append(matchedRGs, rg)
	}

	sort.Slice(matchedRGs, func(i, j int) bool {
		return matchedRGs[i].NumRows() < matchedRGs[j].NumRows()
	})

	// Process matched row groups — parallel when >1 to reduce per-file latency.
	if len(matchedRGs) <= 1 {
		for _, rg := range matchedRGs {
			metrics.ParquetRowGroupsScanned.Inc()
			if err := s.readOneRowGroup(f, rg, startNs, endNs, projectedCols, pdf, writeBlock, traceIDsPtr); err != nil {
				return err
			}
		}
	} else {
		rgWorkers := len(matchedRGs)
		if rgWorkers > 8 {
			rgWorkers = 8
		}
		rgCh := make(chan parquet.RowGroup, len(matchedRGs))
		for _, rg := range matchedRGs {
			rgCh <- rg
		}
		close(rgCh)

		var rgWg sync.WaitGroup
		var rgErr atomic.Value
		for i := 0; i < rgWorkers; i++ {
			rgWg.Add(1)
			go func() {
				defer rgWg.Done()
				for rg := range rgCh {
					if ctx.Err() != nil {
						return
					}
					metrics.ParquetRowGroupsScanned.Inc()
					if err := s.readOneRowGroup(f, rg, startNs, endNs, projectedCols, pdf, writeBlock, traceIDsPtr); err != nil {
						rgErr.CompareAndSwap(nil, err)
						return
					}
				}
			}()
		}
		rgWg.Wait()
		if v := rgErr.Load(); v != nil {
			if err, ok := v.(error); ok {
				return err
			}
		}
	}

	if s.smartCache != nil && len(collectedTraceIDs) > 0 {
		s.smartCache.RecordTraceIDs(fi.Key, collectedTraceIDs)
	}

	if s.crossSignalClient != nil && len(collectedTraceIDs) > 0 {
		s.crossSignalClient.EnqueueHint(collectedTraceIDs, startNs, endNs, string(s.cfg.Mode))
	}

	return nil
}

func (s *Storage) readOneRowGroup(f *parquet.File, rg parquet.RowGroup, startNs, endNs int64, projectedCols map[string]bool, pdf *PushDownFilter, writeBlock logstorage.WriteDataBlockFunc, traceIDs *[]string) error {
	if projectedCols == nil {
		projectedCols = allLeafColumns(f)
	}
	return s.readRowGroupWithProjection(f, rg, startNs, endNs, projectedCols, pdf, writeBlock, traceIDs)
}

func allLeafColumns(f *parquet.File) map[string]bool {
	cols := make(map[string]bool)
	for _, path := range f.Schema().Columns() {
		cols[path[0]] = true
	}
	return cols
}

func (s *Storage) readRowGroup(f *parquet.File, rg parquet.RowGroup, startNs, endNs int64, writeBlock logstorage.WriteDataBlockFunc, traceIDs *[]string) error {
	if s.cfg.Mode == config.ModeTraces {
		return readRowGroupTyped[schema.TraceRow](s, f, rg, startNs, endNs, writeBlock, traceIDs, traceRowToFields)
	}
	return readRowGroupTyped[schema.LogRow](s, f, rg, startNs, endNs, writeBlock, traceIDs, logRowToFields)
}

func readRowGroupTyped[T any](s *Storage, f *parquet.File, rg parquet.RowGroup, startNs, endNs int64, writeBlock logstorage.WriteDataBlockFunc, traceIDs *[]string, toFields func(*T, []field) []field) error {
	reader := parquet.NewGenericRowGroupReader[T](rg)
	buf := make([]T, 256)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			db := typedRowsToDataBlock(s, buf[:n], startNs, endNs, toFields)
			if db != nil && db.RowsCount() > 0 {
				writeBlock(0, db)
				if traceIDs != nil {
					extractTraceIDs(db, traceIDs)
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}
	return nil
}

func (s *Storage) readRowGroupWithProjection(f *parquet.File, rg parquet.RowGroup, startNs, endNs int64, cols map[string]bool, pdf *PushDownFilter, writeBlock logstorage.WriteDataBlockFunc, traceIDs *[]string) error {
	constants := detectConstantColumns(f, rg, cols)

	readCols := cols
	if len(constants) > 0 {
		readCols = make(map[string]bool, len(cols))
		for k, v := range cols {
			readCols[k] = v
		}
		for _, cc := range constants {
			delete(readCols, cc.name)
		}
	}

	bitmap := prewhereFilter(f, rg, pdf)

	// Fast path: columnar reading when no constant columns need merging.
	if len(constants) == 0 && len(readCols) > 0 {
		db := readRowGroupColumnar(f, rg, readCols, s.registry, startNs, endNs, bitmap)
		if db != nil && db.RowsCount() > 0 {
			writeBlock(0, db)
			if traceIDs != nil {
				extractTraceIDs(db, traceIDs)
			}
		}
		return nil
	}

	// Slow path: row-oriented reading with constant column merging.
	var allFields [][]field
	if len(readCols) > 0 {
		var err error
		allFields, err = readRowGroupProjectedBitmap(f, rg, readCols, bitmap)
		if err != nil {
			return err
		}
	}

	if len(constants) > 0 {
		if allFields == nil {
			n := int(rg.NumRows())
			if bitmap != nil {
				n = 0
				for _, b := range bitmap {
					if b {
						n++
					}
				}
			}
			allFields = make([][]field, n)
		}
		for i := range allFields {
			for _, cc := range constants {
				allFields[i] = append(allFields[i], field(cc))
			}
		}
	}

	db := s.projectedFieldsToDataBlock(allFields, startNs, endNs)
	if db != nil && db.RowsCount() > 0 {
		writeBlock(0, db)
		if traceIDs != nil {
			extractTraceIDs(db, traceIDs)
		}
	}
	return nil
}

func (s *Storage) projectedFieldsToDataBlock(rows [][]field, startNs, endNs int64) *logstorage.DataBlock {
	if len(rows) == 0 {
		return nil
	}

	type colData struct {
		name   string
		values []string
	}
	colMap := make(map[string]int)
	var cols []colData

	getCol := func(name string) int {
		if idx, ok := colMap[name]; ok {
			return idx
		}
		idx := len(cols)
		colMap[name] = idx
		cols = append(cols, colData{name: name, values: make([]string, 0, len(rows))})
		return idx
	}

	rowNum := 0
	var seenBitmap []bool
	for _, fields := range rows {
		// Time-range filter
		skip := false
		for _, fld := range fields {
			if fld.name == "timestamp_unix_nano" {
				if ts, ok := fld.value.(int64); ok {
					if ts < startNs || ts > endNs {
						skip = true
						break
					}
				}
			}
		}
		if skip {
			continue
		}

		if cap(seenBitmap) >= len(cols) {
			seenBitmap = seenBitmap[:len(cols)]
		} else {
			seenBitmap = make([]bool, len(cols), len(cols)*2)
		}
		for i := range seenBitmap {
			seenBitmap[i] = false
		}

		scalarFieldNames := make(map[string]bool)
		for _, fld := range fields {
			if _, ok := fld.value.(map[string]string); !ok {
				scalarFieldNames[fld.name] = true
			}
		}

		for _, fld := range fields {
			if mapVal, ok := fld.value.(map[string]string); ok {
				prefix := mapColumnToAttrPrefix(fld.name)
				for k, v := range mapVal {
					if v == "" {
						continue
					}
					var effectivePrefix string
					if schema.VTTopLevelSpanAttrKeys[k] {
						effectivePrefix = ""
					} else {
						if scalarFieldNames[k] {
							continue
						}
						effectivePrefix = prefix
					}
					attrName := bytesutil.InternString(effectivePrefix + k)
					idx := getCol(attrName)
					for idx >= len(seenBitmap) {
						seenBitmap = append(seenBitmap, false)
					}
					if seenBitmap[idx] {
						continue
					}
					seenBitmap[idx] = true
					for len(cols[idx].values) < rowNum {
						cols[idx].values = append(cols[idx].values, "")
					}
					cols[idx].values = append(cols[idx].values, v)
				}
				continue
			}

			internalName := fld.name
			if m := s.registry.ResolveFromParquet(fld.name); m != nil {
				internalName = m.InternalName
			}

			formatted := s.registry.FormatField(internalName, fld.value)
			if formatted == "" {
				continue
			}
			idx := getCol(internalName)
			for idx >= len(seenBitmap) {
				seenBitmap = append(seenBitmap, false)
			}
			if seenBitmap[idx] {
				continue
			}
			seenBitmap[idx] = true
			for len(cols[idx].values) < rowNum {
				cols[idx].values = append(cols[idx].values, "")
			}
			cols[idx].values = append(cols[idx].values, formatted)
		}

		// Fill empty for columns not present in this row
		for i := range cols {
			if i < len(seenBitmap) && !seenBitmap[i] && len(cols[i].values) <= rowNum {
				cols[i].values = append(cols[i].values, "")
			} else if i >= len(seenBitmap) && len(cols[i].values) <= rowNum {
				cols[i].values = append(cols[i].values, "")
			}
		}
		rowNum++
	}

	if len(cols) == 0 {
		return nil
	}

	blockCols := make([]logstorage.BlockColumn, 0, len(cols))
	seen := make(map[string]bool, len(cols))
	for _, col := range cols {
		if seen[col.name] {
			continue
		}
		seen[col.name] = true
		blockCols = append(blockCols, logstorage.BlockColumn{
			Name:   col.name,
			Values: col.values,
		})
	}

	db := &logstorage.DataBlock{}
	db.SetColumns(blockCols)
	return db
}

func mapColumnToAttrPrefix(col string) string {
	switch col {
	case "resource.attributes":
		return "resource_attr:"
	case "log.attributes":
		return "log_attr:"
	case "span.attributes":
		return "span_attr:"
	case "scope.attributes":
		return "scope_attr:"
	default:
		return col + ":"
	}
}

type field struct {
	name  string
	value any
}

func logRowToFields(r *schema.LogRow, buf []field) []field {
	buf = append(buf,
		field{"_time", r.TimestampUnixNano},
		field{"_msg", r.Body},
		field{"level", r.SeverityText},
		field{"severity_number", r.SeverityNumber},
		field{"service.name", r.ServiceName},
		field{"k8s.namespace.name", r.K8sNamespaceName},
		field{"k8s.pod.name", r.K8sPodName},
		field{"k8s.deployment.name", r.K8sDeploymentName},
		field{"k8s.node.name", r.K8sNodeName},
		field{"deployment.environment", r.DeployEnv},
		field{"cloud.region", r.CloudRegion},
		field{"host.name", r.HostName},
		field{"trace_id", r.TraceID},
		field{"span_id", r.SpanID},
		field{"_stream", r.Stream},
		field{"_stream_id", r.StreamID},
		field{"scope.name", r.ScopeName},
	)
	for k, v := range r.ResourceAttributes {
		buf = append(buf, field{k, v})
	}
	for k, v := range r.LogAttributes {
		buf = append(buf, field{k, v})
	}
	return buf
}

func traceRowToFields(r *schema.TraceRow, buf []field) []field {
	buf = append(buf,
		field{"_time", r.TimestampUnixNano},
		field{"start_time", r.StartTimeUnixNano},
		field{"trace_id", r.TraceID},
		field{"span_id", r.SpanID},
		field{"parent_span_id", r.ParentSpanID},
		field{"name", r.SpanName},
		field{"kind", r.SpanKind},
		field{"status_code", r.StatusCode},
		field{"status_message", r.StatusMessage},
		field{"duration", r.DurationNs},
		field{"resource_attr:service.name", r.ServiceName},
		field{"scope_name", r.ScopeName},
		field{"resource_attr:deployment.environment", r.DeployEnv},
		field{"resource_attr:cloud.region", r.CloudRegion},
		field{"resource_attr:host.name", r.HostName},
		field{"resource_attr:k8s.namespace.name", r.K8sNamespaceName},
		field{"resource_attr:k8s.deployment.name", r.K8sDeploymentName},
		field{"resource_attr:k8s.node.name", r.K8sNodeName},
		field{"span_attr:http.method", r.HTTPMethod},
		field{"span_attr:http.status_code", r.HTTPStatusCode},
		field{"span_attr:http.url", r.HTTPUrl},
		field{"span_attr:db.system", r.DBSystem},
		field{"span_attr:db.statement", r.DBStatement},
	)
	for k, v := range r.ResourceAttributes {
		if !tracePromotedResourceKeys[k] {
			buf = append(buf, field{k, v})
		}
	}
	for k, v := range r.SpanAttributes {
		if tracePromotedSpanKeys[k] {
			continue
		}
		buf = append(buf, field{k, v})
	}
	for k, v := range r.ScopeAttributes {
		buf = append(buf, field{k, v})
	}
	return buf
}

var tracePromotedResourceKeys = map[string]bool{
	"service.name":           true,
	"deployment.environment": true,
	"cloud.region":           true,
	"host.name":              true,
	"k8s.namespace.name":     true,
	"k8s.deployment.name":    true,
	"k8s.node.name":          true,
	"k8s.pod.name":           true,
}

var tracePromotedSpanKeys = map[string]bool{
	"http.method":      true,
	"http.status_code": true,
	"http.url":         true,
	"db.system":        true,
	"db.statement":     true,
}

func typedRowsToDataBlock[T any](s *Storage, rows []T, startNs, endNs int64, toFields func(*T, []field) []field) *logstorage.DataBlock {
	if len(rows) == 0 {
		return nil
	}

	type colData struct {
		name   string
		values []string
	}
	colMap := make(map[string]int)
	var cols []colData

	getCol := func(name string) int {
		if idx, ok := colMap[name]; ok {
			return idx
		}
		idx := len(cols)
		colMap[name] = idx
		cols = append(cols, colData{name: name, values: make([]string, 0, len(rows))})
		for len(cols[idx].values) < len(cols[0].values)-1 {
			cols[idx].values = append(cols[idx].values, "")
		}
		return idx
	}

	var seenBitmap []bool
	var fieldBuf []field
	for rowNum, row := range rows {
		fieldBuf = toFields(&row, fieldBuf[:0])
		fields := fieldBuf

		if cap(seenBitmap) >= len(cols) {
			seenBitmap = seenBitmap[:len(cols)]
		} else {
			seenBitmap = make([]bool, len(cols), len(cols)*2)
		}
		for i := range seenBitmap {
			seenBitmap[i] = false
		}

		for _, f := range fields {
			formatted := s.registry.FormatField(f.name, f.value)
			if formatted == "" {
				continue
			}
			idx := getCol(f.name)
			for idx >= len(seenBitmap) {
				seenBitmap = append(seenBitmap, false)
			}
			if seenBitmap[idx] {
				continue
			}
			seenBitmap[idx] = true
			cols[idx].values = append(cols[idx].values, formatted)
		}

		for i := range cols {
			if i < len(seenBitmap) && !seenBitmap[i] && len(cols[i].values) <= rowNum {
				cols[i].values = append(cols[i].values, "")
			} else if i >= len(seenBitmap) && len(cols[i].values) <= rowNum {
				cols[i].values = append(cols[i].values, "")
			}
		}
	}

	if len(cols) == 0 {
		return nil
	}

	blockCols := make([]logstorage.BlockColumn, 0, len(cols))
	seen := make(map[string]bool, len(cols))
	for _, col := range cols {
		if seen[col.name] {
			continue
		}
		seen[col.name] = true
		blockCols = append(blockCols, logstorage.BlockColumn{
			Name:   col.name,
			Values: col.values,
		})
	}

	db := &logstorage.DataBlock{}
	db.SetColumns(blockCols)
	return db
}

// extractTraceIDs collects unique, non-empty trace_id values from a DataBlock
// into the destination slice, capped at 200 entries.
func extractTraceIDs(db *logstorage.DataBlock, dest *[]string) {
	cols := db.GetColumns(false)
	for _, col := range cols {
		if col.Name != "trace_id" {
			continue
		}
		seen := make(map[string]bool)
		for _, v := range col.Values {
			if v != "" && !seen[v] && len(*dest) < 200 {
				seen[v] = true
				*dest = append(*dest, v)
			}
		}
		return
	}
}

func (s *Storage) projectColumns(allCols []string, requested []string) []int {
	if len(requested) == 0 {
		indices := make([]int, len(allCols))
		for i := range indices {
			indices[i] = i
		}
		return indices
	}

	want := make(map[string]bool, len(requested))
	for _, name := range requested {
		want[name] = true
		if m := s.registry.ResolveToParquet(name); m != nil {
			want[m.ParquetColumn] = true
		}
	}
	want["timestamp_unix_nano"] = true

	var indices []int
	for i, name := range allCols {
		internalName := name
		if m := s.registry.ResolveFromParquet(name); m != nil {
			internalName = m.InternalName
		}
		if want[name] || want[internalName] {
			indices = append(indices, i)
		}
	}
	if len(indices) == 0 {
		indices = make([]int, len(allCols))
		for i := range indices {
			indices[i] = i
		}
	}
	return indices
}

type bloomCheck struct {
	colName string
	colIdx  int
	value   parquet.Value
}

func (s *Storage) buildBloomChecks(queryStr string) []bloomCheck {
	if queryStr == "" {
		return nil
	}

	var checks []bloomCheck
	for _, col := range s.registry.PromotedColumns() {
		if !col.HasBloom {
			continue
		}
		vals := extractFilterValues(queryStr, col.InternalName)
		if len(vals) == 0 {
			vals = extractFilterValues(queryStr, col.ParquetColumn)
		}
		for _, val := range vals {
			checks = append(checks, bloomCheck{
				colName: col.ParquetColumn,
				value:   parquet.ValueOf(val),
			})
		}
	}
	return checks
}

func resolveBloomCheckIndices(f *parquet.File, checks []bloomCheck) []bloomCheck {
	resolved := make([]bloomCheck, 0, len(checks))
	for _, check := range checks {
		idx := findColumnIndex(f.Root(), check.colName)
		if idx >= 0 {
			resolved = append(resolved, bloomCheck{
				colName: check.colName,
				colIdx:  idx,
				value:   check.value,
			})
		}
	}
	return resolved
}

func (s *Storage) bloomFilterSkip(_ *parquet.File, rg parquet.RowGroup, checks []bloomCheck) bool {
	if len(checks) == 0 {
		return false
	}

	cols := rg.ColumnChunks()
	for _, check := range checks {
		if check.colIdx >= len(cols) {
			continue
		}

		bf := cols[check.colIdx].BloomFilter()
		if bf == nil || bf.Size() == 0 {
			continue
		}

		found, err := bf.Check(check.value)
		if err != nil {
			continue
		}
		if !found {
			return true
		}
	}
	return false
}

func extractInValues(query, fieldName string) []string {
	prefix := fieldName + `:in(`
	idx := strings.Index(query, prefix)
	if idx < 0 {
		return nil
	}
	start := idx + len(prefix)
	end := strings.Index(query[start:], ")")
	if end < 0 {
		return nil
	}
	inner := query[start : start+end]

	var vals []string
	for _, part := range strings.Split(inner, ",") {
		part = strings.TrimSpace(part)
		part = strings.Trim(part, `"`)
		if part != "" {
			vals = append(vals, part)
		}
	}
	return vals
}

func extractFilterValues(query, fieldName string) []string {
	if vals := extractInValues(query, fieldName); len(vals) > 0 {
		return vals
	}
	if val := extractExactMatch(query, fieldName); val != "" {
		return []string{val}
	}
	return nil
}

func extractExactMatch(query, fieldName string) string {
	// Quoted patterns: trace_id:="abc" or trace_id:"abc"
	quotedPatterns := []string{
		fieldName + `:="`,
		fieldName + `:"`,
	}
	for _, prefix := range quotedPatterns {
		idx := strings.Index(query, prefix)
		if idx < 0 {
			continue
		}
		start := idx + len(prefix)
		end := strings.Index(query[start:], `"`)
		if end < 0 {
			continue
		}
		return query[start : start+end]
	}

	// Unquoted pattern: trace_id:=abc123 (produced by q.String())
	unquotedPrefix := fieldName + `:=`
	if idx := strings.Index(query, unquotedPrefix); idx >= 0 {
		start := idx + len(unquotedPrefix)
		if start < len(query) && query[start] == '"' {
			return ""
		}
		end := strings.IndexAny(query[start:], " |)")
		if end < 0 {
			return query[start:]
		}
		return query[start : start+end]
	}

	return ""
}

// filterFilesByLabels uses manifest-level labels to skip files that definitely
// don't contain the queried values. This avoids downloading files from S3.
func (s *Storage) filterFilesByLabels(files []manifest.FileInfo, queryStr string) []manifest.FileInfo {
	pdf := buildPushDownFilter(queryStr, s.registry)
	if pdf == nil || len(pdf.Checks) == 0 {
		return files
	}

	if result := s.filterByLabelIndex(files, pdf); result != nil {
		return result
	}

	// Column stats pre-filter: skip files where exact-match value is outside [min, max].
	// This avoids downloading files that can't possibly contain the queried value.
	statsFiltered := files[:0]
	statsSkipped := 0
	for _, fi := range files {
		skip := false
		for _, check := range pdf.Checks {
			if check.Op == PushDownExact && !fi.ColumnStatsContains(check.Column, check.Value) {
				skip = true
				break
			}
		}
		if skip {
			statsSkipped++
		} else {
			statsFiltered = append(statsFiltered, fi)
		}
	}
	if statsSkipped > 0 {
		metrics.ParquetRowGroupsSkipped.Inc("column_stats")
		logger.Infof("column stats pre-filter: skipped %d/%d files", statsSkipped, len(files))
		files = statsFiltered
	}

	filtered := files[:0]
	skipped := 0
	for _, fi := range files {
		if fi.Labels == nil {
			filtered = append(filtered, fi)
			continue
		}
		skip := false
		for _, check := range pdf.Checks {
			labelValues := fi.Labels[check.Column]
			if len(labelValues) == 0 || len(labelValues) >= maxLabelsPerField {
				continue
			}
			if !fileLabelsMatch(labelValues, check) {
				skip = true
				break
			}
		}
		if skip {
			skipped++
		} else {
			filtered = append(filtered, fi)
		}
	}

	if skipped > 0 {
		metrics.ParquetRowGroupsSkipped.Inc("label_index")
		logger.Infof("label pre-filter: skipped %d/%d files", skipped, len(files))
	}

	return filtered
}

func (s *Storage) filterByLabelIndex(files []manifest.FileInfo, pdf *PushDownFilter) []manifest.FileInfo {
	var candidateKeys map[string]bool

	for _, check := range pdf.Checks {
		if check.Op != PushDownExact {
			return nil
		}
		keys := s.manifest.GetFileKeysByLabel(check.Column, check.Value)
		if keys == nil {
			return nil
		}
		if candidateKeys == nil {
			candidateKeys = keys
		} else {
			for k := range candidateKeys {
				if !keys[k] {
					delete(candidateKeys, k)
				}
			}
		}
	}

	if candidateKeys == nil {
		return nil
	}

	var result []manifest.FileInfo
	for _, fi := range files {
		if candidateKeys[fi.Key] {
			result = append(result, fi)
		}
	}

	skipped := len(files) - len(result)
	if skipped > 0 {
		metrics.ParquetRowGroupsSkipped.Inc("label_index")
		logger.Infof("label index fast-path: matched %d/%d files", len(result), len(files))
	}
	return result
}

func fileLabelsMatch(values []string, check PushDownCheck) bool {
	for _, v := range values {
		switch check.Op {
		case PushDownExact:
			if v == check.Value {
				return true
			}
		case PushDownPrefix:
			if strings.HasPrefix(v, check.Value) {
				return true
			}
		case PushDownGreaterThan:
			if v > check.Value {
				return true
			}
		case PushDownLessThan:
			if v < check.Value {
				return true
			}
		default:
			return true
		}
	}
	return false
}

func (s *Storage) filterFilesByBloomIndex(files []manifest.FileInfo, queryStr string) []manifest.FileInfo {
	if s.bloomIdx == nil || s.bloomIdx.Len() == 0 {
		return files
	}

	// Build checks for all bloom-enabled columns that have exact matches in the query
	var checks []bloomindex.ColumnCheck
	for _, col := range s.registry.PromotedColumns() {
		if !col.HasBloom {
			continue
		}
		val := extractExactMatch(queryStr, col.InternalName)
		if val == "" {
			val = extractExactMatch(queryStr, col.ParquetColumn)
		}
		if val != "" {
			checks = append(checks, bloomindex.ColumnCheck{
				Column: col.ParquetColumn,
				Value:  val,
			})
		}
	}

	if len(checks) == 0 {
		return files
	}

	keys := make([]string, len(files))
	for i, fi := range files {
		keys[i] = fi.Key
	}

	matching := s.bloomIdx.MayContainAll(keys, checks)
	if len(matching) == len(files) {
		return files
	}

	matchSet := make(map[string]struct{}, len(matching))
	for _, k := range matching {
		matchSet[k] = struct{}{}
	}

	filtered := make([]manifest.FileInfo, 0, len(matching))
	for _, fi := range files {
		if _, ok := matchSet[fi.Key]; ok {
			filtered = append(filtered, fi)
		}
	}

	skipped := len(files) - len(filtered)
	if skipped > 0 {
		logger.Infof("bloom index pre-filter: skipped %d/%d files (checks=%d)", skipped, len(files), len(checks))
	}
	return filtered
}

func (s *Storage) checkFileBloom(ctx context.Context, fi manifest.FileInfo, queryStr string) bool {
	if queryStr == "" {
		return false
	}

	var checks []bloomindex.ColumnCheck
	for _, col := range s.registry.PromotedColumns() {
		if !col.HasBloom {
			continue
		}
		val := extractExactMatch(queryStr, col.InternalName)
		if val == "" {
			val = extractExactMatch(queryStr, col.ParquetColumn)
		}
		if val != "" {
			checks = append(checks, bloomindex.ColumnCheck{
				Column: col.ParquetColumn,
				Value:  val,
			})
		}
	}
	if len(checks) == 0 {
		return false
	}

	bloomKey := fi.Key + ".bloom"
	data, err := s.pool.Download(ctx, bloomKey)
	if err != nil || len(data) == 0 {
		return false // no sidecar, can't skip
	}

	idx, err := bloomindex.Unmarshal(data)
	if err != nil {
		return false
	}

	if !bloomindex.FileBloomMayContainAll(idx, checks) {
		metrics.ParquetBloomChecks.Inc("file_bloom_skip")
		return true
	}
	return false
}

func rowGroupMatchesTimeRange(rg parquet.RowGroup, tsColIdx int, startNs, endNs int64) bool {
	cols := rg.ColumnChunks()
	if tsColIdx >= len(cols) {
		return true
	}

	idx, err := cols[tsColIdx].ColumnIndex()
	if err != nil || idx == nil {
		return true
	}

	numPages := idx.NumPages()
	if numPages == 0 {
		return true
	}

	minVal := idx.MinValue(0)
	maxVal := idx.MaxValue(numPages - 1)

	rgMin := minVal.Int64()
	rgMax := maxVal.Int64()

	return rgMax >= startNs && rgMin < endNs
}

func findColumnIndex(root *parquet.Column, name string) int {
	col := root.Column(name)
	if col != nil && col.Leaf() {
		return col.Index()
	}
	// Fallback: search top-level leaf columns by name
	for _, c := range root.Columns() {
		if c.Name() == name && c.Leaf() {
			return c.Index()
		}
	}
	return -1
}

func columnNames(root *parquet.Column) []string {
	cols := root.Columns()
	names := make([]string, len(cols))
	for i, col := range cols {
		names[i] = bytesutil.InternString(col.Name())
	}
	return names
}

func parquetValueToAny(v parquet.Value) any {
	if v.IsNull() {
		return ""
	}
	switch v.Kind() {
	case parquet.Int32:
		return v.Int32()
	case parquet.Int64:
		return v.Int64()
	case parquet.Float:
		return float64(v.Float())
	case parquet.Double:
		return v.Double()
	case parquet.Boolean:
		return v.Boolean()
	case parquet.ByteArray, parquet.FixedLenByteArray:
		b := v.ByteArray()
		if isPrintable(b) {
			return bytesutil.InternBytes(b)
		}
		return fmt.Sprintf("%x", b)
	default:
		return v.String()
	}
}

func valueToString(v parquet.Value) string {
	if v.IsNull() {
		return ""
	}
	switch v.Kind() {
	case parquet.Int32:
		return strconv.FormatInt(int64(v.Int32()), 10)
	case parquet.Int64:
		return strconv.FormatInt(v.Int64(), 10)
	case parquet.Int96:
		return v.String()
	case parquet.Float:
		return strconv.FormatFloat(float64(v.Float()), 'g', -1, 32)
	case parquet.Double:
		return strconv.FormatFloat(v.Double(), 'g', -1, 64)
	case parquet.ByteArray, parquet.FixedLenByteArray:
		b := v.ByteArray()
		if isPrintable(b) {
			return bytesutil.InternBytes(b)
		}
		return fmt.Sprintf("%x", b)
	case parquet.Boolean:
		if v.Boolean() {
			return "true"
		}
		return "false"
	default:
		return v.String()
	}
}

func valueToInt64(v parquet.Value) int64 {
	if v.IsNull() {
		return 0
	}
	switch v.Kind() {
	case parquet.Int64:
		return v.Int64()
	case parquet.Int32:
		return int64(v.Int32())
	default:
		return 0
	}
}

func isPrintable(b []byte) bool {
	for _, c := range b {
		if c < 0x20 && c != '\t' && c != '\n' && c != '\r' {
			if !strings.ContainsRune("\t\n\r", rune(c)) {
				return false
			}
		}
	}
	return true
}

// updateColumnStats extracts min/max column statistics from the Parquet file
// footer and stores them in the manifest for use by the column-stats pre-filter.
// It only reads the column index metadata — no data pages are downloaded.
func (s *Storage) updateColumnStats(fileKey string, f *parquet.File) {
	statsColumns := []string{"service.name", "severity_text", "level", "status_code", "service_name", "trace_id", "span.name"}

	stats := make(map[string]manifest.ColumnMinMax)
	for _, col := range statsColumns {
		colIdx := findColumnIndex(f.Root(), col)
		if colIdx < 0 {
			continue
		}
		var globalMin, globalMax string
		for _, rg := range f.RowGroups() {
			cols := rg.ColumnChunks()
			if colIdx >= len(cols) {
				continue
			}
			cidx, err := cols[colIdx].ColumnIndex()
			if err != nil || cidx == nil {
				continue
			}
			for p := 0; p < cidx.NumPages(); p++ {
				minVal := cidx.MinValue(p)
				maxVal := cidx.MaxValue(p)
				if minVal.IsNull() || maxVal.IsNull() {
					continue
				}
				pageMin := string(minVal.Bytes())
				pageMax := string(maxVal.Bytes())
				if len(pageMin) == 0 || len(pageMin) > 256 {
					continue
				}
				if globalMin == "" || pageMin < globalMin {
					globalMin = pageMin
				}
				if globalMax == "" || pageMax > globalMax {
					globalMax = pageMax
				}
			}
		}
		if globalMin != "" {
			stats[col] = manifest.ColumnMinMax{Min: globalMin, Max: globalMax}
		}
	}

	if len(stats) > 0 {
		s.manifest.UpdateFileColumnStats(fileKey, stats)
	}
}

func isFileNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "NoSuchKey") ||
		strings.Contains(s, "NotFound") ||
		strings.Contains(s, "404") ||
		strings.Contains(s, "does not exist") ||
		strings.Contains(s, "file not found")
}

func (s *Storage) handle404Recovery(ctx context.Context, fi manifest.FileInfo, filter *logstorage.Filter, hasTombstones bool, filteredWriteBlock func(uint, *logstorage.DataBlock)) {
	metrics.QueryFileNotFoundTotal.Inc()
	if storage.IsTimestampOnly(ctx) && filter == nil && !hasTombstones &&
		fi.RowCount > 0 && fi.MinTimeNs > 0 && fi.MaxTimeNs > 0 {
		db := s.syntheticManifestBlock(fi)
		if db != nil && db.RowsCount() > 0 {
			filteredWriteBlock(0, db)
			metrics.MetadataOnlyFiles.Inc()
		}
		logger.Infof("query recovered compacted file via manifest metadata; key=%s rows=%d", fi.Key, fi.RowCount)
	} else {
		logger.Infof("query skipped compacted/deleted file; key=%s", fi.Key)
	}
}

func (s *Storage) QuerySpecificFiles(ctx context.Context, fileKeys []string, startNs, endNs int64, queryStr string, pipeFields []string, writeBlock logstorage.WriteDataBlockFunc) error {
	if len(fileKeys) == 0 {
		return nil
	}

	keySet := make(map[string]bool, len(fileKeys))
	for _, k := range fileKeys {
		keySet[k] = true
	}

	allFiles := s.manifest.GetFilesForRange(startNs, endNs)

	var files []manifest.FileInfo
	for _, f := range allFiles {
		if keySet[f.Key] {
			files = append(files, f)
		}
	}
	if len(files) == 0 {
		return nil
	}

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.queryFile(ctx, fi, startNs, endNs, queryStr, pipeFields, writeBlock); err != nil {
			logger.Warnf("QuerySpecificFiles: file error: %s; key=%s", err, fi.Key)
			continue
		}
	}

	return nil
}
