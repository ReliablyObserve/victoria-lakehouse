package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

func (s *Storage) RunQuery(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
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
	filter := parseFilterFromQuery(q)

	var rowsEmitted atomic.Int64
	maxRows := s.cfg.Query.MaxRows

	// Wrap writeBlock to apply LogsQL filter evaluation, tombstone filtering,
	// and max_rows enforcement before passing to caller.
	// Pre-filter runs in each worker goroutine without locks.
	// Only the final writeBlock call is serialized.
	var writeBlockPanic atomic.Bool
	preFilter := func(db *logstorage.DataBlock) *logstorage.DataBlock {
		if writeBlockPanic.Load() {
			return nil
		}
		if maxRows > 0 && rowsEmitted.Load() >= maxRows {
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

	// Label-based file pre-filtering
	files = s.filterFilesByLabels(files, queryStr)
	if len(files) == 0 {
		return nil
	}

	// Partition-level bloom file skip
	files = s.bloomFilterFiles(ctx, files, queryStr)

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
				if err := s.queryFile(ctx, fi, startNs, endNs, queryStr, filteredWriteBlock); err != nil {
					logger.Warnf("query file error: %s; key=%s", err, fi.Key)
					continue
				}
			}
		}(i)
	}
	wg.Wait()

	if v := firstErr.Load(); v != nil {
		if err, ok := v.(error); ok && ctx.Err() != nil {
			return err
		}
	}

	if s.bufferBridge != nil && (maxRows <= 0 || rowsEmitted.Load() < maxRows) {
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

	if n := rowsEmitted.Load(); n > 0 {
		metrics.QueryRowsTotal.Add(int(n))
	}

	return nil
}

func (s *Storage) queryFile(ctx context.Context, fi manifest.FileInfo, startNs, endNs int64, queryStr string, writeBlock logstorage.WriteDataBlockFunc) error {
	data, err := s.getFileData(ctx, fi.Key, fi.Size)
	if err != nil {
		return fmt.Errorf("get file data %s: %w", fi.Key, err)
	}

	metrics.ParquetFilesOpened.Inc()
	metrics.ParquetColumnBytesRead.Add(len(data))

	var f *parquet.File
	if s.footerCache != nil {
		if cached, ok := s.footerCache.Get(fi.Key); ok && cached.FileSize == int64(len(data)) {
			f = cached.File
		}
	}
	if f == nil {
		var parseErr error
		if s.footerCache != nil {
			var cached *CachedFooter
			cached, f, parseErr = ParseFooterFromData(fi.Key, data)
			if parseErr != nil {
				return parseErr
			}
			s.footerCache.Put(fi.Key, cached)
		} else {
			f, parseErr = parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
			if parseErr != nil {
				return fmt.Errorf("open parquet file %s: %w", fi.Key, parseErr)
			}
		}
	}

	s.updateLabelIndex(f)

	tsIdx := findColumnIndex(f.Root(), s.registry.TimestampColumn())
	bloomChecks := s.buildBloomChecks(queryStr)
	pdf := buildPushDownFilter(queryStr, s.registry)
	projectedCols := queryColumns(queryStr, s.registry)

	// Hits/stats fast path: when the endpoint only needs timestamps (set via
	// context hint) and the query has no column-specific filters, project only
	// the timestamp column to avoid deserializing all row data.
	if projectedCols == nil && storage.IsTimestampOnly(ctx) {
		projectedCols = map[string]bool{s.registry.TimestampColumn(): true}
	}

	var collectedTraceIDs []string
	var traceIDsPtr *[]string
	if s.smartCache != nil {
		traceIDsPtr = &collectedTraceIDs
	}

	rowGroups := f.RowGroups()

	// Pre-filter row groups using metadata (time range, bloom, pushdown).
	var matchedRGs []parquet.RowGroup
	for _, rg := range rowGroups {
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
		matchedRGs = append(matchedRGs, rg)
	}

	// Process matched row groups — parallel when >1 to reduce per-file latency.
	if len(matchedRGs) <= 1 {
		for _, rg := range matchedRGs {
			metrics.ParquetRowGroupsScanned.Inc()
			if err := s.readOneRowGroup(f, rg, startNs, endNs, projectedCols, writeBlock, traceIDsPtr); err != nil {
				return err
			}
		}
	} else {
		rgWorkers := len(matchedRGs)
		if rgWorkers > 3 {
			rgWorkers = 3
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
					if err := s.readOneRowGroup(f, rg, startNs, endNs, projectedCols, writeBlock, traceIDsPtr); err != nil {
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

	return nil
}

func (s *Storage) readOneRowGroup(f *parquet.File, rg parquet.RowGroup, startNs, endNs int64, projectedCols map[string]bool, writeBlock logstorage.WriteDataBlockFunc, traceIDs *[]string) error {
	if projectedCols != nil {
		return s.readRowGroupWithProjection(f, rg, startNs, endNs, projectedCols, writeBlock, traceIDs)
	}
	return s.readRowGroup(f, rg, startNs, endNs, writeBlock, traceIDs)
}

func (s *Storage) readRowGroup(f *parquet.File, rg parquet.RowGroup, startNs, endNs int64, writeBlock logstorage.WriteDataBlockFunc, traceIDs *[]string) error {
	if s.cfg.Mode == config.ModeTraces {
		return readRowGroupTyped[schema.TraceRow](s, f, rg, startNs, endNs, writeBlock, traceIDs, traceRowToFields)
	}
	return readRowGroupTyped[schema.LogRow](s, f, rg, startNs, endNs, writeBlock, traceIDs, logRowToFields)
}

func readRowGroupTyped[T any](s *Storage, f *parquet.File, rg parquet.RowGroup, startNs, endNs int64, writeBlock logstorage.WriteDataBlockFunc, traceIDs *[]string, toFields func(*T) []field) error {
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

func (s *Storage) readRowGroupWithProjection(f *parquet.File, rg parquet.RowGroup, startNs, endNs int64, cols map[string]bool, writeBlock logstorage.WriteDataBlockFunc, traceIDs *[]string) error {
	allFields, err := readRowGroupProjected(f, rg, cols)
	if err != nil {
		return err
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

		seen := make(map[int]bool)
		for _, fld := range fields {
			// Map parquet column name to internal name
			internalName := fld.name
			if m := s.registry.ResolveFromParquet(fld.name); m != nil {
				internalName = m.InternalName
			}

			formatted := s.registry.FormatField(internalName, fld.value)
			if formatted == "" {
				continue
			}
			idx := getCol(internalName)
			if seen[idx] {
				continue
			}
			seen[idx] = true
			// Pad with empty strings if this column appeared late
			for len(cols[idx].values) < rowNum {
				cols[idx].values = append(cols[idx].values, "")
			}
			cols[idx].values = append(cols[idx].values, formatted)
		}

		// Fill empty for columns not present in this row
		for i := range cols {
			if !seen[i] && len(cols[i].values) <= rowNum {
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

type field struct {
	name  string
	value any
}

func logRowToFields(r *schema.LogRow) []field {
	fields := []field{
		{"_time", r.TimestampUnixNano},
		{"_msg", r.Body},
		{"level", r.SeverityText},
		{"severity_number", r.SeverityNumber},
		{"service.name", r.ServiceName},
		{"k8s.namespace.name", r.K8sNamespaceName},
		{"k8s.pod.name", r.K8sPodName},
		{"k8s.deployment.name", r.K8sDeploymentName},
		{"k8s.node.name", r.K8sNodeName},
		{"deployment.environment", r.DeployEnv},
		{"cloud.region", r.CloudRegion},
		{"host.name", r.HostName},
		{"trace_id", r.TraceID},
		{"span_id", r.SpanID},
		{"_stream", r.Stream},
		{"_stream_id", r.StreamID},
		{"scope.name", r.ScopeName},
	}
	for k, v := range r.ResourceAttributes {
		fields = append(fields, field{k, v})
	}
	for k, v := range r.LogAttributes {
		fields = append(fields, field{k, v})
	}
	return fields
}

func traceRowToFields(r *schema.TraceRow) []field {
	fields := []field{
		{"_time", r.TimestampUnixNano},
		{"start_time", r.StartTimeUnixNano},
		{"trace_id", r.TraceID},
		{"span_id", r.SpanID},
		{"parent_span_id", r.ParentSpanID},
		{"name", r.SpanName},
		{"kind", r.SpanKind},
		{"status_code", r.StatusCode},
		{"status_message", r.StatusMessage},
		{"duration", r.DurationNs},
		{"resource_attr:service.name", r.ServiceName},
		{"scope_attr:otel.library.name", r.ScopeName},
		{"resource_attr:deployment.environment", r.DeployEnv},
		{"resource_attr:cloud.region", r.CloudRegion},
		{"resource_attr:host.name", r.HostName},
		{"resource_attr:k8s.namespace.name", r.K8sNamespaceName},
		{"resource_attr:k8s.deployment.name", r.K8sDeploymentName},
		{"resource_attr:k8s.node.name", r.K8sNodeName},
		{"span_attr:http.method", r.HTTPMethod},
		{"span_attr:http.status_code", r.HTTPStatusCode},
		{"span_attr:http.url", r.HTTPUrl},
		{"span_attr:db.system", r.DBSystem},
		{"span_attr:db.statement", r.DBStatement},
	}
	for k, v := range r.ResourceAttributes {
		fields = append(fields, field{k, v})
	}
	for k, v := range r.SpanAttributes {
		fields = append(fields, field{k, v})
	}
	for k, v := range r.ScopeAttributes {
		fields = append(fields, field{k, v})
	}
	return fields
}

func typedRowsToDataBlock[T any](s *Storage, rows []T, startNs, endNs int64, toFields func(*T) []field) *logstorage.DataBlock {
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

	for rowNum, row := range rows {
		fields := toFields(&row)

		seen := make(map[int]bool)
		for _, f := range fields {
			formatted := s.registry.FormatField(f.name, f.value)
			if formatted == "" {
				continue
			}
			idx := getCol(f.name)
			if seen[idx] {
				continue
			}
			seen[idx] = true
			cols[idx].values = append(cols[idx].values, formatted)
		}

		for i := range cols {
			if !seen[i] && len(cols[i].values) <= rowNum {
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

func (s *Storage) bloomFilterFiles(ctx context.Context, files []manifest.FileInfo, queryStr string) []manifest.FileInfo {
	if s.bloomCache == nil || queryStr == "" {
		return files
	}

	var checks []bloomindex.ColumnCheck
	for _, col := range s.registry.PromotedColumns() {
		if !col.HasBloom {
			continue
		}
		vals := extractFilterValues(queryStr, col.InternalName)
		if len(vals) == 0 {
			vals = extractFilterValues(queryStr, col.ParquetColumn)
		}
		for _, val := range vals {
			checks = append(checks, bloomindex.ColumnCheck{
				Column: col.ParquetColumn,
				Value:  val,
			})
		}
	}
	if len(checks) == 0 {
		return files
	}

	metrics.BloomQueriesTotal.Inc("attempt")

	byPartition := make(map[string][]manifest.FileInfo)
	for _, fi := range files {
		partition := partitionFromKey(fi.Key)
		byPartition[partition] = append(byPartition[partition], fi)
	}

	var result []manifest.FileInfo
	for partition, pFiles := range byPartition {
		idx, err := s.bloomCache.Get(ctx, partition)
		if err != nil || idx == nil {
			result = append(result, pFiles...)
			continue
		}

		keys := make([]string, len(pFiles))
		for i, fi := range pFiles {
			keys[i] = fi.Key
		}
		matching := idx.MayContainAll(keys, checks)
		matchSet := make(map[string]bool, len(matching))
		for _, k := range matching {
			matchSet[k] = true
		}

		before := len(pFiles)
		var bytesAvoided int64
		for _, fi := range pFiles {
			if matchSet[fi.Key] {
				result = append(result, fi)
			} else {
				bytesAvoided += fi.Size
			}
		}
		skipped := before - len(matchSet)
		if skipped > 0 {
			metrics.ParquetFilesSkipped.Add(skipped)
			metrics.BloomFilesSkipped.Add(skipped)
			metrics.BloomBytesAvoided.Add(int(bytesAvoided))
			metrics.ParquetBloomChecks.Add("miss", skipped)
		}
		if len(matchSet) > 0 {
			metrics.ParquetBloomChecks.Add("hit", len(matchSet))
		}
	}
	return result
}

func partitionFromKey(key string) string {
	// Extract "dt=YYYY-MM-DD/hour=HH" from file key
	idx := strings.Index(key, "/hour=")
	if idx < 0 {
		// Try daily partition
		if dtIdx := strings.Index(key, "dt="); dtIdx >= 0 {
			end := strings.IndexByte(key[dtIdx:], '/')
			if end < 0 {
				return key[dtIdx:]
			}
			return key[dtIdx : dtIdx+end]
		}
		return key
	}
	hourEnd := idx + len("/hour=")
	for hourEnd < len(key) && key[hourEnd] != '/' {
		hourEnd++
	}
	return key[:hourEnd]
}

func (s *Storage) bloomFilterSkip(f *parquet.File, rg parquet.RowGroup, checks []bloomCheck) bool {
	if len(checks) == 0 {
		return false
	}

	cols := rg.ColumnChunks()
	for _, check := range checks {
		colIdx := findColumnIndex(f.Root(), check.colName)
		if colIdx < 0 || colIdx >= len(cols) {
			continue
		}

		bf := cols[colIdx].BloomFilter()
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
	type labelCheck struct {
		field string
		value string
	}

	var checks []labelCheck
	for _, col := range s.registry.PromotedColumns() {
		if !col.HasBloom {
			continue
		}
		val := extractExactMatch(queryStr, col.InternalName)
		if val == "" {
			val = extractExactMatch(queryStr, col.ParquetColumn)
		}
		if val != "" {
			checks = append(checks, labelCheck{field: col.ParquetColumn, value: val})
		}
	}

	if len(checks) == 0 {
		return files
	}

	filtered := files[:0]
	skipped := 0
	for _, fi := range files {
		skip := false
		for _, check := range checks {
			labelValues := fi.Labels[check.field]
			if fi.Labels != nil && len(labelValues) > 0 && len(labelValues) < maxLabelsPerField && !fi.MatchesLabel(check.field, check.value) {
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
