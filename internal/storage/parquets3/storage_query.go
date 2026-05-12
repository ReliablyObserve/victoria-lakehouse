package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
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
	var writeBlockPanic atomic.Bool
	filteredWriteBlock := func(workerID uint, db *logstorage.DataBlock) {
		if writeBlockPanic.Load() {
			return
		}
		if maxRows > 0 && rowsEmitted.Load() >= maxRows {
			return
		}
		db = filterDataBlock(db, filter)
		if db == nil || db.RowsCount() == 0 {
			return
		}
		if s.tombstones != nil {
			db = s.filterTombstonedRows(db, startNs, endNs)
			if db == nil || db.RowsCount() == 0 {
				return
			}
		}
		rowsEmitted.Add(int64(db.RowsCount()))
		func() {
			defer func() {
				if r := recover(); r != nil {
					writeBlockPanic.Store(true)
					logger.Warnf("writeBlock panic recovered (unsupported pipe in query): %v", r)
				}
			}()
			writeBlock(workerID, db)
		}()
	}

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		return nil
	}

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

	var wbMu sync.Mutex
	serializedWriteBlock := func(workerID uint, db *logstorage.DataBlock) {
		wbMu.Lock()
		filteredWriteBlock(workerID, db)
		wbMu.Unlock()
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
		go func() {
			defer wg.Done()
			for fi := range taskCh {
				if err := ctx.Err(); err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
				if maxRows > 0 && rowsEmitted.Load() >= maxRows {
					return
				}
				if err := s.queryFile(ctx, fi, startNs, endNs, queryStr, serializedWriteBlock); err != nil {
					logger.Warnf("query file error: %s; key=%s", err, fi.Key)
					continue
				}
			}
		}()
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

	return nil
}

func (s *Storage) queryFile(ctx context.Context, fi manifest.FileInfo, startNs, endNs int64, queryStr string, writeBlock logstorage.WriteDataBlockFunc) error {
	data, err := s.getFileData(ctx, fi.Key, fi.Size)
	if err != nil {
		return fmt.Errorf("get file data %s: %w", fi.Key, err)
	}

	metrics.ParquetFilesOpened.Inc()
	metrics.ParquetColumnBytesRead.Add(len(data))

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("open parquet file %s: %w", fi.Key, err)
	}

	s.updateLabelIndex(f)

	tsIdx := findColumnIndex(f.Root(), s.registry.TimestampColumn())
	bloomChecks := s.buildBloomChecks(queryStr)

	var collectedTraceIDs []string
	var traceIDsPtr *[]string
	if s.smartCache != nil {
		traceIDsPtr = &collectedTraceIDs
	}

	for _, rg := range f.RowGroups() {
		if err := ctx.Err(); err != nil {
			return err
		}

		if tsIdx >= 0 && !rowGroupMatchesTimeRange(rg, tsIdx, startNs, endNs) {
			metrics.ParquetRowGroupsSkipped.Inc("stats")
			continue
		}

		if s.bloomFilterSkip(f, rg, bloomChecks) {
			metrics.ParquetRowGroupsSkipped.Inc("bloom")
			continue
		}

		metrics.ParquetRowGroupsScanned.Inc()
		if err := s.readRowGroup(f, rg, startNs, endNs, writeBlock, traceIDsPtr); err != nil {
			return err
		}
	}

	if s.smartCache != nil && len(collectedTraceIDs) > 0 {
		s.smartCache.RecordTraceIDs(fi.Key, collectedTraceIDs)
	}

	return nil
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

type field struct {
	name  string
	value string
}

func logRowToFields(r *schema.LogRow) []field {
	fields := []field{
		{"_time", time.Unix(0, r.TimestampUnixNano).UTC().Format(time.RFC3339Nano)},
		{"_msg", r.Body},
		{"level", r.SeverityText},
		{"severity_number", fmt.Sprintf("%d", r.SeverityNumber)},
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
		{"_time", time.Unix(0, r.TimestampUnixNano).UTC().Format(time.RFC3339Nano)},
		{"start_time", time.Unix(0, r.StartTimeUnixNano).UTC().Format(time.RFC3339Nano)},
		{"timestamp_unix_nano", fmt.Sprintf("%d", r.TimestampUnixNano)},
		{"start_time_unix_nano", fmt.Sprintf("%d", r.StartTimeUnixNano)},
		{"trace_id", r.TraceID},
		{"span_id", r.SpanID},
		{"parent_span_id", r.ParentSpanID},
		{"name", r.SpanName},
		{"span.kind", fmt.Sprintf("%d", r.SpanKind)},
		{"status_code", fmt.Sprintf("%d", r.StatusCode)},
		{"status.message", r.StatusMessage},
		{"duration", fmt.Sprintf("%d", r.DurationNs)},
		{"service.name", r.ServiceName},
		{"scope.name", r.ScopeName},
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
			if f.value == "" {
				continue
			}
			idx := getCol(f.name)
			seen[idx] = true
			cols[idx].values = append(cols[idx].values, f.value)
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

	blockCols := make([]logstorage.BlockColumn, len(cols))
	for i, col := range cols {
		internalName := col.name
		if m := s.registry.ResolveFromParquet(col.name); m != nil {
			internalName = bytesutil.InternString(m.InternalName)
		}
		blockCols[i] = logstorage.BlockColumn{
			Name:   internalName,
			Values: col.values,
		}
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

func (s *Storage) rowsToDataBlock(rows []parquet.Row, colNames []string, root *parquet.Column, startNs, endNs int64) *logstorage.DataBlock {
	if len(rows) == 0 {
		return nil
	}

	projected := s.projectColumns(colNames, nil)

	columns := make([][]string, len(projected))
	for i := range columns {
		columns[i] = make([]string, 0, len(rows))
	}

	tsColIdx := -1
	for i, name := range colNames {
		if name == "timestamp_unix_nano" {
			tsColIdx = i
			break
		}
	}

	for _, row := range rows {
		if tsColIdx >= 0 && startNs != 0 && endNs != 0 {
			ts := valueToInt64(row[tsColIdx])
			if ts < startNs || ts >= endNs {
				continue
			}
		}

		for outIdx, srcIdx := range projected {
			if srcIdx < len(row) {
				if srcIdx == tsColIdx {
					ns := valueToInt64(row[srcIdx])
					columns[outIdx] = append(columns[outIdx], time.Unix(0, ns).UTC().Format(time.RFC3339Nano))
				} else {
					columns[outIdx] = append(columns[outIdx], valueToString(row[srcIdx]))
				}
			} else {
				columns[outIdx] = append(columns[outIdx], "")
			}
		}
	}

	if len(columns) == 0 || len(columns[0]) == 0 {
		return nil
	}

	blockCols := make([]logstorage.BlockColumn, 0, len(projected))
	for outIdx, srcIdx := range projected {
		name := colNames[srcIdx]
		internalName := name
		if m := s.registry.ResolveFromParquet(name); m != nil {
			internalName = bytesutil.InternString(m.InternalName)
		}
		blockCols = append(blockCols, logstorage.BlockColumn{
			Name:   internalName,
			Values: columns[outIdx],
		})
	}

	db := &logstorage.DataBlock{}
	db.SetColumns(blockCols)
	return db
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
		val := extractExactMatch(queryStr, col.InternalName)
		if val == "" {
			val = extractExactMatch(queryStr, col.ParquetColumn)
		}
		if val != "" {
			checks = append(checks, bloomCheck{
				colName: col.ParquetColumn,
				value:   parquet.ValueOf(val),
			})
		}
	}
	return checks
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

func extractExactMatch(query, fieldName string) string {
	patterns := []string{
		fieldName + `:="`,
		fieldName + `:"`,
	}
	for _, prefix := range patterns {
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
	return ""
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

func valueToString(v parquet.Value) string {
	if v.IsNull() {
		return ""
	}
	switch v.Kind() {
	case parquet.Int32:
		return fmt.Sprintf("%d", v.Int32())
	case parquet.Int64:
		return fmt.Sprintf("%d", v.Int64())
	case parquet.Int96:
		return v.String()
	case parquet.Float:
		return fmt.Sprintf("%g", v.Float())
	case parquet.Double:
		return fmt.Sprintf("%g", v.Double())
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
