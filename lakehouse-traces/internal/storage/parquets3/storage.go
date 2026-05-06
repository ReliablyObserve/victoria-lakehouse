package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/discovery"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peercache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

type Storage struct {
	cfg          *config.Config
	pool         *s3reader.ClientPool
	manifest     *manifest.Manifest
	registry     *schema.Registry
	memCache     *cache.LRU
	diskCache    *cache.DiskCache
	sfGroup      *cache.Group
	labelIndex   *cache.LabelIndex
	persister    *cache.Persister
	discovery    *discovery.Discovery
	peerCache    *peercache.PeerCache
	peerHandler  *peercache.Handler
	writer       *BatchWriter
	bufferBridge *BufferBridge
	tombstones   *delete.TombstoneStore
}

func New(cfg *config.Config) (*Storage, error) {
	pool, err := s3reader.NewClientPool(context.Background(), &cfg.S3)
	if err != nil {
		return nil, fmt.Errorf("create S3 client pool: %w", err)
	}

	prefix := cfg.AutoPrefix()

	var profile schema.Profile
	if cfg.Mode == config.ModeTraces {
		profile = schema.TracesProfile
	} else {
		profile = schema.LogsProfile
	}

	m := manifest.New(cfg.S3.Bucket, prefix)

	memCache := cache.NewLRU(cfg.CacheMemoryBytes())

	var diskCacheInst *cache.DiskCache
	if cfg.Cache.DiskPath != "" {
		dc, err := cache.NewDiskCache(cfg.Cache.DiskPath, cfg.CacheDiskBytes(), cfg.Cache.EvictionWatermark)
		if err != nil {
			logger.Warnf("disk cache init failed, running without disk cache: %s", err)
		} else {
			diskCacheInst = dc
		}
	}

	labelIdx := cache.NewLabelIndex()

	var pers *cache.Persister
	if cfg.Manifest.PersistPath != "" {
		p, err := cache.NewPersister(cfg.Manifest.PersistPath)
		if err != nil {
			logger.Warnf("persister init failed: %s", err)
		} else {
			pers = p
			if saved, err := p.LoadLabelIndex(); err == nil {
				labelIdx = saved
				logger.Infof("recovered label index from disk; labels=%d", saved.Len())
			}
		}
	}

	disc := discovery.New(
		cfg.Discovery.HeadlessService,
		cfg.Discovery.StorageNodes,
		cfg.Discovery.PartitionAuthKey,
		cfg.Discovery.PeerHeadlessService,
		cfg.DefaultPort(),
		cfg.Discovery.Timeout,
	)

	var pc *peercache.PeerCache
	var ph *peercache.Handler
	if cfg.Peer.AuthKey != "" || cfg.Discovery.PeerHeadlessService != "" {
		pc = peercache.New(
			cfg.ListenAddr(),
			cfg.Peer.AuthKey,
			cfg.Peer.Timeout,
			cfg.Peer.MaxConnections,
		)
		ph = peercache.NewHandler(cfg.Peer.AuthKey)
	}

	var bw *BatchWriter
	if cfg.InsertEnabled() {
		bw = NewBatchWriter(&cfg.Insert, pool, m, prefix, cfg.Mode)
	}

	var bb *BufferBridge
	if cfg.SelectEnabled() && cfg.Select.BufferQueryEnabled {
		bb = NewBufferBridge(&cfg.Select, cfg.Mode)
	}

	return &Storage{
		cfg:          cfg,
		pool:         pool,
		manifest:     m,
		registry:     schema.NewRegistry(profile),
		memCache:     memCache,
		diskCache:    diskCacheInst,
		sfGroup:      cache.NewGroup(),
		labelIndex:   labelIdx,
		persister:    pers,
		discovery:    disc,
		peerCache:    pc,
		peerHandler:  ph,
		writer:       bw,
		bufferBridge: bb,
	}, nil
}

// StartWriter begins the background flush loop. Call after New().
// WAL is replayed before starting the flush loop for crash recovery.
func (s *Storage) StartWriter() {
	if s.writer == nil {
		return
	}
	logCount, traceCount := s.writer.ReplayWAL()
	if logCount > 0 || traceCount > 0 {
		logger.Infof("WAL recovery complete; logs=%d, traces=%d", logCount, traceCount)
	}
	s.writer.Start()
}

// Writer returns the batch writer (nil if insert not enabled).
func (s *Storage) Writer() *BatchWriter {
	return s.writer
}

// MustAddLogRows adds log rows to the write buffer. Panics on nil writer.
func (s *Storage) MustAddLogRows(rows []schema.LogRow) {
	s.writer.AddLogRows(rows)
}

// MustAddTraceRows adds trace rows to the write buffer. Panics on nil writer.
func (s *Storage) MustAddTraceRows(rows []schema.TraceRow) {
	s.writer.AddTraceRows(rows)
}

// CanWriteData checks S3 connectivity for writes.
func (s *Storage) CanWriteData() error {
	if s.writer == nil {
		return fmt.Errorf("insert not enabled (role=%s)", s.cfg.Role)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.writer.CanWriteData(ctx)
}

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

	// Wrap writeBlock to apply tombstone filtering before passing to caller.
	filteredWriteBlock := writeBlock
	if s.tombstones != nil {
		filteredWriteBlock = func(workerID uint, db *logstorage.DataBlock) {
			filtered := s.filterTombstonedRows(db, startNs, endNs)
			if filtered != nil && filtered.RowsCount() > 0 {
				writeBlock(workerID, filtered)
			}
		}
	}

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		return nil
	}

	queryStr := q.String()

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.queryFile(ctx, fi, startNs, endNs, queryStr, filteredWriteBlock); err != nil {
			logger.Warnf("query file error: %s; key=%s", err, fi.Key)
			continue
		}
	}

	if s.bufferBridge != nil {
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
		if err := s.readRowGroup(f, rg, startNs, endNs, writeBlock); err != nil {
			return err
		}
	}

	return nil
}

func (s *Storage) getFileData(ctx context.Context, key string, size int64) ([]byte, error) {
	if data, ok := s.memCache.Get(key); ok {
		metrics.CacheHitsTotal.Inc("L1")
		return data, nil
	}
	metrics.CacheMissesTotal.Inc("L1")

	if s.diskCache != nil {
		if path, ok := s.diskCache.Get(key); ok {
			data, err := os.ReadFile(path)
			if err == nil {
				metrics.CacheHitsTotal.Inc("L2")
				s.memCache.Put(key, data)
				return data, nil
			}
			s.diskCache.Delete(key)
		}
		metrics.CacheMissesTotal.Inc("L2")
	}

	if s.peerCache != nil {
		peer, isLocal := s.peerCache.Lookup(key)
		if !isLocal {
			metrics.PeerRequestsTotal.Inc("fetch")
			peerData, found, peerErr := s.peerCache.Fetch(ctx, peer, key)
			if peerErr == nil && found {
				metrics.CacheHitsTotal.Inc("L3")
				metrics.PeerHitsTotal.Inc()
				metrics.PeerBytesTransferred.Add("rx", len(peerData))
				s.memCache.Put(key, peerData)
				return peerData, nil
			}
			metrics.CacheMissesTotal.Inc("L3")
		}
	}

	data, err, shared := s.sfGroup.Do(key, func() ([]byte, error) {
		s3Start := time.Now()
		metrics.S3RequestsTotal.Inc("GET")
		d, dlErr := s.pool.Download(ctx, key)
		metrics.S3RequestDuration.Observe(time.Since(s3Start).Seconds())
		if dlErr != nil {
			metrics.S3ErrorsTotal.Inc("GET")
			return nil, dlErr
		}
		metrics.S3BytesReadTotal.Add(len(d))

		if s.diskCache != nil {
			if _, putErr := s.diskCache.Put(key, d); putErr != nil {
				logger.Warnf("disk cache put failed: %s; key=%s", putErr, key)
			}
		}

		if s.peerHandler != nil {
			s.peerHandler.Put(key, d)
		}

		return d, nil
	})
	if err != nil {
		return nil, err
	}
	if shared {
		metrics.CacheSingleflightDedup.Inc()
	}

	s.memCache.Put(key, data)
	return data, nil
}

func (s *Storage) readRowGroup(f *parquet.File, rg parquet.RowGroup, startNs, endNs int64, writeBlock logstorage.WriteDataBlockFunc) error {
	schema := f.Root()
	rows := rg.Rows()
	defer func() { _ = rows.Close() }()

	colNames := columnNames(schema)

	buf := make([]parquet.Row, 256)
	for {
		n, err := rows.ReadRows(buf)
		if n > 0 {
			db := s.rowsToDataBlock(buf[:n], colNames, schema, startNs, endNs)
			if db != nil && db.RowsCount() > 0 {
				writeBlock(0, db)
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
				columns[outIdx] = append(columns[outIdx], valueToString(row[srcIdx]))
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

func (s *Storage) GetFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	if s.labelIndex.Len() > 0 {
		names := s.labelIndex.GetFieldNames()
		result := make([]logstorage.ValueWithHits, len(names))
		for i, name := range names {
			result[i] = logstorage.ValueWithHits{Value: name, Hits: 1}
		}
		return result, nil
	}

	startNs, endNs := q.GetFilterTimeRange()

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		return nil, nil
	}

	seen := make(map[string]bool)
	var result []logstorage.ValueWithHits

	fi := files[0]
	data, err := s.getFileData(ctx, fi.Key, fi.Size)
	if err != nil {
		return nil, fmt.Errorf("get file data: %w", err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open parquet: %w", err)
	}

	s.updateLabelIndex(f)

	for _, name := range columnNames(f.Root()) {
		internalName := name
		if m := s.registry.ResolveFromParquet(name); m != nil {
			internalName = m.InternalName
		}
		if !seen[internalName] {
			seen[internalName] = true
			result = append(result, logstorage.ValueWithHits{Value: internalName, Hits: 1})
		}
	}

	return result, nil
}

func (s *Storage) GetFieldValues(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	intLimit := safeUint64ToInt(limit)
	if intLimit > 0 && s.labelIndex.Len() > 0 {
		vals := s.labelIndex.GetFieldValues(fieldName, intLimit)
		if len(vals) > 0 {
			result := make([]logstorage.ValueWithHits, len(vals))
			for i, v := range vals {
				result[i] = logstorage.ValueWithHits{Value: v, Hits: 1}
			}
			return result, nil
		}
	}

	startNs, endNs := q.GetFilterTimeRange()

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		return nil, nil
	}

	mapping := s.registry.ResolveToParquet(fieldName)
	if mapping == nil {
		mapping = s.registry.ResolveFromParquet(fieldName)
	}
	if mapping == nil {
		return nil, nil
	}

	seen := make(map[string]uint64)

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		data, err := s.getFileData(ctx, fi.Key, fi.Size)
		if err != nil {
			logger.Warnf("get file data for field values: %s; key=%s", err, fi.Key)
			continue
		}

		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			logger.Warnf("open parquet for field values: %s; key=%s", err, fi.Key)
			continue
		}

		s.updateLabelIndex(f)

		colIdx := findColumnIndex(f.Root(), mapping.ParquetColumn)
		if colIdx < 0 {
			continue
		}

		for _, rg := range f.RowGroups() {
			rows := rg.Rows()
			buf := make([]parquet.Row, 256)
			for {
				n, err := rows.ReadRows(buf)
				for i := 0; i < n; i++ {
					if colIdx < len(buf[i]) {
						val := valueToString(buf[i][colIdx])
						if val != "" {
							seen[val]++
						}
					}
				}
				if err != nil {
					break
				}
			}
			_ = rows.Close()
		}

		if intLimit > 0 && len(seen) >= intLimit {
			break
		}
	}

	result := make([]logstorage.ValueWithHits, 0, len(seen))
	for v, hits := range seen {
		result = append(result, logstorage.ValueWithHits{Value: v, Hits: hits})
	}
	if intLimit > 0 && len(result) > intLimit {
		result = result[:intLimit]
	}
	return result, nil
}

func (s *Storage) GetStreamFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	streamFields := s.registry.StreamFields()
	result := make([]logstorage.ValueWithHits, 0, len(streamFields))
	for _, name := range streamFields {
		result = append(result, logstorage.ValueWithHits{Value: name, Hits: 1})
	}
	return result, nil
}

func (s *Storage) GetStreamFieldValues(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return s.GetFieldValues(ctx, tenantIDs, q, fieldName, limit)
}

func (s *Storage) GetStreams(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	startNs, endNs := q.GetFilterTimeRange()

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		return nil, nil
	}

	streamColName := "_stream"
	if m := s.registry.ResolveToParquet(streamColName); m != nil {
		streamColName = m.ParquetColumn
	}

	intLimit := safeUint64ToInt(limit)
	seen := make(map[string]uint64)

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		data, err := s.getFileData(ctx, fi.Key, fi.Size)
		if err != nil {
			logger.Warnf("get file data for streams: %s; key=%s", err, fi.Key)
			continue
		}

		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			logger.Warnf("open parquet for streams: %s; key=%s", err, fi.Key)
			continue
		}

		streamIdx := findColumnIndex(f.Root(), streamColName)
		if streamIdx < 0 {
			continue
		}

		for _, rg := range f.RowGroups() {
			rows := rg.Rows()
			buf := make([]parquet.Row, 256)
			for {
				n, err := rows.ReadRows(buf)
				for i := 0; i < n; i++ {
					if streamIdx < len(buf[i]) {
						val := valueToString(buf[i][streamIdx])
						if val != "" {
							seen[val]++
						}
					}
				}
				if err != nil {
					break
				}
			}
			_ = rows.Close()
		}

		if intLimit > 0 && len(seen) >= intLimit {
			break
		}
	}

	result := make([]logstorage.ValueWithHits, 0, len(seen))
	for v, hits := range seen {
		result = append(result, logstorage.ValueWithHits{Value: v, Hits: hits})
	}
	if intLimit > 0 && len(result) > intLimit {
		result = result[:intLimit]
	}
	return result, nil
}

func (s *Storage) GetStreamIDs(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}

func (s *Storage) Manifest() *manifest.Manifest {
	return s.manifest
}

func (s *Storage) Close() error {
	if s.writer != nil {
		s.writer.Stop()
		logger.Infof("writer stopped and final flush completed")
	}
	if s.persister != nil {
		if err := s.persister.SaveLabelIndex(s.labelIndex); err != nil {
			logger.Warnf("failed to persist label index: %s", err)
		} else {
			logger.Infof("persisted label index; labels=%d", s.labelIndex.Len())
		}
	}
	return nil
}

func (s *Storage) updateLabelIndex(f *parquet.File) {
	// Columns that should have values extracted (use Parquet column names)
	promotedWithValues := map[string]bool{
		"service.name":            true,
		"severity_text":           true,
		"k8s.namespace.name":      true,
		"k8s.deployment.name":     true,
		"k8s.node.name":           true,
		"deployment.environment":  true,
		"cloud.region":            true,
		"span.name":               true,
	}

	for _, name := range columnNames(f.Root()) {
		internalName := name
		if m := s.registry.ResolveFromParquet(name); m != nil {
			internalName = m.InternalName
		}

		if !promotedWithValues[name] {
			s.labelIndex.Add(internalName, nil)
			continue
		}

		colIdx := findColumnIndex(f.Root(), name)
		if colIdx < 0 {
			s.labelIndex.Add(internalName, nil)
			continue
		}

		vals := extractDistinctFromStats(f, colIdx)
		s.labelIndex.Add(internalName, vals)
	}
}

func extractDistinctFromStats(f *parquet.File, colIdx int) []string {
	seen := make(map[string]bool)
	for _, rg := range f.RowGroups() {
		cols := rg.ColumnChunks()
		if colIdx >= len(cols) {
			continue
		}
		// First try column index stats (fast, no data read)
		ci, err := cols[colIdx].ColumnIndex()
		if err == nil && ci != nil {
			numPages := ci.NumPages()
			for p := 0; p < numPages; p++ {
				if minBytes := ci.MinValue(p).Bytes(); len(minBytes) > 0 && len(minBytes) < 256 && isPrintable(minBytes) {
					seen[string(minBytes)] = true
				}
				if maxBytes := ci.MaxValue(p).Bytes(); len(maxBytes) > 0 && len(maxBytes) < 256 && isPrintable(maxBytes) {
					seen[string(maxBytes)] = true
				}
			}
		}
		// If stats give few values, scan first row group's actual data
		if len(seen) < 50 && rg.NumRows() > 0 {
			rows := rg.Rows()
			buf := make([]parquet.Row, 512)
			n, _ := rows.ReadRows(buf)
			for i := 0; i < n; i++ {
				if colIdx < len(buf[i]) {
					val := buf[i][colIdx]
					if !val.IsNull() {
						if b := val.Bytes(); len(b) > 0 && len(b) < 256 && isPrintable(b) {
							seen[string(b)] = true
						}
					}
				}
			}
			_ = rows.Close()
		}
		if len(seen) > 1000 {
			break
		}
		break // One row group is enough for warmup
	}
	if len(seen) == 0 {
		return nil
	}
	result := make([]string, 0, len(seen))
	for v := range seen {
		result = append(result, v)
	}
	return result
}

// ClearCaches clears the L1 memory cache and L2 disk cache (if present).
// Useful for benchmarking to ensure cold-start query performance.
func (s *Storage) ClearCaches() {
	s.memCache.Clear()
	if s.diskCache != nil {
		if err := s.diskCache.Clear(); err != nil {
			logger.Warnf("disk cache clear failed: %s", err)
		}
	}
	logger.Infof("caches cleared")
}

func (s *Storage) MemCacheStats() cache.Stats {
	return s.memCache.Stats()
}

func (s *Storage) DiskCacheStats() *cache.Stats {
	if s.diskCache == nil {
		return nil
	}
	st := s.diskCache.Stats()
	return &st
}

func (s *Storage) LabelIndex() *cache.LabelIndex {
	return s.labelIndex
}

func (s *Storage) Discovery() *discovery.Discovery {
	return s.discovery
}

func (s *Storage) PeerCache() *peercache.PeerCache {
	return s.peerCache
}

func (s *Storage) PeerHandler() *peercache.Handler {
	return s.peerHandler
}

// BufferBridge returns the buffer bridge (nil if not configured).
func (s *Storage) BufferBridge() *BufferBridge {
	return s.bufferBridge
}

// SetTombstoneStore injects a TombstoneStore for query-time row filtering.
func (s *Storage) SetTombstoneStore(ts *delete.TombstoneStore) {
	s.tombstones = ts
}

// TombstoneStore returns the configured TombstoneStore (nil if not set).
func (s *Storage) TombstoneStore() *delete.TombstoneStore {
	return s.tombstones
}

// filterTombstonedRows removes rows from a DataBlock that match any active tombstone
// in the given time range. Returns nil if all rows are suppressed.
func (s *Storage) filterTombstonedRows(db *logstorage.DataBlock, startNs, endNs int64) *logstorage.DataBlock {
	if s.tombstones == nil {
		return db
	}

	tombstones := s.tombstones.ForRange(startNs, endNs)
	if len(tombstones) == 0 {
		return db
	}

	rowsCount := db.RowsCount()

	columns := db.GetColumns(false)

	// Find the timestamp column index
	tsColIdx := -1
	for i, col := range columns {
		if col.Name == "_time" || col.Name == "timestamp_unix_nano" {
			tsColIdx = i
			break
		}
	}

	// Determine which rows to keep
	keep := make([]bool, rowsCount)
	keepCount := 0

	for rowIdx := 0; rowIdx < rowsCount; rowIdx++ {
		// Build row map for this row
		row := make(map[string]string, len(columns))
		for _, col := range columns {
			row[col.Name] = col.Values[rowIdx]
		}

		// Parse timestamp
		var tsNs int64
		if tsColIdx >= 0 {
			tsNs, _ = strconv.ParseInt(columns[tsColIdx].Values[rowIdx], 10, 64)
		}

		// Check if any tombstone matches this row
		matched := false
		for i := range tombstones {
			if tombstones[i].MatchesRow(row, tsNs) {
				matched = true
				break
			}
		}

		if !matched {
			keep[rowIdx] = true
			keepCount++
		}
	}

	suppressed := rowsCount - keepCount
	if suppressed > 0 {
		metrics.DeleteRowsSuppressed.Add(suppressed)
	}

	if keepCount == 0 {
		return nil
	}

	if keepCount == rowsCount {
		return db
	}

	// Build new DataBlock with only kept rows
	newCols := make([]logstorage.BlockColumn, len(columns))
	for i, col := range columns {
		vals := make([]string, 0, keepCount)
		for rowIdx, v := range col.Values {
			if keep[rowIdx] {
				vals = append(vals, v)
			}
		}
		newCols[i] = logstorage.BlockColumn{
			Name:   col.Name,
			Values: vals,
		}
	}

	filtered := &logstorage.DataBlock{}
	filtered.SetColumns(newCols)
	return filtered
}

// Pool returns the S3 client pool.
func (s *Storage) Pool() *s3reader.ClientPool {
	return s.pool
}

// logRowsToDataBlock converts in-memory LogRow slices to a columnar DataBlock.
func (s *Storage) logRowsToDataBlock(rows []schema.LogRow) *logstorage.DataBlock {
	if len(rows) == 0 {
		return nil
	}

	times := make([]string, len(rows))
	bodies := make([]string, len(rows))
	levels := make([]string, len(rows))
	services := make([]string, len(rows))
	traceIDs := make([]string, len(rows))
	spanIDs := make([]string, len(rows))
	streams := make([]string, len(rows))
	namespaces := make([]string, len(rows))
	pods := make([]string, len(rows))
	deployments := make([]string, len(rows))
	nodes := make([]string, len(rows))
	envs := make([]string, len(rows))
	regions := make([]string, len(rows))
	hosts := make([]string, len(rows))

	for i, row := range rows {
		times[i] = fmt.Sprintf("%d", row.TimestampUnixNano)
		bodies[i] = row.Body
		levels[i] = row.SeverityText
		services[i] = row.ServiceName
		traceIDs[i] = row.TraceID
		spanIDs[i] = row.SpanID
		streams[i] = row.Stream
		namespaces[i] = row.K8sNamespaceName
		pods[i] = row.K8sPodName
		deployments[i] = row.K8sDeploymentName
		nodes[i] = row.K8sNodeName
		envs[i] = row.DeployEnv
		regions[i] = row.CloudRegion
		hosts[i] = row.HostName
	}

	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: times},
		{Name: "_msg", Values: bodies},
		{Name: "level", Values: levels},
		{Name: "service.name", Values: services},
		{Name: "trace_id", Values: traceIDs},
		{Name: "span_id", Values: spanIDs},
		{Name: "_stream", Values: streams},
		{Name: "k8s.namespace.name", Values: namespaces},
		{Name: "k8s.pod.name", Values: pods},
		{Name: "k8s.deployment.name", Values: deployments},
		{Name: "k8s.node.name", Values: nodes},
		{Name: "deployment.environment", Values: envs},
		{Name: "cloud.region", Values: regions},
		{Name: "host.name", Values: hosts},
	})
	return db
}

// traceRowsToDataBlock converts in-memory TraceRow slices to a columnar DataBlock.
func (s *Storage) traceRowsToDataBlock(rows []schema.TraceRow) *logstorage.DataBlock {
	if len(rows) == 0 {
		return nil
	}

	times := make([]string, len(rows))
	traceIDs := make([]string, len(rows))
	spanIDs := make([]string, len(rows))
	names := make([]string, len(rows))
	services := make([]string, len(rows))
	durations := make([]string, len(rows))
	statusCodes := make([]string, len(rows))
	parentSpanIDs := make([]string, len(rows))
	statusMsgs := make([]string, len(rows))

	for i, row := range rows {
		times[i] = fmt.Sprintf("%d", row.TimestampUnixNano)
		traceIDs[i] = row.TraceID
		spanIDs[i] = row.SpanID
		names[i] = row.SpanName
		services[i] = row.ServiceName
		durations[i] = fmt.Sprintf("%d", row.DurationNs)
		statusCodes[i] = fmt.Sprintf("%d", row.StatusCode)
		parentSpanIDs[i] = row.ParentSpanID
		statusMsgs[i] = row.StatusMessage
	}

	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: times},
		{Name: "trace_id", Values: traceIDs},
		{Name: "span_id", Values: spanIDs},
		{Name: "name", Values: names},
		{Name: "service.name", Values: services},
		{Name: "duration", Values: durations},
		{Name: "status_code", Values: statusCodes},
		{Name: "parent_span_id", Values: parentSpanIDs},
		{Name: "status_message", Values: statusMsgs},
	})
	return db
}

func (s *Storage) RefreshDiscovery(ctx context.Context) error {
	if _, err := s.discovery.DiscoverStorageNodes(ctx); err != nil {
		return fmt.Errorf("discover storage nodes: %w", err)
	}
	if _, err := s.discovery.PollPartitionList(ctx); err != nil {
		return fmt.Errorf("poll partition list: %w", err)
	}
	if s.peerCache != nil {
		peers, err := s.discovery.DiscoverPeers(ctx)
		if err != nil {
			return fmt.Errorf("discover peers: %w", err)
		}
		s.peerCache.UpdatePeers(peers)
	}
	return nil
}

func (s *Storage) RefreshManifest(ctx context.Context) error {
	return s.manifest.RefreshFromS3(ctx, s.pool.S3Client())
}

func (s *Storage) WarmLabelIndex(ctx context.Context) {
	if s.labelIndex.Len() > 0 {
		return
	}
	files := s.manifest.GetFilesForRange(0, 1<<62)
	if len(files) == 0 {
		return
	}
	// Sample up to 10 files spread across the range for value diversity
	sampleCount := 10
	if len(files) < sampleCount {
		sampleCount = len(files)
	}
	step := len(files) / sampleCount
	if step == 0 {
		step = 1
	}
	sampled := 0
	for i := 0; i < len(files) && sampled < sampleCount; i += step {
		fi := files[i]
		data, err := s.getFileData(ctx, fi.Key, fi.Size)
		if err != nil {
			continue
		}
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			continue
		}
		s.updateLabelIndex(f)
		sampled++
	}
	logger.Infof("label index warmed; labels=%d, files_sampled=%d", s.labelIndex.Len(), sampled)
}

func (s *Storage) PersistState() error {
	if s.persister == nil {
		return nil
	}
	return s.persister.SaveLabelIndex(s.labelIndex)
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

func safeUint64ToInt(v uint64) int {
	return int(min(v, uint64(math.MaxInt))) //nolint:gosec // overflow guarded by min
}
