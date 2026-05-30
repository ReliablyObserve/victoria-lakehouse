package parquets3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/crosssignal"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/discovery"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peercache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/smartcache"
)

type Storage struct {
	cfg               *config.Config
	pool              *s3reader.ClientPool
	manifest          *manifest.Manifest
	registry          *schema.Registry
	memCache          *cache.LRU
	diskCache         *cache.DiskCache
	sfGroup           *cache.Group
	labelIndex        *cache.LabelIndex
	persister         *cache.Persister
	discovery         *discovery.Discovery
	peerCache         *peercache.PeerCache
	peerHandler       *peercache.Handler
	writer            *BatchWriter
	bufferBridge      *BufferBridge
	tombstones        *delete.TombstoneStore
	smartCache        *smartcache.Controller
	bloomCache        *bloomindex.BloomCache
	bloomObserver     *storageBloomObserver
	footerCache       *FooterCache
	fileBloomCache    *BloomFileCache
	crossSignalClient *crosssignal.Client
	selfAZ            string
	selfFilterEnabled bool
	dlSem             chan struct{}
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
	metrics.SmartCacheBytesLimit.Set(cfg.CacheMemoryBytes())
	metrics.ConcurrentSelectsCap.Set(int64(cfg.Query.MaxConcurrent))
	metrics.MetricsCardinalityLimit.Set(int64(cfg.Stats.MetricsCardinalityLimit))

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
		ph = peercache.NewHandler(cfg.Peer.AuthKey, "")
	}

	maxDL := cfg.S3.MaxConcurrentDownloads
	if maxDL <= 0 {
		maxDL = 16
	}

	var sc *smartcache.Controller
	if cfg.SelectEnabled() {
		metaMap := smartcache.NewMetadataMap()

		if cfg.Cache.DiskPath != "" {
			snapPath := cfg.Cache.DiskPath + "/smartcache.meta.json"
			if err := metaMap.LoadSnapshot(snapPath); err != nil {
				logger.Warnf("failed to load cache metadata snapshot: %s", err)
			}
		}

		var peerLookupImpl smartcache.PeerLookup
		var peerFetchImpl smartcache.PeerFetcher
		if pc != nil {
			peerLookupImpl = &peerLookupAdapter{pc: pc}
			peerFetchImpl = &peerFetchAdapter{pc: pc}
		} else {
			peerLookupImpl = &localOnlyLookup{}
			peerFetchImpl = nil
		}

		sc = smartcache.NewController(smartcache.ControllerConfig{
			L1:           &l1Adapter{lru: memCache},
			L2:           &l2Adapter{dc: diskCacheInst},
			PeerLookup:   peerLookupImpl,
			PeerFetcher:  peerFetchImpl,
			S3Fetcher:    &s3Adapter{pool: pool, dlSem: make(chan struct{}, maxDL)},
			Metadata:     metaMap,
			MaxAge:       cfg.SmartCache.MaxAge,
			HotThreshold: cfg.SmartCache.HotAccessThreshold,
			HotWindow:    cfg.SmartCache.HotWindow,
			GracePeriod:  cfg.SmartCache.QueryGracePeriod,
			Signal:       string(cfg.Mode),
		})
	}

	var bw *BatchWriter
	if cfg.InsertEnabled() {
		bw = NewBatchWriter(&cfg.Insert, pool, m, prefix, cfg.Mode)
	}

	var bb *BufferBridge
	if cfg.SelectEnabled() && cfg.Select.BufferQueryEnabled {
		bb = NewBufferBridge(&cfg.Select, cfg.Mode)
	}

	var bc *bloomindex.BloomCache
	if cfg.SelectEnabled() {
		bc = bloomindex.NewBloomCache(
			10*1024*1024,
			bloomS3Loader(pool, prefix),
		)
	}

	var fc *FooterCache
	var bfc *BloomFileCache
	if cfg.SelectEnabled() {
		fc = NewFooterCache(10000)
		bfc = NewBloomFileCache(1024)
	}

	var csClient *crosssignal.Client
	if cfg.CrossSignal.Enabled && cfg.CrossSignal.Endpoint != "" {
		csClient = crosssignal.NewClient(crosssignal.ClientConfig{
			Endpoint:      cfg.CrossSignal.Endpoint,
			AuthKey:       cfg.CrossSignal.AuthKey,
			Timeout:       cfg.CrossSignal.Timeout,
			MaxBatch:      cfg.CrossSignal.MaxBatch,
			BatchInterval: cfg.CrossSignal.BatchInterval,
		})
	}

	s := &Storage{
		cfg:               cfg,
		pool:              pool,
		manifest:          m,
		registry:          schema.NewRegistry(profile),
		memCache:          memCache,
		diskCache:         diskCacheInst,
		sfGroup:           cache.NewGroup(),
		labelIndex:        labelIdx,
		persister:         pers,
		discovery:         disc,
		peerCache:         pc,
		peerHandler:       ph,
		writer:            bw,
		bufferBridge:      bb,
		smartCache:        sc,
		bloomCache:        bc,
		footerCache:       fc,
		fileBloomCache:    bfc,
		crossSignalClient: csClient,
		dlSem:             make(chan struct{}, maxDL),
	}

	if bw != nil {
		obs := &storageBloomObserver{
			bloom:    bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01),
			pool:     pool,
			manifest: s.manifest,
		}
		bw.bloomObserver = obs
		s.bloomObserver = obs

		// Write-through cache: when running in combined mode (role=all),
		// cache flushed column data locally so queries avoid an S3 round-trip
		// for recently ingested data.
		if sc != nil {
			bw.SetFlushCacheCallback(func(fileKey string, data []byte) {
				cacheOnFlush(sc, fileKey, data)
			})
		}
	}

	return s, nil
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

func (s *Storage) getFileData(ctx context.Context, key string, size int64) ([]byte, error) {
	// When SmartCacheController is available, delegate entirely to it.
	if s.smartCache != nil {
		return s.smartCache.Get(ctx, key, size)
	}

	// Fallback: original cache chain for insert-only nodes without SmartCache.
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
				// PutNoCopy: data was just read from disk by os.ReadFile,
				// is owned by us, and never mutated downstream. Sharing
				// with the cache slot halves transient memory under
				// 16-worker wildcard scans.
				s.memCache.PutNoCopy(key, data)
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
				// PutNoCopy: peerData was just fetched, owned by us,
				// never mutated downstream. See L2 hit branch above for
				// the safety rationale.
				s.memCache.PutNoCopy(key, peerData)
				return peerData, nil
			}
			metrics.CacheMissesTotal.Inc("L3")
		}
	}

	data, err, shared := s.sfGroup.Do(key, func() ([]byte, error) {
		select {
		case s.dlSem <- struct{}{}:
			defer func() { <-s.dlSem }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
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

	// PutNoCopy: data was just downloaded by io.ReadAll inside the
	// singleflight callback, is owned by us, and never mutated
	// downstream — all callers pass it to parquet.OpenFile which only
	// reads. Sharing the buffer between the singleflight return value
	// and the cache slot halves transient memory under 16-worker
	// wildcard scans (the OOM trigger).
	s.memCache.PutNoCopy(key, data)
	return data, nil
}

func (s *Storage) Manifest() *manifest.Manifest {
	return s.manifest
}

func (s *Storage) HasDataForRange(startNs, endNs int64) bool {
	return s.manifest.HasDataForRange(startNs, endNs)
}

func (s *Storage) Close() error {
	if s.crossSignalClient != nil {
		s.crossSignalClient.Close()
	}
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
	s.updateLabelIndexImpl(f, true)
}

// updateLabelIndexNamesOnly registers field names (and MAP-key field names)
// without attempting to extract DISTINCT VALUES for promoted columns. Use
// this when the parquet.File was opened in a footer-only context (where
// data-page reads return nothing or fall back to truncated column-index
// stats — exactly how "notification-ser" first leaked into the label
// index from GetFieldNames over hundreds of files).
func (s *Storage) updateLabelIndexNamesOnly(f *parquet.File) {
	s.updateLabelIndexImpl(f, false)
}

func (s *Storage) updateLabelIndexImpl(f *parquet.File, extractValues bool) {
	// Columns that should have values extracted (use Parquet column names).
	// Only consulted when extractValues=true; footer-only callers must
	// register names without values to avoid surfacing truncated min/max.
	promotedWithValues := map[string]bool{
		"service.name":           true,
		"severity_text":          true,
		"k8s.namespace.name":     true,
		"k8s.deployment.name":    true,
		"k8s.node.name":          true,
		"deployment.environment": true,
		"cloud.region":           true,
		"span.name":              true,
	}

	// MAP columns whose keys should be expanded into individual field names
	mapColumns := map[string]bool{
		"resource.attributes": true,
		"log.attributes":      true,
		"span.attributes":     true,
		"scope.attributes":    true,
	}

	promotedParquetNames := make(map[string]bool)
	for _, m := range s.registry.PromotedColumns() {
		promotedParquetNames[m.ParquetColumn] = true
	}

	for _, name := range columnNames(f.Root()) {
		if mapColumns[name] {
			for _, k := range extractMapDistinctKeys(f, name) {
				if promotedParquetNames[k] {
					continue
				}
				s.labelIndex.Add(k, nil)
			}
			continue
		}

		internalName := name
		if m := s.registry.ResolveFromParquet(name); m != nil {
			internalName = m.InternalName
		}

		if !extractValues || !promotedWithValues[name] {
			s.labelIndex.Add(internalName, nil)
			continue
		}

		colIdx := findColumnIndex(f.Root(), name)
		if colIdx < 0 {
			s.labelIndex.Add(internalName, nil)
			continue
		}

		vals := extractDistinctFromStats(f, colIdx)
		counts := sampleValueFrequency(f, colIdx)
		s.labelIndex.AddWithValueCounts(internalName, vals, counts)
	}
}

// extractMapDistinctKeys reads the key leaf column of a MAP column and returns
// all distinct key names. This expands MAP columns like resource.attributes
// into individual field names matching VL's flat field model.
func extractMapDistinctKeys(f *parquet.File, mapColName string) []string {
	allCols := f.Schema().Columns()
	keyIdx := -1
	for i, path := range allCols {
		if len(path) >= 3 && path[0] == mapColName && path[2] == "key" {
			keyIdx = i
			break
		}
	}
	if keyIdx < 0 {
		return nil
	}

	seen := make(map[string]bool)
	for _, rg := range f.RowGroups() {
		chunks := rg.ColumnChunks()
		if keyIdx >= len(chunks) {
			continue
		}
		pages := chunks[keyIdx].Pages()
		buf := make([]parquet.Value, 256)
		for {
			page, err := pages.ReadPage()
			if err != nil {
				break
			}
			vr := page.Values()
			for {
				n, readErr := vr.ReadValues(buf[:])
				for i := 0; i < n; i++ {
					if !buf[i].IsNull() {
						if b := buf[i].Bytes(); len(b) > 0 && len(b) < 256 {
							seen[string(b)] = true
						}
					}
				}
				if readErr != nil {
					break
				}
			}
		}
		_ = pages.Close()
		break // One row group is enough
	}

	result := make([]string, 0, len(seen))
	for k := range seen {
		result = append(result, k)
	}
	return result
}

// sampleValueFrequency reads up to 1024 rows from the first row group
// and counts how many times each value appears. This gives a frequency
// distribution for proportional storage estimation in the breakdown API.
func sampleValueFrequency(f *parquet.File, colIdx int) map[string]int {
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		return nil
	}
	rg := rgs[0]
	if rg.NumRows() == 0 {
		return nil
	}
	rows := rg.Rows()
	buf := make([]parquet.Row, 1024)
	n, _ := rows.ReadRows(buf)
	_ = rows.Close()
	if n == 0 {
		return nil
	}
	counts := make(map[string]int)
	for i := 0; i < n; i++ {
		if colIdx < len(buf[i]) {
			val := buf[i][colIdx]
			if !val.IsNull() {
				if b := val.Bytes(); len(b) > 0 && len(b) < 256 && isPrintable(b) {
					counts[string(b)]++
				}
			}
		}
	}
	return counts
}

func extractDistinctFromStats(f *parquet.File, colIdx int) []string {
	// IMPORTANT: do NOT read distinct values from parquet column-index
	// min/max stats. parquet-go truncates those stat values (typically
	// at 16 bytes per Apache Parquet's PageIndex spec) so reading them
	// produces values like "notification-ser" alongside the full
	// "notification-service" in the label index — visible to operators
	// as duplicate service names in /select/jaeger/api/services and
	// every other GetFieldValues consumer.
	//
	// Data-page scanning produces full, untruncated values. For
	// low-cardinality fields like service.name this is also cheap —
	// the first row group of any file usually contains every distinct
	// value (datagen produces ~7 services rotated across millions of
	// rows; production traces follow similar shape).
	seen := make(map[string]bool)
	for _, rg := range f.RowGroups() {
		cols := rg.ColumnChunks()
		if colIdx >= len(cols) {
			continue
		}
		if rg.NumRows() == 0 {
			continue
		}
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
	sort.Strings(result)
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

func (s *Storage) SchemaRegistry() *schema.Registry {
	return s.registry
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

		// Parse timestamp (RFC3339Nano format from VL, fallback to raw integer)
		var tsNs int64
		if tsColIdx >= 0 {
			v := columns[tsColIdx].Values[rowIdx]
			if ns, ok := logstorage.TryParseTimestampRFC3339Nano(v); ok {
				tsNs = ns
			} else {
				tsNs, _ = strconv.ParseInt(v, 10, 64)
			}
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
		times[i] = s.registry.FormatField("_time", row.TimestampUnixNano)
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
		times[i] = s.registry.FormatField("_time", row.TimestampUnixNano)
		traceIDs[i] = row.TraceID
		spanIDs[i] = row.SpanID
		names[i] = row.SpanName
		services[i] = row.ServiceName
		durations[i] = s.registry.FormatField("duration", row.DurationNs)
		statusCodes[i] = s.registry.FormatField("status_code", int64(row.StatusCode))
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

// SetSelfFilterEnabled enables or disables hybrid fan-out self-filtering.
// When enabled and smartCache is available, RunQuery filters files to only
// those owned by this node according to the consistent hash ring. This
// prevents duplicate work when a select tier fans out queries to combined
// (insert+select) nodes that each process only their partition.
func (s *Storage) SetSelfFilterEnabled(enabled bool) {
	s.selfFilterEnabled = enabled
}

func (s *Storage) SetSelfAZ(az string) {
	s.selfAZ = az
	if s.peerHandler != nil {
		s.peerHandler.SetSelfAZ(az)
	}
}

func (s *Storage) SelfAZ() string { return s.selfAZ }

func (s *Storage) RefreshDiscovery(ctx context.Context) error {
	if _, err := s.discovery.DiscoverStorageNodes(ctx); err != nil {
		return fmt.Errorf("discover storage nodes: %w", err)
	}
	if _, err := s.discovery.PollPartitionList(ctx); err != nil {
		return fmt.Errorf("poll partition list: %w", err)
	}
	if s.peerCache != nil || s.bufferBridge != nil {
		peers, err := s.discovery.DiscoverPeers(ctx)
		if err != nil {
			return fmt.Errorf("discover peers: %w", err)
		}

		if s.selfAZ != "" && s.cfg.Peer.AZAware {
			peerZones := s.queryPeerAZs(ctx, peers)
			if s.peerCache != nil {
				s.peerCache.UpdatePeersWithZones(peerZones, s.selfAZ)

				stats := s.peerCache.StatsAZ()
				metrics.PeerSameAZMembers.Set(int64(stats.SameAZMembers))
				metrics.PeerCrossAZMembers.Set(int64(stats.CrossAZMembers))

				if s.cfg.Peer.AZMode == "strict" && stats.SameAZMembers < s.cfg.Peer.AZMinPeersPerAZ {
					logger.Warnf("strict AZ mode: only %d same-AZ peers (need %d); falling back to preferred",
						stats.SameAZMembers, s.cfg.Peer.AZMinPeersPerAZ)
				}
			}
			if s.bufferBridge != nil {
				s.bufferBridge.SetEndpointsWithZones(peerZones, s.selfAZ)
			}
		} else {
			if s.peerCache != nil {
				s.peerCache.UpdatePeers(peers)
			}
			if s.bufferBridge != nil {
				s.bufferBridge.SetEndpoints(peers)
			}
		}
	}
	return nil
}

func (s *Storage) queryPeerAZs(ctx context.Context, peers []string) map[string]string {
	peerZones := make(map[string]string, len(peers))
	for _, peer := range peers {
		az := s.fetchPeerAZ(ctx, peer)
		peerZones[peer] = az
	}
	return peerZones
}

func (s *Storage) fetchPeerAZ(ctx context.Context, peer string) string {
	url := fmt.Sprintf("http://%s/internal/cache/stats", peer)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ""
	}
	if s.cfg.Peer.AuthKey != "" {
		req.Header.Set("X-Peer-Auth-Key", s.cfg.Peer.AuthKey)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		AZ string `json:"az"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return ""
	}
	return result.AZ
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

// WarmMetadata loads file metadata from disk cache and S3 sidecars first,
// then batch-prefetches Parquet footers only for remaining files that still
// lack RowCount. This enables the manifest-only fast path for stats/hits
// queries from the first query after startup.
func (s *Storage) WarmMetadata(ctx context.Context) {
	// Phase 1: Load from disk cache (instant, no S3).
	diskLoaded := s.loadFileMetadataFromDisk()

	// Phase 2: Load from S3 sidecars (one GET per partition, much cheaper than footer reads).
	sidecarLoaded := s.manifest.LoadSidecars(ctx, s.pool.S3Client(), 16)

	// Phase 3: Footer prefetch for anything still missing.
	files := s.manifest.GetFilesForRange(0, 1<<62)
	var needEnrich []manifest.FileInfo
	for _, fi := range files {
		if fi.RowCount == 0 {
			needEnrich = append(needEnrich, fi)
		}
	}

	footerEnriched := 0
	if len(needEnrich) > 0 && s.footerCache != nil {
		fetched := prefetchFooters(ctx, s.pool, needEnrich, s.footerCache, 0)
		logger.Infof("metadata warmup: prefetched %d footers for %d files", fetched, len(needEnrich))

		for _, fi := range needEnrich {
			cached, ok := s.footerCache.Get(fi.Key)
			if !ok {
				continue
			}
			enriched := s.enrichFromCachedFooter(fi, cached)
			if enriched {
				footerEnriched++
			}
		}
	}

	// Phase 3b: Small files that footer prefetch skipped (< 32KB).
	// Download fully — they're tiny and cheaper than range reads.
	smallEnriched := 0
	if len(needEnrich) > 0 {
		var stillMissing []manifest.FileInfo
		enrichedKeys := make(map[string]bool, footerEnriched)
		for _, fi := range needEnrich {
			if _, ok := s.footerCache.Get(fi.Key); ok {
				enrichedKeys[fi.Key] = true
			}
		}
		for _, fi := range needEnrich {
			if !enrichedKeys[fi.Key] {
				stillMissing = append(stillMissing, fi)
			}
		}
		if len(stillMissing) > 0 {
			smallEnriched = s.enrichSmallFiles(ctx, stillMissing)
		}
	}

	logger.Infof("metadata warmup: disk=%d sidecar=%d footer=%d small=%d need_enrich=%d total_files=%d",
		diskLoaded, sidecarLoaded, footerEnriched, smallEnriched, len(needEnrich), len(files))

	// Phase 4: Save enriched metadata to disk for next restart.
	s.saveFileMetadataToDisk()
}

func (s *Storage) enrichFromCachedFooter(fi manifest.FileInfo, cached *CachedFooter) bool {
	var totalRows int64
	var minTs, maxTs int64
	tsIdx := findColumnIndex(cached.File.Root(), s.registry.TimestampColumn())
	for _, rg := range cached.File.RowGroups() {
		totalRows += rg.NumRows()
		if tsIdx < 0 {
			continue
		}
		cols := rg.ColumnChunks()
		if tsIdx >= len(cols) {
			continue
		}
		idx, err := cols[tsIdx].ColumnIndex()
		if err != nil || idx == nil || idx.NumPages() == 0 {
			continue
		}
		rgMin := idx.MinValue(0).Int64()
		rgMax := idx.MaxValue(idx.NumPages() - 1).Int64()
		if minTs == 0 || rgMin < minTs {
			minTs = rgMin
		}
		if rgMax > maxTs {
			maxTs = rgMax
		}
	}
	if totalRows > 0 {
		s.manifest.EnrichFileMetadata(fi.Key, totalRows, minTs, maxTs)
		return true
	}
	return false
}

func (s *Storage) enrichSmallFiles(ctx context.Context, files []manifest.FileInfo) int {
	if len(files) == 0 {
		return 0
	}
	concurrency := 16
	if concurrency > len(files) {
		concurrency = len(files)
	}

	taskCh := make(chan manifest.FileInfo, len(files))
	for _, fi := range files {
		taskCh <- fi
	}
	close(taskCh)

	var enriched int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fi := range taskCh {
				if ctx.Err() != nil {
					return
				}
				data, err := s.pool.Download(ctx, fi.Key)
				if err != nil || len(data) == 0 {
					continue
				}
				cached, _, err := ParseFooterFromData(fi.Key, data)
				if err != nil {
					continue
				}
				if s.footerCache != nil {
					s.footerCache.Put(fi.Key, cached)
				}
				if s.enrichFromCachedFooter(fi, cached) {
					mu.Lock()
					enriched++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return enriched
}

func (s *Storage) loadFileMetadataFromDisk() int {
	if s.persister == nil {
		return 0
	}
	fmc, err := s.persister.LoadFileMetadata()
	if err != nil {
		return 0
	}
	enriched := 0
	for _, entry := range fmc.Entries {
		if entry.RowCount > 0 {
			s.manifest.EnrichFileMetadata(entry.Key, entry.RowCount, entry.MinTimeNs, entry.MaxTimeNs)
			enriched++
		}
	}
	return enriched
}

func (s *Storage) saveFileMetadataToDisk() {
	if s.persister == nil {
		return
	}
	files := s.manifest.GetFilesForRange(0, 1<<62)
	var entries []cache.FileMetaEntry
	for _, fi := range files {
		if fi.RowCount > 0 {
			entries = append(entries, cache.FileMetaEntry{
				Key:               fi.Key,
				RowCount:          fi.RowCount,
				MinTimeNs:         fi.MinTimeNs,
				MaxTimeNs:         fi.MaxTimeNs,
				RawBytes:          fi.RawBytes,
				SchemaFingerprint: fi.SchemaFingerprint,
				Labels:            fi.Labels,
			})
		}
	}
	if len(entries) == 0 {
		return
	}
	fmc := &cache.FileMetadataCache{Entries: entries}
	if err := s.persister.SaveFileMetadata(fmc); err != nil {
		logger.Warnf("failed to save file metadata to disk: %v", err)
	} else {
		logger.Infof("saved %d file metadata entries to disk", len(entries))
	}
}

func (s *Storage) PersistState() error {
	if s.persister == nil {
		return nil
	}
	s.saveFileMetadataToDisk()
	return s.persister.SaveLabelIndex(s.labelIndex)
}

// SmartCache returns the SmartCacheController (nil if not configured).
func (s *Storage) SmartCache() *smartcache.Controller {
	return s.smartCache
}

// BloomCache returns the bloom index cache (nil if select is not enabled).
func (s *Storage) BloomCache() *bloomindex.BloomCache {
	return s.bloomCache
}

// WarmFile fetches a file into cache without returning the data.
func (s *Storage) WarmFile(ctx context.Context, key string) error {
	_, err := s.getFileData(ctx, key, 0)
	return err
}

// --- Adapter types bridging existing caches to smartcache interfaces ---

type l1Adapter struct{ lru *cache.LRU }

func (a *l1Adapter) Get(key string) ([]byte, bool)    { return a.lru.Get(key) }
func (a *l1Adapter) Put(key string, val []byte)       { a.lru.Put(key, val) }
func (a *l1Adapter) PutNoCopy(key string, val []byte) { a.lru.PutNoCopy(key, val) }

type l2Adapter struct{ dc *cache.DiskCache }

func (a *l2Adapter) Get(key string) ([]byte, bool) {
	if a.dc == nil {
		return nil, false
	}
	path, ok := a.dc.Get(key)
	if !ok {
		return nil, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		a.dc.Delete(key)
		return nil, false
	}
	return data, true
}

func (a *l2Adapter) Put(key string, data []byte) error {
	if a.dc == nil {
		return nil
	}
	_, err := a.dc.Put(key, data)
	return err
}

func (a *l2Adapter) Delete(key string) {
	if a.dc != nil {
		a.dc.Delete(key)
	}
}

func (a *l2Adapter) Size() int64 {
	if a.dc == nil {
		return 0
	}
	return a.dc.Size()
}

type peerLookupAdapter struct{ pc *peercache.PeerCache }

func (a *peerLookupAdapter) Lookup(key string) (string, bool) { return a.pc.Lookup(key) }
func (a *peerLookupAdapter) Members() []string                { return a.pc.Members() }
func (a *peerLookupAdapter) MemberCount() int                 { return len(a.pc.Members()) }

type peerFetchAdapter struct{ pc *peercache.PeerCache }

func (a *peerFetchAdapter) Fetch(ctx context.Context, peer, key string) ([]byte, bool, error) {
	return a.pc.Fetch(ctx, peer, key)
}

type localOnlyLookup struct{}

func (l *localOnlyLookup) Lookup(key string) (string, bool) { return "self", true }
func (l *localOnlyLookup) Members() []string                { return []string{"self"} }
func (l *localOnlyLookup) MemberCount() int                 { return 1 }

type s3Adapter struct {
	pool  *s3reader.ClientPool
	dlSem chan struct{}
}

func (a *s3Adapter) Download(ctx context.Context, key string) ([]byte, error) {
	if a.dlSem != nil {
		select {
		case a.dlSem <- struct{}{}:
			defer func() { <-a.dlSem }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return a.pool.Download(ctx, key)
}
