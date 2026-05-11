package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

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
	"github.com/ReliablyObserve/victoria-lakehouse/internal/smartcache"
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
	smartCache   *smartcache.Controller
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
			S3Fetcher:    &s3Adapter{pool: pool},
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
		smartCache:   sc,
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

// SmartCache returns the SmartCacheController (nil if not configured).
func (s *Storage) SmartCache() *smartcache.Controller {
	return s.smartCache
}

// WarmFile fetches a file into cache without returning the data.
func (s *Storage) WarmFile(ctx context.Context, key string) error {
	_, err := s.getFileData(ctx, key, 0)
	return err
}

// --- Adapter types bridging existing caches to smartcache interfaces ---

type l1Adapter struct{ lru *cache.LRU }

func (a *l1Adapter) Get(key string) ([]byte, bool) { return a.lru.Get(key) }
func (a *l1Adapter) Put(key string, val []byte)     { a.lru.Put(key, val) }

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

type s3Adapter struct{ pool *s3reader.ClientPool }

func (a *s3Adapter) Download(ctx context.Context, key string) ([]byte, error) {
	return a.pool.Download(ctx, key)
}

