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
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
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
	catalog           *pmeta.Store // unified field/value catalog; nil unless --pmeta
	persister         *cache.Persister
	discovery         *discovery.Discovery
	peerCache         *peercache.PeerCache
	peerHandler       *peercache.Handler
	writer            *BatchWriter
	bufferBridge      *BufferBridge
	localBuffer       LocalBuffer
	tombstones        *delete.TombstoneStore
	smartCache        *smartcache.Controller
	bloomIdx          *bloomindex.Index
	bloomCache        *bloomindex.BloomCache
	footerCache       *FooterCache
	crossSignalClient *crosssignal.Client
	s3Prefix          string
	selfAZ            string
	selfFilterEnabled bool
	dlSem             chan struct{}
	// bounds holds the K8s-style request/limit Bound set for the 5
	// resource surfaces. Mirror of the logs module field. Wired runtime
	// surfaces: FileWorkers (Acquire in worker loop), CacheMemory and
	// SmartCacheDisk (SetBound on the underlying caches), QueryMaxRows
	// (per-query reservation in RunQuery). The S3 download bound is
	// constructed for metric exposure only — traces' getFileData path
	// has no dlSem-based admission point to wire through.
	bounds *resourceBoundSet
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
	if cap := cfg.Cache.LabelIndexMaxFields; cap > 0 {
		labelIdx.SetMaxFields(cap)
	}

	var pers *cache.Persister
	if cfg.Manifest.PersistPath != "" {
		p, err := cache.NewPersister(cfg.Manifest.PersistPath)
		if err != nil {
			logger.Warnf("persister init failed: %s", err)
		} else {
			pers = p
			if saved, err := p.LoadLabelIndex(); err == nil {
				labelIdx = saved
				if cap := cfg.Cache.LabelIndexMaxFields; cap > 0 {
					// SetMaxFields applies the cap to the just-loaded
					// index, evicting if the snapshot held more than
					// the new cap allows.
					labelIdx.SetMaxFields(cap)
				}
				logger.Infof("recovered label index from disk; labels=%d", saved.Len())
			}
		}
	}
	// S3 recovery happens later, in RefreshManifest, once the pool is
	// constructed. Local disk first (no network round-trip), S3 second
	// (cluster-wide recovery). The two are merged via MergeFrom so we
	// don't lose either source's contribution.

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

	// K8s-style request/limit bounds for all 5 resource surfaces.
	// Mirror of the logs module. Bound construction populates the
	// per-surface request/limit info gauges at startup, emits a
	// deprecation warning when the legacy single-value alias is set,
	// and exposes the Acquire path for surfaces with runtime wiring.
	bounds := newResourceBoundSet(cfg)
	maxDL := int(bounds.S3Downloads.Config().Limit)
	if maxDL <= 0 {
		maxDL = 16
	}

	// Wire the cache-memory and smart-cache-disk bounds INTO the
	// existing in-process caches so each Put traverses the K8s-style
	// admission gate. TryAcquire-based: rejection means "skip caching"
	// (best-effort), not "block the write".
	if memCache != nil && bounds.CacheMemory != nil {
		memCache.SetBound(bounds.CacheMemory)
	}
	if diskCacheInst != nil && bounds.SmartCacheDisk != nil {
		diskCacheInst.SetBound(bounds.SmartCacheDisk)
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
	if cfg.SelectEnabled() {
		// Initial cap: operator-configured value or the legacy 10K
		// default. The cap is re-tuned after every RefreshFromS3 via
		// retuneFooterCache() so a growing manifest scales it up
		// without requiring a config reload.
		initialCap := cfg.Cache.FooterMaxItems
		if initialCap <= 0 {
			initialCap = 10000
		}
		fc = NewFooterCache(initialCap)
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

	// pmeta field/value catalog (experimental, --pmeta). nil when disabled, so
	// the hot flush/query paths are unchanged by default.
	catalog := newCatalogStore(cfg.Pmeta, prefix)
	if catalog != nil && bw != nil {
		bw.catalogObserver = &catalogObserver{store: catalog, sketch: sketchSet(cfg.Pmeta.AlwaysSketchFields), pool: pool}
	}

	return &Storage{
		cfg:               cfg,
		pool:              pool,
		manifest:          m,
		catalog:           catalog,
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
		bloomIdx:          bloomindex.New(),
		bloomCache:        bc,
		footerCache:       fc,
		crossSignalClient: csClient,
		s3Prefix:          prefix,
		dlSem:             make(chan struct{}, maxDL),
		bounds:            bounds,
	}, nil
}

// StartWriter begins the background flush loop. Call after New().
func (s *Storage) StartWriter() {
	if s.writer == nil {
		return
	}
	if s.smartCache != nil {
		s.writer.SetFlushCacheCallback(func(fileKey string, data []byte) {
			cacheOnFlush(s.smartCache, fileKey, data)
		})
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
				// PutNoCopy: data was just read from disk, owned by us,
				// never mutated downstream. Mirror of logs module.
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
				// PutNoCopy: peer fetch buffer, owned by us, never
				// mutated downstream. Mirror of logs module.
				s.memCache.PutNoCopy(key, peerData)
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

	// PutNoCopy: freshly downloaded data, owned by us, never mutated
	// downstream. Halves transient memory under 16-worker wildcard
	// scans (per heap-diff). Mirror of logs module.
	s.memCache.PutNoCopy(key, data)
	return data, nil
}

func (s *Storage) SetSelfFilterEnabled(enabled bool) {
	s.selfFilterEnabled = enabled
}

func (s *Storage) BloomCache() *bloomindex.BloomCache {
	return s.bloomCache
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
	// Option B: flush + close the logstorage-native buffer so the persistent
	// data dir captures the last sub-FlushInterval window before exit.
	if s.localBuffer != nil {
		s.localBuffer.Close()
		logger.Infof("Option B logstore buffer flushed and closed")
	}
	if s.persister != nil {
		if err := s.persister.SaveLabelIndex(s.labelIndex); err != nil {
			logger.Warnf("failed to persist label index: %s", err)
		} else {
			logger.Infof("persisted label index; labels=%d", s.labelIndex.Len())
		}
	}
	// Final upload to S3 on graceful shutdown so the next pod start (on
	// any node) can recover the index without rebuilding from parquet
	// reads. Independent of the local-disk persister above.
	if s.labelIndex != nil && s.labelIndex.Len() > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.PersistLabelIndexToS3(ctx); err != nil {
			logger.Warnf("failed to persist label index to S3: %s", err)
		} else {
			logger.Infof("persisted label index to S3; labels=%d", s.labelIndex.Len())
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
	// Derived from the shared dimensional label set so the Cardinality Explorer
	// surfaces real cardinality for every dedicated dimension and can't drift
	// from the manifest label index (same source). High-card id-like columns are
	// absent by design → name-only (bloom-indexed). Twin of the root module.
	// Only consulted when extractValues=true.
	promotedWithValues := make(map[string]bool, len(schema.LogLabelColumns)+len(schema.TraceLabelColumns))
	for _, c := range schema.LogLabelColumns {
		promotedWithValues[c.Name] = true
	}
	for _, c := range schema.TraceLabelColumns {
		promotedWithValues[c.Name] = true
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

	// Tier-2 slot columns surface under the configured name from THIS file's
	// footer KV (config-change-robust); unmapped slots are omitted.
	slotNames := fileSlotMapping(f)
	isSlotCol := make(map[string]bool, len(schema.DedicatedSlotColumns))
	for _, c := range schema.DedicatedSlotColumns {
		isSlotCol[c] = true
	}

	for _, name := range columnNames(f.Root()) {
		if isSlotCol[name] {
			if cfgName, ok := slotNames[name]; ok && cfgName != "" {
				s.labelIndex.Add(cfgName, nil)
			}
			continue
		}
		if mapColumns[name] {
			prefix := mapColumnToAttrPrefix(name)
			for _, k := range extractMapDistinctKeys(f, name) {
				if schema.VTTopLevelSpanAttrKeys[k] {
					s.labelIndex.Add(k, nil)
					continue
				}
				// Dual-read: a promoted key found in the MAP = an OLD
				// (pre-promotion) file. Index it under the prefixed name to
				// match the column form (InternalName carries the prefix).
				// labelIndex.Add is idempotent, so new files (key in the
				// column) never double-count.
				s.labelIndex.Add(prefix+k, nil)
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
		s.labelIndex.Add(internalName, vals)
	}

	// _msg is VL's body field — always present but not stored in parquet MAPs.
	// VT always reports it in field_names; add it for parity.
	s.labelIndex.Add("_msg", nil)
}

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
		break
	}

	result := make([]string, 0, len(seen))
	for k := range seen {
		result = append(result, k)
	}
	return result
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
	// Mirrors the equivalent fix in the logs module.
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

// LocalBuffer is the narrow query surface of the Option B logstorage-native
// buffer (membuffer.Store). Declared as an interface to keep parquets3's
// imports narrow. When set (BufferEngine=logstore, co-located insert+select),
// the SELECT path serves the recent/unflushed window from it via the same
// engine the S3-Parquet scan uses — no struct→DataBlock conversion.
type LocalBuffer interface {
	RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error
	// Close flushes the in-memory window to the persistent data dir and
	// releases the store — called on graceful shutdown so a clean restart
	// loses nothing, not even the sub-FlushInterval window.
	Close()
}

// SetLocalBuffer wires the co-located logstorage-native buffer into the query
// path (Option B P3). nil falls back to the BufferBridge HTTP path.
func (s *Storage) SetLocalBuffer(lb LocalBuffer) {
	s.localBuffer = lb
}

// SetTombstoneStore injects a TombstoneStore for query-time row filtering.
func (s *Storage) SetTombstoneStore(ts *delete.TombstoneStore) {
	s.tombstones = ts
}

// TombstoneStore returns the configured TombstoneStore (nil if not set).
func (s *Storage) TombstoneStore() *delete.TombstoneStore {
	return s.tombstones
}

// PmetaCardinality returns the global HLL distinct-value estimate for a field
// from the pmeta catalog, or 0 if pmeta is off or the field has no sketch — the
// accurate cardinality source the stats API prefers over the lazy, 100-capped
// LabelIndex count. Twin of the root module.
func (s *Storage) PmetaCardinality(field string) uint64 {
	if s.catalog == nil {
		return 0
	}
	return s.catalog.Cardinality(field)
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

// FooterCache returns the storage's footer cache (or nil if disabled).
// Mirror of the same accessor in internal/storage/parquets3/storage.go.
func (s *Storage) FooterCache() *FooterCache {
	return s.footerCache
}

// PrefetchFootersByKeys mirrors the same method in
// internal/storage/parquets3/storage.go — see there for the rationale.
func (s *Storage) PrefetchFootersByKeys(ctx context.Context, keys []string, concurrency int) {
	if s.pool == nil || s.footerCache == nil || len(keys) == 0 {
		return
	}
	files := make([]manifest.FileInfo, 0, len(keys))
	for _, k := range keys {
		if fi, ok := s.manifest.GetFileByKey(k); ok {
			files = append(files, fi)
		}
	}
	if len(files) == 0 {
		return
	}
	fetched := prefetchFooters(ctx, s.pool, files, s.footerCache, concurrency, s.footerPrefetchBytes())
	logger.Infof("footer-cache snapshot prefetch: hydrated %d of %d snapshot keys (manifest matched %d)",
		fetched, len(keys), len(files))
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

// traceRowsToDataBlock converts in-memory TraceRow slices (buffer-bridge
// rows for not-yet-flushed data) to a columnar DataBlock.
//
// It MUST emit the same filterable columns the file-scan path produces —
// in particular `_stream`. VT's Jaeger getTraceIDList and Tempo
// GetTraceList step 1 filter with `_stream:{resource_attr:service.name="X"}`
// (a stream selector). If the buffer DataBlock lacks the `_stream` column
// those filters match ZERO buffer rows, so the freshest (still-buffered)
// spans are invisible to every Jaeger/Tempo search even though wildcard
// `*` and `trace_id:"X"` queries find them. That was the live symptom:
// cold Jaeger/Tempo returned nothing for recent data while hot VT served
// it from memory. Promoted attribute columns are emitted under BOTH their
// parquet name (e.g. `service.name`) and internal alias
// (`resource_attr:service.name`) so a filter spelling either dialect
// matches — mirroring the dual-emission in the file-scan path.
func (s *Storage) traceRowsToDataBlock(rows []schema.TraceRow) *logstorage.DataBlock {
	if len(rows) == 0 {
		return nil
	}

	n := len(rows)
	// Column accumulator: name -> per-row values. Built once; emitted in
	// a stable order at the end.
	cols := make(map[string][]string)
	colOrder := make([]string, 0, 32)
	get := func(name string) []string {
		v, ok := cols[name]
		if !ok {
			v = make([]string, n)
			cols[name] = v
			colOrder = append(colOrder, name)
		}
		return v
	}
	// set fills value at row i; for promoted attrs, dual-emits under the
	// internal alias too.
	set := func(name string, i int, val string) {
		if val == "" {
			return
		}
		get(name)[i] = val
	}
	setDual := func(parquetName, internalName string, i int, val string) {
		if val == "" {
			return
		}
		get(parquetName)[i] = val
		get(internalName)[i] = val
	}

	for i, row := range rows {
		set("_time", i, s.registry.FormatField("_time", row.TimestampUnixNano))
		set("trace_id", i, row.TraceID)
		set("span_id", i, row.SpanID)
		set("parent_span_id", i, row.ParentSpanID)
		set("name", i, row.SpanName)
		set("duration", i, s.registry.FormatField("duration", row.DurationNs))
		set("status_code", i, s.registry.FormatField("status_code", int64(row.StatusCode)))
		set("status_message", i, row.StatusMessage)
		set("kind", i, s.registry.FormatField("kind", int64(row.SpanKind)))
		// Stream selector + id — the load-bearing columns for step-1
		// `_stream:{...}` filters.
		set("_stream", i, row.Stream)
		set("_stream_id", i, row.StreamID)
		set("scope_name", i, row.ScopeName)
		// Promoted resource attributes (dual-emitted).
		setDual("service.name", "resource_attr:service.name", i, row.ServiceName)
		setDual("deployment.environment", "resource_attr:deployment.environment", i, row.DeployEnv)
		setDual("cloud.region", "resource_attr:cloud.region", i, row.CloudRegion)
		setDual("host.name", "resource_attr:host.name", i, row.HostName)
		setDual("k8s.namespace.name", "resource_attr:k8s.namespace.name", i, row.K8sNamespaceName)
		setDual("k8s.pod.name", "resource_attr:k8s.pod.name", i, row.K8sPodName)
		setDual("k8s.deployment.name", "resource_attr:k8s.deployment.name", i, row.K8sDeploymentName)
		setDual("k8s.node.name", "resource_attr:k8s.node.name", i, row.K8sNodeName)
		// Promoted span attributes (dual-emitted) — used by Jaeger/Tempo
		// tag filters (e.g. http.status_code=200).
		setDual("http.method", "span_attr:http.method", i, row.HTTPMethod)
		setDual("http.status_code", "span_attr:http.status_code", i, row.HTTPStatusCode)
		setDual("http.url", "span_attr:http.url", i, row.HTTPUrl)
		setDual("db.system", "span_attr:db.system", i, row.DBSystem)
		setDual("db.statement", "span_attr:db.statement", i, row.DBStatement)
		// start_time_unix_nano is a promoted top-level column the Jaeger/Tempo
		// GetTrace index lookup reads via
		//   trace_id:=X | stats min(_time) _time,
		//                       min(start_time_unix_nano) start_time,
		//                       max(end_time_unix_nano) end_time
		// (see vtstorage_adapter.rewriteTraceIndexQuery). Without it the
		// stats query over still-buffered spans yields empty bounds, GetTrace
		// can't locate the trace, and returns 404 "trace not found" — which
		// makes Grafana's trace panel crash with
		// "Cannot read properties of undefined (reading 'spanID')" on the
		// log→trace drilldown for any recently-ingested trace.
		set("start_time_unix_nano", i, s.registry.FormatField("start_time_unix_nano", row.StartTimeUnixNano))
		for k, v := range row.ResourceAttributes {
			set("resource_attr:"+k, i, v)
		}
		// Span attributes. OTLP metadata fields VT keeps top-level
		// (end_time_unix_nano, flags, dropped_*_count, …) MUST surface
		// WITHOUT the span_attr: prefix so the GetTrace stats lookup's
		// max(end_time_unix_nano) resolves — exactly as the file-scan path
		// does via schema.VTTopLevelSpanAttrKeys. Everything else keeps the
		// span_attr: prefix.
		for k, v := range row.SpanAttributes {
			if schema.VTTopLevelSpanAttrKeys[k] {
				set(k, i, v)
			} else {
				set("span_attr:"+k, i, v)
			}
		}
	}

	blockCols := make([]logstorage.BlockColumn, 0, len(colOrder))
	for _, name := range colOrder {
		blockCols = append(blockCols, logstorage.BlockColumn{Name: name, Values: cols[name]})
	}
	db := &logstorage.DataBlock{}
	db.SetColumns(blockCols)
	return db
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
	if err := s.manifest.RefreshFromS3(ctx, s.pool.S3Client()); err != nil {
		return err
	}
	s.retuneFooterCache()
	s.loadBloomIndex(ctx)
	s.loadLabelIndexFromS3(ctx)
	// Persist label index to S3 alongside the bloom index. The local-disk
	// persister keeps doing its job on graceful shutdown; this gives us
	// cluster-wide recovery for pods that come up on a different node
	// (no local volume) and for crash scenarios where shutdown didn't run.
	if s.cfg.InsertEnabled() && s.labelIndex.Len() > 0 {
		if err := s.PersistLabelIndexToS3(ctx); err != nil {
			logger.Warnf("label index S3 persist failed: %s", err)
		}
	}
	return nil
}

func (s *Storage) bloomIndexKey() string {
	return s.s3Prefix + "_bloom_index.bin"
}

func (s *Storage) loadBloomIndex(ctx context.Context) {
	key := s.bloomIndexKey()
	data, err := s.pool.Download(ctx, key)
	if err != nil {
		return
	}
	idx, err := bloomindex.Unmarshal(data)
	if err != nil {
		logger.Warnf("bloom index unmarshal failed: %s", err)
		return
	}
	s.bloomIdx.MergeFrom(idx)
	logger.Infof("bloom index loaded from S3; entries=%d", idx.Len())
}

// Footer-cache auto-tune bounds. The auto-tune target is
// (manifest files) / footerCacheFileCountDivisor, clamped to
// [footerCacheMinItems, footerCacheMaxItems]. These constants live
// here (not as cfg knobs) because they're internal sizing heuristics —
// operators tune the cap via cfg.Cache.FooterMaxItems, which short-
// circuits the auto-tune entirely when set.
//
// Defaults rationale:
//   - 1/2000 ≈ 0.05% of corpus → ~25K entries at 50M files.
//   - 10K min keeps small deployments (single-host dev) from churning.
//   - 100K max bounds the working set at ~500 MB (5 KB/entry).
const (
	footerCacheFileCountDivisor = 2000
	footerCacheMinItems         = 10000
	footerCacheMaxItems         = 100000
)

// retuneFooterCache re-sizes the footer cache after a successful
// manifest refresh. Sized at 0.05% of the manifest's file count to
// give roughly 25K items per 50M file corpus (~125 MB working set),
// clamped to [10000, 100000] so small deployments don't see noise
// and huge ones don't blow memory.
//
// If an explicit cfg.Cache.FooterMaxItems is set, that takes precedence
// over the auto-tune — operators always retain manual control.
func (s *Storage) retuneFooterCache() {
	if s.footerCache == nil {
		return
	}
	target := s.cfg.Cache.FooterMaxItems
	if target <= 0 {
		// Auto-tune: fraction of file count, clamped.
		files := s.manifest.LiveAggregate().Files
		target = files / footerCacheFileCountDivisor
		if target < footerCacheMinItems {
			target = footerCacheMinItems
		}
		if target > footerCacheMaxItems {
			target = footerCacheMaxItems
		}
	}
	if target == s.footerCache.MaxItems() {
		return
	}
	evicted := s.footerCache.Resize(target)
	logger.Infof("footer cache retuned; max_items=%d, evicted=%d, files_in_manifest=%d",
		target, evicted, s.manifest.LiveAggregate().Files)
}

// labelIndexKey is the S3 key where the label index is persisted so
// pods that lose their local disk volume (or fresh pods coming up on a
// different node) can recover the cluster's accumulated label
// knowledge without re-scanning every parquet file in the bucket. At
// PB-scale that's the difference between a sub-second startup and a
// multi-hour label-index rebuild from full file reads.
func (s *Storage) labelIndexKey() string {
	return s.s3Prefix + "_label_index.json"
}

// loadLabelIndexFromS3 merges any S3-persisted label index into the
// in-memory one. Called after local-disk recovery (storage.go:109) so
// the union of both sources is available. Failure is logged but not
// fatal — pods can still build the index lazily through
// WarmLabelIndex + per-query updates.
func (s *Storage) loadLabelIndexFromS3(ctx context.Context) {
	key := s.labelIndexKey()
	data, err := s.pool.Download(ctx, key)
	if err != nil {
		return
	}
	idx, err := cache.UnmarshalLabelIndex(data)
	if err != nil {
		logger.Warnf("label index unmarshal failed: %s", err)
		return
	}
	s.labelIndex.MergeFrom(idx)
	logger.Infof("label index loaded from S3; labels=%d", s.labelIndex.Len())
}

// PersistLabelIndexToS3 uploads the in-memory label index to S3. Called
// periodically by the background ticker started in StartWriter, and on
// graceful shutdown so the most recent state is captured.
//
// A failure here is non-fatal: the local disk persister keeps writing
// the same data on shutdown, and the next periodic tick or restart
// will retry. We log at Warn so an operator can spot persistent
// failures without paging on transient ones.
func (s *Storage) PersistLabelIndexToS3(ctx context.Context) error {
	if s.labelIndex == nil || s.labelIndex.Len() == 0 || s.pool == nil {
		return nil
	}
	data, err := cache.MarshalLabelIndex(s.labelIndex)
	if err != nil {
		return fmt.Errorf("marshal label index: %w", err)
	}
	key := s.labelIndexKey()
	return s.pool.Upload(ctx, key, data)
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
	s.saveFileMetadataToDisk()
	return s.persister.SaveLabelIndex(s.labelIndex)
}

func (s *Storage) WarmMetadata(ctx context.Context) {
	diskLoaded := s.loadFileMetadataFromDisk()

	// pmeta read-flip: enrich from the in-RAM fileMetaFacet first (no S3), then the
	// `_file_metadata.json` sidecars only for the partitions the facet didn't cover.
	facetEnriched := 0
	var uncovered []string
	if s.catalog != nil {
		facetEnriched, uncovered = s.manifest.EnrichFromProvider(catalogFileMetaProvider{store: s.catalog})
	}
	sidecarLoaded := 0
	switch {
	case s.catalog == nil:
		sidecarLoaded = s.manifest.LoadSidecars(ctx, s.pool.S3Client(), 16)
	case len(uncovered) > 0:
		sidecarLoaded = s.manifest.LoadSidecarsForPartitions(ctx, s.pool.S3Client(), 16, uncovered)
	}

	files := s.manifest.GetFilesForRange(0, 1<<62)
	var needEnrich []manifest.FileInfo
	for _, fi := range files {
		if fi.RowCount == 0 {
			needEnrich = append(needEnrich, fi)
		}
	}

	footerEnriched := 0
	if len(needEnrich) > 0 && s.footerCache != nil {
		fetched := prefetchFooters(ctx, s.pool, needEnrich, s.footerCache, 0, s.footerPrefetchBytes())
		logger.Infof("metadata warmup: prefetched %d footers for %d files", fetched, len(needEnrich))

		for _, fi := range needEnrich {
			cached, ok := s.footerCache.Get(fi.Key)
			if !ok {
				continue
			}
			if s.enrichFromCachedFooter(fi, cached) {
				footerEnriched++
			}
		}
	}

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

	logger.Infof("metadata warmup: disk=%d facet=%d sidecar=%d footer=%d small=%d need_enrich=%d total_files=%d",
		diskLoaded, facetEnriched, sidecarLoaded, footerEnriched, smallEnriched, len(needEnrich), len(files))

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
		// Aggregate across all pages — see columnIndexTimeBounds
		// (storage_query.go). Positional MinValue(0)/MaxValue(N-1) bounds
		// understate the manifest time range when pages are not time-sorted.
		rgMin, rgMax := columnIndexTimeBounds(idx)
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
