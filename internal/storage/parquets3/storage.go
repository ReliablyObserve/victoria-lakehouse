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
	"github.com/ReliablyObserve/victoria-lakehouse/internal/resourcebounds"
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
	localBuffer       LocalBuffer
	tombstones        *delete.TombstoneStore
	smartCache        *smartcache.Controller
	bloomCache        *bloomindex.BloomCache
	catalog           *pmeta.Store // unified field/value catalog; nil unless --pmeta
	footerCache       *FooterCache
	fileBloomCache    *BloomFileCache
	crossSignalClient *crosssignal.Client
	selfAZ            string
	selfFilterEnabled bool
	// dlSem is the legacy channel-based S3 download concurrency
	// gate. Kept as the wire-level mechanism (preserves observable
	// semantics 1:1) while s3DownloadsBound provides the K8s-style
	// request/limit/metrics surface that operators see in dashboards.
	dlSem            chan struct{}
	s3DownloadsBound *resourcebounds.Bound
	// bounds holds the K8s-style request/limit Bound set for all
	// 5 resource surfaces. Surface owners read their bound here;
	// the foundation lives in internal/resourcebounds with metric
	// wiring through internal/metrics.
	//
	// Runtime acquire wiring (all 5 surfaces gated as of this PR):
	//   - S3Downloads:    channel-first admit + bound tick
	//                     (getFileData → s3DownloadsBound.Acquire
	//                     after dlSem admit).
	//   - FileWorkers:    per-file Acquire in fileWorkerLoop
	//                     (storage_query.go).
	//   - CacheMemory:    TryAcquire on LRU.Put + Release on
	//                     evict/Delete/Clear; wired here via
	//                     memCache.SetBound, applied in cache/lru.go.
	//   - SmartCacheDisk: TryAcquire on DiskCache.Put + Release on
	//                     evict/Delete/Clear; wired here via
	//                     diskCacheInst.SetBound, applied in
	//                     cache/disk.go.
	//   - QueryMaxRows:   per-query reservation via
	//                     acquireQueryMaxRowsBudget (storage_query.go).
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
	// Lock the manifest's tenant-scoped LIST to this tier's S3 prefix
	// so a logs pod never enumerates `<tenant>/traces/` (and vice
	// versa). The signal suffix is mandatory at construction-time —
	// mains still call SetSignalSuffix() explicitly as an extra
	// guard, but defaulting here means any future caller of Storage
	// can't forget. Empty Mode falls back to "logs/" to match
	// LogsProfile above.
	signalSuffix := "logs/"
	if cfg.Mode == config.ModeTraces {
		signalSuffix = "traces/"
	}
	m.SetSignalSuffix(signalSuffix)

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

	// K8s-style request/limit bounds for all 5 resource surfaces.
	// Bound construction populates the per-surface request/limit
	// info gauges at startup, emits a deprecation warning when any
	// legacy single-value alias is set, and exposes the Acquire path
	// for surfaces with runtime wiring.
	//
	// As of this PR all 5 surfaces have runtime acquire wiring (see
	// the Storage.bounds field doc above for the per-surface call
	// sites). Operators see the K8s-style triple (request, limit,
	// usage) plus the 429-shaped rejected_total counter for every
	// surface — the bound is load-bearing across the board, not
	// metric-exposure-only.
	bounds := newResourceBoundSet(cfg)
	s3DownloadsBound := bounds.S3Downloads
	maxDL := int(s3DownloadsBound.Config().Limit)
	if maxDL <= 0 {
		maxDL = 16
	}

	// Wire the cache-memory and smart-cache-disk bounds INTO the
	// existing in-process caches so each Put traverses the K8s-style
	// admission gate. The bound is applied non-blockingly (TryAcquire
	// — see resourcebounds.Bound.TryAcquire): when exhausted the cache
	// becomes best-effort and the operator sees rejected_total tick
	// up. Release runs on eviction/Delete/Clear via the per-entry
	// boundRelease closure.
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
		s3DownloadsBound:  s3DownloadsBound,
		bounds:            bounds,
	}

	// pmeta unified metadata layer (--pmeta). The catalog store is built for
	// EVERY role — a select-only pod (bw == nil) has no flush feed but still
	// serves dropdowns/file-meta/bloom from the bundle-warmed facets; gating it
	// on the writer left read-only pods scanning while writer pods used the index.
	if cfg.Pmeta.Enabled {
		s.catalog = newCatalogStore(cfg.Pmeta, prefix)
		// The HLL cardinality tap reads id columns straight off the row structs;
		// only trace_id/span_id have struct fields. Other configured sketch
		// fields are still capped/refused by the catalog but get no sketch.
		for _, f := range cfg.Pmeta.AlwaysSketchFields {
			if f != "trace_id" && f != "span_id" {
				logger.Warnf("pmeta: always-sketch field %q has no HLL tap (only trace_id/span_id are tapped); it is excluded from the catalog but lakehouse_catalog_field_cardinality will not report it", f)
			}
		}
	}

	if bw != nil {
		if s.catalog != nil {
			bw.catalogObserver = &catalogObserver{store: s.catalog, sketch: sketchSet(cfg.Pmeta.AlwaysSketchFields), pool: s.pool}
		}

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
func (s *Storage) StartWriter() {
	if s.writer == nil {
		return
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
			// Once the channel admits us we tick the K8s-style bound
			// for metric visibility (request/limit/acquired/outstanding
			// gauges read by operator dashboards). Acquire MUST come
			// AFTER the channel claim — the channel is the wire-level
			// blocking gate, and acquiring the bound first would
			// double-gate under contention (both carry the same Limit
			// semantics). The bound's release runs from the deferred
			// release; bound errors here are non-fatal (the download
			// already has its slot).
			if s.s3DownloadsBound != nil {
				if relBound, boundErr := s.s3DownloadsBound.Acquire(ctx, 1); boundErr == nil {
					defer relBound()
				}
			}
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
	// Columns that should have values extracted (Parquet column names). Derived
	// from the shared dimensional label set (schema.LogLabelColumns /
	// TraceLabelColumns) so the Cardinality Explorer surfaces real cardinality
	// for every dedicated dimension (k8s.cluster.name, service.version, …) and
	// can't drift from the manifest label index, which draws from the same
	// source. High-card id-like columns are absent from that set by design, so
	// they stay name-only (bloom-indexed, not value-counted). Only consulted when
	// extractValues=true; footer-only callers register names without values.
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

	// Tier-2 slot columns (ded_sNN) carry an operator-configured attribute whose
	// name lives in THIS file's footer KV — read it so the field surfaces under
	// the configured name (e.g. "tenant_id"), not the raw slot name, and stays
	// correct even if the live config changed after the file was written.
	slotNames := fileSlotMapping(f)
	isSlotCol := make(map[string]bool, len(schema.DedicatedSlotColumns))
	for _, c := range schema.DedicatedSlotColumns {
		isSlotCol[c] = true
	}

	for _, name := range columnNames(f.Root()) {
		if mapColumns[name] {
			for _, k := range extractMapDistinctKeys(f, name) {
				// Dual-read: a promoted key found in the MAP means this is an
				// OLD file (written before the key was promoted to a column).
				// Index it — labelIndex.Add is idempotent, so NEW files (key in
				// the column, handled by the non-map branch below) never
				// double-count. Skipping here would silently drop the field
				// from the index for every pre-promotion file.
				s.labelIndex.Add(k, nil)
			}
			continue
		}

		// Tier-2 slot column → surface under the operator-configured name from
		// this file's footer KV. An unmapped slot (no footer entry) is skipped
		// so the raw ded_sNN never leaks into the field list.
		if isSlotCol[name] {
			if cfgName, ok := slotNames[name]; ok && cfgName != "" {
				s.labelIndex.Add(cfgName, nil)
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
// fileSlotMapping reads the Tier-2 dedicated-slot name binding from a file's
// Parquet footer KV (DedicatedSlotsMetaKey). Returns nil if absent/garbage —
// callers then skip the raw slot column. This is what makes Tier-2 files
// self-describing: a file's slots remap by ITS OWN footer, correct even after
// the live config changes.
func fileSlotMapping(f *parquet.File) schema.SlotMapping {
	meta := f.Metadata()
	if meta == nil {
		return nil
	}
	for _, kv := range meta.KeyValueMetadata {
		if kv.Key == schema.DedicatedSlotsMetaKey {
			return schema.UnmarshalSlotMapping([]byte(kv.Value))
		}
	}
	return nil
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

// DiskCacheBytes is this node's on-disk cache footprint in bytes (0 if no disk
// cache). Local to this instance — see the Storage Overview metadata tiles.
func (s *Storage) DiskCacheBytes() int64 {
	if s.diskCache == nil {
		return 0
	}
	return s.diskCache.Size()
}

// PmetaResidentBytes is this node's in-RAM metadata footprint — the pmeta
// catalog/bloom/file-meta bundles + interning dict (0 unless --pmeta). Local to
// this instance.
func (s *Storage) PmetaResidentBytes() int64 {
	if s.catalog == nil {
		return 0
	}
	return s.catalog.ResidentBytes()
}

// PmetaPersistedBytes is the cluster's on-S3 metadata footprint — the sum of
// every resident bundle's encoded size, tracked incrementally on persist/warm/
// compaction (no S3 LIST). 0 unless --pmeta.
func (s *Storage) PmetaPersistedBytes() int64 {
	if s.catalog == nil {
		return 0
	}
	return s.catalog.PersistedBytes()
}

// PmetaPersistedBytesByTenant is the per-tenant on-S3 metadata footprint
// ("account:project" -> bytes), tracked incrementally. nil unless --pmeta.
func (s *Storage) PmetaPersistedBytesByTenant() map[string]int64 {
	if s.catalog == nil {
		return nil
	}
	return s.catalog.PersistedBytesByTenant()
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
// buffer (membuffer.Store). When set (BufferEngine=logstore, co-located
// insert+select), the SELECT path serves the recent/unflushed window from it
// via the same engine the S3-Parquet scan uses — no struct→DataBlock
// conversion.
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
// from the pmeta catalog, or 0 if pmeta is off or the field has no sketch. This
// is the accurate, deterministically-maintained cardinality source (fed on flush,
// merged in compaction) the stats API prefers over the lazily-populated,
// 100-capped LabelIndex count.
func (s *Storage) PmetaCardinality(field string) uint64 {
	if s.catalog == nil {
		return 0
	}
	return s.catalog.FieldCardinality(field)
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
// Exposed so the lifecycle code in cmd/lakehouse-{logs,traces}/main.go
// can persist its key list on shutdown and seed an async prefetch on
// the next start.
func (s *Storage) FooterCache() *FooterCache {
	return s.footerCache
}

// PrefetchFootersByKeys looks up the given S3 keys in the current
// manifest and runs the standard prefetchFooters batch for every match.
// Keys that no longer exist in the manifest (compacted away, retired,
// or written by a previous deployment shape) are silently skipped.
// Called from the post-warmup async path with the list LoadFooterCacheKeys
// returned, so the next first-hit query against a previously-cached file
// is served from cache instead of paying an S3 round-trip.
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

	blockCols := []logstorage.BlockColumn{
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
	}

	// Arbitrary resource/log attributes carried in the maps, surfaced under
	// the same prefixed names the file-scan path uses (resource_attr:K,
	// log_attr:K). Without these, a recent-log query filtering on a map
	// attribute (e.g. `log_attr:http.status="500"`) matches flushed files
	// but NOT still-buffered rows — the logs-side twin of the traces
	// _stream/attribute buffer gap. Built lazily so a column is only
	// materialised when at least one buffered row carries that key.
	attrCols := make(map[string][]string)
	attrOrder := make([]string, 0)
	putAttr := func(name string, i int, val string) {
		if val == "" {
			return
		}
		col, ok := attrCols[name]
		if !ok {
			col = make([]string, len(rows))
			attrCols[name] = col
			attrOrder = append(attrOrder, name)
		}
		col[i] = val
	}
	for i, row := range rows {
		// Dedicated columns (Tier 1) surface under their bare OTel name — same
		// as logRowToFields, so buffer reads and file reads agree. Lazy: only
		// materialised when non-empty (most rows lack most attributes).
		putAttr("container.id", i, row.ContainerID)
		putAttr("service.instance.id", i, row.ServiceInstanceID)
		putAttr("service.version", i, row.ServiceVersion)
		putAttr("exception.type", i, row.ExceptionType)
		putAttr("exception.message", i, row.ExceptionMessage)
		putAttr("k8s.cluster.name", i, row.K8sClusterName)
		putAttr("telemetry.sdk.name", i, row.TelemetrySDKName)
		putAttr("telemetry.sdk.language", i, row.TelemetrySDKLang)
		putAttr("telemetry.sdk.version", i, row.TelemetrySDKVer)
		putAttr("cloud.account.id", i, row.CloudAccountID)
		putAttr("cloud.provider", i, row.CloudProvider)
		putAttr("os.type", i, row.OSType)
		putAttr("host.arch", i, row.HostArch)
		putAttr("process.runtime.name", i, row.ProcessRuntimeName)
		putAttr("process.runtime.version", i, row.ProcessRuntimeVer)
		for k, v := range row.ResourceAttributes {
			putAttr("resource_attr:"+k, i, v)
		}
		for k, v := range row.LogAttributes {
			putAttr("log_attr:"+k, i, v)
		}
	}
	for _, name := range attrOrder {
		blockCols = append(blockCols, logstorage.BlockColumn{Name: name, Values: attrCols[name]})
	}

	db := &logstorage.DataBlock{}
	db.SetColumns(blockCols)
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

	// Phase 2: pmeta read-flip — enrich from the in-RAM fileMetaFacet (no S3; the
	// catalog bundle is warmed before WarmMetadata in runStartup), then fall back to
	// the `_file_metadata.json` sidecars ONLY for files the facet didn't cover (cold
	// bundle / pre-pmeta files). When the facet covers everything the per-partition
	// sidecar GETs are skipped entirely.
	facetEnriched := 0
	var uncovered []string
	if s.catalog != nil {
		facetEnriched, uncovered = s.manifest.EnrichFromProvider(catalogFileMetaProvider{store: s.catalog})
	}
	sidecarLoaded := 0
	switch {
	case s.catalog == nil:
		// pmeta off: load all sidecars (unchanged behavior).
		sidecarLoaded = s.manifest.LoadSidecars(ctx, s.pool.S3Client(), 16)
	case len(uncovered) > 0:
		// pmeta on: only the partitions the bundle didn't fully cover (a fully
		// covered bundle skips the per-partition sidecar GETs entirely).
		sidecarLoaded = s.manifest.LoadSidecarsForPartitions(ctx, s.pool.S3Client(), 16, uncovered)
	}

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
		fetched := prefetchFooters(ctx, s.pool, needEnrich, s.footerCache, 0, s.footerPrefetchBytes())
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
	// Same nil guard as Phase 3: insert-only pods run without a footer cache.
	smallEnriched := 0
	if len(needEnrich) > 0 && s.footerCache != nil {
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
