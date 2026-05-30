package smartcache

import (
	"context"
	"sync"
	"time"
)

type L1Cache interface {
	Get(key string) ([]byte, bool)
	Put(key string, val []byte)
	// PutNoCopy stores val without copying. Caller transfers ownership.
	// Used by the S3-download fast path where the downloaded buffer is
	// allocated for caching and never mutated by the caller — eliminates
	// the LRU copy that doubled transient memory during wildcard scans.
	// See internal/cache/lru.go for the safety contract.
	PutNoCopy(key string, val []byte)
}

type L2Cache interface {
	Get(key string) ([]byte, bool)
	Put(key string, data []byte) error
	Delete(key string)
	Size() int64
}

type PeerLookup interface {
	Lookup(key string) (peer string, isLocal bool)
	Members() []string
	MemberCount() int
}

// AZPeerLookup extends PeerLookup with availability-zone-aware lookup.
// When the cache partition mode is "az-local", the controller uses LookupAZ
// to route ownership through the AZ-scoped ring instead of the global ring.
type AZPeerLookup interface {
	PeerLookup
	LookupAZ(key string) (peer string, isLocal bool, isSameAZ bool)
}

type PeerFetcher interface {
	Fetch(ctx context.Context, peer, key string) ([]byte, bool, error)
}

type S3Fetcher interface {
	Download(ctx context.Context, key string) ([]byte, error)
}

type ControllerConfig struct {
	L1            L1Cache
	L2            L2Cache
	PeerLookup    PeerLookup
	PeerFetcher   PeerFetcher
	S3Fetcher     S3Fetcher
	Metadata      *MetadataMap
	DiskLimit     int64
	MaxAge        time.Duration
	HotThreshold  int
	HotWindow     time.Duration
	GracePeriod   time.Duration
	Signal        string
	PartitionMode string // "az-local" (default), "global", "distributed"
}

type Controller struct {
	l1            L1Cache
	l2            L2Cache
	peerLookup    PeerLookup
	peerFetcher   PeerFetcher
	s3Fetcher     S3Fetcher
	metadata      *MetadataMap
	maxAge        time.Duration
	hotThreshold  int
	hotWindow     time.Duration
	gracePeriod   time.Duration
	signal        string
	partitionMode string
	diskLimit     int64

	sfMu       sync.Mutex
	sfInFlight map[string]*sfCall
}

type sfCall struct {
	wg  sync.WaitGroup
	val []byte
	err error
}

func NewController(cfg ControllerConfig) *Controller {
	if cfg.GracePeriod == 0 {
		cfg.GracePeriod = 5 * time.Minute
	}
	pm := cfg.PartitionMode
	if pm == "" {
		pm = "az-local"
	}
	return &Controller{
		l1:            cfg.L1,
		l2:            cfg.L2,
		peerLookup:    cfg.PeerLookup,
		peerFetcher:   cfg.PeerFetcher,
		s3Fetcher:     cfg.S3Fetcher,
		metadata:      cfg.Metadata,
		maxAge:        cfg.MaxAge,
		hotThreshold:  cfg.HotThreshold,
		hotWindow:     cfg.HotWindow,
		gracePeriod:   cfg.GracePeriod,
		signal:        cfg.Signal,
		partitionMode: pm,
		diskLimit:     cfg.DiskLimit,
		sfInFlight:    make(map[string]*sfCall),
	}
}

// LookupOwner routes cache key ownership through the appropriate ring based on
// the configured partition mode and returns whether this node owns the key.
// Exported for use by hybrid fan-out self-filtering in the select tier.
func (c *Controller) LookupOwner(key string) (peer string, isLocal bool) {
	return c.lookupOwner(key)
}

// IsLocal returns true if the given key is owned by this node according to
// the cache ring. Implements OwnershipChecker for warmup filtering.
func (c *Controller) IsLocal(key string) bool {
	_, local := c.lookupOwner(key)
	return local
}

// lookupOwner routes cache key ownership through the appropriate ring based on
// the configured partition mode. In "az-local" mode it uses the AZ-scoped ring
// when available; in "global" or "distributed" mode it uses the full ring.
func (c *Controller) lookupOwner(key string) (peer string, isLocal bool) {
	if c.partitionMode == "global" || c.partitionMode == "distributed" {
		return c.peerLookup.Lookup(key)
	}
	// az-local: use AZ-scoped ring if available
	if azLookup, ok := c.peerLookup.(AZPeerLookup); ok {
		peer, isLocal, _ = azLookup.LookupAZ(key)
		return peer, isLocal
	}
	return c.peerLookup.Lookup(key)
}

func (c *Controller) Get(ctx context.Context, key string, size int64) ([]byte, error) {
	// L1 hit — fastest path
	if data, ok := c.l1.Get(key); ok {
		c.metadata.RecordAccess(key)
		return data, nil
	}

	peer, isLocal := c.lookupOwner(key)

	if isLocal {
		// Owned key: try L2 disk cache
		if data, ok := c.l2.Get(key); ok {
			c.metadata.RecordAccess(key)
			c.l1.Put(key, data)
			return data, nil
		}
	} else if c.peerFetcher != nil {
		// Non-owned key: try to fetch from the owning peer
		data, found, err := c.peerFetcher.Fetch(ctx, peer, key)
		if err == nil && found {
			c.l1.Put(key, data)
			return data, nil
		}
	}

	// Fall through to S3 download with singleflight dedup
	data, err := c.singleflightDownload(ctx, key, size)
	if err != nil {
		return nil, err
	}

	// Store in L1 without copying. The downloaded buffer is allocated by
	// io.ReadAll exclusively for caching+returning to the query path; no
	// caller mutates it. Sharing the buffer between the singleflight
	// result, the cache slot, and the returned []byte halves transient
	// memory under 16-worker wildcard scans (heap-diff at near-OOM:
	// io.ReadAll=253 MiB + LRU.Put=382 MiB had ~50% overlap that this
	// elides).
	c.l1.PutNoCopy(key, data)

	// Store in L2 if this node owns the key
	if _, isLocal := c.lookupOwner(key); isLocal && c.l2 != nil {
		_ = c.l2.Put(key, data)
		now := time.Now()
		c.metadata.Set(key, EntryMeta{
			CreatedAt:         now,
			LastAccess:        now,
			AccessCount:       1,
			AccessWindowStart: now,
			Signal:            c.signal,
			Size:              int64(len(data)),
		})
	}

	return data, nil
}

func (c *Controller) singleflightDownload(ctx context.Context, key string, size int64) ([]byte, error) {
	c.sfMu.Lock()
	if call, ok := c.sfInFlight[key]; ok {
		// Another goroutine is already downloading this key — wait for it
		c.sfMu.Unlock()
		call.wg.Wait()
		return call.val, call.err
	}
	// First goroutine for this key — we lead the download
	call := &sfCall{}
	call.wg.Add(1)
	c.sfInFlight[key] = call
	c.sfMu.Unlock()

	call.val, call.err = c.s3Fetcher.Download(ctx, key)
	call.wg.Done()

	c.sfMu.Lock()
	delete(c.sfInFlight, key)
	c.sfMu.Unlock()

	return call.val, call.err
}

// Pin marks a cache entry as pinned by a given query ID with the configured grace period.
func (c *Controller) Pin(key, queryID string) {
	c.metadata.Pin(key, queryID, c.gracePeriod)
}

// Unpin removes a pin on a cache entry for a given query ID.
func (c *Controller) Unpin(key, queryID string) {
	c.metadata.Unpin(key, queryID)
}

// RecordTraceIDs associates trace IDs with a cache entry for cross-signal prefetch.
func (c *Controller) RecordTraceIDs(key string, traceIDs []string) {
	meta, ok := c.metadata.Get(key)
	if !ok {
		return
	}
	meta.TraceIDs = traceIDs
	c.metadata.Set(key, meta)
}

// Metadata returns the underlying MetadataMap for inspection or eviction.
func (c *Controller) Metadata() *MetadataMap {
	return c.metadata
}

// RunEvictionOnce runs a single eviction pass, removing expired entries from
// both the L2 cache and the metadata map. If disk usage exceeds 90% of the
// watermark after TTL eviction, falls back to LRU eviction. Returns all evicted keys.
func (c *Controller) RunEvictionOnce() []string {
	expired := CollectExpired(c.metadata, c.maxAge, c.hotThreshold, c.hotWindow)
	for _, key := range expired {
		if _, ok := c.metadata.Get(key); !ok {
			continue
		}
		c.l2.Delete(key)
		c.metadata.Delete(key)
	}

	if c.diskLimit > 0 {
		watermark := int64(float64(c.diskLimit) * 0.9)
		if c.l2.Size() > watermark {
			excess := c.l2.Size() - watermark
			lru := CollectLRU(c.metadata, excess, c.hotThreshold, c.hotWindow)
			for _, key := range lru {
				c.l2.Delete(key)
				c.metadata.Delete(key)
			}
			expired = append(expired, lru...)
		}
	}

	return expired
}

// StartEvictionLoop launches a background goroutine that periodically runs
// eviction. It stops when the stop channel is closed.
func (c *Controller) StartEvictionLoop(interval time.Duration, stop <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				c.RunEvictionOnce()
			}
		}
	}()
}

// StartSnapshotLoop launches a background goroutine that periodically saves
// metadata snapshots to disk. On stop it performs a final save.
func (c *Controller) StartSnapshotLoop(path string, interval time.Duration, stop <-chan struct{}) {
	if path == "" {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				_ = c.metadata.SaveSnapshot(path)
				return
			case <-ticker.C:
				_ = c.metadata.SaveSnapshot(path)
			}
		}
	}()
}

// FindFilesByTraceID returns file keys whose recorded TraceIDs contain the
// given trace ID. Used for trace parent-child prefetch: once a trace is known,
// skip bloom/label checks and go directly to the right files.
func (c *Controller) FindFilesByTraceID(traceID string) []string {
	if traceID == "" {
		return nil
	}
	all := c.metadata.All()
	var keys []string
	for key, meta := range all {
		for _, tid := range meta.TraceIDs {
			if tid == traceID {
				keys = append(keys, key)
				break
			}
		}
	}
	return keys
}

// PutL2 stores data directly into the L2 disk cache and records metadata.
// Used by write-through caching on ingest flush to pre-populate the cache
// for recently ingested data. Errors are non-fatal and returned to the caller
// for logging but should not abort the flush.
func (c *Controller) PutL2(key string, data []byte) error {
	if c.l2 == nil {
		return nil
	}
	if err := c.l2.Put(key, data); err != nil {
		return err
	}
	now := time.Now()
	c.metadata.Set(key, EntryMeta{
		CreatedAt:         now,
		LastAccess:        now,
		AccessCount:       1,
		AccessWindowStart: now,
		Signal:            c.signal,
		Size:              int64(len(data)),
	})
	return nil
}

// DeprioritizeByTraceIDs resets access counts and timestamps for any cached
// entries whose TraceIDs overlap with the provided set. This supports
// cross-signal deprioritization where resolved traces should no longer be
// kept hot in cache. Returns the number of entries deprioritized.
func (c *Controller) DeprioritizeByTraceIDs(traceIDs []string) int {
	traceSet := make(map[string]bool, len(traceIDs))
	for _, id := range traceIDs {
		traceSet[id] = true
	}

	all := c.metadata.All()
	deprioritized := 0
	for key, meta := range all {
		for _, tid := range meta.TraceIDs {
			if traceSet[tid] {
				meta.LastAccess = time.Time{}
				meta.AccessCount = 0
				c.metadata.Set(key, meta)
				deprioritized++
				break
			}
		}
	}
	return deprioritized
}
