package stats

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// FieldSize is the materialized per-field size aggregate. StorageBytes is the
// on-S3 compressed footprint of the field's Parquet column(s), summed across
// every live file. MetadataBytes (bloom + pmeta catalog attributable to the
// field) is populated in a later phase.
type FieldSize struct {
	StorageBytes  int64 `json:"storage_bytes"`
	MetadataBytes int64 `json:"metadata_bytes,omitempty"`
	Rows          int64 `json:"rows,omitempty"`
	Files         int64 `json:"files,omitempty"`
}

// TenantSize is the per-tenant size aggregate.
type TenantSize struct {
	StorageBytes  int64 `json:"storage_bytes"`
	MetadataBytes int64 `json:"metadata_bytes,omitempty"`
	Rows          int64 `json:"rows,omitempty"`
	Files         int64 `json:"files,omitempty"`
}

// StatsAggregate is a materialized, cluster-wide size aggregate. It is maintained
// by diffs through the manifest change-observer (flush AND compaction both route
// through manifest.AddFile/RemoveFile) and reconciled periodically against the
// full manifest (the source of truth). The stats API reads it in O(1) instead of
// rescanning the manifest per request, and it is persisted as a small pmeta
// sidecar so a fresh instance never does a cold full sweep. Because the source
// (the manifest) is cluster-wide + compacted, the aggregate is correct cluster-
// wide without separate gossip. See docs/architecture/stats-aggregate-cache.md.
type StatsAggregate struct {
	mu         sync.RWMutex
	perField   map[string]*FieldSize
	perTenant  map[string]*TenantSize
	totStorage int64
	totRaw     int64
	totRows    int64
	totFiles   int64
	generation uint64
	metaS3     int64 // cluster on-S3 _meta/ bytes; swept periodically, not persisted
}

// NewStatsAggregate returns an empty aggregate.
func NewStatsAggregate() *StatsAggregate {
	return &StatsAggregate{
		perField:  make(map[string]*FieldSize),
		perTenant: make(map[string]*TenantSize),
	}
}

// tenantKeyOf extracts "account:project" from a tenant-prefixed S3 key
// ("<account>/<project>/..."); empty for non-tenant-keyed (legacy) paths.
func tenantKeyOf(fileKey string) string {
	parts := strings.SplitN(fileKey, "/", 3)
	if len(parts) >= 2 {
		return parts[0] + ":" + parts[1]
	}
	return ""
}

// OnAdd folds a newly-added file into the aggregate (flush or compaction output).
// Register as the manifest's onAdd observer.
func (a *StatsAggregate) OnAdd(_ string, fi manifest.FileInfo) {
	a.mu.Lock()
	a.apply(fi, +1)
	a.mu.Unlock()
}

// OnRemove subtracts a removed file (compaction inputs). Register as the
// manifest's onRemove observer.
func (a *StatsAggregate) OnRemove(_ string, fi manifest.FileInfo) {
	a.mu.Lock()
	a.apply(fi, -1)
	a.mu.Unlock()
}

// apply adds (sign=+1) or subtracts (sign=-1) a file's contribution. Caller holds
// a.mu (or owns the receiver exclusively, as in Recompute).
func (a *StatsAggregate) apply(fi manifest.FileInfo, sign int64) {
	for col, b := range fi.ColumnBytes {
		fs := a.perField[col]
		if fs == nil {
			fs = &FieldSize{}
			a.perField[col] = fs
		}
		fs.StorageBytes += sign * b
		fs.Rows += sign * fi.RowCount
		fs.Files += sign
		if fs.Files <= 0 {
			delete(a.perField, col)
		}
	}
	a.totStorage += sign * fi.Size
	a.totRaw += sign * fi.RawBytes
	a.totRows += sign * fi.RowCount
	a.totFiles += sign

	if tk := tenantKeyOf(fi.Key); tk != "" {
		ts := a.perTenant[tk]
		if ts == nil {
			ts = &TenantSize{}
			a.perTenant[tk] = ts
		}
		ts.StorageBytes += sign * fi.Size
		ts.Rows += sign * fi.RowCount
		ts.Files += sign
		if ts.Files <= 0 {
			delete(a.perTenant, tk)
		}
	}
	a.generation++
}

// Recompute rebuilds the aggregate from the full manifest file set — the source
// of truth. Used on startup (after warm-load) and on the periodic reconcile to
// correct any incremental drift. Atomically swaps the rebuilt maps in.
func (a *StatsAggregate) Recompute(allFiles map[string][]manifest.FileInfo) {
	fresh := NewStatsAggregate()
	for _, files := range allFiles {
		for i := range files {
			fresh.apply(files[i], +1) // fresh is local — no lock contention
		}
	}
	a.mu.Lock()
	a.perField = fresh.perField
	a.perTenant = fresh.perTenant
	a.totStorage = fresh.totStorage
	a.totRaw = fresh.totRaw
	a.totRows = fresh.totRows
	a.totFiles = fresh.totFiles
	a.generation++
	a.mu.Unlock()
}

// StorageBytesOf returns the field's on-S3 storage bytes. Tries the exact name,
// then the suffix after ':' so a label-index name like "resource_attr:service.name"
// matches the bare Parquet column "service.name".
func (a *StatsAggregate) StorageBytesOf(field string) int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if fs := a.perField[field]; fs != nil {
		return fs.StorageBytes
	}
	if idx := strings.LastIndex(field, ":"); idx >= 0 {
		if fs := a.perField[field[idx+1:]]; fs != nil {
			return fs.StorageBytes
		}
	}
	return 0
}

// CoveredStorage is the sum of per-field storage bytes — the on-S3 footprint of
// files that already carry ColumnBytes (flushed/compacted since the feature
// landed). Less than TotalStorage while older files are still being backfilled.
func (a *StatsAggregate) CoveredStorage() int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	var s int64
	for _, fs := range a.perField {
		s += fs.StorageBytes
	}
	return s
}

// TotalStorage is the on-S3 compressed total across ALL live files (the manifest
// Size sum), including older files that don't yet carry per-column bytes.
func (a *StatsAggregate) TotalStorage() int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.totStorage
}

// SetMetaS3 records the cluster-wide on-S3 metadata footprint (the _meta/ prefix
// byte sum), swept periodically by the caller. Not part of the persisted snapshot.
func (a *StatsAggregate) SetMetaS3(n int64) {
	a.mu.Lock()
	a.metaS3 = n
	a.mu.Unlock()
}

// MetaS3 returns the last-swept cluster on-S3 metadata footprint. Nil-safe, so it
// can serve as an APIConfig func value even when the aggregate is absent.
func (a *StatsAggregate) MetaS3() int64 {
	if a == nil {
		return 0
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.metaS3
}

// FieldSizes returns a copy of the per-field aggregate (for /stats/storage).
func (a *StatsAggregate) FieldSizes() map[string]FieldSize {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make(map[string]FieldSize, len(a.perField))
	for k, v := range a.perField {
		out[k] = *v
	}
	return out
}

// TenantSizes returns a copy of the per-tenant aggregate (for /tenants).
func (a *StatsAggregate) TenantSizes() map[string]TenantSize {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make(map[string]TenantSize, len(a.perTenant))
	for k, v := range a.perTenant {
		out[k] = *v
	}
	return out
}

// aggregateSnapshot is the JSON shape persisted to the pmeta sidecar object.
type aggregateSnapshot struct {
	Generation uint64                 `json:"generation"`
	PerField   map[string]*FieldSize  `json:"per_field"`
	PerTenant  map[string]*TenantSize `json:"per_tenant"`
	TotStorage int64                  `json:"total_storage"`
	TotRaw     int64                  `json:"total_raw"`
	TotRows    int64                  `json:"total_rows"`
	TotFiles   int64                  `json:"total_files"`
}

// Marshal serialises the aggregate for the S3 sidecar cache.
func (a *StatsAggregate) Marshal() ([]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return json.Marshal(aggregateSnapshot{
		Generation: a.generation,
		PerField:   a.perField,
		PerTenant:  a.perTenant,
		TotStorage: a.totStorage,
		TotRaw:     a.totRaw,
		TotRows:    a.totRows,
		TotFiles:   a.totFiles,
	})
}

// Load installs a snapshot from the S3 sidecar (startup cold-start accelerator).
// A subsequent Recompute against the warm-loaded manifest corrects any staleness.
func (a *StatsAggregate) Load(data []byte) error {
	var s aggregateSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if s.PerField != nil {
		a.perField = s.PerField
	}
	if s.PerTenant != nil {
		a.perTenant = s.PerTenant
	}
	a.totStorage = s.TotStorage
	a.totRaw = s.TotRaw
	a.totRows = s.TotRows
	a.totFiles = s.TotFiles
	a.generation = s.Generation
	return nil
}

// AggregateSidecarKeySuffix is the S3 key suffix (appended to the deployment
// auto-prefix) for the StatsAggregate cold-start sidecar object.
const AggregateSidecarKeySuffix = "_meta/stats-aggregate.json"

// S3Pool is the minimal S3 surface the aggregate sidecar cache needs (satisfied
// by parquets3 store.Pool()).
type S3Pool interface {
	Upload(ctx context.Context, key string, data []byte) error
	Download(ctx context.Context, key string) ([]byte, error)
}

// SaveToS3 persists the aggregate snapshot to its sidecar object so a fresh
// instance can warm its size stats without rescanning the whole manifest. Called
// after each Recompute (warm + refresh) — the snapshot is small (per-field +
// per-tenant sums, not per-file).
func (a *StatsAggregate) SaveToS3(ctx context.Context, pool S3Pool, key string) error {
	data, err := a.Marshal()
	if err != nil {
		return err
	}
	return pool.Upload(ctx, key, data)
}

// LoadFromS3 installs the sidecar snapshot if present (cold-start accelerator).
// A subsequent Recompute against the warm-loaded manifest corrects any staleness,
// so a missing/old object is harmless — callers log-and-continue on error.
func (a *StatsAggregate) LoadFromS3(ctx context.Context, pool S3Pool, key string) error {
	data, err := pool.Download(ctx, key)
	if err != nil {
		return err
	}
	return a.Load(data)
}
