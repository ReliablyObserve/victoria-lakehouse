package manifest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const maxLabelsPerField = 100

type FileInfo struct {
	Key string `json:"key"`
	// Bucket names the S3 bucket holding this object. Empty means the
	// manifest's default bucket — preserves backward compatibility for
	// every file written before bucket-isolation support landed and
	// for tenants that share the default bucket via prefix isolation.
	Bucket            string                  `json:"bucket,omitempty"`
	Size              int64                   `json:"size"`
	RowCount          int64                   `json:"row_count,omitempty"`
	MinTimeNs         int64                   `json:"min_time_ns,omitempty"`
	MaxTimeNs         int64                   `json:"max_time_ns,omitempty"`
	RawBytes          int64                   `json:"raw_bytes,omitempty"`
	SchemaFingerprint string                  `json:"schema_fp,omitempty"`
	CompactionLevel   int                     `json:"compaction_level,omitempty"`
	Labels            map[string][]string     `json:"labels,omitempty"`
	ColumnStats       map[string]ColumnMinMax `json:"column_stats,omitempty"`
	StorageClass      string                  `json:"storage_class,omitempty"`
	ClassCheckedAt    time.Time               `json:"class_checked_at,omitempty"`
	ClassSource       string                  `json:"class_source,omitempty"`
	CreatedAt         time.Time               `json:"created_at,omitempty"`
}

// BucketOr returns the file's bucket, falling back to defaultBucket
// when empty (the common case — most files are in the default
// bucket and don't carry a bucket field). Centralized here so every
// reader call site uses the same resolution.
func (fi FileInfo) BucketOr(defaultBucket string) string {
	if fi.Bucket == "" {
		return defaultBucket
	}
	return fi.Bucket
}

func (fi FileInfo) CompressionRatio() float64 {
	if fi.RawBytes <= 0 || fi.Size <= 0 {
		return 0
	}
	return float64(fi.RawBytes) / float64(fi.Size)
}

func (fi FileInfo) MatchesLabel(field, value string) bool {
	if fi.Labels == nil {
		return false
	}
	for _, v := range fi.Labels[field] {
		if v == value {
			return true
		}
	}
	return false
}

type PartitionMeta struct {
	BloomAvailable  bool      `json:"bloom_available,omitempty"`
	BloomSize       int64     `json:"bloom_size,omitempty"`
	BloomUpdatedAt  time.Time `json:"bloom_updated_at,omitempty"`
	BloomColumns    []string  `json:"bloom_columns,omitempty"`
	LabelsAvailable bool      `json:"labels_available,omitempty"`
}

// partitionEntry holds a parsed partition key with pre-computed time bounds
// for use in the sorted partition index.
type partitionEntry struct {
	key   string    // "dt=2026-01-01/hour=00"
	start time.Time // parsed partition start
	end   time.Time // start + 1 hour
}

type Manifest struct {
	mu               sync.RWMutex
	files            map[string][]FileInfo // "dt=2026-05-02/hour=10" -> files
	sortedPartitions []partitionEntry
	partitionMeta    map[string]*PartitionMeta
	labelIndex       map[string]map[string]map[string]bool // field -> value -> fileKey -> exists

	// byKey indexes the partition that owns each FileInfo key, so per-key
	// mutators (SetFileBucket, UpdateFileColumnStats, EnrichFileMetadata)
	// can avoid the full O(n) two-level scan across every partition every
	// time they touch a single file. At 50M files (the PB-scale target),
	// a single full-manifest scan takes hundreds of milliseconds under
	// the write lock and blocks queries; the byKey index reduces it to
	// O(files-in-one-partition) ≈ O(100) on a balanced cluster.
	//
	// We store the partition (not a *FileInfo) deliberately: the file
	// slice in m.files[partition] gets re-allocated on append/remove, so
	// any *FileInfo we cached would dangle. Per-partition slice scans
	// (~50–100 files) are cheap and stable across mutations.
	//
	// Kept consistent with m.files in: AddFile, RemoveFile,
	// RefreshFromS3, rebuildIndex, snapshot Load.
	byKey map[string]string

	// tenantAggregates is the incremental per-tenant cache backing
	// TenantSummaries(). Without this, /api/v1/tenants, /stats/overview,
	// the Lakehouse Tenants UI tab, and the servicegraph background
	// task's tenant-lister each iterate every file in the manifest on
	// every call — O(50M) at PB-scale.
	//
	// Maintenance contract: every AddFile / RemoveFile / EnrichFileMetadata
	// updates this map atomically under the same write lock that mutates
	// m.files. RefreshFromS3 and snapshot Load rebuild it via
	// rebuildTenantAggregates() after the wholesale m.files reassignment.
	// TenantSummaries() then just snapshots the cache — O(tenants),
	// typically O(10²) not O(10⁷).
	//
	// minTimeNs / maxTimeNs are tracked at nanosecond precision so we
	// don't have to compare time.Time values on the hot path. The
	// per-tenant set of partitions is reference-counted so we can detect
	// when a tenant's earliest/latest partition disappears and trigger a
	// minimal recompute (bounded by the tenant's partition count, not
	// the manifest's file count).
	tenantAggregates map[tenantAccumKey]*tenantAccum

	minTime        time.Time
	maxTime        time.Time
	totalFiles     int
	totalBytes     int64
	lastRefresh    time.Time
	prefix         string
	bucket         string
	prefixTemplate string

	// partitionAttempts maps "dt=YYYY-MM-DD/hour=HH" -> last MarkAttempt
	// timestamp recorded by the compaction scheduler (see
	// internal/compaction.OwnershipResolver + OrphanSweep). In-memory only
	// per spec §2.2.3: after a pod restart Tier A wakes up "blind" for
	// 3×Interval, then HRW resumes normally. Same mutex as files map so
	// AttemptsView is a coherent snapshot relative to the manifest.
	partitionAttempts map[string]time.Time
}

func New(bucket, prefix string) *Manifest {
	return &Manifest{
		files:             make(map[string][]FileInfo),
		labelIndex:        make(map[string]map[string]map[string]bool),
		partitionMeta:     make(map[string]*PartitionMeta),
		partitionAttempts: make(map[string]time.Time),
		byKey:             make(map[string]string),
		tenantAggregates:  make(map[tenantAccumKey]*tenantAccum),
		prefix:            prefix,
		bucket:            bucket,
	}
}

// tenantAccumKey + tenantAccum back the incremental TenantSummaries
// cache. Lives next to m.files under m.mu's protection.
type tenantAccumKey struct {
	account string
	project string
}

type tenantAccum struct {
	files    int
	bytes    int64
	rows     int64
	rawBytes int64
	// partitions tracks the per-partition file count for this tenant.
	// Used to recompute min/max time bounds when a tenant's earliest
	// or latest partition has its last file removed, without scanning
	// the entire manifest.
	partitions map[string]int
	minTimeNs  int64
	maxTimeNs  int64
}

// tenantKeyFromFileKey parses the (account, project) tuple out of a
// FileInfo.Key. Mirrors the parsing in TenantSummaries() so the cache
// agrees with the legacy linear-scan path on every input. Returns
// (key, true) on success or (zero, false) if the key doesn't conform
// to the prefix template.
func (m *Manifest) tenantKeyFromFileKey(fileKey string) (tenantAccumKey, bool) {
	segments := 2
	hasOrgID := strings.Contains(m.prefixTemplate, "{OrgID}")
	if hasOrgID && !strings.Contains(m.prefixTemplate, "{ProjectID}") {
		segments = 1
	}
	parts := strings.SplitN(fileKey, "/", segments+2)
	if len(parts) < segments+1 {
		return tenantAccumKey{}, false
	}
	if segments == 1 {
		return tenantAccumKey{account: parts[0]}, true
	}
	return tenantAccumKey{account: parts[0], project: parts[1]}, true
}

// updateTenantAggregateOnAdd applies a +1 file delta for fi/partition
// to the tenant aggregate cache. Must hold m.mu (write).
func (m *Manifest) updateTenantAggregateOnAdd(partition string, fi FileInfo) {
	tk, ok := m.tenantKeyFromFileKey(fi.Key)
	if !ok {
		return
	}
	a := m.tenantAggregates[tk]
	if a == nil {
		a = &tenantAccum{partitions: make(map[string]int)}
		m.tenantAggregates[tk] = a
	}
	a.files++
	a.bytes += fi.Size
	a.rows += fi.RowCount
	a.rawBytes += fi.RawBytes
	a.partitions[partition]++

	if t, err := parsePartitionTime(partition); err == nil {
		startNs := t.UnixNano()
		endNs := t.Add(time.Hour).UnixNano()
		if a.minTimeNs == 0 || startNs < a.minTimeNs {
			a.minTimeNs = startNs
		}
		if endNs > a.maxTimeNs {
			a.maxTimeNs = endNs
		}
	}
}

// updateTenantAggregateOnRemove applies a -1 file delta. Recomputes
// min/max time iff this was the last file in the tenant's earliest
// or latest partition; the recompute is bounded by the tenant's
// partition count (≈720 for a 30-day window) not the manifest's file
// count. Must hold m.mu (write).
func (m *Manifest) updateTenantAggregateOnRemove(partition string, fi FileInfo) {
	tk, ok := m.tenantKeyFromFileKey(fi.Key)
	if !ok {
		return
	}
	a := m.tenantAggregates[tk]
	if a == nil {
		return
	}
	a.files--
	a.bytes -= fi.Size
	a.rows -= fi.RowCount
	a.rawBytes -= fi.RawBytes

	// Decrement the partition refcount; if we just removed the last
	// file the tenant had in this partition, drop it from the set and
	// check if min/max time bounds need recomputing.
	a.partitions[partition]--
	if a.partitions[partition] <= 0 {
		delete(a.partitions, partition)

		if t, err := parsePartitionTime(partition); err == nil {
			startNs := t.UnixNano()
			endNs := t.Add(time.Hour).UnixNano()
			if startNs == a.minTimeNs || endNs == a.maxTimeNs {
				m.recomputeTenantTimeBounds(a)
			}
		}
	}

	// Drop empty tenant aggregates so TenantSummaries() doesn't have to
	// filter them out on every call.
	if a.files == 0 {
		delete(m.tenantAggregates, tk)
	}
}

// recomputeTenantTimeBounds scans the tenant's partition set (NOT the
// manifest's full file set) to find the new min/max. Bounded by the
// number of partitions the tenant occupies — typically O(720) in a
// 30-day-hot retention.
func (m *Manifest) recomputeTenantTimeBounds(a *tenantAccum) {
	a.minTimeNs = 0
	a.maxTimeNs = 0
	for partition := range a.partitions {
		t, err := parsePartitionTime(partition)
		if err != nil {
			continue
		}
		startNs := t.UnixNano()
		endNs := t.Add(time.Hour).UnixNano()
		if a.minTimeNs == 0 || startNs < a.minTimeNs {
			a.minTimeNs = startNs
		}
		if endNs > a.maxTimeNs {
			a.maxTimeNs = endNs
		}
	}
}

// rebuildTenantAggregates reconstructs the per-tenant cache from
// scratch by iterating m.files once. Called after wholesale m.files
// reassignments (RefreshFromS3, snapshot Load). Must hold m.mu (write).
func (m *Manifest) rebuildTenantAggregates() {
	m.tenantAggregates = make(map[tenantAccumKey]*tenantAccum)
	for partition, files := range m.files {
		for i := range files {
			m.updateTenantAggregateOnAdd(partition, files[i])
		}
	}
}

func (m *Manifest) SetBloomMeta(partition string, meta PartitionMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.partitionMeta[partition] = &meta
}

func (m *Manifest) GetBloomMeta(partition string) *PartitionMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.partitionMeta[partition]
}

func (m *Manifest) BloomAvailable(partition string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pm := m.partitionMeta[partition]
	return pm != nil && pm.BloomAvailable
}

func (m *Manifest) SetPrefixTemplate(tmpl string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prefixTemplate = tmpl
}

// refreshFullBucket enumerates every key under listPrefix (or the
// entire bucket when listPrefix=="") and builds a fresh file map. Used
// by the legacy single-prefix path and as a fallback when tenant
// discovery fails.
func (m *Manifest) refreshFullBucket(ctx context.Context, client *s3.Client, listPrefix string) (map[string][]FileInfo, int, int64, error) {
	files := make(map[string][]FileInfo)
	var totalFiles int
	var totalBytes int64

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(m.bucket),
		Prefix: aws.String(listPrefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("list objects: %w", err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if !strings.HasSuffix(key, ".parquet") {
				continue
			}
			partition := extractPartition(key)
			if partition == "" {
				continue
			}
			files[partition] = append(files[partition], FileInfo{
				Key:  key,
				Size: aws.ToInt64(obj.Size),
			})
			totalFiles++
			totalBytes += aws.ToInt64(obj.Size)
		}
	}
	return files, totalFiles, totalBytes, nil
}

// refreshTenantScoped discovers tenant prefixes via two-level
// delimited LIST, then enumerates each tenant's files in parallel.
// Replaces the full-bucket walk that was hammering S3 at PB-scale.
//
// The discovery itself is O(tenant_count) LIST page calls (typically
// 1–2 pages at <1K tenants); the actual file enumeration is per-tenant
// with controlled parallelism, so total S3 API load is bounded by the
// concurrency knob and tracks tenant count rather than file count.
func (m *Manifest) refreshTenantScoped(ctx context.Context, client *s3.Client) (map[string][]FileInfo, int, int64, error) {
	tenantPrefixes, err := m.discoverTenantPrefixes(ctx, client)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("discover tenants: %w", err)
	}
	if len(tenantPrefixes) == 0 {
		// Empty bucket OR no tenant directories yet — surface as
		// success with no files, same as a full-bucket refresh of an
		// empty bucket would.
		return make(map[string][]FileInfo), 0, 0, nil
	}

	// Per-tenant enumeration in parallel. Bound concurrency so we
	// don't open more in-flight S3 connections than the pool's
	// MaxConcurrentRequests budget.
	const maxParallel = 8
	type tenantResult struct {
		files      map[string][]FileInfo
		fileCount  int
		byteCount  int64
		err        error
	}
	resCh := make(chan tenantResult, len(tenantPrefixes))
	sem := make(chan struct{}, maxParallel)

	for _, tp := range tenantPrefixes {
		sem <- struct{}{}
		go func(prefix string) {
			defer func() { <-sem }()
			f, tf, tb, err := m.refreshFullBucket(ctx, client, prefix)
			resCh <- tenantResult{files: f, fileCount: tf, byteCount: tb, err: err}
		}(tp)
	}

	files := make(map[string][]FileInfo)
	var totalFiles int
	var totalBytes int64
	for i := 0; i < len(tenantPrefixes); i++ {
		r := <-resCh
		if r.err != nil {
			// One tenant's LIST failed; report the error so the caller
			// can fall back to full-bucket. Partial state isn't useful
			// since the legacy path expects a complete view.
			return nil, 0, 0, r.err
		}
		for partition, pFiles := range r.files {
			files[partition] = append(files[partition], pFiles...)
		}
		totalFiles += r.fileCount
		totalBytes += r.byteCount
	}
	return files, totalFiles, totalBytes, nil
}

// discoverTenantPrefixes returns the set of "{AccountID}/{ProjectID}/"
// prefixes present in the bucket, using delimited LIST to walk the two
// directory levels without ever touching individual file keys.
//
// For an "{OrgID}/" template (single segment), returns just the
// top-level OrgID prefixes.
func (m *Manifest) discoverTenantPrefixes(ctx context.Context, client *s3.Client) ([]string, error) {
	accounts, err := m.listCommonPrefixes(ctx, client, "")
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	// OrgID-only template: one segment is enough.
	if strings.Contains(m.prefixTemplate, "{OrgID}") && !strings.Contains(m.prefixTemplate, "{ProjectID}") {
		return accounts, nil
	}

	// Two-level template: walk into each account to find its projects.
	var out []string
	for _, acc := range accounts {
		projects, err := m.listCommonPrefixes(ctx, client, acc)
		if err != nil {
			// Skip accounts that fail; log so the operator can correlate.
			logger.Warnf("list projects under %q failed: %s", acc, err)
			continue
		}
		out = append(out, projects...)
	}
	return out, nil
}

// listCommonPrefixes performs a single delimited LIST under prefix and
// returns the CommonPrefixes entries (with the trailing "/"). Bounded
// by the number of pages, which at typical tenant counts (<1K) is a
// single page.
func (m *Manifest) listCommonPrefixes(ctx context.Context, client *s3.Client, prefix string) ([]string, error) {
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(m.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	var out []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, cp := range page.CommonPrefixes {
			out = append(out, aws.ToString(cp.Prefix))
		}
	}
	return out, nil
}

func (m *Manifest) RefreshFromS3(ctx context.Context, client *s3.Client) error {
	files := make(map[string][]FileInfo)
	var totalFiles int
	var totalBytes int64

	// When per-tenant prefix isolation is configured, the writer
	// writes under "{AccountID}/{ProjectID}/<mode>/" — many distinct
	// prefixes, not the single m.prefix. Two paths:
	//
	//   1. Tenant-scoped refresh (default at PB-scale): discover the
	//      tenant prefixes via two-level delimited LIST, then enumerate
	//      each tenant's keys in parallel. Avoids the full-bucket LIST
	//      that would walk every key (including non-data sidecars and
	//      operational files) every 30s — at 50M files this is the
	//      difference between ~50 narrow LISTs and ~1 enormous LIST,
	//      with the same total bytes but much better S3 cache locality
	//      and tighter API budget.
	//
	//   2. Full-bucket fallback: kept for the single-prefix template
	//      and as a safety net if tenant discovery fails.
	if strings.Contains(m.prefixTemplate, "{AccountID}") {
		f, tf, tb, err := m.refreshTenantScoped(ctx, client)
		if err == nil {
			files, totalFiles, totalBytes = f, tf, tb
		} else {
			// Tenant discovery failed; fall back to the legacy full-bucket
			// LIST so a transient list failure doesn't drop the manifest.
			logger.Warnf("tenant-scoped refresh failed (%s); falling back to full-bucket LIST", err)
			f, tf, tb, ferr := m.refreshFullBucket(ctx, client, "")
			if ferr != nil {
				return ferr
			}
			files, totalFiles, totalBytes = f, tf, tb
		}
	} else {
		f, tf, tb, err := m.refreshFullBucket(ctx, client, m.prefix)
		if err != nil {
			return err
		}
		files, totalFiles, totalBytes = f, tf, tb
	}

	var minT, maxT time.Time
	for partition := range files {
		t, err := parsePartitionTime(partition)
		if err != nil {
			continue
		}
		if minT.IsZero() || t.Before(minT) {
			minT = t
		}
		end := t.Add(time.Hour)
		if maxT.IsZero() || end.After(maxT) {
			maxT = end
		}
	}

	m.mu.Lock()
	// Preserve enrichment fields from previously tracked files (lost on S3 list).
	for partition, newFiles := range files {
		oldFiles := m.files[partition]
		if len(oldFiles) == 0 {
			continue
		}
		oldByKey := make(map[string]FileInfo, len(oldFiles))
		for _, of := range oldFiles {
			oldByKey[of.Key] = of
		}
		for i := range newFiles {
			if old, ok := oldByKey[newFiles[i].Key]; ok {
				newFiles[i].Labels = old.Labels
				newFiles[i].ColumnStats = old.ColumnStats
				newFiles[i].RowCount = old.RowCount
				newFiles[i].RawBytes = old.RawBytes
				newFiles[i].MinTimeNs = old.MinTimeNs
				newFiles[i].MaxTimeNs = old.MaxTimeNs
				newFiles[i].SchemaFingerprint = old.SchemaFingerprint
				newFiles[i].StorageClass = old.StorageClass
				newFiles[i].CompactionLevel = old.CompactionLevel
				newFiles[i].CreatedAt = old.CreatedAt
			}
		}
	}
	// Infer MinTimeNs/MaxTimeNs from partition key for files that don't
	// have explicit time bounds (e.g. files from before this process or
	// after a restart). Hourly partitions give us [hour, hour+1h) bounds.
	for partition, pFiles := range files {
		t, err := parsePartitionTime(partition)
		if err != nil {
			continue
		}
		pMinNs := t.UnixNano()
		pMaxNs := t.Add(time.Hour).UnixNano() - 1
		for i := range pFiles {
			if pFiles[i].MinTimeNs == 0 {
				pFiles[i].MinTimeNs = pMinNs
			}
			if pFiles[i].MaxTimeNs == 0 {
				pFiles[i].MaxTimeNs = pMaxNs
			}
		}
	}

	m.files = files
	m.rebuildByKey()
	m.rebuildTenantAggregates()
	m.rebuildIndex()
	m.minTime = minT
	m.maxTime = maxT
	m.totalFiles = totalFiles
	m.totalBytes = totalBytes
	m.lastRefresh = time.Now()
	m.mu.Unlock()

	metrics.StorageFilesTotal.Set(int64(totalFiles))
	metrics.StorageBytesTotal.Set(totalBytes)
	metrics.StoragePartitionsTotal.Set(int64(len(files)))

	var totalRows int64
	var totalRawBytes int64
	tenants := make(map[string]bool)
	for _, pFiles := range files {
		for _, fi := range pFiles {
			totalRows += fi.RowCount
			totalRawBytes += fi.RawBytes
			parts := strings.SplitN(fi.Key, "/", 3)
			if len(parts) >= 2 {
				tenants[parts[0]+"/"+parts[1]] = true
			}
		}
	}
	metrics.StorageTenantsTotal.Set(int64(len(tenants)))
	metrics.StorageRowsTotal.Set(totalRows)
	metrics.StorageRawBytesTotal.Set(totalRawBytes)
	if totalRows > 0 {
		metrics.StorageAvgRowBytes.Set(totalRawBytes / totalRows)
	}
	if totalBytes > 0 && totalRawBytes > 0 {
		metrics.StorageCompressionRatio.Set(float64(totalRawBytes) / float64(totalBytes))
	}

	logger.Infof("manifest refreshed; partitions=%d, files=%d, bytes=%d, min_time=%v, max_time=%v", len(files), totalFiles, totalBytes, minT, maxT)

	return nil
}

func (m *Manifest) HasDataForRange(startNs, endNs int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.totalFiles == 0 {
		return false
	}

	start := time.Unix(0, startNs)
	end := time.Unix(0, endNs)

	return !start.After(m.maxTime) && !end.Before(m.minTime)
}

func (m *Manifest) GetFilesForRange(startNs, endNs int64) []FileInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	start := time.Unix(0, startNs)
	end := time.Unix(0, endNs)

	// Binary search: find first partition whose end is after query start.
	idx := sort.Search(len(m.sortedPartitions), func(i int) bool {
		return m.sortedPartitions[i].end.After(start)
	})

	var result []FileInfo
	for i := idx; i < len(m.sortedPartitions); i++ {
		p := &m.sortedPartitions[i]
		// Stop once partition start is at or after query end.
		if !p.start.Before(end) {
			break
		}
		for _, fi := range m.files[p.key] {
			if fi.MinTimeNs != 0 && fi.MaxTimeNs != 0 {
				if fi.MaxTimeNs < startNs || fi.MinTimeNs > endNs {
					continue
				}
			}
			result = append(result, fi)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}

// rebuildByKey rebuilds the per-key partition index from m.files. Called
// after bulk reassignments of m.files (RefreshFromS3, snapshot Load) so
// SetFileBucket / UpdateFileColumnStats / EnrichFileMetadata can look up
// any key in O(1) instead of scanning every partition. Must be called
// under m.mu (write lock).
func (m *Manifest) rebuildByKey() {
	// Re-allocate at the expected size to avoid map growth thrash on the
	// first 50M-file rebuild. Map sizing is exact: every key appears once.
	total := 0
	for _, files := range m.files {
		total += len(files)
	}
	m.byKey = make(map[string]string, total)
	for partition, files := range m.files {
		for i := range files {
			m.byKey[files[i].Key] = partition
		}
	}
}

// rebuildIndex rebuilds the sorted partition index and inverted label index from m.files.
// Must be called while holding the write lock (m.mu).
func (m *Manifest) rebuildIndex() {
	m.sortedPartitions = m.sortedPartitions[:0]

	m.labelIndex = make(map[string]map[string]map[string]bool)

	for key := range m.files {
		t, err := parsePartitionTime(key)
		if err != nil {
			continue
		}
		m.sortedPartitions = append(m.sortedPartitions, partitionEntry{
			key:   key,
			start: t,
			end:   t.Add(time.Hour),
		})

		for _, fi := range m.files[key] {
			m.indexFileLabels(fi)
		}
	}

	sort.Slice(m.sortedPartitions, func(i, j int) bool {
		return m.sortedPartitions[i].start.Before(m.sortedPartitions[j].start)
	})
}

func (m *Manifest) indexFileLabels(fi FileInfo) {
	for field, values := range fi.Labels {
		if len(values) >= maxLabelsPerField {
			continue
		}
		fieldMap, ok := m.labelIndex[field]
		if !ok {
			fieldMap = make(map[string]map[string]bool)
			m.labelIndex[field] = fieldMap
		}
		for _, v := range values {
			keySet, ok := fieldMap[v]
			if !ok {
				keySet = make(map[string]bool)
				fieldMap[v] = keySet
			}
			keySet[fi.Key] = true
		}
	}
}

// GetFileKeysByLabel returns the set of file keys that contain the given label field=value.
// Returns nil if no index exists for that field/value pair.
func (m *Manifest) GetFileKeysByLabel(field, value string) map[string]bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fieldMap := m.labelIndex[field]
	if fieldMap == nil {
		return nil
	}
	keys := fieldMap[value]
	if len(keys) == 0 {
		return nil
	}
	cp := make(map[string]bool, len(keys))
	for k := range keys {
		cp[k] = true
	}
	return cp
}

func (m *Manifest) TotalFiles() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.totalFiles
}

func (m *Manifest) TotalBytes() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.totalBytes
}

func (m *Manifest) TotalRows() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var total int64
	for _, files := range m.files {
		for _, fi := range files {
			total += fi.RowCount
		}
	}
	return total
}

func (m *Manifest) TotalRawBytes() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var total int64
	for _, files := range m.files {
		for _, fi := range files {
			total += fi.RawBytes
		}
	}
	return total
}

func (m *Manifest) MinTime() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.minTime
}

func (m *Manifest) MaxTime() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.maxTime
}

func (m *Manifest) PartitionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.files)
}

func (m *Manifest) FilesForPartition(partition string) []FileInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	files := m.files[partition]
	cp := make([]FileInfo, len(files))
	copy(cp, files)
	return cp
}

func (m *Manifest) AllFiles() map[string][]FileInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snap := make(map[string][]FileInfo, len(m.files))
	for k, v := range m.files {
		cp := make([]FileInfo, len(v))
		copy(cp, v)
		snap[k] = cp
	}
	return snap
}

func (m *Manifest) RemoveFile(partition string, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	files := m.files[partition]
	for i, fi := range files {
		if fi.Key == key {
			m.totalFiles--
			m.totalBytes -= fi.Size
			m.updateTenantAggregateOnRemove(partition, fi)
			m.files[partition] = append(files[:i], files[i+1:]...)
			if len(m.files[partition]) == 0 {
				delete(m.files, partition)
			}
			delete(m.byKey, key)
			m.rebuildIndex()
			return
		}
	}
}

// LiveAggregate is the single source of truth for global storage
// totals — sums files / bytes / rows / raw bytes / min+max time by
// iterating the m.files map directly. The same iteration also backs
// TenantSummaries(), so /stats/overview and /tenants can't disagree
// by construction. Prefer this over the cached m.totalFiles /
// m.totalBytes counters, which can drift when RefreshFromS3 resets
// them against an incomplete S3 scan.
type LiveAggregate struct {
	Files     int
	Bytes     int64
	Rows      int64
	RawBytes  int64
	MinTimeNs int64
	MaxTimeNs int64
}

// LiveAggregate iterates the in-memory file map under the read lock
// and returns aggregate counts. O(n) over file count — acceptable
// for the API surfaces that call it once per request.
func (m *Manifest) LiveAggregate() LiveAggregate {
	return m.LiveAggregateWindow(0, 0)
}

// LiveAggregateWindow restricts the aggregate to files whose time
// range overlaps [startNs, endNs]. Pass 0 for either bound to leave
// that side open. Used by the parity endpoint to compare manifest
// totals against a VL stats query bounded to the same window.
func (m *Manifest) LiveAggregateWindow(startNs, endNs int64) LiveAggregate {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var agg LiveAggregate
	for _, files := range m.files {
		for _, fi := range files {
			if startNs > 0 && fi.MaxTimeNs > 0 && fi.MaxTimeNs < startNs {
				continue
			}
			if endNs > 0 && fi.MinTimeNs > 0 && fi.MinTimeNs > endNs {
				continue
			}
			agg.Files++
			agg.Bytes += fi.Size
			agg.Rows += fi.RowCount
			agg.RawBytes += fi.RawBytes
			if fi.MinTimeNs > 0 && (agg.MinTimeNs == 0 || fi.MinTimeNs < agg.MinTimeNs) {
				agg.MinTimeNs = fi.MinTimeNs
			}
			if fi.MaxTimeNs > agg.MaxTimeNs {
				agg.MaxTimeNs = fi.MaxTimeNs
			}
		}
	}
	return agg
}

// findFileLocked returns the slice + index for the file identified by
// key, using the per-key partition index built alongside m.files. Returns
// (nil, -1) when key is unknown.
//
// Caller must already hold m.mu (read or write). O(files-in-one-partition)
// — the byKey lookup is O(1), then we scan within that partition's slice
// (≈50–100 files on a balanced cluster) since slice indices shift on
// append/remove and we can't cache them safely.
func (m *Manifest) findFileLocked(key string) ([]FileInfo, int) {
	partition, ok := m.byKey[key]
	if !ok {
		return nil, -1
	}
	files := m.files[partition]
	for i := range files {
		if files[i].Key == key {
			return files, i
		}
	}
	// byKey said this partition, but the file isn't there — index is
	// stale. Safe to return not-found; the mutator just no-ops.
	return nil, -1
}

// SetFileBucket updates the bucket field for the file identified by
// key. Used by the bucket migration tool to flip ownership after a
// successful S3 server-side copy. Safe for concurrent use.
func (m *Manifest) SetFileBucket(key, bucket string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	files, i := m.findFileLocked(key)
	if i < 0 {
		return
	}
	files[i].Bucket = bucket
}

// UpdateFileColumnStats stores min/max stats for the named columns in the FileInfo
// identified by key. It is safe for concurrent use.
func (m *Manifest) UpdateFileColumnStats(key string, stats map[string]ColumnMinMax) {
	m.mu.Lock()
	defer m.mu.Unlock()
	files, i := m.findFileLocked(key)
	if i < 0 {
		return
	}
	files[i].ColumnStats = stats
}

// EnrichFileMetadata updates RowCount and time bounds for a file identified
// by key. Called after first opening a file during a query, using metadata
// from the Parquet footer. Only updates fields that are zero (not already set).
//
// Updates the tenant aggregate cache by the delta — a query may bump a
// file's RowCount from 0 to (say) 1M, and TenantSummaries must reflect
// that without scanning every file.
func (m *Manifest) EnrichFileMetadata(key string, rowCount int64, minTimeNs, maxTimeNs int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	files, i := m.findFileLocked(key)
	if i < 0 {
		return
	}

	var rowDelta int64
	if files[i].RowCount == 0 && rowCount > 0 {
		rowDelta = rowCount
		files[i].RowCount = rowCount
	}
	if files[i].MinTimeNs == 0 && minTimeNs > 0 {
		files[i].MinTimeNs = minTimeNs
	}
	if files[i].MaxTimeNs == 0 && maxTimeNs > 0 {
		files[i].MaxTimeNs = maxTimeNs
	}

	if rowDelta != 0 {
		if tk, ok := m.tenantKeyFromFileKey(key); ok {
			if a := m.tenantAggregates[tk]; a != nil {
				a.rows += rowDelta
			}
		}
	}
}

func (m *Manifest) AddFile(partition string, fi FileInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Idempotency guard (spec §2.2.4): two compaction loops racing on the
	// same partition can produce duplicate AddFile calls with the same key
	// (and also distinct UUID-suffixed keys; that case is handled by Tier B
	// orphan sweep). For the same-key race, silently skip the second insert
	// so the manifest never grows past one entry per (partition, key).
	// Negative-control proof: removing this loop causes
	// TestManifest_AddFile_IdempotentOnKey to count 2 entries and the
	// race test TestManifest_AddFile_ConcurrentSameKeyOnlyOneEntry to
	// produce N duplicates under -race.
	//
	// byKey is the O(1) authoritative source for "does this key already
	// exist anywhere in the manifest" — same partition or otherwise. A
	// duplicate AddFile is dropped before mutating any state.
	if _, exists := m.byKey[fi.Key]; exists {
		metrics.ManifestAddFileDuplicateKeyTotal.Inc()
		return
	}

	isNew := len(m.files[partition]) == 0
	m.files[partition] = append(m.files[partition], fi)
	m.byKey[fi.Key] = partition
	m.updateTenantAggregateOnAdd(partition, fi)
	m.totalFiles++
	m.totalBytes += fi.Size

	if isNew {
		if t, err := parsePartitionTime(partition); err == nil {
			entry := partitionEntry{key: partition, start: t, end: t.Add(time.Hour)}
			i := sort.Search(len(m.sortedPartitions), func(j int) bool {
				return !m.sortedPartitions[j].start.Before(t)
			})
			m.sortedPartitions = append(m.sortedPartitions, partitionEntry{})
			copy(m.sortedPartitions[i+1:], m.sortedPartitions[i:])
			m.sortedPartitions[i] = entry
		}
	}
	m.indexFileLabels(fi)

	metrics.ManifestFiles.Set(int64(m.totalFiles))
	metrics.ManifestBytes.Set(m.totalBytes)

	t, err := parsePartitionTime(partition)
	if err != nil {
		return
	}
	end := t.Add(time.Hour)
	if m.minTime.IsZero() || t.Before(m.minTime) {
		m.minTime = t
	}
	if m.maxTime.IsZero() || end.After(m.maxTime) {
		m.maxTime = end
	}
}

// MarkAttempt records that the caller began (or is about to begin) a
// compaction attempt on the given partition. The scheduler must call this
// BEFORE selecting files so a crash mid-compaction still leaves a fresh
// timestamp; OrphanSweep Tier A then has to wait the full staleness window
// (default 3×Interval) before stealing — see spec §2.2.2 and §2.4.1.
//
// Safe for concurrent use. Stored in-memory only (spec §2.2.3).
func (m *Manifest) MarkAttempt(partition string, t time.Time) {
	m.mu.Lock()
	m.partitionAttempts[partition] = t
	m.mu.Unlock()
}

// LastAttempt returns the most recent MarkAttempt timestamp for the given
// partition. Returns the zero value when no attempt was ever recorded
// (cold partition). Used by OrphanSweep Tier A. Safe for concurrent use.
func (m *Manifest) LastAttempt(partition string) time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.partitionAttempts[partition]
}

// AttemptsView returns a snapshot of (partition, lastAttempt) for every
// partition the manifest is aware of, including those with no attempt
// recorded (zero-value time). Used by OrphanSweep Tier A. The returned
// map is a copy and safe to mutate. Safe for concurrent use.
func (m *Manifest) AttemptsView() map[string]time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]time.Time, len(m.files))
	for p := range m.files {
		out[p] = m.partitionAttempts[p] // zero time if absent
	}
	return out
}

// KeysUnderPrefix returns all manifest-tracked file keys whose key has
// the given prefix. Used by OrphanSweep Tier B to compare manifest state
// against an S3 LIST output. Pass empty prefix to return every key. The
// returned slice is freshly allocated and safe to mutate. Safe for
// concurrent use.
func (m *Manifest) KeysUnderPrefix(prefix string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string

	// Fast path: the orphan-sweep caller passes a date-bucketed prefix
	// like ".../dt=2026-06-04/". Each such prefix maps to at most 24
	// hourly partitions, which are first-class in m.files. Iterating
	// just those partitions converts a 50M-file scan into a ~2K-file
	// one — a 25,000× speedup at PB-scale.
	//
	// For prefixes without a date component (the empty prefix or
	// arbitrary partial keys), we still need the legacy full scan to
	// preserve semantics.
	if date := extractDateFromPrefix(prefix); date != "" {
		partitionPrefix := "dt=" + date + "/"
		for partition, files := range m.files {
			if !strings.HasPrefix(partition, partitionPrefix) {
				continue
			}
			for _, fi := range files {
				if strings.HasPrefix(fi.Key, prefix) {
					out = append(out, fi.Key)
				}
			}
		}
		return out
	}

	// Fallback: full scan for non-date prefixes. Kept for correctness on
	// arbitrary inputs (admin tooling, future callers).
	for _, files := range m.files {
		for _, fi := range files {
			if prefix == "" || strings.HasPrefix(fi.Key, prefix) {
				out = append(out, fi.Key)
			}
		}
	}
	return out
}

// HasKey returns true if the manifest knows about the given file key.
// O(1) lookup via byKey — preferred over KeysUnderPrefix+contains for
// single-key existence checks (the orphan sweep's third safety gate).
//
// Safe for concurrent use.
func (m *Manifest) HasKey(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.byKey[key]
	return ok
}

// extractDateFromPrefix returns "YYYY-MM-DD" from any prefix that
// contains "dt=YYYY-MM-DD" (with or without trailing characters).
// Returns "" if no date segment is present. Mirrors the parsing in
// datePrefixOf at internal/compaction/orphan_sweep.go so the
// fast-path lookup agrees with the sweep's bucketing.
func extractDateFromPrefix(prefix string) string {
	i := strings.Index(prefix, "dt=")
	if i < 0 {
		return ""
	}
	rest := prefix[i+3:]
	const dateLen = len("YYYY-MM-DD")
	if len(rest) < dateLen {
		return ""
	}
	date := rest[:dateLen]
	// Validate: YYYY-MM-DD has hyphens at indices 4 and 7.
	if date[4] != '-' || date[7] != '-' {
		return ""
	}
	return date
}

// ExtractPartition is the exported wrapper for extractPartition.
func ExtractPartition(key string) string {
	return extractPartition(key)
}

// extractPartition extracts "dt=YYYY-MM-DD/hour=HH" from an S3 key.
func extractPartition(key string) string {
	dir := path.Dir(key)
	parts := strings.Split(dir, "/")

	var dtPart, hourPart string
	for _, p := range parts {
		if strings.HasPrefix(p, "dt=") {
			dtPart = p
		}
		if strings.HasPrefix(p, "hour=") {
			hourPart = p
		}
	}

	if dtPart == "" {
		return ""
	}
	if hourPart == "" {
		return dtPart
	}
	return dtPart + "/" + hourPart
}

type persistedManifest struct {
	Files       map[string][]FileInfo `json:"files"`
	MinTimeNs   int64                 `json:"min_time_ns"`
	MaxTimeNs   int64                 `json:"max_time_ns"`
	TotalFiles_ int                   `json:"total_files"`
	TotalBytes_ int64                 `json:"total_bytes"`
	SavedAt     time.Time             `json:"saved_at"`
}

func (m *Manifest) SaveTo(path string) error {
	m.mu.RLock()
	snap := persistedManifest{
		Files:       m.files,
		MinTimeNs:   m.minTime.UnixNano(),
		MaxTimeNs:   m.maxTime.UnixNano(),
		TotalFiles_: m.totalFiles,
		TotalBytes_: m.totalBytes,
		SavedAt:     time.Now(),
	}
	m.mu.RUnlock()

	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create dir: %w", err)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename manifest: %w", err)
	}

	logger.Infof("manifest saved; path=%s, files=%d, bytes=%d", path, snap.TotalFiles_, len(data))
	return nil
}

func (m *Manifest) LoadFrom(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read manifest: %w", err)
	}

	var snap persistedManifest
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("unmarshal manifest: %w", err)
	}

	m.mu.Lock()
	m.files = snap.Files
	m.rebuildByKey()
	m.rebuildTenantAggregates()
	m.rebuildIndex()
	m.totalFiles = snap.TotalFiles_
	m.totalBytes = snap.TotalBytes_
	if snap.MinTimeNs != 0 {
		m.minTime = time.Unix(0, snap.MinTimeNs)
	}
	if snap.MaxTimeNs != 0 {
		m.maxTime = time.Unix(0, snap.MaxTimeNs)
	}
	m.mu.Unlock()

	logger.Infof("manifest loaded from disk; path=%s, files=%d, bytes=%d, saved_at=%v", path, snap.TotalFiles_, snap.TotalBytes_, snap.SavedAt)
	return nil
}

// ParsePartitionTime is the exported wrapper for parsePartitionTime.
func ParsePartitionTime(partition string) (time.Time, error) {
	return parsePartitionTime(partition)
}

// parsePartitionTime parses "dt=2026-05-02/hour=10" into a time.Time.
func parsePartitionTime(partition string) (time.Time, error) {
	parts := strings.Split(partition, "/")
	var dateStr, hourStr string
	for _, p := range parts {
		if v, ok := strings.CutPrefix(p, "dt="); ok {
			dateStr = v
		}
		if v, ok := strings.CutPrefix(p, "hour="); ok {
			hourStr = v
		}
	}

	if dateStr == "" {
		return time.Time{}, fmt.Errorf("no dt= in partition %q", partition)
	}

	layout := "2006-01-02"
	t, err := time.Parse(layout, dateStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse date %q: %w", dateStr, err)
	}

	if hourStr != "" {
		var hour int
		_, err := fmt.Sscanf(hourStr, "%d", &hour)
		if err == nil && hour >= 0 && hour < 24 {
			t = t.Add(time.Duration(hour) * time.Hour)
		}
	}

	return t, nil
}

type PartitionSummary struct {
	Date  string `json:"date"`
	Hours []int  `json:"hours"`
	Files int    `json:"files"`
	Bytes int64  `json:"bytes"`
}

func (m *Manifest) GetPartitions(startDate, endDate string) []PartitionSummary {
	m.mu.RLock()
	defer m.mu.RUnlock()

	byDate := make(map[string]*PartitionSummary)

	for partition, files := range m.files {
		parts := strings.Split(partition, "/")
		var dateStr, hourStr string
		for _, p := range parts {
			if v, ok := strings.CutPrefix(p, "dt="); ok {
				dateStr = v
			}
			if v, ok := strings.CutPrefix(p, "hour="); ok {
				hourStr = v
			}
		}
		if dateStr == "" {
			continue
		}
		if startDate != "" && dateStr < startDate {
			continue
		}
		if endDate != "" && dateStr > endDate {
			continue
		}

		ps, ok := byDate[dateStr]
		if !ok {
			ps = &PartitionSummary{Date: dateStr}
			byDate[dateStr] = ps
		}
		var totalBytes int64
		for _, f := range files {
			totalBytes += f.Size
		}
		ps.Files += len(files)
		ps.Bytes += totalBytes
		if hourStr != "" {
			var hour int
			if _, err := fmt.Sscanf(hourStr, "%d", &hour); err == nil {
				ps.Hours = append(ps.Hours, hour)
			}
		}
	}

	result := make([]PartitionSummary, 0, len(byDate))
	for _, ps := range byDate {
		sort.Ints(ps.Hours)
		result = append(result, *ps)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Date < result[j].Date
	})
	return result
}

// TenantSummary holds per-tenant aggregate stats derived from manifest S3 keys.
type TenantSummary struct {
	AccountID  string
	ProjectID  string
	TotalFiles int
	TotalBytes int64
	TotalRows  int64
	RawBytes   int64
	Partitions int
	MinTime    time.Time
	MaxTime    time.Time
}

// TenantSummariesInWindow returns TenantSummaries restricted to
// tenants holding files whose time range overlaps [startNs, endNs].
// Pass 0 for either bound to leave that side open. Used by VT's
// servicegraph task to iterate only tenants with recent activity.
func (m *Manifest) TenantSummariesInWindow(startNs, endNs int64) []TenantSummary {
	all := m.TenantSummaries()
	if startNs == 0 && endNs == 0 {
		return all
	}
	startT := time.Unix(0, startNs)
	endT := time.Unix(0, endNs)
	out := make([]TenantSummary, 0, len(all))
	for _, s := range all {
		// Include tenant if its [MinTime, MaxTime] overlaps [start, end].
		if endNs > 0 && !s.MinTime.IsZero() && s.MinTime.After(endT) {
			continue
		}
		if startNs > 0 && !s.MaxTime.IsZero() && s.MaxTime.Before(startT) {
			continue
		}
		out = append(out, s)
	}
	return out
}

// TenantSummaries extracts per-tenant stats from S3 keys.
// Supports both integer template ({AccountID}/{ProjectID}/) and OrgID template ({OrgID}/).
//
// Reads directly from the tenantAggregates incremental cache, which is
// kept consistent with m.files by AddFile / RemoveFile / EnrichFileMetadata
// and rebuilt wholesale by rebuildTenantAggregates() on RefreshFromS3 +
// snapshot Load. The cache is the single source of truth — this function
// only allocates the output slice and sorts it.
//
// Complexity: O(tenants log tenants) for the sort, O(tenants) for the
// snapshot. Previously O(total files) which is O(50M) at PB-scale.
func (m *Manifest) TenantSummaries() []TenantSummary {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]TenantSummary, 0, len(m.tenantAggregates))
	for k, a := range m.tenantAggregates {
		var minT, maxT time.Time
		if a.minTimeNs > 0 {
			minT = time.Unix(0, a.minTimeNs)
		}
		if a.maxTimeNs > 0 {
			maxT = time.Unix(0, a.maxTimeNs)
		}
		result = append(result, TenantSummary{
			AccountID:  k.account,
			ProjectID:  k.project,
			TotalFiles: a.files,
			TotalBytes: a.bytes,
			TotalRows:  a.rows,
			RawBytes:   a.rawBytes,
			Partitions: len(a.partitions),
			MinTime:    minT,
			MaxTime:    maxT,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].TotalBytes > result[j].TotalBytes
	})
	return result
}
