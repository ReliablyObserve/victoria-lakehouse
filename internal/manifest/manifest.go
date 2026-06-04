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
	minTime          time.Time
	maxTime          time.Time
	totalFiles       int
	totalBytes       int64
	lastRefresh      time.Time
	prefix           string
	bucket           string
	prefixTemplate   string

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
		prefix:            prefix,
		bucket:            bucket,
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

func (m *Manifest) RefreshFromS3(ctx context.Context, client *s3.Client) error {
	files := make(map[string][]FileInfo)
	var totalFiles int
	var totalBytes int64

	// When per-tenant prefix isolation is configured, the writer
	// writes under "{AccountID}/{ProjectID}/<mode>/" — many distinct
	// prefixes, not the single m.prefix. List at the bucket root in
	// that case and rely on the partition-extractor below to filter
	// out non-data keys. Single-tenant deployments keep the
	// narrow-prefix scan they had before for cheaper paging.
	listPrefix := m.prefix
	if strings.Contains(m.prefixTemplate, "{AccountID}") {
		listPrefix = ""
	}
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(m.bucket),
		Prefix: aws.String(listPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list objects: %w", err)
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
			m.files[partition] = append(files[:i], files[i+1:]...)
			if len(m.files[partition]) == 0 {
				delete(m.files, partition)
			}
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

// SetFileBucket updates the bucket field for the file identified by
// key. Used by the bucket migration tool to flip ownership after a
// successful S3 server-side copy. Safe for concurrent use.
func (m *Manifest) SetFileBucket(key, bucket string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, files := range m.files {
		for i := range files {
			if files[i].Key == key {
				files[i].Bucket = bucket
				return
			}
		}
	}
}

// UpdateFileColumnStats stores min/max stats for the named columns in the FileInfo
// identified by key. It is safe for concurrent use.
func (m *Manifest) UpdateFileColumnStats(key string, stats map[string]ColumnMinMax) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, files := range m.files {
		for i := range files {
			if files[i].Key == key {
				files[i].ColumnStats = stats
				return
			}
		}
	}
}

// EnrichFileMetadata updates RowCount and time bounds for a file identified
// by key. Called after first opening a file during a query, using metadata
// from the Parquet footer. Only updates fields that are zero (not already set).
func (m *Manifest) EnrichFileMetadata(key string, rowCount int64, minTimeNs, maxTimeNs int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, files := range m.files {
		for i := range files {
			if files[i].Key == key {
				if files[i].RowCount == 0 && rowCount > 0 {
					files[i].RowCount = rowCount
				}
				if files[i].MinTimeNs == 0 && minTimeNs > 0 {
					files[i].MinTimeNs = minTimeNs
				}
				if files[i].MaxTimeNs == 0 && maxTimeNs > 0 {
					files[i].MaxTimeNs = maxTimeNs
				}
				return
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
	for _, existing := range m.files[partition] {
		if existing.Key == fi.Key {
			metrics.ManifestAddFileDuplicateKeyTotal.Inc()
			return
		}
	}

	isNew := len(m.files[partition]) == 0
	m.files[partition] = append(m.files[partition], fi)
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
	for _, files := range m.files {
		for _, fi := range files {
			if prefix == "" || strings.HasPrefix(fi.Key, prefix) {
				out = append(out, fi.Key)
			}
		}
	}
	return out
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

// TenantSummaries extracts per-tenant stats from S3 keys.
// Supports both integer template ({AccountID}/{ProjectID}/) and OrgID template ({OrgID}/).
func (m *Manifest) TenantSummaries() []TenantSummary {
	m.mu.RLock()
	defer m.mu.RUnlock()

	type key struct{ account, project string }
	type accum struct {
		files      int
		bytes      int64
		rows       int64
		rawBytes   int64
		partitions map[string]struct{}
		minT, maxT time.Time
	}

	segments := 2
	hasOrgID := strings.Contains(m.prefixTemplate, "{OrgID}")
	if hasOrgID && !strings.Contains(m.prefixTemplate, "{ProjectID}") {
		segments = 1
	}

	byTenant := make(map[key]*accum)

	for partition, files := range m.files {
		for _, fi := range files {
			parts := strings.SplitN(fi.Key, "/", segments+2)
			if len(parts) < segments+1 {
				continue
			}

			var k key
			if segments == 1 {
				k = key{account: parts[0], project: ""}
			} else {
				k = key{account: parts[0], project: parts[1]}
			}

			a, ok := byTenant[k]
			if !ok {
				a = &accum{partitions: make(map[string]struct{})}
				byTenant[k] = a
			}
			a.files++
			a.bytes += fi.Size
			a.rows += fi.RowCount
			a.rawBytes += fi.RawBytes
			a.partitions[partition] = struct{}{}

			if t, err := parsePartitionTime(partition); err == nil {
				if a.minT.IsZero() || t.Before(a.minT) {
					a.minT = t
				}
				end := t.Add(time.Hour)
				if a.maxT.IsZero() || end.After(a.maxT) {
					a.maxT = end
				}
			}
		}
	}

	result := make([]TenantSummary, 0, len(byTenant))
	for k, a := range byTenant {
		result = append(result, TenantSummary{
			AccountID:  k.account,
			ProjectID:  k.project,
			TotalFiles: a.files,
			TotalBytes: a.bytes,
			TotalRows:  a.rows,
			RawBytes:   a.rawBytes,
			Partitions: len(a.partitions),
			MinTime:    a.minT,
			MaxTime:    a.maxT,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].TotalBytes > result[j].TotalBytes
	})
	return result
}
