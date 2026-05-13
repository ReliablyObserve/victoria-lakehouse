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

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type FileInfo struct {
	Key               string              `json:"key"`
	Size              int64               `json:"size"`
	RowCount          int64               `json:"row_count,omitempty"`
	MinTimeNs         int64               `json:"min_time_ns,omitempty"`
	MaxTimeNs         int64               `json:"max_time_ns,omitempty"`
	RawBytes          int64               `json:"raw_bytes,omitempty"`
	SchemaFingerprint string              `json:"schema_fp,omitempty"`
	CompactionLevel   int                 `json:"compaction_level,omitempty"`
	Labels            map[string][]string `json:"labels,omitempty"`
	StorageClass      string              `json:"storage_class,omitempty"`
	ClassCheckedAt    time.Time           `json:"class_checked_at,omitempty"`
	ClassSource       string              `json:"class_source,omitempty"`
	CreatedAt         time.Time           `json:"created_at,omitempty"`
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

type Manifest struct {
	mu          sync.RWMutex
	files       map[string][]FileInfo // "dt=2026-05-02/hour=10" -> files
	minTime     time.Time
	maxTime     time.Time
	totalFiles  int
	totalBytes  int64
	lastRefresh time.Time
	prefix      string
	bucket      string
}

func New(bucket, prefix string) *Manifest {
	return &Manifest{
		files:  make(map[string][]FileInfo),
		prefix: prefix,
		bucket: bucket,
	}
}

func (m *Manifest) RefreshFromS3(ctx context.Context, client *s3.Client) error {
	files := make(map[string][]FileInfo)
	var totalFiles int
	var totalBytes int64

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(m.bucket),
		Prefix: aws.String(m.prefix),
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
	m.files = files
	m.minTime = minT
	m.maxTime = maxT
	m.totalFiles = totalFiles
	m.totalBytes = totalBytes
	m.lastRefresh = time.Now()
	m.mu.Unlock()

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

	var result []FileInfo
	for partition, files := range m.files {
		t, err := parsePartitionTime(partition)
		if err != nil {
			continue
		}
		partEnd := t.Add(time.Hour)
		if t.Before(end) && partEnd.After(start) {
			result = append(result, files...)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
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
			return
		}
	}
}

func (m *Manifest) AddFile(partition string, fi FileInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.files[partition] = append(m.files[partition], fi)
	m.totalFiles++
	m.totalBytes += fi.Size
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
