package manifest

import (
	"context"
	"fmt"
	"log/slog"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type FileInfo struct {
	Key  string
	Size int64
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
	logger      *slog.Logger
}

func New(bucket, prefix string, logger *slog.Logger) *Manifest {
	return &Manifest{
		files:  make(map[string][]FileInfo),
		prefix: prefix,
		bucket: bucket,
		logger: logger.With("component", "manifest"),
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

	m.logger.Info("manifest refreshed",
		"partitions", len(files),
		"files", totalFiles,
		"bytes", totalBytes,
		"min_time", minT,
		"max_time", maxT,
	)

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

func (m *Manifest) AddFile(partition string, fi FileInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.files[partition] = append(m.files[partition], fi)
	m.totalFiles++
	m.totalBytes += fi.Size

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
