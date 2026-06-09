package manifest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const metadataSidecarName = "_file_metadata.json"

type FileMeta struct {
	RowCount          int64               `json:"rc"`
	MinTimeNs         int64               `json:"mn"`
	MaxTimeNs         int64               `json:"mx"`
	RawBytes          int64               `json:"rb,omitempty"`
	SchemaFingerprint string              `json:"sf,omitempty"`
	Labels            map[string][]string `json:"lb,omitempty"`
}

type FileMetaSidecar struct {
	Files map[string]FileMeta `json:"f"`
}

func MetadataSidecarKey(prefix, partition string) string {
	return prefix + partition + "/" + metadataSidecarName
}

func MarshalFileMetaSidecar(pm *FileMetaSidecar) ([]byte, error) {
	return json.Marshal(pm)
}

func UnmarshalFileMetaSidecar(data []byte) (*FileMetaSidecar, error) {
	var pm FileMetaSidecar
	if err := json.Unmarshal(data, &pm); err != nil {
		return nil, err
	}
	return &pm, nil
}

func FileInfoToMeta(fi FileInfo) FileMeta {
	return FileMeta{
		RowCount:          fi.RowCount,
		MinTimeNs:         fi.MinTimeNs,
		MaxTimeNs:         fi.MaxTimeNs,
		RawBytes:          fi.RawBytes,
		SchemaFingerprint: fi.SchemaFingerprint,
		Labels:            fi.Labels,
	}
}

func (fm FileMeta) ApplyTo(fi *FileInfo) {
	if fi.RowCount == 0 && fm.RowCount > 0 {
		fi.RowCount = fm.RowCount
	}
	if fi.MinTimeNs == 0 && fm.MinTimeNs > 0 {
		fi.MinTimeNs = fm.MinTimeNs
	}
	if fi.MaxTimeNs == 0 && fm.MaxTimeNs > 0 {
		fi.MaxTimeNs = fm.MaxTimeNs
	}
	if fi.RawBytes == 0 && fm.RawBytes > 0 {
		fi.RawBytes = fm.RawBytes
	}
	if fi.SchemaFingerprint == "" && fm.SchemaFingerprint != "" {
		fi.SchemaFingerprint = fm.SchemaFingerprint
	}
	if fi.Labels == nil && fm.Labels != nil {
		fi.Labels = fm.Labels
	}
}

func (m *Manifest) WritePartitionSidecar(ctx context.Context, client *s3.Client, partition string) error {
	m.mu.RLock()
	pFiles := m.files[partition]
	m.mu.RUnlock()

	if len(pFiles) == 0 {
		return nil
	}

	sc := &FileMetaSidecar{Files: make(map[string]FileMeta, len(pFiles))}
	for _, fi := range pFiles {
		if fi.RowCount > 0 {
			sc.Files[fi.Key] = FileInfoToMeta(fi)
		}
	}

	if len(sc.Files) == 0 {
		return nil
	}

	data, err := MarshalFileMetaSidecar(sc)
	if err != nil {
		return fmt.Errorf("marshal partition meta: %w", err)
	}

	key := MetadataSidecarKey(m.prefix, partition)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(m.bucket),
		Key:         aws.String(key),
		Body:        strings.NewReader(string(data)),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("put sidecar %s: %w", key, err)
	}

	return nil
}

// FileMetaProvider supplies per-file metadata from an in-RAM source (the pmeta
// fileMetaFacet) so the manifest can enrich FileInfo without per-partition sidecar
// GETs. Defined here, implemented in the storage layer, so manifest stays decoupled
// from internal/pmeta.
type FileMetaProvider interface {
	FileMeta(partition, fileKey string) (FileMeta, bool)
}

// EnrichFromProvider fills missing FileInfo metadata from the provider. It returns
// the count of files enriched with REAL metadata (RowCount > 0) and the list of
// partitions that STILL contain a file with no metadata afterward (neither the
// disk-cache nor the provider covered it). It is the pmeta read-flip's substitute
// for LoadSidecars: same ApplyTo enrichment, but from RAM (the loaded bundle)
// instead of S3 sidecar GETs. The caller falls back to LoadSidecarsForPartitions on
// ONLY the uncovered partitions — so a fully-covered bundle skips the sidecar GETs
// entirely, and one recent uncovered file no longer forces a full re-load.
func (m *Manifest) EnrichFromProvider(p FileMetaProvider) (int, []string) {
	if p == nil {
		return 0, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	enriched := 0
	var uncovered []string
	for partition, pFiles := range m.files {
		needsSidecar := false
		for i := range pFiles {
			if fm, ok := p.FileMeta(partition, pFiles[i].Key); ok && fm.RowCount > 0 {
				fm.ApplyTo(&pFiles[i])
				enriched++
			}
			if pFiles[i].RowCount == 0 { // still no metadata after disk + facet
				needsSidecar = true
			}
		}
		if needsSidecar {
			uncovered = append(uncovered, partition)
		}
	}
	return enriched, uncovered
}

func (m *Manifest) LoadSidecars(ctx context.Context, client *s3.Client, concurrency int) int {
	m.mu.RLock()
	partitions := make([]string, 0, len(m.files))
	for p := range m.files {
		partitions = append(partitions, p)
	}
	m.mu.RUnlock()
	return m.LoadSidecarsForPartitions(ctx, client, concurrency, partitions)
}

// LoadSidecarsForPartitions loads the `_file_metadata.json` sidecars for ONLY the
// given partitions — the pmeta read-flip's fallback for partitions the bundle did
// not fully cover. LoadSidecars is the all-partitions wrapper.
func (m *Manifest) LoadSidecarsForPartitions(ctx context.Context, client *s3.Client, concurrency int, partitions []string) int {
	if concurrency <= 0 {
		concurrency = 16
	}
	if len(partitions) == 0 {
		return 0
	}

	type sidecarResult struct {
		partition string
		sc        *FileMetaSidecar
	}

	ch := make(chan string, len(partitions))
	for _, p := range partitions {
		ch <- p
	}
	close(ch)

	results := make(chan sidecarResult, len(partitions))
	var wg sync.WaitGroup
	for i := 0; i < concurrency && i < len(partitions); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for partition := range ch {
				if ctx.Err() != nil {
					return
				}
				key := MetadataSidecarKey(m.prefix, partition)
				resp, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(m.bucket),
					Key:    aws.String(key),
				})
				if err != nil {
					continue
				}
				data := make([]byte, 0, 4096)
				buf := make([]byte, 4096)
				for {
					n, readErr := resp.Body.Read(buf)
					if n > 0 {
						data = append(data, buf[:n]...)
					}
					if readErr != nil {
						break
					}
				}
				_ = resp.Body.Close()

				sc, err := UnmarshalFileMetaSidecar(data)
				if err != nil {
					continue
				}
				results <- sidecarResult{partition: partition, sc: sc}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	enriched := 0
	m.mu.Lock()
	for r := range results {
		pFiles := m.files[r.partition]
		for i := range pFiles {
			if fm, ok := r.sc.Files[pFiles[i].Key]; ok {
				fm.ApplyTo(&pFiles[i])
				enriched++
			}
		}
	}
	m.mu.Unlock()

	if enriched > 0 {
		logger.Infof("loaded %d file metadata entries from %d partition sidecars", enriched, len(partitions))
	}
	return enriched
}
