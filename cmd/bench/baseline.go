package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Baseline struct {
	Timestamp time.Time              `json:"timestamp"`
	GitSHA    string                 `json:"git_sha"`
	Tier      string                 `json:"tier"`
	Signal    string                 `json:"signal"`
	FileCount int                    `json:"file_count"`
	Write     map[string]WriteResult `json:"write,omitempty"`
	Read      []ReadResult           `json:"read,omitempty"`
}

type WriteResult struct {
	RowsPerSec       float64 `json:"rows_per_sec"`
	P50Ms            float64 `json:"p50_ms"`
	P95Ms            float64 `json:"p95_ms"`
	FlushMs          float64 `json:"flush_ms"`
	CompressionRatio float64 `json:"compression_ratio"`
}

type ReadResult struct {
	Endpoint string  `json:"endpoint"`
	Filter   string  `json:"filter"`
	ColdMs   float64 `json:"cold_ms"`
	WarmMs   float64 `json:"warm_ms"`
	HotMs    float64 `json:"hot_ms"`
}

func baselineFilePath(dir, signal, tier string) string {
	return fmt.Sprintf("%s/baseline-%s-%s.json", dir, signal, tier)
}

func writeBaseline(path string, b *Baseline) error {
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}
