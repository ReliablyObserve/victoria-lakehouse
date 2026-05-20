package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

var (
	serviceNames = []string{
		"api-gateway", "auth-service", "billing", "cache-proxy",
		"cart-service", "catalog", "checkout", "email-service",
		"frontend", "inventory", "notification", "order-service",
		"payment", "recommendation", "search", "shipping",
		"subscription", "telemetry-collector", "user-service", "webhook",
	}
	levels   = []string{"INFO", "WARN", "ERROR", "DEBUG"}
	hostPool = []string{"host-01", "host-02", "host-03", "host-04", "host-05"}
)

type seedConfig struct {
	endpoint string
	rows     int
	signal   string
}

func seedData(cfg seedConfig) error {
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) // #nosec G404
	client := &http.Client{Timeout: 30 * time.Second}
	batchSize := 1000
	for sent := 0; sent < cfg.rows; sent += batchSize {
		count := batchSize
		if sent+count > cfg.rows {
			count = cfg.rows - sent
		}
		var buf bytes.Buffer
		for i := 0; i < count; i++ {
			entry := map[string]interface{}{
				"_msg":         fmt.Sprintf("benchmark log entry %d", sent+i),
				"_time":        time.Now().Add(-time.Duration(rng.Intn(3600)) * time.Second).Format(time.RFC3339Nano),
				"service.name": serviceNames[rng.Intn(len(serviceNames))],
				"level":        levels[rng.Intn(len(levels))],
				"host.name":    hostPool[rng.Intn(len(hostPool))],
				"trace_id":     fmt.Sprintf("%032x", rng.Uint64()),
			}
			line, _ := json.Marshal(entry)
			buf.Write(line)
			buf.WriteByte('\n')
		}
		req, err := http.NewRequest("POST", cfg.endpoint+"/insert/jsonline", &buf)
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/x-ndjson")
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("insert batch at %d: %w", sent, err)
		}
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			return fmt.Errorf("insert returned %d at batch %d", resp.StatusCode, sent)
		}
	}
	return nil
}
