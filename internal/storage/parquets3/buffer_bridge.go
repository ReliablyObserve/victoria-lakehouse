package parquets3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// BufferBridge queries insert pods for unflushed data via parallel fan-out.
// Select pods use this to achieve zero-delay reads by merging buffered rows
// from insert pods with already-flushed Parquet data from S3.
type BufferBridge struct {
	cfg              *config.SelectConfig
	mode             config.Mode
	client           *http.Client
	mu               sync.RWMutex
	endpoints        []string
	sameAZEndpoints  []string
	crossAZEndpoints []string
	selfAZ           string
}

// NewBufferBridge creates a BufferBridge configured for the given mode.
func NewBufferBridge(cfg *config.SelectConfig, mode config.Mode) *BufferBridge {
	return &BufferBridge{
		cfg:  cfg,
		mode: mode,
		client: &http.Client{
			Timeout: cfg.BufferQueryTimeout,
		},
	}
}

// SetEndpoints updates the list of insert pod endpoints to query.
// Typically called by the DNS discovery loop when the headless service resolves.
func (b *BufferBridge) SetEndpoints(endpoints []string) {
	b.mu.Lock()
	b.endpoints = endpoints
	b.mu.Unlock()
}

// SetEndpointsWithZones updates insert pod endpoints with AZ classification.
func (b *BufferBridge) SetEndpointsWithZones(epZones map[string]string, selfAZ string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.selfAZ = selfAZ
	b.endpoints = make([]string, 0, len(epZones))
	b.sameAZEndpoints = nil
	b.crossAZEndpoints = nil

	for ep, zone := range epZones {
		b.endpoints = append(b.endpoints, ep)
		if zone == selfAZ {
			b.sameAZEndpoints = append(b.sameAZEndpoints, ep)
		} else {
			b.crossAZEndpoints = append(b.crossAZEndpoints, ep)
		}
	}
}

// getQueryEndpoints always returns ALL insert pod endpoints regardless of AZ.
// Buffer queries must reach every insert pod to avoid missing unflushed data —
// with 3 AZs, same-AZ-only would miss 2/3 of buffered rows.
// AZ-aware routing is only appropriate for peer cache (L3), not buffer queries.
func (b *BufferBridge) getQueryEndpoints() []string {
	return b.endpoints
}

// QueryLogs fans out to all insert pod endpoints in parallel and returns
// the merged set of buffered log rows within the given time range.
// Endpoint errors are silently ignored for graceful degradation.
func (b *BufferBridge) QueryLogs(ctx context.Context, startNs, endNs int64) ([]schema.LogRow, error) {
	if !b.cfg.BufferQueryEnabled {
		return nil, nil
	}

	b.mu.RLock()
	eps := b.getQueryEndpoints()
	b.mu.RUnlock()

	if len(eps) == 0 {
		return nil, nil
	}

	var mu sync.Mutex
	var all []schema.LogRow
	var wg sync.WaitGroup

	for _, ep := range eps {
		wg.Add(1)
		go func(endpoint string) {
			defer wg.Done()
			rows, err := b.fetchLogs(ctx, endpoint, startNs, endNs)
			if err != nil {
				return
			}
			mu.Lock()
			all = append(all, rows...)
			mu.Unlock()
		}(ep)
	}
	wg.Wait()

	return all, nil
}

func (b *BufferBridge) fetchLogs(ctx context.Context, endpoint string, startNs, endNs int64) ([]schema.LogRow, error) {
	url := fmt.Sprintf("%s/internal/buffer/query?start=%d&end=%d&mode=%s",
		endpoint, startNs, endNs, string(b.mode))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("buffer query returned %d", resp.StatusCode)
	}

	var rows []schema.LogRow
	dec := json.NewDecoder(resp.Body)
	for dec.More() {
		var row schema.LogRow
		if err := dec.Decode(&row); err != nil {
			break
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// QueryTraces fans out to all insert pod endpoints in parallel and returns
// the merged set of buffered trace rows within the given time range.
// Endpoint errors are silently ignored for graceful degradation.
func (b *BufferBridge) QueryTraces(ctx context.Context, startNs, endNs int64) ([]schema.TraceRow, error) {
	if !b.cfg.BufferQueryEnabled {
		return nil, nil
	}

	b.mu.RLock()
	eps := b.getQueryEndpoints()
	b.mu.RUnlock()

	if len(eps) == 0 {
		return nil, nil
	}

	var mu sync.Mutex
	var all []schema.TraceRow
	var wg sync.WaitGroup

	for _, ep := range eps {
		wg.Add(1)
		go func(endpoint string) {
			defer wg.Done()
			rows, err := b.fetchTraces(ctx, endpoint, startNs, endNs)
			if err != nil {
				return
			}
			mu.Lock()
			all = append(all, rows...)
			mu.Unlock()
		}(ep)
	}
	wg.Wait()

	return all, nil
}

func (b *BufferBridge) fetchTraces(ctx context.Context, endpoint string, startNs, endNs int64) ([]schema.TraceRow, error) {
	url := fmt.Sprintf("%s/internal/buffer/query?start=%d&end=%d&mode=%s",
		endpoint, startNs, endNs, string(b.mode))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("buffer query returned %d", resp.StatusCode)
	}

	var rows []schema.TraceRow
	dec := json.NewDecoder(resp.Body)
	for dec.More() {
		var row schema.TraceRow
		if err := dec.Decode(&row); err != nil {
			break
		}
		rows = append(rows, row)
	}
	return rows, nil
}
