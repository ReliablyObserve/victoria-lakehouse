package crosssignal

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// PrefetchHint is sent to a peer to request prefetching of trace data.
type PrefetchHint struct {
	TraceIDs     []string `json:"trace_ids"`
	StartNs      int64    `json:"start_ns"`
	EndNs        int64    `json:"end_ns"`
	SourceSignal string   `json:"source_signal"`
}

// EvictionHint is sent to a peer to suggest evicting cached trace data.
type EvictionHint struct {
	TraceIDs     []string `json:"trace_ids"`
	SourceSignal string   `json:"source_signal"`
}

// ClientConfig configures the cross-signal Client.
type ClientConfig struct {
	Endpoint      string
	AuthKey       string
	Timeout       time.Duration
	MaxBatch      int
	BatchInterval time.Duration
}

// Client batches and sends cross-signal hints to a peer endpoint.
type Client struct {
	endpoint   string
	authKey    string
	httpClient *http.Client
	maxBatch   int

	mu            sync.Mutex
	pendingIDs    []string
	pendingStart  int64
	pendingEnd    int64
	pendingSignal string

	closed chan struct{}
	wg     sync.WaitGroup
	sendWg sync.WaitGroup
}

// NewClient creates a new cross-signal Client. If cfg.Endpoint is empty,
// all operations become no-ops.
func NewClient(cfg ClientConfig) *Client {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 100
	}
	if cfg.BatchInterval <= 0 {
		cfg.BatchInterval = 500 * time.Millisecond
	}

	c := &Client{
		endpoint: cfg.Endpoint,
		authKey:  cfg.AuthKey,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
		maxBatch: cfg.MaxBatch,
		closed:   make(chan struct{}),
	}

	if cfg.Endpoint != "" {
		c.wg.Add(1)
		go c.batchLoop(cfg.BatchInterval)
	}

	return c
}

// EnqueueHint adds trace IDs to the pending batch. When the batch reaches
// MaxBatch or the BatchInterval fires, the batch is flushed as a PrefetchHint.
// Empty traceIDs are silently ignored.
func (c *Client) EnqueueHint(traceIDs []string, startNs, endNs int64, sourceSignal string) {
	if len(traceIDs) == 0 || c.endpoint == "" {
		return
	}

	c.mu.Lock()
	c.pendingIDs = append(c.pendingIDs, traceIDs...)
	c.pendingStart = startNs
	c.pendingEnd = endNs
	c.pendingSignal = sourceSignal

	var hints []PrefetchHint
	if len(c.pendingIDs) >= c.maxBatch {
		hints = c.drainLocked()
	}
	c.mu.Unlock()

	// Send outside the lock in a single goroutine to preserve ordering.
	if len(hints) > 0 {
		c.sendWg.Add(1)
		go func() {
			defer c.sendWg.Done()
			for _, h := range hints {
				c.sendPrefetchHint(h)
			}
		}()
	}
}

// SendEvictionHint sends an eviction hint synchronously.
// Empty traceIDs are silently ignored.
func (c *Client) SendEvictionHint(traceIDs []string, sourceSignal string) {
	if len(traceIDs) == 0 || c.endpoint == "" {
		return
	}

	hint := EvictionHint{
		TraceIDs:     traceIDs,
		SourceSignal: sourceSignal,
	}
	c.sendEvictionHint(hint)
}

// Close flushes any pending hints and stops the batch loop.
func (c *Client) Close() {
	if c.endpoint == "" {
		return
	}

	// Drain pending hints.
	c.mu.Lock()
	hints := c.drainLocked()
	c.mu.Unlock()

	// Send remaining hints synchronously.
	for _, h := range hints {
		c.sendPrefetchHint(h)
	}

	// Wait for any in-flight async sends to complete.
	c.sendWg.Wait()

	// Stop the batch loop.
	close(c.closed)
	c.wg.Wait()
}

// batchLoop periodically flushes the pending batch.
func (c *Client) batchLoop(interval time.Duration) {
	defer c.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			hints := c.drainLocked()
			c.mu.Unlock()

			for _, h := range hints {
				c.sendPrefetchHint(h)
			}
		case <-c.closed:
			return
		}
	}
}

// drainLocked extracts all pending trace IDs as PrefetchHint batches.
// Caller must hold c.mu.
func (c *Client) drainLocked() []PrefetchHint {
	if len(c.pendingIDs) == 0 {
		return nil
	}

	var hints []PrefetchHint
	for len(c.pendingIDs) > 0 {
		end := c.maxBatch
		if end > len(c.pendingIDs) {
			end = len(c.pendingIDs)
		}
		batch := make([]string, end)
		copy(batch, c.pendingIDs[:end])
		c.pendingIDs = c.pendingIDs[end:]

		hints = append(hints, PrefetchHint{
			TraceIDs:     batch,
			StartNs:      c.pendingStart,
			EndNs:        c.pendingEnd,
			SourceSignal: c.pendingSignal,
		})
	}

	c.pendingIDs = nil
	return hints
}

// sendPrefetchHint POSTs a PrefetchHint to the peer endpoint.
func (c *Client) sendPrefetchHint(hint PrefetchHint) {
	body, err := json.Marshal(hint)
	if err != nil {
		logger.Errorf("crosssignal: failed to marshal prefetch hint: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.httpClient.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/internal/prefetch/hint", bytes.NewReader(body))
	if err != nil {
		logger.Errorf("crosssignal: failed to create prefetch request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if c.authKey != "" {
		req.Header.Set("X-Cross-Signal-Key", c.authKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logger.Errorf("crosssignal: failed to send prefetch hint: %v", err)
		return
	}
	_ = resp.Body.Close()

	metrics.CrossPrefetchSent.Inc()
}

// sendEvictionHint POSTs an EvictionHint to the peer endpoint.
func (c *Client) sendEvictionHint(hint EvictionHint) {
	body, err := json.Marshal(hint)
	if err != nil {
		logger.Errorf("crosssignal: failed to marshal eviction hint: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.httpClient.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/internal/cache/evict-hint", bytes.NewReader(body))
	if err != nil {
		logger.Errorf("crosssignal: failed to create eviction request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if c.authKey != "" {
		req.Header.Set("X-Cross-Signal-Key", c.authKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logger.Errorf("crosssignal: failed to send eviction hint: %v", err)
		return
	}
	_ = resp.Body.Close()

	metrics.CrossEvictionSent.Inc()
}
