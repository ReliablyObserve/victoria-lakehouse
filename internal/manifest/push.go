package manifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

type ManifestUpdate struct {
	Added   []FileInfo `json:"added,omitempty"`
	Removed []string   `json:"removed,omitempty"`
	Source  string     `json:"source"`
}

type PusherConfig struct {
	GetPeers   func() []string
	AuthSecret string
	SelfAddr   string
	Logger     *slog.Logger
}

type Pusher struct {
	cfg    PusherConfig
	client *http.Client
	logger *slog.Logger
}

func NewPusher(cfg PusherConfig) *Pusher {
	lg := cfg.Logger
	if lg == nil {
		lg = slog.Default()
	}
	return &Pusher{
		cfg:    cfg,
		client: &http.Client{Timeout: 2 * time.Second},
		logger: lg.With("component", "manifest.push"),
	}
}

func (p *Pusher) Notify(added []FileInfo, removed []string) {
	peers := p.cfg.GetPeers()
	if len(peers) == 0 {
		return
	}

	payload := ManifestUpdate{
		Added:   added,
		Removed: removed,
		Source:  p.cfg.SelfAddr,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		p.logger.Warn("failed to marshal manifest update", "error", err)
		return
	}

	var wg sync.WaitGroup
	for _, peer := range peers {
		if peer == p.cfg.SelfAddr {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			p.push(addr, data)
		}(peer)
	}
	wg.Wait()

	metrics.ManifestPushTotal.Inc()
	metrics.ManifestPushPeers.Set(int64(len(peers)))
}

func (p *Pusher) push(addr string, data []byte) {
	url := fmt.Sprintf("http://%s/internal/manifest/update", addr)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		p.logger.Warn("push request create failed", "peer", addr, "error", err)
		metrics.ManifestPushErrorsTotal.Inc()
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if p.cfg.AuthSecret != "" {
		req.Header.Set("Authorization", "Bearer "+p.cfg.AuthSecret)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		p.logger.Debug("push failed", "peer", addr, "error", err)
		metrics.ManifestPushErrorsTotal.Inc()
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		p.logger.Debug("push rejected", "peer", addr, "status", resp.StatusCode)
		metrics.ManifestPushErrorsTotal.Inc()
	}
}
