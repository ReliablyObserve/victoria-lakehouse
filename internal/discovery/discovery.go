package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type HotBoundary struct {
	MinDate string
	MaxDate string
	MinTime time.Time
	MaxTime time.Time
}

type Discovery struct {
	mu sync.RWMutex

	storageNodes []string
	hotBoundary  *HotBoundary
	peers        []string

	headlessService     string
	staticStorageNodes  []string
	partitionAuthKey    string
	peerHeadlessService string
	timeout             time.Duration
	logger              *slog.Logger
	httpClient          *http.Client

	lookupSRV  func(ctx context.Context, service, proto, name string) (string, []*net.SRV, error)
	lookupHost func(ctx context.Context, host string) ([]string, error)
}

type Option func(*Discovery)

func WithLookupSRV(fn func(context.Context, string, string, string) (string, []*net.SRV, error)) Option {
	return func(d *Discovery) { d.lookupSRV = fn }
}

func WithLookupHost(fn func(context.Context, string) ([]string, error)) Option {
	return func(d *Discovery) { d.lookupHost = fn }
}

func WithHTTPClient(c *http.Client) Option {
	return func(d *Discovery) { d.httpClient = c }
}

func New(
	headlessService string,
	staticStorageNodes []string,
	partitionAuthKey string,
	peerHeadlessService string,
	timeout time.Duration,
	logger *slog.Logger,
	opts ...Option,
) *Discovery {
	d := &Discovery{
		headlessService:     headlessService,
		staticStorageNodes:  staticStorageNodes,
		partitionAuthKey:    partitionAuthKey,
		peerHeadlessService: peerHeadlessService,
		timeout:             timeout,
		logger:              logger.With("component", "discovery"),
		lookupSRV:           net.DefaultResolver.LookupSRV,
		lookupHost:          net.DefaultResolver.LookupHost,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

func (d *Discovery) DiscoverStorageNodes(ctx context.Context) ([]string, error) {
	if len(d.staticStorageNodes) > 0 {
		d.mu.Lock()
		d.storageNodes = d.staticStorageNodes
		d.mu.Unlock()
		return d.staticStorageNodes, nil
	}

	if d.headlessService == "" {
		return nil, nil
	}

	nodes, err := d.resolveHeadlessService(ctx, d.headlessService)
	if err != nil {
		return nil, fmt.Errorf("discover storage nodes: %w", err)
	}

	sort.Strings(nodes)
	d.mu.Lock()
	d.storageNodes = nodes
	d.mu.Unlock()

	d.logger.Info("discovered storage nodes", "count", len(nodes), "nodes", nodes)
	return nodes, nil
}

func (d *Discovery) DiscoverPeers(ctx context.Context) ([]string, error) {
	if d.peerHeadlessService == "" {
		return nil, nil
	}

	peers, err := d.resolveHeadlessService(ctx, d.peerHeadlessService)
	if err != nil {
		return nil, fmt.Errorf("discover peers: %w", err)
	}

	sort.Strings(peers)
	d.mu.Lock()
	d.peers = peers
	d.mu.Unlock()

	d.logger.Info("discovered peers", "count", len(peers), "peers", peers)
	return peers, nil
}

func (d *Discovery) resolveHeadlessService(ctx context.Context, service string) ([]string, error) {
	host, port := splitHostPort(service)

	_, srvRecords, srvErr := d.lookupSRV(ctx, "", "", host)
	if srvErr == nil && len(srvRecords) > 0 {
		var addrs []string
		for _, srv := range srvRecords {
			target := strings.TrimSuffix(srv.Target, ".")
			srvPort := fmt.Sprintf("%d", srv.Port)
			if port != "" {
				srvPort = port
			}
			addrs = append(addrs, net.JoinHostPort(target, srvPort))
		}
		return addrs, nil
	}

	ips, err := d.lookupHost(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("lookup host %s: %w", host, err)
	}

	if port == "" {
		port = "9428"
	}
	var addrs []string
	for _, ip := range ips {
		ip = strings.TrimSuffix(ip, ".")
		addrs = append(addrs, net.JoinHostPort(ip, port))
	}
	return addrs, nil
}

func (d *Discovery) PollPartitionList(ctx context.Context) (*HotBoundary, error) {
	d.mu.RLock()
	nodes := d.storageNodes
	d.mu.RUnlock()

	if len(nodes) == 0 {
		return nil, nil
	}

	var allDates []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			dates, err := d.fetchPartitions(ctx, addr)
			if err != nil {
				d.logger.Warn("partition list poll failed", "node", addr, "error", err)
				return
			}
			mu.Lock()
			allDates = append(allDates, dates...)
			mu.Unlock()
		}(node)
	}
	wg.Wait()

	if len(allDates) == 0 {
		return nil, nil
	}

	sort.Strings(allDates)
	seen := make(map[string]bool)
	var unique []string
	for _, d := range allDates {
		if !seen[d] {
			seen[d] = true
			unique = append(unique, d)
		}
	}

	minDate := unique[0]
	maxDate := unique[len(unique)-1]

	minTime, _ := time.Parse("20060102", minDate)
	maxTime, _ := time.Parse("20060102", maxDate)
	maxTime = maxTime.Add(24 * time.Hour)

	boundary := &HotBoundary{
		MinDate: minDate,
		MaxDate: maxDate,
		MinTime: minTime,
		MaxTime: maxTime,
	}

	d.mu.Lock()
	d.hotBoundary = boundary
	d.mu.Unlock()

	d.logger.Info("hot boundary updated",
		"min_date", minDate,
		"max_date", maxDate,
		"partitions", len(unique),
	)

	return boundary, nil
}

func (d *Discovery) fetchPartitions(ctx context.Context, addr string) ([]string, error) {
	url := fmt.Sprintf("http://%s/internal/partition/list", addr)
	if d.partitionAuthKey != "" {
		url += "?authKey=" + d.partitionAuthKey
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	var dates []string
	if err := json.NewDecoder(resp.Body).Decode(&dates); err != nil {
		return nil, fmt.Errorf("decode partition list: %w", err)
	}

	return dates, nil
}

func (d *Discovery) GetHotBoundary() *HotBoundary {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.hotBoundary
}

func (d *Discovery) GetStorageNodes() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	cp := make([]string, len(d.storageNodes))
	copy(cp, d.storageNodes)
	return cp
}

func (d *Discovery) GetPeers() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	cp := make([]string, len(d.peers))
	copy(cp, d.peers)
	return cp
}

func (d *Discovery) SetHotBoundaryForTest(b *HotBoundary) {
	d.mu.Lock()
	d.hotBoundary = b
	d.mu.Unlock()
}

func splitHostPort(addr string) (string, string) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, ""
	}
	return host, port
}
