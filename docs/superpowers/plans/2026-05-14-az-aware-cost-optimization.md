# AZ-Aware Cost Optimization — Implementation Plan (v2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Victoria Lakehouse prefer same-AZ peers for peer cache and buffer bridge queries, eliminating cross-AZ data transfer costs ($0.01/GB each direction). Auto-detect AZ at startup. Support strict and preferred modes.

**Architecture:** Simple — a single `internal/azdetect` package detects the pod's AZ at startup via a fallback chain (env var → AWS IMDS → GCP metadata → K8s node label API). Each peer reports its AZ via a new field in `/internal/cache/stats`. The discovery loop collects peer AZs and passes them to the existing `ring.SetWithZones()` and `buffer_bridge.SetEndpointsWithZones()`. No new goroutines, no new protocols, no complex components.

**Tech Stack:** Go 1.24, net/http for IMDS/GCP metadata, k8s.io/client-go for node label lookup

---

## Completed

### Task 1: AZ Configuration ✅
Added `AZAware`, `CrossAZFallback`, `AZEnvVar` to `PeerConfig` and `AZAware`, `CrossAZFallback` to `SelectConfig` in `internal/config/config.go`. All 197 config tests pass.

### Task 2: AZ-Aware Consistent Hash Ring ✅
Added `SetWithZones()`, `LookupAZ()`, `MemberCountByZone()` to `internal/peercache/ring.go`. Same-AZ sub-ring with fallback to full ring. All 60 peercache tests pass.

---

## Revised Design: Strict vs Preferred AZ Modes

### Config

```yaml
lakehouse:
  peer:
    az_aware: true           # enable AZ-aware routing (default: true)
    az_mode: "preferred"     # "preferred" = same-AZ first, fallback to cross-AZ
                             # "strict" = same-AZ only, fail if no same-AZ peers
    az_env_var: "LAKEHOUSE_AZ"  # env var checked first
    az_min_peers_per_az: 2   # strict mode: require >= N same-AZ peers to start
    cross_az_fallback: true  # preferred mode: include cross-AZ in results
  select:
    az_aware: true
    cross_az_fallback: true
```

### AZ Detection Fallback Chain (at startup, once)

```
1. os.Getenv(cfg.Peer.AZEnvVar)       → "us-east-1a"  (operator override)
2. AWS IMDSv2 /meta-data/placement/az → "us-east-1a"  (EKS/EC2, no IAM needed)
3. GCP metadata /instance/zone         → "us-central1-a" (GKE/GCE)
4. K8s API: get node label             → "us-east-1a"  (any K8s, needs RBAC)
5. Give up → "" (AZ-aware routing disabled, log warning)
```

### Peer AZ Discovery (during discovery loop, not per-request)

Each peer already exposes `/internal/cache/stats`. We add `"az": "us-east-1a"` to that response. During the periodic discovery refresh, after DNS resolves peer IPs, we query each peer's `/internal/cache/stats` to get their AZ. This is done once per refresh interval (default 30s), not per cache lookup.

### Strict Mode Behavior

When `az_mode: "strict"`:
- At startup: verify `>= az_min_peers_per_az` same-AZ peers exist. If not, log error and fall back to preferred mode (don't block startup).
- During operation: `LookupAZ()` returns only same-AZ peers. If the only same-AZ peer is self, the lookup returns self (local cache hit).
- If all same-AZ peers go down: log alert, do NOT fall back to cross-AZ (that's the point of strict — user pays $0 cross-AZ).

### Preferred Mode Behavior (default)

When `az_mode: "preferred"`:
- Same-AZ peers are preferred but cross-AZ is used as fallback.
- No minimum peer requirement.
- `LookupAZ()` uses same-AZ sub-ring. If same-AZ ring is empty, uses full ring.

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/azdetect/detect.go` | AZ detection: env → IMDS → GCP → K8s API fallback chain |
| `internal/azdetect/detect_test.go` | Unit tests with mock HTTP servers |
| `internal/config/config.go` | Add `AZMode`, `AZMinPeersPerAZ` to PeerConfig (extend Task 1) |
| `internal/config/config_az_test.go` | Tests for new config fields |
| `internal/peercache/peercache.go` | Add `UpdatePeersWithZones()`, `LookupAZ()`, `StatsAZ()`, selfAZ field |
| `internal/peercache/peercache_az_test.go` | Tests for AZ-aware peer cache |
| `internal/peercache/peercache.go` Handler | Add `selfAZ` to Handler, expose in `/internal/cache/stats` response |
| `internal/storage/parquets3/buffer_bridge.go` | Add `SetEndpointsWithZones()`, `getQueryEndpoints()` |
| `internal/storage/parquets3/buffer_bridge_az_test.go` | Tests for AZ-aware buffer bridge |
| `internal/storage/parquets3/storage.go` | Wire AZ into `RefreshDiscovery()` |
| `internal/metrics/lakehouse.go` | 4 AZ metrics |
| `cmd/lakehouse-logs/main.go` | Call `azdetect.Detect()` at startup, pass to storage |
| `charts/victoria-lakehouse/values.yaml` | AZ config defaults, topology spread |
| `charts/victoria-lakehouse/templates/*.yaml` | NODE_NAME env var injection |
| `tests/e2e/az_test.go` | E2E test: verify AZ detection, peer AZ reporting, routing |
| `deployment/docker/docker-compose-e2e.yml` | Add LAKEHOUSE_AZ env vars to services |

---

### Task 3: AZ Auto-Detection Package

**Files:**
- Create: `internal/azdetect/detect.go`
- Create: `internal/azdetect/detect_test.go`

- [ ] **Step 1: Write the test**

Create `internal/azdetect/detect_test.go`:

```go
package azdetect

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestDetect_EnvVar(t *testing.T) {
	os.Setenv("TEST_AZ_VAR", "us-east-1a")
	defer os.Unsetenv("TEST_AZ_VAR")

	az := Detect(context.Background(), Options{EnvVar: "TEST_AZ_VAR"})
	if az != "us-east-1a" {
		t.Errorf("expected us-east-1a, got %q", az)
	}
}

func TestDetect_EnvVarEmpty_FallsThrough(t *testing.T) {
	os.Unsetenv("NONEXISTENT_AZ")

	// No IMDS/GCP/K8s available either, should return empty
	az := Detect(context.Background(), Options{
		EnvVar:  "NONEXISTENT_AZ",
		Timeout: 100 * time.Millisecond,
	})
	if az != "" {
		t.Errorf("expected empty, got %q", az)
	}
}

func TestDetect_AWSIMDS(t *testing.T) {
	// Mock IMDSv2 token + AZ endpoint
	tokenCalled := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest/api/token":
			if r.Method != http.MethodPut {
				http.Error(w, "method", 405)
				return
			}
			tokenCalled = true
			w.Write([]byte("mock-token"))
		case "/latest/meta-data/placement/availability-zone":
			if r.Header.Get("X-aws-ec2-metadata-token") != "mock-token" {
				http.Error(w, "unauthorized", 401)
				return
			}
			w.Write([]byte("us-west-2b"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	az, err := detectAWSIMDS(context.Background(), srv.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if az != "us-west-2b" {
		t.Errorf("expected us-west-2b, got %q", az)
	}
	if !tokenCalled {
		t.Error("IMDSv2 token endpoint was not called")
	}
}

func TestDetect_GCPMetadata(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			http.Error(w, "missing header", 400)
			return
		}
		w.Write([]byte("projects/123/zones/europe-west1-b"))
	}))
	defer srv.Close()

	az, err := detectGCPMetadata(context.Background(), srv.URL, 2*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if az != "europe-west1-b" {
		t.Errorf("expected europe-west1-b, got %q", az)
	}
}

func TestDetect_Timeout(t *testing.T) {
	// Server that never responds
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := detectAWSIMDS(ctx, srv.URL, 100*time.Millisecond)
	if err == nil {
		t.Error("expected timeout error")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/azdetect/ -v`
Expected: FAIL — package doesn't exist

- [ ] **Step 3: Implement azdetect package**

Create `internal/azdetect/detect.go`:

```go
package azdetect

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

const (
	awsIMDSBase = "http://169.254.169.254"
	gcpMetaBase = "http://metadata.google.internal"
)

type Options struct {
	EnvVar  string
	Timeout time.Duration
}

func Detect(ctx context.Context, opts Options) string {
	if opts.Timeout == 0 {
		opts.Timeout = 2 * time.Second
	}

	// 1. Explicit env var (fastest, always works)
	if opts.EnvVar != "" {
		if az := os.Getenv(opts.EnvVar); az != "" {
			logger.Infof("AZ detected from env %s: %s", opts.EnvVar, az)
			return az
		}
	}

	// 2. AWS IMDSv2
	if az, err := detectAWSIMDS(ctx, awsIMDSBase, opts.Timeout); err == nil && az != "" {
		logger.Infof("AZ detected from AWS IMDS: %s", az)
		return az
	}

	// 3. GCP metadata
	if az, err := detectGCPMetadata(ctx, gcpMetaBase, opts.Timeout); err == nil && az != "" {
		logger.Infof("AZ detected from GCP metadata: %s", az)
		return az
	}

	// 4. K8s node label (requires NODE_NAME env + RBAC)
	if az, err := detectK8sNodeLabel(ctx, opts.Timeout); err == nil && az != "" {
		logger.Infof("AZ detected from K8s node label: %s", az)
		return az
	}

	logger.Infof("AZ not detected; AZ-aware routing will be disabled")
	return ""
}

func detectAWSIMDS(ctx context.Context, baseURL string, timeout time.Duration) (string, error) {
	client := &http.Client{Timeout: timeout}

	// IMDSv2: get session token
	tokenReq, err := http.NewRequestWithContext(ctx, http.MethodPut,
		baseURL+"/latest/api/token", nil)
	if err != nil {
		return "", err
	}
	tokenReq.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "21600")

	tokenResp, err := client.Do(tokenReq)
	if err != nil {
		return "", fmt.Errorf("IMDS token: %w", err)
	}
	token, _ := io.ReadAll(tokenResp.Body)
	tokenResp.Body.Close()

	// Get AZ
	azReq, err := http.NewRequestWithContext(ctx, http.MethodGet,
		baseURL+"/latest/meta-data/placement/availability-zone", nil)
	if err != nil {
		return "", err
	}
	azReq.Header.Set("X-aws-ec2-metadata-token", string(token))

	azResp, err := client.Do(azReq)
	if err != nil {
		return "", fmt.Errorf("IMDS az: %w", err)
	}
	defer azResp.Body.Close()

	if azResp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("IMDS az status %d", azResp.StatusCode)
	}

	body, _ := io.ReadAll(azResp.Body)
	return strings.TrimSpace(string(body)), nil
}

func detectGCPMetadata(ctx context.Context, baseURL string, timeout time.Duration) (string, error) {
	client := &http.Client{Timeout: timeout}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		baseURL+"/computeMetadata/v1/instance/zone", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("GCP metadata: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GCP metadata status %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	// Response: "projects/123/zones/us-central1-a" → extract last segment
	parts := strings.Split(strings.TrimSpace(string(body)), "/")
	return parts[len(parts)-1], nil
}

func detectK8sNodeLabel(ctx context.Context, timeout time.Duration) (string, error) {
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		return "", fmt.Errorf("NODE_NAME not set")
	}

	// Use raw HTTP to avoid k8s client-go dependency weight.
	// In-cluster: service account token at known path, API at kubernetes.default.svc
	tokenBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return "", fmt.Errorf("read SA token: %w", err)
	}

	client := &http.Client{Timeout: timeout}
	url := fmt.Sprintf("https://kubernetes.default.svc/api/v1/nodes/%s", nodeName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+string(tokenBytes))

	// Skip TLS verify for in-cluster (CA cert would be at /var/run/secrets/kubernetes.io/serviceaccount/ca.crt)
	// In production, use proper TLS. For AZ detection this is acceptable.
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("k8s API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("k8s API status %d", resp.StatusCode)
	}

	// Parse just the labels from the node JSON
	var node struct {
		Metadata struct {
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&node); err != nil {
		return "", fmt.Errorf("decode node: %w", err)
	}

	if az := node.Metadata.Labels["topology.kubernetes.io/zone"]; az != "" {
		return az, nil
	}
	// Legacy label
	return node.Metadata.Labels["failure-domain.beta.kubernetes.io/zone"], nil
}
```

Add `"encoding/json"` to imports.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/azdetect/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/azdetect/
git commit -m "feat: add AZ auto-detection package with env/IMDS/GCP/K8s fallback chain"
```

---

### Task 4: Extend Config — AZ Mode and Min Peers

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_az_test.go`

- [ ] **Step 1: Add test for new fields**

Append to `internal/config/config_az_test.go`:

```go
func TestDefaultConfig_AZMode(t *testing.T) {
	cfg := Default()

	if cfg.Peer.AZMode != "preferred" {
		t.Errorf("AZMode should default to preferred, got %q", cfg.Peer.AZMode)
	}
	if cfg.Peer.AZMinPeersPerAZ != 2 {
		t.Errorf("AZMinPeersPerAZ should default to 2, got %d", cfg.Peer.AZMinPeersPerAZ)
	}
}

func TestValidate_AZMode(t *testing.T) {
	cfg := Default()
	cfg.Mode = "logs"
	cfg.S3.Bucket = "test"

	cfg.Peer.AZMode = "invalid"
	if err := cfg.Validate(); err == nil {
		t.Error("expected validation error for invalid AZMode")
	}

	cfg.Peer.AZMode = "strict"
	if err := cfg.Validate(); err != nil {
		t.Errorf("strict should be valid: %v", err)
	}

	cfg.Peer.AZMode = "preferred"
	if err := cfg.Validate(); err != nil {
		t.Errorf("preferred should be valid: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/config/ -run TestDefaultConfig_AZMode -v`

- [ ] **Step 3: Add AZMode and AZMinPeersPerAZ to PeerConfig**

In `internal/config/config.go`, add fields to `PeerConfig`:

```go
type PeerConfig struct {
	AuthKey          string        `yaml:"auth_key"`
	Timeout          time.Duration `yaml:"timeout"`
	MaxConnections   int           `yaml:"max_connections"`
	AZAware          bool          `yaml:"az_aware"`
	AZMode           string        `yaml:"az_mode"`
	CrossAZFallback  bool          `yaml:"cross_az_fallback"`
	AZEnvVar         string        `yaml:"az_env_var"`
	AZMinPeersPerAZ  int           `yaml:"az_min_peers_per_az"`
}
```

Update `Default()`:

```go
Peer: PeerConfig{
	Timeout:         5 * time.Second,
	MaxConnections:  32,
	AZAware:         true,
	AZMode:          "preferred",
	CrossAZFallback: true,
	AZEnvVar:        "LAKEHOUSE_AZ",
	AZMinPeersPerAZ: 2,
},
```

Add validation in `Validate()`:

```go
switch cfg.Peer.AZMode {
case "preferred", "strict", "":
default:
	return fmt.Errorf("--lakehouse.peer.az-mode must be preferred or strict, got %q", cfg.Peer.AZMode)
}
```

Add merge support in `mergeConfig()`:

```go
if overlay.Peer.AZMode != "" {
	base.Peer.AZMode = overlay.Peer.AZMode
}
if overlay.Peer.AZMinPeersPerAZ > 0 {
	base.Peer.AZMinPeersPerAZ = overlay.Peer.AZMinPeersPerAZ
}
```

- [ ] **Step 4: Run tests**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/config/ -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add internal/config/
git commit -m "feat: add AZ mode (strict/preferred) and min-peers-per-AZ config"
```

---

### Task 5: AZ-Aware PeerCache Client

**Files:**
- Modify: `internal/peercache/peercache.go`
- Create: `internal/peercache/peercache_az_test.go`

- [ ] **Step 1: Write the test**

Create `internal/peercache/peercache_az_test.go`:

```go
package peercache

import (
	"fmt"
	"testing"
	"time"
)

func TestPeerCache_UpdatePeersWithZones(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10)

	peerZones := map[string]string{
		"self:9428":   "az-a",
		"peer-a:9428": "az-a",
		"peer-b:9428": "az-b",
	}
	pc.UpdatePeersWithZones(peerZones, "az-a")

	if len(pc.Members()) != 3 {
		t.Errorf("expected 3 members, got %d", len(pc.Members()))
	}
	if pc.SelfAZ() != "az-a" {
		t.Errorf("expected selfAZ=az-a, got %q", pc.SelfAZ())
	}
}

func TestPeerCache_LookupAZ_RoutesSameZone(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10)

	peerZones := map[string]string{
		"self:9428":    "az-a",
		"peer-a:9428":  "az-a",
		"peer-b1:9428": "az-b",
		"peer-b2:9428": "az-b",
	}
	pc.UpdatePeersWithZones(peerZones, "az-a")

	crossAZ := 0
	for i := 0; i < 500; i++ {
		_, _, isSameAZ := pc.LookupAZ(fmt.Sprintf("file-%d.parquet", i))
		if !isSameAZ {
			crossAZ++
		}
	}

	if crossAZ > 0 {
		t.Errorf("expected 0 cross-AZ lookups, got %d", crossAZ)
	}
}

func TestPeerCache_StatsAZ(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10)

	stats := pc.StatsAZ()
	if stats.SelfAZ != "" {
		t.Errorf("expected empty selfAZ before zone config, got %q", stats.SelfAZ)
	}

	peerZones := map[string]string{
		"self:9428":  "az-a",
		"peer:9428":  "az-b",
	}
	pc.UpdatePeersWithZones(peerZones, "az-a")

	stats = pc.StatsAZ()
	if stats.SelfAZ != "az-a" {
		t.Errorf("expected selfAZ=az-a, got %q", stats.SelfAZ)
	}
	if stats.SameAZMembers != 1 {
		t.Errorf("expected 1 same-AZ member, got %d", stats.SameAZMembers)
	}
	if stats.CrossAZMembers != 1 {
		t.Errorf("expected 1 cross-AZ member, got %d", stats.CrossAZMembers)
	}
}
```

- [ ] **Step 2: Run test — expect fail**
- [ ] **Step 3: Implement**

Add `selfAZ string` field to PeerCache struct. Add methods:

```go
func (pc *PeerCache) UpdatePeersWithZones(peerZones map[string]string, selfAZ string) {
	pc.selfAZ = selfAZ
	old := pc.ring.MemberCount()
	pc.ring.SetWithZones(peerZones, selfAZ)
	if pc.ring.MemberCount() != old {
		logger.Infof("peer ring updated with zones; members=%d, selfAZ=%s", pc.ring.MemberCount(), selfAZ)
	}
}

func (pc *PeerCache) LookupAZ(key string) (peer string, isLocal bool, isSameAZ bool) {
	return pc.ring.LookupAZ(key)
}

func (pc *PeerCache) SelfAZ() string { return pc.selfAZ }

type StatsAZ struct {
	Stats
	SelfAZ         string
	SameAZMembers  int
	CrossAZMembers int
}

func (pc *PeerCache) StatsAZ() StatsAZ {
	s := pc.Stats()
	sameAZ, crossAZ := pc.ring.MemberCountByZone()
	return StatsAZ{
		Stats:          s,
		SelfAZ:         pc.selfAZ,
		SameAZMembers:  sameAZ,
		CrossAZMembers: crossAZ,
	}
}
```

Add `selfAZ` field to Handler, expose in `/internal/cache/stats`:

```go
type Handler struct {
	mu      sync.RWMutex
	cache   map[string][]byte
	authKey string
	selfAZ  string
}

func NewHandler(authKey, selfAZ string) *Handler {
	return &Handler{
		cache:   make(map[string][]byte),
		authKey: authKey,
		selfAZ:  selfAZ,
	}
}
```

Add `/internal/cache/stats` case to `ServeHTTP`:

```go
case "/internal/cache/stats":
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"az":%q}`, h.selfAZ)
```

- [ ] **Step 4: Run tests — expect pass**
- [ ] **Step 5: Run full peercache suite**
- [ ] **Step 6: Commit**

```bash
git add internal/peercache/
git commit -m "feat: add AZ-aware PeerCache with zone stats and AZ reporting endpoint"
```

---

### Task 6: AZ-Aware Buffer Bridge

**Files:**
- Modify: `internal/storage/parquets3/buffer_bridge.go`
- Create: `internal/storage/parquets3/buffer_bridge_az_test.go`

- [ ] **Step 1: Write the test**

Create `internal/storage/parquets3/buffer_bridge_az_test.go`:

```go
package parquets3

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func TestBufferBridge_SetEndpointsWithZones(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            true,
		CrossAZFallback:    true,
	}
	bb := NewBufferBridge(cfg, config.ModeLogs)

	epZones := map[string]string{
		"http://insert-0:9428": "az-a",
		"http://insert-1:9428": "az-a",
		"http://insert-2:9428": "az-b",
	}
	bb.SetEndpointsWithZones(epZones, "az-a")

	bb.mu.RLock()
	defer bb.mu.RUnlock()

	if len(bb.endpoints) != 3 {
		t.Errorf("expected 3 total endpoints, got %d", len(bb.endpoints))
	}
	if len(bb.sameAZEndpoints) != 2 {
		t.Errorf("expected 2 same-AZ endpoints, got %d", len(bb.sameAZEndpoints))
	}
	if len(bb.crossAZEndpoints) != 1 {
		t.Errorf("expected 1 cross-AZ endpoint, got %d", len(bb.crossAZEndpoints))
	}
}

func TestBufferBridge_StrictMode_SameAZOnly(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            true,
		CrossAZFallback:    false,
	}
	bb := NewBufferBridge(cfg, config.ModeLogs)

	epZones := map[string]string{
		"http://insert-0:9428": "az-a",
		"http://insert-1:9428": "az-b",
	}
	bb.SetEndpointsWithZones(epZones, "az-a")

	bb.mu.RLock()
	eps := bb.getQueryEndpoints()
	bb.mu.RUnlock()

	if len(eps) != 1 {
		t.Errorf("strict: expected 1 endpoint (same-AZ only), got %d", len(eps))
	}
	if len(eps) > 0 && eps[0] != "http://insert-0:9428" {
		t.Errorf("expected same-AZ endpoint, got %q", eps[0])
	}
}

func TestBufferBridge_PreferredMode_AllEndpoints(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            true,
		CrossAZFallback:    true,
	}
	bb := NewBufferBridge(cfg, config.ModeLogs)

	epZones := map[string]string{
		"http://insert-0:9428": "az-a",
		"http://insert-1:9428": "az-b",
	}
	bb.SetEndpointsWithZones(epZones, "az-a")

	bb.mu.RLock()
	eps := bb.getQueryEndpoints()
	bb.mu.RUnlock()

	if len(eps) != 2 {
		t.Errorf("preferred: expected 2 endpoints, got %d", len(eps))
	}
}

func TestBufferBridge_NoAZ_AllEndpoints(t *testing.T) {
	cfg := &config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
		AZAware:            false,
	}
	bb := NewBufferBridge(cfg, config.ModeLogs)

	bb.SetEndpoints([]string{"http://a:9428", "http://b:9428"})

	bb.mu.RLock()
	eps := bb.getQueryEndpoints()
	bb.mu.RUnlock()

	if len(eps) != 2 {
		t.Errorf("no AZ: expected 2 endpoints, got %d", len(eps))
	}
}
```

- [ ] **Step 2: Run test — expect fail**
- [ ] **Step 3: Implement**

Add fields to BufferBridge struct:

```go
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
```

Add methods:

```go
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

func (b *BufferBridge) getQueryEndpoints() []string {
	if !b.cfg.AZAware || b.selfAZ == "" {
		return b.endpoints
	}
	if !b.cfg.CrossAZFallback && len(b.sameAZEndpoints) > 0 {
		return b.sameAZEndpoints
	}
	return b.endpoints
}
```

Update `QueryLogs` and `QueryTraces` to use `b.getQueryEndpoints()` instead of `b.endpoints`.

- [ ] **Step 4: Run tests — expect pass**
- [ ] **Step 5: Run full storage test suite**
- [ ] **Step 6: Commit**

```bash
git add internal/storage/parquets3/buffer_bridge.go internal/storage/parquets3/buffer_bridge_az_test.go
git commit -m "feat: add AZ-aware buffer bridge with same-AZ endpoint preference"
```

---

### Task 7: AZ Metrics

**Files:**
- Modify: `internal/metrics/lakehouse.go`

- [ ] **Step 1: Add AZ metrics**

```go
// AZ-aware routing metrics
var (
	PeerSameAZMembers     = NewGauge("lakehouse_peer_same_az_members")
	PeerCrossAZMembers    = NewGauge("lakehouse_peer_cross_az_members")
	PeerAZRequestsTotal   = NewCounterVec("lakehouse_peer_az_requests_total", "az_type")
	BufferBridgeAZRequestsTotal = NewCounterVec("lakehouse_buffer_bridge_az_requests_total", "az_type")
)
```

Label values: `az_type="same"` or `az_type="cross"`.

- [ ] **Step 2: Build to verify compilation**
- [ ] **Step 3: Commit**

```bash
git add internal/metrics/lakehouse.go
git commit -m "feat: add AZ routing metrics"
```

---

### Task 8: Wire AZ Into Startup and Discovery

**Files:**
- Modify: `cmd/lakehouse-logs/main.go`
- Modify: `internal/storage/parquets3/storage.go`

This is the wiring task — connects azdetect, peercache, and buffer bridge.

- [ ] **Step 1: Add AZ detection at startup in main.go**

After config load and before `parquets3.New(cfg)`:

```go
selfAZ := azdetect.Detect(context.Background(), azdetect.Options{
	EnvVar:  cfg.Peer.AZEnvVar,
	Timeout: 2 * time.Second,
})
```

Pass `selfAZ` to storage constructor (add parameter or set on config).

- [ ] **Step 2: Update NewHandler call to include selfAZ**

Change `peercache.NewHandler(cfg.Peer.AuthKey)` to `peercache.NewHandler(cfg.Peer.AuthKey, selfAZ)`.

- [ ] **Step 3: Add `/internal/cache/stats` AZ field to main.go handler**

The existing `/internal/cache/stats` handler at line 510 needs to include the AZ:

```go
_ = json.NewEncoder(w).Encode(map[string]any{
	"l1_entries":   stats.Entries,
	"l1_size":      stats.Size,
	"l1_max_size":  stats.MaxSize,
	"l1_hits":      stats.Hits,
	"l1_misses":    stats.Misses,
	"l1_evictions": stats.Evictions,
	"az":           selfAZ,
})
```

- [ ] **Step 4: Wire AZ into RefreshDiscovery**

In `internal/storage/parquets3/storage.go`, modify `RefreshDiscovery()`:

```go
func (s *Storage) RefreshDiscovery(ctx context.Context) error {
	if _, err := s.discovery.DiscoverStorageNodes(ctx); err != nil {
		return fmt.Errorf("discover storage nodes: %w", err)
	}
	if _, err := s.discovery.PollPartitionList(ctx); err != nil {
		return fmt.Errorf("poll partition list: %w", err)
	}
	if s.peerCache != nil {
		peers, err := s.discovery.DiscoverPeers(ctx)
		if err != nil {
			return fmt.Errorf("discover peers: %w", err)
		}

		if s.selfAZ != "" && s.cfg.Peer.AZAware {
			peerZones := s.queryPeerAZs(ctx, peers)
			s.peerCache.UpdatePeersWithZones(peerZones, s.selfAZ)

			// Update metrics
			stats := s.peerCache.StatsAZ()
			metrics.PeerSameAZMembers.Set(int64(stats.SameAZMembers))
			metrics.PeerCrossAZMembers.Set(int64(stats.CrossAZMembers))

			// Strict mode validation
			if s.cfg.Peer.AZMode == "strict" && stats.SameAZMembers < s.cfg.Peer.AZMinPeersPerAZ {
				logger.Warnf("strict AZ mode: only %d same-AZ peers (need %d); falling back to preferred",
					stats.SameAZMembers, s.cfg.Peer.AZMinPeersPerAZ)
			}
		} else {
			s.peerCache.UpdatePeers(peers)
		}
	}
	return nil
}
```

Add `queryPeerAZs()` helper to storage.go:

```go
func (s *Storage) queryPeerAZs(ctx context.Context, peers []string) map[string]string {
	peerZones := make(map[string]string, len(peers))
	for _, peer := range peers {
		az := s.fetchPeerAZ(ctx, peer)
		peerZones[peer] = az
	}
	return peerZones
}

func (s *Storage) fetchPeerAZ(ctx context.Context, peer string) string {
	url := fmt.Sprintf("http://%s/internal/cache/stats", peer)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ""
	}
	if s.cfg.Peer.AuthKey != "" {
		req.Header.Set("Authorization", "Bearer "+s.cfg.Peer.AuthKey)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	var result struct {
		AZ string `json:"az"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return ""
	}
	return result.AZ
}
```

Add `selfAZ string` field to Storage struct, set during construction.

- [ ] **Step 5: Build to verify compilation**

Run: `cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go build ./cmd/lakehouse-logs/`

- [ ] **Step 6: Commit**

```bash
git add cmd/lakehouse-logs/main.go internal/storage/parquets3/storage.go
git commit -m "feat: wire AZ detection and zone-aware peer discovery at startup"
```

---

### Task 9: Helm Chart Defaults

**Files:**
- Modify: `charts/victoria-lakehouse/values.yaml`
- Modify: `charts/victoria-lakehouse/templates/select-statefulset.yaml`
- Modify: `charts/victoria-lakehouse/templates/insert-statefulset.yaml`

- [ ] **Step 1: Add AZ config to values.yaml**

```yaml
lakehouseConfig:
  peer:
    az_aware: true
    az_mode: "preferred"
    cross_az_fallback: true
    az_env_var: "LAKEHOUSE_AZ"
    az_min_peers_per_az: 2
  select:
    az_aware: true
    cross_az_fallback: true
```

- [ ] **Step 2: Add NODE_NAME env var to statefulset templates**

In both select and insert StatefulSet templates, add to container env:

```yaml
- name: NODE_NAME
  valueFrom:
    fieldRef:
      fieldPath: spec.nodeName
```

This enables the K8s API fallback for AZ detection (reads node's topology label).

- [ ] **Step 3: Add default topology spread constraints**

Change default `topologySpreadConstraints: []` to:

```yaml
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels:
        app.kubernetes.io/component: select  # or insert for insert template
```

- [ ] **Step 4: Lint Helm chart**

Run: `helm lint charts/victoria-lakehouse/`

- [ ] **Step 5: Commit**

```bash
git add charts/victoria-lakehouse/
git commit -m "feat: add AZ-aware Helm defaults — topology spread, NODE_NAME injection, AZ config"
```

---

### Task 10: Traces Module — Verify Shared Code

**Files:** None to create (verify only)

The traces module shares `internal/config`, `internal/peercache`, `internal/metrics`, and `internal/storage/parquets3` via Go workspace replace directive. Changes propagate automatically.

- [ ] **Step 1: Verify traces module imports shared code**

```bash
grep -r "victoria-lakehouse/internal/peercache" lakehouse-traces/
grep -r "victoria-lakehouse/internal/config" lakehouse-traces/
```

- [ ] **Step 2: Check traces main.go for parallel wiring needs**

Read `lakehouse-traces/cmd/lakehouse-traces/main.go` (or `lakehouse-traces/main.go`) to find `NewHandler(authKey)` calls that need the `selfAZ` parameter added.

- [ ] **Step 3: Update traces main.go**

Mirror the same azdetect + NewHandler changes from Task 8.

- [ ] **Step 4: Build traces binary**

Run: `cd /private/tmp/victoria-lakehouse-fresh/lakehouse-traces && GOWORK=off go build ./...`

- [ ] **Step 5: Run traces tests**

Run: `cd /private/tmp/victoria-lakehouse-fresh/lakehouse-traces && GOWORK=off go test ./... -v -count=1 -timeout 120s`

- [ ] **Step 6: Commit**

```bash
git add lakehouse-traces/
git commit -m "feat: wire AZ detection into traces module"
```

---

### Task 11: Integration Tests — Startup Verification

**Files:**
- Create: `internal/peercache/integration_az_test.go`
- Create: `internal/azdetect/integration_test.go`

- [ ] **Step 1: Peer cache AZ integration test**

Test full flow: create handlers in different AZs → create peer cache → verify routing + stats:

```go
package peercache

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAZIntegration_FullFlow(t *testing.T) {
	// Two peers in different AZs
	handlerA := NewHandler("", "az-a")
	handlerA.Put("shared-key", []byte("data-from-az-a"))

	handlerB := NewHandler("", "az-b")
	handlerB.Put("shared-key", []byte("data-from-az-b"))

	serverA := httptest.NewServer(handlerA)
	defer serverA.Close()
	serverB := httptest.NewServer(handlerB)
	defer serverB.Close()

	// Peer cache for pod in az-a
	pc := New("self:9428", "", 5*time.Second, 10)

	peerZones := map[string]string{
		serverA.Listener.Addr().String(): "az-a",
		serverB.Listener.Addr().String(): "az-b",
		"self:9428":                       "az-a",
	}
	pc.UpdatePeersWithZones(peerZones, "az-a")

	// Verify zone stats
	stats := pc.StatsAZ()
	if stats.SameAZMembers != 2 {
		t.Errorf("expected 2 same-AZ, got %d", stats.SameAZMembers)
	}
	if stats.CrossAZMembers != 1 {
		t.Errorf("expected 1 cross-AZ, got %d", stats.CrossAZMembers)
	}

	// Verify all lookups route same-AZ
	crossAZ := 0
	for i := 0; i < 200; i++ {
		_, _, isSameAZ := pc.LookupAZ(fmt.Sprintf("key-%d", i))
		if !isSameAZ {
			crossAZ++
		}
	}
	if crossAZ > 0 {
		t.Errorf("expected 0 cross-AZ lookups, got %d/200", crossAZ)
	}
}

func TestAZIntegration_StatsEndpoint(t *testing.T) {
	handler := NewHandler("", "us-east-1a")
	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/internal/cache/stats")
	if err != nil {
		t.Fatalf("stats request failed: %v", err)
	}
	defer resp.Body.Close()

	var result struct {
		AZ string `json:"az"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if result.AZ != "us-east-1a" {
		t.Errorf("expected az=us-east-1a, got %q", result.AZ)
	}
}

func TestAZIntegration_PeerAZDiscovery(t *testing.T) {
	// Simulate what RefreshDiscovery does: query /internal/cache/stats for AZ
	handler1 := NewHandler("", "az-a")
	handler2 := NewHandler("", "az-b")
	srv1 := httptest.NewServer(handler1)
	defer srv1.Close()
	srv2 := httptest.NewServer(handler2)
	defer srv2.Close()

	peers := []string{srv1.Listener.Addr().String(), srv2.Listener.Addr().String()}
	peerZones := make(map[string]string)

	for _, peer := range peers {
		resp, err := http.Get(fmt.Sprintf("http://%s/internal/cache/stats", peer))
		if err != nil {
			t.Fatalf("query peer %s: %v", peer, err)
		}
		var result struct {
			AZ string `json:"az"`
		}
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		peerZones[peer] = result.AZ
	}

	if len(peerZones) != 2 {
		t.Fatalf("expected 2 peer zones, got %d", len(peerZones))
	}
	if peerZones[peers[0]] != "az-a" {
		t.Errorf("peer 0 should be az-a, got %q", peerZones[peers[0]])
	}
	if peerZones[peers[1]] != "az-b" {
		t.Errorf("peer 1 should be az-b, got %q", peerZones[peers[1]])
	}
}
```

- [ ] **Step 2: AZ detect integration test**

```go
package azdetect

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestDetect_FullChain_EnvWins(t *testing.T) {
	// Even if IMDS/GCP would work, env var wins
	os.Setenv("MY_AZ", "override-zone")
	defer os.Unsetenv("MY_AZ")

	imds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("imds-zone"))
	}))
	defer imds.Close()

	az := Detect(context.Background(), Options{EnvVar: "MY_AZ", Timeout: time.Second})
	if az != "override-zone" {
		t.Errorf("env var should win, got %q", az)
	}
}

func TestDetect_AllFail_ReturnsEmpty(t *testing.T) {
	os.Unsetenv("NONEXISTENT")

	az := Detect(context.Background(), Options{
		EnvVar:  "NONEXISTENT",
		Timeout: 100 * time.Millisecond,
	})
	if az != "" {
		t.Errorf("expected empty when all methods fail, got %q", az)
	}
}
```

- [ ] **Step 3: Run all integration tests**

```bash
cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/peercache/ -run TestAZIntegration -v
cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/azdetect/ -v
```

- [ ] **Step 4: Commit**

```bash
git add internal/peercache/integration_az_test.go internal/azdetect/
git commit -m "test: add AZ integration tests for peer discovery, routing, and stats endpoint"
```

---

### Task 12: E2E Test — Real Setup

**Files:**
- Create: `tests/e2e/az_test.go`
- Modify: `deployment/docker/docker-compose-e2e.yml`

- [ ] **Step 1: Add LAKEHOUSE_AZ env vars to docker-compose**

In `docker-compose-e2e.yml`, add to both lakehouse-logs and lakehouse-traces services:

```yaml
environment:
  LAKEHOUSE_AZ: "az-a"
```

This simulates a single-AZ deployment for E2E testing.

- [ ] **Step 2: Write E2E test**

Create `tests/e2e/az_test.go`:

```go
//go:build e2e

package e2e

import (
	"encoding/json"
	"net/url"
	"testing"
)

func TestAZ_CacheStatsIncludesAZ(t *testing.T) {
	body := httpGetBody(t, logsBaseURL, "/internal/cache/stats", nil)

	var stats map[string]any
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatalf("decode cache stats: %v", err)
	}

	az, ok := stats["az"]
	if !ok {
		t.Fatal("cache stats should include 'az' field")
	}

	azStr, ok := az.(string)
	if !ok {
		t.Fatalf("az field should be string, got %T", az)
	}
	if azStr != "az-a" {
		t.Errorf("expected AZ=az-a (from LAKEHOUSE_AZ env), got %q", azStr)
	}
}

func TestAZ_HealthAndReadyWork(t *testing.T) {
	// Verify startup succeeded with AZ detection
	_ = httpGetBody(t, logsBaseURL, "/health", nil)
	_ = httpGetBody(t, logsBaseURL, "/ready", nil)
}

func TestAZ_QueriesStillWorkWithAZ(t *testing.T) {
	// Verify AZ-aware routing doesn't break normal queries
	params := url.Values{
		"query": {"*"},
		"start": {nsToISO(dataMinTime)},
		"end":   {nsToISO(dataMaxTime)},
		"limit": {"5"},
	}
	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	if len(body) == 0 {
		t.Error("query should return data even with AZ-aware routing")
	}
}
```

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/az_test.go deployment/docker/docker-compose-e2e.yml
git commit -m "test: add E2E tests for AZ-aware routing on real docker-compose setup"
```

---

### Task 13: Documentation and CHANGELOG

**Files:**
- Modify: `docs/cross-az-optimization.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Update cross-az-optimization.md**

Add implementation status section at top:

```markdown
:::info Implementation Status
AZ-aware routing is **enabled by default**. AZ is auto-detected at startup via:
1. `LAKEHOUSE_AZ` env var (operator override)
2. AWS IMDSv2 (EKS/EC2)
3. GCP metadata (GKE/GCE)
4. Kubernetes node label API (any K8s, needs node `get` RBAC)

Two modes: `preferred` (default, cross-AZ fallback) and `strict` (same-AZ only, requires `az_min_peers_per_az` same-AZ peers).
:::
```

- [ ] **Step 2: Update CHANGELOG.md**

```markdown
### Added
- AZ auto-detection at startup (env var → AWS IMDS → GCP metadata → K8s node label)
- AZ-aware peer cache routing — same-AZ peers preferred by default
- AZ-aware buffer bridge — select pods prefer same-AZ insert pods
- Strict vs preferred AZ modes with configurable min-peers-per-AZ
- AZ metrics: `lakehouse_peer_same_az_members`, `lakehouse_peer_cross_az_members`, `lakehouse_peer_az_requests_total`, `lakehouse_buffer_bridge_az_requests_total`
- Peer AZ reporting via `/internal/cache/stats` endpoint
- Default topology spread constraints in Helm chart
- E2E tests for AZ-aware routing
```

- [ ] **Step 3: Commit**

```bash
git add docs/cross-az-optimization.md CHANGELOG.md
git commit -m "docs: add AZ-aware routing implementation status and CHANGELOG"
```

---

### Task 14: Final Verification

- [ ] **Step 1: Build both binaries**

```bash
cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go build ./cmd/lakehouse-logs/
cd /private/tmp/victoria-lakehouse-fresh/lakehouse-traces && GOWORK=off go build ./...
```

- [ ] **Step 2: Run full test suites**

```bash
cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./... -count=1 -timeout 120s
cd /private/tmp/victoria-lakehouse-fresh/lakehouse-traces && GOWORK=off go test ./... -count=1 -timeout 120s
```

- [ ] **Step 3: Race detector on peercache**

```bash
cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go test ./internal/peercache/ -race -count=1
```

- [ ] **Step 4: Go vet**

```bash
cd /private/tmp/victoria-lakehouse-fresh && GOWORK=off go vet ./...
```

- [ ] **Step 5: Helm lint**

```bash
helm lint charts/victoria-lakehouse/
```
