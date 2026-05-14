package azdetect

import (
	"context"
	"encoding/json"
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

	if opts.EnvVar != "" {
		if az := os.Getenv(opts.EnvVar); az != "" {
			logger.Infof("AZ detected from env %s: %s", opts.EnvVar, az)
			return az
		}
	}

	if az, err := detectAWSIMDS(ctx, awsIMDSBase, opts.Timeout); err == nil && az != "" {
		logger.Infof("AZ detected from AWS IMDS: %s", az)
		return az
	}

	if az, err := detectGCPMetadata(ctx, gcpMetaBase, opts.Timeout); err == nil && az != "" {
		logger.Infof("AZ detected from GCP metadata: %s", az)
		return az
	}

	if az, err := detectK8sNodeLabel(ctx, opts.Timeout); err == nil && az != "" {
		logger.Infof("AZ detected from K8s node label: %s", az)
		return az
	}

	logger.Infof("AZ not detected; AZ-aware routing will be disabled")
	return ""
}

func detectAWSIMDS(ctx context.Context, baseURL string, timeout time.Duration) (string, error) {
	client := &http.Client{Timeout: timeout}

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
	_ = tokenResp.Body.Close()

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
	defer func() { _ = azResp.Body.Close() }()

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
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GCP metadata status %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	parts := strings.Split(strings.TrimSpace(string(body)), "/")
	return parts[len(parts)-1], nil
}

func detectK8sNodeLabel(ctx context.Context, timeout time.Duration) (string, error) {
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		return "", fmt.Errorf("NODE_NAME not set")
	}

	tokenBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return "", fmt.Errorf("read SA token: %w", err)
	}

	client := &http.Client{Timeout: timeout}
	url := fmt.Sprintf("https://kubernetes.default.svc/api/v1/nodes/%s", nodeName) // #nosec G704 -- host is hardcoded, nodeName from K8s downward API
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+string(tokenBytes))

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("k8s API: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("k8s API status %d", resp.StatusCode)
	}

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
	return node.Metadata.Labels["failure-domain.beta.kubernetes.io/zone"], nil
}
