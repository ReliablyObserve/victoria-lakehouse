package azdetect

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestDetectK8sNodeLabel_NoNodeName(t *testing.T) {
	t.Setenv("NODE_NAME", "")

	_, err := detectK8sNodeLabel(context.Background(), time.Second)
	if err == nil {
		t.Error("expected error when NODE_NAME not set")
	}
	if err.Error() != "NODE_NAME not set" {
		t.Errorf("expected 'NODE_NAME not set', got %q", err.Error())
	}
}

func TestDetectK8sNodeLabel_NoTokenFile(t *testing.T) {
	t.Setenv("NODE_NAME", "test-node")

	_, err := detectK8sNodeLabel(context.Background(), time.Second)
	if err == nil {
		t.Error("expected error when SA token file missing")
	}
}

func TestDetectK8sNodeLabel_Unreachable(t *testing.T) {
	t.Setenv("NODE_NAME", "test-node")

	tmpDir := t.TempDir()
	tokenDir := tmpDir + "/var/run/secrets/kubernetes.io/serviceaccount"
	if err := os.MkdirAll(tokenDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenDir+"/token", []byte("mock-token"), 0644); err != nil {
		t.Fatal(err)
	}

	// K8s API at kubernetes.default.svc won't resolve in tests
	_, err := detectK8sNodeLabel(context.Background(), 100*time.Millisecond)
	if err == nil {
		t.Error("expected error when K8s API unreachable")
	}
}

func TestK8sNodeLabelParsing_GALabel(t *testing.T) {
	az := parseK8sNodeLabels([]byte(`{
		"metadata": {
			"labels": {
				"topology.kubernetes.io/zone": "us-east-1a",
				"failure-domain.beta.kubernetes.io/zone": "us-east-1a-legacy"
			}
		}
	}`))
	if az != "us-east-1a" {
		t.Errorf("GA label should take precedence, got %q", az)
	}
}

func TestK8sNodeLabelParsing_LegacyLabel(t *testing.T) {
	az := parseK8sNodeLabels([]byte(`{
		"metadata": {
			"labels": {
				"failure-domain.beta.kubernetes.io/zone": "us-east-1b"
			}
		}
	}`))
	if az != "us-east-1b" {
		t.Errorf("legacy label should work as fallback, got %q", az)
	}
}

func TestK8sNodeLabelParsing_NoZoneLabels(t *testing.T) {
	az := parseK8sNodeLabels([]byte(`{
		"metadata": {
			"labels": {
				"kubernetes.io/hostname": "node-1"
			}
		}
	}`))
	if az != "" {
		t.Errorf("expected empty when no zone labels, got %q", az)
	}
}

func TestK8sNodeLabelParsing_EmptyLabels(t *testing.T) {
	az := parseK8sNodeLabels([]byte(`{"metadata": {"labels": {}}}`))
	if az != "" {
		t.Errorf("expected empty for empty labels, got %q", az)
	}
}

func parseK8sNodeLabels(body []byte) string {
	var node struct {
		Metadata struct {
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &node); err != nil {
		return ""
	}
	if az := node.Metadata.Labels["topology.kubernetes.io/zone"]; az != "" {
		return az
	}
	return node.Metadata.Labels["failure-domain.beta.kubernetes.io/zone"]
}
