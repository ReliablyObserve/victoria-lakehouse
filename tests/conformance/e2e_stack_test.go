package conformance

import (
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
	"gopkg.in/yaml.v3"
)

// Host-side tests must reach the actual proxy/select services, rather than
// silently succeeding after readiness loops exhaust against unpublished ports.
func TestE2EHostEndpointsPublished(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Ports []string `yaml:"ports"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(readRepoFile(t, root, "deployment/docker/docker-compose-e2e.yml")), &compose); err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct {
				Env map[string]string `yaml:"env"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(readRepoFile(t, root, ".github/workflows/e2e.yaml")), &workflow); err != nil {
		t.Fatal(err)
	}
	var env map[string]string
	for _, step := range workflow.Jobs["e2e"].Steps {
		if step.Env["LOGS_BASE_URL"] != "" {
			env = step.Env
			break
		}
	}
	for _, tc := range []struct{ service, variable, target string }{
		{"lakehouse-logs", "LOGS_BASE_URL", "9428"},
		{"lakehouse-traces", "TRACES_BASE_URL", "10428"},
		{"loki-vl-proxy", "LOKI_PROXY_URL", "3100"},
		{"vlselect", "VLSELECT_URL", "9428"},
		{"vtselect", "VTSELECT_URL", "10428"},
	} {
		t.Run(tc.service, func(t *testing.T) {
			u, err := url.Parse(env[tc.variable])
			if err != nil || u.Port() == "" {
				t.Fatalf("invalid %s=%q", tc.variable, env[tc.variable])
			}
			want := fmt.Sprintf("%s:%s", u.Port(), tc.target)
			for _, port := range compose.Services[tc.service].Ports {
				if port == want || strings.HasSuffix(port, ":"+want) {
					return
				}
			}
			t.Fatalf("%s has no host mapping for %s (%s); published ports=%v", tc.service, tc.variable, want, compose.Services[tc.service].Ports)
		})
	}
}

// Execute the actual workflow shell with unavailable dependencies. Exhausting
// retries must fail, and a failing go test must remain a failure through tee.
func TestE2EWorkflowPropagatesFailures(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]struct {
			Steps []struct{ Name, Run string } `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(readRepoFile(t, root, ".github/workflows/e2e.yaml")), &workflow); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	for name, body := range map[string]string{"curl": "exit 22", "go": "exit 1", "sleep": "exit 0"} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"+body+"\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	checked := 0
	for _, step := range workflow.Jobs["e2e"].Steps {
		if !strings.HasPrefix(step.Name, "Wait for ") && !strings.Contains(step.Run, "go test ") {
			continue
		}
		checked++
		t.Run(step.Name, func(t *testing.T) {
			cmd := exec.Command("bash", "-e", "-c", step.Run)
			cmd.Dir = t.TempDir()
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"))
			if output, err := cmd.CombinedOutput(); err == nil {
				t.Fatalf("unavailable dependency/test unexpectedly succeeded:\n%s", output)
			}
		})
	}
	if checked != 15 {
		t.Fatalf("exercised %d readiness/test scripts, want 15", checked)
	}
}
