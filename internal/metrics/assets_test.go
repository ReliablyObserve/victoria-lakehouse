package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The Grafana dashboards and the Prometheus alert rules are shipped artifacts:
// an operator deploys the files in `dashboards/` and `alerts/` as they are. A
// malformed or empty one is a silently broken deployment — Grafana refuses the
// import, or Prometheus rejects the rule group at load — so they are covered
// here rather than trusted to review.
//
// The assertions are deliberately structural (parses, non-empty, every rule
// has an expression and a name). Asserting that every metric referenced by a
// panel exists in this package would be stronger, but several panels
// legitimately reference recording-rule outputs and histogram-derived series
// (`..._bucket`) that no Go source defines literally.

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("abs repo root: %v", err)
	}
	return root
}

func TestDashboards_ParseAndHavePanels(t *testing.T) {
	dir := filepath.Join(repoRoot(t), "dashboards")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	found := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		found++
		path := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		var dash struct {
			Title  string           `json:"title"`
			Panels []map[string]any `json:"panels"`
		}
		if err := json.Unmarshal(data, &dash); err != nil {
			t.Fatalf("%s: not valid JSON: %v", path, err)
		}
		if strings.TrimSpace(dash.Title) == "" {
			t.Errorf("%s: dashboard has no title", path)
		}
		if len(dash.Panels) == 0 {
			t.Errorf("%s: dashboard has no panels", path)
		}
	}
	if found == 0 {
		t.Fatalf("no dashboards found under %s", dir)
	}
}

func TestAlertRules_ParseAndHaveExpressions(t *testing.T) {
	dir := filepath.Join(repoRoot(t), "alerts")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	type rule struct {
		Alert       string            `yaml:"alert"`
		Record      string            `yaml:"record"`
		Expr        string            `yaml:"expr"`
		Labels      map[string]string `yaml:"labels"`
		Annotations map[string]string `yaml:"annotations"`
		For         string            `yaml:"for"`
	}
	type group struct {
		Name  string `yaml:"name"`
		Rules []rule `yaml:"rules"`
	}
	var file struct {
		Groups []group `yaml:"groups"`
	}

	found := 0
	for _, e := range entries {
		if e.IsDir() || (!strings.HasSuffix(e.Name(), ".yml") && !strings.HasSuffix(e.Name(), ".yaml")) {
			continue
		}
		found++
		path := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		file.Groups = nil
		if err := yaml.Unmarshal(data, &file); err != nil {
			t.Fatalf("%s: not valid YAML: %v", path, err)
		}
		if len(file.Groups) == 0 {
			t.Fatalf("%s: no rule groups", path)
		}
		for _, g := range file.Groups {
			if strings.TrimSpace(g.Name) == "" {
				t.Errorf("%s: rule group without a name", path)
			}
			if len(g.Rules) == 0 {
				t.Errorf("%s: group %q has no rules", path, g.Name)
			}
			for i, r := range g.Rules {
				if strings.TrimSpace(r.Alert) == "" && strings.TrimSpace(r.Record) == "" {
					t.Errorf("%s: group %q rule %d has neither alert nor record", path, g.Name, i)
				}
				if strings.TrimSpace(r.Expr) == "" {
					t.Errorf("%s: group %q rule %q has an empty expr", path, g.Name, r.Alert+r.Record)
				}
			}
		}
	}
	if found == 0 {
		t.Fatalf("no alert rule files found under %s", dir)
	}
}
