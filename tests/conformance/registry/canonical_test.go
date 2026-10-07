package registry

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// The strict loaders refuse what the gate's lenient readers could read differently.
func TestLoaders_RejectNonCanonicalYAML(t *testing.T) {
	for name, tc := range map[string]struct{ body, want string }{
		"pending yes":   {"- {id: a, pending: yes}\n", "pending must be true or false"},
		"pending on":    {"- {id: a, pending: on}\n", "pending must be true or false"},
		"two documents": {"- {id: a}\n---\n- {id: b}\n", "more than one YAML document"},
	} {
		_, err := LoadDir(writeTree(t, map[string]string{"rows/x.yaml": tc.body}) + "/rows")
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("LoadDir %s: %v, want %q", name, err, tc.want)
		}
		_, err = LoadFeatures(writeTree(t, map[string]string{"features/x.yaml": tc.body})+"/features", "")
		if tc.want != "pending must be true or false" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
			t.Errorf("LoadFeatures %s: %v, want %q", name, err, tc.want)
		}
	}
}
