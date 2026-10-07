//go:build parity

package parity

import (
	"encoding/json"
	"strings"
	"testing"
)

// fakeDocker answers the guard's calls from a fixed set of containers.
type fakeDocker struct {
	list     []string                     // ids the list call returns
	info     map[string]map[string]string // id -> labels
	restarts []string
	started  map[string]string
}

func (f *fakeDocker) call(method, path string) ([]byte, error) {
	switch {
	case method == "GET" && strings.HasPrefix(path, "/containers/json"):
		var out []map[string]string
		for _, id := range f.list {
			out = append(out, map[string]string{"Id": id})
		}
		return json.Marshal(out)
	case method == "GET":
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/containers/"), "/json")
		var c containerInfo
		c.ID = id
		c.Config.Labels = f.info[id]
		c.State.StartedAt = f.started[id]
		return json.Marshal(c)
	case method == "POST":
		id := strings.Split(strings.TrimPrefix(path, "/containers/"), "/")[0]
		f.restarts = append(f.restarts, id)
		f.started[id] = "2026-01-02T00:00:00Z"
	}
	return []byte("{}"), nil
}

func guardFixture() (*composeGuard, *fakeDocker, containerInfo) {
	own := map[string]string{labelProject: "proj", labelConfig: "/app/tests/parity/docker-compose.yml", labelService: "parity-tests"}
	f := &fakeDocker{
		list:    []string{"lh1"},
		info:    map[string]map[string]string{"lh1": {labelProject: "proj", labelConfig: "/app/tests/parity/docker-compose.yml", labelService: "lakehouse-logs"}},
		started: map[string]string{"lh1": "2026-01-01T00:00:00Z"},
	}
	var self containerInfo
	self.Config.Labels = own
	f.info["self"] = own
	g := &composeGuard{call: f.call}
	g.allowInspect("self")
	return g, f, self
}

func TestComposeGuard_AllowsTheOwnLakehouseService(t *testing.T) {
	g, f, self := guardFixture()
	c, err := g.target(self, "lakehouse-logs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.restart(c, 0); err != nil {
		t.Fatal(err)
	}
	if len(f.restarts) != 1 || f.restarts[0] != "lh1" {
		t.Fatalf("restarts %v, want [lh1]", f.restarts)
	}
	reportLockCells(t, 1)
}

func TestComposeGuard_Refuses(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(f *fakeDocker)
		service string
		want    string
	}{
		{"wrong project", func(f *fakeDocker) { f.info["lh1"][labelProject] = "other" }, "lakehouse-logs", "compose project"},
		{"wrong config file", func(f *fakeDocker) { f.info["lh1"][labelConfig] = "/elsewhere/docker-compose.yml" }, "lakehouse-logs", "compose config"},
		{"service not allowlisted", func(f *fakeDocker) {}, "victorialogs", "not allowlisted"},
		{"more than one match", func(f *fakeDocker) { f.list = []string{"lh1", "lh2"} }, "lakehouse-logs", "exactly one"},
		{"no match", func(f *fakeDocker) { f.list = nil }, "lakehouse-logs", "exactly one"},
		{"label says another service", func(f *fakeDocker) { f.info["lh1"][labelService] = "victorialogs" }, "lakehouse-logs", "is service"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, f, self := guardFixture()
			tc.mutate(f)
			if _, err := g.target(self, tc.service); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("target error %v, want it to contain %q", err, tc.want)
			}
			if len(f.restarts) != 0 {
				t.Fatalf("a refused target was restarted: %v", f.restarts)
			}
			reportLockCells(t, 1)
		})
	}
}

func TestComposeGuard_OnlyAllowlistedCalls(t *testing.T) {
	g, f, _ := guardFixture()
	for _, c := range []struct{ method, path string }{
		{"POST", "/containers/lh1/restart?t=20"}, // not verified yet
		{"POST", "/containers/lh1/kill"},
		{"POST", "/containers/lh1/stop"},
		{"DELETE", "/containers/lh1"},
		{"POST", "/containers/create"},
		{"GET", "/images/json"},
		{"POST", "/containers/lh1/exec"},
		{"GET", "/containers/lh1/archive"},
	} {
		if _, err := g.do(c.method, c.path); err == nil {
			t.Errorf("%s %s was allowed", c.method, c.path)
		}
		reportLockCells(t, 1)
	}
	if len(f.restarts) != 0 {
		t.Fatalf("something was restarted: %v", f.restarts)
	}
}

// Only the suite's own container and the targets the filtered list returned may
// be inspected: the shared daemon holds other projects' environment.
func TestComposeGuard_InspectsOnlyOwnAndListedContainers(t *testing.T) {
	g, f, self := guardFixture()
	f.info["other-project"] = map[string]string{labelProject: "other"}
	if _, err := g.inspect("other-project"); err == nil {
		t.Fatal("an unrelated container was inspected")
	}
	if _, err := g.inspect("self"); err != nil {
		t.Fatalf("own container: %v", err)
	}
	if _, err := g.inspect("lh1"); err == nil {
		t.Fatal("a container was inspected before the list returned it")
	}
	f.list = []string{"lh1", "lh2"}
	_, _ = g.target(self, "lakehouse-logs") // refused (two matches), but the list ran
	if _, err := g.inspect("lh2"); err != nil {
		t.Fatalf("a listed container may be inspected: %v", err)
	}
	reportLockCells(t, 4)
}
