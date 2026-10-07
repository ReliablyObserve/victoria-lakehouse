//go:build parity

package parity

// A restart of a Lakehouse service through the Docker Engine API. The parity
// suite runs in a container of the compose stack that mounts the Docker socket;
// a socket is a powerful handle, so every call goes through composeGuard, which
// enforces (and TestComposeGuard_* tests) that the helper can only:
//   - GET /containers/json (read-only list, to find the target),
//   - GET /containers/{id}/json (inspect),
//   - POST /containers/{id}/restart of a container it verified first, and
// that a verified target is exactly one container whose compose project and
// compose config files equal those of the suite's own container, and whose
// service is one of the allowlisted Lakehouse services.

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"testing"
	"time"
)

// restartableServices are the only compose services the suite may restart.
var restartableServices = map[string]bool{"lakehouse-logs": true, "lakehouse-traces": true}

// dockerCall performs one Docker Engine API request and returns the body.
type dockerCall func(method, path string) ([]byte, error)

type composeGuard struct {
	call     dockerCall
	verified map[string]bool
	// inspectable holds the only containers GET /containers/{id}/json may read:
	// the suite's own container and the ids the filtered list returned. The
	// daemon is shared with other projects, whose environment must stay unread.
	inspectable map[string]bool
}

func (g *composeGuard) allowInspect(ids ...string) {
	if g.inspectable == nil {
		g.inspectable = map[string]bool{}
	}
	for _, id := range ids {
		g.inspectable[id] = true
	}
}

var (
	listPath    = regexp.MustCompile(`^/containers/json(\?.*)?$`)
	inspectPath = regexp.MustCompile(`^/containers/([0-9a-zA-Z_.-]+)/json$`)
	restartPath = regexp.MustCompile(`^/containers/([0-9a-zA-Z_.-]+)/restart(\?t=\d+)?$`)
)

// do is the only way the helper talks to Docker: it refuses every other
// method/path pair, and a restart of a container that was not verified.
func (g *composeGuard) do(method, path string) ([]byte, error) {
	switch {
	case method == "GET" && listPath.MatchString(path):
	case method == "GET" && inspectPath.MatchString(path):
		if id := inspectPath.FindStringSubmatch(path)[1]; !g.inspectable[id] {
			return nil, fmt.Errorf("refusing to inspect %s: not this suite's own container nor a listed target", id)
		}
	case method == "POST" && restartPath.MatchString(path):
		id := restartPath.FindStringSubmatch(path)[1]
		if !g.verified[id] {
			return nil, fmt.Errorf("refusing to restart %s: not verified as an allowlisted service of this compose project", id)
		}
	default:
		return nil, fmt.Errorf("refusing docker %s %s: not on the allowlist", method, path)
	}
	return g.call(method, path)
}

type containerInfo struct {
	ID     string `json:"Id"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	State struct {
		StartedAt string `json:"StartedAt"`
	} `json:"State"`
}

const (
	labelProject = "com.docker.compose.project"
	labelConfig  = "com.docker.compose.project.config_files"
	labelService = "com.docker.compose.service"
)

func (g *composeGuard) inspect(id string) (containerInfo, error) {
	var c containerInfo
	raw, err := g.do("GET", "/containers/"+id+"/json")
	if err != nil {
		return c, err
	}
	return c, json.Unmarshal(raw, &c)
}

// target finds the one running container of service in the suite's own compose
// project and config files, verifies it and returns it.
func (g *composeGuard) target(self containerInfo, service string) (containerInfo, error) {
	if !restartableServices[service] {
		return containerInfo{}, fmt.Errorf("service %q is not allowlisted for restart", service)
	}
	project, cfg := self.Config.Labels[labelProject], self.Config.Labels[labelConfig]
	if project == "" || cfg == "" {
		return containerInfo{}, fmt.Errorf("the suite's own container has no compose project or config_files label")
	}
	filters, _ := json.Marshal(map[string][]string{"label": {labelProject + "=" + project, labelService + "=" + service}})
	raw, err := g.do("GET", "/containers/json?filters="+url.QueryEscape(string(filters)))
	if err != nil {
		return containerInfo{}, err
	}
	var list []struct {
		ID string `json:"Id"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		return containerInfo{}, err
	}
	for _, l := range list {
		g.allowInspect(l.ID)
	}
	if len(list) != 1 {
		return containerInfo{}, fmt.Errorf("want exactly one running %s container in project %s, found %d", service, project, len(list))
	}
	c, err := g.inspect(list[0].ID)
	if err != nil {
		return containerInfo{}, err
	}
	l := c.Config.Labels
	if l[labelProject] != project {
		return containerInfo{}, fmt.Errorf("container %s belongs to compose project %q, not %q", list[0].ID, l[labelProject], project)
	}
	if l[labelConfig] != cfg {
		return containerInfo{}, fmt.Errorf("container %s was started from compose config %q, not %q", list[0].ID, l[labelConfig], cfg)
	}
	if l[labelService] != service {
		return containerInfo{}, fmt.Errorf("container %s is service %q, not %q", list[0].ID, l[labelService], service)
	}
	if g.verified == nil {
		g.verified = map[string]bool{}
	}
	g.verified[list[0].ID] = true
	return c, nil
}

// restart restarts a verified target and returns once its StartedAt changed.
func (g *composeGuard) restart(c containerInfo, stopTimeoutSeconds int) (time.Time, error) {
	if _, err := g.do("POST", fmt.Sprintf("/containers/%s/restart?t=%d", c.ID, stopTimeoutSeconds)); err != nil {
		return time.Time{}, err
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		now, err := g.inspect(c.ID)
		if err == nil && now.State.StartedAt != "" && now.State.StartedAt != c.State.StartedAt {
			return time.Parse(time.RFC3339Nano, now.State.StartedAt)
		}
		if time.Now().After(deadline) {
			return time.Time{}, fmt.Errorf("container %s did not report a new StartedAt after the restart", c.ID)
		}
		time.Sleep(time.Second)
	}
}

// restartComposeServices restarts the given services of this compose project
// through composeGuard, proves each container really restarted (its StartedAt
// changed), and waits until base answers /health again and its insert buffer
// answers on the endpoint select pods read it with (/internal/buffer/query,
// mode logs or traces), and returns the container's new StartedAt. Rows still
// in the buffer at the restart come back from the buffer's own segments.
func restartComposeServices(t *testing.T, base, mode string, services []string, stopTimeoutSeconds int) (startedAt time.Time) {
	t.Helper()
	g := &composeGuard{call: dockerSocketCall()}
	host, _ := os.Hostname()
	g.allowInspect(host)
	self, err := g.inspect(host)
	if err != nil {
		t.Fatalf("inspect own container %s: %v (is the Docker socket mounted into the parity-tests container?)", host, err)
	}
	for _, svc := range services {
		c, err := g.target(self, svc)
		if err != nil {
			t.Fatal(err)
		}
		at, err := g.restart(c, stopTimeoutSeconds)
		if err != nil {
			t.Fatal(err)
		}
		startedAt = at
	}
	deadline := time.Now().Add(3 * time.Minute)
	for {
		resp, err := httpClient.Get(base + "/health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				if _, err := bufferedRows(base, mode, url.Values{"all_tenants": {"true"}}, time.Unix(0, 0), time.Now()); err == nil {
					return startedAt
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not become healthy, with its buffer serving, within 3m of the restart", base)
		}
		time.Sleep(2 * time.Second)
	}
}
