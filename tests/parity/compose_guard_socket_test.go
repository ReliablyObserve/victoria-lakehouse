//go:build parity

package parity

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// dockerSocketCall talks to the Docker Engine API over the mounted socket.
func dockerSocketCall() dockerCall {
	sock := envOrDefault("DOCKER_SOCK", "/var/run/docker.sock")
	c := &http.Client{
		Timeout: 2 * time.Minute,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		}},
	}
	return func(method, path string) ([]byte, error) {
		req, err := http.NewRequest(method, "http://docker"+path, nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.Do(req)
		if err != nil {
			return nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode/100 != 2 {
			return nil, fmt.Errorf("docker %s %s: %d %s", method, path, resp.StatusCode, b)
		}
		return b, nil
	}
}
