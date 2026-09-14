package delete

import (
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// A documented example that the server rejects is worse than no example: the
// operator copies it, gets a 400, and concludes the delete API is broken. The
// docs showed `start=2025-01-01` while the handler parses `start` as UNIX
// nanoseconds, and `mode=tombstone|rewrite` while the handler accepts
// `hide|permanent|auto`.
//
// This test runs every delete URL in the docs through the real handler. It is a
// doc gate, not a handler test: if an example stops being accepted — because it
// was edited, or because the handler's validation changed — CI says so.

// docExampleURL matches a delete endpoint URL with a query string inside a
// documentation code block.
// The examples are single-quoted shell arguments whose query strings contain
// double quotes (`service.name:="leaked"`), so the match runs to the closing
// single quote or to whitespace.
var docExampleURL = regexp.MustCompile(`https?://[^'\s]+/delete/(?:logsql|tracessql)/[a-z]+\?[^'\s]+`)

func docsDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("..", "..", "docs"))
	if err != nil {
		t.Fatalf("resolve docs dir: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("docs dir not readable: %v", err)
	}
	return dir
}

// TestDocs_DeleteExamplesAreAcceptedByTheHandler walks the operator docs and
// replays each delete/estimate/verify example against the handler.
func TestDocs_DeleteExamplesAreAcceptedByTheHandler(t *testing.T) {
	dir := docsDir(t)
	handler := newTestHandler(defaultCfg(), testFiles())

	var checked int
	for _, name := range []string{"deletion-strategy.md", "operations.md"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, raw := range docExampleURL.FindAllString(string(data), -1) {
			u, err := url.Parse(raw)
			if err != nil {
				t.Errorf("%s: %q is not a URL: %v", name, raw, err)
				continue
			}
			params := u.Query()
			// Placeholder-only examples ("start=...") document the shape, not a
			// runnable command; the endpoint reference above them carries the
			// unit. Replaying them proves nothing.
			if params.Get("start") == "..." || params.Get("start") == "" {
				continue
			}
			checked++

			endpoint := u.Path[strings.LastIndex(u.Path, "/")+1:]
			var h http.HandlerFunc
			switch endpoint {
			case "delete":
				h = handler.handleDelete
			case "estimate":
				h = handler.handleEstimate
			case "verify":
				h = handler.handleVerify
			default:
				t.Errorf("%s: documented endpoint %q is not one this handler serves", name, endpoint)
				continue
			}
			rec := postForm(h, u.Path, params)
			if rec.Code >= 400 {
				t.Errorf("%s documents a request the server rejects with %d (%s):\n  %s",
					name, rec.Code, strings.TrimSpace(rec.Body.String()), raw)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no runnable delete example was found in the docs; this gate would pass on an empty file")
	}
}

// TestDocs_DeleteModesAreTheOnesTheHandlerAccepts pins the other half of the
// same finding: the mode values the docs name must be the ones the handler
// switches on.
func TestDocs_DeleteModesAreTheOnesTheHandlerAccepts(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(docsDir(t), "deletion-strategy.md"))
	if err != nil {
		t.Fatalf("read deletion-strategy.md: %v", err)
	}
	doc := string(data)

	handler := newTestHandler(defaultCfg(), testFiles())
	for _, mode := range []string{"hide", "permanent", "auto"} {
		if !strings.Contains(doc, "`"+mode+"`") {
			t.Errorf("mode %q is accepted by the handler but not documented", mode)
		}
		rec := postForm(handler.handleDelete, "/delete/logsql/delete", url.Values{
			"query": {`service.name:="leaked"`}, "start": {"1000"}, "end": {"9000"}, "mode": {mode},
		})
		if rec.Code >= 400 {
			t.Errorf("documented mode %q is rejected with %d (%s)", mode, rec.Code, strings.TrimSpace(rec.Body.String()))
		}
	}
	// And the modes that never existed are gone from the docs.
	for _, stale := range []string{"mode=tombstone", "mode=rewrite", "&mode=tombstone|rewrite|auto"} {
		if strings.Contains(doc, stale) {
			t.Errorf("the docs still name %q, which the handler rejects with 400", stale)
		}
	}
}

// TestDocs_DeleteTimestampsAreNanoseconds is the unit check on its own: every
// documented start/end must parse the way the handler parses it, and must be
// large enough to be nanoseconds rather than seconds or a date.
func TestDocs_DeleteTimestampsAreNanoseconds(t *testing.T) {
	dir := docsDir(t)
	// 2000-01-01 in nanoseconds: anything smaller is a seconds-or-milliseconds
	// value that would silently select nothing.
	const minNs = 946684800000000000

	for _, name := range []string{"deletion-strategy.md", "operations.md"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, raw := range docExampleURL.FindAllString(string(data), -1) {
			u, err := url.Parse(raw)
			if err != nil {
				continue
			}
			for _, param := range []string{"start", "end"} {
				v := u.Query().Get(param)
				if v == "" || v == "..." {
					continue
				}
				ns, err := parseNs(v)
				if err != nil {
					t.Errorf("%s: %s=%q in %q is not an integer the handler can parse", name, param, v, raw)
					continue
				}
				if ns < minNs {
					t.Errorf("%s: %s=%q in %q is too small to be nanoseconds (want >= %d)", name, param, v, raw, minNs)
				}
			}
		}
	}
}

func parseNs(v string) (int64, error) {
	var ns int64
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, errInjected
		}
		ns = ns*10 + int64(c-'0')
	}
	if v == "" {
		return 0, errInjected
	}
	return ns, nil
}
