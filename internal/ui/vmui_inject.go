package ui

import (
	"bytes"
	"net/http"
	"strings"
)

const injectScript = `<script src="/lakehouse/ui/vmui-tab.js"></script>`

// InjectLakehouseTab wraps an upstream handler and injects a script tag
// into HTML responses just before the closing </body> tag.
func InjectLakehouseTab(upstream http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &responseRecorder{
			ResponseWriter: w,
			body:           &bytes.Buffer{},
		}

		upstream.ServeHTTP(rec, r)

		statusCode := rec.statusCode
		if !rec.wroteHeader {
			statusCode = http.StatusOK
		}

		ct := rec.Header().Get("Content-Type")
		if !strings.HasPrefix(ct, "text/html") {
			w.WriteHeader(statusCode)
			_, _ = w.Write(rec.body.Bytes()) // #nosec G104
			return
		}

		body := rec.body.String()
		idx := strings.LastIndex(body, "</body>")
		if idx < 0 {
			w.WriteHeader(statusCode)
			_, _ = w.Write(rec.body.Bytes()) // #nosec G104
			return
		}

		modified := body[:idx] + injectScript + "\n" + body[idx:]
		rec.Header().Del("Content-Length")
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(modified)) // #nosec G104
	})
}

type responseRecorder struct {
	http.ResponseWriter
	statusCode  int
	body        *bytes.Buffer
	wroteHeader bool
}

func (r *responseRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.wroteHeader = true
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	return r.body.Write(b)
}
