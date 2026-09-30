package main

import "net/http"

// answerHTTP2Probe answers the HTTP/2 prior-knowledge probe ("PRI * HTTP/2.0")
// that clients such as Grafana's Tempo data source send to test for HTTP/2
// streaming support, exactly as VictoriaTraces v0.11.1+ does in its own request
// dispatcher (app/victoria-traces/main.go, which this binary replaces): a plain
// 405, so the probe does not fall through to the router and log an "unsupported
// path" warning on every connection. It reports whether it answered.
func answerHTTP2Probe(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != "PRI" || r.URL.Path != "*" {
		return false
	}
	http.Error(w, "HTTP/2 is currently not supported on this port", http.StatusMethodNotAllowed)
	return true
}
