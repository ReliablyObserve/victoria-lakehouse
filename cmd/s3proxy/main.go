package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"
)

var (
	requestCount atomic.Int64
	totalDelay   atomic.Int64
)

func main() {
	listen := flag.String("listen", ":19001", "Listen address")
	upstream := flag.String("upstream", "http://localhost:19000", "MinIO upstream")
	getDelay := flag.Duration("get-delay", 65*time.Millisecond, "Delay for GET requests (simulates S3 first-byte latency)")
	headDelay := flag.Duration("head-delay", 15*time.Millisecond, "Delay for HEAD requests")
	putDelay := flag.Duration("put-delay", 30*time.Millisecond, "Delay for PUT requests")
	listDelay := flag.Duration("list-delay", 80*time.Millisecond, "Delay for LIST (GET with prefix) requests")
	jitter := flag.Float64("jitter", 0.3, "Random jitter factor (0.3 = ±30%)")
	bandwidthMBps := flag.Float64("bandwidth", 100.0, "Simulated bandwidth limit in MB/s (0 = unlimited)")
	flag.Parse()

	target, err := url.Parse(*upstream)
	if err != nil {
		log.Fatalf("invalid upstream URL: %v", err)
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
	}

	if *bandwidthMBps > 0 {
		proxy.ModifyResponse = func(resp *http.Response) error {
			if resp.ContentLength > 1024*1024 {
				resp.Body = &throttledReader{
					r:     resp.Body,
					bps:   int64(*bandwidthMBps * 1024 * 1024),
					chunk: 64 * 1024,
				}
			}
			return nil
		}
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var delay time.Duration
		switch r.Method {
		case http.MethodGet:
			if r.URL.Query().Get("list-type") != "" || r.URL.Query().Get("prefix") != "" {
				delay = *listDelay
			} else {
				delay = *getDelay
			}
		case http.MethodHead:
			delay = *headDelay
		case http.MethodPut:
			delay = *putDelay
		default:
			delay = *getDelay
		}

		if *jitter > 0 {
			j := 1.0 + (*jitter * (2*rand.Float64() - 1)) // #nosec G404
			delay = time.Duration(float64(delay) * j)
		}

		time.Sleep(delay)
		requestCount.Add(1)
		totalDelay.Add(int64(delay))

		proxy.ServeHTTP(w, r)
	})

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			count := requestCount.Load()
			delay := totalDelay.Load()
			if count > 0 {
				avgMs := float64(delay) / float64(count) / float64(time.Millisecond)
				log.Printf("[s3proxy] requests=%d avg_delay=%.1fms", count, avgMs)
			}
		}
	}()

	log.Printf("[s3proxy] listening on %s → %s (GET=%s HEAD=%s PUT=%s LIST=%s jitter=%.0f%% bw=%.0fMB/s)",
		*listen, *upstream, *getDelay, *headDelay, *putDelay, *listDelay, *jitter*100, *bandwidthMBps)
	log.Fatal(http.ListenAndServe(*listen, handler))
}

type throttledReader struct {
	r     io.ReadCloser
	bps   int64
	chunk int
}

func (t *throttledReader) Read(p []byte) (int, error) {
	if len(p) > t.chunk {
		p = p[:t.chunk]
	}
	n, err := t.r.Read(p)
	if n > 0 && t.bps > 0 {
		sleepDuration := time.Duration(float64(n) / float64(t.bps) * float64(time.Second))
		time.Sleep(sleepDuration)
	}
	return n, err
}

func (t *throttledReader) Close() error {
	return t.r.Close()
}

func init() {
	_ = fmt.Sprintf // avoid unused import
}
