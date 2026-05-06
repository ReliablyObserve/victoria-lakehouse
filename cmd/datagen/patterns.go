package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

type LogPattern func(rng *rand.Rand, ts time.Time, svc string, lvl string) (body string, attrs map[string]string)

var logPatterns = []LogPattern{
	jsonAccessLog,
	logfmtLog,
	nginxCombinedLog,
	javaStackTrace,
	otelLog,
}

func pickPattern(rng *rand.Rand) LogPattern {
	return logPatterns[rng.Intn(len(logPatterns))]
}

func jsonAccessLog(rng *rand.Rand, ts time.Time, svc string, _ string) (string, map[string]string) {
	method := httpMethods[rng.Intn(len(httpMethods))]
	path := []string{"/api/v1/users", "/api/v1/orders", "/api/v1/products", "/api/v1/health", "/api/v2/search"}[rng.Intn(5)]
	status := []int{200, 200, 200, 201, 204, 400, 401, 404, 500}[rng.Intn(9)]
	dur := 1 + rng.Intn(500)
	reqID := randomHex(16)

	body := fmt.Sprintf(`{"method":"%s","path":"%s","status":%d,"duration_ms":%d,"request_id":"%s","service":"%s","ts":"%s"}`,
		method, path, status, dur, reqID, svc, ts.Format(time.RFC3339Nano))

	attrs := map[string]string{
		"http.method":      method,
		"http.target":      path,
		"http.status_code": fmt.Sprintf("%d", status),
		"request_id":       reqID,
	}
	return body, attrs
}

func logfmtLog(rng *rand.Rand, ts time.Time, svc string, lvl string) (string, map[string]string) {
	components := []string{"http", "grpc", "db", "cache", "queue"}
	component := components[rng.Intn(len(components))]
	msgs := []string{
		"request handled", "connection opened", "query executed",
		"cache miss", "message published", "retry succeeded",
		"timeout exceeded", "circuit breaker tripped",
	}
	msg := msgs[rng.Intn(len(msgs))]
	dur := rng.Float64() * 100

	body := fmt.Sprintf("level=%s msg=%q component=%s duration=%.2fms service=%s ts=%s",
		strings.ToLower(lvl), msg, component, dur, svc, ts.Format(time.RFC3339Nano))

	attrs := map[string]string{
		"component": component,
		"format":    "logfmt",
	}
	return body, attrs
}

func nginxCombinedLog(rng *rand.Rand, ts time.Time, _ string, _ string) (string, map[string]string) {
	ips := []string{"10.0.1.42", "10.0.2.17", "192.168.1.100", "172.16.0.55", "10.1.2.33"}
	ip := ips[rng.Intn(len(ips))]
	method := httpMethods[rng.Intn(len(httpMethods))]
	paths := []string{"/", "/index.html", "/api/users", "/static/app.js", "/favicon.ico", "/health"}
	path := paths[rng.Intn(len(paths))]
	status := []int{200, 200, 200, 301, 304, 404, 500}[rng.Intn(7)]
	size := 200 + rng.Intn(50000)
	agents := []string{
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
		"curl/7.88.1",
		"Go-http-client/2.0",
		"python-requests/2.31.0",
	}
	agent := agents[rng.Intn(len(agents))]

	body := fmt.Sprintf(`%s - - [%s] "%s %s HTTP/1.1" %d %d "-" "%s"`,
		ip, ts.Format("02/Jan/2006:15:04:05 -0700"), method, path, status, size, agent)

	attrs := map[string]string{
		"format":    "nginx",
		"client_ip": ip,
	}
	return body, attrs
}

func javaStackTrace(rng *rand.Rand, _ time.Time, svc string, _ string) (string, map[string]string) {
	exceptions := []struct {
		class string
		msg   string
	}{
		{"java.lang.NullPointerException", "Cannot invoke method on null object"},
		{"java.sql.SQLException", "Connection refused: connect"},
		{"java.util.concurrent.TimeoutException", "Timeout waiting for task"},
		{"io.grpc.StatusRuntimeException", "UNAVAILABLE: upstream connect error"},
		{"com.fasterxml.jackson.core.JsonParseException", "Unexpected character ('}' (code 125))"},
		{"java.lang.OutOfMemoryError", "Java heap space"},
	}
	exc := exceptions[rng.Intn(len(exceptions))]

	packages := []string{
		"com.reliablyobserve." + svc + ".handler.RequestHandler",
		"com.reliablyobserve." + svc + ".service.ProcessingService",
		"com.reliablyobserve." + svc + ".repository.DataRepository",
		"org.springframework.web.servlet.FrameworkServlet",
		"io.netty.channel.AbstractChannelHandlerContext",
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "%s: %s\n", exc.class, exc.msg)
	depth := 3 + rng.Intn(8)
	for i := 0; i < depth; i++ {
		pkg := packages[rng.Intn(len(packages))]
		line := 10 + rng.Intn(500)
		fmt.Fprintf(&sb, "\tat %s.process(Unknown Source:%d)\n", pkg, line)
	}
	fmt.Fprintf(&sb, "\t... %d more", 5+rng.Intn(20))

	attrs := map[string]string{
		"exception.type":    exc.class,
		"exception.message": exc.msg,
		"format":            "java_stacktrace",
	}
	return sb.String(), attrs
}

func otelLog(rng *rand.Rand, ts time.Time, svc string, lvl string) (string, map[string]string) {
	msgs := []string{
		"Span started for incoming request",
		"Exporting batch of spans",
		"Metric collection completed",
		"Resource attributes resolved",
		"Baggage propagated to downstream",
		"Sampler decided to record span",
	}
	msg := msgs[rng.Intn(len(msgs))]
	traceID := randomHex(32)
	spanID := randomHex(16)

	body := fmt.Sprintf(`{"timestamp":"%s","severity":"%s","body":"%s","resource":{"service.name":"%s"},"traceId":"%s","spanId":"%s"}`,
		ts.Format(time.RFC3339Nano), lvl, msg, svc, traceID, spanID)

	attrs := map[string]string{
		"format":              "otel",
		"otel.trace_id":       traceID,
		"otel.span_id":        spanID,
		"instrumentation.lib": "opentelemetry-go/1.28.0",
	}
	return body, attrs
}
