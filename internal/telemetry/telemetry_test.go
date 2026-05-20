package telemetry

import (
	"context"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestInit_Disabled(t *testing.T) {
	ctx := context.Background()
	cfg := config.TelemetryConfig{Enabled: false}

	shutdown, err := Init(ctx, cfg, "test-service")
	if err != nil {
		t.Fatalf("Init returned error: %v", err)
	}
	defer shutdown(ctx)

	// With disabled config, the global provider should be noop.
	tp := otel.GetTracerProvider()
	if _, ok := tp.(noop.TracerProvider); !ok {
		t.Errorf("expected noop.TracerProvider, got %T", tp)
	}

	// Spans from a noop provider should not be recording.
	tracer := tp.Tracer("test")
	_, span := tracer.Start(ctx, "test-span")
	defer span.End()

	if span.IsRecording() {
		t.Error("expected noop span to not be recording")
	}
}

func TestInit_Enabled_NoEndpoint(t *testing.T) {
	ctx := context.Background()
	cfg := config.TelemetryConfig{
		Enabled:    true,
		SampleRate: 1.0,
	}

	shutdown, err := Init(ctx, cfg, "test-service")
	if err != nil {
		t.Fatalf("Init returned error: %v", err)
	}
	defer shutdown(ctx)

	// With enabled config and no endpoint, we should get an SDK provider
	// (not noop) that records spans via the discard exporter.
	tracer := Tracer()
	_, span := tracer.Start(ctx, "test-span")
	defer span.End()

	if !span.IsRecording() {
		t.Error("expected span to be recording with enabled config")
	}

	if !span.SpanContext().TraceID().IsValid() {
		t.Error("expected valid TraceID")
	}

	if !span.SpanContext().SpanID().IsValid() {
		t.Error("expected valid SpanID")
	}
}

func TestTracer_ReturnsNamedTracer(t *testing.T) {
	ctx := context.Background()
	cfg := config.TelemetryConfig{
		Enabled:    true,
		SampleRate: 1.0,
	}

	shutdown, err := Init(ctx, cfg, "tracer-test")
	if err != nil {
		t.Fatalf("Init returned error: %v", err)
	}
	defer shutdown(ctx)

	tracer := Tracer()
	if tracer == nil {
		t.Fatal("Tracer() returned nil")
	}

	// Verify the tracer creates valid spans.
	_, span := tracer.Start(ctx, "named-span")
	defer span.End()

	if !span.IsRecording() {
		t.Error("expected span from Tracer() to be recording")
	}

	if !span.SpanContext().IsValid() {
		t.Error("expected valid span context from named tracer")
	}
}

func TestShutdown_Idempotent(t *testing.T) {
	ctx := context.Background()
	cfg := config.TelemetryConfig{
		Enabled:    true,
		SampleRate: 1.0,
	}

	shutdown, err := Init(ctx, cfg, "shutdown-test")
	if err != nil {
		t.Fatalf("Init returned error: %v", err)
	}

	// First shutdown should succeed.
	if err := shutdown(ctx); err != nil {
		t.Errorf("first shutdown returned error: %v", err)
	}

	// Second shutdown should not panic or return error.
	if err := shutdown(ctx); err != nil {
		t.Errorf("second shutdown returned error: %v", err)
	}
}
