package telemetry

import (
	"context"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	globalTracer trace.Tracer
	mu           sync.Mutex
)

// discardExporter implements sdktrace.SpanExporter but discards all spans.
// Useful when telemetry is enabled (spans are recorded) but no endpoint is configured.
type discardExporter struct{}

func (d *discardExporter) ExportSpans(_ context.Context, _ []sdktrace.ReadOnlySpan) error {
	return nil
}

func (d *discardExporter) Shutdown(_ context.Context) error {
	return nil
}

// Init initializes the OpenTelemetry tracing pipeline.
//
// When cfg.Enabled is false, a noop provider is set and a noop shutdown is returned.
// When cfg.Enabled is true with no Endpoint, a discard exporter is used (spans
// are recorded but not exported — useful for testing).
// When cfg.Enabled is true with an Endpoint, an OTLP gRPC exporter sends spans
// to the configured collector.
func Init(ctx context.Context, cfg config.TelemetryConfig, serviceName string) (func(context.Context) error, error) {
	if !cfg.Enabled {
		otel.SetTracerProvider(noop.NewTracerProvider())
		setTracer(noop.NewTracerProvider().Tracer("lakehouse"))
		return func(context.Context) error { return nil }, nil
	}

	var exporter sdktrace.SpanExporter
	if cfg.Endpoint == "" {
		exporter = &discardExporter{}
	} else {
		var err error
		exporter, err = otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(cfg.Endpoint),
			otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		)
		if err != nil {
			return nil, err
		}
	}

	sampleRate := cfg.SampleRate
	if sampleRate <= 0 {
		sampleRate = 1.0
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, err
	}

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRate))),
	}

	if cfg.BatchTimeout > 0 {
		opts = append(opts, sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(cfg.BatchTimeout)))
	} else {
		opts = append(opts, sdktrace.WithBatcher(exporter))
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	setTracer(tp.Tracer("lakehouse"))

	return tp.Shutdown, nil
}

// Tracer returns the package-level tracer for creating spans.
func Tracer() trace.Tracer {
	mu.Lock()
	defer mu.Unlock()
	if globalTracer == nil {
		return noop.NewTracerProvider().Tracer("lakehouse")
	}
	return globalTracer
}

func setTracer(t trace.Tracer) {
	mu.Lock()
	defer mu.Unlock()
	globalTracer = t
}
