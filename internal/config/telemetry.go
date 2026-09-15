package config

import "time"

// TelemetryConfig controls OpenTelemetry tracing of the lakehouse itself.
type TelemetryConfig struct {
	// Enabled traces the lakehouse itself with OpenTelemetry. Without an
	// endpoint, spans are recorded but not exported.
	Enabled bool `yaml:"enabled"`
	// Endpoint is the OTLP gRPC endpoint that receives the lakehouse's own
	// spans.
	Endpoint string `yaml:"endpoint"`
	// SampleRate is the fraction of traces sampled, parent-based.
	SampleRate float64 `yaml:"sample_rate"`
	// AlwaysSampleSlow samples slow queries regardless of sample_rate.
	AlwaysSampleSlow bool `yaml:"always_sample_slow"`
	// ServiceName is the service name reported in the lakehouse's own spans.
	ServiceName string `yaml:"service_name"`
	// BatchTimeout is the span export batch timeout.
	BatchTimeout time.Duration `yaml:"batch_timeout"`
}
