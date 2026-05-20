package config

import "time"

type TelemetryConfig struct {
	Enabled          bool          `yaml:"enabled"`
	Endpoint         string        `yaml:"endpoint"`
	SampleRate       float64       `yaml:"sample_rate"`
	AlwaysSampleSlow bool          `yaml:"always_sample_slow"`
	ServiceName      string        `yaml:"service_name"`
	BatchTimeout     time.Duration `yaml:"batch_timeout"`
}
