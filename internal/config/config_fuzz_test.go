package config

import (
	"testing"
)

func FuzzParseSizeBytes(f *testing.F) {
	f.Add("512MB")
	f.Add("50GB")
	f.Add("1TB")
	f.Add("256KB")
	f.Add("100B")
	f.Add("1024")
	f.Add("")
	f.Add("abc")
	f.Add("  512MB  ")
	f.Add("0MB")
	f.Add("-1GB")
	f.Add("999999999999TB")
	f.Add("MB")
	f.Add("99999999999999999999")
	f.Add("1.5GB")
	f.Add("\x00\x00")
	f.Add("512 MB")

	f.Fuzz(func(t *testing.T, input string) {
		_, _ = ParseSizeBytes(input)
	})
}

func FuzzValidate(f *testing.F) {
	f.Add("logs", "bucket", "auto")
	f.Add("traces", "bucket", "direct")
	f.Add("", "bucket", "auto")
	f.Add("logs", "", "auto")
	f.Add("invalid", "bucket", "auto")
	f.Add("logs", "bucket", "bogus")

	f.Fuzz(func(t *testing.T, mode, bucket, topology string) {
		cfg := Default()
		cfg.Mode = Mode(mode)
		cfg.S3.Bucket = bucket
		cfg.Topology = Topology(topology)
		_ = cfg.Validate()
	})
}
