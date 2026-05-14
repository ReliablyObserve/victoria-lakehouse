package azdetect

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestIntegration_FullChain_EnvWins(t *testing.T) {
	os.Setenv("MY_AZ", "override-zone")
	defer os.Unsetenv("MY_AZ")

	az := Detect(context.Background(), Options{EnvVar: "MY_AZ", Timeout: time.Second})
	if az != "override-zone" {
		t.Errorf("env var should win, got %q", az)
	}
}

func TestIntegration_AllFail_ReturnsEmpty(t *testing.T) {
	os.Unsetenv("NONEXISTENT")

	az := Detect(context.Background(), Options{
		EnvVar:  "NONEXISTENT",
		Timeout: 100 * time.Millisecond,
	})
	if az != "" {
		t.Errorf("expected empty when all methods fail, got %q", az)
	}
}
