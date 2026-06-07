package config

import "testing"

func TestBufferEngine_Validation(t *testing.T) {
	for _, tc := range []struct {
		val   string
		valid bool
	}{
		{"", true},         // default → legacy
		{"buffer", true},   // explicit legacy
		{"logstore", true}, // Option B
		{"logstorage", false},
		{"on", false},
		{"true", false},
	} {
		c := Default()
		c.Insert.BufferEngine = tc.val
		err := c.validateInsert()
		if tc.valid && err != nil {
			t.Errorf("buffer_engine=%q: unexpected error: %v", tc.val, err)
		}
		if !tc.valid && err == nil {
			t.Errorf("buffer_engine=%q: expected validation error, got nil", tc.val)
		}
	}
}

func TestBufferEngine_LogstoreFlag(t *testing.T) {
	c := Default()
	if c.Insert.BufferEngineLogstore() {
		t.Fatal("default config must NOT select logstore (legacy buffer is the default)")
	}
	c.Insert.BufferEngine = "logstore"
	if !c.Insert.BufferEngineLogstore() {
		t.Fatal("BufferEngine=logstore must report logstore")
	}
	c.Insert.BufferEngine = "buffer"
	if c.Insert.BufferEngineLogstore() {
		t.Fatal("BufferEngine=buffer must NOT report logstore")
	}
}

func TestBufferEngine_OverlayMerge(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Insert.BufferEngine = "logstore"
	overlay.Insert.BufferDir = "/tmp/custom-buffer"
	merged := mergeConfig(base, overlay)
	if !merged.Insert.BufferEngineLogstore() {
		t.Fatalf("overlay buffer_engine=logstore did not merge (got %q)", merged.Insert.BufferEngine)
	}
	if merged.Insert.BufferDir != "/tmp/custom-buffer" {
		t.Fatalf("overlay buffer_dir did not merge (got %q)", merged.Insert.BufferDir)
	}
	// Empty overlay must NOT clobber the base default.
	merged2 := mergeConfig(Default(), &Config{})
	if merged2.Insert.BufferEngine != "buffer" {
		t.Fatalf("empty overlay clobbered BufferEngine: %q", merged2.Insert.BufferEngine)
	}
}

func TestBufferEngine_Defaults(t *testing.T) {
	c := Default()
	if c.Insert.BufferEngine != "buffer" {
		t.Errorf("default BufferEngine = %q, want \"buffer\"", c.Insert.BufferEngine)
	}
	if c.Insert.BufferDir == "" {
		t.Error("default BufferDir must be set")
	}
	if c.Insert.BufferRetention <= 0 {
		t.Error("default BufferRetention must be positive")
	}
}
