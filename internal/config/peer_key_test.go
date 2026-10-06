package config

import "testing"

func TestNormalizePeerAuthKey(t *testing.T) {
	for in, want := range map[string]string{"": "", "s3cr3t": "s3cr3t", "s3cr3t\n": "s3cr3t", " \ts3cr3t\r\n": "s3cr3t"} {
		got, err := NormalizePeerAuthKey(in)
		if err != nil || got != want {
			t.Errorf("NormalizePeerAuthKey(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"s3 cr3t", "s3\ncr3t", "s3\x00cr3t", "s3\x7fcr3t", "s3\tcr3t", "\n", " \t\r\n"} {
		if got, err := NormalizePeerAuthKey(in); err == nil {
			t.Errorf("NormalizePeerAuthKey(%q) = %q, want an error", in, got)
		}
	}
}

func TestValidate_NormalizesPeerAuthKey(t *testing.T) {
	c := Default()
	c.Mode = ModeLogs
	c.S3.Bucket = "b"
	c.Peer.AuthKey = "s3cr3t\n"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.Peer.AuthKey != "s3cr3t" {
		t.Fatalf("peer.auth_key = %q, want it trimmed", c.Peer.AuthKey)
	}
	c.Peer.AuthKey = "\n" // set but empty after trimming: must not turn into "no key"
	if err := c.Validate(); err == nil {
		t.Fatal("a whitespace-only key was accepted (it would run the pod without a key)")
	}
	c.Peer.AuthKey = "bad\x01key"
	if err := c.Validate(); err == nil {
		t.Fatal("a key with a control character was accepted")
	}
}
