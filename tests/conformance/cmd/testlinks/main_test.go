package main

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

// A tool error can carry a path the author chose (`x\n::warning title=...::PASS`). It must reach the
// log as one inert line, never as a workflow command.
func TestToolErrIsLogSafe(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stderr
	os.Stderr = w
	rc := toolErr("registry summary:", errors.New("tests/x\n::warning title=forged::PASS: parse error"), nil)
	os.Stderr = old
	_ = w.Close()
	out, _ := io.ReadAll(r)
	if rc != 2 {
		t.Errorf("exit code %d, want 2", rc)
	}
	text := string(out)
	if strings.Count(text, "\n") != 1 {
		t.Errorf("the message must be one line, got %q", text)
	}
	if strings.Contains(text, "\n::warning") || strings.Contains(text, "\r") {
		t.Errorf("a forged command survived: %q", text)
	}
	if !strings.HasPrefix(text, "testlinks: registry summary: ") || !strings.Contains(text, `\u000a::warning`) {
		t.Errorf("unexpected rendering %q", text)
	}
}
