package config

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

var (
	yamlFence    = regexp.MustCompile("(?s)```ya?ml\n(.*?)```")
	lakehouseDoc = regexp.MustCompile(`(?m)^lakehouse:`)
)

// strictConfigErrors decodes a config document the way LoadWithMode does,
// but rejecting unknown keys at every depth, so a documented example with a
// typo, a removed key or a wrong nested shape fails here instead of
// silently doing nothing (unknown keys) or stopping a binary at startup
// (wrong shapes).
func strictConfigErrors(doc []byte) error {
	var wrapper struct {
		Lakehouse Config `yaml:"lakehouse"`
	}
	dec := yaml.NewDecoder(bytes.NewReader(doc))
	dec.KnownFields(true)
	return dec.Decode(&wrapper)
}

// TestDocumentedConfigExamplesLoad loads every full `lakehouse:` YAML
// example in docs/ and README.md, and every config file under
// deployment/docker, with strict decoding. Fragments without the
// `lakehouse:` root are checked key by key by
// scripts/ci/config_drift_report.py.
func TestDocumentedConfigExamplesLoad(t *testing.T) {
	root := filepath.Join("..", "..")
	var sources []string
	err := filepath.Walk(filepath.Join(root, "docs"), func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(path, ".md") {
			sources = append(sources, path)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	sources = append(sources, filepath.Join(root, "README.md"))

	checked := 0
	for _, path := range sources {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range yamlFence.FindAllSubmatch(data, -1) {
			if !lakehouseDoc.Match(m[1]) {
				continue
			}
			checked++
			if err := strictConfigErrors(m[1]); err != nil {
				t.Errorf("%s: documented config example does not load: %v", path, err)
			}
		}
	}

	configs, err := filepath.Glob(filepath.Join(root, "deployment", "docker", "lakehouse-*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range configs {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		if err := strictConfigErrors(data); err != nil {
			t.Errorf("%s: config file does not load: %v", path, err)
		}
	}
	if checked < 5 {
		t.Fatalf("checked only %d config documents; the docs or deployment layout moved", checked)
	}
}

func TestStrictConfigErrors(t *testing.T) {
	cases := map[string]bool{
		"lakehouse:\n  query:\n    file_workers: 8\n":                                                     true,
		"lakehouse:\n  query:\n    file_wrokers: 8\n":                                                     false,
		"lakehouse:\n  tenant:\n    overrides:\n      \"1:1\":\n        retention: 7d\n":                  false,
		"lakehouse:\n  tenant:\n    overrides:\n      \"1:1\":\n        retention:\n          keep: 7d\n": true,
	}
	for doc, ok := range cases {
		if err := strictConfigErrors([]byte(doc)); (err == nil) != ok {
			t.Errorf("strictConfigErrors(%q) = %v, want ok=%v", doc, err, ok)
		}
	}
}
