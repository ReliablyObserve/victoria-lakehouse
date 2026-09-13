package inventory

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// protocolConstRe matches the internal peer-protocol version constants
// VictoriaLogs declares in app/vlstorage/netselect/netselect.go, e.g.
//
//	QueryProtocolVersion            = "v5"
//	DeleteRunTaskProtocolVersion    = "v2"
//
// Both the select-side and the delete-side constants live in the same block;
// the optional `const` keyword also matches a single-constant declaration, so
// a tree that stops grouping them is still read correctly.
var protocolConstRe = regexp.MustCompile(`(?m)^\s*(?:const\s+)?([A-Za-z]+)ProtocolVersion\s*=\s*"([A-Za-z0-9.]+)"`)

// netselectRelPath is where the constants live inside a VictoriaLogs checkout.
var netselectRelPath = filepath.Join("app", "vlstorage", "netselect", "netselect.go")

// deleteConstPrefixes are the constant-name prefixes that belong to the
// /internal/delete/* family. Everything else in the block is a
// /internal/select/* endpoint.
var deleteConstPrefixes = []string{"DeleteRunTask", "DeleteStopTask", "DeleteActiveTasks"}

// Protocol is the pair of internal peer-protocol versions one vendored
// VictoriaLogs tree expects. Every /internal/select/* request must carry
// version=<Select> and every /internal/delete/* request version=<Delete>, or
// the handler rejects it before any query logic runs (checkProtocolVersion in
// app/vlselect/internalselect/internalselect.go).
type Protocol struct {
	Select string `yaml:"select"`
	Delete string `yaml:"delete"`
}

// Protocols records the protocol pair PER MODULE, because the two binaries
// vendor different VictoriaLogs trees and the versions are not in lockstep:
//
//	VL — the logs binary, from deps/VictoriaLogs (VL_VERSION_LOGS)
//	VT — the traces binary, from lakehouse-traces/deps/VictoriaLogs
//	     (VL_COMMIT_TRACES). lakehouse-traces mounts VictoriaLogs'
//	     internalselect package for /internal/select/* and /internal/delete/*,
//	     so its protocol versions come from ITS VictoriaLogs copy, not from
//	     VictoriaTraces and not from the logs pin.
//
// At VL v1.52.0 / VL v1.51.0 (VT v0.11.0's pin) the select versions agree at
// v5 while the delete versions do not (v2 vs v1) — exactly the kind of split
// the registry's {{proto.internal_select}} / {{proto.internal_delete}}
// placeholders exist to express, and the reason they resolve per module.
type Protocols struct {
	VL Protocol `yaml:"vl"`
	VT Protocol `yaml:"vt"`
}

// IsZero reports whether no protocol versions were extracted.
func (p Protocols) IsZero() bool { return p == Protocols{} }

// ExtractProtocol reads the protocol pair from one vendored VictoriaLogs
// checkout. Every select-side constant in the block must carry the same
// version, and so must every delete-side one: upstream bumps them together,
// and a split would mean a single placeholder can no longer stand for the
// whole family — a registry change, not something to average over silently.
func ExtractProtocol(vlDir string) (Protocol, error) {
	path := filepath.Join(vlDir, netselectRelPath)
	data, err := os.ReadFile(path)
	if err != nil {
		return Protocol{}, fmt.Errorf("read protocol versions: %w", err)
	}
	matches := protocolConstRe.FindAllStringSubmatch(string(data), -1)
	if len(matches) == 0 {
		return Protocol{}, fmt.Errorf("%s declares no *ProtocolVersion constants — upstream moved them; update extract_protocol.go", path)
	}

	selectVersions := map[string][]string{}
	deleteVersions := map[string][]string{}
	for _, m := range matches {
		name, version := m[1], m[2]
		bucket := selectVersions
		for _, p := range deleteConstPrefixes {
			if strings.HasPrefix(name, p) {
				bucket = deleteVersions
				break
			}
		}
		bucket[version] = append(bucket[version], name+"ProtocolVersion")
	}

	sel, err := single(selectVersions, "select", path)
	if err != nil {
		return Protocol{}, err
	}
	del, err := single(deleteVersions, "delete", path)
	if err != nil {
		return Protocol{}, err
	}
	return Protocol{Select: sel, Delete: del}, nil
}

// single collapses a version->constants map to the one version they all
// share, failing loudly when the family is empty or split.
func single(byVersion map[string][]string, family, path string) (string, error) {
	switch len(byVersion) {
	case 0:
		return "", fmt.Errorf("%s declares no %s-side *ProtocolVersion constants — update deleteConstPrefixes in extract_protocol.go", path, family)
	case 1:
		for v := range byVersion {
			return v, nil
		}
	}
	var parts []string
	for v, names := range byVersion {
		sort.Strings(names)
		parts = append(parts, fmt.Sprintf("%s=%s", v, strings.Join(names, ",")))
	}
	sort.Strings(parts)
	return "", fmt.Errorf("%s: the %s-side protocol constants disagree (%s); the registry's {{proto.internal_%s}} placeholder assumes one version for the whole family and must be split before this can be recorded",
		path, family, strings.Join(parts, " "), family)
}

// ExtractProtocols reads both vendored VictoriaLogs trees — the logs pin and
// the traces module's own pin — so a bump that moves either protocol version
// shows up as a diff in inventory.generated.yaml instead of as a runtime
// "unexpected protocol version" error between a Lakehouse node and its peers.
func ExtractProtocols(vlLogsDir, vlTracesDir string) (Protocols, error) {
	logs, err := ExtractProtocol(vlLogsDir)
	if err != nil {
		return Protocols{}, fmt.Errorf("logs pin: %w", err)
	}
	traces, err := ExtractProtocol(vlTracesDir)
	if err != nil {
		return Protocols{}, fmt.Errorf("traces pin: %w", err)
	}
	return Protocols{VL: logs, VT: traces}, nil
}
