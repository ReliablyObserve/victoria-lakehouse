package vtselect

import "strings"

func RequestHandler(path string) bool {
	if path == "/select/buildinfo" {
		return true
	}
	if path == "/select/vmui" {
		return true
	}
	if strings.HasPrefix(path, "/select/vmui/") {
		return true
	}
	if path == "/select/logsql/tail" {
		return true
	}
	if strings.HasPrefix(path, "/select/jaeger/") {
		return true
	} else if strings.HasPrefix(path, "/select/tempo/") {
		return true
	}
	return false
}
