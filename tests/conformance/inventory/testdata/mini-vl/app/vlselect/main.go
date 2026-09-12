package vlselect

import "strings"

func RequestHandler(path string) bool {
	if strings.HasPrefix(path, "/select/") {
		return true
	}
	if strings.HasPrefix(path, "/select/vmalert/") {
		return true
	}
	if path == "/select/buildinfo" {
		return true
	}
	switch path {
	case "/select/logsql/query":
		return true
	case "/select/logsql/hits":
		return true
	case "/select/tenant_ids":
		return true
	case "/delete/run_task":
		return true
	}
	return false
}
