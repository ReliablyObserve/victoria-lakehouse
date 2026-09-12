package vlinsert

import "strings"

func RequestHandler(path string) bool {
	switch path {
	case "/insert/jsonline":
		return true
	case "/insert/datadog/api/v1/validate", "/insert/datadog/api/v2/logs":
		return true
	}
	switch {
	case strings.HasPrefix(path, "/insert/loki/"):
		return true
	}
	return false
}
