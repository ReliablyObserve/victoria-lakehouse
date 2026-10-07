package schema

import (
	"fmt"
	"strconv"
	"strings"
)

// StreamField is one name="value" tag of a stream name.
type StreamField struct {
	Name  string
	Value string
}

// ParseStreamFields appends the tags of a stream name such as
// `{app="x",ns="n1"}` to dst. It is VictoriaLogs' parseStreamFields
// (lib/logstorage/storage_search.go), which that package does not export: the
// tags of the streams are what stream_field_names and stream_field_values list
// (forEachStreamField), so the cold tier must read them the same way. An empty
// stream name `{}` has no tags; a malformed name is an error (upstream skips
// such a stream).
func ParseStreamFields(dst []StreamField, s string) ([]StreamField, error) {
	if len(s) == 0 || s[0] != '{' {
		return dst, fmt.Errorf("missing '{' at the beginning of stream name")
	}
	s = s[1:]
	if len(s) == 0 || s[len(s)-1] != '}' {
		return dst, fmt.Errorf("missing '}' at the end of stream name")
	}
	s = s[:len(s)-1]
	if len(s) == 0 {
		return dst, nil
	}
	for {
		n := strings.Index(s, `="`)
		if n < 0 {
			return dst, fmt.Errorf("cannot find field value in double quotes at [%s]", s)
		}
		name := s[:n]
		s = s[n+1:]
		value, off := unquoteStreamValue(s)
		if off < 0 {
			return dst, fmt.Errorf("cannot parse field value in double quotes at [%s]", s)
		}
		s = s[off:]
		dst = append(dst, StreamField{Name: name, Value: value})
		if len(s) == 0 {
			return dst, nil
		}
		if s[0] != ',' {
			return dst, fmt.Errorf("missing ',' after %s=%q", name, value)
		}
		s = s[1:]
	}
}

// unquoteStreamValue unquotes the Go-quoted string at the start of s and
// returns it with the number of bytes it took, or -1.
func unquoteStreamValue(s string) (string, int) {
	if len(s) == 0 || s[0] != '"' {
		return "", -1
	}
	for i := 1; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case '"':
			v, err := strconv.Unquote(s[:i+1])
			if err != nil {
				return "", -1
			}
			return v, i + 1
		}
	}
	return "", -1
}
