package buffer

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func segmentsRequest(base time.Time, mode string) *http.Request {
	return httptest.NewRequest(http.MethodGet, fmt.Sprintf("/internal/buffer/query?start=%d&end=%d&mode=%s&account_id=0&project_id=0&tenant_scope=v1",
		base.UnixNano(), base.Add(time.Hour).UnixNano(), mode), nil)
}

// The response lists the nonces of the segments the rows were read from, sorted
// and comma-separated, and ParseSegments reads them back.
func TestSegmentsHeader_ListsTheNonces(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	for _, mode := range []string{"logs", "traces"} {
		for _, tc := range []struct {
			name   string
			nonces []string
			want   string
		}{
			{"no segment", nil, ""},
			{"one segment", []string{"65000000aaaabbbb"}, "65000000aaaabbbb"},
			{"several, sorted", []string{"65000000ccccdddd", "65000000aaaabbbb", "65000000bbbbcccc"}, "65000000aaaabbbb,65000000bbbbcccc,65000000ccccdddd"},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				store := &mockBufferStore{
					nonces:    tc.nonces,
					logRows:   []schema.LogRow{{TimestampUnixNano: base.UnixNano(), Body: "a"}},
					traceRows: []schema.TraceRow{{TimestampUnixNano: base.UnixNano(), TraceID: "t"}},
				}
				rec := httptest.NewRecorder()
				NewHandler(store, "").ServeHTTP(rec, segmentsRequest(base, mode))
				if rec.Code != http.StatusOK {
					t.Fatalf("status = %d", rec.Code)
				}
				got := rec.Header().Get(SegmentsHeader)
				if got != tc.want {
					t.Errorf("%s = %q, want %q", SegmentsHeader, got, tc.want)
				}
				wantList := ParseSegments(tc.want)
				if back := ParseSegments(got); !reflect.DeepEqual(back, wantList) {
					t.Errorf("ParseSegments(%q) = %v, want %v", got, back, wantList)
				}
			})
		}
	}
}

// The header is sent also when the buffer holds no row in the window: the pod
// still serves its segments, and the select pod must not read their objects.
func TestSegmentsHeader_SentWithAnEmptyAnswer(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	rec := httptest.NewRecorder()
	NewHandler(&mockBufferStore{nonces: []string{"65000000aaaabbbb"}}, "").ServeHTTP(rec, segmentsRequest(base, "logs"))
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("status %d, %d body bytes", rec.Code, rec.Body.Len())
	}
	if got := rec.Header().Get(SegmentsHeader); got != "65000000aaaabbbb" {
		t.Errorf("%s = %q", SegmentsHeader, got)
	}
}

// A buffer that cannot be read is an error response, with no header and no rows.
func TestBufferQuery_SourceErrorIs500(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	rec := httptest.NewRecorder()
	NewHandler(&mockBufferStore{err: errors.New("segment unreadable"), nonces: []string{"65000000aaaabbbb"}}, "").ServeHTTP(rec, segmentsRequest(base, "logs"))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	if got := rec.Header().Get(SegmentsHeader); got != "" {
		t.Errorf("an error response carries segments %q", got)
	}
}

func TestParseSegments(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"65000000aaaabbbb", []string{"65000000aaaabbbb"}},
		{"65000000aaaabbbb,65000000ccccdddd", []string{"65000000aaaabbbb", "65000000ccccdddd"}},
	} {
		if got := ParseSegments(tc.in); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("ParseSegments(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
