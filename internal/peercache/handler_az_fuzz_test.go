package peercache

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func FuzzHandlerStatsEndpoint(f *testing.F) {
	f.Add("us-east-1a", "", "")
	f.Add("", "", "")
	f.Add("zone-with-special-chars-!@#", "", "")
	f.Add("az-a", "secret", "secret")
	f.Add("az-b", "secret", "wrong")
	f.Add("az-c", "secret", "")
	f.Add("日本語", "", "")
	f.Add("zone\"with\"quotes", "", "")
	f.Add("zone\nwith\nnewlines", "", "")
	f.Add("a", "", "")

	f.Fuzz(func(t *testing.T, selfAZ, authKey, reqAuth string) {
		h := NewHandler(authKey, selfAZ)
		req, err := http.NewRequest("GET", "/internal/cache/stats", nil)
		if err != nil {
			return
		}
		if reqAuth != "" {
			req.Header.Set("X-Peer-Auth-Key", reqAuth)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if authKey != "" && reqAuth != authKey {
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("expected 401 with wrong auth, got %d", rec.Code)
			}
			return
		}

		if rec.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", rec.Code)
			return
		}

		// Response must be valid JSON
		var result struct {
			AZ string `json:"az"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Errorf("response not valid JSON for selfAZ=%q: %v\nbody: %s", selfAZ, err, rec.Body.String())
			return
		}

		// json.Marshal replaces invalid UTF-8 with U+FFFD, so decoded value
		// may differ from raw input for non-UTF-8 strings. Just verify valid JSON.
	})
}

func TestHandler_StatsEndpoint_SpecialCharsInAZ(t *testing.T) {
	cases := []string{
		`zone"quotes`,
		"zone\nnewline",
		"zone\ttab",
		`zone\backslash`,
		"zone/slash",
		"",
		"a",
		"日本語ゾーン",
	}

	for _, az := range cases {
		t.Run(az, func(t *testing.T) {
			h := NewHandler("", az)
			req, _ := http.NewRequest("GET", "/internal/cache/stats", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d", rec.Code)
			}

			var result struct {
				AZ string `json:"az"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
				t.Fatalf("invalid JSON for az=%q: %v\nbody: %s", az, err, rec.Body.String())
			}
			if result.AZ != az {
				t.Errorf("expected %q, got %q", az, result.AZ)
			}
		})
	}
}

func TestHandler_SetSelfAZ_ConcurrentAccess(t *testing.T) {
	h := NewHandler("", "initial")

	done := make(chan bool, 20)
	for i := 0; i < 10; i++ {
		go func(i int) {
			h.SetSelfAZ("zone-" + string(rune('a'+i)))
			done <- true
		}(i)
		go func() {
			req, _ := http.NewRequest("GET", "/internal/cache/stats", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("expected 200, got %d", rec.Code)
			}
			done <- true
		}()
	}
	for i := 0; i < 20; i++ {
		<-done
	}
}
