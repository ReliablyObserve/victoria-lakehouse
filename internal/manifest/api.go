package manifest

import (
	"encoding/json"
	"net/http"
)

type RangeResponse struct {
	MinTime    int64  `json:"minTime"`
	MaxTime    int64  `json:"maxTime"`
	MinDate    string `json:"minDate"`
	MaxDate    string `json:"maxDate"`
	TotalFiles int    `json:"totalFiles"`
	TotalBytes int64  `json:"totalBytes"`
}

func (m *Manifest) RangeHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		m.mu.RLock()
		resp := RangeResponse{
			MinTime:    m.minTime.UnixNano(),
			MaxTime:    m.maxTime.UnixNano(),
			MinDate:    m.minTime.Format("2006-01-02"),
			MaxDate:    m.maxTime.Format("2006-01-02"),
			TotalFiles: m.totalFiles,
			TotalBytes: m.totalBytes,
		}
		m.mu.RUnlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}
