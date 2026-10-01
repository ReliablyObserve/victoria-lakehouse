package parquets3

// countS3 — in-process S3 server for the S3 evidence harnesses (test-only, never shipped).
// Copy into the package under test as zz_s3count_test.go and replace "package PKG".
//
// - Injects latency as a sleep before the response headers: one round trip of
//   time-to-first-byte per request (SetLatency at any time).
// - Counts EVERY HTTP request at the server: GET, ranged GET, HEAD, PUT
//   (incl. If-None-Match / If-Match), DELETE, DeleteObjects (POST ?delete),
//   ListObjectsV2 (prefix, delimiter, max-keys, continuation-token, start-after),
//   multipart (create / upload part / complete / abort).
// - Records per request: op, key class, bytes in/out, status, start, end.
// - Report(): totals by op and class, bytes, peak in-flight, and the sequential
//   round-trip chain (longest chain of requests each starting after the previous
//   one ended).

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type countReq struct {
	Op, Class, Key string
	In, Out        int64
	Off, Len       int64 // byte range served by a GET (a whole-object GET: 0, size)
	Status         int
	Start, End     time.Time
}

type countS3 struct {
	mu       sync.Mutex
	objs     map[string][]byte // "bucket/key" -> data
	uploads  map[string]map[int][]byte
	log      []countReq
	srv      *httptest.Server
	latency  atomic.Int64 // ns
	inflight atomic.Int64
	peak     atomic.Int64
	upSeq    atomic.Int64
}

func newCountS3() *countS3 {
	m := &countS3{objs: map[string][]byte{}, uploads: map[string]map[int][]byte{}}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	return m
}

func (m *countS3) URL() string                { return m.srv.URL }
func (m *countS3) Close()                     { m.srv.Close() }
func (m *countS3) SetLatency(d time.Duration) { m.latency.Store(int64(d)) }

// Put seeds an object without counting it.
func (m *countS3) Put(bucket, key string, data []byte) {
	m.mu.Lock()
	m.objs[bucket+"/"+key] = data
	m.mu.Unlock()
}

// Requests returns a snapshot of the request log without clearing it.
func (m *countS3) Requests() []countReq {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]countReq(nil), m.log...)
}

// Reset returns and clears the request log.
func (m *countS3) Reset() []countReq {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.log
	m.log = nil
	m.peak.Store(0)
	return l
}

// classify maps a key to a request class; adjust to the layout under test.
func classify(key string) string {
	switch {
	case key == "":
		return "bucket"
	case strings.Contains(key, "_pmeta") || strings.Contains(key, "pmeta/"):
		return "pmeta"
	case strings.HasSuffix(key, ".parquet"):
		return "data"
	case strings.Contains(key, "manifest") || strings.Contains(key, "_meta/"):
		return "manifest"
	case strings.Contains(key, "bloom") || strings.HasSuffix(key, ".json"):
		return "sidecar"
	case strings.Contains(key, "probe") || strings.Contains(key, "health"):
		return "probe"
	case strings.Contains(key, "tombstone") || strings.Contains(key, "delete"):
		return "tombstone"
	}
	return "other"
}

func (m *countS3) record(r countReq) {
	m.mu.Lock()
	m.log = append(m.log, r)
	m.mu.Unlock()
}

func (m *countS3) handle(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	cur := m.inflight.Add(1)
	defer m.inflight.Add(-1)
	for {
		p := m.peak.Load()
		if cur <= p || m.peak.CompareAndSwap(p, cur) {
			break
		}
	}
	if d := time.Duration(m.latency.Load()); d > 0 {
		time.Sleep(d)
	}
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()
	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	key, _ = url.PathUnescape(key)
	q := r.URL.Query()
	rec := countReq{Key: key, Class: classify(key), In: int64(len(body)), Start: start}
	status, out := m.serve(w, r, bucket, key, q, body, &rec)
	rec.Status, rec.Out, rec.End = status, out, time.Now()
	m.record(rec)
}

func (m *countS3) serve(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values, body []byte, rec *countReq) (int, int64) {
	full := bucket + "/" + key
	switch {
	case r.Method == http.MethodGet && key == "" && q.Get("list-type") == "2":
		rec.Op, rec.Class = "LIST", classify(q.Get("prefix"))
		return m.list(w, bucket, q)
	case r.Method == http.MethodGet && key == "":
		rec.Op = "LIST_V1"
		return m.list(w, bucket, q)
	case r.Method == http.MethodHead && key == "":
		rec.Op = "HEAD_BUCKET"
		w.WriteHeader(200)
		return 200, 0
	case r.Method == http.MethodPost && q.Has("delete"):
		rec.Op = "DELETE_OBJECTS"
		return m.deleteObjects(w, bucket, body)
	case r.Method == http.MethodPost && q.Has("uploads"):
		rec.Op = "MPU_CREATE"
		id := strconv.FormatInt(m.upSeq.Add(1), 10)
		m.mu.Lock()
		m.uploads[id] = map[int][]byte{}
		m.mu.Unlock()
		b := fmt.Sprintf(`<InitiateMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, bucket, key, id)
		w.WriteHeader(200)
		_, _ = io.WriteString(w, b)
		return 200, int64(len(b))
	case r.Method == http.MethodPut && q.Get("uploadId") != "":
		rec.Op = "MPU_PART"
		n, _ := strconv.Atoi(q.Get("partNumber"))
		m.mu.Lock()
		m.uploads[q.Get("uploadId")][n] = body
		m.mu.Unlock()
		w.Header().Set("ETag", fmt.Sprintf(`"p%d"`, n))
		w.WriteHeader(200)
		return 200, 0
	case r.Method == http.MethodPost && q.Get("uploadId") != "":
		rec.Op = "MPU_COMPLETE"
		m.mu.Lock()
		parts := m.uploads[q.Get("uploadId")]
		var nums []int
		for n := range parts {
			nums = append(nums, n)
		}
		sort.Ints(nums)
		var buf bytes.Buffer
		for _, n := range nums {
			buf.Write(parts[n])
		}
		if r.Header.Get("If-None-Match") == "*" {
			if _, ok := m.objs[full]; ok {
				m.mu.Unlock()
				w.WriteHeader(412)
				return 412, 0
			}
		}
		m.objs[full] = buf.Bytes()
		delete(m.uploads, q.Get("uploadId"))
		m.mu.Unlock()
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `<CompleteMultipartUploadResult><ETag>"x"</ETag></CompleteMultipartUploadResult>`)
		return 200, 0
	case r.Method == http.MethodDelete && q.Get("uploadId") != "":
		rec.Op = "MPU_ABORT"
		w.WriteHeader(204)
		return 204, 0
	case r.Method == http.MethodPut:
		rec.Op = "PUT"
		if r.Header.Get("x-amz-copy-source") != "" {
			rec.Op = "COPY"
			src, _ := url.PathUnescape(strings.TrimPrefix(r.Header.Get("x-amz-copy-source"), "/"))
			m.mu.Lock()
			d, ok := m.objs[src]
			if ok {
				m.objs[full] = d
			}
			m.mu.Unlock()
			if !ok {
				w.WriteHeader(404)
				return 404, 0
			}
			w.WriteHeader(200)
			_, _ = io.WriteString(w, `<CopyObjectResult><ETag>"x"</ETag></CopyObjectResult>`)
			return 200, 0
		}
		m.mu.Lock()
		_, exists := m.objs[full]
		if r.Header.Get("If-None-Match") == "*" && exists {
			m.mu.Unlock()
			w.WriteHeader(412)
			return 412, 0
		}
		if im := r.Header.Get("If-Match"); im != "" && (!exists || im != etagOf(m.objs[full])) {
			m.mu.Unlock()
			w.WriteHeader(412)
			return 412, 0
		}
		m.objs[full] = body
		m.mu.Unlock()
		w.Header().Set("ETag", etagOf(body))
		w.WriteHeader(200)
		return 200, 0
	case r.Method == http.MethodDelete:
		rec.Op = "DELETE"
		m.mu.Lock()
		delete(m.objs, full)
		m.mu.Unlock()
		w.WriteHeader(204)
		return 204, 0
	case r.Method == http.MethodHead || r.Method == http.MethodGet:
		m.mu.Lock()
		data, ok := m.objs[full]
		m.mu.Unlock()
		rec.Op = "GET"
		if r.Method == http.MethodHead {
			rec.Op = "HEAD"
		}
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(404)
			if r.Method == http.MethodGet {
				_, _ = io.WriteString(w, `<?xml version="1.0"?><Error><Code>NoSuchKey</Code></Error>`)
			}
			return 404, 0
		}
		if inm := r.Header.Get("If-None-Match"); inm != "" && inm == etagOf(data) {
			w.WriteHeader(304)
			return 304, 0
		}
		w.Header().Set("ETag", etagOf(data))
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		off, end := int64(0), int64(len(data))-1
		code := 200
		if rh := r.Header.Get("Range"); strings.HasPrefix(rh, "bytes=") && r.Method == http.MethodGet {
			rec.Op = "GET_RANGE"
			a, b, _ := strings.Cut(strings.TrimPrefix(rh, "bytes="), "-")
			if a == "" { // suffix range
				n, _ := strconv.ParseInt(b, 10, 64)
				off = int64(len(data)) - n
				if off < 0 {
					off = 0
				}
			} else {
				off, _ = strconv.ParseInt(a, 10, 64)
				if b != "" {
					end, _ = strconv.ParseInt(b, 10, 64)
				}
			}
			if end >= int64(len(data)) {
				end = int64(len(data)) - 1
			}
			if off > end {
				w.WriteHeader(416)
				return 416, 0
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, end, len(data)))
			code = 206
		}
		w.Header().Set("Content-Length", strconv.FormatInt(end-off+1, 10))
		w.WriteHeader(code)
		if r.Method == http.MethodGet {
			rec.Off, rec.Len = off, end-off+1
			_, _ = w.Write(data[off : end+1])
			return code, end - off + 1
		}
		return code, 0
	}
	rec.Op = "UNSUPPORTED_" + r.Method
	w.WriteHeader(501)
	return 501, 0
}

func etagOf(b []byte) string {
	h := uint32(2166136261)
	for _, c := range b {
		h ^= uint32(c)
		h *= 16777619
	}
	return fmt.Sprintf(`"%08x-%d"`, h, len(b))
}

func (m *countS3) list(w http.ResponseWriter, bucket string, q url.Values) (int, int64) {
	prefix, delim := q.Get("prefix"), q.Get("delimiter")
	maxKeys := 1000
	if v, err := strconv.Atoi(q.Get("max-keys")); err == nil && v > 0 && v < 1000 {
		maxKeys = v
	}
	after := q.Get("continuation-token")
	if after == "" {
		after = q.Get("start-after")
	}
	m.mu.Lock()
	var keys []string
	sizes := map[string]int{}
	for k, d := range m.objs {
		if !strings.HasPrefix(k, bucket+"/") {
			continue
		}
		kk := strings.TrimPrefix(k, bucket+"/")
		if strings.HasPrefix(kk, prefix) {
			keys = append(keys, kk)
			sizes[kk] = len(d)
		}
	}
	m.mu.Unlock()
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>`)
	fmt.Fprintf(&b, "<Name>%s</Name><Prefix>%s</Prefix>", bucket, xmlEsc(prefix))
	n := 0
	seenCP := map[string]bool{}
	truncated := false
	last := ""
	for _, k := range keys {
		if after != "" && k <= after {
			continue
		}
		if delim != "" {
			rest := strings.TrimPrefix(k, prefix)
			if i := strings.Index(rest, delim); i >= 0 {
				cp := prefix + rest[:i+len(delim)]
				if seenCP[cp] {
					last = k
					continue
				}
				if n >= maxKeys {
					truncated = true
					break
				}
				seenCP[cp] = true
				fmt.Fprintf(&b, "<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>", xmlEsc(cp))
				n++
				last = k
				continue
			}
		}
		if n >= maxKeys {
			truncated = true
			break
		}
		fmt.Fprintf(&b, "<Contents><Key>%s</Key><Size>%d</Size><ETag>\"x\"</ETag><LastModified>%s</LastModified><StorageClass>STANDARD</StorageClass></Contents>", xmlEsc(k), sizes[k], time.Now().UTC().Format(time.RFC3339))
		n++
		last = k
	}
	fmt.Fprintf(&b, "<KeyCount>%d</KeyCount><MaxKeys>%d</MaxKeys><IsTruncated>%t</IsTruncated>", n, maxKeys, truncated)
	if truncated {
		fmt.Fprintf(&b, "<NextContinuationToken>%s</NextContinuationToken>", xmlEsc(last))
	}
	b.WriteString(`</ListBucketResult>`)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(200)
	_, _ = io.WriteString(w, b.String())
	return 200, int64(b.Len())
}

func xmlEsc(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

func (m *countS3) deleteObjects(w http.ResponseWriter, bucket string, body []byte) (int, int64) {
	var req struct {
		Objects []struct {
			Key string `xml:"Key"`
		} `xml:"Object"`
	}
	_ = xml.Unmarshal(body, &req)
	m.mu.Lock()
	for _, o := range req.Objects {
		delete(m.objs, bucket+"/"+o.Key)
	}
	m.mu.Unlock()
	w.WriteHeader(200)
	_, _ = io.WriteString(w, `<DeleteResult></DeleteResult>`)
	return 200, 0
}

// countReport summarises a request log.
type countReport struct {
	Requests  int
	ByOp      map[string]int
	ByClass   map[string]int
	ByOpClass map[string]int
	BytesOut  int64
	BytesIn   int64
	Chain     int   // sequential round trips on the critical path
	Peak      int64 // peak in-flight requests (since the last Reset)
	SpanMs    float64
}

func (m *countS3) Report(l []countReq) countReport {
	r := countReport{ByOp: map[string]int{}, ByClass: map[string]int{}, ByOpClass: map[string]int{}, Peak: m.peak.Load()}
	for _, q := range l {
		r.Requests++
		r.ByOp[q.Op]++
		r.ByClass[q.Class]++
		r.ByOpClass[q.Op+":"+q.Class]++
		r.BytesOut += q.Out
		r.BytesIn += q.In
	}
	r.Chain = chainOf(l)
	if len(l) > 0 {
		first, last := l[0].Start, l[0].End
		for _, q := range l {
			if q.Start.Before(first) {
				first = q.Start
			}
			if q.End.After(last) {
				last = q.End
			}
		}
		r.SpanMs = float64(last.Sub(first).Microseconds()) / 1000
	}
	return r
}

func chainOf(l []countReq) int {
	s := append([]countReq(nil), l...)
	sort.Slice(s, func(i, j int) bool { return s[i].Start.Before(s[j].Start) })
	depth := make([]int, len(s))
	best := 0
	for i := range s {
		depth[i] = 1
		for j := 0; j < i; j++ {
			if !s[j].End.After(s[i].Start) && depth[j]+1 > depth[i] {
				depth[i] = depth[j] + 1
			}
		}
		if depth[i] > best {
			best = depth[i]
		}
	}
	return best
}

// AWS S3 Standard list prices (us-east-1), per request.
const (
	pricePUTClass = 0.005 / 1000  // PUT, COPY, POST, LIST
	priceGETClass = 0.0004 / 1000 // GET, HEAD
)

// Dollars prices a report: LIST/PUT/COPY/POST/MPU at PUT-class, GET/HEAD at GET-class, DELETE free.
func (r countReport) Dollars() float64 {
	var d float64
	for op, n := range r.ByOp {
		switch {
		case strings.HasPrefix(op, "GET") || strings.HasPrefix(op, "HEAD"):
			d += float64(n) * priceGETClass
		case op == "DELETE" || op == "MPU_ABORT":
		default:
			d += float64(n) * pricePUTClass
		}
	}
	return d
}
