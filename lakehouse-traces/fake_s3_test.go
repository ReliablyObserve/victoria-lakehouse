package main

// fakeS3 is an in-process S3 server for the wiring and multi-instance tests
// (test-only, never shipped). It keeps objects in memory, counts every HTTP
// request it serves, and a second endpoint can share its objects
// (newFakeS3On), so several pods can run over one bucket with a request log
// each. Adapted from the internal/storage/parquets3 test harness.

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

type fakeReq struct {
	Op, Class, Key string
	In, Out        int64
	Off, Len       int64 // byte range served by a GET (a whole-object GET: 0, size)
	Status         int
	Start, End     time.Time
}

// fakeFaults are the injected faults of a bucket, shared by its endpoints.
type fakeFaults struct {
	holdDel atomic.Int64 // ns a DELETE of a .parquet object waits before it is applied
	failPut atomic.Value // string: a PUT of a key containing it answers 500 while it is set
	failDel atomic.Value // string: a DELETE of a key containing it answers 500 while it is set
}

type fakeS3 struct {
	mu       *sync.Mutex
	objs     map[string][]byte    // "bucket/key" -> data
	mods     map[string]time.Time // "bucket/key" -> last modified
	uploads  map[string]map[int][]byte
	log      []fakeReq
	srv      *httptest.Server
	latency  atomic.Int64 // ns
	f        *fakeFaults  // faults shared by every endpoint onto the same bucket
	inflight atomic.Int64
	peak     atomic.Int64
	upSeq    atomic.Int64
}

func newFakeS3() *fakeS3 {
	m := &fakeS3{f: &fakeFaults{}, mu: new(sync.Mutex), objs: map[string][]byte{}, mods: map[string]time.Time{}, uploads: map[string]map[int][]byte{}}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	return m
}

// newFakeS3On returns a second endpoint over the SAME objects as other, with its
// own request log: one bucket, several pods, each pod counted on its own.
func newFakeS3On(other *fakeS3) *fakeS3 {
	m := &fakeS3{f: other.f, mu: other.mu, objs: other.objs, mods: other.mods, uploads: other.uploads}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	return m
}

func (m *fakeS3) URL() string                { return m.srv.URL }
func (m *fakeS3) Close()                     { m.srv.Close() }
func (m *fakeS3) SetLatency(d time.Duration) { m.latency.Store(int64(d)) }

// Age makes every object d older, as listed by the bucket (the insert pod's
// segment guard and the compaction age checks read the listing's times).
func (m *fakeS3) Age(d time.Duration) {
	m.mu.Lock()
	for k := range m.mods {
		m.mods[k] = m.mods[k].Add(-d)
	}
	m.mu.Unlock()
}

// AgePrefix makes every object whose key contains substr d older, as listed.
func (m *fakeS3) AgePrefix(substr string, d time.Duration) {
	m.mu.Lock()
	for k := range m.mods {
		if strings.Contains(k, substr) {
			m.mods[k] = m.mods[k].Add(-d)
		}
	}
	m.mu.Unlock()
}

// get returns an object by bucket and key.
func (m *fakeS3) get(bucket, key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.objs[bucket+"/"+key]
	return d, ok
}

// FailDeletes makes every DELETE of a key that contains substr answer 500 until it is
// called again with an empty string.
func (m *fakeS3) FailDeletes(substr string) { m.f.failDel.Store(substr) }

// FailPuts makes every PUT of a key that contains substr answer 500 until it is
// called again with an empty string.
func (m *fakeS3) FailPuts(substr string) { m.f.failPut.Store(substr) }

// HoldDeletes makes a DELETE of a .parquet object wait d before it is applied.
func (m *fakeS3) HoldDeletes(d time.Duration) { m.f.holdDel.Store(int64(d)) }

// Put seeds an object without counting it.
func (m *fakeS3) Put(bucket, key string, data []byte) {
	m.mu.Lock()
	m.objs[bucket+"/"+key] = data
	m.mods[bucket+"/"+key] = time.Now()
	m.mu.Unlock()
}

// Requests returns a snapshot of the request log without clearing it.
func (m *fakeS3) Requests() []fakeReq {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]fakeReq(nil), m.log...)
}

// Reset returns and clears the request log.
func (m *fakeS3) Reset() []fakeReq {
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

func (m *fakeS3) record(r fakeReq) {
	m.mu.Lock()
	m.log = append(m.log, r)
	m.mu.Unlock()
}

func (m *fakeS3) handle(w http.ResponseWriter, r *http.Request) {
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
	rec := fakeReq{Key: key, Class: classify(key), In: int64(len(body)), Start: start}
	var status int
	var out int64
	if s, _ := m.f.failPut.Load().(string); r.Method == http.MethodPut && s != "" && strings.Contains(key, s) {
		rec.Op = "PUT"
		w.WriteHeader(500)
		status = 500
	} else if s, _ := m.f.failDel.Load().(string); r.Method == http.MethodDelete && s != "" && strings.Contains(key, s) {
		rec.Op = "DELETE"
		w.WriteHeader(500)
		status = 500
	} else {
		status, out = m.serve(w, r, bucket, key, q, body, &rec)
	}
	rec.Status, rec.Out, rec.End = status, out, time.Now()
	m.record(rec)
}

func (m *fakeS3) serve(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values, body []byte, rec *fakeReq) (int, int64) {
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
		m.mods[full] = time.Now()
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
				m.mods[full] = time.Now()
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
		m.mods[full] = time.Now()
		m.mu.Unlock()
		w.Header().Set("ETag", etagOf(body))
		w.WriteHeader(200)
		return 200, 0
	case r.Method == http.MethodDelete:
		rec.Op = "DELETE"
		if d := time.Duration(m.f.holdDel.Load()); d > 0 && strings.HasSuffix(key, ".parquet") {
			time.Sleep(d)
		}
		m.mu.Lock()
		delete(m.objs, full)
		delete(m.mods, full)
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

func (m *fakeS3) list(w http.ResponseWriter, bucket string, q url.Values) (int, int64) {
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
	modified := map[string]time.Time{}
	tags := map[string]string{}
	for k, d := range m.objs {
		if !strings.HasPrefix(k, bucket+"/") {
			continue
		}
		kk := strings.TrimPrefix(k, bucket+"/")
		if strings.HasPrefix(kk, prefix) {
			keys = append(keys, kk)
			sizes[kk] = len(d)
			modified[kk] = m.mods[k]
			tags[kk] = etagOf(d)
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
		fmt.Fprintf(&b, "<Contents><Key>%s</Key><Size>%d</Size><ETag>%s</ETag><LastModified>%s</LastModified><StorageClass>STANDARD</StorageClass></Contents>", xmlEsc(k), sizes[k], strings.ReplaceAll(tags[k], `"`, "&quot;"), modified[k].UTC().Format(time.RFC3339))
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

func (m *fakeS3) deleteObjects(w http.ResponseWriter, bucket string, body []byte) (int, int64) {
	var req struct {
		Objects []struct {
			Key string `xml:"Key"`
		} `xml:"Object"`
	}
	_ = xml.Unmarshal(body, &req)
	m.mu.Lock()
	for _, o := range req.Objects {
		delete(m.objs, bucket+"/"+o.Key)
		delete(m.mods, bucket+"/"+o.Key)
	}
	m.mu.Unlock()
	w.WriteHeader(200)
	_, _ = io.WriteString(w, `<DeleteResult></DeleteResult>`)
	return 200, 0
}
