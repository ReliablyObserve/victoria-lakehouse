// Package s3compat is an S3-backend compatibility harness for Lakehouse.
//
// It builds the S3 client with the production constructor
// (internal/s3reader.NewClientPool, same aws-sdk-go-v2 pin as the binaries)
// and exercises the operations Lakehouse issues in production (TestProd*),
// plus operations that Lakehouse does not issue today but that other tooling
// or future features may rely on (TestExtra*).
//
// Point it at any endpoint:
//
//	go test ./tests/s3compat -count=1 -v -args \
//	    -endpoint=http://127.0.0.1:9000 -key=AK -secret=SK
//
// or via S3COMPAT_ENDPOINT / S3COMPAT_KEY / S3COMPAT_SECRET / S3COMPAT_BUCKET /
// S3COMPAT_REGION. Without an endpoint every test is skipped.
package s3compat

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

var (
	fEndpoint = flag.String("endpoint", envOr("S3COMPAT_ENDPOINT", ""), "S3 endpoint URL (http[s]://host:port)")
	fKey      = flag.String("key", envOr("S3COMPAT_KEY", ""), "access key")
	fSecret   = flag.String("secret", envOr("S3COMPAT_SECRET", ""), "secret key")
	fBucket   = flag.String("bucket", envOr("S3COMPAT_BUCKET", "s3compat"), "bucket (created if missing)")
	fRegion   = flag.String("region", envOr("S3COMPAT_REGION", "us-east-1"), "region")
	fPathSt   = flag.Bool("pathstyle", true, "force path-style addressing (production default for custom endpoints)")
	fLarge    = flag.Int("large-mib", 200, "size of the large-object test in MiB (0 to skip)")
	fIters    = flag.Int("consistency-iters", 200, "iterations for read/list-after-write tests")
)

var (
	setupOnce sync.Once
	setupErr  error
	pool      *s3reader.ClientPool
	raw       *s3.Client
	runID     = fmt.Sprintf("run-%d", time.Now().UnixNano())
)

func setup(t *testing.T) {
	t.Helper()
	if *fEndpoint == "" {
		t.Skip("no -endpoint / S3COMPAT_ENDPOINT given")
	}
	setupOnce.Do(func() {
		p, err := newPool(*fKey, *fSecret)
		if err != nil {
			setupErr = err
			return
		}
		pool = p
		raw = p.S3Client()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		_, err = raw.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(*fBucket)})
		if err != nil {
			var ae smithy.APIError
			if errors.As(err, &ae) && (ae.ErrorCode() == "BucketAlreadyOwnedByYou" || ae.ErrorCode() == "BucketAlreadyExists") {
				err = nil
			}
		}
		setupErr = err
	})
	if setupErr != nil {
		t.Fatalf("setup (create bucket %q): %v", *fBucket, setupErr)
	}
}

func newPool(key, secret string) (*s3reader.ClientPool, error) {
	return s3reader.NewClientPool(context.Background(), &config.S3Config{
		Bucket:         *fBucket,
		Region:         *fRegion,
		Endpoint:       *fEndpoint,
		AccessKey:      key,
		SecretKey:      secret,
		ForcePathStyle: *fPathSt,
		MaxConnections: 32,
	})
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// pfx returns a per-test unique key prefix so tests never interfere.
func pfx(t *testing.T) string {
	return runID + "/" + strings.ReplaceAll(t.Name(), "/", "_") + "/"
}

func status(err error) int {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

func code(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

func payload(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func listAll(ctx context.Context, in *s3.ListObjectsV2Input) (keys, prefixes []string, err error) {
	in.Bucket = aws.String(*fBucket)
	pg := s3.NewListObjectsV2Paginator(raw, in)
	for pg.HasMorePages() {
		page, e := pg.NextPage(ctx)
		if e != nil {
			return nil, nil, e
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
		for _, p := range page.CommonPrefixes {
			prefixes = append(prefixes, aws.ToString(p.Prefix))
		}
	}
	return
}

// ---------------------------------------------------------------- PROD ----

func TestProd_PutGetRoundtrip(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	key := pfx(t) + "obj.parquet"
	data := payload(1 << 20)
	if err := pool.Upload(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	got, err := pool.Download(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("body mismatch: got %d bytes want %d", len(got), len(data))
	}
}

func TestProd_ZeroByteObject(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	key := pfx(t) + "empty"
	if err := pool.Upload(ctx, key, nil); err != nil {
		t.Fatal(err)
	}
	h, err := raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToInt64(h.ContentLength) != 0 {
		t.Fatalf("content-length %d want 0", aws.ToInt64(h.ContentLength))
	}
	got, err := pool.Download(ctx, key)
	if err != nil || len(got) != 0 {
		t.Fatalf("download: len=%d err=%v", len(got), err)
	}
}

func TestProd_HeadMissing404(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	ok, err := pool.Exists(ctx, pfx(t)+"nope")
	if err != nil || ok {
		t.Fatalf("Exists(missing) = %v, %v; want false,nil", ok, err)
	}
	_, err = raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(pfx(t) + "nope")})
	if status(err) != 404 {
		t.Fatalf("HEAD missing: status=%d err=%v; want 404", status(err), err)
	}
}

func TestProd_GetMissingNoSuchKey(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	_, err := raw.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(pfx(t) + "nope")})
	var nsk *types.NoSuchKey
	if !errors.As(err, &nsk) {
		t.Fatalf("GET missing: want NoSuchKey, got status=%d code=%q err=%v", status(err), code(err), err)
	}
}

func TestProd_HeadBadCreds403(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	bad, err := newPool(*fKey, "definitely-wrong-secret")
	if err != nil {
		t.Fatal(err)
	}
	// retryS3 / SDK retry may take a bit; bound it.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err = bad.S3Client().HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(*fBucket), Key: aws.String("x")})
	if status(err) != 403 {
		t.Fatalf("HEAD with bad secret: status=%d err=%v; want 403", status(err), err)
	}
	_, err = bad.S3Client().GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(*fBucket), Key: aws.String("x")})
	if status(err) != 403 {
		t.Fatalf("GET with bad secret: status=%d code=%q err=%v; want 403", status(err), code(err), err)
	}
}

func TestProd_RangeReads(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	key := pfx(t) + "range.bin"
	data := payload(100_000)
	if err := pool.Upload(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	get := func(rng string) ([]byte, *s3.GetObjectOutput, error) {
		o, err := raw.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(key), Range: aws.String(rng)})
		if err != nil {
			return nil, nil, err
		}
		defer o.Body.Close()
		b, err := io.ReadAll(o.Body)
		return b, o, err
	}
	cases := []struct {
		rng  string
		want []byte
	}{
		{"bytes=0-0", data[0:1]},
		{"bytes=10-99", data[10:100]},
		{"bytes=99990-99999", data[99990:]},
		{"bytes=-1000", data[len(data)-1000:]}, // suffix: parquet footer read
		{"bytes=-8", data[len(data)-8:]},       // parquet trailer magic+len
		{"bytes=99000-", data[99000:]},         // open ended
		{"bytes=99990-200000", data[99990:]},   // end beyond EOF is clamped
		{"bytes=0-99999", data},                // whole object as range
		{"bytes=-200000", data},                // suffix larger than object
	}
	for _, c := range cases {
		t.Run(c.rng, func(t *testing.T) {
			b, o, err := get(c.rng)
			if err != nil {
				t.Fatalf("%s: %v", c.rng, err)
			}
			if !bytes.Equal(b, c.want) {
				t.Fatalf("%s: body mismatch (got %d bytes want %d)", c.rng, len(b), len(c.want))
			}
			if o.ContentRange == nil {
				t.Errorf("%s: no Content-Range header", c.rng)
			}
		})
	}
	t.Run("unsatisfiable-416", func(t *testing.T) {
		_, _, err := get("bytes=500000-600000")
		if status(err) != 416 {
			t.Fatalf("status=%d code=%q err=%v; want 416", status(err), code(err), err)
		}
	})
	t.Run("pool.DownloadRange", func(t *testing.T) {
		b, err := pool.DownloadRange(ctx, key, 1234, 5678)
		if err != nil || !bytes.Equal(b, data[1234:1234+5678]) {
			t.Fatalf("DownloadRange: err=%v len=%d", err, len(b))
		}
	})
	t.Run("ReaderAt", func(t *testing.T) {
		r := pool.NewReaderAt(ctx, key, int64(len(data)))
		buf := make([]byte, 4096)
		n, err := r.ReadAt(buf, 50_000)
		if err != nil || n != 4096 || !bytes.Equal(buf, data[50_000:54_096]) {
			t.Fatalf("ReadAt: n=%d err=%v", n, err)
		}
		// tail read shorter than buffer -> io.EOF with partial data
		buf = make([]byte, 4096)
		n, err = r.ReadAt(buf, int64(len(data))-100)
		if n != 100 || (err != nil && err != io.EOF) {
			t.Fatalf("tail ReadAt: n=%d err=%v", n, err)
		}
	})
}

func TestProd_DeleteObject(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	key := pfx(t) + "d"
	if err := pool.Upload(ctx, key, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := pool.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if ok, err := pool.Exists(ctx, key); ok || err != nil {
		t.Fatalf("Exists after delete = %v,%v", ok, err)
	}
	// Retention/compaction delete may race: deleting a missing key must succeed (S3: 204).
	if err := pool.Delete(ctx, key); err != nil {
		t.Fatalf("delete of missing key must be idempotent: %v", err)
	}
}

func TestProd_CopyObject(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	data := payload(300_000)
	// Lakehouse builds CopySource as url.PathEscape(bucket + "/" + key), so
	// the whole thing (including the bucket/key separator) is sent as one
	// escaped segment. Probe each special character separately.
	for _, name := range []string{
		"plain.parquet", "date=2026-09-30/hour=01/a b+c.parquet",
		"tenant=0/x=y/f.parquet", "col:on/f.parquet", "com,ma/f.parquet", "semi;colon/f.parquet",
		"at@/f.parquet", "dollar$/f.parquet", "bang!/f.parquet", "quote'/f.parquet", "tilde~/f.parquet",
		"pct%25/f.parquet", "uni/żółć.parquet", "q?uery/f.parquet", "amp&/f.parquet",
	} {
		t.Run(name, func(t *testing.T) {
			src := pfx(t) + "src/" + name
			dst := pfx(t) + "dst/" + name
			if err := pool.Upload(ctx, src, data); err != nil {
				t.Fatal(err)
			}
			if err := pool.Copy(ctx, "", src, "", dst); err != nil {
				t.Fatalf("Copy: %v", err)
			}
			got, err := pool.Download(ctx, dst)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("copy dst mismatch: err=%v len=%d", err, len(got))
			}
		})
	}
}

func TestProd_ListPrefixAndDelimiter(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	base := pfx(t)
	// logs/<tenant>/date=YYYY-MM-DD/hour=HH/file.parquet
	var want []string
	for _, tn := range []string{"0_0", "1_2"} {
		for _, d := range []string{"2026-09-29", "2026-09-30"} {
			for _, h := range []string{"00", "13"} {
				k := fmt.Sprintf("%slogs/%s/date=%s/hour=%s/f.parquet", base, tn, d, h)
				want = append(want, k)
				if err := pool.Upload(ctx, k, []byte("p")); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	// a sibling with a common string prefix but different dir
	if err := pool.Upload(ctx, base+"logs2/x.parquet", []byte("p")); err != nil {
		t.Fatal(err)
	}
	t.Run("flat-prefix", func(t *testing.T) {
		keys, _, err := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(base + "logs/")})
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(keys)
		sort.Strings(want)
		if strings.Join(keys, "\n") != strings.Join(want, "\n") {
			t.Fatalf("flat list mismatch: got %d keys want %d\n%v", len(keys), len(want), keys)
		}
	})
	t.Run("delimiter-level1", func(t *testing.T) {
		keys, cps, err := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(base + "logs/"), Delimiter: aws.String("/")})
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 0 {
			t.Errorf("unexpected keys at level 1: %v", keys)
		}
		exp := []string{base + "logs/0_0/", base + "logs/1_2/"}
		sort.Strings(cps)
		if strings.Join(cps, ",") != strings.Join(exp, ",") {
			t.Fatalf("CommonPrefixes = %v want %v", cps, exp)
		}
	})
	t.Run("delimiter-depth3", func(t *testing.T) {
		_, cps, err := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(base + "logs/0_0/date=2026-09-30/"), Delimiter: aws.String("/")})
		if err != nil {
			t.Fatal(err)
		}
		exp := []string{base + "logs/0_0/date=2026-09-30/hour=00/", base + "logs/0_0/date=2026-09-30/hour=13/"}
		sort.Strings(cps)
		if strings.Join(cps, ",") != strings.Join(exp, ",") {
			t.Fatalf("CommonPrefixes = %v want %v", cps, exp)
		}
	})
	t.Run("prefix-without-trailing-slash", func(t *testing.T) {
		keys, _, err := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(base + "logs")})
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != len(want)+1 {
			t.Fatalf("got %d keys want %d", len(keys), len(want)+1)
		}
	})
	t.Run("empty-prefix-result", func(t *testing.T) {
		keys, cps, err := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(base + "does-not-exist/")})
		if err != nil || len(keys)+len(cps) != 0 {
			t.Fatalf("keys=%v cps=%v err=%v", keys, cps, err)
		}
	})
	t.Run("lexicographic-order", func(t *testing.T) {
		keys, _, err := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(base + "logs/")})
		if err != nil {
			t.Fatal(err)
		}
		if !sort.StringsAreSorted(keys) {
			t.Fatalf("list not in UTF-8 binary order: %v", keys)
		}
	})
}

func TestProd_ListPagination1100(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	base := pfx(t)
	const n = 1100
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	var mu sync.Mutex
	var firstErr error
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := pool.Upload(ctx, fmt.Sprintf("%sk%05d.parquet", base, i), []byte("x")); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	t.Run("paginator-all", func(t *testing.T) {
		keys, _, err := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(base)})
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != n {
			t.Fatalf("got %d keys want %d", len(keys), n)
		}
		seen := map[string]bool{}
		for _, k := range keys {
			if seen[k] {
				t.Fatalf("duplicate key across pages: %s", k)
			}
			seen[k] = true
		}
		if !sort.StringsAreSorted(keys) {
			t.Fatal("keys not sorted across pages")
		}
	})
	t.Run("default-page-size-and-truncation", func(t *testing.T) {
		o, err := raw.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(*fBucket), Prefix: aws.String(base)})
		if err != nil {
			t.Fatal(err)
		}
		if len(o.Contents) != 1000 || !aws.ToBool(o.IsTruncated) || o.NextContinuationToken == nil {
			t.Fatalf("first page: n=%d truncated=%v token=%v; want 1000/true/token", len(o.Contents), aws.ToBool(o.IsTruncated), o.NextContinuationToken != nil)
		}
	})
	t.Run("maxkeys-and-token", func(t *testing.T) {
		o, err := raw.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(*fBucket), Prefix: aws.String(base), MaxKeys: aws.Int32(7)})
		if err != nil {
			t.Fatal(err)
		}
		if len(o.Contents) != 7 || aws.ToInt32(o.KeyCount) != 7 {
			t.Fatalf("n=%d keycount=%d want 7", len(o.Contents), aws.ToInt32(o.KeyCount))
		}
		o2, err := raw.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(*fBucket), Prefix: aws.String(base), MaxKeys: aws.Int32(7), ContinuationToken: o.NextContinuationToken})
		if err != nil {
			t.Fatal(err)
		}
		if aws.ToString(o2.Contents[0].Key) != fmt.Sprintf("%sk%05d.parquet", base, 7) {
			t.Fatalf("token page starts at %s", aws.ToString(o2.Contents[0].Key))
		}
	})
	t.Run("start-after", func(t *testing.T) {
		sa := fmt.Sprintf("%sk%05d.parquet", base, 1049)
		keys, _, err := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(base), StartAfter: aws.String(sa)})
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 50 || keys[0] != fmt.Sprintf("%sk%05d.parquet", base, 1050) {
			t.Fatalf("got %d keys, first=%v; want 50 starting k01050", len(keys), keys[:min(1, len(keys))])
		}
	})
	t.Run("delete-1100-individually-then-list-empty", func(t *testing.T) {
		keys, _, _ := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(base)})
		for _, k := range keys {
			if err := pool.Delete(ctx, k); err != nil {
				t.Fatal(err)
			}
		}
		left, _, err := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(base)})
		if err != nil || len(left) != 0 {
			t.Fatalf("after deleting all: %d keys left, err=%v", len(left), err)
		}
	})
}

func TestProd_SpecialCharKeys(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	base := pfx(t)
	names := []string{
		"date=2026-09-30/hour=07/part-0001.parquet",
		"tenant=acme/stream=a=b/x.parquet",
		"a b/c d.parquet",
		"plus+sign/x+y.parquet",
		"percent%20lit/x%2Fy.parquet",
		"unicode/żółć-日本語.parquet",
		"amp&semi;/q?x=1#frag.parquet",
		"star*paren(1)/[br]{cu}.parquet",
		"dot..dot/x..y.parquet",
		"tilde~/at@/colon:/comma,/semi;/dollar$/bang!/quote'/x.parquet",
		"_pmeta/bundle.bin",
		"trailing-dot./x.",
	}
	for _, n := range names {
		t.Run(n, func(t *testing.T) {
			k := base + n
			data := []byte("data-for-" + n)
			if err := pool.Upload(ctx, k, data); err != nil {
				t.Fatalf("PUT: %v", err)
			}
			got, err := pool.Download(ctx, k)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("GET: err=%v got=%q", err, got)
			}
			if ok, err := pool.Exists(ctx, k); !ok || err != nil {
				t.Fatalf("HEAD: ok=%v err=%v", ok, err)
			}
			keys, _, err := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(k)})
			if err != nil || len(keys) != 1 || keys[0] != k {
				t.Fatalf("LIST exact: keys=%q err=%v (key returned must round-trip unchanged)", keys, err)
			}
			if err := pool.Delete(ctx, k); err != nil {
				t.Fatalf("DELETE: %v", err)
			}
			if ok, _ := pool.Exists(ctx, k); ok {
				t.Fatal("still exists after delete")
			}
		})
	}
}

func TestProd_ETagEquality(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	key := pfx(t) + "etag"
	data := payload(123_457)
	p, err := raw.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(key), Body: bytes.NewReader(data)})
	if err != nil {
		t.Fatal(err)
	}
	h, err := raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	g, err := raw.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	g.Body.Close()
	l, _, _ := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(key)})
	lo, err := raw.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(*fBucket), Prefix: aws.String(key)})
	if err != nil || len(l) != 1 {
		t.Fatalf("list: %v %v", l, err)
	}
	sum := md5.Sum(data)
	wantETag := `"` + hex.EncodeToString(sum[:]) + `"`
	pe, he, ge, le := aws.ToString(p.ETag), aws.ToString(h.ETag), aws.ToString(g.ETag), aws.ToString(lo.Contents[0].ETag)
	if pe != he || he != ge || ge != le {
		t.Fatalf("ETag differs across ops: put=%s head=%s get=%s list=%s", pe, he, ge, le)
	}
	if pe != wantETag {
		t.Errorf("single-part ETag %s != MD5 of body %s (clients assuming md5 etags break)", pe, wantETag)
	}
	// HEAD LastModified must be present (orphan-sweep TTL gate reads it)
	if h.LastModified == nil || h.LastModified.IsZero() || time.Since(*h.LastModified) > time.Hour || time.Until(*h.LastModified) > time.Hour {
		t.Errorf("HEAD LastModified implausible: %v", h.LastModified)
	}
	if lo.Contents[0].LastModified == nil || aws.ToInt64(lo.Contents[0].Size) != int64(len(data)) {
		t.Errorf("list entry size/lastmodified wrong: %+v", lo.Contents[0])
	}
}

func TestProd_ReadAfterWrite(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	base := pfx(t)
	for i := 0; i < *fIters; i++ {
		k := fmt.Sprintf("%sraw-%04d", base, i)
		data := []byte(fmt.Sprintf("v-%d", i))
		if err := pool.Upload(ctx, k, data); err != nil {
			t.Fatalf("iter %d put: %v", i, err)
		}
		got, err := pool.Download(ctx, k)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("iter %d: GET right after PUT failed: err=%v got=%q", i, err, got)
		}
		if ok, err := pool.Exists(ctx, k); !ok || err != nil {
			t.Fatalf("iter %d: HEAD right after PUT failed: %v %v", i, ok, err)
		}
	}
}

func TestProd_ListAfterWrite(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	base := pfx(t)
	for i := 0; i < *fIters; i++ {
		k := fmt.Sprintf("%slaw-%04d", base, i)
		if err := pool.Upload(ctx, k, []byte("x")); err != nil {
			t.Fatalf("iter %d put: %v", i, err)
		}
		keys, _, err := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(base)})
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != i+1 || keys[len(keys)-1] != k {
			t.Fatalf("iter %d: LIST right after PUT sees %d keys (want %d), missing %s", i, len(keys), i+1, k)
		}
	}
}

func TestProd_OverwriteThenRead(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	k := pfx(t) + "ow"
	for i := 0; i < *fIters; i++ {
		data := []byte(fmt.Sprintf("version-%d-%s", i, strings.Repeat("z", i%50)))
		if err := pool.Upload(ctx, k, data); err != nil {
			t.Fatal(err)
		}
		got, err := pool.Download(ctx, k)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("iter %d: stale/invalid read after overwrite: err=%v got=%q want=%q", i, err, got, data)
		}
		r, err := pool.DownloadRange(ctx, k, 0, 3)
		if err != nil || string(r) != "ver" {
			t.Fatalf("iter %d: range after overwrite: %q %v", i, r, err)
		}
	}
}

func TestProd_DeleteThenList(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	base := pfx(t)
	for i := 0; i < *fIters; i++ {
		k := fmt.Sprintf("%sdtl-%04d", base, i)
		if err := pool.Upload(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := pool.Delete(ctx, k); err != nil {
			t.Fatal(err)
		}
		keys, _, err := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(base)})
		if err != nil || len(keys) != 0 {
			t.Fatalf("iter %d: LIST after DELETE sees %v err=%v", i, keys, err)
		}
		if ok, _ := pool.Exists(ctx, k); ok {
			t.Fatalf("iter %d: HEAD after DELETE says exists", i)
		}
	}
}

// Default SDK behaviour (RequestChecksumCalculation=WhenSupported): every PUT
// carries an x-amz-checksum-crc32 (trailer with aws-chunked over plain HTTP).
// Uploading via the production pool exercises exactly that.
func TestProd_DefaultSDKChecksumPut(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	for _, sz := range []int{1, 1000, 64 << 10, 5 << 20, 17 << 20} {
		t.Run(fmt.Sprintf("%dB", sz), func(t *testing.T) {
			k := fmt.Sprintf("%sck-%d", pfx(t), sz)
			data := payload(sz)
			if err := pool.Upload(ctx, k, data); err != nil {
				t.Fatal(err)
			}
			got, err := pool.Download(ctx, k)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("roundtrip mismatch: err=%v len=%d want=%d", err, len(got), sz)
			}
			// object must not be stored with aws-chunked framing
			h, err := raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(k)})
			if err != nil || aws.ToInt64(h.ContentLength) != int64(sz) {
				t.Fatalf("stored length %d want %d (chunk framing leaked into object?) err=%v", aws.ToInt64(h.ContentLength), sz, err)
			}
		})
	}
}

func TestProd_LargeObject(t *testing.T) {
	setup(t)
	if *fLarge <= 0 {
		t.Skip("-large-mib=0")
	}
	ctx := ctxT(t)
	sz := *fLarge << 20
	data := payload(sz)
	sum := sha256.Sum256(data)
	k := pfx(t) + "large.parquet"
	t0 := time.Now()
	if err := pool.Upload(ctx, k, data); err != nil {
		t.Fatalf("PUT %d MiB single request: %v", *fLarge, err)
	}
	up := time.Since(t0)
	t0 = time.Now()
	got, err := pool.Download(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != sum {
		t.Fatalf("large object corrupted (len got=%d want=%d)", len(got), sz)
	}
	rng, err := pool.DownloadRange(ctx, k, int64(sz)/2, 1<<20)
	if err != nil || !bytes.Equal(rng, data[sz/2:sz/2+1<<20]) {
		t.Fatalf("mid-range of large object wrong: %v", err)
	}
	t.Logf("%d MiB: PUT %.2fs, GET %.2fs", *fLarge, up.Seconds(), time.Since(t0).Seconds())
}

// --------------------------------------------------------------- EXTRA ----
// Not issued by Lakehouse today; recorded because user backends / future
// features (multipart uploader, conditional writes, batch delete) need them.

func TestExtra_DeleteObjects1000(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	base := pfx(t)
	const n = 1000
	var ids []types.ObjectIdentifier
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%sk%04d", base, i)
		ids = append(ids, types.ObjectIdentifier{Key: aws.String(k)})
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			_ = pool.Upload(ctx, k, []byte("x"))
		}()
	}
	wg.Wait()
	out, err := raw.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(*fBucket), Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)},
	})
	if err != nil {
		t.Fatalf("DeleteObjects(1000): %v", err)
	}
	if len(out.Errors) != 0 {
		t.Fatalf("DeleteObjects returned %d per-key errors, first: %s %s", len(out.Errors), aws.ToString(out.Errors[0].Code), aws.ToString(out.Errors[0].Message))
	}
	left, _, err := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(base)})
	if err != nil || len(left) != 0 {
		t.Fatalf("%d objects left after DeleteObjects, err=%v", len(left), err)
	}
	// verbose (non-quiet) response for missing keys
	o2, err := raw.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(*fBucket), Delete: &types.Delete{Objects: ids[:3]}})
	if err != nil || len(o2.Errors) != 0 {
		t.Fatalf("DeleteObjects of already-missing keys: err=%v errs=%v", err, o2.Errors)
	}
}

func TestExtra_MultipartUpload(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	k := pfx(t) + "mp.bin"
	const partSz = 5 << 20
	parts := [][]byte{payload(partSz), payload(partSz), payload(1234567)}
	cm, err := raw.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(*fBucket), Key: aws.String(k)})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	var cps []types.CompletedPart
	for i, p := range parts {
		up, err := raw.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(*fBucket), Key: aws.String(k), UploadId: cm.UploadId,
			PartNumber: aws.Int32(int32(i + 1)), Body: bytes.NewReader(p),
		})
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
		cps = append(cps, types.CompletedPart{ETag: up.ETag, PartNumber: aws.Int32(int32(i + 1))})
	}
	lp, err := raw.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(*fBucket), Key: aws.String(k), UploadId: cm.UploadId})
	if err != nil || len(lp.Parts) != 3 {
		t.Fatalf("ListParts: n=%d err=%v", len(lp.Parts), err)
	}
	co, err := raw.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(*fBucket), Key: aws.String(k), UploadId: cm.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: cps},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if !strings.HasSuffix(strings.Trim(aws.ToString(co.ETag), `"`), "-3") {
		t.Errorf("multipart ETag %s lacks -3 suffix", aws.ToString(co.ETag))
	}
	got, err := pool.Download(ctx, k)
	want := bytes.Join(parts, nil)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("assembled body mismatch: err=%v len=%d want=%d", err, len(got), len(want))
	}
	// range across part boundary
	r, err := pool.DownloadRange(ctx, k, partSz-100, 200)
	if err != nil || !bytes.Equal(r, want[partSz-100:partSz+100]) {
		t.Fatalf("range across part boundary wrong: %v", err)
	}
	t.Run("abort", func(t *testing.T) {
		k2 := pfx(t) + "abort.bin"
		c2, err := raw.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(*fBucket), Key: aws.String(k2)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = raw.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(*fBucket), Key: aws.String(k2), UploadId: c2.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(payload(partSz))}); err != nil {
			t.Fatal(err)
		}
		if _, err = raw.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(*fBucket), Key: aws.String(k2), UploadId: c2.UploadId}); err != nil {
			t.Fatalf("Abort: %v", err)
		}
		if ok, _ := pool.Exists(ctx, k2); ok {
			t.Fatal("aborted upload produced an object")
		}
	})
	t.Run("too-small-part-rejected", func(t *testing.T) {
		k3 := pfx(t) + "small.bin"
		c3, err := raw.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(*fBucket), Key: aws.String(k3)})
		if err != nil {
			t.Fatal(err)
		}
		var cp []types.CompletedPart
		for i := 1; i <= 2; i++ {
			up, err := raw.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(*fBucket), Key: aws.String(k3), UploadId: c3.UploadId, PartNumber: aws.Int32(int32(i)), Body: bytes.NewReader(payload(1000))})
			if err != nil {
				t.Fatal(err)
			}
			cp = append(cp, types.CompletedPart{ETag: up.ETag, PartNumber: aws.Int32(int32(i))})
		}
		_, err = raw.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(*fBucket), Key: aws.String(k3), UploadId: c3.UploadId, MultipartUpload: &types.CompletedMultipartUpload{Parts: cp}})
		_, _ = raw.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(*fBucket), Key: aws.String(k3), UploadId: c3.UploadId})
		if code(err) != "EntityTooSmall" {
			t.Errorf("AWS returns EntityTooSmall for <5MiB non-last parts; got code=%q err=%v (informational)", code(err), err)
		}
	})
}

func TestExtra_ConditionalPut(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	k := pfx(t) + "cond"
	put := func(body string, inm, im *string) (*s3.PutObjectOutput, error) {
		return raw.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(k), Body: strings.NewReader(body), IfNoneMatch: inm, IfMatch: im})
	}
	star := aws.String("*")
	t.Run("if-none-match-star-creates", func(t *testing.T) {
		if _, err := put("v1", star, nil); err != nil {
			t.Fatalf("If-None-Match:* on absent key: %v", err)
		}
	})
	t.Run("if-none-match-star-existing-412", func(t *testing.T) {
		_, err := put("v2", star, nil)
		if status(err) != 412 {
			t.Fatalf("status=%d code=%q err=%v; want 412", status(err), code(err), err)
		}
		if got, _ := pool.Download(ctx, k); string(got) != "v1" {
			t.Fatalf("object was overwritten despite failed precondition: %q", got)
		}
	})
	var etag *string
	t.Run("if-match-correct-etag", func(t *testing.T) {
		h, err := raw.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(k)})
		if err != nil {
			t.Fatal(err)
		}
		out, err := put("v3", nil, h.ETag)
		if err != nil {
			t.Fatalf("If-Match with current ETag: %v", err)
		}
		etag = out.ETag
	})
	t.Run("if-match-stale-etag-412", func(t *testing.T) {
		_, err := put("v4", nil, aws.String(`"00000000000000000000000000000000"`))
		if status(err) != 412 {
			t.Fatalf("status=%d code=%q err=%v; want 412", status(err), code(err), err)
		}
		if got, _ := pool.Download(ctx, k); string(got) != "v3" {
			t.Fatalf("overwritten despite failed If-Match: %q (etag %v)", got, aws.ToString(etag))
		}
	})
	t.Run("if-match-missing-key-404", func(t *testing.T) {
		_, err := raw.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(k + "-absent"), Body: strings.NewReader("x"), IfMatch: aws.String(`"abc"`)})
		if err == nil {
			t.Fatal("If-Match on missing key succeeded; AWS returns 404 NoSuchKey")
		}
		if s := status(err); s != 404 && s != 412 {
			t.Errorf("status=%d err=%v; AWS returns 404", s, err)
		}
	})
	t.Run("concurrent-create-exactly-one-winner", func(t *testing.T) {
		kk := pfx(t) + "race"
		var wg sync.WaitGroup
		var mu sync.Mutex
		wins, fails := 0, 0
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, err := raw.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(kk), Body: strings.NewReader(fmt.Sprintf("w%d", i)), IfNoneMatch: star})
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					wins++
				} else if s := status(err); s == 412 || s == 409 {
					fails++
				}
			}(i)
		}
		wg.Wait()
		if wins != 1 || fails != 15 {
			t.Fatalf("wins=%d precondition-failures=%d; want 1 and 15", wins, fails)
		}
	})
}

func TestExtra_ExplicitChecksums(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	data := payload(300_000)
	for _, alg := range []types.ChecksumAlgorithm{types.ChecksumAlgorithmCrc32, types.ChecksumAlgorithmCrc32c, types.ChecksumAlgorithmSha256, types.ChecksumAlgorithmSha1, types.ChecksumAlgorithmCrc64nvme} {
		t.Run(string(alg), func(t *testing.T) {
			k := pfx(t) + string(alg)
			out, err := raw.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(k), Body: bytes.NewReader(data), ChecksumAlgorithm: alg})
			if err != nil {
				t.Fatalf("PUT with %s: %v", alg, err)
			}
			g, err := raw.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(k), ChecksumMode: types.ChecksumModeEnabled})
			if err != nil {
				t.Fatalf("GET checksum mode: %v", err)
			}
			b, _ := io.ReadAll(g.Body)
			g.Body.Close()
			if !bytes.Equal(b, data) {
				t.Fatal("body mismatch")
			}
			var stored, sent *string
			switch alg {
			case types.ChecksumAlgorithmCrc32:
				stored, sent = g.ChecksumCRC32, out.ChecksumCRC32
			case types.ChecksumAlgorithmCrc32c:
				stored, sent = g.ChecksumCRC32C, out.ChecksumCRC32C
			case types.ChecksumAlgorithmSha256:
				stored, sent = g.ChecksumSHA256, out.ChecksumSHA256
			case types.ChecksumAlgorithmSha1:
				stored, sent = g.ChecksumSHA1, out.ChecksumSHA1
			case types.ChecksumAlgorithmCrc64nvme:
				stored, sent = g.ChecksumCRC64NVME, out.ChecksumCRC64NVME
			}
			if stored == nil || *stored == "" {
				t.Errorf("%s checksum not returned on GET (checksum silently dropped)", alg)
			} else if sent != nil && *sent != *stored {
				t.Errorf("%s PUT vs GET checksum differ: %s vs %s", alg, *sent, *stored)
			}
		})
	}
	t.Run("wrong-sha256-rejected", func(t *testing.T) {
		_, err := raw.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(*fBucket), Key: aws.String(pfx(t) + "bad"), Body: bytes.NewReader(data),
			ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(make([]byte, 32))),
		})
		if err == nil {
			t.Fatal("PUT with wrong x-amz-checksum-sha256 accepted; AWS returns 400 BadDigest/InvalidRequest")
		}
	})
	t.Run("content-md5-good", func(t *testing.T) {
		sum := md5.Sum(data)
		_, err := raw.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(pfx(t) + "md5"), Body: bytes.NewReader(data), ContentMD5: aws.String(base64.StdEncoding.EncodeToString(sum[:]))})
		if err != nil {
			t.Fatalf("PUT with valid Content-MD5: %v", err)
		}
	})
	t.Run("content-md5-bad-rejected", func(t *testing.T) {
		_, err := raw.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(*fBucket), Key: aws.String(pfx(t) + "md5bad"), Body: bytes.NewReader(data), ContentMD5: aws.String(base64.StdEncoding.EncodeToString(make([]byte, 16)))})
		if err == nil {
			t.Fatal("PUT with wrong Content-MD5 accepted; AWS returns 400 BadDigest")
		}
	})
}

// Keys with "." / ".." path segments are legal in S3 (keys are opaque);
// filesystem-backed gateways often normalise or reject them. Lakehouse does
// not generate such keys.
func TestExtra_DotSegmentKeys(t *testing.T) {
	setup(t)
	ctx := ctxT(t)
	for _, n := range []string{"a/./b.parquet", "a/../b.parquet", "a//b.parquet", "/leading-slash.parquet"} {
		t.Run(n, func(t *testing.T) {
			k := pfx(t) + n
			data := []byte("d-" + n)
			if err := pool.Upload(ctx, k, data); err != nil {
				t.Fatalf("PUT: %v", err)
			}
			got, err := pool.Download(ctx, k)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("GET: err=%v got=%q", err, got)
			}
			keys, _, err := listAll(ctx, &s3.ListObjectsV2Input{Prefix: aws.String(k)})
			if err != nil || len(keys) != 1 || keys[0] != k {
				t.Fatalf("LIST: keys=%q err=%v", keys, err)
			}
		})
	}
}
