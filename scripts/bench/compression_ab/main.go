// compression_ab measures the real-data effect of the schema encoding tags
// (delta timestamps + RLE_DICTIONARY low-card strings): it downloads REAL
// parquet files from the live e2e MinIO, decodes their rows, re-encodes them
// with (a) the pre-tag baseline schema and (b) the current tagged schema —
// identical rows, identical zstd level — and reports sizes.
//
// Usage:
//
//	go run ./scripts/bench/compression_ab \
//	  -endpoint http://127.0.0.1:29000 -bucket obs-archive -n 12
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// baselineLogRow mirrors schema.LogRow EXACTLY as it was before the encoding
// tags (bare parquet tags) — the A side of the A/B.
type baselineLogRow struct {
	AccountID          uint32            `parquet:"account_id"`
	ProjectID          uint32            `parquet:"project_id"`
	TimestampUnixNano  int64             `parquet:"timestamp_unix_nano"`
	Body               string            `parquet:"body"`
	SeverityText       string            `parquet:"severity_text"`
	SeverityNumber     int32             `parquet:"severity_number"`
	ServiceName        string            `parquet:"service.name"`
	TraceID            string            `parquet:"trace_id"`
	SpanID             string            `parquet:"span_id"`
	K8sNamespaceName   string            `parquet:"k8s.namespace.name"`
	K8sPodName         string            `parquet:"k8s.pod.name"`
	K8sDeploymentName  string            `parquet:"k8s.deployment.name"`
	K8sNodeName        string            `parquet:"k8s.node.name"`
	DeployEnv          string            `parquet:"deployment.environment"`
	CloudRegion        string            `parquet:"cloud.region"`
	HostName           string            `parquet:"host.name"`
	Stream             string            `parquet:"_stream"`
	StreamID           string            `parquet:"_stream_id"`
	ScopeName          string            `parquet:"scope.name"`
	ResourceAttributes map[string]string `parquet:"resource.attributes,optional"`
	LogAttributes      map[string]string `parquet:"log.attributes,optional"`
	ScopeAttributes    map[string]string `parquet:"scope.attributes,optional"`
}

func toBaseline(r schema.LogRow) baselineLogRow {
	return baselineLogRow{r.AccountID, r.ProjectID, r.TimestampUnixNano, r.Body,
		r.SeverityText, r.SeverityNumber, r.ServiceName, r.TraceID, r.SpanID,
		r.K8sNamespaceName, r.K8sPodName, r.K8sDeploymentName, r.K8sNodeName,
		r.DeployEnv, r.CloudRegion, r.HostName, r.Stream, r.StreamID, r.ScopeName,
		r.ResourceAttributes, r.LogAttributes, r.ScopeAttributes}
}

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:29000", "S3 endpoint")
	bucket := flag.String("bucket", "obs-archive", "bucket")
	n := flag.Int("n", 12, "max files to sample")
	flag.Parse()

	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	if err != nil {
		die(err)
	}
	cl := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(*endpoint)
		o.UsePathStyle = true
	})

	// Sample real LOG parquet files across partitions (largest first for signal).
	var keys []string
	var sizes = map[string]int64{}
	p := s3.NewListObjectsV2Paginator(cl, &s3.ListObjectsV2Input{Bucket: bucket})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			die(err)
		}
		for _, o := range page.Contents {
			k := *o.Key
			if strings.Contains(k, "/logs/dt=") && strings.HasSuffix(k, ".parquet") {
				keys = append(keys, k)
				sizes[k] = *o.Size
			}
		}
	}
	sort.Slice(keys, func(i, j int) bool { return sizes[keys[i]] > sizes[keys[j]] })
	if len(keys) > *n {
		keys = keys[:*n]
	}
	if len(keys) == 0 {
		die(fmt.Errorf("no log parquet files found"))
	}

	levels := map[string]*zstd.Codec{
		"zstd-default": {Level: zstd.SpeedDefault},
		"zstd-best":    {Level: zstd.SpeedBestCompression},
	}
	var totOrig, totBase, totNew, totSorted = int64(0), map[string]int64{}, map[string]int64{}, map[string]int64{}

	fmt.Printf("%-58s %9s | %22s | %22s\n", "file", "orig", "baseline(def/best)", "tagged(def/best)")
	for _, k := range keys {
		obj, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: &k})
		if err != nil {
			die(err)
		}
		buf := new(bytes.Buffer)
		if _, err := buf.ReadFrom(obj.Body); err != nil {
			die(err)
		}
		obj.Body.Close()
		rows, err := parquet.Read[schema.LogRow](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		if err != nil {
			die(fmt.Errorf("decode %s: %w", k, err))
		}
		sorted := make([]schema.LogRow, len(rows))
		copy(sorted, rows)
		sort.Slice(sorted, func(i, j int) bool {
			if sorted[i].StreamID != sorted[j].StreamID {
				return sorted[i].StreamID < sorted[j].StreamID
			}
			return sorted[i].TimestampUnixNano < sorted[j].TimestampUnixNano
		})
		base := make([]baselineLogRow, len(rows))
		for i, r := range rows {
			base[i] = toBaseline(r)
		}
		out := map[string]int64{}
		for lname, codec := range levels {
			var b bytes.Buffer
			w := parquet.NewGenericWriter[baselineLogRow](&b, parquet.Compression(codec))
			if _, err := w.Write(base); err != nil {
				die(err)
			}
			w.Close()
			out["base-"+lname] = int64(b.Len())
			totBase[lname] += int64(b.Len())

			var b2 bytes.Buffer
			w2 := parquet.NewGenericWriter[schema.LogRow](&b2, parquet.Compression(codec))
			if _, err := w2.Write(rows); err != nil {
				die(err)
			}
			w2.Close()
			out["new-"+lname] = int64(b2.Len())
			totNew[lname] += int64(b2.Len())

			var b3 bytes.Buffer
			w3 := parquet.NewGenericWriter[schema.LogRow](&b3, parquet.Compression(codec))
			if _, err := w3.Write(sorted); err != nil {
				die(err)
			}
			w3.Close()
			out["sorted-"+lname] = int64(b3.Len())
			totSorted[lname] += int64(b3.Len())
		}
		totOrig += sizes[k]
		short := k
		if len(short) > 56 {
			short = "…" + short[len(short)-55:]
		}
		fmt.Printf("%-58s %9d | %10d/%10d | %10d/%10d\n", short, sizes[k],
			out["base-zstd-default"], out["base-zstd-best"], out["new-zstd-default"], out["new-zstd-best"])
	}
	fmt.Printf("\nTOTAL across %d real files (orig on-disk: %d bytes)\n", len(keys), totOrig)
	for _, l := range []string{"zstd-default", "zstd-best"} {
		d := 100 * float64(totNew[l]-totBase[l]) / float64(totBase[l])
		ds := 100 * float64(totSorted[l]-totBase[l]) / float64(totBase[l])
		fmt.Printf("  %-13s baseline=%9d  tagged=%9d (%+.1f%%)  tagged+sorted=%9d (%+.1f%%)\n",
			l, totBase[l], totNew[l], d, totSorted[l], ds)
	}
}

func die(err error) { fmt.Fprintln(os.Stderr, "compression_ab:", err); os.Exit(1) }
