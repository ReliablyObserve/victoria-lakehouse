package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func main() {
	key := os.Args[1]
	ctx := context.Background()
	cfg, _ := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")))
	cl := s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = aws.String("http://127.0.0.1:29000"); o.UsePathStyle = true })
	bucket := "obs-archive"
	obj, err := cl.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		panic(err)
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(obj.Body)
	obj.Body.Close()
	rows, err := parquet.Read[schema.LogRow](bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		panic(err)
	}

	sorted := make([]schema.LogRow, len(rows))
	copy(sorted, rows)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].StreamID != sorted[j].StreamID {
			return sorted[i].StreamID < sorted[j].StreamID
		}
		return sorted[i].TimestampUnixNano < sorted[j].TimestampUnixNano
	})

	colSizes := func(rs []schema.LogRow) map[string]int64 {
		var b bytes.Buffer
		w := parquet.NewGenericWriter[schema.LogRow](&b, parquet.Compression(&zstd.Codec{Level: zstd.SpeedBestCompression}))
		w.Write(rs)
		w.Close()
		f, _ := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()))
		out := map[string]int64{}
		for _, rg := range f.RowGroups() {
			for _, cc := range rg.ColumnChunks() {
				path := f.Schema().Columns()[cc.Column()]
				name := path[len(path)-1]
				if ccm, ok := cc.(*parquet.FileColumnChunk); ok {
					_ = ccm
				}
				// use the column chunk's TotalCompressedSize via metadata
				out[fmt.Sprint(path)] += colChunkSize(f, rg, cc, name)
			}
		}
		return out
	}
	_ = colSizes
	// simpler: use the low-level metadata
	report := func(label string, rs []schema.LogRow) map[string]int64 {
		var b bytes.Buffer
		w := parquet.NewGenericWriter[schema.LogRow](&b, parquet.Compression(&zstd.Codec{Level: zstd.SpeedBestCompression}))
		w.Write(rs)
		w.Close()
		f, _ := parquet.OpenFile(bytes.NewReader(b.Bytes()), int64(b.Len()))
		m := map[string]int64{}
		meta := f.Metadata()
		for _, rg := range meta.RowGroups {
			for _, cc := range rg.Columns {
				m[fmt.Sprint(cc.MetaData.PathInSchema)] += cc.MetaData.TotalCompressedSize
			}
		}
		fmt.Printf("%s total=%d\n", label, b.Len())
		return m
	}
	a := report("unsorted(tagged)", rows)
	b := report("sorted(tagged)", sorted)
	type d struct {
		name  string
		delta int64
	}
	var ds []d
	for k, v := range b {
		ds = append(ds, d{k, v - a[k]})
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i].delta > ds[j].delta })
	fmt.Println("top column deltas (sorted - unsorted), bytes:")
	for _, x := range ds[:10] {
		fmt.Printf("  %-40s %+d\n", x.name, x.delta)
	}
}

func colChunkSize(f *parquet.File, rg parquet.RowGroup, cc parquet.ColumnChunk, name string) int64 {
	return 0
}
