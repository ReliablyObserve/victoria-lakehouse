---
title: Scaling
sidebar_position: 13
---

# Scaling

## Vertical Scaling

### CPU

Driven by Parquet decompression (ZSTD) and LogsQL filter evaluation. Typical: 0.5-2 vCPU per instance.

Increase CPU for:
- High query concurrency (`--lakehouse.query.max-concurrent`)
- Full-text search queries (scan-heavy)
- Large row group decompression

### Memory

L1 memory cache + query working set. Typical: 512MB-2GB per instance.

Increase memory for:
- Larger L1 cache (`--lakehouse.cache.memory-limit`)
- More concurrent queries (each holds DataBlocks in memory)
- Large MAP column deserialization

### Disk

L2 disk cache. Size to hold 2-4 weeks of frequently queried data.

- EBS gp3 recommended (3000 IOPS, 125 MB/s included)
- Cost: ~$0.08/GB/month
- Rule of thumb: 5-10% of total S3 dataset size

## Horizontal Scaling

Add replicas to increase throughput. No coordination between instances.

| Component | Scales With | Notes |
|---|---|---|
| Query throughput | Replicas | Linear scaling |
| L2 disk cache | Replicas | Each instance has own cache |
| Peer cache | Replicas | Fleet-wide L2 sharing |
| Manifest | Replicas | Replicated per instance (lightweight) |
| S3 connections | Replicas | Per-instance `--lakehouse.s3.max-connections` |

### Multi-AZ Deployment

Deploy 3-4 instances per AZ for HA:

```
AZ-a: lakehouse-logs-0, lakehouse-logs-1, lakehouse-logs-2
AZ-b: lakehouse-logs-3, lakehouse-logs-4, lakehouse-logs-5
AZ-c: lakehouse-logs-6, lakehouse-logs-7, lakehouse-logs-8
```

Peer cache works within and across AZs (cross-AZ adds ~5-15ms latency).

## Sizing Guide

| S3 Dataset | Replicas per Signal | CPU/Instance | Memory/Instance | L2 Disk |
|---|---|---|---|---|
| 100 GB | 3 (1/AZ) | 0.5 vCPU | 512 MB | 10 GB |
| 1 TB | 6 (2/AZ) | 1 vCPU | 1 GB | 50 GB |
| 10 TB | 12 (4/AZ) | 2 vCPU | 2 GB | 100 GB |
| 100 TB | 24 (8/AZ) | 2 vCPU | 4 GB | 200 GB |

Both logs and traces modes need separate instances. A 10 TB deployment with both signals: 24 instances total (12 logs + 12 traces).

## EBS Sizing Estimates

| Dataset (S3) | L2 Disk (EBS gp3) | L1 Memory | Monthly EBS Cost |
|---|---|---|---|
| 100 GB | 10 GB | 512 MB | $0.80 |
| 1 TB | 50 GB | 1 GB | $4.00 |
| 10 TB | 100 GB | 2 GB | $8.00 |
| 100 TB | 200 GB | 4 GB | $16.00 |

L2 disk cache is the most cost-effective optimization: $4-16/month avoids thousands of S3 GET requests.

## Scaling Indicators

**Scale up when:**
- `lakehouse_concurrent_select_current` consistently near `_capacity`
- `lakehouse_cache_hit_ratio{tier="L2"}` < 0.7 (cache too small)
- `lakehouse_s3_throttle_total` increasing (too many S3 connections)
- `lakehouse_http_request_duration_seconds{quantile="0.95"}` exceeding targets

**Scale down when:**
- CPU utilization consistently < 20%
- `lakehouse_concurrent_select_current` consistently < 25% of capacity
- L2 disk cache utilization < 30%
