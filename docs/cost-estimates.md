# Cost Estimates

## Pricing Basis (AWS us-east-1)

| Resource | Price |
|---|---|
| EBS gp3 | $0.08/GB/month |
| S3 Standard | $0.023/GB/month |
| S3 Infrequent Access | $0.0125/GB/month |
| S3 GET requests | $0.0004/1000 requests |
| EC2 m5.xlarge (4 vCPU, 16GB) | ~$140/month |
| EKS pod (1 vCPU, 2GB) | ~$30-40/month |

## Compression Ratios

| Format | Ratio | Notes |
|---|---|---|
| VL native (LSM) | ~10:1 | Optimized for read+write |
| VT native | ~8:1 | Structured span fields |
| Parquet + ZSTD (logs) | ~12:1 | Columnar advantage |
| Parquet + ZSTD (traces) | ~10:1 | Structured data |

## 250 GB/month Logs (Multi-AZ)

VL stored: 25 GB/mo. Parquet stored: ~21 GB/mo.

| Retention | All-EBS (3 AZ) | All-S3 Lakehouse | Savings |
|---|---|---|---|
| 1 month | $216/mo | $135/mo | 37% |
| 6 months | $246/mo | $137/mo | 44% |
| 1 year | $282/mo | $138/mo | 51% |
| 2 years | $354/mo | $141/mo | 60% |

At small scale, all-S3 standalone Lakehouse is cheapest.

## 500 GB/month Logs (Multi-AZ)

VL stored: 50 GB/mo. Parquet stored: ~42 GB/mo.

| Retention | All-EBS (3 AZ) | All-S3 Lakehouse | Savings |
|---|---|---|---|
| 1 month | $432/mo | $136/mo | 69% |
| 6 months | $492/mo | $138/mo | 72% |
| 1 year | $564/mo | $141/mo | 75% |
| 2 years | $708/mo | $148/mo | 79% |

## 1 PB/month Logs (Multi-AZ)

VL stored: 100 TB/mo. Parquet stored: ~83 TB/mo.

| Retention | All-EBS (3 AZ) | Hybrid (1mo hot) | All-S3 | EBS Savings |
|---|---|---|---|---|
| 3 months | $84,600/mo | $39,035/mo | $3,473/mo | 54% hybrid |
| 6 months | $159,000/mo | $42,288/mo | $6,725/mo | 73% hybrid |
| 1 year | $303,000/mo | $48,513/mo | $12,950/mo | 84% hybrid |
| 2 years | $591,000/mo | $60,963/mo | $25,400/mo | 90% hybrid |

## Annual Savings Summary

| Scenario | 1yr Retention | 2yr Retention |
|---|---|---|
| 250 GB/mo (all-S3) | $1.7K/yr (51%) | $2.6K/yr (60%) |
| 500 GB/mo (all-S3) | $5.1K/yr (75%) | $6.7K/yr (79%) |
| 1 PB/mo (hybrid 1mo hot) | $3.05M/yr (84%) | $6.36M/yr (90%) |
| 1 PB/mo logs+traces (hybrid) | $4.58M/yr (84%) | $9.54M/yr (90%) |

## Why S3 Wins at Scale

1. **Multi-AZ by default**: S3 is 11 nines durability across AZs at no extra cost. EBS requires per-AZ replicas, tripling storage cost.
2. **No compaction overhead**: VL/VT LSM compaction consumes CPU. Lakehouse is read-only.
3. **Tiered storage**: S3 lifecycle rules automatically move old data to IA ($0.0125/GB) or Glacier ($0.004/GB).
4. **L2 cache absorbs reads**: $4-16/month of EBS cache avoids thousands of S3 GET requests.

## Recommendation

| Scale | Recommendation |
|---|---|
| <500 GB/mo | All-S3 Lakehouse (simplest, cheapest) |
| 500 GB - 10 TB/mo | Hybrid (1-2mo hot EBS + S3 cold) |
| >10 TB/mo | Hybrid (1mo hot EBS + S3 cold) — saves millions/year |
