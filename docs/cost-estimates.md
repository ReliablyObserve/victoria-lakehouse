---
title: Cost Estimates
sidebar_position: 15
---

# Cost Estimates

Monthly AWS infrastructure cost for logs and traces together (60% logs, 40% traces), at 1 query per second, for every system deployed as a **production HA setup** unless its name says otherwise. Storage, compute, cross-AZ network and S3 requests are all counted.

Both models are scripts in this repository. Change a constant, re-run, and every table below is regenerated:

```bash
python3 scripts/cost/cost_model.py            # 0.1 / 1 / 10 TB per day, cost tables
python3 scripts/cost/cost_model.py --res      # the projected resources behind each figure
python3 scripts/cost/cost_model_scale.py      # 50 / 100 / 300 / 500 TB per day, HA topology
NODE_TB=60 python3 scripts/cost/cost_model_scale.py   # sensitivity: 60 TB of EBS per node
python3 scripts/cost/cost_tiering.py         # long-term retention and S3 lifecycle tiering
VL_CPU=loki python3 scripts/cost/cost_model.py        # sensitivity: heavier VL/VT CPU rule
```

## Read this first: what is measured and what is not

Every input is labelled **measured**, **sourced** (vendor documentation or price list) or **assumed**.

| Input | Class | Note |
|---|---|---|
| AWS and ClickHouse Cloud prices | sourced | list prices, us-east-1, fetched 2026-09-30 |
| Lakehouse compression (logs 6.1x, traces 9.4x) | **measured** | [ZSTD compression benchmark](zstd-compression-benchmark.md) |
| ClickHouse compression and CPU | sourced | ClickStack sizing guide: 10x compression; 0.1 vCPU per MB/s ingest, 0.3 per MB/s per query |
| VictoriaLogs / VictoriaTraces compression | sourced range | "10x or more" (VT docs); relative claims for VL |
| Loki and Tempo sizing | sourced | Loki sizing tiers, Tempo sizing guide |
| OpenSearch storage and nodes | sourced | AWS OpenSearch best-practice formulas |
| **Lakehouse CPU** | **assumed** | same per-MB/s rule as ClickHouse's guide until measured |
| **VL/VT CPU** | **assumed** | same rule; `VL_CPU=loki` gives a heavier, derived alternative |
| Wire compression (5x), 1 QPS, 7-day hot window | assumed | applied to every system equally |

**Cost is not performance.** See [Performance](performance.md) and the validated benchmark in `bench-results/baseline-2026-09`.

## 0.1 – 10 TB per day

Monthly, mid estimate (the scripts also print low/high ranges).

| System | 0.1 TB/day, 30d | 0.1 TB/day, 1y | 1 TB/day, 30d | 1 TB/day, 1y | 10 TB/day, 30d | 10 TB/day, 1y |
|---|---:|---:|---:|---:|---:|---:|
| **Lakehouse (S3)** | **$137** | **$246** | **$367** | **$1,453** | **$3,646** | **$14,504** |
| ClickHouse, 1 replica, 7d EBS + S3 (**no compute HA**) | $211 | $275 | $482 | $1,124 | $3,192 | $9,613 |
| Hybrid: VL/VT hot 7d + Lakehouse S3 after 7d | $276 | $385 | $670 | $1,756 | $6,680 | $17,538 |
| ClickHouse self-hosted, 7d EBS + S3, RF2 | $230 | $358 | $670 | $1,954 | $5,069 | $17,910 |
| ClickHouse Cloud (Scale) | $454 | $596 | $691 | $2,103 | $6,909 | $21,035 |
| Loki + Tempo (S3) | $731 | $909 | $1,315 | $3,088 | $23,435 | $41,157 |
| VL/VT HA (2 clusters, EBS) | $181 | $672 | $792 | $5,706 | $7,924 | $57,057 |
| ClickHouse self-hosted, EBS, RF2 | $257 | $815 | $961 | $6,544 | $8,002 | $63,836 |
| OpenSearch, 7d hot + searchable snapshots | $969 | $1,816 | $5,242 | $15,868 | $49,244 | $157,047 |

## 50 – 500 TB per day (HA topology)

Each system is sized as a production cluster: shards × replicas, Keeper or masters, Kafka where the system needs it. Data nodes are r7g.8xlarge and stateless pools m7g.8xlarge, at on-demand list price; volume discounts are not modelled and would apply to every row.

| System | 50 TB/d 30d | 50 TB/d 1y | 100 TB/d 30d | 100 TB/d 1y | 300 TB/d 1y | 500 TB/d 1y |
|---|---:|---:|---:|---:|---:|---:|
| Lakehouse (**projected, not ready at this scale**) | $23k | $73k | $43k | $142k | $420k | $697k |
| ClickHouse self-hosted, 7d EBS + S3, RF2 | $34k | $93k | $64k | $182k | $543k | $904k |
| Hybrid: VL/VT hot 7d + Lakehouse S3 after 7d | $46k | $96k | $86k | $185k | $549k | $911k |
| ClickHouse Cloud (Scale, list price) | $35k | $105k | $69k | $210k | $631k | $1,052k |
| Loki + Tempo | $142k | $223k | $283k | $445k | $1,329k | $2,214k |
| VL/VT HA (2 clusters) | $49k | $487k | $97k | $974k | $2,916k | $4,861k |
| ClickHouse self-hosted, EBS, RF2 | $51k | $550k | $102k | $1,098k | $3,287k | $5,477k |
| OpenSearch, 7d hot + searchable snapshots | $309k | $1,156k | $616k | $2,312k | $6,932k | $11,553k |

## Long-term retention and S3 storage-class tiering

Lakehouse is built for S3 lifecycle tiering (`lh.feature.storage.lifecycle_tiering`, shipped since v0.49.0; per-tenant schedules since v0.100.0):
- **Few, large objects.** Compaction produces large immutable Parquet objects, about 64 MB each, which is what lifecycle transitions want. Few objects means cheap transitions, and objects stay well above the 128 KB minimum billable size of the infrequent-access classes.
- **Escalating compression.** Compression escalates with age as compaction levels rise. The extra saving is not measured yet, so the tables below use the measured 6.1x / 9.4x throughout.
- **Rules and stats agree.** The transition itself is an S3 bucket lifecycle rule. Lakehouse mirrors the same rules in its config, globally or per tenant, so tenant stats, cost estimates and the delete path know which storage class each object is in.
- **Deletes never pay retrieval fees.** Deletes on Standard-IA or Glacier objects are applied as tombstones, never as retrieval plus rewrite ([deletion strategy](deletion-strategy.md)).
- **Glacier IR stays queryable.** Glacier Instant Retrieval objects answer normal GETs in milliseconds; a query that reads them pays $0.03 per GB read (Standard-IA: $0.01 per GB).
- **Deep Archive is for retention only.** Objects there must be restored before they can be queried.
- **Transition after compaction.** The first transition should come after compaction has finished with an object. Rewriting an object that is already in Standard-IA or Glacier IR costs a retrieval fee plus the rest of its minimum storage duration (30 and 90 days).

Reproduce: `python3 scripts/cost/cost_tiering.py`.

Steady state (retention window full). Includes PUTs and lifecycle-transition requests; retrieval fees for reading IA/Glacier IR data are extra ($0.01 / $0.03 per GB read).

**S3 Standard only**

| Ingest/day | 30 days | 90 days | 1 year | 3 years | 7 years |
|---|---:|---:|---:|---:|---:|
| 1 TB | $98 | $292 | $1,182 | $3,445 | $7,971 |
| 10 TB | $976 | $2,843 | $11,304 | $32,906 | $76,111 |
| 50 TB | $4,717 | $13,833 | $54,522 | $163k | $379k |
| 100 TB | $9,383 | $27,166 | $109k | $325k | $757k |
| 500 TB | $45,053 | $134k | $541k | $1,621k | $3,781k |

**Standard 30d → Standard-IA → Glacier IR at 90d (all queryable)**

| Ingest/day | 30 days | 90 days | 1 year | 3 years | 7 years |
|---|---:|---:|---:|---:|---:|
| 1 TB | $98 | $204 | $360 | $772 | $1,595 |
| 10 TB | $976 | $2,039 | $3,602 | $7,717 | $15,946 |
| 50 TB | $4,717 | $10,034 | $17,850 | $38,424 | $79,571 |
| 100 TB | $9,383 | $20,018 | $35,651 | $76,797 | $159k |
| 500 TB | $45,053 | $98,226 | $176k | $382k | $794k |

**Standard 30d → Glacier IR → Deep Archive after 1y (archive)**

| Ingest/day | 30 days | 90 days | 1 year | 3 years | 7 years |
|---|---:|---:|---:|---:|---:|
| 1 TB | $98 | $133 | $288 | $393 | $597 |
| 10 TB | $976 | $1,327 | $2,877 | $3,928 | $5,965 |
| 50 TB | $4,717 | $6,474 | $14,224 | $19,481 | $29,665 |
| 100 TB | $9,383 | $12,897 | $28,398 | $38,912 | $59,280 |
| 500 TB | $45,053 | $62,623 | $140k | $193k | $295k |

### The same policy against the alternatives

Storage + compute + cross-AZ network + requests. Lakehouse uses the all-queryable policy (Standard 30d → Standard-IA → Glacier IR at 90d); Loki + Tempo is shown on S3 Standard and with the same lifecycle; VL/VT HA keeps two EBS copies (no tiering on EBS). Compute for Loki + Tempo and VL/VT is taken from scripts/cost/cost_model.py at 10 TB/day.

| 10 TB/day, retention | Lakehouse + lifecycle | Loki + Tempo, S3 Standard | Loki + Tempo + same lifecycle | VL/VT HA on EBS |
|---|---:|---:|---:|---:|
| 30 days | **$3,644** | $23,268 | $23,268 | $7,924 |
| 90 days | **$4,707** | $26,304 | $25,010 | $16,724 |
| 1 year | **$6,271** | $39,830 | $27,574 | $57,057 |
| 3 years | **$10,385** | $75,089 | $34,290 | $164k |
| 7 years | **$18,615** | $146k | $47,722 | $378k |

Loki + Tempo can use the same S3 lifecycle rules, and they help it too. Its compute baseline and replication-factor-3 network stay. VL/VT keeps data on EBS, where there is no storage-class tiering.

### Cumulative spend over time

Sum of monthly bills while data accumulates up to the retention window, then steady state. Storage, compute, network and requests.

| Ingest/day, retention | first 1 y | first 3 y | first 7 y |
|---|---:|---:|---:|
| 1 TB/day, 1 year | $6,263 | $21,312 | $51,411 |
| 1 TB/day, 3 years | $6,263 | $26,317 | $76,167 |
| 1 TB/day, 7 years | $6,263 | $26,317 | $95,647 |
| 10 TB/day, 1 year | $62,625 | $213k | $514k |
| 10 TB/day, 3 years | $62,625 | $263k | $762k |
| 10 TB/day, 7 years | $62,625 | $263k | $956k |
| 100 TB/day, 1 year | $622k | $2,118k | $5,110k |
| 100 TB/day, 3 years | $622k | $2,618k | $7,585k |
| 100 TB/day, 7 years | $622k | $2,618k | $9,533k |

The rows at 50 TB/day and above are projections: Lakehouse is not ready at that scale yet (see below).

## Where Lakehouse wins, and where it does not

- **Lowest-cost HA option from 0.1 to 10 TB per day** in this model. Lakehouse nodes are stateless over a single S3 copy, so HA does not double storage.
- **A single-replica ClickHouse on S3 is cheaper at long retention** (10 TB/day for a year: $9.6k vs $14.5k). It compresses better (about 12x vs 6.1x/9.4x), but open-source ClickHouse has no supported way to share one S3 copy between replicas (zero-copy replication is not recommended upstream), so that setup has **no compute HA**.
- **Above about 50 TB per day Lakehouse is not ready yet.** Every replica holds the full file manifest, manifest refresh lists the whole bucket, metadata has no eviction, and trace-ID lookup grows with file count. At 50 TB/day and 64 MB files that is about 40 million files a year. The large-scale rows show the cost after that work is done.
- **VL/VT on local disk compresses best and answers fastest**, but upstream HA means two independent clusters, so it is costly at long retention. The hybrid keeps VL/VT for the recent window and moves older data to Lakehouse. Moving data from VL/VT into Lakehouse by age is a design; today Lakehouse receives every write directly.
- **Loki + Tempo** costs more at every size because of replication-factor-3 ingesters, Kafka for Tempo, and lower compression.
- **OpenSearch** keeps an index about the size of the source plus a replica; keeping a year hot is shown for completeness, not as a realistic deployment.

## Prices

| Item | Price |
|---|---|
| S3 Standard | $0.023 / $0.022 / $0.021 per GB-month (first 50 TB / next 450 TB / above) |
| S3 PUT / GET | $0.005 / $0.0004 per 1,000 |
| S3 Standard-IA / Glacier Instant Retrieval / Glacier Deep Archive | $0.0125 / $0.004 / $0.00099 per GB-month; retrieval $0.01 / $0.03 per GB / restore first |
| Lifecycle transition requests (into Standard-IA / Glacier IR / Deep Archive) | $0.01 / $0.02 / $0.05 per 1,000 objects |
| EBS gp3 | $0.08 per GB-month |
| Cross-AZ transfer | $0.02 per GB (both directions) |
| m7g.xlarge / r7g.xlarge | $0.1632 / $0.2142 per hour (larger sizes scale linearly) |
| ClickHouse Cloud Scale | $0.2985 per unit-hour (2 vCPU + 8 GiB); storage $25.30 per compressed TB-month |

## Topology as modelled

| System | Data copies | Compute rule (per MB/s raw ingest, at 1 QPS) |
|---|---|---|
| Lakehouse | one S3 copy | assumed: 0.15 vCPU ingest (×2 insert nodes), 0.3 vCPU query, 4 GiB/vCPU |
| ClickHouse self-hosted | RF2 (`ReplicatedMergeTree`); tiered keeps two S3 copies | ClickStack guide, plus 3 Keeper nodes |
| ClickHouse Cloud | one copy (SharedMergeTree) plus backup | same vCPU as the guide, minimum 2 units |
| VL/VT HA | two independent clusters (VictoriaLogs does not replicate) | assumed: same ClickStack rule, split across the two clusters |
| Loki + Tempo | one S3 copy; ingesters RF3; Tempo on Kafka | Loki sizing tiers; Tempo sizing guide; Kafka 3 × 2 vCPU |
| OpenSearch | primary + 1 replica, ×1.45 headroom | AWS best-practice node sizing |
| Hybrid | VL/VT HA for 7 days, Lakehouse for older data | VL/VT as above, plus Lakehouse insert, compaction and cold-query pools |

See also: [Cost Comparison vs Loki and Tempo](cost-comparison.md), [Cross-AZ Optimization](cross-az-optimization.md), [ZSTD Compression Benchmark](zstd-compression-benchmark.md).
