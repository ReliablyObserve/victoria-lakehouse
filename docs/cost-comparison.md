---
title: Cost Comparison
sidebar_position: 17
---

# Victoria Lakehouse vs Loki and Tempo: where the money goes

This page explains **why** Lakehouse costs what it costs on AWS, line by line, and why its cost grows in a straight line with ingest and retention. Every number is either an AWS list price or output of the cost model in `scripts/cost/` (see [Cost Estimates](cost-estimates.md) for all systems and all sizes).

**Assumptions used everywhere on this page:**
- AWS us-east-1, on-demand, Linux.
- Logs and traces together, 60% logs and 40% traces by raw volume, at 1 query per second.
- Every system is deployed for production HA.
- **Lakehouse CPU is assumed**: the same per-MB/s rule ClickHouse publishes for its own sizing, until Lakehouse's is measured.
- Loki and Tempo sizing comes from Grafana's published guides.
- **Every Lakehouse figure is on S3 Standard unless it says "example ladder".** Lakehouse's default is S3 Standard with no transitions. Cold classes (Standard-IA, Glacier Instant Retrieval) are cheaper to store but charge per GB read, per early deletion and per transition: read [Cost and cold storage classes: read this first](cost-estimates.md#cost-and-cold-storage-classes-read-this-first) before using any ladder number.

## 1. AWS list prices used

Compute and EBS prices fetched 2026-10-01 from AWS's public price feed, us-east-1; S3 prices re-read 2026-10-17 (below).

**Compute (EC2 on-demand, Linux)**

| Instance | vCPU | RAM | $/hour | $/month (730 h) |
|---|---:|---:|---:|---:|
| m7g.xlarge | 4 | 16 GiB | 0.1632 | 119.14 |
| r7g.xlarge | 4 | 32 GiB | 0.2142 | 156.37 |
| m7g.8xlarge | 32 | 128 GiB | 1.3056 | 953.09 |
| r7g.8xlarge | 32 | 256 GiB | 1.7136 | 1,250.93 |

The small-scale model prices compute per vCPU at the m7g rate ($0.0408 per vCPU-hour, which includes 4 GiB). RAM beyond 4 GiB per vCPU is charged at the r7g−m7g difference ($0.0032 per GiB-hour). The large-scale model counts whole r7g.8xlarge and m7g.8xlarge nodes.

**Block storage (EBS gp3)**

| Item | Price |
|---|---|
| Storage | $0.08 per GB-month |
| Included per volume | 3,000 IOPS and 125 MB/s |
| Extra IOPS / throughput | $0.005 per IOPS-month / $40.96 per GiB/s-month (not modelled; would raise every EBS-based row) |

**Object storage (S3)**

| Storage class | $ per GB-month | Retrieval | GET per 1,000 | Notes |
|---|---:|---:|---:|---|
| **S3 Standard, first 50 TB (the default)** | 0.023 | free | $0.0004 | no minimum duration |
| S3 Standard, next 450 TB | 0.022 | free | $0.0004 | |
| S3 Standard, over 500 TB | 0.021 | free | $0.0004 | |
| S3 Standard-IA | 0.0125 | $0.01 per GB | $0.001 | 30-day minimum storage duration, early deletion billed pro rata; 128 KB minimum billable size |
| S3 Glacier Instant Retrieval | 0.004 | $0.03 per GB | $0.01 | millisecond GETs; 90-day minimum, early deletion billed pro rata; 128 KB minimum billable size |
| S3 Glacier Flexible Retrieval | 0.0036 | restore first | restore first | 90-day minimum; not queryable in place |
| S3 Glacier Deep Archive | 0.00099 | restore first | restore first | 180-day minimum; archive only |

S3 prices (sourced): AWS price feed, us-east-1, publication 2026-10-15, read 2026-10-17 (`https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json`, `https://aws.amazon.com/s3/pricing/`). The Glacier Deep Archive price ($0.00099) is a carried-over list price that the us-east-1 feed file read on 2026-10-17 does not list as a Glacier Deep Archive line (only the Intelligent-Tiering Deep Archive Access tier carries it): unverified. Minimum durations and the 128 KB rule: AWS S3 documentation (storage classes and lifecycle transition considerations), checked 2026-10-17. Compute and EBS rows above were fetched 2026-10-01.

| S3 request | Price |
|---|---|
| PUT, COPY, POST, LIST (Standard) | $0.005 per 1,000 |
| GET and others (Standard) | $0.0004 per 1,000 |
| Lifecycle transition into Standard-IA / Glacier Instant Retrieval | $0.01 / $0.02 per 1,000 objects |
| Data between S3 and EC2 in the same region | free |

**Network**

| Item | Price |
|---|---|
| Cross-AZ transfer between instances | $0.01 per GB each direction ($0.02 per GB round trip) |

## 2. Lakehouse in one picture: two pod types and one bucket

```mermaid
flowchart LR
    A["Agents and SDKs<br/>(OTLP, Loki push, ES bulk,<br/>Splunk, Datadog, jsonline…)"] --> I["insert pods<br/>(stateless + local buffer)"]
    I -- "one Parquet object per flush<br/>(S3 PUT, same region, no transfer fee)" --> S3[("S3 bucket<br/>one copy, 11-nines durability,<br/>replicated across AZs by S3")]
    C["compaction pods"] -- "merge small files" --> S3
    S3 --> Q["select pods<br/>(stateless + cache)"]
    I -. "unflushed rows<br/>(buffer bridge)" .-> Q
    Q --> G["Grafana, Jaeger UI,<br/>VMUI/VTUI, APIs"]
    S3 --> X["DuckDB, ClickHouse,<br/>Spark, Trino, pyarrow"]
```

- **Insert pods** accept every VictoriaLogs/VictoriaTraces protocol, buffer locally, and flush compressed Parquet to S3.
- **Select pods** answer queries from S3 plus the insert pods' unflushed rows.
- **Compaction** merges small files in the background.

None of them owns data. S3 is the only copy, and S3 already spreads it across availability zones, so **HA never multiplies storage**: add pods, not copies.

Loki + Tempo for comparison:

```mermaid
flowchart LR
    A["Agents"] --> D["distributors"]
    D -- "replication factor 3<br/>(2 extra cross-AZ copies)" --> IN["Loki ingesters<br/>(WAL on disk)"]
    D --> K["Kafka (RF3, 24 h)<br/>for Tempo 3.x"]
    K --> LS["Tempo live-store"]
    IN --> S3[("S3: Loki chunks<br/>+ TSDB index")]
    LS --> S3T[("S3: Tempo vParquet blocks")]
    S3 --> QL["Loki queriers + frontends<br/>+ compactor + caches"]
    S3T --> QT["Tempo queriers + compactor"]
```

## 3. Unit economics: what one raw TB costs to keep

Compression decides how many bytes each system stores per raw byte ingested. The copies decide how many times you pay for them.

| System | Effective compression (60/40 mix) | Stored per raw TB | Copies paid for | Medium | **$ per raw TB per month kept** |
|---|---:|---:|---:|---|---:|
| **Lakehouse** | **7.1x** (logs 6.1x, traces 9.4x, measured) | 141 GB | **1** | S3 Standard | **$3.24** |
| *Example only:* Lakehouse with every byte in Glacier Instant Retrieval | 7.1x | 141 GB | 1 | S3 Glacier IR | $0.56 at zero reads; each GB read adds $0.03, so it costs more than Standard once about 63% of the bytes are read in a month; 90-day minimum |
| Loki + Tempo | 4.35x (logs ~4x, traces ~5x, typical) | 230 GB | 1 | S3 Standard | $5.29 |
| ClickHouse tiered (data older than 7 days) | 12x (vendor guide) | 83 GB | 2 (no supported shared copy) | S3 Standard | $3.83 |
| ClickHouse on EBS / first 7 days | 12x | 83 GB | 2, plus 25% free-space headroom | EBS gp3 | $16.67 |
| VictoriaLogs / VictoriaTraces HA | 13.6x (vendor range) | 73 GB | 2 clusters, plus 20% free space | EBS gp3 | $14.67 |

How to read this:
- **VL/VT and ClickHouse compress better than Lakehouse.** They lose the storage line anyway once they keep data on EBS, because EBS costs 3.5x S3 per GB and HA doubles it.
- **Against Loki + Tempo**, Lakehouse stores about **40% fewer bytes** for the same raw data (ZSTD Parquet vs Loki chunks), in one copy on the same S3.
- **Retention is where this compounds.** Keeping 1 TB/day for a year means about 365 raw TB retained:
  - Lakehouse: **~$1,180 per month** of storage.
  - Loki + Tempo: about $2,050.
  - VL/VT HA on EBS: about $5,350.

## 4. Where the money goes, line by line

Monthly, mid estimate, from `python3 scripts/cost/cost_model.py`.

### 1 TB/day, 1-year retention (storage-dominated)

| | Storage | Compute | Cross-AZ network | S3 requests | **Total** |
|---|---:|---:|---:|---:|---:|
| **Lakehouse** | $1,183 (81%) | $207 (14%) | $60 (4%) | $3 | **$1,453** |
| Loki + Tempo | $2,051 (66%) | $717 (23%) | $300 (10%) | $20 | $3,088 |
| ClickHouse 7d EBS + S3, RF2 | $1,489 (76%) | $351 (18%) | $110 (6%) | $4 | $1,954 |
| VL/VT HA on EBS | $5,353 (94%) | $172 (3%) | $180 (3%) | $0 | $5,706 |

### 10 TB/day, 30-day retention (compute and network matter)

| | Storage | Compute | Cross-AZ network | S3 requests | **Total** |
|---|---:|---:|---:|---:|---:|
| **Lakehouse** | $972 (27%) | $2,068 (57%) | $600 (16%) | $6 | **$3,646** |
| Loki + Tempo | $2,787 (12%) | $17,492 (75%) | $3,000 (13%) | $156 | $23,435 |
| ClickHouse 7d EBS + S3, RF2 | $2,048 (40%) | $1,902 (38%) | $1,100 (22%) | $18 | $5,069 |
| VL/VT HA on EBS | $4,400 (56%) | $1,724 (22%) | $1,800 (23%) | $0 | $7,924 |

### The resources behind those numbers (10 TB/day)

| | vCPU | RAM | EBS | S3 kept (30 d / 1 y) | Cross-AZ per month |
|---|---:|---:|---:|---:|---:|
| **Lakehouse** | **69** | 278 GiB | **0** | 42 TB / 514 TB | **30 TB** |
| Loki + Tempo | 587 | 1,511 GiB | 15 TB (Kafka) | 69 TB / 840 TB | 150 TB |
| ClickHouse 7d EBS + S3, RF2 | 64 | 255 GiB | 15 TB | 38 TB / 597 TB | 55 TB |
| VL/VT HA on EBS | 58 | 231 GiB | 55 TB / 669 TB | 0 | 90 TB |

### What drives each line

**Storage** = stored bytes × copies × price per GB. Lakehouse pays S3 Standard on one copy (section 3). Nothing sits on EBS: insert pods use a small local disk for the buffer and select pods a cache disk, both sized by throughput, not by retention.

**Compute** follows the ingest rate and query load, not retention.
- **Lakehouse:** insert at 0.15 vCPU per MB/s with two insert nodes minimum, plus select at 0.3 vCPU per MB/s at 1 QPS. **This is assumed** until measured. At 10 TB/day (116 MB/s raw) that is 69 vCPU.
- **Loki:** Grafana's sizing tier for 3–30 TB/day is a fixed baseline of 410 vCPU and 780 GiB before frontends, compactor and caches (+30%). That is why Loki + Tempo compute jumps at 3 TB/day and dominates at 10 TB/day. Well-tuned deployments can run leaner than the vendor baseline.
- **Tempo:** distributors, live-store and queriers per the Tempo guide, plus a 3-broker Kafka.

**Cross-AZ network** is where replication shows up on the AWS bill.

| System | Cross-AZ bytes per ingested wire byte | Why |
|---|---:|---|
| **Lakehouse** | **0.5** | Only clients crossing AZs to reach an insert pod. Insert → S3 is same-region and free; S3 replicates across AZs at no transfer charge. |
| ClickHouse RF2 | 0.5 + stored replication | `ReplicatedMergeTree` ships every part to the replica. |
| VL/VT HA | 1.5 | Every write goes to two independent clusters. |
| Loki + Tempo | 2.5 | Replication factor 3: two extra copies of every write cross AZs; Tempo's Kafka is RF3. |

**S3 requests** are small for everyone. The model **assumes** a 64 MB average Lakehouse object, so 1 TB/day raw (141 GB stored) is about 2,200 PUTs a day ($0.33 a month). That is an assumption, not a measurement: the flush seal is by estimated row count, real objects are smaller today (production median about 71 KiB at low volume; one anecdotal synthetic segment became a 12 MiB object), and at 12 MiB the same data is about 11,200 PUTs a day ($1.68 a month). See [Object size](cost-estimates.md#cost-and-cold-storage-classes-read-this-first). Loki's ~1.5 MB chunks need about 40x more PUTs per stored byte. The model counts one write per stored object plus a fixed 200,000 GETs a day for every system. Compaction rewrites are not counted for any system.

## 5. Scaling: linear, and two knobs

From `python3 scripts/cost/cost_model_scale.py`: whole r7g.8xlarge / m7g.8xlarge nodes, production HA, 1-year retention.

| Ingest/day | Lakehouse pods (m7g.8xlarge) | Lakehouse compute | Lakehouse S3 kept | **Lakehouse total** | Loki + Tempo nodes | Loki + Tempo total |
|---|---|---:|---:|---:|---|---:|
| 100 TB | 9 insert + 17 select + 3 compaction | $28k | 5.1 PB | **$142k** | 158 r7g.8xlarge + Kafka 28 × m7g.8xlarge | $445k |
| 500 TB | 40 insert + 79 select + 13 compaction | $126k | 25.7 PB | **$697k** | 785 r7g.8xlarge + Kafka 139 × m7g.8xlarge | $2,214k |

**Lakehouse is not ready at these sizes yet**: every replica holds the full file manifest, and metadata grows with file count, which at an assumed 64 MB average object is about 80 million files a year at 100 TB/day (more if real objects are smaller). These rows show the cost once that work is done ([limits](cost-estimates.md#where-lakehouse-wins-and-where-it-does-not)).

Why the Lakehouse column is a straight line:
- **Two independent knobs.** Ingest rate sets the insert (and compaction) pod count, about 8 insert pods per 100 TB/day at the assumed rule. Query load sets the select pod count. Retention sets only the S3 line. None of these affects the others.
- **No data placement.**
  - Adding pods moves no data: there are no shards to rebalance, no replicas to resync and no disks to grow.
  - ClickHouse adds shards and must place data on them.
  - VL/VT on EBS grows nodes with retention, because every node's disk fills.
  - Loki and Tempo scale ingesters behind replication factor 3 and Kafka partitions.
- **Autoscaling is plain HPA.** Select pods are stateless; scale them on query load and to zero-ish off-hours. A select pod restart costs a cache warm-up, not a data recovery.
- **Retention is a single S3 lifecycle rule.** Lengthening retention adds S3 bytes and nothing else (on Standard, the default). Optionally, an example ladder moves older objects to a cheaper class; Glacier Instant Retrieval stores at $0.004 against $0.023 per GB-month (83% less) and stays queryable, but at $0.03 per GB read back, so that saving holds only while the cold data is rarely read ([break-even](cost-estimates.md#break-even-when-a-cold-class-stops-paying)).
  - Transition fees follow the object count. At the assumed 64 MB average object that is about 15,600 objects per stored TB, about $0.31 per TB moved into Glacier IR; at 12 MiB (an anecdotal synthetic object, used here only as an illustration) about 79,500 objects, $1.59 per TB.
  - Loki's 1.5 MB chunks need about 670,000 objects per TB, about $13 per TB.
  - Objects must stay 90 days in Glacier IR (30 in Standard-IA) before compaction or a delete rewrites them, or AWS bills the remainder. Lakehouse freezes tiered objects against compaction for that reason.

## 6. Long retention: Standard by default, cold-class ladders as an option

Lakehouse objects are large, immutable and compacted, so you *can* add S3 lifecycle rules that move older ones to cheaper storage classes while they stay queryable. Lakehouse does not do this for you: the default is S3 Standard and the rule lists are empty. An example ladder would be:
- S3 Standard for the first 30 days.
- Standard-IA until day 90.
- Glacier Instant Retrieval after that.

If you configure rules, you also copy them into Lakehouse's config (globally or per tenant; nothing reads the bucket's lifecycle configuration) so its cost stats and deletes know where each object lives, and deletes on cold classes are tombstones that never trigger retrieval fees. The full calculator for 1–500 TB/day and 30 days to 7 years, with the read-retrieval, early-deletion and request terms included, is in [Cost Estimates](cost-estimates.md#long-term-retention-and-s3-storage-class-tiering) (`python3 scripts/cost/cost_tiering.py` prints every table).

All columns: storage + compute + cross-AZ network + requests, 10 TB/day raw, from `scripts/cost/cost_tiering.py` (banded Standard prices). Loki + Tempo compute and VL/VT compute are taken from `scripts/cost/cost_model.py` at 10 TB/day. The headline 1-year Lakehouse figure in section 4 ($14,504, `cost_model.py`) prices Standard at a flat $0.023; the banded 1-year Standard figure here is $14,036 (3.2% lower, storage banding only).

**Standard against Standard (the like-for-like comparison, all assumed or sourced as above):**

| 10 TB/day, retention | Lakehouse, S3 Standard | Loki + Tempo, S3 Standard | VL/VT HA on EBS |
|---|---:|---:|---:|
| 30 days | **$3,644** | $23,270 | $7,924 |
| 90 days | **$5,513** | $26,306 | $16,724 |
| 1 year | **$14,036** | $39,893 | $57,057 |
| 3 years | **$35,638** | $75,152 | $164k |
| 7 years | **$78,842** | $146k | $378k |

**Example ladder (Standard 30 d, Standard-IA to day 90, Glacier IR after), with reads included.** ρ is the fraction of the cold bytes read from S3 per month and is **assumed**; the right-hand pair puts Loki + Tempo on the same ladder at the same ρ.

| 10 TB/day, retention | Lakehouse ρ = 0 | Lakehouse ρ = 0.01 | Lakehouse ρ = 0.1 | Loki + Tempo ρ = 0.01 | Loki + Tempo ρ = 0.1 |
|---|---:|---:|---:|---:|---:|
| 30 days (no tiering) | $3,644 | $3,644 | $3,644 | $23,270 | $23,270 |
| 90 days | $4,707 | $4,716 | $4,793 | $25,025 | $25,151 |
| 1 year | $6,271 | $6,400 | $7,566 | $27,787 | $29,690 |
| 3 years | $10,385 | $10,836 | $14,893 | $35,027 | $41,649 |
| 7 years | $18,615 | $19,708 | $29,547 | $49,508 | $65,567 |

At 10 TB/day:
- **3 years on Standard:** Lakehouse costs about **$35.6k a month in total**, against $75.1k for Loki + Tempo on S3 Standard and $164k for VL/VT HA on EBS.
- **3 years on the example ladder at ρ = 0.1:** $14.9k against $41.6k for Loki + Tempo on the same ladder. At ρ = 0 the ladder is $10.4k.
- **The ladder is not free money:** against Lakehouse on Standard it saves 46% of the all-in total at 1 year and 58% at 3 years when 10% of the cold bytes are read each month, and it loses once about 55–60% of them are (at ρ = 1, 1-year cost is $19,224 against $14,036 on Standard).
- **Ladder rules to respect:** keep retention at 180 days or more (or the Glacier IR hop pays early deletion), and do not rewrite objects after they move.

## 7. What these numbers do not say

- **Lakehouse CPU is assumed.** At 30-day retention compute is about 57% of the Lakehouse bill, so a 2x error there moves those totals noticeably. At 1-year retention storage dominates and the ranking holds.
- **Lakehouse vs Loki and Tempo performance has not been benchmarked.** The only measured comparison is VictoriaLogs behind loki-vl-proxy vs Loki: on 36 Grafana Logs Drilldown queries over 8 M log entries, Loki returned data for 9 and silently empty results for 13, while the proxy over VictoriaLogs returned data for 35 and was 10–24x faster where both answered ([loki-vl-proxy measurements](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/docs/honest-tldr.md)). That is VictoriaLogs, not Lakehouse.
- **Not modelled for any system:**
  - Reserved instances and savings plans; these lower compute for every row, most for compute-heavy rows.
  - Extra EBS IOPS or throughput.
  - Compaction request costs.
  - Egress to the internet.
  - For cold-class ladders: restore charges for Glacier Flexible Retrieval and Deep Archive, rewrite (delete or compaction) retrieval, and any read fraction other than the ρ stated next to a figure.
- **Object sizes are assumed.** Request and transition counts use a 64 MB average object; the flush seal is by estimated rows, real objects are smaller today (production median about 71 KiB at low volume, under the 128 KB cold-class minimum; see [Object size](cost-estimates.md#cost-and-cold-storage-classes-read-this-first)), and no production distribution beyond that median is published here.
- **A single-replica ClickHouse on S3 is cheaper** than Lakehouse at long retention, because it compresses better, but it has no compute HA. See [Cost Estimates](cost-estimates.md).
