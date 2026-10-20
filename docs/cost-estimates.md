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
python3 scripts/cost/cost_tiering.py         # long-term retention and S3 lifecycle tiering (storage and requests only, see below)
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
| S3 storage-class prices, retrieval, request, transition and minimum-duration rules | sourced | AWS price feed (publication 2026-10-15, us-east-1) and AWS S3 documentation, checked 2026-10-17; see the section below |
| Average Parquet object size of 64 MB (request and transition counts) | **assumed** | the target is 128 MiB of *estimated raw* bytes, so real objects are smaller; see the section below |
| Fraction of cold-class bytes read per month (ρ) | **assumed** | the access metrics that would measure it are not shipped yet; every cold-class total below states the ρ it uses |

**Cost is not performance.** See [Performance](performance.md) and the validated benchmark in `bench-results/baseline-2026-09`.

## Cost and cold storage classes: read this first

**The default is S3 Standard with no transitions.** Lakehouse writes every object to the bucket's default class and moves nothing: `stats.s3_lifecycle_rules`, `delete.lifecycle_rules` and `tenant.overrides.<tenant>.lifecycle` are all empty by default. Every cost figure in these pages that is not labelled **example ladder** is Standard. A ladder that moves older objects to Standard-IA or Glacier Instant Retrieval (Glacier IR) is something you add yourself on the bucket (you then copy the same rules into Lakehouse's config; nothing reads the bucket's lifecycle configuration, and the config copy feeds its cost estimates and delete decisions). Treat the ladders in these pages as worked examples, not recommendations.

**What a cold class costs.** A cold class is cheaper to store and dearer to use. Besides the storage price, AWS charges:
- **Retrieval, per GB read.** Every byte a query fetches from the class, including the ranged reads Lakehouse makes of single columns and row groups.
- **Higher request prices.** GET is 2.5x Standard on Standard-IA and 25x on Glacier IR.
- **Early deletion.** An object deleted, overwritten or moved out before the minimum duration is billed for the rest of it. A rewrite deletes the old object, so a compaction of a cold object (or the physical removal planned for deletes) pays retrieval and, inside the minimum, early deletion as well. Lakehouse avoids this by freezing tiered objects against compaction and by hiding deletes with tombstones ([operations](operations.md), [deletion strategy](deletion-strategy.md)).
- **A minimum billable object size** of 128 KB, and no default transition for objects under 128 KB.
- **A transition fee per 1,000 objects** moved in.

| Class | Storage $/GB-month | Retrieval $/GB | GET $/1,000 | PUT, COPY $/1,000 | Transition in $/1,000 | Minimum duration | Queryable in place |
|---|---:|---:|---:|---:|---:|---:|---|
| **Standard (default)** | 0.023 / 0.022 / 0.021 | 0 | 0.0004 | 0.005 | n/a | none | yes |
| Standard-IA | 0.0125 | 0.01 | 0.001 | 0.01 | 0.01 | 30 d | yes |
| Glacier Instant Retrieval | 0.004 | 0.03 | 0.01 | 0.02 | 0.02 | 90 d | yes |
| Glacier Flexible Retrieval | 0.0036 | restore first | restore first | not modelled | not modelled | 90 d | no |
| Glacier Deep Archive | 0.00099 | restore first | restore first | not modelled | 0.05 | 180 d | no |

Sources (us-east-1, **sourced**): the AWS price feed `https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json` (publication 2026-10-15, read 2026-10-17) and `https://aws.amazon.com/s3/pricing/`. Minimum durations and the 128 KB rules: `https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html` and `.../lifecycle-transition-general-considerations.html`, checked 2026-10-17. Standard storage is banded (first 50 TB / next 450 TB / over 500 TB). The Glacier Deep Archive storage price of $0.00099 per GB-month is a list price carried over from earlier versions of these pages: it is not a Glacier Deep Archive line in the us-east-1 feed file read on 2026-10-17 (only the Intelligent-Tiering Deep Archive Access tier carries that figure there), so treat it as unverified. Flexible Retrieval and Deep Archive restore charges, and the Deep Archive transition price, are not modelled here.

### Break-even: when a cold class stops paying

A cold class saves the storage difference and loses it to retrieval. With ρ = bytes read from the class per month divided by bytes held in it:

- Break-even ρ = (Standard price − class price) ÷ retrieval price.
- Standard-IA: (0.023 − 0.0125) ÷ 0.01 = **1.05**. Glacier IR: (0.023 − 0.004) ÷ 0.03 = **0.633**. In the 0.021 Standard band Glacier IR drops to 0.567.
- So a Glacier IR class loses to Standard once about **63% of its bytes are fetched in a month** (about 57% at over 500 TB), and Standard-IA once about 105%. Reads of the same bytes count again each time they are fetched, so a dashboard that re-reads the same cold week every minute can pass ρ = 1 on its own unless the cache absorbs it. The read fraction of real Lakehouse queries is **not measured yet**: it is an assumed input.

Reproduce:

```bash
python3 -c 'std=0.023
for n,p,r in (("Standard-IA",0.0125,0.01),("Glacier IR",0.004,0.03)): print(n, round((std-p)/r,3))'
```

**Worked example: 1 TB (1,000 GB) held for a month, first price band, request fees ignored.**

| Read per month (ρ) | Standard | Glacier IR |
|---|---:|---:|
| 0 | 1,000 × 0.023 = **$23.00** | 1,000 × 0.004 = **$4.00** |
| 0.1 (100 GB read) | $23.00 | 4.00 + 100 × 0.03 = **$7.00** |
| 0.633 (633 GB read) | $23.00 | 4.00 + 633 × 0.03 = **$22.99** (break-even) |
| 1 (all of it read once) | $23.00 | 4.00 + 1,000 × 0.03 = **$34.00** |

```bash
python3 -c '
for rho in (0, 0.1, 0.633, 1): print(rho, 1000*0.023, round(1000*0.004 + rho*1000*0.03, 2))'
```

**Minimum duration, same unit.** Glacier IR bills 90 days even when the object is deleted earlier. A GB that lives 10 days there costs 0.004 × 90 ÷ 30 = $0.0120, against 0.023 × 10 ÷ 30 = $0.0077 on Standard. Glacier IR only beats Standard for an object that stays at least 90 × 0.004 ÷ 0.023 = 15.7 days, so a ladder whose retention ends soon after the transition (for example moving to Glacier IR at day 90 with 100-day retention) pays more than it saves. The same arithmetic gives 16.3 days for Standard-IA (30 × 0.0125 ÷ 0.023).

**One query can cost real money.** A full scan of one stored day: at 10 TB/day raw the model stores about 1,409 GB per day (7.1x measured compression), which costs 1,409 × 0.01 = $14 to read from Standard-IA and 1,409 × 0.03 = $42 from Glacier IR, plus the GET delta. On Standard the same scan has no retrieval fee (about $0.07 of GETs at an assumed 8 MiB per ranged read). At 100 TB/day raw the day is about 14,091 GB: $141 and $423.

**Reversing is not free.** Moving data back from Glacier IR to Standard is a copy out of Glacier IR: $0.03 per GB retrieved plus the remainder of the 90-day minimum.

### How the totals below treat cold classes

The "S3 Standard only", "Standard-IA → Glacier IR" and "archive" blocks and the cumulative tables below count **storage, PUT requests and transition requests only**, so a cold-class cell in them is the best case ρ = 0. The "including reads" tables put read retrieval, the GET price difference and early deletion back, from the formulas above, and label the ρ used. `python3 scripts/cost/cost_tiering.py` prints every one of these tables. Nothing is hidden in the headline numbers: if a cold-class figure has no ρ next to it, it is a ρ = 0 storage figure.

### Object size: what the model assumes and what the code does today

- **Assumed (model):** an average Parquet object of 64 MB, which sets the PUT and transition counts. About 2,200 PUTs a day for 1 TB/day raw (141 GB stored), $0.33 a month, 15,625 objects per stored TB, and $0.31 per TB moved into Glacier IR.
- **Target (code):** `insert.target_file_size` defaults to 128 MiB, and compaction merges small objects towards it afterwards.
- **What the flush seal does today (code):** it is a row-count rule, not a byte rule. A segment is sealed when `rows × 512 B` (an assumed raw size per row, both signals) reaches the target, so 128 MiB ÷ 512 B = **262,144 rows** per group, cut at whole-second boundaries: a single second with more rows is one group. Parquet compresses, so the written object is smaller than 128 MiB by the compression ratio and by how far real rows are from 512 B.
- **Observed at low volume (sourced):** the production median object is about 71 KiB ([field-metadata test cells](perf/field-metadata-cells.md), issue 281 and the [market comparison](market-comparison.md): about 71 KB median at low volume). That is below the 128 KB threshold, so at low volume most objects are **not transitioned by default** and are billed as 128 KB in Standard-IA and Glacier IR. The ladder examples on this page therefore weaken at low volume: they assume the 64 MB average, and a bucket of small objects pays the transition and minimum-size overhead for little saving until compaction has merged them.
- **Anecdotal (one synthetic run, not reproducible from this repository):** a very compressible segment of 298,000 rows, all in one second, was flushed as one 12 MiB object (about 42 B per stored row). Treat it as an illustration of the row-count rule, not as a measurement. No production object-size distribution beyond the 71 KiB median is published here.
- **Why it matters for cost:** requests and transitions scale with the object count. At 12 MiB instead of 64 MB, 141 GB stored is about 11,200 PUTs a day ($1.68 a month, 5.1x) and a Glacier IR transition is about $1.59 per TB (79,500 objects) instead of $0.31. Storage and retrieval prices do not depend on object size, but Standard-IA and Glacier IR bill a 128 KB minimum per object, so very small objects can cost more in a cold class than in Standard.

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

**Optional, and off by default.** Everything on this page below the Standard-only block is an **example ladder** you would have to configure on the bucket yourself. The default is S3 Standard with no transitions (see [the section above](#cost-and-cold-storage-classes-read-this-first)), and each ladder total says which read fraction ρ it includes.

Lakehouse supports S3 lifecycle tiering (`lh.feature.storage.lifecycle_tiering`, shipped since v0.49.0; per-tenant schedules since v0.100.0). What it does and does not do:
- **Object count decides the request bill.** A ladder pays one transition request per object. The model assumes a 64 MB average object (15,625 per stored TB, $0.31 per TB into Glacier IR); real objects are smaller today, because the flush seal is by row count (see "Object size" above), and at 12 MiB the same transition costs about $1.59 per TB. Objects under 128 KB are not transitioned by default and are billed as 128 KB in the cold classes.
- **Escalating compression.** Compression escalates with age as compaction levels rise. The extra saving is not measured yet, so the tables below use the measured 6.1x / 9.4x throughout.
- **Rules and stats agree.** The transition itself is an S3 bucket lifecycle rule. The operator copies the same rules into Lakehouse's config (the lists are empty by default), globally or per tenant; nothing reads the bucket's lifecycle configuration. Tenant stats, cost estimates and the delete path use that copy, and the storage class of listed objects as the bucket listing reports it.
- **Deletes stay free by default.** Deletes on Standard-IA or Glacier objects are applied as tombstones, never as retrieval plus rewrite ([deletion strategy](deletion-strategy.md)). A forced rewrite pays retrieval and any early-deletion remainder.
- **Glacier IR stays queryable, at a price.** Glacier Instant Retrieval objects answer normal GETs in milliseconds; a query that reads them pays $0.03 per GB read (Standard-IA: $0.01 per GB) and 25x (2.5x) the GET price.
- **Deep Archive is for retention only.** Objects there must be restored before they can be queried; restore charges are not modelled.
- **Transition after compaction.** The first transition should come after compaction has finished with an object. Rewriting an object that is already in Standard-IA or Glacier IR costs a retrieval fee plus the rest of its minimum storage duration (30 and 90 days).

Reproduce every table below: `python3 scripts/cost/cost_tiering.py`. The first three blocks count **storage, PUTs and transition requests only** (ρ = 0, no early deletion, no GET price difference); the "including reads" tables add the read terms at the stated ρ (the script's `RHOS`).

Steady state (retention window full), 60/40 logs and traces, 7.1x measured compression, assumed 64 MB objects.

**S3 Standard only (the default)**

| Ingest/day | 30 days | 90 days | 1 year | 3 years | 7 years |
|---|---:|---:|---:|---:|---:|
| 1 TB | $98 | $292 | $1,183 | $3,446 | $7,972 |
| 10 TB | $976 | $2,845 | $11,368 | $32,970 | $76,174 |
| 50 TB | $4,718 | $13,896 | $54,585 | $163k | $379k |
| 100 TB | $9,385 | $27,229 | $109k | $325k | $757k |
| 500 TB | $45,116 | $134k | $541k | $1,621k | $3,781k |

**Example ladder: Standard 30d → Standard-IA → Glacier IR at 90d, storage and requests only (ρ = 0, best case)**

| Ingest/day | 30 days | 90 days | 1 year | 3 years | 7 years |
|---|---:|---:|---:|---:|---:|
| 1 TB | $98 | $204 | $360 | $772 | $1,595 |
| 10 TB | $976 | $2,039 | $3,602 | $7,717 | $15,946 |
| 50 TB | $4,718 | $10,035 | $17,851 | $38,425 | $79,572 |
| 100 TB | $9,385 | $20,019 | $35,652 | $76,799 | $159k |
| 500 TB | $45,116 | $98,289 | $176k | $382k | $794k |

No early deletion applies in this ladder at the retentions shown: Standard-IA holds each object 60 days (minimum 30) and Glacier IR is only reached after day 90, so retention of 180 days or more keeps every object at least 90 days there. A retention between 90 and 180 days does pay: at 10 TB/day and 120-day retention each month deletes 30 days of data after 30 days in Glacier IR, an early-deletion charge of 1,409 GB/day × 30 d × 0.004 × (90 − 30) ÷ 30 = **$338 a month**; at 100-day retention it is $451.

**Example ladder: Standard 30d → Glacier IR → Deep Archive after 1y (archive), storage and requests only (ρ = 0)**

| Ingest/day | 30 days | 90 days | 1 year | 3 years | 7 years |
|---|---:|---:|---:|---:|---:|
| 1 TB | $98 | $133 | $288 | $393 | $597 |
| 10 TB | $976 | $1,327 | $2,877 | $3,928 | $5,965 |
| 50 TB | $4,718 | $6,475 | $14,225 | $19,482 | $29,666 |
| 100 TB | $9,385 | $12,899 | $28,399 | $38,913 | $59,281 |
| 500 TB | $45,116 | $62,686 | $140k | $193k | $295k |

The 90-day column leaves out an early-deletion charge: objects reach Glacier IR at day 30 and are deleted at day 90, a 60-day residency against a 90-day minimum. The missing amount is stored GB per day × 0.004 × (90 − 60) ÷ 30 × 30 days, about **$169 a month at 10 TB/day** (1,409 GB × 0.12), $17 at 1 TB/day, $1,691 at 100 TB/day. Deep Archive columns also exclude restore charges, and Deep Archive data cannot be queried until restored.

### Totals including reads: Standard against the example ladder

10 TB/day raw, total monthly cost (storage, compute, cross-AZ network, requests). **Source of these totals: `scripts/cost/cost_tiering.py`, which prices Standard in the three AWS bands (first 51,200 GB, next up to 512,000 GB, above).** The headline 1-year figure in the 0.1 – 10 TB table above (Lakehouse $14,504) comes from `cost_model.py`, which prices Standard at a flat $0.023 and does not band it, so the Standard-only 1-year total reads $14,036 here: the 3.2% difference is storage banding only.

The ladder columns add (all printed by the script), on top of the ρ = 0 storage and requests: read retrieval = IA GB × ρ × 0.01 + Glacier IR GB × ρ × 0.03, and the GET price difference (IA +$0.0006, Glacier IR +$0.0096 per 1,000 GETs over Standard) at an **assumed** 8 MiB per ranged GET. Held bytes at 10 TB/day (1,409 GB per day): 84,548 GB in Standard-IA (60 days) from 90 days of retention onward, 387,513 GB in Glacier IR at 1 year (275 days), 1,416,184 GB at 3 years, 3,473,526 GB at 7 years.

| Retention | Lakehouse on **Standard (default)** | Ladder, ρ = 0 | Ladder, ρ = 0.01 | Ladder, ρ = 0.1 | Ladder, ρ = 1 |
|---|---:|---:|---:|---:|---:|
| 30 days | **$3,644** | $3,644 | $3,644 | $3,644 | $3,644 |
| 90 days | **$5,513** | $4,707 | $4,716 | $4,793 | $5,559 |
| 1 year | **$14,036** | $6,271 | $6,400 | $7,566 | $19,224 |
| 3 years | **$35,638** | $10,385 | $10,836 | $14,893 | $55,463 |
| 7 years | **$78,842** | $18,615 | $19,708 | $29,547 | $128k |

Worked cell (1 year, ρ = 0.1): $6,271 + 84,548 × 0.1 × 0.01 ($84.5) + 387,513 × 0.1 × 0.03 ($1,162.6) + GET difference (about $48) = **$7,566**.

How to read it:
- **At 30 days there is nothing to tier.** The ladder never starts.
- **The ladder saves a lot when cold data is rarely read:** of the all-in total, 55% at 1 year and 71% at 3 years at ρ = 0, and 46% and 58% at ρ = 0.1. An earlier version of these pages said storage drops "68% at 1 year and 79% at 7 years"; that was the storage line alone at ρ = 0.
- **It loses when the cold data is read heavily.** Break-even against Standard-only, with the same ρ applied to both cold classes: 0.95 at 90 days (Standard-IA only), **0.60 at 1 year, 0.56 at 3 years, 0.55 at 7 years** (assumed workload shape). At ρ = 1 (every cold byte fetched once a month) the ladder costs $19,224 against $14,036 at 1 year.
- **Check ρ before you configure it.** The access metrics that would tell you ρ are not shipped yet; until they are, the safe state is the default.

### The same policy against the alternatives

Same 10 TB/day workload, 60/40 mix, storage + compute + cross-AZ network + requests. Source: `scripts/cost/cost_tiering.py` for all columns, with Loki + Tempo compute and VL/VT compute taken from `scripts/cost/cost_model.py` at 10 TB/day. The ladder comparison applies the same ρ and the same assumed 8 MiB per ranged GET to both systems; Loki chunks are about 1.5 MB, so Loki would really issue more GETs (the GET price difference is small next to retrieval either way).

**Standard against Standard (the like-for-like comparison):**

| Retention | Lakehouse, Standard | Loki + Tempo, Standard | VL/VT HA on EBS (no tiering) |
|---|---:|---:|---:|
| 30 days | **$3,644** | $23,270 | $7,924 |
| 90 days | **$5,513** | $26,306 | $16,724 |
| 1 year | **$14,036** | $39,893 | $57,057 |
| 3 years | **$35,638** | $75,152 | $164k |
| 7 years | **$78,842** | $146k | $378k |

**Both on the same example ladder, ρ included** (Loki + Tempo uses its own stored bytes and 1.5 MB chunk objects; the same ρ is assumed for both, which is itself an assumption):

| Retention | Lakehouse ρ = 0.01 | Lakehouse ρ = 0.1 | Loki + Tempo ρ = 0.01 | Loki + Tempo ρ = 0.1 |
|---|---:|---:|---:|---:|
| 90 days | $4,716 | $4,793 | $25,025 | $25,151 |
| 1 year | $6,400 | $7,566 | $27,787 | $29,690 |
| 3 years | $10,836 | $14,893 | $35,027 | $41,649 |
| 7 years | $19,708 | $29,547 | $49,508 | $65,567 |

Loki + Tempo can use the same S3 lifecycle rules, and they help it too. Its compute baseline and replication-factor-3 network stay. VL/VT keeps data on EBS, where there is no storage-class tiering.

### Cumulative spend over time

Sum of monthly bills while data accumulates up to the retention window, then steady state. Storage, compute, network and requests (no read terms). Source: `scripts/cost/cost_tiering.py`.

**S3 Standard (the default):**

| Ingest/day, retention | first 1 y | first 3 y | first 7 y |
|---|---:|---:|---:|
| 1 TB/day, 1 year | $10,790 | $45,588 | $115k |
| 1 TB/day, 3 years | $10,790 | $73,117 | $251k |
| 1 TB/day, 7 years | $10,790 | $73,117 | $358k |
| 10 TB/day, 1 year | $105k | $442k | $1,116k |
| 10 TB/day, 3 years | $105k | $705k | $2,415k |
| 10 TB/day, 7 years | $105k | $705k | $3,438k |
| 100 TB/day, 1 year | $1,020k | $4,267k | $10,761k |
| 100 TB/day, 3 years | $1,020k | $6,894k | $23,757k |
| 100 TB/day, 7 years | $1,020k | $6,894k | $33,984k |

**Example ladder (Standard 30d → Standard-IA → Glacier IR at 90d), ρ = 0, so a best case that excludes read retrieval and early deletion:**

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
- **Above about 50 TB per day Lakehouse is not ready yet.** Every replica holds the full file manifest, manifest refresh lists the whole bucket, metadata has no eviction, and trace-ID lookup grows with file count. At 50 TB/day and an assumed 64 MB average object that is about 40 million files a year; smaller real objects (see "Object size" above) mean more. The large-scale rows show the cost after that work is done.
- **VL/VT on local disk compresses best and answers fastest**, but upstream HA means two independent clusters, so it is costly at long retention. The hybrid keeps VL/VT for the recent window and moves older data to Lakehouse. Moving data from VL/VT into Lakehouse by age is a design; today Lakehouse receives every write directly.
- **Loki + Tempo** costs more at every size because of replication-factor-3 ingesters, Kafka for Tempo, and lower compression.
- **OpenSearch** keeps an index about the size of the source plus a replica; keeping a year hot is shown for completeness, not as a realistic deployment.

## Prices

| Item | Price |
|---|---|
| S3 Standard | $0.023 / $0.022 / $0.021 per GB-month (first 50 TB / next 450 TB / above) |
| S3 PUT / GET (Standard) | $0.005 / $0.0004 per 1,000 |
| S3 Standard-IA / Glacier Instant Retrieval / Glacier Deep Archive | $0.0125 / $0.004 / $0.00099 per GB-month; retrieval $0.01 / $0.03 per GB / restore first |
| GET on Standard-IA / Glacier IR | $0.001 / $0.01 per 1,000 |
| Minimum storage duration (Standard-IA / Glacier IR / Deep Archive) | 30 / 90 / 180 days, early deletion billed pro rata; 128 KB minimum billable object size in Standard-IA and Glacier IR |
| Lifecycle transition requests (into Standard-IA / Glacier IR / Deep Archive) | $0.01 / $0.02 / $0.05 per 1,000 objects |
| EBS gp3 | $0.08 per GB-month |
| Cross-AZ transfer | $0.02 per GB (both directions) |
| m7g.xlarge / r7g.xlarge | $0.1632 / $0.2142 per hour (larger sizes scale linearly) |
| ClickHouse Cloud Scale | $0.2985 per unit-hour (2 vCPU + 8 GiB); storage $25.30 per compressed TB-month |

S3 prices: AWS price feed, us-east-1, publication 2026-10-15, read 2026-10-17 (sourced). Other rows: sourced, with the dates given at the top of this page and in [Cost Comparison](cost-comparison.md#1-aws-list-prices-used).

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
