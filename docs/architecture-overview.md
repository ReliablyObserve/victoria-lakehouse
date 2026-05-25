# Victoria Lakehouse Architecture Overview

## System Architecture

```mermaid
graph TB
    subgraph "Victoria Lakehouse"
        subgraph "Ingestion Layer"
            VLI[VL Insert Handlers<br/>Logs :9428]
            VTI[VT Insert Handlers<br/>Traces :10428]
        end

        subgraph "Query Layer"
            VLS[VL Select Handlers<br/>LogsQL API]
            VTS[VT Select Handlers<br/>Traces API]
            JAE[Jaeger API<br/>gRPC + HTTP]
            LOKI[Loki Proxy<br/>Compatibility]
        end

        subgraph "Storage Layer (parquets3)"
            BW[BatchWriter<br/>WAL + Parquet Flush]
            RQ[RunQuery<br/>Parallel File Scan]
            FA[Field APIs<br/>field_names/values<br/>stream_fields/ids]
        end

        subgraph "Query Optimization Stack"
            FPD[Filter Pushdown]
            BI[Bloom Index<br/>Per-partition + Per-file]
            MFP[Manifest Fast Path<br/>Zero-S3 Stats]
            RGS[RG Column Stats<br/>Min/Max Skip]
            PRJ[Projection<br/>Column Pruning]
            PRW[PreWhere<br/>Early Filter]
            SF[Self-Filter<br/>Hash-Ring Ownership]
            CA[Cache Affinity<br/>Footer-first Sort]
            TB[Token Bloom<br/>Body Text Pruning]
            LI[Label Index<br/>Fast Field Lookup]
        end

        subgraph "Cache Hierarchy"
            L1[L1: BudgetedL1<br/>Memory LRU]
            L2[L2: Disk LRU<br/>SmartCache Controller]
            L3[L3: Peer Cache<br/>Consistent Hash Ring]
            SC[SmartCache Controller<br/>TTL + Hot + LRU + Pin]
            SP[Snapshot Persistence<br/>Versioned Envelope]
            SZ[Sizing Calculator<br/>Ingestion + Query Based]
            CC[Chunk Cache<br/>Column-level]
            CP[Column Popularity<br/>Adaptive Caching]
            PG[Pollution Guard<br/>Scan Protection]
            FC[Footer Cache<br/>Parquet Metadata]
        end

        subgraph "Infrastructure"
            MAN[Manifest<br/>File Registry + Stats]
            CMP[Compaction<br/>L0 to L1 Merge]
            PFE[Prefetch Engine<br/>Cross-Signal + ReadAhead]
            BLC[Bloom Cache<br/>LRU + Lazy S3 Load]
            DSC[Discovery<br/>S3 + DNS SRV]
            PR[Peer Ring<br/>AZ-Aware Routing]
            HT[Health Tracking<br/>Failure Counting]
        end

        subgraph "Tenant & Stats"
            TR[Tenant Resolver<br/>OrgID Mapping]
            TS[Tenant Sync<br/>Fleet CRDT Merge]
            ST[Stats Registry<br/>Per-Tenant Metrics]
            CE[Cost Estimator<br/>Storage Class Aware]
            UI[Explorer UI<br/>3-Tab Preact App]
            VUI[VMUI Tab<br/>Injected Dashboard]
            CAR[Cardinality API<br/>Field Explorer]
        end

        subgraph "Cross-Signal"
            CSC[Cross-Signal Client<br/>Batched Hints]
            CSH[Cross-Signal Handler<br/>Hint Receiver]
            EVH[Eviction Hints<br/>Connected Data]
        end

        subgraph "S3 I/O Optimization"
            RAB[Read-Ahead Buffer<br/>256KB Streaming]
            RCO[Range Coalescing<br/>64KB Gap Merge]
            HTT[HTTP/2 Transport<br/>Connection Tuning]
            RGW[RG Worker Pool<br/>8 Concurrent Workers]
            FPF[Footer Prefetch<br/>Batch S3 Range Reads]
        end

        subgraph "K8s Integration"
            AZD[AZ Detection<br/>Env/IMDS/GCP/K8s]
            STM[Startup Manager<br/>Phase-Based Init]
            SHD[Shutdown Handler<br/>Graceful Drain]
            BB[Buffer Bridge<br/>AZ-Aware Fan-Out]
        end
    end

    subgraph "External"
        S3[(S3 / MinIO<br/>Parquet Storage)]
        VL[VictoriaLogs<br/>Upstream Insert]
        VT[VictoriaTraces<br/>Upstream Insert]
    end

    subgraph "Deployment (Helm)"
        INS[Insert Pods<br/>StatefulSet + PV]
        SEL[Select Pods<br/>StatefulSet + PV]
        COM[Compaction Pods<br/>StatefulSet]
        HPA[HPA<br/>CPU Autoscaling]
        PDB[PDB<br/>Availability]
    end

    VLI --> BW
    VTI --> BW
    VLS --> RQ
    VTS --> RQ
    JAE --> RQ
    LOKI --> VLS

    BW --> S3
    RQ --> FPD & BI & MFP
    RQ --> L1 --> L2 --> L3
    RQ --> RAB --> RCO --> S3

    SC --> L2
    SC --> SP
    SC --> SZ

    CSC <--> CSH
    PFE --> SC

    TR --> TS
    ST --> CE
    UI --> ST
    VUI --> UI

    INS --> VLI & VTI
    SEL --> VLS & VTS
    COM --> CMP
```

## Data Flow

```mermaid
flowchart LR
    subgraph "Write Path"
        A[Client] -->|HTTP POST| B[VL/VT Insert Handler]
        B --> C[BatchWriter]
        C -->|Buffer| D{Flush Trigger}
        D -->|Size/Time| E[Parquet Writer]
        E --> F[S3 Upload]
        E --> G[Bloom Observer]
        G --> H[Per-file .bloom Sidecar]
        G --> I[Partition _bloom.bin]
        E --> J[Metadata Sidecar]
        E --> K[Cache-on-Flush L2]
        F --> L[(S3 Bucket)]
    end
```

```mermaid
flowchart LR
    subgraph "Read Path"
        A[Client] -->|LogsQL Query| B[Select Handler]
        B --> C[RunQuery]
        C --> D[Manifest: GetFilesForRange]
        D --> E[Label Index Filter]
        E --> F[Bloom Filter]
        F --> G[Manifest Fast Path?]
        G -->|Yes: count/hits| H[Synthetic Block]
        G -->|No: full scan| I[Parallel File Workers x8]
        I --> J[L1 Memory Check]
        J -->|Miss| K[Hash Ring: Owner?]
        K -->|Local| L[L2 Disk Cache]
        K -->|Remote| M[L3 Peer Fetch]
        L -->|Miss| N[S3 Download]
        M -->|Miss| N
        N --> O[Read-Ahead + Coalescing]
        O --> P[Filter Pushdown]
        P --> Q[RG Column Stats Skip]
        Q --> R[Row Scan + Project]
        R --> S[DataBlock Output]
    end
```

## Cache Architecture

```mermaid
flowchart TB
    subgraph "SmartCache Controller"
        direction TB
        GET[Get Request] --> L1C{L1 Hit?}
        L1C -->|Yes| RET[Return Data]
        L1C -->|No| OWN{Owner Check<br/>Hash Ring}
        OWN -->|Local| L2C{L2 Disk Hit?}
        OWN -->|Remote| PEER[L3 Peer Fetch]
        L2C -->|Yes| PROM[Promote to L1]
        L2C -->|No| S3DL[S3 Download<br/>Singleflight Dedup]
        PEER -->|Found| L1ONLY[Store L1 Only]
        PEER -->|Miss| S3DL
        S3DL --> STORE[Store L1 + L2]
        STORE --> META[Update Metadata<br/>AccessCount++]
    end

    subgraph "Eviction Priority (lowest first)"
        E1["1. Expired Cold<br/>createdAt > maxAge"]
        E2["2. Unpinned Cold<br/>Below hot threshold, LRU"]
        E3["3. Unpinned Hot<br/>lastAccess > maxAge"]
        E4["4. Pinned<br/>Active query, NEVER evict"]
    end

    subgraph "Watermark Eviction"
        WM["If diskUsage > 90% diskLimit<br/>→ LRU fallback eviction"]
    end
```

## Bloom Index Architecture

```mermaid
flowchart TB
    subgraph "Write Path"
        FLUSH[File Flush] --> OBS[BloomObserver.OnFileFlush]
        OBS --> PFB[Per-file .bloom Sidecar<br/>S3 Upload]
        OBS --> PI[PartitionedIndex.AddFile]
        PI --> DIRTY[Mark Partition Dirty]
        DIRTY --> PERSIST[PersistDirty<br/>S3: partition/_bloom.bin]
        PERSIST --> MMETA[Manifest SetBloomMeta]
    end

    subgraph "Read Path"
        QUERY[Query with Filters] --> BF[bloomFilterFiles]
        BF --> BC{BloomCache<br/>Per-Partition}
        BC -->|Hit| CHECK[Check Columns]
        BC -->|Miss| S3LOAD[Lazy S3 Load<br/>partition/_bloom.bin]
        S3LOAD --> CHECK
        CHECK --> SKIP{Contains Match?}
        SKIP -->|No| PRUNE[Prune File]
        SKIP -->|Yes| KEEP[Keep File]

        QUERY --> FBL[Per-file .bloom Check]
        FBL --> FSKIP{File Bloom Hit?}
        FSKIP -->|No| PRUNE2[Prune File]
        FSKIP -->|Yes| KEEP2[Keep File]
    end

    subgraph "Tiering"
        T1["Per-RG Bloom<br/>Recent data (hours)"]
        T2["Per-File Bloom<br/>Medium age (days)"]
        T3["Summary Bloom<br/>Old data (weeks+)"]
    end
```

## Deployment Topology

```mermaid
graph TB
    subgraph "Kubernetes Cluster"
        subgraph "Insert Tier (StatefulSet)"
            I1[insert-0<br/>WAL + S3 Flush]
            I2[insert-1<br/>WAL + S3 Flush]
        end

        subgraph "Select Tier (StatefulSet)"
            S1[select-0<br/>Query + Cache]
            S2[select-1<br/>Query + Cache]
            S3S[select-2<br/>Query + Cache]
        end

        subgraph "Compaction (StatefulSet)"
            C1[compaction-0<br/>L0→L1 Merge]
        end

        VMAUTH[vmauth<br/>Request Router]
        HPA1[HPA<br/>CPU Scaling]
        PDB1[PDB<br/>minAvailable]
    end

    LB[Load Balancer] --> VMAUTH
    VMAUTH -->|/insert/| I1 & I2
    VMAUTH -->|/select/| S1 & S2 & S3S

    S1 <-->|Peer Cache<br/>Hash Ring| S2
    S2 <-->|Peer Cache<br/>Hash Ring| S3S

    I1 & I2 --> S3B[(S3 Bucket<br/>Parquet Files)]
    S1 & S2 & S3S --> S3B
    C1 --> S3B

    HPA1 -.->|Scale| S1 & S2 & S3S
    PDB1 -.->|Protect| S1 & S2 & S3S
```

## Spec Compliance Status

```mermaid
pie title Feature Implementation Status (213 Requirements)
    "Implemented (132)" : 132
    "Smart Cache Gaps (11)" : 11
    "Tenant Stats Gaps (12)" : 12
    "Tenant Mapping Gaps (6)" : 6
    "Bloom Index Gaps (10)" : 10
    "S3 I/O Gaps (16)" : 16
    "K8s Safety Gaps (26)" : 26
```

| Spec | Requirements | Implemented | Gap | Status |
|------|-------------|-------------|-----|--------|
| Smart Cache & Cross-Signal | 42 | 31 | **11** | ~74% |
| Tenant Stats & Explorer | 45 | 33 | **12** | ~73% |
| Tenant Name Mapping | 28 | 22 | **6** | ~79% |
| Partitioned Bloom Index | 30 | 20 | **10** | ~67% |
| S3 I/O Optimization | 28 | 12 | **16** | ~43% |
| K8s Scaling Safety | 40 | 14 | **26** | ~35% |
| **TOTAL** | **213** | **132** | **81** | **62%** |

## Logs-Traces Parity Status

```mermaid
graph LR
    subgraph "Shared (Both Modules)"
        SC2[SmartCache<br/>Controller]
        MAN2[Manifest<br/>Core]
        TR2[Tenant Resolver]
        AZ2[AZ Detection]
        PR2[Peer Ring]
        BB2[Buffer Bridge]
        PFE2[Prefetch Engine]
        CS2[Cross-Signal<br/>Client/Handler]
        ST2[Stats Registry]
        CMP2[Compaction]
    end

    subgraph "Logs Only (25 features)"
        COF[Cache-on-Flush]
        RGK[RG Skip by Stats]
        TBL[Token Bloom]
        BCC[BloomCache<br/>Partition-level]
        BOB[BloomObserver<br/>+ PartitionedIndex]
        MSC[Metadata Sidecar<br/>Writes]
        SEM[S3 Download<br/>Semaphore]
        WMD[WarmMetadata<br/>4-Phase Startup]
        PFF[prefetchFooters]
        FMP[File Metadata<br/>Disk Persist]
        MFP2[manifestFastPath]
        QSF[QuerySpecificFiles]
        SFL[Self-Filter]
        CAS[Cache Affinity Sort]
        NPG[Negated Predicate<br/>Guard]
        MRE[Max-Rows<br/>Enforcement]
        TSO[TimestampOnly<br/>Hint]
        OTL[OTel Spans]
        TRS[Tenant in<br/>SelectAPI]
        TSY[/tenant/sync<br/>Endpoint]
        SVF[Value Frequency<br/>Sampling]
        FCH[FooterCache.Has]
        FRA[footerReaderAt]
        PMG[Prometheus<br/>Gauge Init]
        SSF[SetSelfFilter<br/>Enabled]
    end

    subgraph "Traces Only (3 features)"
        BFI[BackfillBloomIndex]
        CTC[Context Cancel<br/>at RunQuery Top]
        S3P[s3Prefix Field]
    end

    style COF fill:#ff6b6b
    style RGK fill:#ff6b6b
    style TBL fill:#ff6b6b
    style BCC fill:#ff6b6b
    style BOB fill:#ff6b6b
    style MSC fill:#ff6b6b
    style SEM fill:#ff6b6b
    style WMD fill:#ff6b6b
    style PFF fill:#ff6b6b
    style FMP fill:#ff6b6b
    style MFP2 fill:#ff6b6b
    style QSF fill:#ff6b6b
    style SFL fill:#ff6b6b
    style CAS fill:#ff6b6b
    style NPG fill:#ff6b6b
    style MRE fill:#ff6b6b
    style TSO fill:#ff6b6b
    style OTL fill:#ff6b6b
    style TRS fill:#ff6b6b
    style TSY fill:#ff6b6b
    style SVF fill:#ff6b6b
    style FCH fill:#ff6b6b
    style FRA fill:#ff6b6b
    style PMG fill:#ff6b6b
    style SSF fill:#ff6b6b

    style BFI fill:#4ecdc4
    style CTC fill:#4ecdc4
    style S3P fill:#4ecdc4
```

## Test Coverage Matrix

| Package | Coverage | Status |
|---------|----------|--------|
| prefetch | 100.0% | Excellent |
| startup | 100.0% | Excellent |
| metrics | 100.0% | Excellent |
| schema | 100.0% | Excellent |
| retention | 99.0% | Excellent |
| manifest | 98.0% | Excellent |
| discovery | 97.8% | Excellent |
| s3reader | 97.4% | Excellent |
| bloomindex | 96.0% | Excellent |
| peercache | 96.1% | Excellent |
| smartcache | 95.9% | Excellent |
| tenant | 95.8% | Excellent |
| vlstorage | 95.0% | Excellent |
| wal | 94.7% | Good |
| config | 94.3% | Good |
| stats | 93.9% | Good |
| buffer | 93.9% | Good |
| ui | 93.9% | Good |
| cache | 95.3% | Excellent |
| selectapi | 93.7% | Good |
| delete | 93.7% | Good |
| crosssignal | 93.3% | Good |
| telemetry | 92.6% | Good |
| election | 91.1% | Good |
| compaction | 90.2% | Good |
| azdetect | 90.0% | Good |
| parquets3 | 82.9% | Needs S3 integration tests |

**26 of 29 packages at 90%+ coverage.** parquets3 remaining gap is in S3-dependent integration functions (RunQuery, queryFile, openParquetFile).

### Missing Benchmarks

- `BenchmarkRunQuery` - hottest query path
- `BenchmarkCompact` - compaction throughput
- `BenchmarkSmartCacheGet` - cache tier latency
