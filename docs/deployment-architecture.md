---
title: Deployment Architecture
sidebar_position: 6
---

# Deployment Architecture

Victoria Lakehouse fits into observability infrastructure as a **cold storage tier** alongside hot VictoriaLogs/VictoriaTraces clusters. This document describes the production architecture including data collection, hot/cold tiering, disaster recovery, and analytics access.

## High-Level Architecture

```mermaid
graph TB
    subgraph "Data Collection"
        APP["Applications<br/>(OTEL SDK)"]
        K8S["Kubernetes<br/>Pods"]
        INFRA["Infrastructure<br/>Logs"]
    end

    subgraph "Log Collection — vlagent"
        VA["vlagent<br/>(log collector)"]
    end

    subgraph "Trace Collection — OTEL Collector"
        OC["OTEL Collector<br/>(traces)"]
    end

    subgraph "Hot Tier — VictoriaLogs (1 month, EBS)"
        VLI["vlinsert"]
        VLS["vlstorage<br/>(multi-AZ EBS)"]
        VLSEL["vlselect"]
    end

    subgraph "Hot Tier — VictoriaTraces (1 month, EBS)"
        VTI["vtinsert"]
        VTS["vtstorage<br/>(multi-AZ EBS)"]
        VTSEL["vtselect"]
    end

    subgraph "Cold Tier — Victoria Lakehouse (S3, all history)"
        LHI_L["lakehouse-insert<br/>mode=logs :9428"]
        LHI_T["lakehouse-insert<br/>mode=traces :10428"]
        LHS_L["lakehouse-select<br/>mode=logs :9428"]
        LHS_T["lakehouse-select<br/>mode=traces :10428"]
        S3[("S3 Bucket<br/>Parquet Files<br/>(11 nines durability)")]
    end

    subgraph "Consumers"
        GF["Grafana"]
        TRINO["Trino / Spark<br/>/ DuckDB"]
    end

    APP --> OC
    K8S --> VA
    INFRA --> VA

    VA -->|mirror| VLI
    VA -->|mirror| LHI_L

    OC -->|fanout| VTI
    OC -->|fanout| LHI_T

    VLI --> VLS
    VTI --> VTS

    LHI_L --> S3
    LHI_T --> S3

    GF --> VLSEL
    GF --> VTSEL
    VLSEL --> VLS
    VLSEL -->|cold fan-out| LHS_L
    VTSEL --> VTS
    VTSEL -->|cold fan-out| LHS_T

    LHS_L --> S3
    LHS_T --> S3
    TRINO --> S3

    style S3 fill:#e76f51,color:#fff
    style LHI_L fill:#5a189a,color:#fff
    style LHI_T fill:#5a189a,color:#fff
    style LHS_L fill:#2d6a4f,color:#fff
    style LHS_T fill:#2d6a4f,color:#fff
    style VA fill:#264653,color:#fff
    style OC fill:#264653,color:#fff
```

## Data Flow Summary

| Signal | Collector | Hot Tier | Cold Tier | Retention |
|---|---|---|---|---|
| Logs | vlagent | VictoriaLogs (multi-AZ EBS) | Victoria Lakehouse (S3 Parquet) | Hot: 1 month, Cold: unlimited |
| Traces | OTEL Collector | VictoriaTraces (multi-AZ EBS) | Victoria Lakehouse (S3 Parquet) | Hot: 1 month, Cold: unlimited |

Both collectors **mirror** (duplicate) traffic to hot and cold tiers simultaneously. No data passes through the hot tier to reach cold storage — they are independent write paths.

## Logs: vlagent Pipeline

[vlagent](https://docs.victoriametrics.com/victorialogs/vlagent/) is VictoriaMetrics' lightweight log collector. It collects logs from files, journald, syslog, and Kubernetes pods, then forwards to VictoriaLogs-compatible endpoints.

### Architecture

```mermaid
flowchart LR
    subgraph "Sources"
        FILES["Log files"]
        JOURNALD["journald"]
        K8S["Kubernetes<br/>pod logs"]
        SYSLOG["Syslog"]
    end

    subgraph "vlagent"
        VA["vlagent"]
    end

    subgraph "Hot (1 month)"
        VL["VictoriaLogs<br/>vlinsert:9428"]
    end

    subgraph "Cold (all history)"
        LH["Lakehouse Insert<br/>:9428"]
    end

    FILES --> VA
    JOURNALD --> VA
    K8S --> VA
    SYSLOG --> VA

    VA -->|"mirror 1"| VL
    VA -->|"mirror 2"| LH

    style VL fill:#264653,color:#fff
    style LH fill:#5a189a,color:#fff
```

### vlagent Configuration

vlagent uses `remoteWrite` with multiple destinations for mirroring:

```yaml
# vlagent.yaml — mirror logs to hot VictoriaLogs + cold Lakehouse
server:
  log_level: info

scrape_configs:
  # Kubernetes pod logs
  - job_name: kubernetes-pods
    kubernetes_sd_configs:
      - role: pod
    pipeline_stages:
      - docker: {}
      - match:
          selector: '{namespace=~".+"}'
          stages:
            - labels:
                namespace:
                pod:
                container:

  # Syslog input
  - job_name: syslog
    syslog:
      listen_address: 0.0.0.0:1514
      labels:
        job: syslog

  # File-based logs
  - job_name: application-logs
    static_configs:
      - targets: [localhost]
        labels:
          job: app
          __path__: /var/log/app/*.log

# Mirror to both hot and cold
remoteWrite:
  # Hot tier — VictoriaLogs (1 month retention)
  - url: http://vlinsert.monitoring.svc.cluster.local:9428/insert/jsonline
    name: hot-victorialogs
    queue_config:
      capacity: 10000
      max_shards: 10
      min_shards: 1
      max_samples_per_send: 5000
      batch_send_deadline: 5s

  # Cold tier — Victoria Lakehouse (unlimited retention)
  - url: http://lakehouse-insert.monitoring.svc.cluster.local:9428/insert/jsonline
    name: cold-lakehouse
    queue_config:
      capacity: 10000
      max_shards: 10
      min_shards: 1
      max_samples_per_send: 5000
      batch_send_deadline: 10s
```

### vlagent Helm Values (Kubernetes)

```yaml
# values-vlagent.yaml
vlagent:
  image:
    repository: victoriametrics/vlagent
    tag: latest

  config:
    remoteWrite:
      # Hot VictoriaLogs cluster
      - url: http://vlinsert.monitoring.svc.cluster.local:9428/insert/jsonline
        name: hot-victorialogs
      # Cold Victoria Lakehouse
      - url: http://lakehouse-insert.monitoring.svc.cluster.local:9428/insert/jsonline
        name: cold-lakehouse

    kubernetes:
      enabled: true
      namespaceSelector: {}

  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      memory: 512Mi

  tolerations:
    - operator: Exists
      effect: NoSchedule
```

### Multi-AZ Hot Cluster Configuration

For production, run VictoriaLogs in cluster mode across availability zones:

```yaml
# VictoriaLogs cluster (hot tier, 1 month retention)
vlinsert:
  replicaCount: 3
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        - topologyKey: topology.kubernetes.io/zone
          labelSelector:
            matchLabels:
              app: vlinsert
  extraArgs:
    storageNode: "vlstorage-0.vlstorage.monitoring.svc:9428,vlstorage-1.vlstorage.monitoring.svc:9428,vlstorage-2.vlstorage.monitoring.svc:9428"

vlstorage:
  replicaCount: 3
  persistence:
    enabled: true
    storageClass: gp3
    size: 500Gi
  extraArgs:
    retentionPeriod: 30d
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        - topologyKey: topology.kubernetes.io/zone

vlselect:
  replicaCount: 3
  extraArgs:
    # Fan out to both hot vlstorage and cold lakehouse
    storageNode: "vlstorage-0.vlstorage.monitoring.svc:9428,vlstorage-1.vlstorage.monitoring.svc:9428,vlstorage-2.vlstorage.monitoring.svc:9428,lakehouse-select.monitoring.svc:9428"
```

## Traces: OTEL Collector Pipeline

The [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) receives, processes, and exports telemetry data. For traces, it fans out to both VictoriaTraces (hot) and Victoria Lakehouse (cold) using the `fanout` connector or multiple exporters.

### Architecture

```mermaid
flowchart LR
    subgraph "Sources"
        OTLP["OTLP gRPC<br/>:4317"]
        JAEGER["Jaeger<br/>:14250"]
        ZIPKIN["Zipkin<br/>:9411"]
    end

    subgraph "OTEL Collector"
        REC["Receivers"]
        PROC["Processors<br/>(batch, memory_limiter)"]
        EXP["Exporters"]
    end

    subgraph "Hot (1 month)"
        VT["VictoriaTraces<br/>vtinsert:10428"]
    end

    subgraph "Cold (all history)"
        LH["Lakehouse Insert<br/>:10428"]
    end

    OTLP --> REC
    JAEGER --> REC
    ZIPKIN --> REC

    REC --> PROC
    PROC --> EXP

    EXP -->|"otlphttp/hot"| VT
    EXP -->|"otlphttp/cold"| LH

    style VT fill:#264653,color:#fff
    style LH fill:#5a189a,color:#fff
```

### OTEL Collector Configuration

```yaml
# otel-collector-config.yaml — fan out traces to hot VictoriaTraces + cold Lakehouse
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

  jaeger:
    protocols:
      grpc:
        endpoint: 0.0.0.0:14250
      thrift_http:
        endpoint: 0.0.0.0:14268

  zipkin:
    endpoint: 0.0.0.0:9411

processors:
  batch:
    send_batch_size: 10000
    timeout: 5s

  memory_limiter:
    check_interval: 1s
    limit_mib: 1024
    spike_limit_mib: 256

  resource:
    attributes:
      - key: environment
        value: production
        action: upsert

exporters:
  # Hot tier — VictoriaTraces (OTLP HTTP, 1 month retention)
  otlphttp/hot:
    endpoint: http://vtinsert.monitoring.svc.cluster.local:10428
    tls:
      insecure: true
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 300s

  # Cold tier — Victoria Lakehouse (OTLP HTTP, unlimited retention)
  otlphttp/cold:
    endpoint: http://lakehouse-insert.monitoring.svc.cluster.local:10428
    tls:
      insecure: true
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 300s
    sending_queue:
      enabled: true
      num_consumers: 10
      queue_size: 5000

service:
  pipelines:
    traces:
      receivers: [otlp, jaeger, zipkin]
      processors: [memory_limiter, batch, resource]
      exporters: [otlphttp/hot, otlphttp/cold]

  telemetry:
    logs:
      level: info
    metrics:
      address: 0.0.0.0:8888
```

### OTEL Collector Helm Values (Kubernetes)

```yaml
# values-otel-collector.yaml
mode: deployment

config:
  receivers:
    otlp:
      protocols:
        grpc:
          endpoint: 0.0.0.0:4317
        http:
          endpoint: 0.0.0.0:4318

  processors:
    batch:
      send_batch_size: 10000
      timeout: 5s
    memory_limiter:
      check_interval: 1s
      limit_mib: 1024

  exporters:
    otlphttp/hot:
      endpoint: http://vtinsert.monitoring.svc.cluster.local:10428
      tls:
        insecure: true
    otlphttp/cold:
      endpoint: http://lakehouse-insert.monitoring.svc.cluster.local:10428
      tls:
        insecure: true
      sending_queue:
        enabled: true
        queue_size: 5000

  service:
    pipelines:
      traces:
        receivers: [otlp]
        processors: [memory_limiter, batch]
        exporters: [otlphttp/hot, otlphttp/cold]

replicaCount: 3

resources:
  requests:
    cpu: 200m
    memory: 256Mi
  limits:
    memory: 1Gi

autoscaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70
```

### Multi-AZ Hot Traces Cluster

```yaml
# VictoriaTraces cluster (hot tier, 1 month retention)
vtinsert:
  replicaCount: 3
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        - topologyKey: topology.kubernetes.io/zone

vtstorage:
  replicaCount: 3
  persistence:
    enabled: true
    storageClass: gp3
    size: 500Gi
  extraArgs:
    retentionPeriod: 30d

vtselect:
  replicaCount: 3
  extraArgs:
    # Fan out to both hot vtstorage and cold lakehouse
    storageNode: "vtstorage-0.vtstorage.monitoring.svc:10428,vtstorage-1.vtstorage.monitoring.svc:10428,vtstorage-2.vtstorage.monitoring.svc:10428,lakehouse-select.monitoring.svc:10428"
```

## Disaster Recovery

Victoria Lakehouse serves as a **disaster recovery** (DR) backend for the hot cluster. If the hot VictoriaLogs/VictoriaTraces cluster is unavailable (outage, upgrade, migration), Lakehouse continues serving all historical data from S3.

### DR Architecture

```mermaid
flowchart TB
    subgraph "Normal Operation"
        GF1["Grafana"] --> VLSEL1["vlselect"]
        VLSEL1 --> VLS1["vlstorage<br/>(hot, EBS)"]
        VLSEL1 -->|cold queries| LHS1["lakehouse-select<br/>(S3)"]
    end

    subgraph "Hot Cluster Down (DR Mode)"
        GF2["Grafana"] -->|failover| LHS2["lakehouse-select<br/>(S3)"]
        LHS2 --> S3_2[("S3<br/>all data")]
    end

    subgraph "During Upgrade/Migration"
        GF3["Grafana"] -->|route all| LHS3["lakehouse-select<br/>(S3)"]
        LHS3 --> S3_3[("S3<br/>all data")]
        NOTE3["Slower queries (S3 vs EBS)<br/>but ALL data available"]
    end

    style VLS1 fill:#264653,color:#fff
    style LHS1 fill:#2d6a4f,color:#fff
    style LHS2 fill:#e76f51,color:#fff
    style LHS3 fill:#e76f51,color:#fff
    style S3_2 fill:#e76f51,color:#fff
    style S3_3 fill:#e76f51,color:#fff
```

### DR Scenarios

| Scenario | Behavior | Query Latency |
|---|---|---|
| **Normal operation** | vlselect fans out to vlstorage (hot) + lakehouse (cold). Hot queries <10ms, cold queries <500ms | Hot: <10ms, Cold: <500ms |
| **Hot cluster down** | Grafana/vmauth routes all queries to lakehouse-select. All data available from S3 | 50-500ms (S3-backed) |
| **Hot cluster upgrade** | Drain vlselect, route queries to lakehouse during maintenance window | 50-500ms during window |
| **AZ failure** | Multi-AZ hot cluster continues on remaining AZs. Lakehouse unaffected (S3 is multi-AZ by default) | Unchanged |
| **Region failure** | S3 cross-region replication enables lakehouse in DR region | Based on DR region S3 latency |

### Grafana DR Routing with vmauth

Use vmauth to automatically fail over to lakehouse when the hot cluster is unavailable:

```yaml
# vmauth-dr-config.yaml
unauthorized_user:
  url_map:
    # Try hot cluster first, fall back to lakehouse
    - src_paths:
        - "/select/.*"
      url_prefix:
        - "http://vlselect.monitoring.svc:9428/"
        - "http://lakehouse-select.monitoring.svc:9428/"
      load_balancing_policy: first_available
      retry_status_codes: [502, 503]

    - src_paths:
        - "/insert/.*"
      url_prefix:
        - "http://lakehouse-insert.monitoring.svc:9428/"
```

### DR Playbook

**Failover to Lakehouse:**

1. Update vmauth/Grafana datasource URL to point directly at lakehouse-select
2. Verify data availability: `curl http://lakehouse-select:9428/manifest/range`
3. Monitor query latency: expect 50-500ms vs normal <10ms for recent data
4. Data remains available — slower but complete

**Failback to Hot Cluster:**

1. Restore hot VictoriaLogs/VictoriaTraces cluster
2. Verify hot tier data: `curl http://vlselect:9428/health`
3. Re-register lakehouse as `-storageNode` on vlselect
4. Update vmauth/Grafana back to normal routing
5. Hot tier handles recent queries, lakehouse handles cold as usual

## Write Path Architecture

```mermaid
flowchart TB
    subgraph "Log Write Path"
        VA_W["vlagent"] -->|mirror 1| VLI_W["vlinsert → vlstorage<br/>(hot EBS, 1 month)"]
        VA_W -->|mirror 2| LHI_W["lakehouse-insert<br/>mode=logs"]
        LHI_W --> WAL_L["WAL"]
        WAL_L --> BUF_L["Partition Buffers"]
        BUF_L -->|flush| S3_L[("S3<br/>logs/dt=YYYY-MM-DD/hour=HH/*.parquet")]
    end

    subgraph "Trace Write Path"
        OC_W["OTEL Collector"] -->|export 1| VTI_W["vtinsert → vtstorage<br/>(hot EBS, 1 month)"]
        OC_W -->|export 2| LHI_T2["lakehouse-insert<br/>mode=traces"]
        LHI_T2 --> WAL_T["WAL"]
        WAL_T --> BUF_T["Partition Buffers"]
        BUF_T -->|flush| S3_T[("S3<br/>traces/dt=YYYY-MM-DD/hour=HH/*.parquet")]
    end

    style S3_L fill:#e76f51,color:#fff
    style S3_T fill:#e76f51,color:#fff
    style LHI_W fill:#5a189a,color:#fff
    style LHI_T2 fill:#5a189a,color:#fff
```

## Read Path Architecture

```mermaid
flowchart TB
    subgraph "Grafana Queries"
        GF_Q["Grafana"]
    end

    subgraph "Hot Tier (recent 1 month)"
        VLSEL_Q["vlselect / vtselect"]
        VLSTO_Q["vlstorage / vtstorage<br/>(EBS, <10ms)"]
    end

    subgraph "Cold Tier (all history)"
        LHSEL_Q["lakehouse-select"]
        MAN_Q["Manifest"]
        CACHE_Q["Cache L1→L2→L3"]
        S3_Q[("S3 Parquet<br/>(50-150ms)")]
    end

    subgraph "Buffer Bridge (unflushed data)"
        LHINS_Q["lakehouse-insert<br/>/internal/buffer/query"]
    end

    GF_Q --> VLSEL_Q
    VLSEL_Q -->|hot| VLSTO_Q
    VLSEL_Q -->|cold fan-out| LHSEL_Q
    LHSEL_Q --> MAN_Q
    MAN_Q --> CACHE_Q
    CACHE_Q --> S3_Q
    LHSEL_Q -.->|buffer query| LHINS_Q

    style S3_Q fill:#e76f51,color:#fff
    style LHSEL_Q fill:#2d6a4f,color:#fff
    style VLSTO_Q fill:#264653,color:#fff
```

## Complete Kubernetes Deployment

A complete deployment includes these components:

```
monitoring/
├── vlagent/                    # Log collection (DaemonSet)
│   └── values.yaml
├── otel-collector/             # Trace collection (Deployment)
│   └── values.yaml
├── victorialogs-cluster/       # Hot logs (1 month, multi-AZ)
│   └── values.yaml
├── victoriatraces-cluster/     # Hot traces (1 month, multi-AZ)
│   └── values.yaml
├── victoria-lakehouse-logs/    # Cold logs (S3, unlimited)
│   └── values.yaml
├── victoria-lakehouse-traces/  # Cold traces (S3, unlimited)
│   └── values.yaml
└── grafana/
    └── values.yaml             # Datasources for hot + cold
```

### Lakehouse Helm Values (Logs)

```yaml
# victoria-lakehouse-logs/values.yaml
mode: logs

s3:
  bucket: obs-archive
  region: us-east-1

insertComponent:
  enabled: true
  replicaCount: 2
  persistence:
    enabled: true
    size: 50Gi

select:
  enabled: true
  replicaCount: 3
  persistence:
    enabled: true
    size: 100Gi

vmauth:
  enabled: true

insert:
  flushInterval: 10s
  walEnabled: true
```

### Lakehouse Helm Values (Traces)

```yaml
# victoria-lakehouse-traces/values.yaml
mode: traces

s3:
  bucket: obs-archive
  region: us-east-1

insertComponent:
  enabled: true
  replicaCount: 2
  persistence:
    enabled: true
    size: 50Gi

select:
  enabled: true
  replicaCount: 3
  persistence:
    enabled: true
    size: 100Gi

vmauth:
  enabled: true

insert:
  flushInterval: 10s
  walEnabled: true
```

### Grafana Datasources

```yaml
# grafana/provisioning/datasources.yaml
apiVersion: 1
datasources:
  # Hot logs (recent 1 month, fast)
  - name: VictoriaLogs
    type: victorialogs-datasource
    access: proxy
    url: http://vlselect.monitoring.svc:9428
    isDefault: true

  # Cold logs (all history, S3-backed)
  - name: Cold Logs (Lakehouse)
    type: victorialogs-datasource
    access: proxy
    url: http://lakehouse-logs-select.monitoring.svc:9428

  # Unified logs (vlselect fans out to hot + cold automatically)
  # This is the recommended setup — vlselect handles routing
  - name: All Logs (Unified)
    type: victorialogs-datasource
    access: proxy
    url: http://vlselect.monitoring.svc:9428
    jsonData:
      note: "vlselect registered with lakehouse as -storageNode"

  # Hot traces (recent 1 month)
  - name: VictoriaTraces
    type: jaeger
    access: proxy
    url: http://vtselect.monitoring.svc:10428

  # Cold traces (all history)
  - name: Cold Traces (Lakehouse)
    type: jaeger
    access: proxy
    url: http://lakehouse-traces-select.monitoring.svc:10428

  # Unified traces
  - name: All Traces (Unified)
    type: jaeger
    access: proxy
    url: http://vtselect.monitoring.svc:10428
    jsonData:
      note: "vtselect registered with lakehouse as -storageNode"
```
