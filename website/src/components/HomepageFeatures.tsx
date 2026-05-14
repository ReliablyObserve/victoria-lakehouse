import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';

type FeatureItem = {
  title: string;
  description: JSX.Element;
};

const FeatureList: FeatureItem[] = [
  {
    title: '60-96% Cost Reduction',
    description: (
      <>
        S3 Standard at $0.023/GB vs EBS at $0.08-0.15/GB. Glacier tiering at $0.004/GB for compliance retention. At 1 PB/month, save $614K/year vs Loki/Tempo.
      </>
    ),
  },
  {
    title: '11+ Ingestion Formats',
    description: (
      <>
        Loki push, Elasticsearch bulk, OTLP, Syslog, Fluentd, Logstash, Datadog, Journald, NDJSON, Zipkin, Jaeger. Every VL/VT-supported format works unchanged.
      </>
    ),
  },
  {
    title: 'Full Query Compatibility',
    description: (
      <>
        LogsQL full-text search, Jaeger trace UI, Loki API via proxy, Grafana dashboards. Every VL/VT query endpoint implemented. Sub-100ms bloom filter lookups.
      </>
    ),
  },
  {
    title: '9+ Analytics Engines',
    description: (
      <>
        DuckDB, ClickHouse, Spark, Trino, Databricks, Snowflake, StarRocks, Doris, pandas. Standard Apache Parquet — query cold data with any tool, zero lock-in.
      </>
    ),
  },
  {
    title: 'Disaster Recovery',
    description: (
      <>
        When the hot cluster is down, Lakehouse serves all data from S3. 11 nines durability, always available. 10-20x cheaper than maintaining a hot standby.
      </>
    ),
  },
  {
    title: 'Drop-In Storage Node',
    description: (
      <>
        Register as a -storageNode on vlselect/vtselect. Hot cluster handles recent data on EBS, Lakehouse handles cold on S3. Transparent to queries and Grafana.
      </>
    ),
  },
  {
    title: 'Multi-Tenant',
    description: (
      <>
        Per-tenant S3 prefix or bucket isolation. 8 per-tenant Prometheus metrics for cost allocation. Built-in Lakehouse Explorer UI for tenant management.
      </>
    ),
  },
  {
    title: 'Apache 2.0 Licensed',
    description: (
      <>
        No AGPL restrictions. Fork it, embed it, build on it. Full source on GitHub. No vendor lock-in on code or data format.
      </>
    ),
  },
];

function Feature({title, description}: FeatureItem) {
  return (
    <div className={clsx('col col--3')}>
      <div className="text--center padding-horiz--md padding-vert--md">
        <div className={styles.featureBox}>
          <h3>{title}</h3>
          <p>{description}</p>
        </div>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): JSX.Element {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
