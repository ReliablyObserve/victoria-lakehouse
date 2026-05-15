import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function AnalyticsCompliance() {
  return (
    <Layout
      title="Analytics & Compliance — Query Cold Logs with DuckDB, ClickHouse, Spark, Trino"
      description="Victoria Lakehouse stores observability data in open Apache Parquet on S3. Query with DuckDB, ClickHouse, Spark, Trino, Databricks, Snowflake, StarRocks, Doris, or pandas. GDPR right-to-erasure via cost-aware deletion. SOC 2, HIPAA, PCI DSS compliant retention.">
      <header className="hero hero--primary" style={{padding: '3rem 0', textAlign: 'center'}}>
        <div className="container">
          <h1 className="hero__title">Analytics &amp; Compliance</h1>
          <p className="hero__subtitle" style={{maxWidth: 700, margin: '0 auto'}}>
            Standard Apache Parquet on S3. Query with 9+ analytics engines.
            GDPR, SOC 2, HIPAA, and PCI DSS ready. No vendor lock-in.
          </p>
        </div>
      </header>

      <main className="container margin-vert--xl">
        <div className="row margin-bottom--xl">
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '1.5rem', display: 'block', marginBottom: '0.5rem'}}>DuckDB</strong>
              Zero-infrastructure SQL on S3 Parquet. Single binary, sub-second queries.
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '1.5rem', display: 'block', marginBottom: '0.5rem'}}>ClickHouse</strong>
              OTEL-compatible views, Grafana plugin, sub-second OLAP at scale.
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '1.5rem', display: 'block', marginBottom: '0.5rem'}}>Spark / Trino</strong>
              Petabyte-scale distributed analytics and ETL pipelines.
            </div>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '1.5rem', display: 'block', marginBottom: '0.5rem'}}>Databricks</strong>
              Delta Lake integration, ML feature extraction, notebook analytics.
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '1.5rem', display: 'block', marginBottom: '0.5rem'}}>Snowflake</strong>
              External table queries for cross-domain business intelligence.
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '1.5rem', display: 'block', marginBottom: '0.5rem'}}>StarRocks / Doris</strong>
              Real-time OLAP, materialized views, MySQL protocol compatible.
            </div>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--6">
            <h2>Two Access Patterns, One Dataset</h2>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              <strong>Observability APIs</strong> for day-to-day operations:
              LogsQL full-text search, Jaeger and Tempo trace visualization, Loki API via proxy,
              Grafana dashboards. Every VL/VT query endpoint works unchanged.
            </p>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              <strong>SQL analytics</strong> for business intelligence:
              DuckDB for ad-hoc investigation, ClickHouse for OLAP dashboards,
              Spark for ML pipelines, Trino for federated queries across data sources.
              Standard Parquet &mdash; no export, no ETL, no lock-in.
            </p>
          </div>
          <div className="col col--6">
            <h2>Compliance &amp; Audit</h2>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              Direct S3 access means compliance teams can audit observability data
              without going through application APIs. Open Parquet format is readable
              by any tool &mdash; no vendor dependency for regulatory access.
            </p>
            <div className="highlight-box">
              <strong>GDPR Right-to-Erasure:</strong> Cost-aware deletion with three modes.
              <em>Hide</em> (tombstone, instant, $0) makes data immediately invisible.
              <em>Permanent</em> physically removes from S3 Standard files.
              <em>Auto</em> uses tombstones + background rewrite, never triggers Glacier retrieval fees.
            </div>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--6">
            <h2>Data Warehouse Integration</h2>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              Load observability data into Snowflake, BigQuery, Databricks, or your
              data warehouse for cross-domain analysis. Join logs with business metrics,
              traces with customer journeys. Parquet is natively understood by all
              modern warehouses &mdash; no transformation needed.
            </p>
          </div>
          <div className="col col--6">
            <h2>Machine Learning</h2>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              Train anomaly detection models on historical logs and traces using
              Spark, Databricks, or pandas. Read S3 Parquet directly from notebooks.
              OTEL semantic conventions (service.name, trace_id, span_id, duration)
              provide clean feature columns without preprocessing.
            </p>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>Cost Allocation &amp; Chargeback</h2>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              Victoria Lakehouse tracks per-tenant storage metrics: files, bytes,
              rows, ingestion volume, and query counts. Use the built-in Lakehouse
              Explorer UI or export Prometheus metrics to build chargeback dashboards.
              Per-tenant S3 prefix isolation enables accurate cost attribution.
            </p>
          </div>
        </div>

        <div className="text--center margin-vert--xl">
          <Link to="/docs/analytics-engines/" className="button button--primary button--lg">
            Analytics Engine Setup
          </Link>
          {' '}
          <Link to="/docs/open-parquet-format/" className="button button--secondary button--lg">
            Open Parquet Format
          </Link>
          {' '}
          <Link to="/docs/getting-started/" className="button button--secondary button--lg">
            Get Started
          </Link>
        </div>
      </main>
    </Layout>
  );
}
