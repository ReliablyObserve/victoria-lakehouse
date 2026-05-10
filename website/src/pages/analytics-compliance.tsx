import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function AnalyticsCompliance() {
  return (
    <Layout
      title="Analytics & Compliance"
      description="Open-format Parquet data for analytics, ML, and compliance">
      <header className="hero hero--primary" style={{padding: '3rem 0', textAlign: 'center'}}>
        <div className="container">
          <h1 className="hero__title">Analytics & Compliance</h1>
          <p className="hero__subtitle" style={{maxWidth: 600, margin: '0 auto'}}>
            Standard Apache Parquet on S3. Query with DuckDB, Spark, Trino,
            or ClickHouse. No vendor lock-in.
          </p>
        </div>
      </header>

      <main className="container margin-vert--xl">
        <div className="row margin-bottom--xl">
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '1.5rem', display: 'block', marginBottom: '0.5rem'}}>DuckDB</strong>
              Local SQL queries on S3 Parquet
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '1.5rem', display: 'block', marginBottom: '0.5rem'}}>Spark / Trino</strong>
              Distributed analytics at scale
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '1.5rem', display: 'block', marginBottom: '0.5rem'}}>ClickHouse</strong>
              High-performance OLAP queries
            </div>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--6">
            <h2>Compliance & Audit</h2>
            <p style={{fontSize: '1.1rem'}}>
              Direct S3 access means compliance teams can audit observability data
              without going through APIs. Export to data warehouses for compliance
              testing, retention verification, and e-discovery.
            </p>
            <div className="highlight-box">
              <strong>GDPR Compliant:</strong> Immediate inaccessibility via tombstones satisfies
              right-to-erasure. Optional physical deletion for strict requirements.
            </div>
          </div>
          <div className="col col--6">
            <h2>Machine Learning</h2>
            <p style={{fontSize: '1.1rem'}}>
              Train anomaly detection models on historical logs and traces using
              Spark or pandas. No need to export data — query S3 Parquet directly.
            </p>
            <div className="highlight-box">
              <strong>Schema:</strong> OTEL semantic conventions (service.name, trace_id,
              span_id, etc.) with Hive partitioning by date/hour.
            </div>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--6">
            <h2>Data Warehouse Integration</h2>
            <p style={{fontSize: '1.1rem'}}>
              Load observability data into Snowflake, BigQuery, or your data warehouse
              for cross-domain analysis. Parquet format is natively understood by all
              modern warehouses.
            </p>
          </div>
          <div className="col col--6">
            <h2>Cost Allocation & Chargeback</h2>
            <p style={{fontSize: '1.1rem'}}>
              Aggregate logs/traces by service, namespace, or team. Create chargeback
              models based on actual data patterns. Query directly from S3 without
              loading into a separate system.
            </p>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>Data Retention & Deletion</h2>
            <p style={{fontSize: '1.1rem'}}>Victoria Lakehouse supports cost-aware deletion with three modes:</p>
            <ul style={{fontSize: '1.1rem', lineHeight: '2'}}>
              <li><strong>Hide (Tombstone)</strong> — instant, $0, data invisible to queries</li>
              <li><strong>Permanent (Rewrite)</strong> — physical removal from S3 Standard files</li>
              <li><strong>Auto (Smart)</strong> — tombstone immediately, background rewrite for Standard files</li>
            </ul>
          </div>
        </div>

        <div className="text--center margin-vert--xl">
          <Link to="/docs/open-parquet-format/" className="button button--primary button--lg">
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
