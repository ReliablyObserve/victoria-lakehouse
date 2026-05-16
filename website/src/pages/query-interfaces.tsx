import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

const interfaces = [
  {
    name: 'LogsQL',
    category: 'Observability',
    description: 'VictoriaLogs native query language. Full-text search, regex matching, field operations, pipe processors (stats, sort, uniq, top, limit), time range filtering, stream selectors. Every VL query endpoint works: /select/logsql/query, /hits, /stats_query, /field_names, /field_values, /streams, /tail.',
    endpoints: ['/select/logsql/query', '/select/logsql/hits', '/select/logsql/stats_query', '/select/logsql/field_names', '/select/logsql/field_values', '/select/logsql/streams', '/select/logsql/tail'],
    grafana: 'VictoriaLogs datasource',
  },
  {
    name: 'Jaeger UI & API',
    category: 'Observability',
    description: 'Full Jaeger trace search and visualization. Search traces by service, operation, tags, duration, and time range. Trace detail view with span tree, process info, and logs. Service dependency graph. Trace comparison. All Jaeger HTTP API endpoints implemented.',
    endpoints: ['/select/jaeger/api/traces', '/select/jaeger/api/traces/{id}', '/select/jaeger/api/services', '/select/jaeger/api/services/{service}/operations'],
    grafana: 'Jaeger datasource',
  },
  {
    name: 'Tempo API',
    category: 'Observability',
    description: 'Grafana Tempo-compatible trace query API. Search traces, browse tags and tag values, retrieve traces by ID. TraceQL query support. Provided by VictoriaTraces upstream — Lakehouse inherits the Tempo API via the VT storage dispatch layer.',
    endpoints: ['/tempo/api/search', '/tempo/api/v2/search/tags', '/tempo/api/v2/search/tag/{name}/values', '/tempo/api/v2/traces/{id}', '/tempo/api/echo'],
    grafana: 'Tempo datasource',
  },
  {
    name: 'Loki API (via loki-vl-proxy)',
    category: 'Observability',
    description: 'Grafana Loki-compatible query API through loki-vl-proxy. LogQL queries are translated to LogsQL. Use with Grafana Loki datasource, Grafana Explore, and Loki Drilldown. Supports label discovery, log volume, and structured metadata.',
    endpoints: ['/loki/api/v1/query_range', '/loki/api/v1/labels', '/loki/api/v1/label/{name}/values', '/loki/api/v1/index/volume_range'],
    grafana: 'Loki datasource (via proxy)',
  },
  {
    name: 'VL/VT Binary Protocol',
    category: 'Internal',
    description: 'Native VictoriaLogs/VictoriaTraces internal select protocol. ZSTD-compressed DataBlock streaming over HTTP. Register as a -storageNode on vlselect/vtselect for transparent hot+cold fan-out. Same wire format as upstream VL/VT.',
    endpoints: ['/internal/select/query', '/internal/select/field_names', '/internal/select/field_values', '/internal/select/stream_field_names', '/internal/select/stream_field_values', '/internal/select/streams', '/internal/select/stream_ids', '/internal/select/tenants'],
    grafana: 'Transparent via vlselect/vtselect',
  },
  {
    name: 'DuckDB',
    category: 'Analytics',
    description: 'Query Parquet files directly with SQL. Local in-process engine or remote via httpfs extension pointing at S3. Ideal for ad-hoc investigation, compliance audits, and data export. Zero infrastructure — single binary.',
    endpoints: ['SELECT * FROM read_parquet(\'s3://bucket/logs/dt=2024-01-15/*.parquet\')'],
    grafana: 'DuckDB Grafana datasource (community)',
  },
  {
    name: 'ClickHouse',
    category: 'Analytics',
    description: 'High-performance OLAP queries via s3() table function. Pre-configured OTEL-compatible views (otel_logs, otel_traces) for Grafana ClickHouse plugin with native log and trace panel visualization. Tenant-scoped views for multi-tenant analytics.',
    endpoints: ['SELECT * FROM s3(\'s3://bucket/logs/**/*.parquet\', \'Parquet\')', 'SELECT * FROM lakehouse.otel_logs', 'SELECT * FROM lakehouse.otel_traces'],
    grafana: 'ClickHouse Grafana datasource (OTEL mode)',
  },
  {
    name: 'Apache Spark',
    category: 'Analytics',
    description: 'Distributed analytics at petabyte scale. Read Parquet from S3 with full predicate pushdown. Use for batch processing, ETL pipelines, ML feature extraction, and cross-dataset joins.',
    endpoints: ['spark.read.parquet("s3a://bucket/logs/")'],
    grafana: 'Via JDBC/ODBC bridge',
  },
  {
    name: 'Trino (PrestoSQL)',
    category: 'Analytics',
    description: 'Federated SQL queries across multiple data sources. Join observability data with business databases. Hive metastore integration for schema management. Interactive queries at scale.',
    endpoints: ['SELECT * FROM hive.lakehouse.logs WHERE dt = \'2024-01-15\''],
    grafana: 'Via JDBC/ODBC bridge',
  },
  {
    name: 'Databricks / Snowflake',
    category: 'Analytics',
    description: 'Enterprise data platform integration. Register S3 Parquet as external tables for cross-domain analytics, compliance reporting, and business intelligence. Schema auto-detection from Parquet metadata.',
    endpoints: ['External table on S3 Parquet'],
    grafana: 'Native platform connectors',
  },
  {
    name: 'StarRocks / Apache Doris',
    category: 'Analytics',
    description: 'Real-time OLAP engines with native S3 Parquet support. Sub-second queries on cold observability data. Materialized views for pre-aggregated dashboards.',
    endpoints: ['SELECT * FROM FILES("path" = "s3://bucket/logs/**", "format" = "parquet")'],
    grafana: 'MySQL protocol compatible',
  },
  {
    name: 'pandas / Python',
    category: 'Analytics',
    description: 'Direct Parquet reading for data science workflows. Use pyarrow or fastparquet backends. Ideal for anomaly detection, pattern analysis, and notebook-based investigation.',
    endpoints: ['pd.read_parquet("s3://bucket/logs/dt=2024-01-15/")'],
    grafana: 'Via custom datasource or API',
  },
];

export default function QueryInterfaces() {
  return (
    <Layout
      title="Query Interfaces — LogsQL, Jaeger, Tempo, Loki, DuckDB, ClickHouse, Spark, and More"
      description="Victoria Lakehouse supports 12+ query interfaces: LogsQL full-text search, Jaeger trace UI, Tempo API, Loki API, VL/VT binary protocol, DuckDB, ClickHouse, Spark, Trino, Databricks, Snowflake, StarRocks, Doris, and pandas. Observability APIs for operations, SQL analytics for business intelligence.">
      <header className="hero hero--primary" style={{padding: '3rem 0', textAlign: 'center'}}>
        <div className="container">
          <h1 className="hero__title">12+ Query Interfaces</h1>
          <p className="hero__subtitle" style={{maxWidth: 700, margin: '0 auto'}}>
            Observability APIs for operations. SQL analytics for business intelligence.
            Same data, every access pattern.
          </p>
        </div>
      </header>

      <main className="container margin-vert--xl">
        <div className="row margin-bottom--xl">
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>5</strong>
              Observability query APIs (LogsQL, Jaeger, Tempo, Loki, VL/VT binary)
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>9+</strong>
              Analytics engines (DuckDB, ClickHouse, Spark, Trino, Databricks, Snowflake...)
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>1</strong>
              Data format &mdash; open Apache Parquet, no lock-in
            </div>
          </div>
        </div>

        {['Observability', 'Internal', 'Analytics'].map(cat => (
          <div key={cat}>
            <h2 style={{borderBottom: '2px solid var(--ifm-color-primary)', paddingBottom: '0.5rem', marginTop: '2rem'}}>
              {cat === 'Observability' ? 'Observability Query APIs' : cat === 'Internal' ? 'Internal Protocol' : 'SQL Analytics Engines'}
            </h2>
            {interfaces.filter(i => i.category === cat).map((iface, idx) => (
              <div key={idx} className="row margin-bottom--lg">
                <div className="col col--12">
                  <div style={{
                    padding: '1.5rem',
                    background: 'var(--ifm-color-emphasis-100)',
                    borderRadius: '8px',
                    borderLeft: `4px solid ${cat === 'Analytics' ? '#2563eb' : cat === 'Internal' ? '#6b7280' : 'var(--ifm-color-primary)'}`,
                  }}>
                    <h3 style={{margin: '0 0 0.5rem'}}>{iface.name}</h3>
                    <p style={{margin: '0 0 0.75rem', lineHeight: 1.6}}>{iface.description}</p>
                    <div style={{display: 'flex', flexWrap: 'wrap', gap: '0.5rem', marginBottom: '0.5rem'}}>
                      {iface.endpoints.map((ep, j) => (
                        <code key={j} style={{fontSize: '0.8rem', padding: '0.2rem 0.5rem', borderRadius: '4px'}}>{ep}</code>
                      ))}
                    </div>
                    <p style={{margin: 0, fontSize: '0.85rem', color: 'var(--ifm-color-emphasis-600)'}}>
                      <strong>Grafana:</strong> {iface.grafana}
                    </p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        ))}

        <div className="text--center margin-vert--xl">
          <Link to="/docs/getting-started/" className="button button--primary button--lg">
            Quick Start Guide
          </Link>
          {' '}
          <Link to="/docs/analytics-engines/" className="button button--secondary button--lg">
            Analytics Engine Setup
          </Link>
        </div>
      </main>
    </Layout>
  );
}
