import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function AnalyticsCompliance() {
  return (
    <Layout
      title="Analytics & Compliance"
      description="Open-format Parquet data for analytics, ML, and compliance">
      <main className="container margin-vert--lg">
        <div className="row">
          <div className="col col--8 col--offset-2">
            <h1>Analytics & Compliance with Victoria Lakehouse</h1>

            <section className="margin-vert--lg">
              <h2>Open Data Format</h2>
              <p>
                Victoria Lakehouse stores data as standard Apache Parquet files on S3. No proprietary formats,
                no vendor lock-in. Your data is accessible to any tool in the data ecosystem.
              </p>

              <div className="cost-metric">
                <strong>Supported Tools:</strong> DuckDB, Apache Spark, Trino, ClickHouse, Pandas, and more<br/>
                <strong>Schema:</strong> OTEL semantic conventions (service.name, trace_id, span_id, etc.)<br/>
                <strong>Storage:</strong> Hive partitioned by date/hour on S3
              </div>
            </section>

            <section className="margin-vert--lg">
              <h2>Use Cases</h2>

              <h3>Compliance & Audit</h3>
              <p>
                Direct S3 access means compliance teams can audit observability data without going through APIs.
                Export to data warehouses for compliance testing, retention verification, and e-discovery.
              </p>

              <h3>Machine Learning & Anomaly Detection</h3>
              <p>
                Train models on historical logs and traces using Spark or pandas. No need to export data;
                query S3 directly via DuckDB or ClickHouse.
              </p>

              <h3>Data Warehouse Integration</h3>
              <p>
                Load observability data into Snowflake, BigQuery, or your data warehouse for cross-domain analysis.
                Parquet format is natively understood by all modern warehouses.
              </p>

              <h3>Cost Allocation & Chargeback</h3>
              <p>
                Aggregate logs/traces by service, namespace, or team. Create chargeback models based on actual data patterns.
              </p>
            </section>

            <section className="margin-vert--lg">
              <h2>Examples</h2>

              <h3>Query with DuckDB</h3>
              <pre><code>{`
SELECT service_name, COUNT(*) as log_count
FROM read_parquet('s3://bucket/dt=2025-01-01/**/*.parquet')
WHERE severity_text = 'ERROR'
GROUP BY service_name
ORDER BY log_count DESC;
`}</code></pre>

              <h3>Analyze with Spark</h3>
              <pre><code>{`
df = spark.read.parquet("s3://bucket/dt=2025-01-*/hour=*/")
df.filter(col("service.name") == "api-gateway").show()
`}</code></pre>

              <h3>Trino SQL</h3>
              <pre><code>{`
SELECT
  date_format(from_unixtime(timestamp_unix_nano / 1e9), '%Y-%m-%d') as date,
  service_name,
  COUNT(*) as events
FROM s3.default.lakehouse_logs
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
`}</code></pre>
            </section>

            <section className="margin-vert--lg">
              <h2>Data Retention & Deletion</h2>
              <p>
                Victoria Lakehouse supports cost-aware deletion with three modes:
              </p>
              <ul>
                <li><strong>Hide (Tombstone):</strong> Instant, $0, data invisible to queries</li>
                <li><strong>Permanent (Rewrite):</strong> Physical removal from S3 (takes minutes, costs $)</li>
                <li><strong>Auto (Smart):</strong> Tombstone by default, periodic background rewrites</li>
              </ul>

              <div className="highlight-box">
                <strong>GDPR Compliant:</strong> Immediate inaccessibility satisfies right-to-erasure.
                Optional physical deletion for strict compliance requirements.
              </div>
            </section>

            <section className="margin-vert--lg">
              <h2>Next Steps</h2>
              <p>
                Learn how to integrate Victoria Lakehouse with your analytics stack:
              </p>
              <Link to="/docs/open-parquet-format/" className="button button--primary">
                Open Parquet Format
              </Link>
              {' '}
              <Link to="/docs/analytics/" className="button button--secondary">
                Analytics Guide
              </Link>
            </section>
          </div>
        </div>
      </main>
    </Layout>
  );
}
