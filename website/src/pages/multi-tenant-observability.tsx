import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function MultiTenantObservability() {
  return (
    <Layout
      title="Multi-Tenant Observability — Per-Tenant S3 Isolation, Cost Allocation, and Storage Metrics"
      description="Victoria Lakehouse supports multi-tenant observability with per-tenant S3 prefix or bucket isolation, cost allocation metrics, storage class tracking, and Lakehouse Explorer UI for tenant management.">
      <header className="hero hero--primary" style={{padding: '3rem 0', textAlign: 'center'}}>
        <div className="container">
          <h1 className="hero__title">Multi-Tenant Observability</h1>
          <p className="hero__subtitle" style={{maxWidth: 700, margin: '0 auto'}}>
            Per-tenant S3 isolation with cost allocation, storage metrics,
            and a management UI. One binary serves all tenants.
          </p>
        </div>
      </header>

      <main className="container margin-vert--xl">
        <div className="row margin-bottom--xl">
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>Prefix or Bucket</strong>
              Two isolation modes for different security requirements
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>8 Metrics</strong>
              Per-tenant files, bytes, rows, ingestion, queries, timestamps
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>Explorer UI</strong>
              3-tab dashboard: Storage, Tenants, Cardinality
            </div>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--6">
            <h2>Prefix Isolation (Default)</h2>
            <p style={{fontSize: '1.05rem', lineHeight: 1.7}}>
              Tenants share a single S3 bucket with prefix-based isolation.
              Data is stored under <code>{'{AccountID}/{ProjectID}/logs/...'}</code>.
              Header-based routing matches the Grafana Loki/Tempo multi-tenancy pattern.
              Zero configuration for single-tenant deployments (defaults to <code>0/0/</code>).
            </p>
            <pre style={{fontSize: '0.85rem'}}>
{`# Tenant routing via headers
X-Scope-OrgID: acme/production
# → S3 path: s3://bucket/acme/production/logs/dt=.../`}
            </pre>
          </div>
          <div className="col col--6">
            <h2>Bucket Isolation (Enterprise)</h2>
            <p style={{fontSize: '1.05rem', lineHeight: 1.7}}>
              Each tenant gets a dedicated S3 bucket with separate IAM policies.
              Maximum security isolation for compliance-sensitive workloads.
              Bucket names are derived from a template: <code>{'{tenant}'}-observability</code>.
              Per-tenant lifecycle rules and storage class pricing overrides.
            </p>
            <pre style={{fontSize: '0.85rem'}}>
{`# Bucket-per-tenant
lakehouseConfig:
  tenant:
    isolation: bucket
    bucket_template: "{tenant}-observability"`}
            </pre>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>Per-Tenant Metrics &amp; Cost Allocation</h2>
            <p style={{fontSize: '1.05rem', lineHeight: 1.7}}>
              Victoria Lakehouse exposes per-tenant Prometheus metrics for cost allocation,
              capacity planning, and chargeback reporting:
            </p>
            <table style={{width: '100%'}}>
              <thead>
                <tr>
                  <th>Metric</th>
                  <th>Type</th>
                  <th>Use Case</th>
                </tr>
              </thead>
              <tbody>
                <tr><td><code>lakehouse_tenant_files</code></td><td>Gauge</td><td>File count per tenant</td></tr>
                <tr><td><code>lakehouse_tenant_bytes</code></td><td>Gauge</td><td>Storage bytes (compressed)</td></tr>
                <tr><td><code>lakehouse_tenant_raw_bytes</code></td><td>Gauge</td><td>Raw bytes (pre-compression)</td></tr>
                <tr><td><code>lakehouse_tenant_rows_total</code></td><td>Gauge</td><td>Total row count</td></tr>
                <tr><td><code>lakehouse_tenant_ingestion_bytes_total</code></td><td>Counter</td><td>Ingestion volume tracking</td></tr>
                <tr><td><code>lakehouse_tenant_queries_total</code></td><td>Counter</td><td>Query count per tenant</td></tr>
                <tr><td><code>lakehouse_tenant_last_write_timestamp</code></td><td>Gauge</td><td>Data freshness monitoring</td></tr>
                <tr><td><code>lakehouse_tenant_last_query_timestamp</code></td><td>Gauge</td><td>Activity tracking</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>Lakehouse Explorer UI</h2>
            <p style={{fontSize: '1.05rem', lineHeight: 1.7}}>
              Built-in management dashboard injected as a tab in the VictoriaLogs VMUI.
              Three tabs for operational visibility:
            </p>
            <ul style={{fontSize: '1.05rem', lineHeight: '2'}}>
              <li><strong>Storage Overview</strong> &mdash; total files, bytes, compression ratio, cost estimate, storage class distribution</li>
              <li><strong>Tenants</strong> &mdash; per-tenant file count, bytes, rows, time range, last write/query timestamps</li>
              <li><strong>Cardinality</strong> &mdash; promoted vs MAP column distribution, field type breakdown, top cardinality fields</li>
            </ul>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>Global Read Mode</h2>
            <p style={{fontSize: '1.05rem', lineHeight: 1.7}}>
              Optional cross-tenant query capability for admin dashboards and compliance audits.
              Disabled by default, requires explicit opt-in via header matching.
              Query across all tenants from a single Grafana datasource for fleet-wide visibility.
            </p>
          </div>
        </div>

        <div className="text--center margin-vert--xl">
          <Link to="/docs/multi-tenancy/" className="button button--primary button--lg">
            Multi-Tenancy Docs
          </Link>
          {' '}
          <Link to="/docs/tenant-stats/" className="button button--secondary button--lg">
            Tenant Stats API
          </Link>
          {' '}
          <Link to="/docs/lakehouse-explorer/" className="button button--secondary button--lg">
            Explorer UI
          </Link>
        </div>
      </main>
    </Layout>
  );
}
