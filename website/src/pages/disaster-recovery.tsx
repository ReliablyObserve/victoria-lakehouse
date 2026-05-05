import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function DisasterRecovery() {
  return (
    <Layout
      title="Disaster Recovery"
      description="Use Victoria Lakehouse as a disaster recovery solution for observability data">
      <main className="container margin-vert--lg">
        <div className="row">
          <div className="col col--8 col--offset-2">
            <h1>Disaster Recovery with Victoria Lakehouse</h1>

            <section className="margin-vert--lg">
              <h2>The Problem</h2>
              <p>
                When your hot observability cluster (VictoriaLogs or VictoriaTraces) goes down due to outages,
                upgrades, or migrations, you lose access to historical data. This creates a critical visibility
                gap during incident response and investigation.
              </p>
            </section>

            <section className="margin-vert--lg">
              <h2>The Solution</h2>
              <p>
                Victoria Lakehouse stores all data to S3 in parallel with your hot cluster. When the hot cluster
                is unavailable, your observability infrastructure transparently falls back to querying cold data
                from S3 — slower but always available.
              </p>

              <div className="cost-metric">
                <strong>Recovery Time:</strong> Minutes to enable fallback to Lakehouse via vmauth or Grafana datasource config<br/>
                <strong>Data Loss:</strong> Zero — all data is already replicated to S3<br/>
                <strong>Cost:</strong> S3 queries are ~$0.0007 per GB scanned (vs EBS hot tier)
              </div>
            </section>

            <section className="margin-vert--lg">
              <h2>Deployment Patterns</h2>

              <h3>Pattern 1: Automatic Failover with vmauth</h3>
              <pre><code>{`
vmauth:
  routing:
    - port: 9449
      routes:
        - path: /select
          backends:
            - url: http://vlselect:8481
              weight: 1
            - url: http://lakehouse-select:9428  # Fallback
              weight: 1
          loadBalancing: "first_available"
`}</code></pre>

              <h3>Pattern 2: Manual Failover</h3>
              <ol>
                <li>Grafana is pointing to vlselect (primary)</li>
                <li>If vlselect is down, manually reconfigure datasource to point to lakehouse-select</li>
                <li>Queries work against cold S3 data with ~50-150ms latency</li>
              </ol>
            </section>

            <section className="margin-vert--lg">
              <h2>Key Benefits</h2>
              <ul>
                <li><strong>Zero Data Loss:</strong> Everything written to hot cluster is mirrored to S3</li>
                <li><strong>Always-On Observability:</strong> Query cold data while hot cluster recovers</li>
                <li><strong>Transparent to Applications:</strong> No changes to vlagent, OTEL Collector, or Grafana configuration</li>
                <li><strong>Cost-Effective DR:</strong> S3 is 10-20x cheaper than maintaining a hot standby</li>
                <li><strong>Multi-AZ Built-In:</strong> S3 already provides 11 nines of durability across AZs</li>
              </ul>
            </section>

            <section className="margin-vert--lg">
              <h2>Next Steps</h2>
              <p>
                Learn how to set up Victoria Lakehouse for your environment:
              </p>
              <Link to="/docs/getting-started/" className="button button--primary">
                Getting Started
              </Link>
              {' '}
              <Link to="/docs/deployment-architecture/" className="button button--secondary">
                Deployment Architecture
              </Link>
            </section>
          </div>
        </div>
      </main>
    </Layout>
  );
}
