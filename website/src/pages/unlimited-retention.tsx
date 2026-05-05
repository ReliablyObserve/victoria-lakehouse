import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function UnlimitedRetention() {
  return (
    <Layout
      title="Unlimited Retention"
      description="Store years of observability data cost-effectively with Victoria Lakehouse">
      <main className="container margin-vert--lg">
        <div className="row">
          <div className="col col--8 col--offset-2">
            <h1>Unlimited Retention with Victoria Lakehouse</h1>

            <section className="margin-vert--lg">
              <h2>The Challenge</h2>
              <p>
                VictoriaLogs and VictoriaTraces retention on EBS is expensive. Keeping 1 year of data costs
                ~$303,000/month for 1 PB/month ingestion. 2+ years becomes prohibitively expensive.
              </p>
              <p>
                Yet regulations, compliance, and forensic analysis often require keeping 1-2 years of data available.
              </p>
            </section>

            <section className="margin-vert--lg">
              <h2>The Solution: Hybrid Tiers</h2>
              <div className="cost-metric">
                <strong>Hot tier:</strong> 1-3 months on EBS (fast queries)<br/>
                <strong>Cold tier:</strong> Unlimited on S3 + Glacier (cheap storage, slower queries)<br/>
                <strong>Query result:</strong> Transparent to applications — vlselect/vtselect fan-out to both
              </div>

              <p>
                Keep 2 years of data accessible for compliance and forensic investigation without breaking your budget.
              </p>
            </section>

            <section className="margin-vert--lg">
              <h2>Tiering Strategy</h2>
              <pre><code>{`
1. Write to hot cluster (VictoriaLogs/Traces on EBS)
   ↓ Mirror to Lakehouse (S3 Standard)
   ↓ (after 30 days) S3 Intelligent-Tiering
   ↓ (after 90 days) S3 Glacier Deep Archive ($0.0036/GB/month)
   ↓ (after 2 years) Lifecycle expiry or keep indefinitely

Query Path:
  - Recent data (hot):  vlselect/vtselect → EBS (50-100ms)
  - Old data (cold):    lakehouse-select  → S3/Glacier (100-500ms)
  - Mixed:              Both tiers merged transparently
`}</code></pre>
            </section>

            <section className="margin-vert--lg">
              <h2>Use Cases</h2>
              <ul>
                <li><strong>Compliance:</strong> Keep 2+ years for SOC 2, ISO 27001, HIPAA requirements</li>
                <li><strong>Forensic Analysis:</strong> Query incident logs from months ago without re-indexing</li>
                <li><strong>Trend Analysis:</strong> Year-over-year performance comparison</li>
                <li><strong>Capacity Planning:</strong> Historical patterns inform future infrastructure</li>
                <li><strong>Audit Trail:</strong> Immutable, queryable audit logs for years</li>
              </ul>
            </section>

            <section className="margin-vert--lg">
              <h2>Cost Comparison: 2 Year Retention</h2>
              <table>
                <thead>
                  <tr>
                    <th>Storage</th>
                    <th>All-EBS</th>
                    <th>Hybrid (1mo hot + S3 cold)</th>
                    <th>Savings</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>1 PB/mo ingestion, 2 years</td>
                    <td>$591,000/mo</td>
                    <td>$60,963/mo</td>
                    <td>90%</td>
                  </tr>
                </tbody>
              </table>
            </section>

            <section className="margin-vert--lg">
              <h2>Query Performance</h2>
              <ul>
                <li><strong>Hot tier (recent):</strong> &lt;100ms (instant results)</li>
                <li><strong>Cold tier (S3 Standard):</strong> 100-300ms (good for analysis)</li>
                <li><strong>Cold tier (Glacier):</strong> 500ms-5s (acceptable for compliance lookups)</li>
              </ul>
              <p>
                Queries spanning both tiers merge results transparently — users don't need to know which tier data came from.
              </p>
            </section>

            <section className="margin-vert--lg">
              <h2>Next Steps</h2>
              <Link to="/docs/getting-started/" className="button button--primary">
                Get Started
              </Link>
              {' '}
              <Link to="/docs/cost-estimates/" className="button button--secondary">
                Calculate Your Savings
              </Link>
            </section>
          </div>
        </div>
      </main>
    </Layout>
  );
}
