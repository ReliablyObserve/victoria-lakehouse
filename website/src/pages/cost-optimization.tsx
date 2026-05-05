import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function CostOptimization() {
  return (
    <Layout
      title="Cost Optimization"
      description="Reduce observability costs by 60-96% with Victoria Lakehouse">
      <main className="container margin-vert--lg">
        <div className="row">
          <div className="col col--8 col--offset-2">
            <h1>Cost Optimization with Victoria Lakehouse</h1>

            <section className="margin-vert--lg">
              <h2>The Cost Problem at Scale</h2>
              <p>
                EBS costs scale linearly with retention. A PB/month of logs retained for 1 year on all-EBS
                infrastructure costs ~$303,000/month — even with all-inclusive storage, replication, and backup.
              </p>

              <div className="cost-metric">
                <strong>250 GB/mo, 1 year retention:</strong> $282/mo all-EBS vs $138/mo hybrid = 51% savings<br/>
                <strong>1 PB/mo, 1 year retention:</strong> $303,000/mo all-EBS vs $48,513/mo hybrid = 84% savings<br/>
                <strong>1 PB/mo, 2 year retention:</strong> $591,000/mo all-EBS vs $60,963/mo hybrid = 90% savings
              </div>
            </section>

            <section className="margin-vert--lg">
              <h2>Why S3 is So Much Cheaper</h2>
              <ul>
                <li><strong>3-6x cheaper per GB:</strong> S3 Standard is $0.023/GB/month vs EBS $0.10-0.15/GB/month</li>
                <li><strong>Multi-AZ included:</strong> S3 is 11 nines durable across AZs with no extra cost</li>
                <li><strong>No compute tax:</strong> Query S3 directly; no need for standby replicas</li>
                <li><strong>Lifecycle tiers:</strong> Transparent tiering to Glacier ($0.004/GB/month) after 30 days</li>
              </ul>
            </section>

            <section className="margin-vert--lg">
              <h2>Hybrid Tier Architecture</h2>
              <pre><code>{`
Hot Tier (1 month, EBS):       Fast queries, high cost
  vlstorage/vtstorage (multi-AZ replica)

Cold Tier (unlimited, S3):     Slow queries, low cost
  Victoria Lakehouse (Parquet on S3)

Query Router:
  vlselect/vtselect fan-out to both tiers
  Manifest check prevents cold queries for recent data
`}</code></pre>
            </section>

            <section className="margin-vert--lg">
              <h2>Real-World Example: 1 PB/month Ingestion</h2>
              <table>
                <thead>
                  <tr>
                    <th>Metric</th>
                    <th>All-EBS (3 AZ)</th>
                    <th>Hybrid (1mo hot + S3 cold)</th>
                    <th>Savings</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>Monthly Ingest</td>
                    <td>1 PB</td>
                    <td>1 PB</td>
                    <td>—</td>
                  </tr>
                  <tr>
                    <td>Retention</td>
                    <td>1 Year</td>
                    <td>1 Year</td>
                    <td>—</td>
                  </tr>
                  <tr>
                    <td>Storage Cost</td>
                    <td>$303,000/mo</td>
                    <td>$48,513/mo</td>
                    <td>84%</td>
                  </tr>
                  <tr>
                    <td>Compute (Queries)</td>
                    <td>Included</td>
                    <td>~$500/mo</td>
                    <td>Negligible</td>
                  </tr>
                  <tr>
                    <td>Total</td>
                    <td>~$303,000/mo</td>
                    <td>~$49,000/mo</td>
                    <td>84%</td>
                  </tr>
                </tbody>
              </table>
            </section>

            <section className="margin-vert--lg">
              <h2>Advanced Cost Optimization</h2>
              <ul>
                <li><strong>Lifecycle Tiering:</strong> Auto-tier S3 Standard → Glacier after 30 days = 5x cheaper</li>
                <li><strong>Cost-Aware Deletion:</strong> Tomb stone deleted data instantly (no rewrite cost)</li>
                <li><strong>Intelligent Tiering:</strong> Let AWS move data between access tiers automatically</li>
                <li><strong>Query Filtering:</strong> Push predicates down to Parquet to reduce S3 data scanned</li>
              </ul>
            </section>

            <section className="margin-vert--lg">
              <h2>Next Steps</h2>
              <p>
                Calculate your exact savings based on ingestion rate and retention needs:
              </p>
              <Link to="/docs/cost-estimates/" className="button button--primary">
                Cost Estimator
              </Link>
              {' '}
              <Link to="/guides/getting-started/" className="button button--secondary">
                Get Started
              </Link>
            </section>
          </div>
        </div>
      </main>
    </Layout>
  );
}
