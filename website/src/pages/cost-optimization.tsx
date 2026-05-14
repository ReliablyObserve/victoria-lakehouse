import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function CostOptimization() {
  return (
    <Layout
      title="Cost Optimization — Reduce Observability Storage Costs by 60-96% with S3 Parquet"
      description="Victoria Lakehouse reduces observability storage costs by 60-96% vs all-EBS. S3 is 3-6x cheaper per GB. Glacier tiering drops to $0.004/GB. VL/VT 47-70x compression combined with Parquet columnar format. Detailed cost calculator for 250 GB/day to 1 PB/month.">
      <header className="hero hero--primary" style={{padding: '3rem 0', textAlign: 'center'}}>
        <div className="container">
          <h1 className="hero__title">Cost Optimization</h1>
          <p className="hero__subtitle" style={{maxWidth: 700, margin: '0 auto'}}>
            Reduce observability storage costs by 60&ndash;96% with S3 Parquet cold storage.
            Same APIs, same queries, dramatically lower bills.
          </p>
        </div>
      </header>

      <main className="container margin-vert--xl">
        <div className="row margin-bottom--xl">
          <div className="col col--3">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2.5rem', display: 'block', marginBottom: '0.5rem'}}>22%</strong>
              savings at 500 GB/day vs Loki
            </div>
          </div>
          <div className="col col--3">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2.5rem', display: 'block', marginBottom: '0.5rem'}}>53%</strong>
              savings at 1 PB/month vs Loki
            </div>
          </div>
          <div className="col col--3">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2.5rem', display: 'block', marginBottom: '0.5rem'}}>$614K</strong>
              annual savings at 1 PB/month
            </div>
          </div>
          <div className="col col--3">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2.5rem', display: 'block', marginBottom: '0.5rem'}}>$0.004</strong>
              per GB/month on Glacier
            </div>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>The Cost Problem at Scale</h2>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              EBS costs scale linearly with retention. A PB/month of logs retained for 1 year
              on all-EBS costs ~$54,900/month. With Lakehouse hybrid (1 month hot EBS + S3 cold),
              that drops to ~$46,400/month. Against Loki/Tempo, you save $614K/year.
            </p>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              VL/VT's 47&ndash;70x compression makes EBS-only cheapest for short retention.
              Lakehouse adds value through <strong>open Parquet format</strong> (9+ analytics engines),{' '}
              <strong>S3 11-nines durability</strong>, <strong>disaster recovery</strong>,
              and <strong>Glacier tiering</strong> for 3yr+ compliance retention.
            </p>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>Why S3 + Parquet is Cheaper</h2>
            <ul style={{fontSize: '1.1rem', lineHeight: '2'}}>
              <li><strong>3&ndash;6x cheaper per GB</strong> &mdash; S3 Standard $0.023/GB vs EBS $0.08&ndash;0.15/GB</li>
              <li><strong>Multi-AZ included</strong> &mdash; 11 nines durability across AZs at no extra cost</li>
              <li><strong>No IOPS provisioning</strong> &mdash; no EBS volume sizing, snapshot management, or AZ replication</li>
              <li><strong>Lifecycle tiers</strong> &mdash; auto-tier to IA ($0.0125/GB) or Glacier ($0.004/GB)</li>
              <li><strong>Columnar compression</strong> &mdash; Parquet + ZSTD achieves 6&ndash;9x compression on logs</li>
              <li><strong>L2 disk cache</strong> &mdash; $4&ndash;16/month of local EBS cache avoids repeated S3 GETs</li>
            </ul>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--10 col--offset-1">
            <h2>Hybrid Tier Architecture</h2>
            <table style={{width: '100%'}}>
              <thead>
                <tr>
                  <th>Tier</th>
                  <th>Storage</th>
                  <th>Retention</th>
                  <th>Query Speed</th>
                  <th>Cost/GB/mo</th>
                  <th>Use Case</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td><strong>Hot</strong></td>
                  <td>VL/VT on EBS</td>
                  <td>1&ndash;3 months</td>
                  <td>&lt;10ms</td>
                  <td>$0.08&ndash;0.15</td>
                  <td>Real-time alerting, dashboards</td>
                </tr>
                <tr>
                  <td><strong>Cold</strong></td>
                  <td>Lakehouse on S3 Standard</td>
                  <td>3&ndash;12 months</td>
                  <td>50&ndash;300ms</td>
                  <td>$0.023</td>
                  <td>Incident investigation, trends</td>
                </tr>
                <tr>
                  <td><strong>Cool</strong></td>
                  <td>S3 Infrequent Access</td>
                  <td>1&ndash;3 years</td>
                  <td>100&ndash;500ms</td>
                  <td>$0.0125</td>
                  <td>Compliance, forensics</td>
                </tr>
                <tr>
                  <td><strong>Archive</strong></td>
                  <td>S3 Glacier Deep Archive</td>
                  <td>3+ years</td>
                  <td>Minutes&ndash;hours</td>
                  <td>$0.004</td>
                  <td>Legal hold, regulatory</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--10 col--offset-1">
            <h2>Cost at Scale (1 PB/month, 1 Year Retention, 3 AZ)</h2>
            <table style={{width: '100%'}}>
              <thead>
                <tr>
                  <th>Solution</th>
                  <th>Monthly Cost</th>
                  <th>Annual Cost</th>
                  <th>vs. Loki/Tempo</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td><strong>VL/VT EBS Only</strong></td>
                  <td>$54,900/mo</td>
                  <td>$658,800/yr</td>
                  <td>40% cheaper</td>
                </tr>
                <tr>
                  <td><strong>Lakehouse Hybrid</strong></td>
                  <td>$46,400/mo</td>
                  <td>$556,800/yr</td>
                  <td>49% cheaper</td>
                </tr>
                <tr>
                  <td><strong>Loki + Tempo</strong></td>
                  <td>$91,400/mo</td>
                  <td>$1,096,800/yr</td>
                  <td>&mdash;</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>Cost-Aware Deletion</h2>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              Victoria Lakehouse supports VL-compatible delete APIs with intelligent storage-class awareness.
              Three deletion modes prevent accidental Glacier retrieval fees ($40&ndash;120/file):
            </p>
            <ul style={{fontSize: '1.05rem', lineHeight: '2'}}>
              <li><strong>Hide (Tombstone)</strong> &mdash; instant, $0, data invisible to queries immediately</li>
              <li><strong>Permanent (Rewrite)</strong> &mdash; physical removal from S3 Standard files only</li>
              <li><strong>Auto (Smart)</strong> &mdash; tombstone immediately, background rewrite for Standard, lifecycle expiry for Glacier</li>
            </ul>
          </div>
        </div>

        <div className="text--center margin-vert--xl">
          <Link to="/docs/cost-estimates/" className="button button--primary button--lg">
            Full Cost Calculator
          </Link>
          {' '}
          <Link to="/docs/cost-comparison/" className="button button--secondary button--lg">
            Detailed Comparison
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
