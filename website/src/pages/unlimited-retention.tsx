import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function UnlimitedRetention() {
  return (
    <Layout
      title="Unlimited Retention"
      description="Store years of observability data cost-effectively with Victoria Lakehouse">
      <header className="hero hero--primary" style={{padding: '3rem 0', textAlign: 'center'}}>
        <div className="container">
          <h1 className="hero__title">Unlimited Retention</h1>
          <p className="hero__subtitle" style={{maxWidth: 600, margin: '0 auto'}}>
            Keep years of logs and traces accessible on S3. Compliance-ready,
            queryable, at a fraction of EBS cost.
          </p>
        </div>
      </header>

      <main className="container margin-vert--xl">
        <div className="row margin-bottom--xl">
          <div className="col col--6">
            <h2>The Challenge</h2>
            <p style={{fontSize: '1.1rem'}}>
              VictoriaLogs and VictoriaTraces retention on EBS is expensive.
              Keeping 1 year of data costs ~$303,000/month for 1 PB/month ingestion.
              Yet regulations often require 1-2 years of available data.
            </p>
          </div>
          <div className="col col--6">
            <h2>The Solution</h2>
            <p style={{fontSize: '1.1rem'}}>
              Keep 1-3 months hot on EBS, everything else on S3 with automatic lifecycle
              tiering. vlselect/vtselect fans out to both tiers transparently.
            </p>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>Storage Tiers</h2>
            <table style={{width: '100%'}}>
              <thead>
                <tr>
                  <th>Tier</th>
                  <th>Age</th>
                  <th>Storage</th>
                  <th>Query Speed</th>
                  <th>Cost/GB/mo</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td><strong>Hot</strong></td>
                  <td>0-30 days</td>
                  <td>VL/VT on EBS</td>
                  <td>&lt;100ms</td>
                  <td>$0.10-0.15</td>
                </tr>
                <tr>
                  <td><strong>Cold</strong></td>
                  <td>30-90 days</td>
                  <td>Lakehouse on S3 Standard</td>
                  <td>100-300ms</td>
                  <td>$0.023</td>
                </tr>
                <tr>
                  <td><strong>Archive</strong></td>
                  <td>90+ days</td>
                  <td>S3 Glacier Deep Archive</td>
                  <td>500ms-5s</td>
                  <td>$0.004</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>90%</strong>
              cost savings at 2 year retention vs all-EBS
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>$0.004</strong>
              per GB/month on Glacier Deep Archive
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>11 nines</strong>
              S3 durability with no replication overhead
            </div>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>Use Cases</h2>
            <ul style={{fontSize: '1.1rem', lineHeight: '2'}}>
              <li><strong>Compliance</strong> — SOC 2, ISO 27001, HIPAA require 2+ years retention</li>
              <li><strong>Forensic Analysis</strong> — query incident logs from months ago</li>
              <li><strong>Trend Analysis</strong> — year-over-year performance comparison</li>
              <li><strong>Capacity Planning</strong> — historical patterns inform future infrastructure</li>
              <li><strong>Audit Trail</strong> — immutable, queryable logs for years</li>
            </ul>
          </div>
        </div>

        <div className="text--center margin-vert--xl">
          <Link to="/docs/getting-started/" className="button button--primary button--lg">
            Get Started
          </Link>
          {' '}
          <Link to="/docs/cost-estimates/" className="button button--secondary button--lg">
            Calculate Your Savings
          </Link>
        </div>
      </main>
    </Layout>
  );
}
