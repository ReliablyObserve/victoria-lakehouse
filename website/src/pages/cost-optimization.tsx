import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function CostOptimization() {
  return (
    <Layout
      title="Cost Optimization"
      description="Reduce observability costs by 60-96% with Victoria Lakehouse">
      <header className="hero hero--primary" style={{padding: '3rem 0', textAlign: 'center'}}>
        <div className="container">
          <h1 className="hero__title">Cost Optimization</h1>
          <p className="hero__subtitle" style={{maxWidth: 600, margin: '0 auto'}}>
            Reduce observability storage costs by 60-96% with S3-backed cold storage.
          </p>
        </div>
      </header>

      <main className="container margin-vert--xl">
        <div className="row margin-bottom--xl">
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2.5rem', display: 'block', marginBottom: '0.5rem'}}>51%</strong>
              savings at 250 GB/day
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2.5rem', display: 'block', marginBottom: '0.5rem'}}>84%</strong>
              savings at 1 PB/month
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2.5rem', display: 'block', marginBottom: '0.5rem'}}>90%</strong>
              savings at 2 year retention
            </div>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>The Cost Problem at Scale</h2>
            <p style={{fontSize: '1.1rem'}}>
              EBS costs scale linearly with retention. A PB/month of logs retained for 1 year
              on all-EBS infrastructure costs ~$303,000/month. With Lakehouse hybrid architecture,
              that drops to ~$49,000/month.
            </p>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>Why S3 is So Much Cheaper</h2>
            <ul style={{fontSize: '1.1rem', lineHeight: '2'}}>
              <li><strong>3-6x cheaper per GB</strong> — S3 Standard $0.023/GB vs EBS $0.10-0.15/GB</li>
              <li><strong>Multi-AZ included</strong> — 11 nines durability across AZs at no extra cost</li>
              <li><strong>No compute tax</strong> — query S3 directly, no standby replicas needed</li>
              <li><strong>Lifecycle tiers</strong> — auto-tier to Glacier ($0.004/GB) after configurable period</li>
            </ul>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>Hybrid Tier Architecture</h2>
            <table style={{width: '100%'}}>
              <thead>
                <tr>
                  <th>Tier</th>
                  <th>Storage</th>
                  <th>Retention</th>
                  <th>Query Speed</th>
                  <th>Cost/GB</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td><strong>Hot</strong></td>
                  <td>VL/VT on EBS</td>
                  <td>1 month</td>
                  <td>&lt;10ms</td>
                  <td>$0.10-0.15</td>
                </tr>
                <tr>
                  <td><strong>Cold</strong></td>
                  <td>Lakehouse on S3</td>
                  <td>Unlimited</td>
                  <td>50-150ms</td>
                  <td>$0.023</td>
                </tr>
                <tr>
                  <td><strong>Archive</strong></td>
                  <td>S3 Glacier</td>
                  <td>Unlimited</td>
                  <td>Minutes</td>
                  <td>$0.004</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <div className="text--center margin-vert--xl">
          <Link to="/docs/cost-estimates/" className="button button--primary button--lg">
            Full Cost Analysis
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
