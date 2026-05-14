import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function UnlimitedRetention() {
  return (
    <Layout
      title="Unlimited Log and Trace Retention — S3 Glacier Tiering for Compliance"
      description="Store years of logs and traces on S3 with Victoria Lakehouse. Glacier Deep Archive at $0.004/GB/month. SOC 2, ISO 27001, HIPAA, GDPR compliance-ready. Queryable with LogsQL, Jaeger, DuckDB, ClickHouse, Spark, Trino. 90% cheaper than EBS at 2-year retention.">
      <header className="hero hero--primary" style={{padding: '3rem 0', textAlign: 'center'}}>
        <div className="container">
          <h1 className="hero__title">Unlimited Retention</h1>
          <p className="hero__subtitle" style={{maxWidth: 700, margin: '0 auto'}}>
            Keep years of logs and traces on S3. Compliance-ready, queryable,
            at a fraction of EBS cost. Glacier Deep Archive at $0.004/GB/month.
          </p>
        </div>
      </header>

      <main className="container margin-vert--xl">
        <div className="row margin-bottom--xl">
          <div className="col col--6">
            <h2>The Challenge</h2>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              Keeping 1 year of data at 1 PB/month on all-EBS costs ~$54,900/month.
              Regulations (SOC 2, ISO 27001, HIPAA, PCI DSS) often require 1&ndash;7 years
              of available observability data. GDPR mandates right-to-erasure.
            </p>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              Traditional solutions force a choice: pay EBS costs for compliance
              or lose access to historical data.
            </p>
          </div>
          <div className="col col--6">
            <h2>The Solution</h2>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              Keep 1&ndash;3 months hot on VL/VT EBS, everything else on S3 with
              automatic lifecycle tiering. vlselect/vtselect fans out to both
              tiers transparently &mdash; queries spanning hot and cold data work
              automatically.
            </p>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              Data on S3 is queryable with LogsQL, Jaeger, Loki API, DuckDB,
              ClickHouse, Spark, Trino, and 5+ more engines. Open Parquet format
              means you're never locked in.
            </p>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--10 col--offset-1">
            <h2>Storage Tiers &amp; Lifecycle</h2>
            <table style={{width: '100%'}}>
              <thead>
                <tr>
                  <th>Tier</th>
                  <th>Age</th>
                  <th>Storage</th>
                  <th>Query Speed</th>
                  <th>Cost/GB/mo</th>
                  <th>Queryable?</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td><strong>Hot</strong></td>
                  <td>0&ndash;30 days</td>
                  <td>VL/VT on EBS</td>
                  <td>&lt;10ms</td>
                  <td>$0.08&ndash;0.15</td>
                  <td>Yes (native VL/VT)</td>
                </tr>
                <tr>
                  <td><strong>Cold</strong></td>
                  <td>30&ndash;90 days</td>
                  <td>Lakehouse on S3 Standard</td>
                  <td>50&ndash;300ms</td>
                  <td>$0.023</td>
                  <td>Yes (all APIs + SQL)</td>
                </tr>
                <tr>
                  <td><strong>Cool</strong></td>
                  <td>90 days&ndash;1 year</td>
                  <td>S3 Infrequent Access</td>
                  <td>100&ndash;500ms</td>
                  <td>$0.0125</td>
                  <td>Yes (all APIs + SQL)</td>
                </tr>
                <tr>
                  <td><strong>Archive</strong></td>
                  <td>1&ndash;7+ years</td>
                  <td>S3 Glacier Deep Archive</td>
                  <td>Hours (restore first)</td>
                  <td>$0.004</td>
                  <td>After restore</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>90%</strong>
              cost savings at 2yr retention vs all-EBS
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
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>11 Nines</strong>
              S3 durability with no replication overhead
            </div>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--6">
            <h2>Compliance Use Cases</h2>
            <ul style={{fontSize: '1.05rem', lineHeight: '2'}}>
              <li><strong>SOC 2 / ISO 27001</strong> &mdash; 1&ndash;2 year log retention required</li>
              <li><strong>HIPAA</strong> &mdash; 6-year audit trail for healthcare</li>
              <li><strong>PCI DSS</strong> &mdash; 1-year online + archive for payment processing</li>
              <li><strong>GDPR</strong> &mdash; cost-aware deletion for right-to-erasure</li>
              <li><strong>e-Discovery</strong> &mdash; queryable historical data for legal</li>
              <li><strong>Forensic Analysis</strong> &mdash; investigate incidents from months ago</li>
            </ul>
          </div>
          <div className="col col--6">
            <h2>Analytics Use Cases</h2>
            <ul style={{fontSize: '1.05rem', lineHeight: '2'}}>
              <li><strong>Trend Analysis</strong> &mdash; year-over-year performance comparison</li>
              <li><strong>Capacity Planning</strong> &mdash; historical patterns inform infrastructure</li>
              <li><strong>ML Training</strong> &mdash; train anomaly detection on historical data</li>
              <li><strong>Cost Allocation</strong> &mdash; per-tenant, per-service usage over time</li>
              <li><strong>SLA Reporting</strong> &mdash; long-term availability and error rate trends</li>
              <li><strong>Audit Trail</strong> &mdash; immutable, queryable logs for years</li>
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
          {' '}
          <Link to="/analytics-compliance/" className="button button--secondary button--lg">
            Analytics &amp; Compliance
          </Link>
        </div>
      </main>
    </Layout>
  );
}
