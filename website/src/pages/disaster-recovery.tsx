import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function DisasterRecovery() {
  return (
    <Layout
      title="Disaster Recovery"
      description="Use Victoria Lakehouse as a disaster recovery solution for observability data">
      <header className="hero hero--primary" style={{padding: '3rem 0', textAlign: 'center'}}>
        <div className="container">
          <h1 className="hero__title">Disaster Recovery</h1>
          <p className="hero__subtitle" style={{maxWidth: 600, margin: '0 auto'}}>
            When your hot cluster goes down, Lakehouse serves all data from S3.
            Zero data loss, always available.
          </p>
        </div>
      </header>

      <main className="container margin-vert--xl">
        <div className="row margin-bottom--xl">
          <div className="col col--6">
            <h2>The Problem</h2>
            <p>
              When your hot observability cluster (VictoriaLogs or VictoriaTraces) goes down
              due to outages, upgrades, or migrations, you lose access to historical data.
              This creates a critical visibility gap during incident response.
            </p>
          </div>
          <div className="col col--6">
            <h2>The Solution</h2>
            <p>
              Victoria Lakehouse stores all data to S3 in parallel with your hot cluster.
              When it's unavailable, your infrastructure transparently falls back to querying
              cold data from S3 — slower but always available.
            </p>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>Minutes</strong>
              Recovery time via vmauth failover or Grafana datasource switch
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>Zero</strong>
              Data loss — all data is already replicated to S3
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>11 nines</strong>
              S3 durability across multiple availability zones
            </div>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>Key Benefits</h2>
            <ul style={{fontSize: '1.1rem', lineHeight: '2'}}>
              <li><strong>Zero Data Loss</strong> — everything written to hot cluster is mirrored to S3</li>
              <li><strong>Always-On Observability</strong> — query cold data while hot cluster recovers</li>
              <li><strong>Transparent to Applications</strong> — no changes to vlagent, OTEL Collector, or Grafana</li>
              <li><strong>Cost-Effective DR</strong> — S3 is 10-20x cheaper than maintaining a hot standby</li>
              <li><strong>Multi-AZ Built-In</strong> — S3 provides durability across AZs with no extra cost</li>
            </ul>
          </div>
        </div>

        <div className="text--center margin-vert--xl">
          <Link to="/docs/getting-started/" className="button button--primary button--lg">
            Getting Started
          </Link>
          {' '}
          <Link to="/docs/architecture/" className="button button--secondary button--lg">
            Architecture
          </Link>
        </div>
      </main>
    </Layout>
  );
}
