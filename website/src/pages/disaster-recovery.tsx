import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function DisasterRecovery() {
  return (
    <Layout
      title="Disaster Recovery — Always-On Observability with S3 Cold Tier Failover"
      description="Victoria Lakehouse provides disaster recovery for VictoriaLogs and VictoriaTraces. When the hot cluster goes down, Lakehouse serves all data from S3. Zero data loss, 11 nines S3 durability, minutes-to-recover via vmauth failover.">
      <header className="hero hero--primary" style={{padding: '3rem 0', textAlign: 'center'}}>
        <div className="container">
          <h1 className="hero__title">Disaster Recovery</h1>
          <p className="hero__subtitle" style={{maxWidth: 700, margin: '0 auto'}}>
            When your hot cluster goes down, Lakehouse serves all data from S3.
            Zero data loss. Always-on observability. 11 nines durability.
          </p>
        </div>
      </header>

      <main className="container margin-vert--xl">
        <div className="row margin-bottom--xl">
          <div className="col col--6">
            <h2>The Problem</h2>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              When your VictoriaLogs or VictoriaTraces hot cluster goes down
              (outage, upgrade, migration, disk failure), you lose access to
              all observability data. This creates a critical visibility gap
              during incident response &mdash; exactly when you need it most.
            </p>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              Traditional DR solutions require expensive standby replicas.
              S3-based DR costs a fraction because S3 provides multi-AZ
              durability at no extra cost.
            </p>
          </div>
          <div className="col col--6">
            <h2>The Solution</h2>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              Victoria Lakehouse writes all data to S3 Parquet in parallel with
              your hot cluster. When VL/VT is unavailable, your infrastructure
              transparently falls back to querying cold data from S3.
            </p>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              Same query APIs (LogsQL, Jaeger, Tempo, Loki via proxy), same Grafana
              datasources. Slower (50&ndash;300ms vs &lt;10ms) but always available.
              Recovery is automatic via vmauth health checks or manual Grafana
              datasource switch.
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
              Data loss &mdash; all data is already on S3
            </div>
          </div>
          <div className="col col--4">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>11 Nines</strong>
              S3 durability (99.999999999%) across multiple AZs
            </div>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>How Failover Works</h2>
            <ol style={{fontSize: '1.1rem', lineHeight: '2.2'}}>
              <li><strong>Normal operation:</strong> vlselect/vtselect fans out to both hot (EBS) and cold (S3). Manifest check ensures &lt;1ms fast path for recent data.</li>
              <li><strong>Hot cluster down:</strong> vlselect health checks detect failure. vmauth routes all queries to Lakehouse.</li>
              <li><strong>Queries continue:</strong> Lakehouse serves from S3 Parquet. Multi-tier cache (L1 memory, L2 disk) keeps frequently accessed data fast.</li>
              <li><strong>Hot cluster recovers:</strong> vlselect re-adds hot storage nodes. Traffic automatically balances back to hot+cold.</li>
            </ol>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>Key Benefits</h2>
            <ul style={{fontSize: '1.1rem', lineHeight: '2'}}>
              <li><strong>Zero Data Loss</strong> &mdash; everything written to hot cluster is mirrored to S3</li>
              <li><strong>Always-On Observability</strong> &mdash; query cold data while hot cluster recovers</li>
              <li><strong>Transparent to Applications</strong> &mdash; no changes to vmagent, OTEL Collector, or Grafana</li>
              <li><strong>Cost-Effective DR</strong> &mdash; S3 is 10&ndash;20x cheaper than maintaining a hot standby</li>
              <li><strong>Multi-AZ Built-In</strong> &mdash; S3 provides durability across AZs with no extra cost</li>
              <li><strong>Same Query APIs</strong> &mdash; LogsQL, Jaeger, Tempo, Loki API all work against cold tier</li>
              <li><strong>Full History Available</strong> &mdash; cold tier has complete data history, not just recent</li>
            </ul>
          </div>
        </div>

        <div className="text--center margin-vert--xl">
          <Link to="/docs/getting-started/" className="button button--primary button--lg">
            Getting Started
          </Link>
          {' '}
          <Link to="/docs/deployment-architecture/" className="button button--secondary button--lg">
            Deployment Architecture
          </Link>
        </div>
      </main>
    </Layout>
  );
}
