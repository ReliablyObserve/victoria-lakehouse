import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function LokiTempoAlternative() {
  return (
    <Layout
      title="Loki & Tempo Alternative — Victoria Lakehouse vs Grafana Loki and Tempo"
      description="Victoria Lakehouse is 22-53% cheaper than Grafana Loki and Tempo. VL/VT's 47-70x compression beats Loki's 3.5x. Open Parquet format vs proprietary chunks. Apache 2.0 vs AGPL. Sub-10ms hot queries vs 100ms+. Unified logs and traces in one system.">
      <header className="hero hero--primary" style={{padding: '3rem 0', textAlign: 'center'}}>
        <div className="container">
          <h1 className="hero__title">Loki &amp; Tempo Alternative</h1>
          <p className="hero__subtitle" style={{maxWidth: 700, margin: '0 auto'}}>
            22&ndash;53% cheaper. 47&ndash;70x compression vs 3.5x. Open Parquet vs proprietary chunks.
            Apache 2.0 vs AGPL.
          </p>
        </div>
      </header>

      <main className="container margin-vert--xl">
        <div className="row margin-bottom--xl">
          <div className="col col--3">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>22&ndash;53%</strong>
              cheaper at 500 GB/day
            </div>
          </div>
          <div className="col col--3">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>47&ndash;70x</strong>
              compression vs Loki's 3.5x
            </div>
          </div>
          <div className="col col--3">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>&lt;10ms</strong>
              hot tier queries (Loki: 100ms+)
            </div>
          </div>
          <div className="col col--3">
            <div className="cost-metric" style={{textAlign: 'center', height: '100%'}}>
              <strong style={{fontSize: '2rem', display: 'block', marginBottom: '0.5rem'}}>Apache 2.0</strong>
              no AGPL restrictions
            </div>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--10 col--offset-1">
            <h2>Feature Comparison</h2>
            <table style={{width: '100%'}}>
              <thead>
                <tr>
                  <th>Feature</th>
                  <th>Victoria Lakehouse</th>
                  <th>Grafana Loki + Tempo</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td><strong>License</strong></td>
                  <td>Apache 2.0</td>
                  <td>AGPLv3</td>
                </tr>
                <tr>
                  <td><strong>Log compression</strong></td>
                  <td>47&ndash;70x (VL native) / 6&ndash;9x (Parquet ZSTD)</td>
                  <td>3.5x</td>
                </tr>
                <tr>
                  <td><strong>Hot query speed</strong></td>
                  <td>&lt;10ms (EBS hot tier)</td>
                  <td>100&ndash;500ms (all S3)</td>
                </tr>
                <tr>
                  <td><strong>Cold query speed</strong></td>
                  <td>&lt;500ms (Parquet columnar + bloom)</td>
                  <td>1&ndash;10s (chunk sequential scan)</td>
                </tr>
                <tr>
                  <td><strong>Data format</strong></td>
                  <td>Open Apache Parquet</td>
                  <td>Proprietary chunks (Loki-only)</td>
                </tr>
                <tr>
                  <td><strong>Traces</strong></td>
                  <td>Native VT + Jaeger + Tempo API (same binary)</td>
                  <td>Separate Tempo deployment</td>
                </tr>
                <tr>
                  <td><strong>Query language</strong></td>
                  <td>LogsQL (full-text, regex, field ops, pipes)</td>
                  <td>LogQL (label matchers, line filters)</td>
                </tr>
                <tr>
                  <td><strong>Analytics engines</strong></td>
                  <td>DuckDB, ClickHouse, Spark, Trino, 9+ total</td>
                  <td>Loki API only</td>
                </tr>
                <tr>
                  <td><strong>Point lookups (trace_id)</strong></td>
                  <td>&lt;100ms (bloom filter)</td>
                  <td>1&ndash;5s (sequential scan)</td>
                </tr>
                <tr>
                  <td><strong>Ingestion formats</strong></td>
                  <td>11+ (Loki, ES, OTLP, Syslog, Fluentd, DD, Jaeger...)</td>
                  <td>Loki push + OTLP (logs), Tempo push + OTLP (traces)</td>
                </tr>
                <tr>
                  <td><strong>Grafana compatibility</strong></td>
                  <td>VL + Jaeger + Tempo datasources, Loki via proxy</td>
                  <td>Native Loki + Tempo datasources</td>
                </tr>
                <tr>
                  <td><strong>Glacier tiering</strong></td>
                  <td>Yes (S3 lifecycle, cost-aware deletion)</td>
                  <td>No (compaction breaks lifecycle)</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--10 col--offset-1">
            <h2>Cost Comparison (500 GB/day, 1 Year, 3 AZ)</h2>
            <table style={{width: '100%'}}>
              <thead>
                <tr>
                  <th>Component</th>
                  <th>Victoria Lakehouse Hybrid</th>
                  <th>Loki + Tempo (Simple Scalable)</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>Hot tier compute</td>
                  <td>VL/VT cluster: 3x m6i.xlarge = $432/mo</td>
                  <td>Loki read+write+backend: 6x m6i.xlarge = $864/mo</td>
                </tr>
                <tr>
                  <td>Hot tier storage</td>
                  <td>EBS gp3 30d: $3,600/mo</td>
                  <td>N/A (all S3)</td>
                </tr>
                <tr>
                  <td>Cold tier storage</td>
                  <td>S3 Standard ~180TB: $4,140/mo</td>
                  <td>S3 Standard ~180TB: $4,140/mo</td>
                </tr>
                <tr>
                  <td>S3 requests</td>
                  <td>~$50/mo</td>
                  <td>~$200/mo</td>
                </tr>
                <tr>
                  <td>Index storage</td>
                  <td>Included (manifest)</td>
                  <td>DynamoDB/BoltDB: ~$300/mo</td>
                </tr>
                <tr style={{fontWeight: 'bold'}}>
                  <td>Total/month</td>
                  <td>~$8,222/mo</td>
                  <td>~$5,504/mo</td>
                </tr>
              </tbody>
            </table>
            <p style={{fontSize: '0.9rem', color: 'var(--ifm-color-emphasis-600)', marginTop: '0.5rem'}}>
              Note: Loki appears cheaper on pure storage because it has no hot EBS tier, but the
              performance and feature differences above change the calculus. At 1 PB/month, Lakehouse
              saves $614K/year (52%) vs Loki/Tempo.
            </p>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--5 col--offset-1">
            <h2>When to Choose Lakehouse</h2>
            <ul style={{fontSize: '1.05rem', lineHeight: '2'}}>
              <li>Query speed matters (sub-10ms hot, sub-500ms cold)</li>
              <li>Need open format for analytics (DuckDB, Spark, Trino)</li>
              <li>Unified logs + traces in one system</li>
              <li>Apache 2.0 license requirement</li>
              <li>Glacier tiering for 3yr+ compliance</li>
              <li>VictoriaMetrics ecosystem (VM, VL, VT)</li>
            </ul>
          </div>
          <div className="col col--5">
            <h2>When to Choose Loki/Tempo</h2>
            <ul style={{fontSize: '1.05rem', lineHeight: '2'}}>
              <li>Cost-first, simple ops, Grafana-native</li>
              <li>Large community, proven at mega-scale</li>
              <li>Already invested in Grafana Cloud</li>
              <li>Don't need analytics on cold data</li>
              <li>AGPL is acceptable</li>
            </ul>
          </div>
        </div>

        <div className="row margin-bottom--xl">
          <div className="col col--8 col--offset-2">
            <h2>Migration Path</h2>
            <p style={{fontSize: '1.1rem', lineHeight: 1.7}}>
              Victoria Lakehouse supports the <strong>Loki push API</strong> for ingestion,
              meaning your existing Promtail/Alloy/Fluentd pipelines work with zero changes.
              For queries, use <strong>loki-vl-proxy</strong> to translate LogQL to LogsQL,
              so your existing Grafana dashboards continue to work.
              Migrate incrementally: run both in parallel, verify data parity, then cut over.
            </p>
          </div>
        </div>

        <div className="text--center margin-vert--xl">
          <Link to="/docs/cost-comparison/" className="button button--primary button--lg">
            Detailed Cost Comparison
          </Link>
          {' '}
          <Link to="/docs/getting-started/" className="button button--secondary button--lg">
            Try It Now
          </Link>
        </div>
      </main>
    </Layout>
  );
}
