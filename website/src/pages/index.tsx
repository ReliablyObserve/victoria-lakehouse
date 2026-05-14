import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';

import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <h1 className="hero__title">{siteConfig.title}</h1>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <p className={styles.heroDescription}>
          Drop-in storage node for VictoriaLogs and VictoriaTraces.
          Ingest via Loki, Elasticsearch, OTLP, Syslog, Fluentd, Datadog, and 10+ formats.
          Query with LogsQL, Jaeger, Loki API, or any Parquet engine.
          60&ndash;96% cheaper than all-EBS. Apache 2.0 licensed.
        </p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/getting-started/">
            Quick Start
          </Link>
          <Link
            className="button button--outline button--lg"
            to="/ingestion-formats/">
            See All Integrations
          </Link>
          <Link
            className="button button--outline button--lg"
            to="/cost-optimization/">
            Calculate Savings
          </Link>
        </div>
      </div>
    </header>
  );
}

function HowItWorks() {
  return (
    <section className={styles.howItWorks}>
      <div className="container">
        <h2 className="text--center margin-bottom--lg">How It Works</h2>
        <div className="row">
          <div className="col col--4">
            <div className={styles.stepCard}>
              <div className={styles.stepNumber}>1</div>
              <h3>Ingest &mdash; Any Format</h3>
              <p>
                Full VictoriaLogs and VictoriaTraces insert API compatibility.
                Send data via <strong>Loki push</strong>, <strong>Elasticsearch bulk</strong>,{' '}
                <strong>OTLP</strong>, <strong>Syslog</strong>, <strong>Fluentd</strong>,{' '}
                <strong>Logstash</strong>, <strong>Datadog</strong>, <strong>Journald</strong>,{' '}
                <strong>NDJSON</strong>, or <strong>Zipkin/Jaeger</strong>.
                Every VL/VT-supported ingestion format works unchanged.
                Data is buffered, converted to columnar Parquet with ZSTD compression
                and bloom filters, then flushed to S3.
              </p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.stepCard}>
              <div className={styles.stepNumber}>2</div>
              <h3>Query &mdash; Full VL/VT API</h3>
              <p>
                Every VictoriaLogs and VictoriaTraces query endpoint works:
                {' '}<strong>LogsQL</strong> full-text search, <strong>Jaeger</strong> trace UI,{' '}
                <strong>Loki API</strong> via loki-vl-proxy, <strong>Grafana</strong> with
                VictoriaLogs and Jaeger datasources.
                vlselect/vtselect fans out to both hot (EBS) and cold (S3).
                Manifest check skips S3 for recent data in under 1ms.
                Bloom filters on service.name and trace_id enable sub-100ms point lookups.
              </p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.stepCard}>
              <div className={styles.stepNumber}>3</div>
              <h3>Analyze &mdash; Open Parquet</h3>
              <p>
                Cold data lives in standard Apache Parquet on S3. Query it with
                {' '}<strong>DuckDB</strong>, <strong>ClickHouse</strong>, <strong>Spark</strong>,{' '}
                <strong>Trino</strong>, <strong>Databricks</strong>, <strong>Snowflake</strong>,{' '}
                <strong>StarRocks</strong>, <strong>Doris</strong>, or <strong>pandas</strong>.
                Same data, two access patterns: observability APIs for operations,
                SQL analytics for business intelligence, compliance, and ML.
                No vendor lock-in &mdash; your data is standard Parquet forever.
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function IngestionFormats() {
  const formats = [
    {name: 'Loki Push API', desc: '/insert/loki/api/v1/push', type: 'logs'},
    {name: 'Elasticsearch Bulk', desc: '/_bulk, /_index/_bulk', type: 'logs'},
    {name: 'OTLP (OpenTelemetry)', desc: 'gRPC and HTTP/JSON', type: 'both'},
    {name: 'Syslog', desc: 'RFC 3164 / RFC 5424', type: 'logs'},
    {name: 'Fluentd / Fluent Bit', desc: 'Forward protocol', type: 'logs'},
    {name: 'Logstash', desc: 'HTTP output plugin', type: 'logs'},
    {name: 'Datadog', desc: 'Datadog Logs API compatible', type: 'logs'},
    {name: 'Journald', desc: 'systemd journal native', type: 'logs'},
    {name: 'NDJSON', desc: '/insert/jsonline', type: 'logs'},
    {name: 'Zipkin', desc: '/api/v2/spans', type: 'traces'},
    {name: 'Jaeger', desc: 'Thrift/gRPC', type: 'traces'},
  ];

  return (
    <section className={styles.ingestionSection}>
      <div className="container">
        <h2 className="text--center margin-bottom--md">11+ Ingestion Formats</h2>
        <p className="text--center margin-bottom--lg" style={{maxWidth: 600, margin: '0 auto 2rem'}}>
          Every format VictoriaLogs and VictoriaTraces support. No code changes &mdash;
          point your existing agents at Lakehouse.
        </p>
        <div className={styles.formatGrid}>
          {formats.map((f, i) => (
            <div key={i} className={styles.formatItem}>
              <strong>{f.name}</strong>
              <span className={styles.formatDesc}>{f.desc}</span>
              <span className={clsx(styles.formatBadge, styles[`badge${f.type}`])}>
                {f.type === 'both' ? 'logs + traces' : f.type}
              </span>
            </div>
          ))}
        </div>
        <div className="text--center margin-top--lg">
          <Link to="/ingestion-formats/" className="button button--primary button--md">
            Ingestion Format Details
          </Link>
        </div>
      </div>
    </section>
  );
}

function QueryInterfaces() {
  const interfaces = [
    {name: 'LogsQL', desc: 'Full-text search, regex, field ops, pipes, stats', target: 'Logs'},
    {name: 'Jaeger UI', desc: 'Native trace search, service graph, compare', target: 'Traces'},
    {name: 'Loki API', desc: 'Via loki-vl-proxy, Grafana Loki datasource', target: 'Logs'},
    {name: 'Grafana', desc: 'VictoriaLogs + Jaeger datasources, Explore, dashboards', target: 'Both'},
    {name: 'DuckDB', desc: 'SQL on Parquet, local or httpfs from S3', target: 'Analytics'},
    {name: 'ClickHouse', desc: 'OTEL views, s3() table function, Grafana plugin', target: 'Analytics'},
    {name: 'Spark / Trino', desc: 'Distributed SQL at petabyte scale', target: 'Analytics'},
    {name: 'VL/VT Binary Protocol', desc: '/internal/select/* with ZSTD DataBlocks', target: 'Internal'},
  ];

  return (
    <section className={styles.querySection}>
      <div className="container">
        <h2 className="text--center margin-bottom--md">8+ Query Interfaces</h2>
        <p className="text--center margin-bottom--lg" style={{maxWidth: 650, margin: '0 auto 2rem'}}>
          Observability APIs for operations. SQL analytics for business intelligence.
          Same data, every access pattern.
        </p>
        <div className="row">
          {interfaces.map((q, i) => (
            <div key={i} className="col col--3">
              <div className={styles.queryItem}>
                <h4>{q.name}</h4>
                <p>{q.desc}</p>
                <span className={styles.queryTarget}>{q.target}</span>
              </div>
            </div>
          ))}
        </div>
        <div className="text--center margin-top--lg">
          <Link to="/query-interfaces/" className="button button--primary button--md">
            Query Interface Details
          </Link>
        </div>
      </div>
    </section>
  );
}

function ComparisonHighlight() {
  return (
    <section className={styles.comparison}>
      <div className="container">
        <h2 className="text--center margin-bottom--lg">Why Victoria Lakehouse?</h2>
        <div className="row">
          <div className="col col--3">
            <div className={styles.comparisonCard}>
              <h3>vs. All-EBS</h3>
              <div className={styles.statValue}>60&ndash;96%</div>
              <div className={styles.statLabel}>cost reduction</div>
              <p>
                S3 is 3&ndash;6x cheaper per GB. Glacier tiering for 3yr+ retention
                drops to $0.004/GB/month.
              </p>
            </div>
          </div>
          <div className="col col--3">
            <div className={styles.comparisonCard}>
              <h3>vs. Loki/Tempo</h3>
              <div className={styles.statValue}>22&ndash;53%</div>
              <div className={styles.statLabel}>cheaper at scale</div>
              <p>
                VL/VT 47&ndash;70x compression beats Loki's 3.5x. Parquet columnar scans
                5&ndash;10x faster than chunk decompress.
              </p>
            </div>
          </div>
          <div className="col col--3">
            <div className={styles.comparisonCard}>
              <h3>Open Format</h3>
              <div className={styles.statValue}>9+</div>
              <div className={styles.statLabel}>analytics engines</div>
              <p>
                DuckDB, ClickHouse, Spark, Trino, Databricks, Snowflake, StarRocks,
                Doris, pandas. Your data, any tool.
              </p>
            </div>
          </div>
          <div className="col col--3">
            <div className={styles.comparisonCard}>
              <h3>Apache 2.0</h3>
              <div className={styles.statValue}>Free</div>
              <div className={styles.statLabel}>open source</div>
              <p>
                No AGPL restrictions. Fork it, embed it, sell with it.
                Full source on GitHub.
              </p>
            </div>
          </div>
        </div>
        <div className="text--center margin-top--lg">
          <Link to="/cost-optimization/" className="button button--primary button--lg margin-right--md">
            See Full Cost Analysis
          </Link>
          <Link to="/loki-tempo-alternative/" className="button button--secondary button--lg">
            Compare vs. Loki/Tempo
          </Link>
        </div>
      </div>
    </section>
  );
}

function DeploymentPatterns() {
  return (
    <section className={styles.deploymentSection}>
      <div className="container">
        <h2 className="text--center margin-bottom--lg">Deploy Your Way</h2>
        <div className="row">
          <div className="col col--4">
            <div className={styles.deployCard}>
              <h3>Hybrid Hot+Cold</h3>
              <p>
                VL/VT handles recent data on EBS (&lt;10ms). Lakehouse serves historical
                data from S3. vlselect fans out to both transparently.
              </p>
              <code>lakehouse --lakehouse.mode=logs</code>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.deployCard}>
              <h3>Standalone Cold</h3>
              <p>
                Point Grafana directly at Lakehouse. No VL/VT hot tier needed.
                Full query capability on S3 Parquet.
              </p>
              <code>docker pull ghcr.io/reliablyobserve/lakehouse-logs</code>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.deployCard}>
              <h3>Kubernetes (Helm)</h3>
              <p>
                Victoria-style Helm chart with per-component scaling,
                headless services, vmauth routing. Single chart, dual mode.
              </p>
              <code>helm install lakehouse oci://ghcr.io/reliablyobserve/charts/victoria-lakehouse</code>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function CTASection() {
  return (
    <section className={styles.ctaSection}>
      <div className="container text--center">
        <h2>Ready to Cut Your Observability Storage Costs?</h2>
        <p style={{maxWidth: 600, margin: '0 auto 2rem', fontSize: '1.1rem'}}>
          Victoria Lakehouse deploys in minutes. Same APIs your team already uses.
          Open Parquet format means zero lock-in.
        </p>
        <div className={styles.buttons}>
          <Link className="button button--primary button--lg" to="/docs/getting-started/">
            Get Started in 5 Minutes
          </Link>
          <Link className="button button--secondary button--lg"
            href="https://github.com/ReliablyObserve/victoria-lakehouse">
            Star on GitHub
          </Link>
        </div>
      </div>
    </section>
  );
}

export default function Home(): JSX.Element {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title="S3 Cold Storage for VictoriaLogs & VictoriaTraces — Open Parquet, Unlimited Retention"
      description={siteConfig.customFields?.siteDescription as string || 'S3-backed cold storage for VictoriaLogs and VictoriaTraces with open Parquet format, 11+ ingestion formats, 8+ query interfaces, and 9+ analytics engines. 60-96% cheaper than all-EBS. Apache 2.0 licensed.'}>
      <HomepageHeader />
      <main>
        <HomepageFeatures />
        <HowItWorks />
        <IngestionFormats />
        <QueryInterfaces />
        <ComparisonHighlight />
        <DeploymentPatterns />
        <CTASection />
      </main>
    </Layout>
  );
}
