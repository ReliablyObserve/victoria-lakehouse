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
          Unlimited retention, disaster recovery, and open Parquet format.
          Within 5% of VL/VT EBS cost &mdash; 22% cheaper than Loki/Tempo.
        </p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/getting-started/">
            Get Started
          </Link>
          <Link
            className="button button--outline button--lg"
            to="/disaster-recovery/">
            Explore Use Cases
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
              <h3>Ingest</h3>
              <p>
                Data flows into VictoriaLogs or VictoriaTraces as usual.
                Lakehouse writes Parquet files to S3 in parallel.
              </p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.stepCard}>
              <div className={styles.stepNumber}>2</div>
              <h3>Query</h3>
              <p>
                vlselect/vtselect fans out to both hot (EBS) and cold (S3).
                Manifest check skips S3 for recent data in under 1ms.
              </p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.stepCard}>
              <div className={styles.stepNumber}>3</div>
              <h3>Analyze</h3>
              <p>
                Query cold data with DuckDB, Spark, Trino, or ClickHouse.
                Standard Parquet &mdash; no vendor lock-in.
              </p>
            </div>
          </div>
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
          <div className="col col--4">
            <div className={styles.comparisonCard}>
              <h3>vs. All-EBS</h3>
              <div className={styles.statValue}>60&ndash;96%</div>
              <div className={styles.statLabel}>cost reduction</div>
              <p>
                S3 is 3&ndash;6x cheaper per GB. At 1 PB/month, save $250K+/year
                with hybrid hot+cold architecture.
              </p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.comparisonCard}>
              <h3>vs. Loki/Tempo</h3>
              <div className={styles.statValue}>22%</div>
              <div className={styles.statLabel}>cheaper at 500 GB/day</div>
              <p>
                VL/VT hot tier delivers sub-10ms queries. Cold tier uses columnar
                Parquet with bloom filters &mdash; 5&ndash;10x faster than chunk scans.
              </p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.comparisonCard}>
              <h3>Open Format</h3>
              <div className={styles.statValue}>0</div>
              <div className={styles.statLabel}>vendor lock-in</div>
              <p>
                Standard Apache Parquet. Your data is yours &mdash; query it with
                any tool, migrate anytime.
              </p>
            </div>
          </div>
        </div>
        <div className="text--center margin-top--lg">
          <Link to="/cost-optimization/" className="button button--primary button--lg">
            See Full Cost Analysis
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
      title={`${siteConfig.title} — Cold Storage for VictoriaLogs & VictoriaTraces`}
      description="S3-backed cold storage for VictoriaLogs and VictoriaTraces">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
        <HowItWorks />
        <ComparisonHighlight />
      </main>
    </Layout>
  );
}
