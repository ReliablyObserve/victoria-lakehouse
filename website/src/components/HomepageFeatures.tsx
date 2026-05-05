import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';

type FeatureItem = {
  title: string;
  image?: string;
  description: JSX.Element;
};

const FeatureList: FeatureItem[] = [
  {
    title: '60-96% Cost Reduction',
    description: (
      <>
        S3 storage is 3-6x cheaper per GB than EBS. At scale (1 PB/month), hybrid deployments save $3-9.5M/year compared to all-EBS.
      </>
    ),
  },
  {
    title: 'Unlimited Retention',
    description: (
      <>
        Store years of historical data on S3 Glacier while keeping hot data on EBS. Query spanning both tiers transparently via vlselect/vtselect.
      </>
    ),
  },
  {
    title: 'Disaster Recovery',
    description: (
      <>
        When the hot cluster is down, lakehouse serves all data from S3 — slower but always available. Zero data loss.
      </>
    ),
  },
  {
    title: 'Sub-Millisecond Hot Path',
    description: (
      <>
        Queries within the hot tier's range get an immediate empty response via the partition manifest. Zero S3 I/O for recent queries.
      </>
    ),
  },
  {
    title: 'Open Parquet Format',
    description: (
      <>
        DuckDB, Trino, Spark, and ClickHouse read the same files directly for analytics, compliance, and ML — no proprietary formats.
      </>
    ),
  },
  {
    title: 'Drop-In Storage Node',
    description: (
      <>
        Register as a -storageNode on vlselect/vtselect. Existing VL/VT clusters handle hot data on EBS, lakehouse handles cold on S3.
      </>
    ),
  },
];

function Feature({title, description}: FeatureItem) {
  return (
    <div className={clsx('col col--6')}>
      <div className="text--center padding-horiz--md padding-vert--md">
        <div className={styles.featureBox}>
          <h3>{title}</h3>
          <p>{description}</p>
        </div>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): JSX.Element {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
