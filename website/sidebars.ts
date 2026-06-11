import type {SidebarsConfig} from '@docusaurus/types';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'getting-started',
    {
      type: 'category',
      label: 'Core Concepts',
      items: [
        'architecture',
        'write-path',
        'read-path',
        'durability',
        'storage-flow',
        'manifest-system',
        'cache-architecture',
        'bloom-index',
        'deletion-strategy',
      ],
    },
    {
      type: 'category',
      label: 'Architecture Deep Dives',
      items: [
        'architecture-overview',
        'architecture/metadata-consolidation',
        'architecture/field-value-catalog',
        'architecture/metadata-and-s3-optimization',
        'architecture/restart-and-warmup-design',
        'architecture/pb-scale-resources-pmeta',
        'architecture/restore-and-elasticity-pmeta',
        'architecture/scaling-restart-scenarios',
      ],
    },
    {
      type: 'category',
      label: 'Deployment',
      items: [
        'deployment-architecture',
        'kubernetes-deployment',
        'docker-compose-setup',
      ],
    },
    {
      type: 'category',
      label: 'Operations',
      items: [
        'configuration',
        'operations',
        'operations/lifecycle',
        'operations/sizing',
        'observability',
        'telemetry',
        'multi-tenancy',
        'tenant-stats',
        'lakehouse-explorer',
        'security',
        'scaling',
      ],
    },
    {
      type: 'category',
      label: 'Performance & Cost',
      items: [
        'performance',
        'performance-machinery',
        'cost-estimates',
        'cost-comparison',
        'cross-az-optimization',
        'vl-comparison',
        'benchmarks',
        'benchmarks/full-scope-s3',
        'zstd-compression-benchmark',
        'petabyte-scale-audit',
        'parity-and-gaps',
      ],
    },
    {
      type: 'category',
      label: 'Analytics & Integrations',
      items: [
        'use-cases',
        'analytics',
        'analytics-engines',
        'open-parquet-format',
      ],
    },
  ],
};

export default sidebars;
