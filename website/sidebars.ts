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
        'storage-flow',
        'manifest-system',
        'cache-architecture',
        'deletion-strategy',
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
        'observability',
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
        'cost-estimates',
        'cost-comparison',
        'vl-comparison',
        'benchmarks',
        'zstd-compression-benchmark',
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
