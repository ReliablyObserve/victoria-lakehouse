import type {SidebarsConfig} from '@docusaurus/types';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'getting-started',
    {
      label: 'Core Concepts',
      items: [
        'architecture',
        'write-path',
        'read-path',
        'deletion-strategy',
      ],
    },
    {
      label: 'Deployment',
      items: [
        'deployment-architecture',
        'kubernetes-deployment',
        'docker-compose-setup',
      ],
    },
    {
      label: 'Operations',
      items: [
        'configuration',
        'operations',
        'observability',
        'security',
        'scaling',
      ],
    },
    {
      label: 'Performance & Cost',
      items: [
        'performance',
        'cost-estimates',
        'cost-comparison',
        'benchmarks',
      ],
    },
    {
      label: 'Analytics',
      items: [
        'use-cases',
        'analytics',
        'open-parquet-format',
      ],
    },
  ],
};

export default sidebars;
