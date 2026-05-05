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
        'benchmarks',
      ],
    },
    {
      type: 'category',
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
