import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const siteTitle = 'Victoria Lakehouse';
const siteTagline = 'S3 Cold Storage for VictoriaLogs & VictoriaTraces — Open Parquet, Unlimited Retention';
const siteDescription =
  'Victoria Lakehouse is an S3 Parquet cold storage tier for VictoriaLogs and VictoriaTraces. Drop-in VL/VT storage node with full API compatibility. Ingest via Loki, Elasticsearch, OTLP, Syslog, Fluentd, Logstash, Datadog, and more. Query with LogsQL, Jaeger, Loki API, or DuckDB/Spark/Trino/ClickHouse on open Parquet files. 60-96% cheaper than all-EBS. Unlimited retention with S3 Glacier tiering. Apache 2.0 licensed.';

const config: Config = {
  title: siteTitle,
  tagline: siteTagline,
  future: {
    v4: true,
  },
  url: 'https://reliablyobserve.github.io',
  baseUrl: '/victoria-lakehouse/',
  trailingSlash: true,
  organizationName: 'ReliablyObserve',
  projectName: 'victoria-lakehouse',
  onBrokenLinks: 'throw',
  markdown: {
    format: 'md',
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },
  themes: ['@docusaurus/theme-mermaid'],
  headTags: [
    {
      tagName: 'meta',
      attributes: {
        property: 'og:type',
        content: 'website',
      },
    },
    {
      tagName: 'meta',
      attributes: {
        property: 'og:title',
        content: 'Victoria Lakehouse — S3 Cold Storage for VictoriaLogs & VictoriaTraces',
      },
    },
    {
      tagName: 'meta',
      attributes: {
        property: 'og:description',
        content: siteDescription,
      },
    },
    {
      tagName: 'meta',
      attributes: {
        name: 'twitter:card',
        content: 'summary_large_image',
      },
    },
    {
      tagName: 'script',
      attributes: {
        type: 'application/ld+json',
      },
      innerHTML: JSON.stringify({
        '@context': 'https://schema.org',
        '@type': 'SoftwareApplication',
        name: 'Victoria Lakehouse',
        applicationCategory: 'DeveloperApplication',
        operatingSystem: 'Linux, macOS',
        description: siteDescription,
        url: 'https://reliablyobserve.github.io/victoria-lakehouse/',
        license: 'https://www.apache.org/licenses/LICENSE-2.0',
        softwareRequirements: 'S3-compatible object storage',
        offers: {
          '@type': 'Offer',
          price: '0',
          priceCurrency: 'USD',
        },
        author: {
          '@type': 'Organization',
          name: 'ReliablyObserve',
          url: 'https://github.com/ReliablyObserve',
        },
      }),
    },
  ],
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },
  presets: [
    [
      'classic',
      {
        docs: {
          path: '../docs',
          routeBasePath: 'docs',
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/ReliablyObserve/victoria-lakehouse/tree/main/',
          showLastUpdateTime: true,
          exclude: ['superpowers/**'],
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
        sitemap: {
          changefreq: 'weekly',
          priority: 0.7,
          ignorePatterns: ['/tags/**'],
          filename: 'sitemap.xml',
        },
      } satisfies Preset.Options,
    ],
  ],
  themeConfig: {
    metadata: [
      {name: 'description', content: siteDescription},
      {
        name: 'keywords',
        content: [
          'Victoria Lakehouse',
          'VictoriaLogs cold storage',
          'VictoriaTraces cold storage',
          'S3 Parquet storage',
          'observability cold tier',
          'log storage S3',
          'trace storage S3',
          'Loki alternative',
          'Tempo alternative',
          'open Parquet format',
          'DuckDB observability',
          'ClickHouse log analytics',
          'cost optimization observability',
          'disaster recovery logs',
          'unlimited log retention',
          'OTLP S3 storage',
          'Elasticsearch bulk API',
          'Syslog S3 storage',
          'LogsQL',
          'Jaeger traces S3',
          'Grafana cold storage',
          'VictoriaMetrics ecosystem',
          'Apache Parquet logs',
          'compliance log retention',
          'multi-tenant observability',
        ].join(', '),
      },
      {name: 'robots', content: 'index, follow'},
      {property: 'og:site_name', content: 'Victoria Lakehouse'},
    ],
    colorMode: {
      defaultMode: 'light',
      respectPrefersColorScheme: true,
      disableSwitch: false,
    },
    navbar: {
      title: 'Victoria Lakehouse',
      logo: {
        alt: 'Victoria Lakehouse — S3 cold storage for VictoriaLogs and VictoriaTraces',
        src: 'img/logo.svg',
        srcDark: 'img/logo.svg',
      },
      items: [
        {to: '/', label: 'Overview', position: 'left'},
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          label: 'Use Cases',
          position: 'left',
          items: [
            {to: '/disaster-recovery/', label: 'Disaster Recovery'},
            {to: '/cost-optimization/', label: 'Cost Optimization'},
            {to: '/unlimited-retention/', label: 'Unlimited Retention'},
            {to: '/analytics-compliance/', label: 'Analytics & Compliance'},
            {to: '/loki-tempo-alternative/', label: 'Loki/Tempo Alternative'},
            {to: '/multi-tenant-observability/', label: 'Multi-Tenant Observability'},
          ],
        },
        {
          to: '/docs/getting-started/',
          label: 'Quick Start',
          position: 'left',
        },
        {
          label: 'Integrations',
          position: 'left',
          items: [
            {to: '/ingestion-formats/', label: 'Ingestion Formats'},
            {to: '/query-interfaces/', label: 'Query Interfaces'},
            {to: '/docs/analytics-engines/', label: 'Analytics Engines'},
          ],
        },
        {
          href: 'https://github.com/ReliablyObserve/victoria-lakehouse',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Use Cases',
          items: [
            {label: 'Disaster Recovery', to: '/disaster-recovery/'},
            {label: 'Cost Optimization', to: '/cost-optimization/'},
            {label: 'Unlimited Retention', to: '/unlimited-retention/'},
            {label: 'Analytics & Compliance', to: '/analytics-compliance/'},
            {label: 'Loki/Tempo Alternative', to: '/loki-tempo-alternative/'},
            {label: 'Multi-Tenant Observability', to: '/multi-tenant-observability/'},
          ],
        },
        {
          title: 'Integrations',
          items: [
            {label: 'Ingestion Formats', to: '/ingestion-formats/'},
            {label: 'Query Interfaces', to: '/query-interfaces/'},
            {label: 'Analytics Engines', to: '/docs/analytics-engines/'},
            {label: 'Open Parquet Format', to: '/docs/open-parquet-format/'},
          ],
        },
        {
          title: 'Docs',
          items: [
            {label: 'Getting Started', to: '/docs/getting-started/'},
            {label: 'Architecture', to: '/docs/architecture/'},
            {label: 'Configuration', to: '/docs/configuration/'},
            {label: 'Performance', to: '/docs/performance/'},
            {label: 'Cost Estimates', to: '/docs/cost-estimates/'},
            {label: 'Multi-Tenancy', to: '/docs/multi-tenancy/'},
          ],
        },
        {
          title: 'Project',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/ReliablyObserve/victoria-lakehouse',
            },
            {
              label: 'Releases',
              href: 'https://github.com/ReliablyObserve/victoria-lakehouse/releases',
            },
            {
              label: 'Helm Chart',
              href: 'https://github.com/ReliablyObserve/victoria-lakehouse/tree/main/charts/victoria-lakehouse',
            },
            {
              label: 'Apache 2.0 License',
              href: 'https://github.com/ReliablyObserve/victoria-lakehouse/blob/main/LICENSE',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} ReliablyObserve. Apache 2.0 Licensed. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.vsDark,
      additionalLanguages: ['bash', 'yaml', 'json', 'sql', 'go'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
