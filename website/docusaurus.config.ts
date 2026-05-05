import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const siteTitle = 'Victoria Lakehouse';
const siteDescription =
  'S3-backed cold storage for VictoriaLogs and VictoriaTraces — 60-96% cost reduction with unlimited retention, disaster recovery, and open Parquet format for analytics.';

const config: Config = {
  title: siteTitle,
  tagline: siteDescription,
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
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },
  themes: ['@docusaurus/theme-mermaid'],
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
        content:
          'Victoria Lakehouse, S3 storage, VictoriaLogs cold storage, VictoriaTraces cold storage, Parquet, cold tier, cost optimization, observability',
      },
    ],
    colorMode: {
      defaultMode: 'light',
      respectPrefersColorScheme: true,
      disableSwitch: false,
    },
    navbar: {
      title: 'Victoria Lakehouse',
      logo: {
        alt: 'Victoria Lakehouse logo',
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
          to: '/use-cases/disaster-recovery/',
          label: 'Use Cases',
          position: 'left',
        },
        {
          to: '/guides/getting-started/',
          label: 'Guides',
          position: 'left',
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
            {
              label: 'Disaster Recovery',
              to: '/use-cases/disaster-recovery/',
            },
            {
              label: 'Cost Optimization',
              to: '/use-cases/cost-optimization/',
            },
            {
              label: 'Unlimited Retention',
              to: '/use-cases/unlimited-retention/',
            },
            {
              label: 'Analytics & Compliance',
              to: '/use-cases/analytics-compliance/',
            },
          ],
        },
        {
          title: 'Deployment',
          items: [
            {
              label: 'Getting Started',
              to: '/guides/getting-started/',
            },
            {
              label: 'Hot + Cold Architecture',
              to: '/guides/hot-cold-architecture/',
            },
            {
              label: 'Kubernetes Deployment',
              to: '/guides/kubernetes-deployment/',
            },
          ],
        },
        {
          title: 'Docs',
          items: [
            {label: 'Architecture', to: '/docs/architecture/'},
            {label: 'Configuration', to: '/docs/configuration/'},
            {label: 'Operations', to: '/docs/operations/'},
            {label: 'Performance', to: '/docs/performance/'},
          ],
        },
        {
          title: 'Project',
          items: [
            {
              label: 'Repository',
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
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} ReliablyObserve. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.vsDark,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
