# Victoria Lakehouse Website

This directory contains the Docusaurus-based website for Victoria Lakehouse.

## Development

```bash
cd website
npm install
npm run start
```

The website will be available at `http://localhost:3000`.

## Building

```bash
npm run build
```

The static site will be generated in the `build/` directory.

## Deployment

The website is automatically deployed to GitHub Pages on every push to `main` via the `.github/workflows/docs-site.yaml` workflow.

## Structure

- `src/pages/` - Main pages and custom landing pages
- `src/components/` - React components
- `src/css/` - Global styles
- `static/` - Static assets (images, fonts, etc.)
- `docusaurus.config.ts` - Docusaurus configuration
- `sidebars.ts` - Documentation sidebar configuration
- `../docs/` - Documentation markdown files (linked from parent directory)

## Adding Documentation

1. Create markdown files in the `../docs/` directory
2. Update `sidebars.ts` to include the new files
3. The documentation will be automatically available at `/docs/`

## Adding Use Case Pages

Create a new TypeScript/TSX file in `src/pages/` with the structure:

```tsx
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function UseCasePage() {
  return (
    <Layout
      title="Use Case Title"
      description="Description">
      <main>
        {/* Page content */}
      </main>
    </Layout>
  );
}
```

## Styling

The website uses Docusaurus's default theming with custom CSS in `src/css/custom.css`. Colors are defined as CSS variables and support dark mode.
