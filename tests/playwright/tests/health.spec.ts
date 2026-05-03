import { test, expect } from '@playwright/test';

test.describe('Grafana Health', () => {
  test('Grafana loads', async ({ page }) => {
    await page.goto('/');
    await expect(page).toHaveTitle(/Grafana/i);
  });

  test('datasources exist', async ({ request }) => {
    const response = await request.get('/api/datasources');
    expect(response.ok()).toBeTruthy();

    const datasources = await response.json();
    const uids = datasources.map((ds: { uid: string }) => ds.uid);

    expect(uids).toContain('victoria-lakehouse-logs');
    expect(uids).toContain('victoria-lakehouse-traces');
  });

  test('logs datasource healthy', async ({ request }) => {
    const response = await request.get(
      '/api/datasources/proxy/uid/victoria-lakehouse-logs/health'
    );
    expect(response.status()).toBe(200);
  });

  test('traces datasource healthy', async ({ request }) => {
    const response = await request.get(
      '/api/datasources/proxy/uid/victoria-lakehouse-traces/health'
    );
    expect(response.status()).toBe(200);
  });
});
