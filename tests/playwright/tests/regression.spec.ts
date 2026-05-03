import { test, expect } from '@playwright/test';

test.describe('Cross-cutting Regression', () => {
  test('no JavaScript errors during navigation', async ({ page }) => {
    const jsErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        jsErrors.push(msg.text());
      }
    });

    // Navigate through key pages
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    await page.goto('/explore');
    await page.waitForLoadState('networkidle');

    await page.goto('/datasources');
    await page.waitForLoadState('networkidle');

    // Filter out known benign Grafana console errors (e.g., ResizeObserver, favicon)
    const criticalErrors = jsErrors.filter(
      (err) =>
        !err.includes('ResizeObserver') &&
        !err.includes('favicon') &&
        !err.includes('net::ERR') &&
        !err.includes('Script error')
    );

    expect(criticalErrors).toEqual([]);
  });

  test('no "No data" for logs datasource', async ({ page }) => {
    await page.goto('/explore');
    await page.waitForLoadState('networkidle');

    // Select VL datasource
    const dsPickerButton = page.locator(
      '[data-testid="data-testid datasource picker select container"]'
    );
    if (await dsPickerButton.isVisible({ timeout: 5_000 }).catch(() => false)) {
      const currentDs = await dsPickerButton.textContent();
      if (!currentDs?.includes('Victoria Lakehouse Logs')) {
        await dsPickerButton.click();
        await page
          .locator('[data-testid="data-testid datasource picker list"]')
          .getByText('Victoria Lakehouse Logs')
          .click();
      }
    }

    // Enter query and run
    const queryInput = page.locator(
      'textarea, [data-testid="query-editor-row"] input, .view-lines, [role="textbox"]'
    ).first();
    await queryInput.waitFor({ state: 'visible', timeout: 10_000 });
    await queryInput.click();
    await queryInput.fill('*');

    const runButton = page.locator(
      '[data-testid="data-testid RefreshPicker run button"], button:has-text("Run query")'
    ).first();
    await runButton.click();

    // Wait for data to load
    await page.waitForTimeout(10_000);

    // Verify "No data" message does NOT appear
    const noDataMessage = page.locator(
      ':text("No data"), [data-testid*="no-data"], [class*="noData"]'
    );
    await expect(noDataMessage).toHaveCount(0);
  });

  test('no "No data" for traces datasource', async ({ page }) => {
    await page.goto('/explore');
    await page.waitForLoadState('networkidle');

    // Select Jaeger datasource
    const dsPickerButton = page.locator(
      '[data-testid="data-testid datasource picker select container"]'
    );
    if (await dsPickerButton.isVisible({ timeout: 5_000 }).catch(() => false)) {
      const currentDs = await dsPickerButton.textContent();
      if (!currentDs?.includes('Victoria Lakehouse Traces')) {
        await dsPickerButton.click();
        await page
          .locator('[data-testid="data-testid datasource picker list"]')
          .getByText('Victoria Lakehouse Traces')
          .click();
      }
    }

    // Select a service and search
    const serviceSelect = page.locator(
      '[aria-label*="Service" i], [data-testid*="service" i], input[placeholder*="Service" i], [id*="service" i]'
    ).first();

    if (await serviceSelect.isVisible({ timeout: 10_000 }).catch(() => false)) {
      await serviceSelect.click();
      await serviceSelect.fill('api-gateway');
      const option = page.locator(
        '[role="option"]:has-text("api-gateway"), li:has-text("api-gateway")'
      ).first();
      if (await option.isVisible({ timeout: 5_000 }).catch(() => false)) {
        await option.click();
      } else {
        await page.keyboard.press('Enter');
      }
    }

    const runButton = page.locator(
      '[data-testid="data-testid RefreshPicker run button"], button:has-text("Run query"), button:has-text("Find traces")'
    ).first();
    await runButton.click();

    // Wait for data to load
    await page.waitForTimeout(10_000);

    // Verify "No data" message does NOT appear
    const noDataMessage = page.locator(
      ':text("No data"), [data-testid*="no-data"], [class*="noData"]'
    );
    await expect(noDataMessage).toHaveCount(0);
  });

  test('API health endpoints respond', async ({ request }) => {
    // Check logs lakehouse health via Grafana proxy
    const logsHealth = await request.get(
      '/api/datasources/proxy/uid/victoria-lakehouse-logs/health'
    );
    expect(logsHealth.status()).toBe(200);

    // Check traces lakehouse health via Grafana proxy
    const tracesHealth = await request.get(
      '/api/datasources/proxy/uid/victoria-lakehouse-traces/health'
    );
    expect(tracesHealth.status()).toBe(200);

    // Check Grafana's own health
    const grafanaHealth = await request.get('/api/health');
    expect(grafanaHealth.ok()).toBeTruthy();
  });
});
