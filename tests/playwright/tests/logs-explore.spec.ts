import { test, expect } from '@playwright/test';

const KNOWN_SERVICES = [
  'api-gateway',
  'user-service',
  'order-service',
  'payment-service',
  'notification-service',
];

test.describe('Logs Explore', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to Explore with VL datasource pre-selected
    await page.goto('/explore');
    await page.waitForLoadState('networkidle');
  });

  test('Explore page loads with VL datasource', async ({ page }) => {
    // Select VL datasource if not already selected
    const dsPickerButton = page.locator(
      '[data-testid="data-testid datasource picker select container"]'
    );

    // If the datasource picker is visible, ensure VL is selected
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

    // Verify Explore page rendered without crash
    await expect(page.locator('body')).not.toContainText('Application error');
  });

  test('query returns results', async ({ page }) => {
    // Select datasource
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

    // Find query input — VL plugin uses a text input / textarea / CodeMirror
    const queryInput = page.locator(
      'textarea, [data-testid="query-editor-row"] input, .view-lines, [role="textbox"]'
    ).first();
    await queryInput.waitFor({ state: 'visible', timeout: 10_000 });
    await queryInput.click();
    await queryInput.fill('*');

    // Click Run query
    const runButton = page.locator(
      '[data-testid="data-testid RefreshPicker run button"], button:has-text("Run query")'
    ).first();
    await runButton.click();

    // Wait for log rows to appear
    const logRows = page.locator(
      '[data-testid="logRows"], [data-testid^="logRow"], .log-row, tr[class*="logRow"], [class*="logs-row"]'
    ).first();
    await expect(logRows).toBeVisible({ timeout: 30_000 });
  });

  test('log volume renders', async ({ page }) => {
    // Select datasource and run a query
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

    // Wait for results, then check for volume chart (histogram/bar chart rendered by VL plugin)
    // The VL plugin renders a log volume histogram above the log lines
    const volumeChart = page.locator(
      '[data-testid="logVolume"], canvas, [class*="logs-volume"], svg[class*="chart"], [class*="Graph"]'
    ).first();
    await expect(volumeChart).toBeVisible({ timeout: 30_000 });
  });

  test('log lines have expected fields', async ({ page }) => {
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

    // Wait for log rows
    const logRows = page.locator(
      '[data-testid="logRows"], [data-testid^="logRow"], .log-row, tr[class*="logRow"], [class*="logs-row"]'
    ).first();
    await expect(logRows).toBeVisible({ timeout: 30_000 });

    // Verify that at least one known service name appears in the log output
    const logContent = await page.locator('.scrollbar-view, [class*="logs-rows"], [data-testid="logRows"]').first().textContent();
    const hasService = KNOWN_SERVICES.some((svc) => logContent?.includes(svc));
    expect(hasService).toBeTruthy();
  });

  test('no error banners', async ({ page }) => {
    // Run a query and verify no Grafana error/alert banners appear
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

    // Wait a moment for any potential errors
    await page.waitForTimeout(5_000);

    // Check that no Grafana error banners/alerts are visible
    const errorBanners = page.locator(
      '[data-testid="data-testid Alert error"], .alert-error, [class*="alert-error"], [aria-label="Alert error"]'
    );
    await expect(errorBanners).toHaveCount(0);
  });
});
