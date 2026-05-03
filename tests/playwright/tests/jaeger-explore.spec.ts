import { test, expect } from '@playwright/test';

const KNOWN_SERVICES = [
  'api-gateway',
  'user-service',
  'order-service',
  'payment-service',
  'notification-service',
];

/**
 * Helper: navigate to Explore and select the Jaeger datasource.
 */
async function selectJaegerDatasource(page: import('@playwright/test').Page) {
  await page.goto('/explore');
  await page.waitForLoadState('networkidle');

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
}

test.describe('Jaeger Explore', () => {
  test('Explore page loads with Jaeger datasource', async ({ page }) => {
    await selectJaegerDatasource(page);

    // Verify page rendered without crash
    await expect(page.locator('body')).not.toContainText('Application error');
    // Jaeger query editor should have a service selector
    const queryEditor = page.locator(
      '[data-testid="query-editor-row"], [class*="query-editor"], form'
    ).first();
    await expect(queryEditor).toBeVisible({ timeout: 10_000 });
  });

  test('service dropdown populated', async ({ page }) => {
    await selectJaegerDatasource(page);

    // Jaeger datasource in Grafana has a "Service" dropdown/select
    // Look for select/input with label or placeholder mentioning "Service"
    const serviceSelect = page.locator(
      '[aria-label*="Service" i], [data-testid*="service" i], input[placeholder*="Service" i], [id*="service" i]'
    ).first();

    // If it is a combobox/select, click to open the dropdown
    if (await serviceSelect.isVisible({ timeout: 10_000 }).catch(() => false)) {
      await serviceSelect.click();
    } else {
      // Fallback: look for any select-like component in the query editor
      const selectInput = page.locator(
        '[data-testid="query-editor-row"] input, [data-testid="query-editor-row"] [role="combobox"]'
      ).first();
      await selectInput.click();
    }

    // Wait for dropdown options to appear
    await page.waitForTimeout(3_000);

    // Verify at least one known service appears in the dropdown or page
    const pageContent = await page.content();
    const hasService = KNOWN_SERVICES.some((svc) => pageContent.includes(svc));
    expect(hasService).toBeTruthy();
  });

  test('search returns traces', async ({ page }) => {
    await selectJaegerDatasource(page);

    // Select a service from the dropdown
    const serviceSelect = page.locator(
      '[aria-label*="Service" i], [data-testid*="service" i], input[placeholder*="Service" i], [id*="service" i]'
    ).first();

    if (await serviceSelect.isVisible({ timeout: 10_000 }).catch(() => false)) {
      await serviceSelect.click();
      // Type a known service to filter
      await serviceSelect.fill('api-gateway');
      // Select from dropdown
      const option = page.locator(
        '[role="option"]:has-text("api-gateway"), [data-testid*="option"]:has-text("api-gateway"), li:has-text("api-gateway")'
      ).first();
      if (await option.isVisible({ timeout: 5_000 }).catch(() => false)) {
        await option.click();
      } else {
        await page.keyboard.press('Enter');
      }
    }

    // Click "Run query" or "Find traces"
    const runButton = page.locator(
      '[data-testid="data-testid RefreshPicker run button"], button:has-text("Run query"), button:has-text("Find traces")'
    ).first();
    await runButton.click();

    // Wait for trace results table
    const traceResults = page.locator(
      'table tbody tr, [data-testid*="trace"], [class*="trace-table"], [class*="TraceSearchResult"]'
    ).first();
    await expect(traceResults).toBeVisible({ timeout: 30_000 });
  });

  test('trace detail renders', async ({ page }) => {
    await selectJaegerDatasource(page);

    // Select service and run query
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

    // Wait for trace rows and click first one
    const firstTrace = page.locator(
      'table tbody tr, [data-testid*="trace"], [class*="TraceSearchResult"]'
    ).first();
    await expect(firstTrace).toBeVisible({ timeout: 30_000 });
    await firstTrace.click();

    // Verify trace timeline/flamegraph renders
    const traceDetail = page.locator(
      '[data-testid*="TraceTimelineViewer"], [class*="TraceTimelineViewer"], [class*="trace-detail"], [data-testid*="trace-view"], [class*="SpanBar"], svg'
    ).first();
    await expect(traceDetail).toBeVisible({ timeout: 15_000 });
  });

  test('span details visible', async ({ page }) => {
    await selectJaegerDatasource(page);

    // Select service and run query
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

    // Wait for and click first trace
    const firstTrace = page.locator(
      'table tbody tr, [data-testid*="trace"], [class*="TraceSearchResult"]'
    ).first();
    await expect(firstTrace).toBeVisible({ timeout: 30_000 });
    await firstTrace.click();

    // Wait for trace detail view, then click a span
    await page.waitForTimeout(3_000);
    const span = page.locator(
      '[data-testid*="SpanTreeOffset"], [class*="SpanBar"], [class*="span-name"], [data-testid*="span-view"]'
    ).first();
    if (await span.isVisible({ timeout: 10_000 }).catch(() => false)) {
      await span.click();
    }

    // Verify span tags/details section appears
    const spanDetail = page.locator(
      '[data-testid*="SpanDetail"], [class*="SpanDetail"], [class*="AccordionHeader"]:has-text("Tags"), [class*="KeyValueTable"], table:has(td)'
    ).first();
    await expect(spanDetail).toBeVisible({ timeout: 15_000 });
  });

  test('process info visible', async ({ page }) => {
    await selectJaegerDatasource(page);

    // Select service and run query
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

    // Wait for and click first trace
    const firstTrace = page.locator(
      'table tbody tr, [data-testid*="trace"], [class*="TraceSearchResult"]'
    ).first();
    await expect(firstTrace).toBeVisible({ timeout: 30_000 });
    await firstTrace.click();

    // In trace detail view, verify process/service info is shown
    // Grafana trace view shows service names in the span tree
    await page.waitForTimeout(3_000);
    const pageContent = await page.content();
    const hasServiceInfo = KNOWN_SERVICES.some((svc) => pageContent.includes(svc));
    expect(hasServiceInfo).toBeTruthy();
  });
});
