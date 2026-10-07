import { defineConfig } from "@playwright/test";

// Visual-proof capture (ported from loki-vl-proxy bench/visual). Pages x ranges run in parallel; the three
// sides of one page and range (base, PR, reference) run in sequence inside a test, so they see the same state.
export default defineConfig({
  testDir: ".",
  testMatch: "capture.spec.ts",
  timeout: 600_000,
  workers: process.env.WORKERS ? parseInt(process.env.WORKERS) : 2,
  fullyParallel: true,
  reporter: [["list"]],
  use: {
    baseURL: process.env.GRAFANA_URL || "http://127.0.0.1:48300",
    viewport: { width: 1500, height: 1100 },
    timezoneId: "UTC",
    locale: "en-US",
  },
  projects: [{
    name: "chromium",
    use: {
      browserName: "chromium",
      launchOptions: process.env.PLAYWRIGHT_EXECUTABLE_PATH ? { executablePath: process.env.PLAYWRIGHT_EXECUTABLE_PATH } : {},
    },
  }],
});
