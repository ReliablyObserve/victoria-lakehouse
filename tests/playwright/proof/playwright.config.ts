import { defineConfig } from "@playwright/test";
import * as fs from "fs";

// Grafana: GRAFANA_URL, else the port the stack recorded in its state file (VP_STATE).
const statePorts = process.env.VP_STATE && fs.existsSync(process.env.VP_STATE) ? JSON.parse(fs.readFileSync(process.env.VP_STATE, "utf8")).ports : null;
const grafana = process.env.GRAFANA_URL || (statePorts ? `http://127.0.0.1:${statePorts.grafana}` : "http://127.0.0.1:48300");

// Visual-proof capture (ported from loki-vl-proxy/bench/visual/playwright.config.ts@429f15b9). Pages x ranges run in parallel; the three
// sides of one page and range (base, PR, reference) run in sequence inside a test, so they see the same state.
export default defineConfig({
  testDir: ".",
  testMatch: "capture.spec.ts",
  timeout: 600_000,
  workers: process.env.WORKERS ? parseInt(process.env.WORKERS) : 2,
  fullyParallel: true,
  reporter: [["list"]],
  use: {
    baseURL: grafana,
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
