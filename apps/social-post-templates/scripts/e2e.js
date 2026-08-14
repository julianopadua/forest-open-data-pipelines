import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";
import { chromium } from "playwright";
import { PRESET_REGISTRY, THEME_REGISTRY } from "../src/shared/preset-registry.js";

const appRoot = dirname(fileURLToPath(new URL("../package.json", import.meta.url)));
const baseUrl = process.env.BASE_URL ?? "http://127.0.0.1:4174";
const ownsServer = !process.env.BASE_URL;

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

async function waitForServer(url) {
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url);
      if (response.ok) return;
    } catch {
      //aguarda o vite iniciar
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`Vite did not start at ${url}`);
}

function startServer() {
  return spawn("npm", ["run", "dev", "--", "--host", "127.0.0.1", "--port", "4174", "--strictPort"], {
    cwd: appRoot,
    stdio: "ignore",
  });
}

async function assertBlankAndStyles(browser) {
  const page = await browser.newPage();
  const styles = encodeURIComponent(JSON.stringify({ title: { fontSize: 101 } }));
  await page.goto(`${baseUrl}/green/slides/cover.html?title=Visible&topic_tag=Forest&blank=1&style=${styles}`);
  const title = page.locator('[data-slot="title"]');
  const topic = page.locator('[data-slot="topic_tag"]');
  await title.waitFor();
  assert(await title.textContent() === "", "blank=1 must hide slide text");
  assert(await topic.textContent() === "Forest", "blank=1 must preserve chrome slots");
  assert(await title.evaluate((element) => element.style.fontSize) === "101px", "style parameter must apply slot style");
  await page.close();
}

async function assertHubsAndComposers(browser) {
  const page = await browser.newPage();
  await page.goto(`${baseUrl}/`);
  for (const theme of Object.keys(THEME_REGISTRY)) {
    assert(await page.locator(`a[href="/${theme}/composer.html"]`).count() === 1, `home must link to ${theme} composer`);
  }
  for (const theme of Object.keys(THEME_REGISTRY)) {
    await page.goto(`${baseUrl}/${theme}/index.html`);
    assert(await page.locator("body").textContent(), `${theme} hub must render content`);
  }
  for (const [preset, config] of Object.entries(PRESET_REGISTRY)) {
    await page.goto(`${baseUrl}${config.composerPath ?? THEME_REGISTRY[config.theme].composerPath}?preset=${preset}`);
    const runId = page.locator("#runId");
    await runId.waitFor();
    await page.locator("#previewFrame").waitFor();
    assert((await runId.inputValue()).length > 0, `${preset} must load into its composer`);
  }
  await page.close();
}

async function assertZipExport(browser) {
  const page = await browser.newPage();
  await page.goto(`${baseUrl}/green/composer.html`);
  await page.locator("#previewFrame").waitFor();
  const downloadPromise = page.waitForEvent("download", { timeout: 60000 });
  await page.locator("#btnZipBlank").click();
  const download = await downloadPromise;
  assert((await download.suggestedFilename()).endsWith(".zip"), "blank export must create a ZIP file");
  await page.close();
}

async function main() {
  const server = ownsServer ? startServer() : null;
  try {
    await waitForServer(baseUrl);
    const browser = await chromium.launch();
    try {
      await assertHubsAndComposers(browser);
      await assertBlankAndStyles(browser);
      await assertZipExport(browser);
    } finally {
      await browser.close();
    }
  } finally {
    if (server) server.kill("SIGTERM");
  }
  console.log("Social post templates E2E checks passed.");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
