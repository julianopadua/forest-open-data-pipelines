import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { chromium } from "playwright";
import { PRESET_REGISTRY, THEME_REGISTRY } from "../src/shared/preset-registry.js";

const appRoot = dirname(fileURLToPath(new URL("../package.json", import.meta.url)));
const testPort = process.env.SOCIAL_TEST_PORT ?? "4175";
const baseUrl = process.env.BASE_URL ?? `http://127.0.0.1:${testPort}`;
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
  return spawn(process.execPath, [join(appRoot, "node_modules", "vite", "bin", "vite.js"), "--host", "127.0.0.1", "--port", testPort, "--strictPort"], {
    cwd: appRoot,
    stdio: "ignore",
  });
}

async function assertBlankAndStyles(browser) {
  const page = await browser.newPage();
  const styles = encodeURIComponent(JSON.stringify({ title: { fontSize: 101 } }));
  await page.goto(`${baseUrl}/green/slides/cover.html?title=Visible&topic_tag=Forest&blank=1&style=${styles}`, { waitUntil: "domcontentloaded" });
  const title = page.locator('[data-slot="title"]');
  const topic = page.locator('[data-slot="topic_tag"]');
  await title.waitFor({ state: "attached" });
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
    await page.locator("#preview").waitFor();
    assert((await runId.inputValue()).length > 0, `${preset} must load into its composer`);
  }
  await page.close();
}

async function assertZipExport(browser) {
  const page = await browser.newPage();
  await page.goto(`${baseUrl}/green/composer.html`);
  await page.locator("#preview").waitFor();
  assert(await page.locator("#btnZipImages").count() === 1, "composer must expose ZIP export");
  assert(await page.locator("#btnZipBlank").count() === 1, "composer must expose blank ZIP export");
  const blankUrls = await page.evaluate(() => window.__composerZip.getSlideUrlsBlank());
  assert(blankUrls.length > 0 && blankUrls.every((url) => url.includes("blank=1")), "blank export must configure blank slides");
  await page.close();
}

async function main() {
  console.log("Starting social post templates E2E checks.");
  const server = ownsServer ? startServer() : null;
  try {
    await waitForServer(baseUrl);
    const browser = await chromium.launch();
    try {
      await assertHubsAndComposers(browser);
      console.log("Validated hubs, composers, and presets.");
      await assertBlankAndStyles(browser);
      console.log("Validated blank mode and slot styles.");
      await assertZipExport(browser);
      console.log("Validated ZIP export configuration.");
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
