/**
 * Export PNG screenshots of post templates.
 *
 * Prerequisites:
 *   npm install
 *   npx playwright install chromium
 *
 * Usage:
 *   # Legacy: one PNG per theme (green/navy/white index pages)
 *   npm run export
 *
 *   # Manifest: sequence of green slides (dev server must be running)
 *   npm run export:manifest -- examples/green-manifest.example.json
 *   MANIFEST=examples/foo.json npm run export:manifest
 *
 * Env:
 *   BASE_URL   default http://localhost:5173
 *   MANIFEST   path to manifest JSON (same as first CLI arg)
 */

import { chromium } from "playwright";
import { mkdirSync, readFileSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join, dirname, resolve } from "node:path";

const __dir = dirname(fileURLToPath(import.meta.url));
const appRoot = join(__dir, "..");
const BASE_URL = process.env.BASE_URL ?? "http://localhost:5173";
const CARD_W = 1080;
const CARD_H = 1350;

const LEGACY_TEMPLATES = [
  { name: "green", path: "/green/slides/cover.html" },
  { name: "navy", path: "/navy/index.html" },
  { name: "white", path: "/white/index.html" },
];

const GREEN_SLIDE_PATHS = {
  cover: "/green/slides/cover.html",
  body_image_text: "/green/slides/body-image-text.html",
  body_chart: "/green/slides/body-chart.html",
  body_text: "/green/slides/body-text.html",
  cta: "/green/slides/cta.html",
};

function pad2(n) {
  return String(n).padStart(2, "0");
}

function manifestPathFromArgs() {
  const arg = process.argv[2];
  if (arg) return resolve(arg);
  if (process.env.MANIFEST) return resolve(process.env.MANIFEST);
  return null;
}

async function screenshotCard(page, outPath) {
  await page.waitForLoadState("networkidle");
  await page.evaluate(() => {
    document.documentElement.style.setProperty("--canvas-scale", "1");
  });
  const card = page.locator("#card");
  await card.screenshot({ path: outPath });
}

async function exportLegacy(browser) {
  for (const { name, path: p } of LEGACY_TEMPLATES) {
    const outDir = join(appRoot, "dist-exports", name);
    mkdirSync(outDir, { recursive: true });
    const page = await browser.newPage();
    await page.setViewportSize({ width: CARD_W, height: CARD_H });
    await page.goto(`${BASE_URL}${p}`);
    await screenshotCard(page, join(outDir, "card.png"));
    console.log(`Exported: dist-exports/${name}/card.png`);
    await page.close();
  }
}

async function exportGreenManifest(browser, manifestFile) {
  const raw = readFileSync(manifestFile, "utf8");
  const manifest = JSON.parse(raw);
  if (!manifest.slides || !Array.isArray(manifest.slides)) {
    throw new Error("Manifest must include a slides array");
  }
  const runId = manifest.runId ?? `run-${Date.now()}`;
  const outDir = join(appRoot, "dist-exports", "green", runId);
  mkdirSync(outDir, { recursive: true });

  const total = manifest.slides.length;
  let index = 0;

  for (const slide of manifest.slides) {
    const pathKey = GREEN_SLIDE_PATHS[slide.type];
    if (!pathKey) {
      throw new Error(`Unknown slide type: ${slide.type}`);
    }
    const page = await browser.newPage();
    await page.setViewportSize({ width: CARD_W, height: CARD_H });
    await page.goto(`${BASE_URL}${pathKey}`);

    const cardNum = `${pad2(index + 1)} / ${pad2(total)}`;
    const slots = { ...(slide.slots || {}), card_number: cardNum };

    await page.waitForLoadState("networkidle");
    await page.waitForFunction(
      () => typeof window.applySlots === "function" && typeof window.applyChromeSizes === "function"
    );
    await page.evaluate(() => {
      document.documentElement.style.setProperty("--canvas-scale", "1");
    });
    const chromeSizes = manifest.sizes || {};
    await page.evaluate(({ slide: s, slots: sl, sizes }) => {
      window.applyChromeSizes(sizes || {});
      if (s.type === "body_image_text") {
        const el = document.querySelector("[data-body-image-text]");
        if (el) el.dataset.side = s.imageSide || "left";
      }
      if (s.type === "body_text") {
        const el = document.querySelector("[data-body-text]");
        if (el) el.dataset.cols = String(s.columns ?? 2);
      }
      window.applySlots(sl);
    }, { slide, slots, sizes: chromeSizes });

    const fileName = `${pad2(index + 1)}-${slide.type}.png`;
    const outPath = join(outDir, fileName);
    const card = page.locator("#card");
    await card.screenshot({ path: outPath });
    console.log(`Exported: dist-exports/green/${runId}/${fileName}`);

    await page.close();
    index += 1;
  }
}

async function exportAll() {
  const mp = manifestPathFromArgs();
  const browser = await chromium.launch();

  try {
    if (mp) {
      if (!existsSync(mp)) {
        throw new Error(`Manifest not found: ${mp}`);
      }
      console.log(`Using manifest: ${mp}`);
      await exportGreenManifest(browser, mp);
    } else {
      await exportLegacy(browser);
    }
  } finally {
    await browser.close();
  }
  console.log("Done.");
}

exportAll().catch((err) => {
  console.error(err);
  process.exit(1);
});
