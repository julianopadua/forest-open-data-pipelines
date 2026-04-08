/**
 * Export PNG screenshots of all post templates.
 *
 * Prerequisites:
 *   npm install --save-dev playwright
 *   npx playwright install chromium
 *
 * Usage:
 *   npm run export          (expects dev server running on localhost:5173)
 *   BASE_URL=http://localhost:5173 node scripts/export.js
 *
 * Output: dist-exports/{green,navy,white}/card.png
 */

// TODO: uncomment when playwright is installed
// import { chromium } from "playwright";
import { mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join, dirname } from "node:path";

const __dir = dirname(fileURLToPath(import.meta.url));
const BASE_URL = process.env.BASE_URL ?? "http://localhost:5173";
const CARD_W = 1080;
const CARD_H = 1350;

const TEMPLATES = [
  { name: "green", path: "/green/index.html" },
  { name: "navy", path: "/navy/index.html" },
  { name: "white", path: "/white/index.html" },
];

async function exportAll() {
  // TODO: remove this guard when playwright is installed
  console.error(
    "⚠  Playwright is not yet installed.\n" +
    "   Run:  npm install --save-dev playwright\n" +
    "         npx playwright install chromium\n" +
    "   Then uncomment the import at the top of this file and remove this guard."
  );
  process.exit(1);

  /* --- uncomment below when playwright is ready ---
  const browser = await chromium.launch();

  for (const { name, path } of TEMPLATES) {
    const outDir = join(__dir, "..", "dist-exports", name);
    mkdirSync(outDir, { recursive: true });

    const page = await browser.newPage();
    await page.setViewportSize({ width: CARD_W, height: CARD_H });
    await page.goto(`${BASE_URL}${path}`);
    await page.waitForLoadState("networkidle");

    // Force canvas to exact dimensions (disable the resize scale)
    await page.evaluate((w) => {
      document.documentElement.style.setProperty("--canvas-scale", "1");
    }, CARD_W);

    const card = page.locator("#card");
    await card.screenshot({ path: join(outDir, "card.png") });
    console.log(`Exported: dist-exports/${name}/card.png`);

    await page.close();
  }

  await browser.close();
  console.log("Done.");
  --- end uncomment --- */
}

exportAll().catch(console.error);
