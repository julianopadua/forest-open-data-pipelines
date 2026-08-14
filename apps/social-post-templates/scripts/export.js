import { chromium } from "playwright";
import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { getThemeConfig } from "../src/shared/preset-registry.js";
import { validateManifest } from "../src/shared/manifest-validation.js";

const appRoot = dirname(fileURLToPath(new URL("../package.json", import.meta.url)));
const baseUrl = process.env.BASE_URL ?? "http://localhost:5173";
const cardWidth = 1080;
const cardHeight = 1350;

function pad2(value) {
  return String(value).padStart(2, "0");
}

function manifestPathFromArgs() {
  return process.argv[2] ? resolve(process.argv[2]) : process.env.MANIFEST ? resolve(process.env.MANIFEST) : null;
}

function loadManifest(manifestPath) {
  if (!manifestPath || !existsSync(manifestPath)) {
    throw new Error(`Manifest not found: ${manifestPath ?? "provide a manifest path"}`);
  }
  let manifest;
  try {
    manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  } catch (error) {
    throw new Error(`Manifest is not valid JSON: ${error.message}`);
  }
  const errors = validateManifest(manifest);
  if (errors.length) throw new Error(`Invalid manifest:\n${errors.map((error) => `- ${error}`).join("\n")}`);
  return manifest;
}

async function preparePage(browser, path) {
  const page = await browser.newPage({ viewport: { width: cardWidth, height: cardHeight } });
  await page.goto(`${baseUrl}${path}`, { waitUntil: "networkidle" });
  await page.waitForFunction(
    () => typeof window.applySlots === "function" && typeof window.applyChromeSizes === "function",
  );
  await page.evaluate(() => {
    document.documentElement.style.setProperty("--canvas-scale", "1");
  });
  return page;
}

async function exportManifest(browser, manifest) {
  const themeConfig = getThemeConfig(manifest.theme);
  const outputDirectory = join(appRoot, "dist-exports", manifest.theme, manifest.runId);
  mkdirSync(outputDirectory, { recursive: true });

  for (const [index, slide] of manifest.slides.entries()) {
    const page = await preparePage(browser, themeConfig.slidePaths[slide.type]);
    const slots = {
      ...manifest.globalSlots,
      ...slide.slots,
      card_number: `${pad2(index + 1)} / ${pad2(manifest.slides.length)}`,
    };
    await page.evaluate(({ currentSlide, currentSlots, sizes }) => {
      window.applyChromeSizes(sizes ?? {});
      const imageText = document.querySelector("[data-body-image-text]");
      if (imageText && currentSlide.imageSide) imageText.dataset.side = currentSlide.imageSide;
      if (imageText && currentSlide.imageFit) imageText.dataset.fit = currentSlide.imageFit;
      const bodyText = document.querySelector("[data-body-text]");
      if (bodyText && currentSlide.columns) bodyText.dataset.cols = String(currentSlide.columns);
      const logoHalf = document.querySelector("[data-body-logo-half]");
      if (logoHalf && currentSlide.logoSide) logoHalf.dataset.side = currentSlide.logoSide;
      window.applySlots(currentSlots);
      for (const slot of currentSlide.hiddenSlots ?? []) {
        document.querySelectorAll(`[data-slot="${slot}"]`).forEach((element) => {
          element.style.display = "none";
        });
      }
      for (const [slot, styles] of Object.entries(currentSlide.slotStyles ?? {})) {
        document.querySelectorAll(`[data-slot="${slot}"]`).forEach((element) => {
          Object.assign(element.style, styles);
        });
      }
    }, { currentSlide: slide, currentSlots: slots, sizes: manifest.sizes });
    const fileName = `${pad2(index + 1)}-${slide.type}.png`;
    await page.locator("#card").screenshot({ path: join(outputDirectory, fileName) });
    await page.close();
    console.log(`Exported: dist-exports/${manifest.theme}/${manifest.runId}/${fileName}`);
  }
}

async function main() {
  const manifestPath = manifestPathFromArgs();
  const manifest = loadManifest(manifestPath);
  const browser = await chromium.launch();
  try {
    await exportManifest(browser, manifest);
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
