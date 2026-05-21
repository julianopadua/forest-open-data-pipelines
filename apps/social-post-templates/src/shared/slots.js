import { applyChromeFromQuery } from "../chrome/sizes.js";

/**
 * Apply slot values from URL query (?key=value) for preview / quick tests.
 * Values are decoded with decodeURIComponent.
 */
export function applySlotsFromQuery() {
  applyChromeFromQuery();
  applyLayoutParams();
  const params = new URLSearchParams(window.location.search);
  const slots = {};
  for (const [k, v] of params.entries()) {
    if (k === "side" || k === "cols" || k === "fit" || k === "blank") continue;
    if (k.startsWith("chrome_")) continue;
    slots[k] = v;
  }
  applySlots(slots);
  if (params.get("blank") === "1") applyBlankMode();
}

const CHROME_SLOTS = new Set(["topic_tag", "published_at", "card_number"]);
const STRUCTURAL_FRAMES = [
  ".body-image-frame",
  ".body-chart-frame",
  ".body-text-image-frame",
  ".lh-logo",
];

function applyBlankMode() {
  document.querySelectorAll("[data-slot]").forEach((el) => {
    const slot = el.getAttribute("data-slot");
    if (CHROME_SLOTS.has(slot)) return;
    if (el instanceof HTMLImageElement) {
      el.style.display = "none";
    } else {
      el.textContent = "";
    }
  });
  STRUCTURAL_FRAMES.forEach((sel) => {
    document.querySelectorAll(sel).forEach((el) => {
      el.style.display = "none";
    });
  });
}

/**
 * Apply slot object (used by Playwright export via page.evaluate).
 * @param {Record<string, string>} slots
 */
export function applySlots(slots) {
  if (!slots || typeof slots !== "object") return;
  for (const [key, value] of Object.entries(slots)) {
    if (value == null) continue;
    const str = String(value);
    document.querySelectorAll(`[data-slot="${key}"]`).forEach((el) => {
      if (el instanceof HTMLImageElement) {
        el.src = str;
        el.removeAttribute("data-placeholder");
      } else {
        el.textContent = str;
      }
    });
  }
}

function applyLayoutParams() {
  const params = new URLSearchParams(window.location.search);
  const side = params.get("side");
  if (side === "left" || side === "right") {
    const root = document.querySelector("[data-body-image-text]");
    if (root) {
      root.dataset.side = side;
    }
    const logoRoot = document.querySelector("[data-body-logo-half]");
    if (logoRoot) {
      logoRoot.dataset.side = side;
    }
  }
  const cols = params.get("cols");
  if (cols === "1" || cols === "2") {
    const root = document.querySelector("[data-body-text]");
    if (root) {
      root.dataset.cols = cols;
    }
  }
  const fit = params.get("fit");
  if (fit) {
    const root = document.querySelector("[data-body-image-text]");
    if (root) {
      root.dataset.fit = fit;
    }
  }
}

window.applySlotsFromQuery = applySlotsFromQuery;
window.applySlots = applySlots;
