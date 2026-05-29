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
    if (k === "side" || k === "cols" || k === "fit" || k === "blank" || k === "hide" || k === "style") continue;
    if (k.startsWith("chrome_")) continue;
    slots[k] = v;
  }
  applySlots(slots);
  const styleParam = params.get("style");
  if (styleParam) applySlotStyles(styleParam);
  const hide = params.get("hide");
  if (hide) applyHiddenSlots(hide);
  if (params.get("blank") === "1") applyBlankMode();
}

function applyHiddenSlots(hideParam) {
  hideParam
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .forEach((key) => {
      document.querySelectorAll(`[data-slot="${key}"]`).forEach((el) => {
        el.style.display = "none";
      });
    });
}

const PX_PROPS = new Set([
  "fontSize",
  "marginTop",
  "marginRight",
  "marginBottom",
  "marginLeft",
  "letterSpacing",
  "maxHeight",
  "maxWidth",
  "width",
  "height",
  "top",
  "right",
  "bottom",
  "left",
]);

function normalizeStyleValue(prop, value) {
  if (value == null) return null;
  const str = String(value).trim();
  if (!str) return null;
  //lineHeight unitless (ex.: 1, 1.25) nao recebe sufixo px
  if (prop === "lineHeight" && /^-?\d+(\.\d+)?$/.test(str)) return str;
  if (PX_PROPS.has(prop) && /^-?\d+(\.\d+)?$/.test(str)) return str + "px";
  return str;
}

function applySlotStyles(styleParam) {
  let map;
  try {
    map = JSON.parse(styleParam);
  } catch (e) {
    console.warn("Invalid style param (not JSON)", e);
    return;
  }
  if (!map || typeof map !== "object") return;
  Object.entries(map).forEach(([key, props]) => {
    if (!props || typeof props !== "object") return;
    document.querySelectorAll(`[data-slot="${key}"]`).forEach((el) => {
      Object.entries(props).forEach(([prop, raw]) => {
        const value = normalizeStyleValue(prop, raw);
        if (value == null) return;
        el.style[prop] = value;
      });
    });
  });
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
