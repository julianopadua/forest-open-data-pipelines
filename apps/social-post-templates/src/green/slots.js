/**
 * Apply slot values from URL query (?key=value) for preview / quick tests.
 * Values are decoded with decodeURIComponent.
 */
export function applySlotsFromQuery() {
  applyLayoutParams();
  const params = new URLSearchParams(window.location.search);
  const slots = {};
  for (const [k, v] of params.entries()) {
    if (k === "side" || k === "cols") continue;
    slots[k] = v;
  }
  applySlots(slots);
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
  }
  const cols = params.get("cols");
  if (cols === "1" || cols === "2") {
    const root = document.querySelector("[data-body-text]");
    if (root) {
      root.dataset.cols = cols;
    }
  }
}

window.applySlotsFromQuery = applySlotsFromQuery;
window.applySlots = applySlots;
