/**
 * Chrome / “meta” typography: topic (canto esq.), data (canto dir.), número da página, altura do logo.
 * Valores em px; aplicados em :root para refletir no preview e nos PNGs (export chama applyChromeSizes).
 */

export const DEFAULT_CHROME = {
  topicTagPx: 24,
  datePx: 26,
  pageNumberPx: 24,
  logoHeightPx: 54,
};

function n(v, fallback) {
  if (v == null || v === "") return fallback;
  const x = Number(v);
  return Number.isFinite(x) ? x : fallback;
}

/**
 * @param {Partial<typeof DEFAULT_CHROME>} sizes
 */
export function applyChromeSizes(sizes = {}) {
  const t = n(sizes.topicTagPx, DEFAULT_CHROME.topicTagPx);
  const d = n(sizes.datePx, DEFAULT_CHROME.datePx);
  const p = n(sizes.pageNumberPx, DEFAULT_CHROME.pageNumberPx);
  const h = n(sizes.logoHeightPx, DEFAULT_CHROME.logoHeightPx);
  const r = document.documentElement;
  r.style.setProperty("--chrome-topic-px", `${t}px`);
  r.style.setProperty("--chrome-date-px", `${d}px`);
  r.style.setProperty("--chrome-page-px", `${p}px`);
  r.style.setProperty("--chrome-logo-h", `${h}px`);
}

/** Query: chrome_topic, chrome_date, chrome_page, chrome_logo (números em px) */
export function applyChromeFromQuery() {
  const params = new URLSearchParams(window.location.search);
  applyChromeSizes({
    topicTagPx: params.get("chrome_topic"),
    datePx: params.get("chrome_date"),
    pageNumberPx: params.get("chrome_page"),
    logoHeightPx: params.get("chrome_logo"),
  });
}

window.applyChromeSizes = applyChromeSizes;
window.applyChromeFromQuery = applyChromeFromQuery;
