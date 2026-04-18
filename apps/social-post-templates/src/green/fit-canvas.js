export function initFitCanvas() {
  function fit() {
    const vw = window.innerWidth;
    const scale = Math.min((vw - 64) / 1080, 0.85);
    document.documentElement.style.setProperty("--canvas-scale", scale);
  }
  fit();
  window.addEventListener("resize", fit);
}
