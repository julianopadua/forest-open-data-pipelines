import { toBlob } from "html-to-image";
import JSZip from "jszip";

const CARD_W = 1080;
const CARD_H = 1350;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitForIframeScripts(iframe) {
  const win = iframe.contentWindow;
  if (!win) throw new Error("iframe sem janela");
  let n = 0;
  while (n < 100) {
    if (typeof win.applySlots === "function" && typeof win.applyChromeSizes === "function") {
      return;
    }
    await sleep(30);
    n += 1;
  }
  throw new Error("Timeout: scripts do slide não carregaram (recarregue e tente de novo).");
}

async function waitForImages(doc) {
  const imgs = Array.from(doc.images);
  await Promise.all(
    imgs.map(
      (img) =>
        img.complete && img.naturalWidth
          ? Promise.resolve()
          : new Promise((resolve) => {
              img.onload = () => resolve();
              img.onerror = () => resolve();
            }),
    ),
  );
}

async function prepareCapture(win) {
  const doc = win.document;
  doc.documentElement.style.setProperty("--canvas-scale", "1");
  await doc.fonts?.ready?.catch(() => {});
  await waitForImages(doc);
  await new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)));
}

function waitForFrameNavigate(iframe, url) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error("Timeout ao carregar slide (60s).")),
      60000,
    );
    const onLoad = () => {
      clearTimeout(timeout);
      iframe.removeEventListener("load", onLoad);
      resolve();
    };
    iframe.addEventListener("load", onLoad);
    iframe.onerror = () => {
      clearTimeout(timeout);
      reject(new Error("Falha ao carregar slide."));
    };
    iframe.src = url;
  });
}

/**
 * @param {{ getSlideUrls: () => string[]; getRunId: () => string; onProgress?: (done: number, total: number) => void }} opts
 */
export async function downloadCarouselZip(opts) {
  const { getSlideUrls, getRunId, onProgress } = opts;
  const urls = getSlideUrls();
  if (!urls.length) throw new Error("Adicione pelo menos um slide.");

  const iframe = document.createElement("iframe");
  iframe.setAttribute("title", "zip-capture");
  iframe.setAttribute(
    "style",
    "position:fixed;left:-10000px;top:0;width:1120px;height:1400px;border:0;opacity:0;pointer-events:none",
  );
  document.body.appendChild(iframe);

  const zip = new JSZip();
  try {
    for (let i = 0; i < urls.length; i++) {
      onProgress?.(i + 1, urls.length);
      await waitForFrameNavigate(iframe, urls[i]);
      await waitForIframeScripts(iframe);
      await prepareCapture(iframe.contentWindow);

      const doc = iframe.contentDocument;
      const card = doc?.getElementById("card");
      if (!card) throw new Error("Slide sem #card");

      const blob = await toBlob(card, {
        pixelRatio: 1,
        width: CARD_W,
        height: CARD_H,
        cacheBust: true,
      });
      if (!blob) throw new Error("Falha ao rasterizar slide");
      zip.file(`img${String(i + 1).padStart(2, "0")}.png`, blob);
    }

    const out = await zip.generateAsync({ type: "blob", compression: "DEFLATE" });
    const base = getRunId().replace(/[^\w\-]+/g, "_") || "post";
    const a = document.createElement("a");
    a.href = URL.createObjectURL(out);
    a.download = `${base}-carousel.zip`;
    a.click();
    URL.revokeObjectURL(a.href);
  } finally {
    iframe.remove();
  }
}
