/**
 * Forest Studio — free-form WYSIWYG editor.
 * One canvas per slide; each slide is a list of absolute-positioned text/image elements.
 * Drag, resize and snap via Moveable. Inline text edit via contenteditable.
 */

import Moveable from "moveable";
import { toBlob } from "html-to-image";
import JSZip from "jszip";
import { applyChromeSizes, DEFAULT_CHROME } from "../src/chrome/sizes.js";

const THEMES = {
  green: {
    cssPath: "/src/green/theme.css",
    logo: "/images/logos/002-wbig-logo.png",
    palette: [
      ["--accent",         "#2ecc9a", "accent"],
      ["--accent-muted",   "#1a7a5e", "muted"],
      ["--text-primary",   "#f0fff9", "texto"],
      ["--text-secondary", "#a7d7c5", "secund."],
      ["--text-muted",     "#5fa88a", "discreto"],
      ["--bg",             "#0b3d2e", "bg"],
      ["--bg-secondary",   "#0e4d39", "bg-alt"],
    ],
    label: "verde",
  },
  red: {
    cssPath: "/src/red/theme.css",
    logo: "/images/logos/002-wbig-logo.png",
    palette: [
      ["--accent",         "#e53e3e", "accent"],
      ["--accent-muted",   "#7a1f1a", "muted"],
      ["--text-primary",   "#fff5f0", "texto"],
      ["--text-secondary", "#f0c2b0", "secund."],
      ["--text-muted",     "#b07a6a", "discreto"],
      ["--bg",             "#3d0b0b", "bg"],
      ["--bg-secondary",   "#4d0e0e", "bg-alt"],
    ],
    label: "vermelho",
  },
  white: {
    cssPath: "/src/white/theme.css",
    logo: "/images/logos/002-big-logo.png",
    palette: [
      ["--accent",         "#0B7B56", "accent"],
      ["--accent-muted",   "#D4EDE4", "muted"],
      ["--text-primary",   "#0B1F17", "texto"],
      ["--text-secondary", "#2C5F47", "secund."],
      ["--text-muted",     "#8ABAAA", "discreto"],
      ["--bg",             "#FFFFFF", "bg"],
      ["--bg-secondary",   "#F4F9F6", "bg-alt"],
    ],
    label: "branco",
  },
  navy: {
    cssPath: "/src/navy/theme.css",
    logo: "/images/logos/002-wbig-logo.png",
    palette: [
      ["--accent",         "#4A9EFF", "accent"],
      ["--accent-muted",   "#1C4A80", "muted"],
      ["--text-primary",   "#F0F6FF", "texto"],
      ["--text-secondary", "#90B8E8", "secund."],
      ["--text-muted",     "#4A6FA5", "discreto"],
      ["--bg",             "#0D1B2E", "bg"],
      ["--bg-secondary",   "#112240", "bg-alt"],
    ],
    label: "azul marinho",
  },
};

// ---------- Theme bootstrap ----------
const params = new URLSearchParams(window.location.search);
const themeKey = THEMES[params.get("theme")] ? params.get("theme") : "green";
const THEME = THEMES[themeKey];
document.getElementById("themeCss").href = THEME.cssPath;
document.getElementById("hdrThemeName").textContent = THEME.label;

// ---------- State ----------
let _idCounter = 0;
function uid(prefix) { _idCounter += 1; return `${prefix}_${Date.now().toString(36)}_${_idCounter}`; }

const state = {
  theme: themeKey,
  globalSlots: { topic_tag: "Instituto Forest", published_at: "Mai 2026" },
  sizes: { ...DEFAULT_CHROME },
  slides: [],
  selectedSlideIdx: 0,
  selectedElementId: null,
};

let moveable = null;

// ---------- Slide / element factories ----------
function blankSlide() {
  return { id: uid("slide"), kind: "blank", elements: [] };
}

function coverSlide() {
  const palette = THEME.palette;
  const accent = palette[0][1];
  const textPrimary = palette[2][1];
  const textSecondary = palette[3][1];
  return {
    id: uid("slide"),
    kind: "cover",
    elements: [
      { id: uid("el"), type: "text", x: 480, y: 220, w: 552, h: 40,
        content: "Análise de dados", fontSize: 22, color: accent, fontWeight: 600,
        textAlign: "right", lineHeight: 1.2, letterSpacing: 2.4, textTransform: "uppercase" },
      { id: uid("el"), type: "text", x: 60, y: 264, w: 980, h: 400,
        content: "Título da capa", fontSize: 132, color: textPrimary, fontWeight: 900,
        textAlign: "right", lineHeight: 1.0, letterSpacing: -2, textTransform: "none" },
      { id: uid("el"), type: "text", x: 48, y: 920, w: 520, h: 200,
        content: "Resumo curto que apresenta o conteúdo da capa em uma ou duas frases.",
        fontSize: 34, color: textSecondary, fontWeight: 400,
        textAlign: "left", lineHeight: 1.35, letterSpacing: 0, textTransform: "none" },
    ],
  };
}

function ctaSlide() {
  const palette = THEME.palette;
  const accent = palette[0][1];
  const textPrimary = palette[2][1];
  const textSecondary = palette[3][1];
  return {
    id: uid("slide"),
    kind: "cta",
    elements: [
      { id: uid("el"), type: "text", x: 48, y: 240, w: 984, h: 60,
        content: "Quer continuar acompanhando?", fontSize: 30, color: accent, fontWeight: 600,
        textAlign: "center", lineHeight: 1.2, letterSpacing: 3, textTransform: "uppercase" },
      { id: uid("el"), type: "text", x: 48, y: 320, w: 984, h: 220,
        content: "Acompanhe o Instituto Forest", fontSize: 64, color: textPrimary, fontWeight: 900,
        textAlign: "center", lineHeight: 1.05, letterSpacing: -1, textTransform: "none" },
      { id: uid("el"), type: "text", x: 130, y: 560, w: 820, h: 200,
        content: "Dados abertos, código aberto e análises reprodutíveis em um portal centralizado.",
        fontSize: 28, color: textSecondary, fontWeight: 400,
        textAlign: "center", lineHeight: 1.4, letterSpacing: 0, textTransform: "none" },
      { id: uid("el"), type: "text", x: 48, y: 820, w: 984, h: 60,
        content: "institutoforest.org", fontSize: 36, color: accent, fontWeight: 600,
        textAlign: "center", lineHeight: 1.2, letterSpacing: 0, textTransform: "none" },
    ],
  };
}

function makeTextElement() {
  return {
    id: uid("el"), type: "text", x: 240, y: 615, w: 600, h: 120,
    content: "Texto", fontSize: 48, color: THEME.palette[2][1], fontWeight: 700,
    textAlign: "left", lineHeight: 1.25, letterSpacing: 0, textTransform: "none",
  };
}

function makeImageElement(src) {
  return {
    id: uid("el"), type: "image", x: 240, y: 475, w: 600, h: 400,
    src, objectFit: "contain", objectPosition: "center center",
  };
}

// Init with one blank slide
state.slides.push(blankSlide());

// ---------- Rendering ----------
const card = document.getElementById("card");

function currentSlide() {
  return state.slides[state.selectedSlideIdx];
}

function renderChrome() {
  // wipe and re-populate the card with chrome corners
  card.innerHTML = "";
  card.style.backgroundColor = "var(--bg)";

  const topRow = document.createElement("div");
  topRow.className = "absolute top-0 left-0 right-0 flex items-center justify-between px-12 pt-10 z-10";
  topRow.style.pointerEvents = "none";

  const topic = document.createElement("span");
  topic.className = "chrome-topic tracking-widest uppercase font-semibold";
  topic.style.color = "var(--accent)";
  topic.dataset.slot = "topic_tag";
  topic.textContent = state.globalSlots.topic_tag;
  topRow.appendChild(topic);

  const date = document.createElement("span");
  date.className = "chrome-date font-medium";
  date.style.color = "var(--text-muted)";
  date.dataset.slot = "published_at";
  date.textContent = state.globalSlots.published_at;
  topRow.appendChild(date);
  card.appendChild(topRow);

  const bottomRow = document.createElement("div");
  bottomRow.className = "absolute bottom-0 left-0 right-0 flex items-end justify-between px-12 pb-10 z-10";
  bottomRow.style.pointerEvents = "none";

  const logo = document.createElement("img");
  logo.className = "footer-brand-logo";
  logo.src = THEME.logo;
  logo.alt = "Instituto Forest";
  logo.width = 280;
  logo.height = 80;
  bottomRow.appendChild(logo);

  const page = document.createElement("span");
  page.className = "chrome-page font-semibold tabular-nums";
  page.style.color = "var(--accent)";
  page.dataset.slot = "card_number";
  page.textContent = `${pad(state.selectedSlideIdx + 1)} / ${pad(state.slides.length)}`;
  bottomRow.appendChild(page);
  card.appendChild(bottomRow);

  const accentBar = document.createElement("div");
  accentBar.className = "absolute bottom-0 left-0 right-0 h-1";
  accentBar.style.background = "var(--accent)";
  accentBar.style.pointerEvents = "none";
  card.appendChild(accentBar);

  // Synthetic centre + edge guidelines for Moveable's elementGuidelines
  ["centerV", "centerH", "edgeL", "edgeR", "edgeT", "edgeB"].forEach((g) => {
    const gn = document.createElement("div");
    gn.className = `guide guide-${g}`;
    gn.style.position = "absolute";
    gn.style.pointerEvents = "none";
    gn.style.visibility = "hidden";
    if (g === "centerV") { gn.style.left = "540px"; gn.style.top = "0"; gn.style.width = "0"; gn.style.height = "1350px"; }
    if (g === "centerH") { gn.style.top = "675px"; gn.style.left = "0"; gn.style.width = "1080px"; gn.style.height = "0"; }
    if (g === "edgeL")   { gn.style.left = "0"; gn.style.top = "0"; gn.style.width = "0"; gn.style.height = "1350px"; }
    if (g === "edgeR")   { gn.style.left = "1080px"; gn.style.top = "0"; gn.style.width = "0"; gn.style.height = "1350px"; }
    if (g === "edgeT")   { gn.style.top = "0"; gn.style.left = "0"; gn.style.width = "1080px"; gn.style.height = "0"; }
    if (g === "edgeB")   { gn.style.top = "1350px"; gn.style.left = "0"; gn.style.width = "1080px"; gn.style.height = "0"; }
    card.appendChild(gn);
  });
}

function renderElement(el) {
  const wrap = document.createElement("div");
  wrap.className = `el el-${el.type}`;
  wrap.dataset.elementId = el.id;
  applyElementGeom(wrap, el);

  if (el.type === "text") {
    const inner = document.createElement("div");
    inner.className = "text-content";
    inner.innerHTML = el.content;
    applyTextStyle(inner, el);
    wrap.appendChild(inner);
  } else if (el.type === "image") {
    const img = document.createElement("img");
    img.src = el.src;
    img.style.objectFit = el.objectFit || "contain";
    img.style.objectPosition = el.objectPosition || "center center";
    wrap.appendChild(img);
  }
  card.appendChild(wrap);
  return wrap;
}

function applyElementGeom(wrap, el) {
  wrap.style.left = `${el.x}px`;
  wrap.style.top = `${el.y}px`;
  wrap.style.width = `${el.w}px`;
  wrap.style.height = `${el.h}px`;
}

function applyTextStyle(node, el) {
  node.style.fontSize = `${el.fontSize}px`;
  node.style.color = el.color;
  node.style.fontWeight = String(el.fontWeight ?? 400);
  node.style.textAlign = el.textAlign || "left";
  node.style.lineHeight = String(el.lineHeight ?? 1.25);
  node.style.letterSpacing = `${el.letterSpacing ?? 0}px`;
  node.style.textTransform = el.textTransform || "none";
}

function renderActiveSlide() {
  renderChrome();
  const slide = currentSlide();
  slide.elements.forEach(renderElement);
  applyChromeSizes(state.sizes);
  attachSelection();
  reattachMoveable();
}

function attachSelection() {
  card.querySelectorAll(".el").forEach((wrap) => {
    const id = wrap.dataset.elementId;
    wrap.addEventListener("mousedown", (e) => {
      if (wrap.querySelector(".text-content[contenteditable='true']")) return;
      e.stopPropagation();
      selectElement(id);
    });
    const textNode = wrap.querySelector(".text-content");
    if (textNode) {
      wrap.addEventListener("dblclick", (e) => {
        e.stopPropagation();
        beginInlineEdit(id, textNode);
      });
    }
  });
  card.addEventListener("mousedown", (e) => {
    if (e.target === card) selectElement(null);
  });
}

function selectElement(id) {
  state.selectedElementId = id;
  card.querySelectorAll(".el").forEach((w) => {
    w.classList.toggle("selected", w.dataset.elementId === id);
  });
  reattachMoveable();
  renderPropsPanel();
}

function reattachMoveable() {
  if (moveable) { moveable.destroy(); moveable = null; }
  if (!state.selectedElementId) return;
  const target = card.querySelector(`.el[data-element-id="${state.selectedElementId}"]`);
  if (!target) return;
  const guidelines = Array.from(card.querySelectorAll(".el")).filter((n) => n !== target)
    .concat(Array.from(card.querySelectorAll(".guide")));

  moveable = new Moveable(card, {
    target,
    draggable: true,
    resizable: true,
    keepRatio: false,
    snappable: true,
    snapCenter: true,
    snapThreshold: 5,
    elementGuidelines: guidelines,
    snapDirections: { top: true, left: true, right: true, bottom: true, center: true, middle: true },
    elementSnapDirections: { top: true, left: true, right: true, bottom: true, center: true, middle: true },
  });

  moveable.on("drag", (e) => {
    e.target.style.transform = e.transform;
  });
  moveable.on("dragEnd", (e) => {
    const el = findElement(state.selectedElementId);
    if (!el) return;
    el.x += e.lastEvent?.beforeTranslate?.[0] ?? 0;
    el.y += e.lastEvent?.beforeTranslate?.[1] ?? 0;
    e.target.style.transform = "";
    applyElementGeom(e.target, el);
    moveable.updateRect();
    renderPropsPanel();
  });
  moveable.on("resize", (e) => {
    e.target.style.width = `${e.width}px`;
    e.target.style.height = `${e.height}px`;
    e.target.style.transform = e.drag.transform;
  });
  moveable.on("resizeEnd", (e) => {
    const el = findElement(state.selectedElementId);
    if (!el) return;
    el.w = parseFloat(e.target.style.width);
    el.h = parseFloat(e.target.style.height);
    el.x += e.lastEvent?.drag?.beforeTranslate?.[0] ?? 0;
    el.y += e.lastEvent?.drag?.beforeTranslate?.[1] ?? 0;
    e.target.style.transform = "";
    applyElementGeom(e.target, el);
    moveable.updateRect();
    renderPropsPanel();
  });
}

function findElement(id) {
  const slide = currentSlide();
  return slide.elements.find((e) => e.id === id) || null;
}

function beginInlineEdit(id, node) {
  node.setAttribute("contenteditable", "true");
  node.focus();
  document.execCommand && document.execCommand("selectAll", false, null);
  const finish = () => {
    node.setAttribute("contenteditable", "false");
    const el = findElement(id);
    if (el) el.content = node.innerHTML;
    node.removeEventListener("blur", finish);
    node.removeEventListener("keydown", onKey);
  };
  const onKey = (e) => { if (e.key === "Escape") { node.blur(); } };
  node.addEventListener("blur", finish);
  node.addEventListener("keydown", onKey);
}

function pad(n) { return String(n).padStart(2, "0"); }

// ---------- Slides list ----------
const slidesList = document.getElementById("slidesList");
function renderSlidesList() {
  slidesList.innerHTML = "";
  state.slides.forEach((s, i) => {
    const row = document.createElement("div");
    row.className = "slide-row" + (i === state.selectedSlideIdx ? " active" : "");
    row.innerHTML = `<span class="num">${pad(i + 1)}</span><span class="label">${labelFor(s)}</span>`;
    const actions = document.createElement("span");
    actions.className = "actions";
    const up = document.createElement("button"); up.className = "icon-btn"; up.textContent = "↑";
    up.onclick = (e) => { e.stopPropagation(); moveSlide(i, -1); };
    const down = document.createElement("button"); down.className = "icon-btn"; down.textContent = "↓";
    down.onclick = (e) => { e.stopPropagation(); moveSlide(i, 1); };
    const dup = document.createElement("button"); dup.className = "icon-btn"; dup.title = "Duplicar"; dup.textContent = "⎘";
    dup.onclick = (e) => { e.stopPropagation(); duplicateSlide(i); };
    const del = document.createElement("button"); del.className = "icon-btn"; del.textContent = "×";
    del.onclick = (e) => { e.stopPropagation(); deleteSlide(i); };
    actions.append(up, down, dup, del);
    row.appendChild(actions);
    row.onclick = () => { state.selectedSlideIdx = i; state.selectedElementId = null; renderAll(); };
    slidesList.appendChild(row);
  });
}

function labelFor(s) {
  if (s.kind === "cover") return "Capa";
  if (s.kind === "cta") return "Final";
  return "Slide";
}

function moveSlide(i, delta) {
  const j = i + delta;
  if (j < 0 || j >= state.slides.length) return;
  const t = state.slides[i];
  state.slides[i] = state.slides[j];
  state.slides[j] = t;
  if (state.selectedSlideIdx === i) state.selectedSlideIdx = j;
  else if (state.selectedSlideIdx === j) state.selectedSlideIdx = i;
  renderAll();
}

function duplicateSlide(i) {
  const clone = JSON.parse(JSON.stringify(state.slides[i]));
  clone.id = uid("slide");
  clone.elements.forEach((el) => { el.id = uid("el"); });
  state.slides.splice(i + 1, 0, clone);
  state.selectedSlideIdx = i + 1;
  state.selectedElementId = null;
  renderAll();
}

function deleteSlide(i) {
  if (state.slides[i].elements.length > 0 && !confirm("Apagar este slide?")) return;
  state.slides.splice(i, 1);
  if (state.slides.length === 0) state.slides.push(blankSlide());
  state.selectedSlideIdx = Math.min(state.selectedSlideIdx, state.slides.length - 1);
  state.selectedElementId = null;
  renderAll();
}

document.getElementById("btnAddCover").onclick = () => addSlide(coverSlide());
document.getElementById("btnAddBlank").onclick = () => addSlide(blankSlide());
document.getElementById("btnAddCta").onclick   = () => addSlide(ctaSlide());

function addSlide(slide) {
  state.slides.splice(state.selectedSlideIdx + 1, 0, slide);
  state.selectedSlideIdx += 1;
  state.selectedElementId = null;
  renderAll();
}

// ---------- Add element buttons ----------
document.getElementById("btnAddText").onclick = () => {
  const el = makeTextElement();
  currentSlide().elements.push(el);
  state.selectedElementId = el.id;
  renderAll();
};

const imgFileInput = document.getElementById("imgFileInput");
document.getElementById("btnAddImage").onclick = () => imgFileInput.click();
imgFileInput.addEventListener("change", (e) => {
  const file = e.target.files?.[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = () => {
    const el = makeImageElement(reader.result);
    currentSlide().elements.push(el);
    state.selectedElementId = el.id;
    renderAll();
  };
  reader.readAsDataURL(file);
  imgFileInput.value = "";
});

document.getElementById("btnAddImageUrl").onclick = () => {
  const url = document.getElementById("imgUrlInput").value.trim();
  if (!url) return;
  const el = makeImageElement(url);
  currentSlide().elements.push(el);
  state.selectedElementId = el.id;
  document.getElementById("imgUrlInput").value = "";
  renderAll();
};

// ---------- Global metadata ----------
const globalTopic = document.getElementById("globalTopic");
const globalPubAt = document.getElementById("globalPubAt");
globalTopic.value = state.globalSlots.topic_tag;
globalPubAt.value = state.globalSlots.published_at;
globalTopic.oninput = () => { state.globalSlots.topic_tag = globalTopic.value; renderActiveSlide(); };
globalPubAt.oninput = () => { state.globalSlots.published_at = globalPubAt.value; renderActiveSlide(); };

const chromeInputs = [
  ["chromeTopic", "topicTagPx"],
  ["chromeDate", "datePx"],
  ["chromePage", "pageNumberPx"],
  ["chromeLogo", "logoHeightPx"],
];
chromeInputs.forEach(([id, key]) => {
  const node = document.getElementById(id);
  node.value = state.sizes[key];
  node.oninput = () => {
    const v = Number(node.value);
    if (Number.isFinite(v)) state.sizes[key] = v;
    applyChromeSizes(state.sizes);
  };
});

// ---------- Palette swatches + custom color ----------
const swatchesEl = document.getElementById("paletteSwatches");
THEME.palette.forEach(([varName, hex, label]) => {
  const s = document.createElement("button");
  s.className = "swatch";
  s.dataset.label = label;
  s.style.background = hex;
  s.title = `${label} (${hex})`;
  s.onclick = () => applyColorToSelection(hex);
  swatchesEl.appendChild(s);
});
document.getElementById("customColor").oninput = (e) => applyColorToSelection(e.target.value);

function applyColorToSelection(color) {
  const el = findElement(state.selectedElementId);
  if (!el || el.type !== "text") return;
  el.color = color;
  const wrap = card.querySelector(`.el[data-element-id="${el.id}"] .text-content`);
  if (wrap) applyTextStyle(wrap, el);
  renderPropsPanel();
}

// ---------- Properties panel ----------
const elementProps = document.getElementById("elementProps");
const propsBody = document.getElementById("propsBody");
const emptyHint = document.getElementById("emptyHint");

function renderPropsPanel() {
  const el = findElement(state.selectedElementId);
  if (!el) {
    elementProps.classList.add("hidden");
    emptyHint.classList.remove("hidden");
    return;
  }
  elementProps.classList.remove("hidden");
  emptyHint.classList.add("hidden");
  propsBody.innerHTML = "";

  const slide = currentSlide();
  const idx = slide.elements.indexOf(el);
  const layerInfo = document.createElement("p");
  layerInfo.className = "hint";
  layerInfo.textContent = `Camada ${idx + 1} de ${slide.elements.length} · ${el.type === "text" ? "Texto" : "Imagem"}`;
  propsBody.appendChild(layerInfo);

  const xywh = document.createElement("div");
  xywh.className = "grid-2";
  ["x", "y", "w", "h"].forEach((k) => {
    const wrap = document.createElement("div");
    const l = document.createElement("label"); l.textContent = k.toUpperCase();
    const i = document.createElement("input"); i.type = "number"; i.value = String(el[k]);
    i.oninput = () => {
      const v = Number(i.value);
      if (!Number.isFinite(v)) return;
      el[k] = v;
      const wrapEl = card.querySelector(`.el[data-element-id="${el.id}"]`);
      if (wrapEl) applyElementGeom(wrapEl, el);
      if (moveable) moveable.updateRect();
    };
    wrap.appendChild(l); wrap.appendChild(i);
    xywh.appendChild(wrap);
  });
  propsBody.appendChild(xywh);

  if (el.type === "text") renderTextProps(el);
  else renderImageProps(el);

  const actions = document.createElement("div");
  actions.className = "btn-row";
  const front = btn("Trazer p/ frente", "small", () => { slide.elements.splice(idx, 1); slide.elements.push(el); renderAll(); selectElement(el.id); });
  const back  = btn("Enviar p/ trás", "small", () => { slide.elements.splice(idx, 1); slide.elements.unshift(el); renderAll(); selectElement(el.id); });
  const dup   = btn("Duplicar", "small", () => {
    const c = JSON.parse(JSON.stringify(el));
    c.id = uid("el"); c.x += 20; c.y += 20;
    slide.elements.splice(idx + 1, 0, c);
    renderAll(); selectElement(c.id);
  });
  const del   = btn("Apagar", "small danger", () => {
    slide.elements.splice(idx, 1);
    state.selectedElementId = null;
    renderAll();
  });
  actions.append(front, back, dup, del);
  propsBody.appendChild(actions);
}

function btn(label, klass, onclick) {
  const b = document.createElement("button");
  b.className = "btn " + (klass || "");
  b.textContent = label;
  b.onclick = onclick;
  return b;
}

function renderTextProps(el) {
  const grid = document.createElement("div");
  grid.className = "grid-2";
  addNumber(grid, "Fonte (px)", "fontSize", el, applyTextOnly);
  addNumber(grid, "Peso", "fontWeight", el, applyTextOnly);
  addSelect(grid, "Alinhar", "textAlign", el, [["left","esq"],["center","centro"],["right","dir"],["justify","just"]], applyTextOnly);
  addSelect(grid, "Transformar", "textTransform", el, [["none","Aa"],["uppercase","AA"],["lowercase","aa"]], applyTextOnly);
  addNumber(grid, "Entrelinha", "lineHeight", el, applyTextOnly, 0.05);
  addNumber(grid, "Letter-spc", "letterSpacing", el, applyTextOnly);
  propsBody.appendChild(grid);

  const colorWrap = document.createElement("div");
  colorWrap.style.marginTop = "0.5rem";
  const cl = document.createElement("label"); cl.textContent = "Cor"; colorWrap.appendChild(cl);
  const ci = document.createElement("input"); ci.type = "color"; ci.value = el.color;
  ci.oninput = () => { el.color = ci.value; applyTextOnly(el); };
  colorWrap.appendChild(ci);
  propsBody.appendChild(colorWrap);
}

function renderImageProps(el) {
  const wrap = document.createElement("div");

  const lbl = document.createElement("label"); lbl.textContent = "src"; wrap.appendChild(lbl);
  const srcInput = document.createElement("input"); srcInput.type = "text"; srcInput.value = el.src.startsWith("data:") ? "(arquivo carregado)" : el.src;
  srcInput.readOnly = true; wrap.appendChild(srcInput);

  const swap = btn("Trocar imagem", "small", () => {
    const f = document.createElement("input"); f.type = "file"; f.accept = "image/*";
    f.onchange = () => {
      const file = f.files?.[0]; if (!file) return;
      const r = new FileReader();
      r.onload = () => { el.src = r.result; renderAll(); selectElement(el.id); };
      r.readAsDataURL(file);
    };
    f.click();
  });
  wrap.appendChild(swap);

  const grid = document.createElement("div");
  grid.className = "grid-2";
  addSelect(grid, "Object-fit", "objectFit", el, [["contain","contain"],["cover","cover"],["fill","fill"],["none","none"]], applyImageOnly);
  addSelect(grid, "Object-pos", "objectPosition", el, [
    ["top center","topo"],["center","centro"],["bottom center","base"],
    ["left center","esquerda"],["right center","direita"],
    ["top left","topo-esq"],["top right","topo-dir"],
    ["bottom left","base-esq"],["bottom right","base-dir"],
  ], applyImageOnly);
  wrap.appendChild(grid);

  propsBody.appendChild(wrap);
}

function applyTextOnly(el) {
  const node = card.querySelector(`.el[data-element-id="${el.id}"] .text-content`);
  if (node) applyTextStyle(node, el);
}
function applyImageOnly(el) {
  const node = card.querySelector(`.el[data-element-id="${el.id}"] img`);
  if (!node) return;
  node.src = el.src;
  node.style.objectFit = el.objectFit;
  node.style.objectPosition = el.objectPosition;
}

function addNumber(parent, label, key, el, apply, step = 1) {
  const wrap = document.createElement("div");
  const l = document.createElement("label"); l.textContent = label;
  const i = document.createElement("input"); i.type = "number"; i.step = String(step); i.value = String(el[key] ?? "");
  i.oninput = () => {
    const v = Number(i.value);
    if (Number.isFinite(v)) el[key] = v;
    apply(el);
  };
  wrap.append(l, i);
  parent.appendChild(wrap);
}

function addSelect(parent, label, key, el, opts, apply) {
  const wrap = document.createElement("div");
  const l = document.createElement("label"); l.textContent = label;
  const s = document.createElement("select");
  opts.forEach(([v, t]) => {
    const o = document.createElement("option"); o.value = v; o.textContent = t;
    if (el[key] === v) o.selected = true;
    s.appendChild(o);
  });
  s.onchange = () => { el[key] = s.value; apply(el); };
  wrap.append(l, s);
  parent.appendChild(wrap);
}

// ---------- Save / Load manifest ----------
document.getElementById("btnSave").onclick = () => {
  const data = JSON.stringify({
    theme: state.theme,
    globalSlots: state.globalSlots,
    sizes: state.sizes,
    slides: state.slides,
  }, null, 2);
  const blob = new Blob([data], { type: "application/json" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = `studio-${state.theme}-${Date.now()}.json`;
  a.click();
  URL.revokeObjectURL(a.href);
};

const loadFile = document.getElementById("loadFile");
document.getElementById("btnLoad").onclick = () => loadFile.click();
loadFile.addEventListener("change", (e) => {
  const f = e.target.files?.[0]; if (!f) return;
  const r = new FileReader();
  r.onload = () => {
    try {
      const data = JSON.parse(r.result);
      if (data.theme && data.theme !== state.theme) {
        alert(`Manifest é do tema "${data.theme}", abra o studio nesse tema (?theme=${data.theme}).`);
        return;
      }
      state.globalSlots = data.globalSlots || state.globalSlots;
      state.sizes = { ...DEFAULT_CHROME, ...(data.sizes || {}) };
      state.slides = data.slides && data.slides.length ? data.slides : [blankSlide()];
      state.selectedSlideIdx = 0;
      state.selectedElementId = null;
      globalTopic.value = state.globalSlots.topic_tag;
      globalPubAt.value = state.globalSlots.published_at;
      chromeInputs.forEach(([id, key]) => { document.getElementById(id).value = state.sizes[key]; });
      renderAll();
    } catch (err) {
      alert("Manifest inválido: " + err.message);
    }
  };
  r.readAsText(f);
  loadFile.value = "";
});

// ---------- Export ZIP PNG ----------
async function exportZip(blank = false) {
  const hint = document.getElementById("exportHint");
  const prevScale = getComputedStyle(document.documentElement).getPropertyValue("--canvas-scale");
  const prevIdx = state.selectedSlideIdx;
  const prevSelected = state.selectedElementId;

  // capture at 1:1
  document.documentElement.style.setProperty("--canvas-scale", "1");
  state.selectedElementId = null;

  const zip = new JSZip();
  try {
    for (let i = 0; i < state.slides.length; i++) {
      hint.textContent = `Gerando ${i + 1}/${state.slides.length}...`;
      state.selectedSlideIdx = i;
      renderActiveSlide();
      if (blank) {
        card.querySelectorAll(".el").forEach((n) => { n.style.display = "none"; });
      }
      await waitForImages(card);
      const blob = await toBlob(card, { width: 1080, height: 1350, pixelRatio: 1, cacheBust: true });
      if (!blob) throw new Error("toBlob falhou");
      zip.file(`img${pad(i + 1)}.png`, blob);
    }
    const out = await zip.generateAsync({ type: "blob", compression: "DEFLATE" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(out);
    a.download = `studio-${state.theme}${blank ? "-blank" : ""}.zip`;
    a.click();
    URL.revokeObjectURL(a.href);
    hint.textContent = `Download iniciado: ${state.slides.length} PNGs.`;
  } catch (e) {
    console.error(e);
    hint.textContent = "Erro: " + (e.message || e);
  } finally {
    document.documentElement.style.setProperty("--canvas-scale", prevScale.trim() || "0.5");
    state.selectedSlideIdx = prevIdx;
    state.selectedElementId = prevSelected;
    renderActiveSlide();
  }
}
document.getElementById("btnExport").onclick = () => exportZip(false);
document.getElementById("btnExportBlank").onclick = () => exportZip(true);

async function waitForImages(root) {
  const imgs = Array.from(root.querySelectorAll("img"));
  await Promise.all(imgs.map((img) => img.complete && img.naturalWidth
    ? Promise.resolve()
    : new Promise((res) => { img.onload = res; img.onerror = res; })));
  await new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)));
}

// ---------- Initial fit + render ----------
function fitCanvas() {
  const stage = document.getElementById("stage");
  const vw = stage.clientWidth;
  const vh = stage.clientHeight;
  const scale = Math.min((vw - 32) / 1080, (vh - 32) / 1350, 0.85);
  document.documentElement.style.setProperty("--canvas-scale", scale);
}
window.addEventListener("resize", fitCanvas);
setTimeout(fitCanvas, 0);

function renderAll() {
  renderSlidesList();
  renderActiveSlide();
  renderPropsPanel();
}
renderAll();
