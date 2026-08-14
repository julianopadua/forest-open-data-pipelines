export const DEFAULT_CHROME_SIZES = Object.freeze({
  topicTagPx: 24,
  datePx: 26,
  pageNumberPx: 24,
  logoHeightPx: 54,
});

export const DEFAULT_GLOBAL_SLOTS = Object.freeze({
  topic_tag: "Instituto Forest",
  published_at: "Mai 2026",
});

export const THEME_REGISTRY = Object.freeze({
  green: {
    label: "Verde",
    composerPath: "/green/composer.html",
    slidePaths: {
      cover: "/green/slides/cover.html",
      body_image_text: "/green/slides/body-image-text.html",
      body_chart: "/green/slides/body-chart.html",
      body_text: "/green/slides/body-text.html",
      body_logo_half: "/green/slides/body-logo-half.html",
      body_text_image: "/green/slides/body-text-image.html",
      cta: "/green/slides/cta.html",
    },
  },
  red: {
    label: "Vermelho",
    composerPath: "/red/composer.html",
    slidePaths: {
      cover: "/red/slides/cover.html",
      body_image_text: "/red/slides/body-image-text.html",
      body_chart: "/red/slides/body-chart.html",
      body_text: "/red/slides/body-text.html",
      cta: "/red/slides/cta.html",
    },
  },
  white: {
    label: "Branco",
    composerPath: "/white/composer.html",
    slidePaths: {
      cover: "/white/slides/cover.html",
      body_image_text: "/white/slides/body-image-text.html",
      body_chart: "/white/slides/body-chart.html",
      body_text: "/white/slides/body-text.html",
      cta: "/white/slides/cta.html",
    },
  },
  navy: {
    label: "Azul marinho",
    composerPath: "/navy/composer.html",
    slidePaths: {
      cover: "/navy/slides/cover.html",
      body_image_text: "/navy/slides/body-image-text.html",
      body_chart: "/navy/slides/body-chart.html",
      body_text: "/navy/slides/body-text.html",
      cta: "/navy/slides/cta.html",
    },
  },
});

export const PRESET_REGISTRY = Object.freeze({
  "boas-vindas": {
    theme: "green",
    manifestPath: "/examples/forest-boas-vindas.manifest.json",
  },
  bdqueimadas: {
    theme: "red",
    manifestPath: "/examples/bdqueimadas-social.manifest.json",
  },
  "bdqueimadas-diario": {
    theme: "red",
    manifestPath: "/examples/bdqueimadas-daily-social.manifest.json",
  },
  "bdqueimadas-daily": {
    theme: "red",
    manifestPath: "/examples/bdqueimadas-daily-social.manifest.json",
  },
  "research-trends": {
    theme: "white",
    manifestPath: "/examples/research-trends.manifest.json",
  },
  "anp-producao-petroleo-gas": {
    theme: "navy",
    manifestPath: "/examples/anp-producao-petroleo-gas.manifest.json",
  },
});

export function getThemeConfig(theme) {
  const config = THEME_REGISTRY[theme];
  if (!config) throw new Error(`Unsupported theme: ${theme}`);
  return config;
}

export function getPresetConfig(preset) {
  return PRESET_REGISTRY[preset] ?? null;
}
