import { DEFAULT_CHROME_SIZES, THEME_REGISTRY } from "./preset-registry.js";

const GLOBAL_SLOT_NAMES = new Set(["topic_tag", "published_at"]);
const CHROME_SIZE_NAMES = new Set(Object.keys(DEFAULT_CHROME_SIZES));
const STYLE_PROPERTIES = new Set([
  "fontSize",
  "lineHeight",
  "letterSpacing",
  "marginTop",
  "marginRight",
  "marginBottom",
  "marginLeft",
  "maxHeight",
  "maxWidth",
  "width",
  "height",
  "top",
  "right",
  "bottom",
  "left",
  "color",
  "fontWeight",
  "textAlign",
]);

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function validateStringMap(value, name, errors) {
  if (value === undefined) return;
  if (!isPlainObject(value)) {
    errors.push(`${name} must be an object`);
    return;
  }
  for (const [key, item] of Object.entries(value)) {
    if (typeof item !== "string" && typeof item !== "number") {
      errors.push(`${name}.${key} must be a string or number`);
    }
  }
}

export function validateManifest(manifest, { assetExists } = {}) {
  const errors = [];
  if (!isPlainObject(manifest)) return ["manifest must be an object"];

  const theme = manifest.theme;
  const themeConfig = THEME_REGISTRY[theme];
  if (!themeConfig) errors.push("theme must be one of green, red, white, navy");
  if (typeof manifest.runId !== "string" || !manifest.runId.trim()) {
    errors.push("runId must be a non-empty string");
  }
  validateStringMap(manifest.globalSlots, "globalSlots", errors);
  for (const key of Object.keys(manifest.globalSlots ?? {})) {
    if (!GLOBAL_SLOT_NAMES.has(key)) errors.push(`globalSlots.${key} is not supported`);
  }
  if (manifest.sizes !== undefined) {
    validateStringMap(manifest.sizes, "sizes", errors);
    for (const [key, value] of Object.entries(manifest.sizes ?? {})) {
      if (!CHROME_SIZE_NAMES.has(key)) errors.push(`sizes.${key} is not supported`);
      if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
        errors.push(`sizes.${key} must be a positive number`);
      }
    }
  }
  if (!Array.isArray(manifest.slides) || manifest.slides.length === 0) {
    errors.push("slides must be a non-empty array");
    return errors;
  }

  for (const [index, slide] of manifest.slides.entries()) {
    const prefix = `slides[${index}]`;
    if (!isPlainObject(slide)) {
      errors.push(`${prefix} must be an object`);
      continue;
    }
    if (!themeConfig?.slidePaths[slide.type]) {
      errors.push(`${prefix}.type is not supported by theme ${theme}`);
    }
    validateStringMap(slide.slots, `${prefix}.slots`, errors);
    if (slide.hiddenSlots !== undefined) {
      if (!Array.isArray(slide.hiddenSlots) || slide.hiddenSlots.some((key) => typeof key !== "string")) {
        errors.push(`${prefix}.hiddenSlots must be an array of strings`);
      }
    }
    if (slide.slotStyles !== undefined) {
      if (!isPlainObject(slide.slotStyles)) {
        errors.push(`${prefix}.slotStyles must be an object`);
      } else {
        for (const [slot, styles] of Object.entries(slide.slotStyles)) {
          if (!isPlainObject(styles)) {
            errors.push(`${prefix}.slotStyles.${slot} must be an object`);
            continue;
          }
          for (const property of Object.keys(styles)) {
            if (!STYLE_PROPERTIES.has(property)) {
              errors.push(`${prefix}.slotStyles.${slot}.${property} is not supported`);
            }
          }
        }
      }
    }
    const imageUrl = slide.slots?.image_url;
    if (imageUrl !== undefined) {
      if (typeof imageUrl !== "string" || (!imageUrl.startsWith("/") && !imageUrl.startsWith("https://"))) {
        errors.push(`${prefix}.slots.image_url must be an absolute public path or https URL`);
      } else if (assetExists && imageUrl.startsWith("/generated/") && !assetExists(imageUrl)) {
        errors.push(`${prefix}.slots.image_url does not exist: ${imageUrl}`);
      }
    }
  }
  return errors;
}
