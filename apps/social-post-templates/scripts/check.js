import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { PRESET_REGISTRY } from "../src/shared/preset-registry.js";
import { validateManifest } from "../src/shared/manifest-validation.js";

const appRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
const examplesDir = join(appRoot, "examples");
const publicDir = join(appRoot, "public");

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function assetExists(publicPath) {
  const pathWithoutQuery = publicPath.split("?", 1)[0];
  return existsSync(join(publicDir, pathWithoutQuery.replace(/^\//, "")));
}

const manifestFiles = readdirSync(examplesDir)
  .filter((name) => name.endsWith(".manifest.json"))
  .sort();
const registeredPaths = new Set(Object.values(PRESET_REGISTRY).map((preset) => preset.manifestPath));
const failures = [];

for (const fileName of manifestFiles) {
  const path = join(examplesDir, fileName);
  let manifest;
  try {
    manifest = readJson(path);
  } catch (error) {
    failures.push(`${fileName}: invalid JSON (${error.message})`);
    continue;
  }
  const errors = validateManifest(manifest, { assetExists });
  for (const error of errors) failures.push(`${fileName}: ${error}`);
}

for (const [name, preset] of Object.entries(PRESET_REGISTRY)) {
  if (!registeredPaths.has(preset.manifestPath) || !existsSync(join(publicDir, preset.manifestPath))) {
    failures.push(`preset ${name} does not have a public manifest: ${preset.manifestPath}`);
  }
}

if (failures.length) {
  console.error("Social template validation failed:");
  for (const failure of failures) console.error(`- ${failure}`);
  process.exit(1);
}

console.log(`Validated ${manifestFiles.length} manifests and ${Object.keys(PRESET_REGISTRY).length} registered presets.`);
