# forest-open-data-pipelines

Python monorepo for ingesting open datasets, building structured report packages, and generating social media assets — all published to Supabase Storage for direct consumption by the `forest-portal` frontend.

## Table of contents

1. [Quick start](#quick-start)
2. [What this repo does](#what-this-repo-does)
3. [Architecture](#architecture)
4. [Repository layout](#repository-layout)
5. [Configuration](#configuration)
6. [Available make targets](#available-make-targets)
7. [CLI reference](#cli-reference)
8. [Registered datasets](#registered-datasets)
9. [Automation](#automation)
10. [apps/ directory](#apps-directory)
11. [Module documentation](#module-documentation)
12. [Troubleshooting](#troubleshooting)

---

## Quick start

```bash
git clone <repo-url> forest-open-data-pipelines
cd forest-open-data-pipelines
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\Activate.ps1
make dev
cp .env.example .env   # then fill in credentials
make check-env
make sync-inpe
```

`make dev` installs the package and dev dependencies in editable mode. `make check-env` confirms all required environment variables are present before you run anything.

---

## What this repo does

**Dataset sync.** Downloads open datasets (CVM, INPE, EIA, INMET, Noticias Agricolas), caches them under `data/`, uploads objects to a Supabase Storage bucket, and publishes a `manifest.json` at each dataset prefix.

**Reports.** Aggregates or transforms data into structured report packages (e.g., BDQueimadas fire overview) and publishes them to Storage, including manifests and public URLs suitable for embedding in the portal.

**Social media generation.** Generates Instagram carousel charts and LLM-written captions from INPE fire data. Charts are Vega-Lite specs; captions are produced via Groq.

---

## Architecture

```
Upstream sources                  Python pipeline              Supabase Storage          forest-portal
─────────────────                 ───────────────              ────────────────          ─────────────
CVM (CVM Portal)    ─────┐
INPE (BDQueimadas)  ─────┤
EIA (API)           ─────┼──► forest-pipelines CLI ──────► public bucket ──────────► Next.js frontend
INMET (FTP/HTTP)    ─────┤        (Typer)                  manifest.json              fetches manifests
Noticias Agricolas  ─────┤                                  + objects                  + public URLs
dados.gov.br (ANP)  ─────┘
                                      │
                               Groq LLM (optional)
                               for social captions
```

The frontend fetches the public `manifest.json` at each dataset prefix directly from Storage — no dedicated download API is needed on the Next.js side.

---

## Repository layout

| Path | Role |
| --- | --- |
| `src/forest_pipelines/` | Package root: CLI, settings, dataset runners, storage client, manifests, reports, audits, LLM, social generation. |
| `src/forest_pipelines/dados_abertos/` | ANP catalog scraping from dados.gov.br. |
| `configs/app.yml` | Directory paths, Supabase bucket env var name, LLM defaults. |
| `configs/datasets/` | One YAML per dataset (source URLs, `bucket_prefix`, sync parameters). |
| `configs/reports/` | Report definitions consumed by `build-report`. |
| `data/` | Local download cache (gitignored). |
| `logs/` | Rotated run logs (gitignored). |
| `docs/` | Audit outputs and per-module notes. |
| `apps/social-post-templates/` | Static frontend for rendering social carousel slides (see [apps/ directory](#apps-directory)). |
| `scripts/` | Optional helpers (e.g., CVM historical backfill). |
| `.github/workflows/` | Scheduled and manual GitHub Actions workflows. |

---

## Configuration

### Prerequisites

- Python 3.11+
- A Supabase project with Storage enabled and the target bucket marked public

### Environment variables

Copy `.env.example` to `.env` and set:

| Variable | Required | Purpose |
| --- | --- | --- |
| `SUPABASE_URL` | yes | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | yes | Service role key for Storage uploads — keep secret |
| `SUPABASE_BUCKET_OPEN_DATA` | yes | Bucket name, e.g. `open-data` |
| `GROQ_API_KEY` | for LLM targets only | Required by `bdqueimadas-social-full` and any report using LLM captions |

Run `make check-env` after editing `.env` to verify all required variables are present.

### Application YAML

`configs/app.yml` controls directory names relative to the repo root, the env var name used to resolve the bucket, and default LLM parameters. Individual dataset YAMLs under `configs/datasets/` supply source URLs, `bucket_prefix`, and parameters such as `latest_months`.

---

## Available make targets

Run `make` or `make help` to see this list in your terminal.

### Setup

| Target | Description |
| --- | --- |
| `make venv` | Create `.venv` virtual environment |
| `make install` | Install package in editable mode (`pip install -e .`) |
| `make dev` | Install with dev dependencies (pytest, linting) |
| `make check-env` | Verify all required environment variables are set |

### Dataset sync

| Target | Description |
| --- | --- |
| `make sync-cvm` | Sync CVM daily fund information (last 12 months) |
| `make sync-inpe` | Sync INPE BDQueimadas fire focus data |
| `make sync-eia` | Sync EIA weekly petroleum data |
| `make sync-inmet` | Sync INMET historical weather data |
| `make sync-news` | Sync Noticias Agricolas news feed |
| `make sync-all` | Run all primary syncs sequentially |

### Reports

| Target | Description |
| --- | --- |
| `make build-report-bdqueimadas` | Build and publish BDQueimadas fire overview report |

### Audits

| Target | Description |
| --- | --- |
| `make audit-bdqueimadas` | Run audit on INPE BDQueimadas dataset; output under `docs/audits/` |

### ANP open data

| Target | Description |
| --- | --- |
| `make anp-catalog` | Scrape full ANP catalog from dados.gov.br (all pages) |
| `make anp-catalog-smoke` | Quick smoke test — fetch first 5 pages only, no Supabase needed |

### Social media

| Target | Description |
| --- | --- |
| `make bdqueimadas-social-assets` | Generate BDQueimadas carousel charts + manifest (no LLM required) |
| `make bdqueimadas-social-full` | Generate carousel + LLM captions — requires `GROQ_API_KEY` |

### Tests and cleanup

| Target | Description |
| --- | --- |
| `make test` | Run test suite |
| `make test-verbose` | Run tests with full output |
| `make clean` | Remove `.venv`, build artifacts, `__pycache__`, logs, and data cache |

---

## CLI reference

The `make` targets above are the primary interface. Each target calls `forest-pipelines` (the console entry point installed by `pyproject.toml`) under the hood. You can invoke commands directly when you need flags not exposed by the Makefile.

All commands accept `--config-path` (default: `configs/app.yml`).

### `sync`

Downloads a registered dataset, uploads artifacts to Storage, and publishes `manifest.json`.

```
forest-pipelines sync <dataset_id> [--latest-months N] [--config-path PATH]
```

`--latest-months` overrides the value in the dataset YAML. Not all runners use it.

### `build-report`

Builds and publishes a registered report package.

```
forest-pipelines build-report <report_id> [--config-path PATH]
```

Currently registered: `bdqueimadas_overview`.

### `audit-dataset`

Runs a registered audit and writes Markdown + JSON summaries under `docs/audits/`.

```
forest-pipelines audit-dataset <dataset_id> [--config-path PATH]
```

Currently registered: `inpe_bdqueimadas_focos`.

### `anp-catalog`

Scrapes the dados.gov.br CKAN API for one organization (default: ANP), paginates through all results, and extracts direct CSV download links. Writes `anp_catalogo_supabase.json` and `anp_catalogo_supabase.csv`. No Supabase credentials required.

```
forest-pipelines anp-catalog [--org-id UUID] [--offset-start N] [--limit N] [--output-dir PATH]
```

Use `--limit 5` for a smoke test without fetching all pages.

### `anp-compact`

Transforms a raw ANP portal snapshot JSON (produced by `anp-catalog`) into a compact canonical envelope with schema validation.

```
forest-pipelines anp-compact <input_json> [--output PATH] [--validate/--no-validate]
```

Output is a JSON envelope with `schema_version`, `generated_at`, and `datasets[]`. Pass `--no-validate` to skip JSON Schema validation.

### `anp-publish`

Uploads a compact ANP catalog envelope to Supabase Storage and prints the public catalog URL.

```
forest-pipelines anp-publish <compact_json> [--config-path PATH] [--bucket-prefix PREFIX] [--validate/--no-validate]
```

Default prefix: `anp/catalog`. Validates the envelope against JSON Schema v1 before upload unless `--no-validate` is passed.

---

## Registered datasets

| Identifier | Source |
| --- | --- |
| `cvm_fi_inf_diario` | CVM |
| `cvm_fi_doc_extrato` | CVM |
| `cvm_fi_cad_registro_fundo_classe` | CVM |
| `cvm_fi_cad_nao_adaptados_rcvm175` | CVM |
| `cvm_fi_cad_icvm555_hist` | CVM |
| `cvm_fii_doc_inf_trimestral` | CVM |
| `cvm_fi_doc_entrega` | CVM |
| `cvm_fii_doc_inf_mensal` | CVM |
| `cvm_fii_doc_inf_anual` | CVM |
| `eia_petroleum_weekly` | EIA |
| `eia_heating_oil_propane` | EIA |
| `eia_petroleum_monthly` | EIA |
| `inpe_bdqueimadas_focos` | INPE |
| `inpe_area_queimada_focos1km` | INPE |
| `inmet_dados_historicos` | INMET |
| `noticias_agricolas_news` | Noticias Agricolas |

`noticias_agricolas_news` publishes a JSON news feed (not file downloads) under `news/noticias-agricolas/` and does not use `--latest-months`. See [docs/datasets/noticias_agricolas_news.md](docs/datasets/noticias_agricolas_news.md) for contract details.

For a large historical CVM pull, use the backfill script:

```bash
python scripts/backfill_cvm_inf_diario.py
```

---

## Automation

`.github/workflows/weekly_sync.yml` runs `make sync-cvm` on Mondays at 12:00 UTC and on manual dispatch. Set repository secrets `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, and `SUPABASE_BUCKET_OPEN_DATA`.

---

## apps/ directory

`apps/social-post-templates/` is a static HTML template app for rendering the social carousel slides generated by the pipeline. It reads the chart specs and manifest produced by `make bdqueimadas-social-assets` / `make bdqueimadas-social-full` and renders them as browser-based slide previews. It is deployed independently from the Python pipeline.

---

## Module documentation

Per-module notes live under `docs/src/`, mirroring the `src/forest_pipelines/` tree. See [docs/INDEX.md](docs/INDEX.md) for the full index.

---

## Troubleshooting

**`check-env` fails.** Confirm `.env` is present, that your shell has sourced it (e.g., via `python-dotenv` or `export $(cat .env | xargs)`), and that the active virtual environment is the one where the package was installed.

**Upload errors / HTTP 401 or 403.** The bucket either does not exist, has the wrong name in `SUPABASE_BUCKET_OPEN_DATA`, or is not public. Verify in the Supabase dashboard.

**404 on `manifest.json`.** The sync may not have completed, or `bucket_prefix` in the dataset YAML does not match the requested path. Check logs under `logs/`.

**`anp-catalog` returns HTTP 401.** The dados.gov.br public API is rate-limiting or blocking the request. Try from a different network or confirm the API is accessible at [dados.gov.br](https://dados.gov.br).

**GitHub Actions failures.** Confirm all three secrets (`SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_BUCKET_OPEN_DATA`) are set on the repository under Settings → Secrets and variables → Actions.

To capture console output while running a sync:

```bash
forest-pipelines sync inpe_bdqueimadas_focos | tee logs/run_console.log
```
