# forest-open-data-pipelines

Python monorepo that discovers official open-data source URLs, profiles each resource locally, publishes catalog and manifest JSON to Supabase Storage, and powers the [Instituto Forest](https://institutoforest.org) portal, the public read-only HTTP API at `https://institutoforest.org/api/v1`, and the [`forest-data`](#using-the-data-the-forest-data-python-sdk) Python SDK on PyPI.

> **First time here?** Most users do **not** need to run this repo at all. If you just want to **consume Forest open data**, install the SDK. See [Using the data: the `forest-data` Python SDK](#using-the-data-the-forest-data-python-sdk) below. The instructions further down are for contributors who maintain or extend the pipelines.

---

## Table of contents

- [What this repo does](#what-this-repo-does)
- [Using the data: the `forest-data` Python SDK](#using-the-data-the-forest-data-python-sdk)
- [Public HTTP API](#public-http-api)
- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [Quick start (contributors)](#quick-start-contributors)
- [Configuration](#configuration)
- [Available make targets](#available-make-targets)
- [CLI reference](#cli-reference)
- [Registered datasets](#registered-datasets)
- [Portal catalog (single source of truth)](#portal-catalog-single-source-of-truth)
- [Reports](#reports)
- [Social media generation](#social-media-generation)
- [Automation](#automation)
- [apps/ directory](#apps-directory)
- [Module documentation](#module-documentation)
- [Troubleshooting](#troubleshooting)
- [Release status](#release-status)
- [Contributing](#contributing)
- [License and data](#license-and-data)

---

## What this repo does

**Dataset sync.** Discovers official source URLs for open datasets (CVM, INPE, EIA, INMET, MMA/CNUC, Noticias Agricolas, ANP), downloads resources temporarily for local profiling, deletes the temporary payloads by default, and publishes a `manifest.json` at each dataset prefix in Supabase Storage. Dataset payload bytes are **not** uploaded to Supabase. Consumers fetch them from the official source listed in each item's `source_url`.

Incremental by default: before profiling, the pipeline reads the existing published `manifest.json`. URLs already profiled are reused; new URLs are profiled and added. Use `make sync-force` to reprofile everything.

**Portal catalog.** Builds and publishes `catalog/open_data_catalog.json` for the portal and public HTTP API, plus `catalog/reports_catalog.json` for portal report pages.

**Reports.** Aggregates datasets into structured report packages (e.g., BDQueimadas fire overview) and publishes them as report JSON with their own manifest, ready to embed in the portal.

**Social media generation.** Generates Instagram carousel charts (Vega-Lite specs) and optional LLM-written captions (via Groq) from INPE fire data.

---

## Using the data: the `forest-data` Python SDK

The recommended way to consume Forest open data is the official Python SDK. It wraps the public HTTP API, returns typed manifests, exposes every official `source_url`, and can download the source bytes directly to disk with optional `sha256` verification.

### Install

```bash
pip install forest-data
```

### Quick example

```python
import forest_data

client = forest_data.Client()

for dataset in client.list_datasets():
    print(dataset.id, dataset.title)

manifest = client.get_dataset("inpe_bdqueimadas_focos")
for item in manifest.items:
    print(item.period, item.source_url, item.profile_status, item.row_count)

# Download every item from its official source URL
paths = client.download("inpe_bdqueimadas_focos", path="./data")
```

### SDK documentation

- **PyPI package**: <https://pypi.org/project/forest-data/>
- **SDK README** (in this repo): [sdk/forest_data/README.md](sdk/forest_data/README.md)
- **HTTP API reference** (what the SDK wraps): <https://institutoforest.org/docs/api/v1>
- **Source**: [sdk/forest_data/](sdk/forest_data/)

The SDK lives under `sdk/forest_data/` with its own `pyproject.toml`. It is a self-contained package that shares no runtime code with the pipeline, only the manifest schema. Co-locating both lets schema changes ship as a single atomic PR across pipeline, portal, and SDK.

---

## Public HTTP API

The portal exposes a public, read-only REST API at `https://institutoforest.org/api/v1`. Every route the SDK calls is also reachable directly:

| Route | Returns |
|------|---------|
| `GET /api/v1/health` | Service + schema version probe |
| `GET /api/v1/catalog` | Compact dataset list |
| `GET /api/v1/datasets/{id}` | Full dataset manifest |
| `GET /api/v1/datasets/{id}/items` | Items array only |
| `GET /api/v1/sources` | Source agencies + counts |
| `GET /api/v1/openapi.json` | OpenAPI 3.1 spec |

The API is metadata-only. It never serves dataset payload bytes. Hit each item's `source_url` for the raw file. See the human-readable reference at <https://institutoforest.org/docs/api/v1>.

---

## Architecture

```
Upstream sources                 forest-open-data-pipelines        Supabase Storage              Consumers
─────────────────                ───────────────────────────       ────────────────              ─────────
CVM (CVM Portal)    ─────┐
INPE (BDQueimadas)  ─────┤
EIA (API)           ─────┤                                                                       forest-portal
INMET (FTP/HTTP)    ─────┼──► forest-pipelines CLI ───────►    public bucket                ┌──► (Next.js)
MMA / CNUC (CKAN)   ─────┤        (Typer)                       manifest.json      ────────┤
ANP (gov.br HTML)   ─────┤                                      + catalog envelopes         │   public HTTP API
Noticias Agricolas  ─────┘                                                                  ├──► /api/v1
                                       │                                                    │
                                Groq LLM (optional)                                         └──► forest-data SDK
                                for social captions                                              (Python, on PyPI)
```

The portal and API fetch manifests and catalog envelopes from Storage. Dataset bytes are fetched by users and SDK clients from each item's `source_url`, not from Supabase.

---

## Repository layout

| Path | Role |
| --- | --- |
| `src/forest_pipelines/` | CLI, settings, dataset runners, storage client, manifests, reports, audits, LLM, social generation. |
| `src/forest_pipelines/datasets/anp/` | ANP gov.br open-data discovery and manifest publication. |
| `sdk/forest_data/` | Public Python SDK published to PyPI as `forest-data`. Independent package. |
| `configs/app.yml` | Directory paths, Supabase bucket env var name, LLM defaults. |
| `configs/datasets/` | One YAML per dataset (landing-page source URLs, `bucket_prefix`, sync parameters). |
| `configs/reports/` | Report definitions consumed by `build-report`. |
| `configs/catalog/` | Catalog SSOT: `open_data.yml` (dataset list) and `reports.yml` (report list). |
| `data/` | Local working directory for selected workflows (gitignored). |
| `logs/` | Rotated run logs (gitignored). |
| `docs/` | Audit outputs and per-module notes. |
| `apps/social-post-templates/` | Static frontend for rendering social carousel slides. |
| `scripts/` | Operational helpers. |
| `.github/workflows/` | Scheduled and manual GitHub Actions workflows. |

---

## Quick start (contributors)

```bash
git clone <repo-url> forest-open-data-pipelines
cd forest-open-data-pipelines
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\Activate.ps1
make dev
cp .env.example .env   # then fill in credentials
make check-env
make sync
```

`make dev` installs the package and dev dependencies in editable mode. `make check-env` confirms all required environment variables are present before you run anything.

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
| `SUPABASE_SERVICE_ROLE_KEY` | yes | Service role key for Storage uploads. Keep secret |
| `SUPABASE_BUCKET_OPEN_DATA` | yes | Bucket name, e.g. `open-data` |
| `GROQ_API_KEY` | for LLM targets only | Required by `bdqueimadas-social-full`, BDQueimadas report LLM mode, and any report using LLM captions |

Run `make check-env` after editing `.env` to verify all required variables are present.

### Application YAML

`configs/app.yml` controls directory names relative to the repo root, the env var name used to resolve the bucket, and default LLM parameters. Individual dataset YAMLs under `configs/datasets/` supply landing-page source URLs, `bucket_prefix`, and parameters such as `latest_months`. The catalog SSOT lives under `configs/catalog/`.

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
| `make sync` | Sync every registered dataset incrementally, then publish the catalog |
| `make sync-force` | Sync every registered dataset and reprofile every source URL |
| `make sync-cvm` | Sync CVM daily fund information (last 12 months) |
| `make sync-inpe` | Sync INPE BDQueimadas fire focus data |
| `make sync-inpe-boletins` | Sync INPE BDQueimadas integrated bulletins |
| `make sync-eia` | Sync EIA weekly petroleum data |
| `make sync-inmet` | Sync INMET historical weather data |
| `make sync-mma-cnuc` | Sync MMA CNUC Unidades de Conservação (CKAN URL catalog) |
| `make sync-news` | Sync Noticias Agricolas news feed |
| `make sync-all` | Alias for `make sync` |

### Reports

| Target | Description |
| --- | --- |
| `make build-report-bdqueimadas` | Build and publish BDQueimadas fire overview (interactive prompts) |
| `make build-report-bdqueimadas-force` | Same, overwriting without prompt |
| `make build-report-bdqueimadas-no-llm` | Current-year scope, deterministic fallback (no LLM) |
| `make build-report-bdqueimadas-force-no-llm` | Full history, no LLM |
| `make build-report-bdqueimadas-refresh` | Refresh mensal CSVs, use previous reference month |

### Audits

| Target | Description |
| --- | --- |
| `make audit-bdqueimadas` | Run audit on INPE BDQueimadas dataset; output under `docs/audits/` |

### Portal catalog

| Target | Description |
| --- | --- |
| `make publish-catalog` | Build and upload `catalog/open_data_catalog.json` + `catalog/reports_catalog.json` |

### Social media

| Target | Description |
| --- | --- |
| `make bdqueimadas-social-assets` | Generate BDQueimadas carousel charts + manifest (no LLM required) |
| `make bdqueimadas-social-full` | Generate carousel + LLM captions. Requires `GROQ_API_KEY` |

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

Scrapes a registered dataset, profiles official source URLs locally, and publishes `manifest.json`.

```
forest-pipelines sync <dataset_id> [--latest-months N] [--force] [--config-path PATH]
```

`--latest-months` overrides the value in the dataset YAML (not all runners use it). `--force` reprofiles every source URL even when an existing manifest is present.

### `sync-all`

Syncs every dataset registered in `configs/catalog/open_data.yml` and (by default) publishes the catalog envelopes at the end.

```
forest-pipelines sync-all [--force] [--publish-catalog/--no-publish-catalog]
```

### `publish-catalog`

Rebuilds and uploads `catalog/open_data_catalog.json` and `catalog/reports_catalog.json` without re-running any dataset sync. Use this after editing `configs/catalog/*.yml`.

```
forest-pipelines publish-catalog [--bucket-prefix catalog]
```

### `build-report`

Builds and publishes a registered report package.

```
forest-pipelines build-report <report_id> [--scope current|full] [--llm|--no-llm] [--force] [--config-path PATH]
```

Currently registered: `bdqueimadas_overview`.

### `audit-dataset`

Runs a registered audit and writes Markdown + JSON summaries under `docs/audits/`.

```
forest-pipelines audit-dataset <dataset_id> [--config-path PATH]
```

Currently registered: `inpe_bdqueimadas_focos`.

## Registered datasets

| Identifier | Source |
| --- | --- |
| `cvm_fi_inf_diario` | CVM |
| `cvm_fi_doc_extrato` | CVM |
| `cvm_fi_cad` | CVM |
| `cvm_fii_doc_inf_trimestral` | CVM |
| `cvm_fi_doc_entrega` | CVM |
| `cvm_fii_doc_inf_mensal` | CVM |
| `cvm_fii_doc_inf_anual` | CVM |
| `eia_petroleum_weekly` | EIA |
| `eia_heating_oil_propane` | EIA |
| `eia_petroleum_monthly` | EIA |
| `inpe_bdqueimadas_focos` | INPE |
| `inpe_bdqueimadas_boletins_integrados` | INPE |
| `inpe_bdqueimadas_painel_fogo` | INPE |
| `inpe_area_queimada_focos1km` | INPE |
| `inmet_dados_historicos` | INMET |
| `mma_cnuc_unidades_conservacao` | MMA / CNUC |
| `anp_*` | ANP |
| `noticias_agricolas_news` | Noticias Agricolas |

`noticias_agricolas_news` publishes a JSON news feed (not file downloads) under `news/noticias-agricolas/` and does not use `--latest-months`. See [docs/datasets/noticias_agricolas_news.md](docs/datasets/noticias_agricolas_news.md) for contract details.

---

## Portal catalog (single source of truth)

The portal and the public HTTP API both read two catalog envelopes from Supabase Storage:

- `catalog/open_data_catalog.json`: built from `configs/catalog/open_data.yml`.
- `catalog/reports_catalog.json`: built from `configs/catalog/reports.yml`.

Both envelopes share the manifest envelope shape: `schema_version`, `generated_at`, `generation_status`, `warnings[]`, plus the payload array.

**Adding a new dataset to the portal requires two steps**:

1. Register a runner + YAML in `configs/datasets/<source>/<id>.yml` (publishes the manifest).
2. Add an entry to `configs/catalog/open_data.yml` (publishes to the catalog).

Then run `make sync` (or `forest-pipelines publish-catalog` if only metadata changed). Do **not** edit the portal source. The portal fetches the catalog at runtime.

---

## Reports

Reports are described in `configs/reports/<report_id>.yml` and built by `forest-pipelines build-report <report_id>`. Each report publishes its own `manifest.json` plus report JSON to Storage, and is listed in `catalog/reports_catalog.json`. See `bdqueimadas_overview` for a complete reference implementation, including the optional Groq LLM path with a deterministic fallback.

---

## Social media generation

`forest_pipelines.social` generates Instagram carousel charts and (optionally) LLM-written captions from INPE fire data. Output:

- Vega-Lite chart specs.
- A `manifest.json` describing the carousel.
- Optional caption text generated through Groq.

The static rendering frontend lives in [apps/social-post-templates/](apps/social-post-templates/) and consumes those outputs.

---

## Automation

`.github/workflows/weekly_sync.yml` runs `make sync-cvm` on Mondays at 12:00 UTC and on manual dispatch. Set repository secrets `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, and `SUPABASE_BUCKET_OPEN_DATA`.

---

## apps/ directory

`apps/social-post-templates/` is a static HTML template app for rendering the social carousel slides generated by the pipeline. It reads the chart specs and manifest produced by `make bdqueimadas-social-assets` / `make bdqueimadas-social-full` and renders them as browser-based slide previews. It is deployed independently from the Python pipeline.

---

## Module documentation

The `docs/` directory contains active operational notes for datasets, integrations, and the social LLM flow. See also [AGENTS.md](AGENTS.md) for the full contributor contract (manifest envelope, registry pattern, LLM rules, security, common pitfalls).

---

## Troubleshooting

**`check-env` fails.** Confirm `.env` is present, that your shell has sourced it (e.g., via `python-dotenv` or `export $(cat .env | xargs)`), and that the active virtual environment is the one where the package was installed.

**Upload errors / HTTP 401 or 403.** The bucket either does not exist, has the wrong name in `SUPABASE_BUCKET_OPEN_DATA`, or is not public. Verify in the Supabase dashboard.

**404 on `manifest.json`.** The sync may not have completed, or `bucket_prefix` in the dataset YAML does not match the requested path. Check logs under `logs/`.

**Catalog out of date in the portal.** Run `make publish-catalog`. The portal caches the catalog for up to one hour.

**ANP gov.br scraping returns no resources.** Check whether the detail page changed its article structure or resource section labels. The runner uses fixtures for CI and keeps official indirect links as skipped items when a public system page has no direct file URL.

**GitHub Actions failures.** Confirm all three secrets (`SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_BUCKET_OPEN_DATA`) are set on the repository under Settings → Secrets and variables → Actions.

To capture console output while running a sync:

```bash
forest-pipelines sync inpe_bdqueimadas_focos | tee logs/run_console.log
```

---

## Release status

`v0.1.0-alpha` is the first public alpha target. Contracts, documentation, dataset coverage, and SDK behavior may evolve while the project stabilizes.

---

## Contributing

Contributions are described in [CONTRIBUTING.md](CONTRIBUTING.md). Sensitive security reports should follow [SECURITY.md](SECURITY.md).

---

## License and data

Source code is licensed under the [MIT License](LICENSE). Data, content, metadata, reports, and third-party dataset terms are described in [DATA-LICENSING.md](DATA-LICENSING.md).
