# forest-open-data-pipelines

Python pipelines for ingesting open datasets, mirroring artifacts to public Supabase Storage, and publishing `manifest.json` documents for direct consumption by a static frontend. The repository is decoupled from the `forest-portal` frontend so that Python dependencies, scheduled execution (cron, GitHub Actions, or a VM), and local caches remain isolated from site deployment.

## Table of contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Repository layout](#repository-layout)
4. [Prerequisites](#prerequisites)
5. [Installation](#installation)
6. [Configuration](#configuration)
7. [Command-line interface](#command-line-interface)
8. [Registered datasets](#registered-datasets)
9. [Reports and audits](#reports-and-audits)
10. [Outputs](#outputs)
11. [Downstream consumption](#downstream-consumption)
12. [Automation](#automation)
13. [Module documentation (`docs/src`)](#module-documentation-docssrc)
14. [Troubleshooting](#troubleshooting)

## Overview

The system performs three classes of tasks.

**Dataset synchronization.** For a given dataset identifier, the pipeline resolves upstream sources (often monthly archives or APIs), downloads files into a local cache under `data/`, uploads objects to a configurable prefix inside a public Supabase bucket, and writes a JSON manifest describing items and metadata. The manifest is uploaded to the same prefix as `manifest.json`.

**Report builds.** Selected reports aggregate or transform data (for example, fire overview content) and publish structured packages to Storage, including manifests and public URLs suitable for embedding or linking from the portal.

**Dataset audits.** Optional audit commands compare local or documented expectations against upstream behavior and emit Markdown and JSON summaries under `docs/` for traceability.

The frontend may fetch the public manifest URL without a dedicated backend for file delivery, provided the bucket remains public and CORS policies allow browser access.

## Architecture

The data flow is: upstream open data source, Python pipeline (Typer CLI), Supabase Storage (public bucket), optional LLM configuration (Groq) for report-related features, and the `forest-portal` frontend reading published JSON.

## Repository layout

| Path | Role |
| --- | --- |
| `src/forest_pipelines/` | Package root: CLI, settings, dataset runners, storage client, manifests, reports, audits, utilities. |
| `configs/app.yml` | Application paths (`data_dir`, `logs_dir`, `docs_dir`), Supabase bucket env var name, LLM defaults. |
| `configs/datasets/` | One YAML file per dataset (mirrors registry identifiers). |
| `configs/reports/` | Report definitions consumed by `build-report`. |
| `data/` | Local download cache (typically gitignored). |
| `logs/` | Rotated run logs (typically gitignored). |
| `docs/` | Generated audit outputs, human-readable indexes, and `docs/src/` per-module notes. |
| `scripts/` | Optional helpers (for example, long backfill for CVM daily information). |
| `.github/workflows/` | Scheduled or manual GitHub Actions workflows. |

## Prerequisites

- Python 3.11 or newer
- Git
- A Supabase project with Storage enabled
- The Supabase service role key available only via environment variables or CI secrets (never committed)

## Installation

Create and activate a virtual environment, then install the package in editable mode from the repository root.

```bash
python -m venv .venv
```

Windows (PowerShell):

```powershell
.venv\Scripts\Activate.ps1
```

Linux or macOS:

```bash
source .venv/bin/activate
```

```bash
python -m pip install --upgrade pip
pip install -e .
```

Optional test dependencies:

```bash
pip install -e ".[dev]"
python -m pytest
```

The console entry point is `forest-pipelines` (see `pyproject.toml`).

## Configuration

### Environment variables

Copy `.env.example` to `.env` and set at least:

| Variable | Purpose |
| --- | --- |
| `SUPABASE_URL` | Project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key for Storage uploads (secret) |
| `SUPABASE_BUCKET_OPEN_DATA` | Bucket name (for example `open-data`) |
| `GROQ_API_KEY` | Required when report or LLM features call Groq (see `configs/app.yml`) |

The bucket must exist and be marked public if unauthenticated reads are required for manifests and objects.

### Application YAML

`configs/app.yml` defines directory names relative to the repository root, the environment variable name used to resolve the bucket (`supabase.bucket_open_data_env`), paths to dataset and report configuration directories, and default LLM parameters. Individual dataset YAML files under `configs/datasets/` supply source URLs, `bucket_prefix`, and sync parameters such as `latest_months`.

## Command-line interface

All commands accept `--config-path` (default `configs/app.yml`).

### `sync`

Synchronizes one registered dataset, uploads artifacts, and publishes `manifest.json` at the dataset prefix.

```bash
forest-pipelines sync <dataset_id> [--latest-months N]
```

If `--latest-months` is omitted, the value from the dataset YAML is used.

### `build-report`

Builds and publishes a registered report package to Storage.

```bash
forest-pipelines build-report <report_id>
```

### `audit-dataset`

Runs a registered audit for a dataset identifier and writes outputs under `docs/` (paths are logged on completion).

```bash
forest-pipelines audit-dataset <dataset_id>
```

## Registered datasets

The following identifiers are registered in `src/forest_pipelines/registry/datasets.py`.

| Identifier |
| --- |
| `cvm_fi_inf_diario` |
| `cvm_fi_doc_extrato` |
| `cvm_fi_cad_registro_fundo_classe` |
| `cvm_fi_cad_nao_adaptados_rcvm175` |
| `cvm_fi_cad_icvm555_hist` |
| `cvm_fii_doc_inf_trimestral` |
| `cvm_fi_doc_entrega` |
| `cvm_fii_doc_inf_mensal` |
| `cvm_fii_doc_inf_anual` |
| `eia_petroleum_weekly` |
| `eia_heating_oil_propane` |
| `eia_petroleum_monthly` |
| `inpe_bdqueimadas_focos` |
| `inpe_area_queimada_focos1km` |
| `inmet_dados_historicos` |
| `noticias_agricolas_news` |

The `noticias_agricolas_news` dataset publishes a JSON news feed (not file downloads) under `news/noticias-agricolas/`, including timestamped snapshots. See [docs/datasets/noticias_agricolas_news.md](docs/datasets/noticias_agricolas_news.md) for the contract, validation rules, and limitations. The `--latest-months` flag does not apply to this dataset.

Example:

```bash
forest-pipelines sync cvm_fi_inf_diario --latest-months 12
```

### Optional backfill script

For a large historical pull of CVM daily information, the repository provides `scripts/backfill_cvm_inf_diario.py`, which invokes `cvm_fi_inf_diario` with an extended `latest_months` window and publishes the manifest. Run from the repository root with the virtual environment activated:

```bash
python scripts/backfill_cvm_inf_diario.py
```

`scripts/run_local.sh` illustrates a local venv setup and a sample `sync` invocation on Unix-like systems.

## Reports and audits

**Reports.** `src/forest_pipelines/reports/registry/reports.py` currently registers `bdqueimadas_overview`, driven by `configs/reports/bdqueimadas_overview.yml`.

**Audits.** `src/forest_pipelines/audits/registry.py` registers `inpe_bdqueimadas_focos`. Audit artifacts may include material under `docs/audits/`.

## Outputs

After a successful `sync`, objects appear under the configured bucket prefix, including `manifest.json`. Public URLs follow the pattern:

`<SUPABASE_URL>/storage/v1/object/public/<bucket>/<object_path>`

Log files are written under `logs/`, with subpaths that reflect the dataset, report, or audit identifier.

## Downstream consumption

The `forest-portal` application may request the public `manifest.json`, render `items` and metadata, and use `public_url` fields for direct downloads when the bucket is public. This pattern avoids a separate download API on the Next.js deployment.

## Automation

The workflow `.github/workflows/weekly_sync.yml` installs the package and runs `forest-pipelines sync cvm_fi_inf_diario --latest-months 12` once per week (Mondays 12:00 UTC) and on manual dispatch. Configure repository secrets `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY`, and ensure `SUPABASE_BUCKET_OPEN_DATA` is set in the workflow environment if the bucket name differs from the default.

## Module documentation (`docs/src`)

Longer-form notes for selected modules live under `docs/src/`, organized in a tree that mirrors `src/forest_pipelines/`. The file [`docs/INDEX.md`](docs/INDEX.md) lists the correspondence between source files and Markdown paths. The table below repeats those links for convenience; some narrative files may lag the code and are intended to be refreshed alongside implementation changes.

| Source (under `src/forest_pipelines/`) | Documentation |
| --- | --- |
| `__init__.py` | [`docs/src/__init__/__init__.md`](docs/src/__init__/__init__.md) |
| `cli.py` | [`docs/src/cli/cli.md`](docs/src/cli/cli.md) |
| `http.py` | [`docs/src/http/http.md`](docs/src/http/http.md) |
| `logging_.py` | [`docs/src/logging_/logging_.md`](docs/src/logging_/logging_.md) |
| `settings.py` | [`docs/src/settings/settings.md`](docs/src/settings/settings.md) |
| `datasets/__init__.py` | [`docs/src/datasets/__init__/__init__.md`](docs/src/datasets/__init__/__init__.md) |
| `datasets/cvm/__init__.py` | [`docs/src/datasets/cvm/__init__/__init__.md`](docs/src/datasets/cvm/__init__/__init__.md) |
| `datasets/cvm/fi_cad_icvm555_hist.py` | [`docs/src/datasets/cvm/fi_cad_icvm555_hist/fi_cad_icvm555_hist.md`](docs/src/datasets/cvm/fi_cad_icvm555_hist/fi_cad_icvm555_hist.md) |
| `datasets/cvm/fi_cad_nao_adaptados_rcvm175.py` | [`docs/src/datasets/cvm/fi_cad_nao_adaptados_rcvm175/fi_cad_nao_adaptados_rcvm175.md`](docs/src/datasets/cvm/fi_cad_nao_adaptados_rcvm175/fi_cad_nao_adaptados_rcvm175.md) |
| `datasets/cvm/fi_cad_registro_fundo_classe.py` | [`docs/src/datasets/cvm/fi_cad_registro_fundo_classe/fi_cad_registro_fundo_classe.md`](docs/src/datasets/cvm/fi_cad_registro_fundo_classe/fi_cad_registro_fundo_classe.md) |
| `datasets/cvm/fi_doc_extrato.py` | [`docs/src/datasets/cvm/fi_doc_extrato/fi_doc_extrato.md`](docs/src/datasets/cvm/fi_doc_extrato/fi_doc_extrato.md) |
| `datasets/cvm/fi_inf_diario.py` | [`docs/src/datasets/cvm/fi_inf_diario/fi_inf_diario.md`](docs/src/datasets/cvm/fi_inf_diario/fi_inf_diario.md) |
| `datasets/cvm/fii_doc_inf_trimestral.py` | [`docs/src/datasets/cvm/fii_doc_inf_trimestral/fii_doc_inf_trimestral.md`](docs/src/datasets/cvm/fii_doc_inf_trimestral/fii_doc_inf_trimestral.md) |
| `manifests/__init__.py` | [`docs/src/manifests/__init__/__init__.md`](docs/src/manifests/__init__/__init__.md) |
| `manifests/build_manifest.py` | [`docs/src/manifests/build_manifest/build_manifest.md`](docs/src/manifests/build_manifest/build_manifest.md) |
| `registry/__init__.py` | [`docs/src/registry/__init__/__init__.md`](docs/src/registry/__init__/__init__.md) |
| `registry/datasets.py` | [`docs/src/registry/datasets/datasets.md`](docs/src/registry/datasets/datasets.md) |
| `storage/__init__.py` | [`docs/src/storage/__init__/__init__.md`](docs/src/storage/__init__/__init__.md) |
| `storage/supabase_storage.py` | [`docs/src/storage/supabase_storage/supabase_storage.md`](docs/src/storage/supabase_storage/supabase_storage.md) |
| `utils/__init__.py` | [`docs/src/utils/__init__/__init__.md`](docs/src/utils/__init__/__init__.md) |
| `utils/dates.py` | [`docs/src/utils/dates/dates.md`](docs/src/utils/dates/dates.md) |
| `utils/hashing.py` | [`docs/src/utils/hashing/hashing.md`](docs/src/utils/hashing/hashing.md) |

Additional subpackages (for example `datasets/eia/`, `datasets/inpe/`, `datasets/inmet/`, `reports/`, `audits/`, `llm/`) do not yet have matching files under `docs/src/`; [`docs/INDEX.md`](docs/INDEX.md) should be extended when those notes are added.

## Troubleshooting

**Missing environment variables.** The Storage client requires `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY`. Confirm `.env` is loaded and the active shell uses the intended virtual environment.

**Bucket missing or not public.** Upload errors or HTTP 401 or 403 on public URLs usually indicate a missing bucket, incorrect bucket name, or a bucket that is not public.

**404 on `manifest.json`.** Verify that the sync completed, that `bucket_prefix` in the dataset YAML matches the requested URL, and that `SUPABASE_BUCKET_OPEN_DATA` points to the correct bucket.

**GitHub Actions.** Confirm secrets are set on the repository and that workflow files live under `.github/workflows/`.

To duplicate console output to a file while syncing (Unix-like):

```bash
forest-pipelines sync cvm_fi_inf_diario --latest-months 12 | tee logs/run_console.log
```

Windows PowerShell:

```powershell
forest-pipelines sync cvm_fi_inf_diario --latest-months 12 | Tee-Object -FilePath logs\run_console.log
```
