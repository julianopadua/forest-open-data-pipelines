# AGENTS.md — forest-open-data-pipelines

## Purpose

CLI-driven data ingestion monorepo. Downloads open datasets from Brazilian and US government sources (INPE, CVM, EIA, INMET, ANP, Notícias Agrícolas), caches locally, uploads to Supabase Storage, publishes `manifest.json` envelopes for portal consumption, generates LLM-augmented report packages, and produces social media carousel assets.

---

## Manifest Envelope (schema 1.0)

Every dataset manifest emitted by the pipelines follows this shape. The builder
`build_manifest()` in `src/forest_pipelines/manifests/build_manifest.py` is the
sole producer — do not build manifests by hand.

```python
build_manifest(
    dataset_id, title, source_dataset_url, bucket_prefix,
    items,                         # list[OpenDataItem]
    meta={                         # dict | None — strict envelope
        "source_agency": "INPE - Programa Queimadas",   # optional
        "notes": "...",                                 # optional
        "metadata_file": { "filename": ..., "public_url": ..., ... },  # optional
        "release": { "last_release_iso": ..., "next_release_iso": ..., "week_ending": ... },
        "custom_tags": { "total_years": 25 },           # free-form
    },
    generation_status="success",   # "success" | "success_partial_fallback" | "failed"
    warnings=None,                 # list[str] — surfaced to the portal
)
```

Report manifests follow the same envelope contract (schema 1.0) in
`src/forest_pipelines/reports/publish/supabase.py`: `schema_version`,
`generation_status`, `warnings`, and a strict `meta` with `custom_tags` for
free-form data.

**When to set `success_partial_fallback`**: when the pipeline succeeded but did
not deliver 100% of the expected artifacts (some downloads failed but the rest
were published). Append human-readable strings to `warnings` so the portal can
surface them. Prefer partial success over `failed` when at least some items
were published and the manifest is internally consistent.

## Portal Catalog SSOT

The pipelines own the portal's dataset/reports catalogs. Two artifacts are
published to Supabase Storage (default prefix `catalog/`):

| Artifact | Built from | Command |
|----------|-----------|---------|
| `catalog/open_data_catalog.json` | `configs/catalog/open_data.yml` + ANP compact envelope | `forest-pipelines publish-catalog` |
| `catalog/reports_catalog.json` | `configs/catalog/reports.yml` | same |

The envelope format mirrors the manifest envelope: `schema_version`,
`generated_at`, `generation_status`, `warnings[]`, and the payload array
(`datasets[]` or `reports[]`). If the ANP compact file is not present when
publishing, the catalog is still produced with `generation_status =
success_partial_fallback` and a warning.

**Adding a new dataset to the portal requires two steps**:

1. Register a runner + YAML in `configs/datasets/…` (publishes the manifest).
2. Add an entry to `configs/catalog/open_data.yml` (publishes to the catalog).

Do NOT edit portal source to register a dataset — the portal fetches the
catalog at runtime.

## Architecture

```
External Source (HTTP / CKAN / FTP / API)
    │
    ▼
stream_download()               ← SHA256 hashing, streaming, retry
    │
    ▼
Local cache  (data/<dataset>/)  ← avoids redundant downloads
    │
    ▼
SupabaseStorage.upload_file()   ← upsert=True, 3 retries, exponential backoff
    │
    ▼
manifest.json upload            ← ALWAYS the final upload (atomicity guarantee)
    │
    ▼
Supabase Storage (public bucket)
    │
    ▼
forest-portal reads manifest → displays data
```

**Orchestration**: offline CLI (Typer). No web server. Scheduling is external (GitHub Actions for CVM weekly sync; everything else is manual or cron).

---

## Registry Pattern

Datasets, reports, and audits are registered by string ID. The CLI resolves IDs to runner functions at startup.


| Registry file                                      | Maps                          | Signature                                                            |
| -------------------------------------------------- | ----------------------------- | -------------------------------------------------------------------- |
| `src/forest_pipelines/registry/datasets.py`        | `dataset_id → sync()`         | `sync(settings, storage, logger, latest_months) → dict`              |
| `src/forest_pipelines/reports/registry/reports.py` | `report_id → build_package()` | `build_package(settings, storage, logger, current_year_only) → dict` |
| `src/forest_pipelines/audits/registry.py`          | `audit_id → run()`            | `run(settings, logger)`                                              |


Extending the system means: write a YAML config + a Python runner + a registry entry. Never extend the CLI itself for new datasets or reports.

---

## Adding a New Dataset

**Step 1** — Config (`configs/datasets/<source>/<id>.yml`):

```yaml
id: <source>_<category>_<name>      # e.g. cvm_fi_inf_diario
title: "Human-readable title"
source_url: "https://..."
bucket_prefix: <source>/<category>/<name>   # e.g. cvm/fi/inf_diario
# optional fields:
include_meta: false
latest_months: 12
```

**Step 2** — Runner (`src/forest_pipelines/datasets/<source>/<id>.py`):

```python
def sync(
    settings: Settings,
    storage: SupabaseStorage,
    logger: Logger,
    latest_months: int | None = None,
) -> dict:
    cfg = load_dataset_cfg(settings.datasets_dir, "<source>/<id>")
    items = []
    # ... download logic using stream_download() ...
    # ... upload each file using storage.upload_file() ...
    return build_manifest(cfg.id, cfg.title, cfg.source_url, cfg.bucket_prefix, items, meta={})
```

**Step 3** — Register:

```python
# src/forest_pipelines/registry/datasets.py
"<source>_<name>": sync_<name>,
```

**Step 4** — Makefile target:

```makefile
sync-<name>:
    forest-pipelines sync <source>_<name>
```

**Step 5** — Write at least one unit test for the parser/scraper (mock HTTP, no real network calls in tests).

---

## Adding a New Report

1. Create `configs/reports/<report_id>.yml` (see `bdqueimadas_overview.yml` for reference).
2. Create `src/forest_pipelines/reports/builders/<report_id>.py` with `build_package()`.
3. Register in `src/forest_pipelines/reports/registry/reports.py`.
4. If LLM-enabled, add prompts under `src/forest_pipelines/reports/llm/prompts/<report_id>/`.
5. Add a `make build-report-<report_id>` target.

---

## LLM Integration Guidelines

LLM is an optional enhancement, never a hard dependency. Every LLM path must have a working non-LLM fallback.

**Client contract**:

- Use `GroqClient` from `src/forest_pipelines/llm/groq_client.py`. Never call the `groq` SDK directly.
- `generate_text()` for prose; `generate_json()` for structured output (handles fenced code blocks, validates required keys).
- All calls are `async`. Use `asyncio.run()` at the outermost synchronous boundary.

**Configuration**:

- Model names come from `settings.llm.preferred_models` list. Never hardcode model string literals in source code.
- Respect `cfg.llm.enabled` flag. Check it before any LLM call.
- Default params: `temperature=0.2`, `max_completion_tokens=700`, `stream=False`.

**Safety**:

- Do not log raw LLM responses — they may contain sensitive content from source documents.
- Do not pass user-supplied strings directly into LLM prompts without sanitization.
- LLM outputs used in public reports must be deterministically reproducible (low temperature + fixed prompt version).

**Groq model routing**:

```yaml
# configs/app.yml
llm:
  preferred_models:
    - openai/gpt-oss-20b
    - llama-3.3-70b-versatile
    - qwen/qwen3-32b
```

The router tries models in order until one succeeds.

---

## Config Conventions


| Convention      | Rule                                                               |
| --------------- | ------------------------------------------------------------------ |
| Format          | YAML only (`.yml`), no JSON configs                                |
| Secrets         | Never in YAML — use `api_key_env: ENV_VAR_NAME` pattern            |
| Dataset IDs     | `<source>_<category>_<name>` (snake_case, no hyphens)              |
| Bucket prefixes | `<source>/<category>/<name>` (forward slashes, no leading slash)   |
| Config loading  | Always via `load_settings()` — never read YAML directly in runners |


---

## Storage Operations

Use `SupabaseStorage` methods exclusively. Never use the raw Supabase client for storage operations.

```python
# Upload a file from disk
storage.upload_file(object_path, str(local_path), content_type, upsert=True)

# Upload bytes directly (manifests, small JSON)
storage.upload_bytes(object_path, data_bytes, content_type, upsert=True)

# Get public URL (deterministic, no expiry)
url = storage.public_url(object_path)
```

**Manifest atomicity**: the manifest upload is always the last operation. If any file upload fails, the manifest must not be written. The CLI enforces this — runners return the manifest dict and the CLI uploads it after all runner work completes.

---

## HTTP and Download Guidelines

- Use `stream_download()` from `src/forest_pipelines/http.py` for all file downloads. It streams to disk, computes SHA256, and retries on transient errors.
- All downloads go under `settings.data_dir`. Never download to `/tmp`, `.` or any hardcoded path.
- Add `time.sleep(1)` between paginated scraping requests to respect source rate limits.
- Validate `Content-Type` before processing responses as a specific format.
- Respect redirects but set a max redirect limit in `requests.get(allow_redirects=True, max_redirects=5)`.

---

## Testing Requirements

Every contribution must satisfy:

- New parsers and scrapers have unit tests with mocked `requests` responses.
- New data transformations have unit tests with representative sample inputs.
- LLM calls in tests are always mocked (`unittest.mock.MagicMock`). Real API calls are forbidden in the test suite.
- Storage calls in tests are mocked. No real Supabase writes in tests.
- `make test` passes with zero failures before any commit.

**Test file conventions**:

- `tests/test_<module_name>.py` (mirrors `src/` structure)
- Use `pytest.fixture` for shared setup. No global mutable state in tests.
- Each test function tests exactly one behavior. Name tests as `test_<what>_<when>_<expected>`.

---

## Code Style

- Python 3.11+. Use `match` statements, structural pattern matching, and `|` union types.
- Pydantic v2 for all external data models. Frozen dataclasses for internal settings.
- Full type annotations on all public functions and class attributes.
- `rich` for CLI output. `logging_` module (`src/forest_pipelines/logging_.py`) for file logging.
- No `print()` in library code. Pass and use the `logger` argument.
- No global mutable state. Pass `settings`, `storage`, and `logger` as explicit arguments.
- Keep runner functions free of I/O side effects outside of the designated `data_dir` and Supabase Storage paths.

---

## Security


| Concern                     | Rule                                                                                              |
| --------------------------- | ------------------------------------------------------------------------------------------------- |
| `SUPABASE_SERVICE_ROLE_KEY` | Load from env only. Never log, never pass to external services.                                   |
| `GROQ_API_KEY`              | Load from env only. Reference via `settings.llm.api_key_env`.                                     |
| SHA256 hashes               | Always compute and include in manifest items. Consumers may verify integrity.                     |
| Scraping targets            | Set request timeouts (`timeout=30`). Validate response content-type. Treat all HTML as untrusted. |
| Dependency updates          | Run `uv lock --upgrade` periodically. Pin major versions in `pyproject.toml`.                     |


---

## Common Pitfalls


| Pitfall                         | Consequence                                                 | Fix                                      |
| ------------------------------- | ----------------------------------------------------------- | ---------------------------------------- |
| Uploading manifest before files | Portal shows broken dataset (valid manifest, missing files) | Upload manifest last, always             |
| Missing `upsert=True`           | Upload fails if object already exists                       | Always pass `upsert=True`                |
| Hardcoded year ranges           | Misses new data, fails on year rollover                     | Discover years by scraping source URLs   |
| Hardcoded model names           | Breaks when Groq retires a model                            | Use `settings.llm.preferred_models` list |
| Real network calls in tests     | Flaky CI, slow tests                                        | Mock all HTTP with `unittest.mock`       |
| `print()` in library code       | Logs bypass the rotating file logger                        | Use the `logger` argument                |


---

## CLI Reference

```bash
# Dataset sync
forest-pipelines sync <dataset_id> [--latest-months N] [--config PATH]

# Report generation
forest-pipelines build-report <report_id> [--current-year-only]

# Dataset audit (local, no Storage writes)
forest-pipelines audit-dataset <dataset_id>

# Portal catalog (SSOT) — publishes open_data_catalog.json + reports_catalog.json
forest-pipelines publish-catalog [--bucket-prefix catalog] [--anp-compact PATH]

# ANP open-data catalog
forest-pipelines anp-catalog [--limit N]
forest-pipelines anp-compact <input.json>
forest-pipelines anp-publish <compact.json>
```

**Make targets** (preferred for routine operations):

```bash
make sync-cvm
make sync-inpe
make sync-eia
make sync-inmet
make sync-news
make sync-all
make build-report-bdqueimadas
make audit-bdqueimadas
make anp-catalog
make bdqueimadas-social-full   # With LLM captions
make test
make clean
```

