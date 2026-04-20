# Datasets Ingestion

## Scope
Review of `src/forest_pipelines/datasets` and related dataset configs under `configs/datasets`.

## Strengths
- Registry-based runner model via `src/forest_pipelines/registry/datasets.py` keeps orchestration simple.
- Consistent manifest publication pattern through `src/forest_pipelines/manifests/build_manifest.py`.
- `noticias_agricolas` pipeline shows stronger resilience and validation (`sync.py`, `http_client.py`, `validation.py`).

## Findings
### Critical
- None identified from current code sample.

### High
- Duplicated patterns across CVM modules increase maintenance cost and regression risk (`src/forest_pipelines/datasets/cvm/*.py`).
- Inconsistent retry/fault handling across families (CVM/INPE/INMET/EIA).
- Sparse tests for dataset `sync()` behavior and edge-case source drift.

### Medium
- Config drift risk from keys that exist in YAML but are not consistently consumed.
- Incremental freshness logic is uneven (some modules always re-download/re-upload).

### Low
- Repeated boilerplate around URL extraction and upload loops reduces readability.

## Holes / Incomplete Areas
- No typed shared schema validator for dataset YAMLs.
- No standard idempotency contract for `sync` outputs across modules.
- No centralized data-quality thresholds (min rows, required columns, stale-source alerts).

## Test Gaps
- Missing integration tests for CVM/EIA/INPE/INMET sync entrypoints.
- Limited negative-path tests for network failures and upstream HTML/layout drift.

## Recommendations
1. Build shared ingestion primitives (config load/validate, HTTP client policy, upload helpers). (High, L)
2. Add typed config validation (Pydantic/dataclasses) for dataset YAML contracts. (High, M)
3. Add family-level contract tests for `sync()` output manifests. (High, L)
4. Standardize partial-failure policy and success thresholds. (Medium-High, M)
5. Add unchanged-content short-circuit where feasible to reduce redundant uploads. (Medium, M)

## Suggested Next Actions (30-60-90 days)
- 30 days: implement shared HTTP retry utility + start contract tests on highest-volume datasets.
- 60 days: migrate CVM runners to shared primitives and enforce config schema.
- 90 days: complete cross-family idempotency/freshness model and alerts.

