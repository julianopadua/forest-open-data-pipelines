# Project Review Overview

## Scope
Cross-module review of `forest-open-data-pipelines` covering ingestion datasets, ANP open-data pipeline, reports, social generation, audits, orchestration/storage, and testing/documentation quality.

## Top 5 Risks
1. High code duplication and drift risk across dataset runners (`src/forest_pipelines/datasets/cvm/*`, mixed HTTP/retry patterns).
2. Incomplete integration tests at operational entrypoints (`forest-pipelines sync`, ANP CLI flows, report publish flows).
3. Non-atomic multi-object publication risks inconsistent public state (`src/forest_pipelines/reports/publish/supabase.py`).
4. Logging and error semantics are inconsistent for platform-level observability and failure handling (`src/forest_pipelines/logging_.py`, `src/forest_pipelines/cli.py`).
5. Documentation/test coverage lags code growth in key modules (`docs/src` partial coverage; no direct tests for `audits`).

## Top 5 Improvements
1. Introduce shared HTTP + retry policy and typed config validation for dataset runners.
2. Add integration tests for CLI orchestration paths (`sync`, `anp-*`, `build-report`, `audit-dataset`).
3. Implement publish consistency pattern: versioned artifact paths + manifest pointer written last.
4. Refactor large orchestration functions (notably social builder) into testable stages with typed DTOs.
5. Add contract/schema checks for generic manifests and stricter docs parity checks.

## Suggested Execution Order
1. Reliability quick wins: logging UTC correctness, retry policy unification, error taxonomy.
2. Regression shield: integration tests for most-used CLI flows.
3. Contract hardening: manifest schema/version validation and ANP pipeline resilience.
4. Structural refactors: CVM runner dedup + social/reports decoupling.
5. Scaling and observability: checkpointing, incremental/streaming exports, run-level metrics.

## Quick Wins vs Structural Changes
- Quick wins (S/M): logging fixes, docs parity updates, audit fault isolation, post-merge output validation.
- Structural (L/XL): shared ingestion framework for datasets, transactional-style publish architecture, social/reports domain boundary redesign.

