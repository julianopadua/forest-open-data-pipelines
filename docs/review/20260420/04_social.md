# Social Media Generation

## Scope
Review of `src/forest_pipelines/social` flow for chart generation, payload assembly, and LLM integration.

## Strengths
- Good stage-level logging (`social/logging.py`).
- Clear runnable entrypoint (`social/__main__.py`).
- Structured prompt/payload organization under `social/llm/`.

## Findings
### Critical
- None identified from current code sample.

### High
- Cross-module coupling to private report internals in `social/bdqueimadas_monthly_chart.py` increases breakage risk during refactors.
- Large orchestration surface in one function reduces testability and maintenance.

### Medium
- Repeated file scans and counting passes increase runtime cost.
- Sequential LLM generation path may become latency bottleneck with more scopes.

### Low
- Some stage boundaries are implicit rather than explicit DTO contracts.

## Holes / Incomplete Areas
- Missing explicit domain boundary between shared BDQueimadas logic and module-specific orchestration.
- No robust fallback contract tests for carousel manifest + LLM failures.

## Test Gaps
- Limited tests beyond happy-path social LLM runtime.
- Missing edge-case tests for chart spec validation and month/window boundaries.

## Recommendations
1. Extract shared BDQueimadas domain API and stop importing private report symbols. (High, M)
2. Split social builder into staged functions with typed inputs/outputs. (High, L)
3. Cache monthly parsed data per run to avoid repeated file passes. (Medium-High, S-M)
4. Add social artifact contract tests (chart spec, carousel manifest, fallback behavior). (High, M)
5. Parallelize safe LLM calls with bounded concurrency controls. (Medium, M)

## Suggested Next Actions (30-60-90 days)
- 30 days: remove private imports and add cache for repeated reads.
- 60 days: staged refactor + contract tests.
- 90 days: optimize LLM orchestration and observability metrics.

