# Cross-Cutting Quality and Tests

## Scope
Cross-cutting quality review for test strategy, documentation coverage, and maintainability signals.

## Strengths
- Existing tests cover several parser/transform units (`tests/test_dados_abertos_parse.py`, `tests/test_anp_catalog_compact.py`, `tests/test_noticias_agricolas_parsers.py`).
- README is relatively complete and transparent about docs lag.
- Existing `docs/project_state/20260417` provides useful baseline architecture notes.

## Findings
### Critical
- None identified from current code sample.

### High
- Integration/E2E test coverage is thin where operational failures happen (CLI + publish).
- `docs/src` module coverage is partial and some docs are stale relative to current commands/registries.

### Medium
- Quality gates appear uneven across modules (some robust validation, others minimal).
- Large generated JSON artifacts in source tree reduce review quality and repository hygiene.

### Low
- Inconsistent conventions for resilience/idempotency across families increase cognitive load.

## Holes / Incomplete Areas
- No explicit test matrix by module criticality.
- No docs parity checks tied to code changes.
- No standardized review artifact cadence under `docs/review` before this run.

## Test Gaps
- No direct tests for audits and many sync/report publish negative paths.
- Limited coverage for transient failure/retry behavior and partial success handling.

## Recommendations
1. Define a risk-based test matrix (unit + integration) per module family. (High, M)
2. Add CI check for docs parity on core interfaces (CLI commands, registry IDs). (High, S-M)
3. Move generated bulky data outputs out of source package tree. (High, S-M)
4. Add reliability-focused tests (retry, partial failures, idempotency). (Medium-High, M)
5. Establish quarterly modular architecture review cadence with tracked backlog burn-down. (Medium, S)

## Suggested Next Actions (30-60-90 days)
- 30 days: baseline integration tests and docs parity checks.
- 60 days: reliability test expansion and artifact hygiene cleanup.
- 90 days: recurring architecture review workflow with measurable KPIs.

