# Audits Module

## Scope
Review of `src/forest_pipelines/audits` plus current audit outputs under `docs/audits`.

## Strengths
- Good separation of registry/helpers/renderer/domain runner.
- Dual-output artifacts (`README.md` + `summary.json`) support humans and automation.
- Utility functions are reasonably defensive with delimiter/encoding handling.

## Findings
### Critical
- None identified from current code sample.

### High
- No direct test coverage for audit module paths.
- Single-file corruption can fail broad portions of the audit flow.

### Medium
- Repeated ZIP scans and row counting can be expensive as history grows.
- Member selection in archives may be too heuristic when upstream package structure changes.

### Low
- Generated docs may include host-specific absolute paths, reducing portability.

## Holes / Incomplete Areas
- No partial-success policy with explicit failed-file reporting.
- No snapshot history strategy for audit outputs (overwrite tendency).

## Test Gaps
- Missing tests for `audits/utils.py`, markdown rendering, and registry wiring.
- Missing CLI-level audit integration tests.

## Recommendations
1. Add audit-focused tests (utils, markdown, runner integration). (High, M)
2. Isolate per-archive failures and produce partial-success summary. (High, M)
3. Reduce repeated ZIP passes and centralize extraction metadata. (Medium, M)
4. Add stricter archive member selection assertions/pattern checks. (Medium, S)
5. Sanitize generated artifact paths to repo-relative output. (Medium, S)

## Suggested Next Actions (30-60-90 days)
- 30 days: test scaffolding + partial-failure handling.
- 60 days: optimize IO scans and improve archive selection logic.
- 90 days: add historical audit snapshots and trend tracking.

