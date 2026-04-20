# Reports Pipeline

## Scope
Review of `src/forest_pipelines/reports` builders, definitions, LLM helpers, and publication.

## Strengths
- Strong typed config contracts (`reports/definitions/base.py`).
- Incremental annual cache strategy is solid (`reports/builders/bdqueimadas_incremental.py`).
- LLM fallback behavior reduces total pipeline failure (`reports/llm/base.py`).

## Findings
### Critical
- None identified from current code sample.

### High
- Report publication is multi-object and non-atomic (`reports/publish/supabase.py`), enabling inconsistent reader state.
- Large payload assembly can grow memory and output size nonlinearly (`reports/builders/bdqueimadas_overview.py`).

### Medium
- Post-override contract validation is limited (risk of malformed shape after merges).
- Registry extensibility appears narrow with one dominant report path.

### Low
- Repeated upload/serialization patterns could be centralized.

## Holes / Incomplete Areas
- No explicit version handshake ensuring manifest-to-artifact consistency.
- Limited schema validation after editorial override merge.

## Test Gaps
- Sparse tests for builder cache invariants and publication consistency.
- No robust edge-case tests for report package integrity.

## Recommendations
1. Publish versioned artifacts first; write manifest pointer last. (High, M)
2. Add strict post-merge output schema validation before publish. (Medium-High, S-M)
3. Add regression tests for incremental cache reuse/rebuild scenarios. (High, M)
4. Separate payload-heavy sections into bounded/optional fragments. (Medium, M)
5. Expand report registry contract and tests for multi-report growth. (Medium, M)

## Suggested Next Actions (30-60-90 days)
- 30 days: publish consistency guard + schema check.
- 60 days: add builder contract tests and package integrity tests.
- 90 days: optimize payload architecture for larger historical windows.

