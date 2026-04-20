---
name: modular-project-review
description: Reviews the forest-open-data-pipelines codebase module-by-module, using parallel subagents when useful, and writes actionable review reports under docs/review. Use when the user asks to review architecture, identify weaknesses, technical debt, scalability risks, inefficiencies, missing pieces, or improvement opportunities across the project.
---

# Modular Project Review

## Goal

Run a structured, modular review of this repository and produce durable review artifacts in `docs/review/`.

This skill is for broad project review, not for isolated bug fixes.

## Project Module Map

Use this module split as default:

1. `datasets/` ingestion pipelines (`src/forest_pipelines/datasets`)
2. open-data/ANP catalog pipeline (`src/forest_pipelines/dados_abertos`)
3. reports pipeline (`src/forest_pipelines/reports`)
4. social media generation (`src/forest_pipelines/social`)
5. audits (`src/forest_pipelines/audits`)
6. orchestration and contracts (`src/forest_pipelines/cli.py`, `src/forest_pipelines/registry`, `src/forest_pipelines/settings.py`, `src/forest_pipelines/manifests`, `src/forest_pipelines/storage`)
7. cross-cutting quality (`tests/`, config hygiene, docs coverage, observability/logging)

If the repo evolves, adjust the module map before reviewing.

## Required Output

Always write review files under:

- `docs/review/<YYYYMMDD>/00_overview.md`
- `docs/review/<YYYYMMDD>/01_datasets.md`
- `docs/review/<YYYYMMDD>/02_dados_abertos_anp.md`
- `docs/review/<YYYYMMDD>/03_reports.md`
- `docs/review/<YYYYMMDD>/04_social.md`
- `docs/review/<YYYYMMDD>/05_audits.md`
- `docs/review/<YYYYMMDD>/06_orchestration_and_storage.md`
- `docs/review/<YYYYMMDD>/07_quality_and_tests.md`
- `docs/review/<YYYYMMDD>/99_backlog.md`

Also update or create:

- `docs/review/LATEST.md` (points to latest run and summarizes top priorities)

## Review Criteria Per Module

For each module file, include:

1. **Scope**
   - What the module is responsible for.
2. **Strengths**
   - What is solid and should be preserved.
3. **Weak points**
   - Design flaws, fragile flows, maintainability concerns.
4. **Holes / incompleteness**
   - Missing features, missing contracts, missing docs, missing runbooks.
5. **Scalability risks**
   - Throughput bottlenecks, memory growth, coupling, serialization limits.
6. **Efficiency issues**
   - Repeated work, unnecessary I/O, expensive operations without caching.
7. **Reliability and data quality risks**
   - Idempotency gaps, retry gaps, partial failure behavior, schema drift risk.
8. **Security and compliance concerns**
   - Secrets handling, unsafe defaults, public-data assumptions.
9. **Test coverage assessment**
   - What exists, what is missing, highest-risk missing tests.
10. **Actionable recommendations**
   - Prioritized fixes with impact and effort (High/Medium/Low).

## Execution Flow

1. **Scan and map**
   - Read `README.md`, `src/forest_pipelines/`, configs, and tests.
   - Confirm module boundaries and key entrypoints.

2. **Parallel exploration**
   - When possible, launch multiple subagents in parallel, each focused on one module.
   - Ask each subagent to return:
     - top risks
     - missing capabilities
     - inefficiencies
     - test gaps
     - concrete recommendations

3. **Synthesize**
   - Consolidate subagent findings.
   - Remove duplicates.
   - Resolve contradictions with direct code checks.

4. **Write artifacts**
   - Create/update markdown files in `docs/review/<YYYYMMDD>/`.
   - Keep each module report concise and actionable.
   - Add an implementation backlog in `99_backlog.md`.

5. **Prioritize**
   - `00_overview.md` must include:
     - top 5 risks
     - top 5 improvements
     - suggested execution order
     - quick wins vs structural changes

## Severity and Prioritization Standard

Use this scale:

- **Critical**: likely data corruption, major downtime risk, or severe security exposure.
- **High**: major reliability/scalability/maintainability risk.
- **Medium**: meaningful quality/performance/developer productivity issue.
- **Low**: nice-to-have improvements.

Use this effort scale:

- **S** (hours)
- **M** (1-3 days)
- **L** (up to 2 weeks)
- **XL** (multi-sprint)

`99_backlog.md` must include: `item`, `severity`, `effort`, `owner_suggestion`, `module`, `rationale`.

## Markdown Template (Per Module)

Use this structure in each module file:

```markdown
# <Module Name>

## Scope
...

## Strengths
- ...

## Findings
### Critical
- ...

### High
- ...

### Medium
- ...

### Low
- ...

## Holes / Incomplete Areas
- ...

## Test Gaps
- ...

## Recommendations
1. ...
2. ...

## Suggested Next Actions (30-60-90 days)
- 30 days: ...
- 60 days: ...
- 90 days: ...
```

## Guardrails

- Do not produce generic advice disconnected from repository code.
- Prefer concrete references to files/symbols.
- Focus on actionable findings over long narrative.
- If evidence is incomplete, explicitly mark assumptions.
- Keep recommendations modular: avoid one giant refactor as the only path.

## When To Use

Trigger this skill when user asks for:

- full project review
- architecture review
- technical debt mapping
- scalability assessment
- inefficiency analysis
- “what is missing” / “holes” audit
- prioritized improvement roadmap
