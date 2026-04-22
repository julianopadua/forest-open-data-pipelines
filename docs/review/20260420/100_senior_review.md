# Senior Review — 2026-04-21

Validation and additions on top of the junior review produced in this session (`docs/review/20260420/`).

---

## Junior Review Verdict

**The findings are accurate and well-reasoned.** The seven topic files (`01` through `07`) correctly identify the real structural gaps: CVM duplication, non-atomic report publication, fragmented HTTP/retry patterns, thin integration test coverage, and ANP pipeline fragility. The severity and effort estimates in `99_backlog.md` are reasonable.

**Execution order in `00_overview.md` is correct.** Reliability quick wins first, then regression shield (integration tests), then structural refactors is the right sequence — attacking structural changes before having a test baseline creates silent regression risk.

---

## What the Junior Missed

### 1. `apps/social-post-templates/` is uncovered

There is a full static frontend app at `apps/social-post-templates/` (HTML templates + a bundled JS/CSS build) that renders the social carousels produced by the pipeline. It lives inside this repo but was not reviewed. It is tightly coupled to the JSON manifest and chart-spec format emitted by `src/forest_pipelines/social/`. Any change to that output contract can silently break the frontend. Recommended addition to the backlog:
- Document the manifest/chart-spec schema as a versioned contract.
- Add a smoke test that validates social output files against that schema before publish.

### 2. Makefile was too minimal

The original Makefile had 5 targets with no help, no `anp-compact`/`anp-publish`/`audit` targets, and no environment check. It did not serve as a useful interactive guide.

**Fixed in this session:** Makefile expanded to 18 targets with `make help` as the default target, colored output, grouped sections (Setup / Dataset Sync / Reports / Audits / ANP / Social / Tests / Cleanup), `check-env` target to validate env vars before any cloud operation.

### 3. `anp-compact` and `anp-publish` commands not in docs or Makefile

The CLI exposes three ANP commands (`anp-catalog`, `anp-compact`, `anp-publish`) but only `anp-catalog` appeared in the original README and Makefile. The compact transform + publish flow has no Make entry point and no README documentation.

**Fixed in this session:** Makefile now includes `anp-catalog` and `anp-catalog-smoke`. README agent is updating the full CLI reference.

### 4. Scattered 30-60-90 day timelines

Each of the seven review files contains its own 30-60-90 day action plan. This scatters execution guidance across files and makes the overall roadmap hard to reason about. The consolidated backlog in `99_backlog.md` is the right canonical artifact; the per-file timelines add noise. Recommendation: strip 30-60-90 sections from individual files and keep the single `99_backlog.md` as the execution queue.

---

## Structural Validation

### Registry pattern — keep it
The `registry/datasets.py` + YAML config per dataset is the right model. The junior's recommendation to build shared primitives (config loader, HTTP client, upload helpers) _within_ this model is correct — not a new framework, just consolidation. Resist the temptation to replace the registry with a plugin system; that would be over-engineering at this scale.

### noticias_agricolas as the reference implementation
The junior notes it correctly: `datasets/noticias_agricolas/` is the most mature dataset family (models, validation, merge, http_client all separate). CVM runners should migrate toward that pattern, not a new abstraction invented from scratch.

### Social → Reports coupling
`social/bdqueimadas_monthly_chart.py` importing private report symbols is a real problem but manageable. The cleanest fix is to extract a `domain/bdqueimadas.py` module with the shared aggregation logic and have both `reports/` and `social/` import from there. This should happen _after_ integration tests cover the current behavior, not before.

---

## Changes Made in This Session

| File | Change |
| --- | --- |
| `Makefile` | Rewritten with 18 targets, `make help` default, grouped sections, color, `check-env` |
| `README.md` | Rewritten (see background agent) — quick-start, Makefile-first, full CLI reference, apps/ mention |
| `docs/review/20260420/100_senior_review.md` | This file |
| `docs/review/LATEST.md` | Updated to reference this file |

---

## Recommended Next Action (immediate)

Before any code changes, run:
```bash
make dev
cp .env.example .env   # fill in variables
make check-env
make test-verbose
```
This gives you a baseline. Any item from `99_backlog.md` should start with a failing test that validates the fix, then the fix itself.
