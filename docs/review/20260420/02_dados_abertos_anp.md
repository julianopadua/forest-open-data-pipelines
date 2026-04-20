# Dados Abertos / ANP Pipeline

## Scope
Review of `src/forest_pipelines/dados_abertos` (`anp-catalog`, compact transform, schema validation, publish).

## Strengths
- Clean modular split (`api_client.py`, `parse.py`, `anp_catalog_compact.py`, `publish_anp_catalog.py`).
- Good baseline contract artifacts (`schemas/anp_catalog_compact.v1.schema.json`, validator, tests).
- Resource merge and normalization logic is thoughtful (`_merge_resource_lists`, `nfc_text`).

## Findings
### Critical
- None identified from current code sample.

### High
- Missing end-to-end tests for ANP CLI command paths in `src/forest_pipelines/cli.py`.
- Generated large ANP artifacts under `src/` are operationally fragile and noisy for repo health.
- Full-run export is memory-bound in `src/forest_pipelines/dados_abertos/anp_catalog.py`.

### Medium
- Retry policy in `api_client.fetch_json_with_retries` is basic for real WAF/rate-limit behavior.
- Compact schema allows broad extras (`additionalProperties`), reducing strict contract guarantees.

### Low
- Minor duplicated serialization logic in publish helper.

## Holes / Incomplete Areas
- No checkpoint/resume support for long catalog runs.
- No incremental/delta mode for ANP catalog refresh.
- Limited machine-readable runtime telemetry beyond logs.

## Test Gaps
- No integration tests for `anp-catalog`, `anp-compact`, `anp-publish` flows.
- Thin negative tests for publish failure scenarios.

## Recommendations
1. Add CLI integration tests for `anp-*` commands with mocked network/storage. (High, M)
2. Implement streaming + checkpointing in ANP catalog fetch/export loop. (High, L)
3. Improve retry with jitter/status-aware backoff in API client. (Medium-High, M)
4. Move generated snapshot artifacts out of `src/` into dedicated outputs path. (High, S-M)
5. Tighten schema strictness and document backward-compat contract. (Medium, M)

## Suggested Next Actions (30-60-90 days)
- 30 days: add CLI ANP tests and retry hardening.
- 60 days: migrate outputs location and document runbook.
- 90 days: complete streaming/checkpoint architecture.

