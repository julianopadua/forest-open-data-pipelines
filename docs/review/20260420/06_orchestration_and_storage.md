# Orchestration, Contracts, and Storage

## Scope
Review of orchestration/platform layer: `cli.py`, `cli_help.py`, `settings.py`, `registry/`, `manifests/`, `storage/`, `logging_.py`, `http.py`.

## Strengths
- Clear CLI command boundaries and registry-driven execution model.
- Centralized storage abstraction in `storage/supabase_storage.py`.
- Helpful dynamic CLI help generated from registries.

## Findings
### Critical
- None identified from current code sample.

### High
- Logging timestamp format appends `Z` without guaranteed UTC converter (`logging_.py`), risking misleading timestamps.
- Inconsistent exception semantics across CLI/runtime paths hamper operator debugging.
- Generic manifest contract lacks strict schema/version enforcement.

### Medium
- HTTP/retry logic is fragmented across modules; `http.py` is minimal and not a shared resilient client.
- `load_settings` combines parse + side effects (directory creation), reducing testability.

### Low
- Some runner/manifest mutation patterns in CLI can create subtle behavior coupling.

## Holes / Incomplete Areas
- Missing unified error taxonomy (recoverable vs fatal classes).
- Missing shared platform metrics (run id, retries, bytes uploaded, duration by stage).
- Missing orchestration integration tests with mocked storage and registries.

## Test Gaps
- Minimal tests for settings parsing error paths.
- Sparse tests for storage retry behavior and URL contract edge cases.
- No end-to-end CLI orchestration tests across commands.

## Recommendations
1. Fix logging UTC correctness and avoid unsafe global handler reset patterns. (High, S-M)
2. Define and enforce manifest schema/version for dataset sync output. (High, M)
3. Introduce shared retry/backoff policy utility for HTTP + storage. (High, M-L)
4. Split settings parse/validate from filesystem init side effects. (Medium, M)
5. Add integration tests for CLI orchestration paths with monkeypatched registries. (Medium, M)

## Suggested Next Actions (30-60-90 days)
- 30 days: logging + exception semantics baseline.
- 60 days: manifest schema and retry policy unification.
- 90 days: full orchestration integration test suite + run-level metrics.

