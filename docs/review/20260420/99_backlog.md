# Review Backlog

| item | severity | effort | owner_suggestion | module | rationale |
| --- | --- | --- | --- | --- | --- |
| Add integration tests for `anp-catalog`, `anp-compact`, `anp-publish` | High | M | data-platform | dados_abertos | Protects key operational entrypoints currently under-tested |
| Introduce shared HTTP retry/backoff utility (status-aware + jitter) | High | M | platform | datasets/orchestration | Removes resilience drift and duplicated logic |
| Define schema/version validation for generic dataset `manifest.json` | High | M | platform | orchestration/manifests | Prevents silent contract drift to downstream consumers |
| Refactor CVM dataset runners into shared primitives | High | L | data-platform | datasets | Reduces duplication and maintenance blast radius |
| Enforce publish consistency (versioned paths + manifest pointer last) | High | M | platform | reports | Avoids mixed-version public state during uploads |
| Decouple social from private report internals via shared domain API | High | M | social+reports | social/reports | Improves modularity and safer refactoring |
| Add audit module tests and partial-failure isolation | High | M | data-quality | audits | Increases trust and resilience of audit outputs |
| Fix logging UTC correctness and handler safety | High | S-M | platform | logging | Improves observability correctness and concurrency safety |
| Move generated ANP snapshots and bulky artifacts out of `src/` | High | S-M | data-platform | dados_abertos/quality | Better repo hygiene and faster code review/CI |
| Add post-override schema validation for report outputs | Medium-High | S-M | reports | reports | Reduces malformed output risk before publish |
| Add unchanged-content short-circuit/idempotency checks for syncs | Medium | M | datasets | datasets | Reduces redundant network/storage work |
| Add docs parity CI checks for CLI commands and registry IDs | Medium | S-M | platform | quality/docs | Keeps docs accurate as interfaces evolve |

