# Contributing to forest-open-data-pipelines

Thank you for considering a contribution to Instituto Forest.

Instituto Forest is an open-source data intelligence project for environmental and commodity data in Brazil. This repository contains the data discovery, profiling, catalog publishing, report generation, and public Python SDK code that support the Instituto Forest portal and public read-only API.

## Welcome contributions

Contributions are welcome when they improve correctness, reproducibility, maintainability, or public usefulness. Good early-stage contributions include:

- Bug reports with clear reproduction steps.
- Documentation improvements.
- Dataset source suggestions with official source information.
- Small fixes to catalog metadata.
- Tests for manifest, catalog, SDK, or report behavior.
- Improvements to local profiling that preserve the URL-only data contract.

Avoid broad rewrites unless they are discussed first.

## Report bugs

Open a GitHub issue with:

- A short description of the problem.
- Steps to reproduce the behavior.
- Expected behavior and actual behavior.
- Python version and operating system.
- The command that failed, with sanitized output.

Do not include secrets, private tokens, service-role keys, credentials, or sensitive infrastructure details.

## Suggest datasets

Dataset suggestions may be sent through the public Instituto Forest suggestion form or through a GitHub issue.

Every dataset suggestion should include:

- The official source URL.
- The responsible institution.
- The access method, such as direct file URL, API endpoint, catalog page, or manual download page.
- The update frequency, if known.
- A brief explanation of the dataset's relevance to Instituto Forest.

Dataset contributions must preserve official source provenance. Raw dataset files should not be uploaded to Supabase Storage as part of the public open-data contract.

## Improve documentation

Documentation improvements may target the README, operational notes under `docs/`, SDK docs, catalog explanations, or report documentation. Keep changes factual, concise, and easy to verify.

If a documentation change describes manifest behavior, verify that it matches the current builders and schemas.

## Open pull requests

Before opening a pull request:

1. Use a focused branch with a clear purpose.
2. Keep the diff small and reviewable.
3. Explain what changed and why.
4. Link related issues when applicable.
5. Preserve existing architecture and naming conventions.

Catalog and dataset changes should start from the source configuration under `configs/catalog/`. Do not bypass the manifest builders or silently change envelope shape. Breaking contract changes require coordinated updates with the portal and SDK.

## Validation before submitting

Run the relevant local checks before opening a pull request:

```bash
make test
```

For dataset changes, also describe any local smoke sync or profiling command you ran. If a full sync is too large for the pull request, include the exact command the maintainer should run.

## Security

Never commit secrets, service-role keys, credentials, private tokens, `.env` files, local data dumps, or sensitive infrastructure details.

`SUPABASE_SERVICE_ROLE_KEY`, `GROQ_API_KEY`, and similar credentials must stay in local environment files or repository secrets. The public SDK must not embed private credentials.

Report sensitive security issues privately. See [SECURITY.md](SECURITY.md).

## Licenses

Source code is licensed under the [MIT License](LICENSE).

Data, content, metadata, and third-party dataset terms are described in [DATA-LICENSING.md](DATA-LICENSING.md).
