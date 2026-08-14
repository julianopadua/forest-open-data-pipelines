# Forest Open Data Pipelines

Python pipelines for discovering official open-data URLs, profiling data locally, publishing dataset metadata to Supabase Storage, generating reports and maintaining the `forest-data` SDK.

## Documentation

Formal documentation is maintained in the workspace wiki:

- [Documentation index](../llm-wiki-forest/documents/README.md)
- [System requirements](../llm-wiki-forest/documents/requirements/system-requirements.md)
- [Architecture](../llm-wiki-forest/documents/technical/architecture.md)
- [Backend and data contract](../llm-wiki-forest/documents/backend/backend-and-data-contract.md)
- [Public API, Portuguese](../llm-wiki-forest/documents/backend/api-v1.pt.md)
- [Public API, English](../llm-wiki-forest/documents/backend/api-v1.en.md)

## Requirements

- Python 3.11+
- Supabase Storage credentials
- Groq API key only for LLM-enabled report and social commands

## Install and run

```bash
make dev
cp .env.example .env
make check-env
make sync
```

Pipelines publish catalog envelopes and dataset manifests. Dataset payload bytes are never uploaded to Storage. Each item retains its official `source_url`.

## Quality checks

```bash
ruff check src tests sdk/forest_data/forest_data sdk/forest_data/tests
make test
```

The social templates application has its own checks:

```bash
cd apps/social-post-templates
npm ci
npm run check
npm run build
npm run test:e2e
```

## Public API and SDK

The portal exposes a dataset-only public API at `https://institutoforest.org/api/v1`. The SDK wraps this contract:

```bash
pip install forest-data
```

```python
import forest_data

client = forest_data.Client()
manifest = client.get_dataset("inpe_bdqueimadas_focos")
for item in manifest.items:
    print(item.source_url)
```

## Security

- Keep `SUPABASE_SERVICE_ROLE_KEY` and `GROQ_API_KEY` in `.env` only.
- Do not publish source payload bytes or credentials.
- Run `forest-api-tests/scripts/run_pentest.sh` before deploying API or SDK changes.

## License

See [LICENSE](LICENSE).
