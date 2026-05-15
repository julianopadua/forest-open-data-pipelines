# forest-data

Public Python client for the Instituto Forest open-data API.

The package wraps the read-only HTTP API at `https://institutoforest.org/api/v1`
and adds local download with sha256 verification. It carries no business logic
beyond that; the catalog itself is owned by the Forest pipelines.

## Install

```bash
pip install forest-data
```

## Usage

```python
import forest_data

client = forest_data.Client()

# discover datasets
for d in client.list_datasets():
    print(d.id, "-", d.title)

# fetch a single manifest
manifest = client.get_dataset("inpe_bdqueimadas_focos")
print(manifest.bucket_prefix, len(manifest.items), "files")

# download every file in a dataset, verify sha256, save under ./data
paths = client.download("inpe_bdqueimadas_focos", path="./data")
```

## Configuration

Default base URL: `https://institutoforest.org/api/v1`.

Override with `Client(base_url=...)` or via the `FOREST_API_BASE_URL` env var.

## Reference

Full API documentation: https://institutoforest.org/docs/api/v1
