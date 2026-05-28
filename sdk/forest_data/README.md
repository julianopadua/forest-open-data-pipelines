# forest-data

Public Python client for the Instituto Forest open-data API.

The package wraps `https://institutoforest.org/api/v1`. The API returns metadata, official source URLs, and profiling results. Dataset bytes are downloaded from the official source listed in each item `source_url`. Forest does not mirror raw dataset files in Supabase.

The SDK is dataset-focused. Forest report pages and report JSON artifacts are not part of the public API v1 or SDK contract.

**Coverage.** All registered open-data sources are reachable through the same `Client.list_datasets()` / `Client.get_dataset(id)` surface, including ANP gov.br datasets. ANP items expose official `source_url` values and profiling status like any other dataset.

**Security.** `Client.download()` contains every write to the chosen target directory. Manifest items with absolute filenames, `..` traversal segments, or Windows drive letters raise `UnsafeFilenameError` before any HTTP request. This is enforced from `0.1.0a1` onwards. Earlier versions allowed traversal; upgrade with `pip install -U forest-data`.

## Install

```bash
pip install forest-data
```

## Basic Use

```python
import forest_data

client = forest_data.Client()

for dataset in client.list_datasets():
    print(dataset.id, dataset.title)

manifest = client.get_dataset("inpe_bdqueimadas_focos")
for item in manifest.items:
    print(item.period, item.source_url, item.profile_status, item.row_count)
```

## Download From Official Sources

```python
import forest_data

client = forest_data.Client()
paths = client.download("inpe_bdqueimadas_focos", path="./data")
```

`download()` follows item `source_url` values. If an item has `sha256`, the SDK verifies it by default. If an item does not have `sha256`, the SDK downloads it without checksum verification.

## URL Discovery

```python
import forest_data

client = forest_data.Client()
urls = client.get_source_urls("inpe_bdqueimadas_focos")
```

Use `get_dataset_items()` or `iter_items()` when your pipeline needs profile metadata before deciding what to download:

```python
for item in client.iter_items("inpe_bdqueimadas_focos"):
    if item.profile_status == "ok" and item.row_count:
        print(item.filename, item.row_count, item.source_url)
```

## Profile Fields

`profiled_at` is the UTC time when Forest finished profiling the item.

`profile_status` describes the profiling result:

- `ok`: download and expected profiling metrics succeeded.
- `partial`: the resource was downloaded, but some metrics are incomplete.
- `failed`: the URL was discovered, but download or parsing failed.
- `skipped`: profiling was intentionally not attempted.

`profile_warnings` contains public-safe warning objects with `code` and `message`. Use these warnings to decide whether an item is fit for automated ingestion.

## Configuration

Default base URL: `https://institutoforest.org/api/v1`.

Override with `Client(base_url=...)` or `FOREST_API_BASE_URL`.

Full API documentation: https://institutoforest.org/docs/api/v1
