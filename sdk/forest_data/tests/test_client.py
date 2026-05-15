"""Smoke tests for forest_data.Client. Mocked HTTP via respx, no real network."""
from __future__ import annotations

import hashlib

import httpx
import pytest
import respx

from forest_data import Client, NotFoundError


CATALOG_BODY = {
    "schema_version": "1.0",
    "api_version": "v1",
    "generated_at": "2026-04-23T12:00:00Z",
    "generation_status": "success",
    "warnings": [],
    "datasets": [
        {
            "id": "inpe_bdqueimadas_focos",
            "slug": "focos-bdqueimadas",
            "title": "INPE - BDQueimadas - Focos Brasil",
            "description": "x",
            "source_id": "inpe",
            "source_title": "INPE",
            "category_title": "Meio ambiente",
            "subcategory_title": "Queimadas",
            "source_url": "https://example.org",
            "manifest_path": "inpe/bdqueimadas/focos_br_ref/manifest.json",
        }
    ],
}

MANIFEST_BODY_TEMPLATE = {
    "schema_version": "1.0",
    "api_version": "v1",
    "generated_at": "2026-04-22T03:11:09Z",
    "generation_status": "success",
    "warnings": [],
    "manifest": {
        "schema_version": "1.0",
        "dataset_id": "inpe_bdqueimadas_focos",
        "title": "INPE - BDQueimadas - Focos Brasil",
        "source_dataset_url": "https://example.org",
        "generated_at": "2026-04-22T03:11:09Z",
        "generation_status": "success",
        "warnings": [],
        "bucket_prefix": "inpe/bdqueimadas/focos_br_ref",
        "items": [],
        "meta": {"custom_tags": {}},
    },
}


@respx.mock
def test_list_datasets_parses_envelope():
    respx.get("https://example.test/api/v1/catalog").mock(
        return_value=httpx.Response(200, json=CATALOG_BODY),
    )
    client = Client(base_url="https://example.test/api/v1")
    datasets = client.list_datasets()
    assert len(datasets) == 1
    assert datasets[0].id == "inpe_bdqueimadas_focos"
    assert datasets[0].source_id == "inpe"


@respx.mock
def test_get_dataset_returns_manifest():
    respx.get("https://example.test/api/v1/datasets/inpe_bdqueimadas_focos").mock(
        return_value=httpx.Response(200, json=MANIFEST_BODY_TEMPLATE),
    )
    client = Client(base_url="https://example.test/api/v1")
    manifest = client.get_dataset("inpe_bdqueimadas_focos")
    assert manifest.dataset_id == "inpe_bdqueimadas_focos"
    assert manifest.bucket_prefix == "inpe/bdqueimadas/focos_br_ref"


@respx.mock
def test_not_found_raises():
    respx.get("https://example.test/api/v1/datasets/nope").mock(
        return_value=httpx.Response(
            404,
            json={
                "type": "https://example.org/errors/not-found",
                "title": "Resource not found",
                "status": 404,
                "detail": 'No dataset with id or slug "nope".',
            },
        ),
    )
    client = Client(base_url="https://example.test/api/v1")
    with pytest.raises(NotFoundError):
        client.get_dataset("nope")


@respx.mock
def test_download_verifies_sha256(tmp_path):
    body = b"hello world\n"
    expected_sha = hashlib.sha256(body).hexdigest()

    manifest_payload = {
        **MANIFEST_BODY_TEMPLATE,
        "manifest": {
            **MANIFEST_BODY_TEMPLATE["manifest"],
            "items": [
                {
                    "kind": "data",
                    "period": "2024",
                    "filename": "hello.txt",
                    "sha256": expected_sha,
                    "size_bytes": len(body),
                    "public_url": "https://files.test/hello.txt",
                    "source_url": "https://example.org",
                }
            ],
        },
    }

    respx.get("https://example.test/api/v1/datasets/inpe_bdqueimadas_focos").mock(
        return_value=httpx.Response(200, json=manifest_payload),
    )
    respx.get("https://files.test/hello.txt").mock(
        return_value=httpx.Response(200, content=body),
    )

    client = Client(base_url="https://example.test/api/v1")
    paths = client.download("inpe_bdqueimadas_focos", path=tmp_path)
    assert len(paths) == 1
    assert paths[0].read_bytes() == body
