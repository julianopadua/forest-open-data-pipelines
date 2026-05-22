"""Smoke tests for forest_data.Client. Mocked HTTP via respx, no real network."""
from __future__ import annotations

import hashlib

import httpx
import pytest
import respx

from forest_data import Client, NotFoundError, UnsafeFilenameError


CATALOG_BODY = {
    "schema_version": "2.0",
    "api_version": "v1",
    "generated_at": "2026-04-23T12:00:00Z",
    "generation_status": "success",
    "warnings": [],
    "datasets": [
        {
            "id": "inpe_bdqueimadas_focos",
            "slug": "focos-bdqueimadas",
            "title": "INPE - BDQueimadas - Focos Brasil",
            "title_en": "INPE - BDQueimadas - Brazil Hotspots",
            "description": "x",
            "description_en": "y",
            "source_id": "inpe",
            "source_title": "INPE",
            "source_title_en": "INPE",
            "category_title": "Meio ambiente",
            "category_title_en": "Environment",
            "subcategory_title": "Queimadas",
            "subcategory_title_en": "Wildfires",
            "source_url": "https://example.org",
            "manifest_path": "inpe/bdqueimadas/focos_br_ref/manifest.json",
        }
    ],
}

MANIFEST_BODY_TEMPLATE = {
    "schema_version": "2.0",
    "api_version": "v1",
    "generated_at": "2026-04-22T03:11:09Z",
    "generation_status": "success",
    "warnings": [],
    "manifest": {
        "schema_version": "2.0",
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
    assert datasets[0].title_en == "INPE - BDQueimadas - Brazil Hotspots"
    assert datasets[0].description_en == "y"


@respx.mock
def test_list_reports_parses_card_metadata():
    reports_payload = {
        "schema_version": "2.0",
        "api_version": "v1",
        "generated_at": "2026-05-22T12:00:00Z",
        "generation_status": "success",
        "warnings": [],
        "reports": [
            {
                "id": "bdqueimadas_overview",
                "slug": "bdqueimadas-overview",
                "title": "BDQueimadas",
                "title_en": "BDQueimadas",
                "description": "Fallback",
                "description_en": "Fallback EN",
                "excerpt": "Resumo do card",
                "excerpt_en": "Card summary",
                "generated_at": "2026-05-22T12:00:00Z",
                "coverage": {
                    "first_year": 2003,
                    "latest_year": 2026,
                    "year_range": "2003-2026",
                    "latest_period": "2026-05",
                },
                "source_title": "INPE",
                "source_title_en": "INPE",
                "category_title": "Meio ambiente",
                "category_title_en": "Environment",
                "manifest_path": "reports/bdqueimadas/overview/manifest.json",
                "stable_report_path": "reports/bdqueimadas/overview/report.json",
                "tags": ["queimadas"],
            }
        ],
    }
    respx.get("https://example.test/api/v1/catalog/reports").mock(
        return_value=httpx.Response(200, json=reports_payload),
    )
    client = Client(base_url="https://example.test/api/v1")
    reports = client.list_reports()
    assert len(reports) == 1
    assert reports[0].excerpt_en == "Card summary"
    assert reports[0].coverage is not None
    assert reports[0].coverage.year_range == "2003-2026"


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
def test_get_items_reads_source_urls_and_profile_fields():
    items_payload = {
        "schema_version": "2.0",
        "api_version": "v1",
        "generated_at": "2026-04-22T03:11:09Z",
        "generation_status": "success",
        "warnings": [],
        "items": [
            {
                "kind": "data",
                "period": "2024",
                "filename": "rows.csv",
                "source_url": "https://files.test/rows.csv",
                "size_bytes": 12,
                "row_count": 2,
                "column_count": 2,
                "columns": ["a", "b"],
                "profile_status": "ok",
                "profile_warnings": [],
            }
        ],
    }
    respx.get("https://example.test/api/v1/datasets/inpe_bdqueimadas_focos/items").mock(
        return_value=httpx.Response(200, json=items_payload),
    )
    client = Client(base_url="https://example.test/api/v1")
    items = client.get_dataset_items("inpe_bdqueimadas_focos")
    assert client.get_source_urls("inpe_bdqueimadas_focos") == ["https://files.test/rows.csv"]
    assert items[0].source_url == "https://files.test/rows.csv"
    assert items[0].profile_status == "ok"
    assert items[0].row_count == 2
    assert items[0].columns == ["a", "b"]


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
                    "source_url": "https://files.test/hello.txt",
                    "profile_status": "ok",
                    "profile_warnings": [],
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


@respx.mock
@pytest.mark.parametrize(
    "evil_filename",
    [
        "../escape.txt",
        "../../escape.txt",
        "/tmp/forest_pentest_escape.txt",
        "nested/../../escape.txt",
        "",
    ],
)
def test_download_rejects_unsafe_filename(tmp_path, evil_filename):
    manifest_payload = {
        **MANIFEST_BODY_TEMPLATE,
        "manifest": {
            **MANIFEST_BODY_TEMPLATE["manifest"],
            "items": [
                {
                    "kind": "data",
                    "period": "2024",
                    "filename": evil_filename,
                    "source_url": "https://files.test/x",
                    "profile_status": "ok",
                    "profile_warnings": [],
                }
            ],
        },
    }
    respx.get("https://example.test/api/v1/datasets/x").mock(
        return_value=httpx.Response(200, json=manifest_payload),
    )

    client = Client(base_url="https://example.test/api/v1")
    with pytest.raises(UnsafeFilenameError):
        client.download("x", path=tmp_path)
