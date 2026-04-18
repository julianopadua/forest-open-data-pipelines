# tests/test_publish_anp_catalog.py
from __future__ import annotations

import json
from unittest.mock import MagicMock

from forest_pipelines.dados_abertos.publish_anp_catalog import (
    DEFAULT_ANP_CATALOG_PREFIX,
    publish_anp_catalog_compact,
)


def test_publish_uploads_catalog_then_manifest() -> None:
    storage = MagicMock()
    storage.public_url.side_effect = lambda p: f"https://x.test/storage/{p}"

    envelope = {
        "schema_version": "1",
        "generated_at": "2026-01-01T00:00:00+00:00",
        "source_total_registros": 2,
        "datasets": [{"package_id": "a", "slug": "s", "title": "T", "resources": []}],
    }
    logger = MagicMock()

    manifest = publish_anp_catalog_compact(storage, envelope, logger)

    assert storage.upload_bytes.call_count == 2
    calls = storage.upload_bytes.call_args_list
    cat_path = f"{DEFAULT_ANP_CATALOG_PREFIX}/anp_catalog_compact.json"
    man_path = f"{DEFAULT_ANP_CATALOG_PREFIX}/manifest.json"
    assert calls[0].kwargs["object_path"] == cat_path
    assert calls[1].kwargs["object_path"] == man_path

    body0 = json.loads(calls[0].kwargs["data"].decode("utf-8"))
    assert body0["schema_version"] == "1"
    assert len(body0["datasets"]) == 1

    body1 = json.loads(calls[1].kwargs["data"].decode("utf-8"))
    assert body1["catalog_id"] == "anp_catalog_compact"
    assert body1["dataset_count"] == 1
    assert body1["paths"]["catalog"] == cat_path
    assert body1["public_urls"]["catalog"] == f"https://x.test/storage/{cat_path}"

    assert manifest["public_urls"]["manifest"] == f"https://x.test/storage/{man_path}"


def test_publish_custom_prefix() -> None:
    storage = MagicMock()
    storage.public_url.side_effect = lambda p: f"https://x/{p}"
    env = {
        "schema_version": "1",
        "generated_at": "2026-01-01T00:00:00+00:00",
        "datasets": [],
    }
    publish_anp_catalog_compact(storage, env, None, bucket_prefix="anp/catalog/v1")
    paths = [c.kwargs["object_path"] for c in storage.upload_bytes.call_args_list]
    assert paths == [
        "anp/catalog/v1/anp_catalog_compact.json",
        "anp/catalog/v1/manifest.json",
    ]
