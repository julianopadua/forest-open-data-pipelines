# src/forest_pipelines/dados_abertos/publish_anp_catalog.py
"""Publica o envelope `anp_catalog_compact` no Storage Supabase (bucket open-data)."""
from __future__ import annotations

import json
from typing import Any


DEFAULT_ANP_CATALOG_PREFIX = "anp/catalog"


def _to_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def publish_anp_catalog_compact(
    storage: Any,
    envelope: dict[str, Any],
    logger: Any,
    *,
    bucket_prefix: str = DEFAULT_ANP_CATALOG_PREFIX,
) -> dict[str, Any]:
    """
    Sobe ``anp_catalog_compact.json`` e ``manifest.json`` sob ``bucket_prefix``.

    ``envelope`` deve ser o dict já validado (ex.: ``validate_compact_envelope``).
    """
    prefix = bucket_prefix.strip().rstrip("/")
    catalog_path = f"{prefix}/anp_catalog_compact.json"
    manifest_path = f"{prefix}/manifest.json"

    catalog_bytes = json.dumps(envelope, ensure_ascii=False, indent=2).encode("utf-8")
    storage.upload_bytes(
        object_path=catalog_path,
        data=catalog_bytes,
        content_type="application/json; charset=utf-8",
        upsert=True,
    )

    manifest: dict[str, Any] = {
        "catalog_id": "anp_catalog_compact",
        "schema_version": envelope.get("schema_version"),
        "generated_at": envelope.get("generated_at"),
        "source_total_registros": envelope.get("source_total_registros"),
        "dataset_count": len(envelope.get("datasets") or []),
        "bucket_prefix": prefix,
        "paths": {
            "catalog": catalog_path,
            "manifest": manifest_path,
        },
        "public_urls": {
            "catalog": storage.public_url(catalog_path),
            "manifest": storage.public_url(manifest_path),
        },
    }

    storage.upload_bytes(
        object_path=manifest_path,
        data=_to_bytes(manifest),
        content_type="application/json; charset=utf-8",
        upsert=True,
    )

    if logger:
        logger.info("Catálogo ANP publicado: %s", manifest["public_urls"]["catalog"])
        logger.info("Manifest: %s", manifest["public_urls"]["manifest"])

    return manifest
