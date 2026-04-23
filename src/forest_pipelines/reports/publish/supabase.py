# src/forest_pipelines/reports/publish/supabase.py
from __future__ import annotations

import json
from typing import Any

REPORT_MANIFEST_SCHEMA_VERSION = "1.0"

_STRICT_REPORT_META_KEYS: tuple[str, ...] = (
    "source_label",
    "dataset_id",
    "first_year",
    "latest_year",
    "year_range",
    "latest_period",
    "llm_enabled",
    "publish_generated_as_live",
    "available_locales",
    "default_locale",
    "available_biomes",
    "custom_tags",
)


def _normalize_report_meta(meta: dict[str, Any] | None) -> dict[str, Any]:
    """Same contract as dataset manifests: strict keys stay top-level, rest → custom_tags."""
    if not meta:
        return {"custom_tags": {}}
    if not isinstance(meta, dict):
        raise TypeError("report meta must be a dict or None")

    normalized: dict[str, Any] = {}
    custom_tags: dict[str, Any] = {}
    for k, v in meta.items():
        if k == "schema_version":
            continue  # root-level concern, not meta
        if k in _STRICT_REPORT_META_KEYS:
            normalized[k] = v
        else:
            custom_tags[k] = v

    existing_tags = normalized.get("custom_tags") or {}
    if not isinstance(existing_tags, dict):
        raise TypeError("report meta.custom_tags must be a dict")
    normalized["custom_tags"] = {**existing_tags, **custom_tags}
    return normalized


def _to_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def publish_report_package(
    storage: Any,
    package: dict[str, Any],
    logger: Any,
) -> dict[str, Any]:
    report_id = package["report_id"]
    title = package["title"]
    bucket_prefix = package["bucket_prefix"].rstrip("/")

    generated_report = package["generated_report"]
    live_report = package["live_report"]

    generated_path = f"{bucket_prefix}/generated/report.json"
    live_path = f"{bucket_prefix}/live/report.json"
    stable_live_path = f"{bucket_prefix}/report.json"
    manifest_path = f"{bucket_prefix}/manifest.json"

    storage.upload_bytes(
        object_path=generated_path,
        data=_to_bytes(generated_report),
        content_type="application/json",
        upsert=True,
    )

    storage.upload_bytes(
        object_path=live_path,
        data=_to_bytes(live_report),
        content_type="application/json",
        upsert=True,
    )

    storage.upload_bytes(
        object_path=stable_live_path,
        data=_to_bytes(live_report),
        content_type="application/json",
        upsert=True,
    )

    manifest = {
        "schema_version": REPORT_MANIFEST_SCHEMA_VERSION,
        "report_id": report_id,
        "title": title,
        "generated_at": generated_report.get("generated_at"),
        "live_generated_at": live_report.get("generated_at"),
        "generation_status": package.get("generation_status", "success"),
        "warnings": list(package.get("warnings") or []),
        "bucket_prefix": bucket_prefix,
        "paths": {
            "generated_report": generated_path,
            "live_report": live_path,
            "stable_live_report": stable_live_path,
            "manifest": manifest_path,
        },
        "public_urls": {
            "generated_report": storage.public_url(generated_path),
            "live_report": storage.public_url(live_path),
            "stable_live_report": storage.public_url(stable_live_path),
            "manifest": storage.public_url(manifest_path),
        },
        "meta": _normalize_report_meta(package.get("meta")),
    }

    storage.upload_bytes(
        object_path=manifest_path,
        data=_to_bytes(manifest),
        content_type="application/json",
        upsert=True,
    )

    logger.info("Report publicado: %s", manifest["public_urls"]["live_report"])
    return manifest