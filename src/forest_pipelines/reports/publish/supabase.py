# src/forest_pipelines/reports/publish/supabase.py
from __future__ import annotations

import json
from typing import Any


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
        "report_id": report_id,
        "title": title,
        "generated_at": generated_report.get("generated_at"),
        "live_generated_at": live_report.get("generated_at"),
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
        "meta": package.get("meta", {}),
    }

    storage.upload_bytes(
        object_path=manifest_path,
        data=_to_bytes(manifest),
        content_type="application/json",
        upsert=True,
    )

    logger.info("Report publicado: %s", manifest["public_urls"]["live_report"])
    return manifest