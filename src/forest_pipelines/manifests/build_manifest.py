# src/forest_pipelines/manifests/build_manifest.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_manifest(
    dataset_id: str,
    title: str,
    source_dataset_url: str,
    bucket_prefix: str,
    items: list[dict[str, Any]],
    meta: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "title": title,
        "source_dataset_url": source_dataset_url,
        "generated_at": now_iso(),
        "bucket_prefix": bucket_prefix,
        "items": items,
        "meta": meta,
    }
