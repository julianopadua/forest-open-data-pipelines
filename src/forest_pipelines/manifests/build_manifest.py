# src/forest_pipelines/manifests/build_manifest.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Literal

MANIFEST_SCHEMA_VERSION = "1.0"

GenerationStatus = Literal["success", "success_partial_fallback", "failed"]

_STRICT_META_KEYS: tuple[str, ...] = (
    "source_agency",
    "notes",
    "metadata_file",
    "release",
    "custom_tags",
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_meta(meta: dict[str, Any] | None) -> dict[str, Any]:
    """
    Enforce the strict meta envelope:
      - Known keys stay at the top level (source_agency, notes, metadata_file, release).
      - Any other keys are collapsed into meta.custom_tags.

    Passing a legacy meta (loose dict or file-descriptor) is tolerated: unknown keys
    are moved into custom_tags, and a legacy file-descriptor meta (carrying
    `kind: meta|metadata` and `filename`) is promoted into `metadata_file`.
    """
    if meta is None:
        return {"custom_tags": {}}

    if not isinstance(meta, dict):
        raise TypeError("meta must be a dict or None")

    normalized: dict[str, Any] = {}
    custom_tags: dict[str, Any] = {}

    # Legacy shape: meta itself was a file-descriptor (CVM-style metadata_file).
    if meta.get("kind") in {"meta", "metadata"} and "filename" in meta:
        normalized["metadata_file"] = {
            k: v for k, v in meta.items() if k != "kind"
        }
        return {"custom_tags": {}, **normalized}

    for k, v in meta.items():
        if k in _STRICT_META_KEYS:
            normalized[k] = v
        else:
            custom_tags[k] = v

    # Merge any pre-existing custom_tags the caller provided.
    existing_tags = normalized.get("custom_tags") or {}
    if not isinstance(existing_tags, dict):
        raise TypeError("meta.custom_tags must be a dict")
    merged_tags = {**existing_tags, **custom_tags}
    normalized["custom_tags"] = merged_tags

    return normalized


def build_manifest(
    dataset_id: str,
    title: str,
    source_dataset_url: str,
    bucket_prefix: str,
    items: list[dict[str, Any]],
    meta: dict[str, Any] | None,
    *,
    generation_status: GenerationStatus = "success",
    warnings: Iterable[str] | None = None,
    schema_version: str = MANIFEST_SCHEMA_VERSION,
) -> dict[str, Any]:
    """Build the versioned dataset manifest envelope (schema 1.0+)."""
    return {
        "schema_version": schema_version,
        "dataset_id": dataset_id,
        "title": title,
        "source_dataset_url": source_dataset_url,
        "generated_at": now_iso(),
        "generation_status": generation_status,
        "warnings": list(warnings) if warnings else [],
        "bucket_prefix": bucket_prefix,
        "items": items,
        "meta": _normalize_meta(meta),
    }
