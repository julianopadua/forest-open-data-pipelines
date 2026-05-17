#src/forest_pipelines/manifests/build_manifest.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Literal

MANIFEST_SCHEMA_VERSION = "2.0"

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


def _normalize_metadata_file(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    normalized = dict(value)
    if "source_url" not in normalized and normalized.get("public_url"):
        normalized["source_url"] = normalized["public_url"]
    normalized.pop("public_url", None)
    normalized.pop("storage_path", None)
    return normalized


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

    #legacy shape: meta itself was a file descriptor
    if meta.get("kind") in {"meta", "metadata"} and "filename" in meta:
        normalized["metadata_file"] = _normalize_metadata_file(
            {k: v for k, v in meta.items() if k != "kind"}
        )
        return {"custom_tags": {}, **normalized}

    for k, v in meta.items():
        if k in _STRICT_META_KEYS:
            normalized[k] = _normalize_metadata_file(v) if k == "metadata_file" else v
        else:
            custom_tags[k] = v

    #merge any existing custom_tags the caller provided
    existing_tags = normalized.get("custom_tags") or {}
    if not isinstance(existing_tags, dict):
        raise TypeError("meta.custom_tags must be a dict")
    merged_tags = {**existing_tags, **custom_tags}
    normalized["custom_tags"] = merged_tags

    return normalized


def _normalize_item(item: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise TypeError("manifest items must be dicts")

    normalized = dict(item)
    if "source_url" not in normalized and normalized.get("public_url"):
        normalized["source_url"] = normalized["public_url"]
    if "source_url" not in normalized and normalized.get("url"):
        normalized["source_url"] = normalized["url"]
    if not normalized.get("source_url"):
        raise ValueError("manifest item missing required source_url")

    normalized.pop("public_url", None)
    normalized.pop("storage_path", None)
    return normalized


def _status_from_items(items: list[dict[str, Any]], fallback: GenerationStatus) -> GenerationStatus:
    if fallback != "success":
        return fallback
    incomplete = {"partial", "failed", "skipped"}
    if any(str(item.get("profile_status") or "") in incomplete for item in items):
        return "success_partial_fallback"
    return fallback


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
    """Build the versioned dataset manifest envelope (schema 2.0+)."""
    normalized_items = [_normalize_item(item) for item in items]
    status = _status_from_items(normalized_items, generation_status)
    return {
        "schema_version": schema_version,
        "dataset_id": dataset_id,
        "title": title,
        "source_dataset_url": source_dataset_url,
        "generated_at": now_iso(),
        "generation_status": status,
        "warnings": list(warnings) if warnings else [],
        "bucket_prefix": bucket_prefix,
        "items": normalized_items,
        "meta": _normalize_meta(meta),
    }
