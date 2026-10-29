from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable
from urllib.parse import unquote, urlsplit

DATASET_PREFIXES = frozenset({"anp", "cvm", "eia", "inmet", "inpe", "mma"})
RAW_META_SUFFIXES = frozenset({".csv", ".pdf", ".txt", ".xls", ".xlsx", ".zip"})
REFERENCE_KEYS = frozenset({"source_url", "public_url", "storage_path", "url", "href"})


@dataclass(frozen=True)
class StorageObject:
    path: str
    size_bytes: int


def is_legacy_object(path: str) -> bool:
    parts = path.split("/")
    if len(parts) < 3:
        return False
    if parts[:3] == ["news", "noticias-agricolas", "snapshots"]:
        return True
    if parts[0] not in DATASET_PREFIXES:
        return False
    if "data" in parts[1:-1]:
        return True
    if "meta" in parts[1:-1]:
        return any(path.lower().endswith(suffix) for suffix in RAW_META_SUFFIXES)
    return False


def _row_size(row: dict[str, Any]) -> int:
    metadata = row.get("metadata")
    if not isinstance(metadata, dict):
        return 0
    try:
        return max(0, int(metadata.get("size") or 0))
    except (TypeError, ValueError):
        return 0


def list_objects(bucket_client: Any) -> list[StorageObject]:
    objects: list[StorageObject] = []
    visited: set[str] = set()

    def visit(prefix: str) -> None:
        if prefix in visited:
            return
        visited.add(prefix)
        offset = 0
        while True:
            rows = bucket_client.list(
                prefix,
                {"limit": 100, "offset": offset, "sortBy": {"column": "name", "order": "asc"}},
            )
            if not isinstance(rows, list):
                raise RuntimeError(f"Storage listing returned an invalid response at {prefix}")
            for row in rows:
                if not isinstance(row, dict) or not isinstance(row.get("name"), str):
                    raise RuntimeError(f"Storage listing returned an invalid object at {prefix}")
                path = f"{prefix}/{row['name']}" if prefix else row["name"]
                if row.get("id") is None and row.get("metadata") is None:
                    visit(path)
                else:
                    objects.append(StorageObject(path=path, size_bytes=_row_size(row)))
            if len(rows) < 100:
                break
            offset += len(rows)

    visit("")
    return sorted(objects, key=lambda obj: obj.path)


def _storage_path_from_reference(value: str, bucket: str) -> str | None:
    if value.startswith(f"{bucket}/"):
        return value[len(bucket) + 1:]
    parsed = urlsplit(value)
    marker = f"/storage/v1/object/public/{bucket}/"
    position = parsed.path.find(marker)
    if position >= 0:
        return unquote(parsed.path[position + len(marker):])
    return None


def referenced_legacy_paths(payload: Any, bucket: str, candidates: set[str]) -> set[str]:
    found: set[str] = set()

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key in REFERENCE_KEYS and isinstance(value, str):
                    path = _storage_path_from_reference(value, bucket)
                    if value in candidates:
                        found.add(value)
                    elif path in candidates:
                        found.add(path)
                else:
                    visit(value)
        elif isinstance(node, list):
            for value in node:
                visit(value)

    visit(payload)
    return found


def audit_references(bucket_client: Any, bucket: str, objects: Iterable[StorageObject]) -> set[str]:
    all_objects = list(objects)
    candidates = {obj.path for obj in all_objects if is_legacy_object(obj.path)}
    referenced: set[str] = set()
    for obj in all_objects:
        if obj.path in candidates or not obj.path.endswith(".json"):
            continue
        raw = bucket_client.download(obj.path)
        if not isinstance(raw, (bytes, bytearray)):
            raise RuntimeError(f"Could not read JSON object {obj.path}")
        payload = json.loads(raw)
        referenced.update(referenced_legacy_paths(payload, bucket, candidates))
    return referenced
