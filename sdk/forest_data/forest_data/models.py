"""Lightweight dataclasses mirroring the API v1 envelope payloads."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class DatasetSummary:
    id: str
    slug: str
    title: str
    description: str
    source_id: str
    source_title: str
    category_title: str
    subcategory_title: str
    source_url: str
    manifest_path: str
    segment_title: str | None = None
    generated_at: str | None = None
    last_release_iso: str | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "DatasetSummary":
        return cls(
            id=raw["id"],
            slug=raw["slug"],
            title=raw["title"],
            description=raw.get("description", ""),
            source_id=raw["source_id"],
            source_title=raw["source_title"],
            category_title=raw["category_title"],
            subcategory_title=raw["subcategory_title"],
            source_url=raw["source_url"],
            manifest_path=raw["manifest_path"],
            segment_title=raw.get("segment_title"),
            generated_at=raw.get("generated_at"),
            last_release_iso=raw.get("last_release_iso"),
        )


@dataclass(frozen=True, slots=True)
class OpenDataItem:
    kind: str
    period: str
    filename: str
    sha256: str
    size_bytes: int
    public_url: str
    source_url: str
    storage_path: str | None = None
    title: str | None = None
    release_time: str | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "OpenDataItem":
        return cls(
            kind=raw.get("kind", "data"),
            period=raw["period"],
            filename=raw["filename"],
            sha256=raw["sha256"],
            size_bytes=int(raw["size_bytes"]),
            public_url=raw["public_url"],
            source_url=raw["source_url"],
            storage_path=raw.get("storage_path"),
            title=raw.get("title"),
            release_time=raw.get("release_time"),
        )


@dataclass(frozen=True, slots=True)
class DatasetManifest:
    schema_version: str
    dataset_id: str
    title: str
    source_dataset_url: str
    bucket_prefix: str
    generated_at: str
    generation_status: str
    warnings: list[str]
    items: list[OpenDataItem]
    meta: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "DatasetManifest":
        return cls(
            schema_version=raw["schema_version"],
            dataset_id=raw["dataset_id"],
            title=raw["title"],
            source_dataset_url=raw["source_dataset_url"],
            bucket_prefix=raw["bucket_prefix"],
            generated_at=raw["generated_at"],
            generation_status=raw["generation_status"],
            warnings=list(raw.get("warnings", [])),
            items=[OpenDataItem.from_dict(it) for it in raw.get("items", [])],
            meta=dict(raw.get("meta", {})),
        )


@dataclass(frozen=True, slots=True)
class ReportSummary:
    id: str
    slug: str
    title: str
    description: str
    source_title: str
    category_title: str
    manifest_path: str
    stable_report_path: str
    tags: list[str] = field(default_factory=list)
    source_dataset_url: str | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ReportSummary":
        return cls(
            id=raw["id"],
            slug=raw["slug"],
            title=raw["title"],
            description=raw.get("description", ""),
            source_title=raw["source_title"],
            category_title=raw["category_title"],
            manifest_path=raw["manifest_path"],
            stable_report_path=raw["stable_report_path"],
            tags=list(raw.get("tags", [])),
            source_dataset_url=raw.get("source_dataset_url"),
        )
