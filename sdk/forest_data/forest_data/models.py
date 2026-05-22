"""Lightweight dataclasses mirroring the API v1 envelope payloads."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class ProfileWarning:
    code: str
    message: str

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ProfileWarning":
        return cls(
            code=str(raw.get("code", "")),
            message=str(raw.get("message", "")),
        )


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
    title_en: str | None = None
    description_en: str | None = None
    source_title_en: str | None = None
    category_title_en: str | None = None
    subcategory_title_en: str | None = None
    segment_title: str | None = None
    segment_title_en: str | None = None
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
            title_en=raw.get("title_en"),
            description_en=raw.get("description_en"),
            source_title_en=raw.get("source_title_en"),
            category_title_en=raw.get("category_title_en"),
            subcategory_title_en=raw.get("subcategory_title_en"),
            segment_title=raw.get("segment_title"),
            segment_title_en=raw.get("segment_title_en"),
            generated_at=raw.get("generated_at"),
            last_release_iso=raw.get("last_release_iso"),
        )


@dataclass(frozen=True, slots=True)
class OpenDataItem:
    kind: str
    period: str
    filename: str
    source_url: str
    sha256: str | None = None
    size_bytes: int | None = None
    row_count: int | None = None
    column_count: int | None = None
    columns: list[str] = field(default_factory=list)
    content_type: str | None = None
    format: str | None = None
    last_modified: str | None = None
    profiled_at: str | None = None
    profile_status: str | None = None
    profile_warnings: list[ProfileWarning] = field(default_factory=list)
    archive_profile: dict[str, Any] | None = None
    title: str | None = None
    release_time: str | None = None
    source_page_url: str | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "OpenDataItem":
        return cls(
            kind=raw.get("kind", "data"),
            period=raw["period"],
            filename=raw["filename"],
            source_url=raw["source_url"],
            sha256=raw.get("sha256"),
            size_bytes=int(raw["size_bytes"]) if raw.get("size_bytes") is not None else None,
            row_count=int(raw["row_count"]) if raw.get("row_count") is not None else None,
            column_count=int(raw["column_count"]) if raw.get("column_count") is not None else None,
            columns=[str(col) for col in raw.get("columns", [])],
            content_type=raw.get("content_type"),
            format=raw.get("format"),
            last_modified=raw.get("last_modified"),
            profiled_at=raw.get("profiled_at"),
            profile_status=raw.get("profile_status"),
            profile_warnings=[
                ProfileWarning.from_dict(w)
                for w in raw.get("profile_warnings", [])
                if isinstance(w, dict)
            ],
            archive_profile=raw.get("archive_profile"),
            title=raw.get("title"),
            release_time=raw.get("release_time"),
            source_page_url=raw.get("source_page_url"),
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
class ReportSummaryCoverage:
    first_year: int | None = None
    latest_year: int | None = None
    year_range: str | None = None
    latest_period: str | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> "ReportSummaryCoverage | None":
        if not isinstance(raw, dict):
            return None
        return cls(
            first_year=int(raw["first_year"]) if raw.get("first_year") is not None else None,
            latest_year=int(raw["latest_year"]) if raw.get("latest_year") is not None else None,
            year_range=raw.get("year_range"),
            latest_period=raw.get("latest_period"),
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
    title_en: str | None = None
    description_en: str | None = None
    source_title_en: str | None = None
    category_title_en: str | None = None
    excerpt: str | None = None
    excerpt_en: str | None = None
    generated_at: str | None = None
    coverage: ReportSummaryCoverage | None = None
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
            title_en=raw.get("title_en"),
            description_en=raw.get("description_en"),
            source_title_en=raw.get("source_title_en"),
            category_title_en=raw.get("category_title_en"),
            excerpt=raw.get("excerpt"),
            excerpt_en=raw.get("excerpt_en"),
            generated_at=raw.get("generated_at"),
            coverage=ReportSummaryCoverage.from_dict(raw.get("coverage")),
            source_dataset_url=raw.get("source_dataset_url"),
        )
